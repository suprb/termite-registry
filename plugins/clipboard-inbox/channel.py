#!/usr/bin/env python3
"""Clipboard Inbox: explicit, bounded macOS clipboard polling."""

from pathlib import Path
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import selectors
import signal
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request


CHANNEL_ID = "dev.termite.clipboard-inbox.clipboard"
CONFIG_PATH = Path(__file__).with_name("config.json")
MAX_CLIPBOARD_BYTES = 64 * 1024
MAX_HTTP_BYTES = 1024 * 1024
MAX_EVENT_BYTES = 128 * 1024
SAFE_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}


def as_bool(value):
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "on"}


def utf8_prefix(value, max_bytes):
    return str(value).encode("utf-8")[:max_bytes].decode("utf-8", "ignore")


def iso_now(offset_seconds=0):
    value = datetime.now(timezone.utc) + timedelta(seconds=max(0, offset_seconds))
    return value.isoformat().replace("+00:00", "Z")


def load_config(path=CONFIG_PATH, environ=None):
    environ = os.environ if environ is None else environ
    config = {
        "enabled": False, "pollClipboard": False, "writeApprovedReplies": False,
        "includeCurrentClipboard": False, "pollIntervalSeconds": 1,
    }
    if path.exists():
        if path.stat().st_size > 64 * 1024:
            raise ValueError("config.json exceeds 64 KiB")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("config.json must contain an object")
        config.update(value)
    mapping = {
        "enabled": "TERMITE_CLIPBOARD_ENABLED",
        "pollClipboard": "TERMITE_CLIPBOARD_POLL",
        "writeApprovedReplies": "TERMITE_CLIPBOARD_WRITE_REPLIES",
        "includeCurrentClipboard": "TERMITE_CLIPBOARD_INCLUDE_CURRENT",
        "pollIntervalSeconds": "TERMITE_CLIPBOARD_INTERVAL",
    }
    for key, env_key in mapping.items():
        if environ.get(env_key) not in (None, ""):
            config[key] = environ[env_key]
    for key in ("enabled", "pollClipboard", "writeApprovedReplies", "includeCurrentClipboard"):
        config[key] = as_bool(config.get(key))
    try:
        config["pollIntervalSeconds"] = min(300.0, max(0.5, float(config["pollIntervalSeconds"])))
    except (TypeError, ValueError) as exc:
        raise ValueError("pollIntervalSeconds must be a number") from exc
    if not config["enabled"] or not config["pollClipboard"]:
        raise ValueError("clipboard polling requires enabled and pollClipboard to be true")
    return config


def run_bounded(argv, input_bytes=b"", timeout=3, max_stdout=MAX_CLIPBOARD_BYTES):
    """Run a fixed argv without a shell, killing it at timeout or output limit."""
    with tempfile.TemporaryFile() as stdin_file:
        stdin_file.write(input_bytes)
        stdin_file.seek(0)
        process = subprocess.Popen(
            argv, stdin=stdin_file, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            shell=False, env=SAFE_ENV, close_fds=True, start_new_session=True,
        )
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        chunks = {"stdout": bytearray(), "stderr": bytearray()}
        deadline = time.monotonic() + timeout
        try:
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"{argv[0]} exceeded {timeout:g}s")
                for key, _ in selector.select(min(remaining, 0.1)):
                    data = os.read(key.fileobj.fileno(), 8192)
                    if not data:
                        selector.unregister(key.fileobj)
                        continue
                    chunks[key.data].extend(data)
                    limit = max_stdout if key.data == "stdout" else 4096
                    if len(chunks[key.data]) > limit:
                        raise ValueError(f"{argv[0]} {key.data} exceeds {limit} bytes")
                if process.poll() is not None and not selector.get_map():
                    break
            return_code = process.wait(timeout=max(0.01, deadline - time.monotonic()))
        except Exception:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
        finally:
            selector.close()
            process.stdout.close()
            process.stderr.close()
        if return_code != 0:
            error = chunks["stderr"].decode("utf-8", "replace").strip()[:1024]
            raise RuntimeError(f"{argv[0]} exited {return_code}: {error}")
        return bytes(chunks["stdout"])


def read_clipboard(runner=run_bounded):
    return runner(["/usr/bin/pbpaste"], timeout=3, max_stdout=MAX_CLIPBOARD_BYTES)


def write_clipboard(body, runner=run_bounded):
    encoded = body.encode("utf-8")
    if len(encoded) > MAX_CLIPBOARD_BYTES:
        raise ValueError("approved reply exceeds 64 KiB")
    runner(["/usr/bin/pbcopy"], input_bytes=encoded, timeout=3, max_stdout=1024)
    return hashlib.sha256(encoded).hexdigest()


def clipboard_item(raw):
    if len(raw) > MAX_CLIPBOARD_BYTES:
        raise ValueError("clipboard exceeds 64 KiB")
    body = raw.decode("utf-8")
    if not body.strip():
        return None
    digest = hashlib.sha256(raw).hexdigest()
    first_line = next((line.strip() for line in body.splitlines() if line.strip()), "Clipboard text")
    return {
        "id": "clip-" + digest[:24],
        "deliveryID": "clipboard:" + digest,
        "conversationID": "macos-clipboard",
        "senderID": "local-clipboard",
        "senderName": "macOS Clipboard",
        "title": utf8_prefix(first_line, 512),
        "body": body,
    }


class TermiteAPI:
    def __init__(self, port, token, timeout=10):
        self.base = f"http://127.0.0.1:{int(port)}"
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self.timeout = timeout

    def request(self, path, body=None):
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        req = urllib.request.Request(self.base + path, data=data, headers=self.headers,
                                     method="GET" if body is None else "POST")
        with urllib.request.urlopen(req, timeout=self.timeout) as response:
            raw = response.read(MAX_HTTP_BYTES + 1)
        if len(raw) > MAX_HTTP_BYTES:
            raise ValueError("Termite response exceeds 1 MiB")
        return json.loads(raw) if raw else {}

    def ack(self, reply_id, delivered, error=None):
        body = {"delivered": delivered}
        if error:
            body["error"] = error[:1024]
        rid = urllib.parse.quote(str(reply_id), safe="")
        return self.request(f"/v1/channel-replies/{rid}/ack", body)

    def begin_attempt(self, reply_id):
        rid = urllib.parse.quote(str(reply_id), safe="")
        return self.request(f"/v1/channel-replies/{rid}/attempt", {})

    def verification_needed(self, reply_id, error):
        rid = urllib.parse.quote(str(reply_id), safe="")
        return self.request(f"/v1/channel-replies/{rid}/ack", {
            "state": "verification-needed", "error": utf8_prefix(error, 1024),
        })

    def report_health(self, status, detail="", error="", retry_in=None):
        body = {"status": status, "detail": utf8_prefix(detail, 4096)}
        if status == "healthy":
            body["lastSuccessAt"] = iso_now()
        if error:
            body["error"] = utf8_prefix(error, 4096)
            body["lastErrorAt"] = iso_now()
        if retry_in is not None:
            body["nextRetryAt"] = iso_now(retry_in)
        return self.request(f"/v1/channels/{CHANNEL_ID}/health", body)

    def events(self):
        req = urllib.request.Request(self.base + "/v1/events", headers=self.headers)
        return urllib.request.urlopen(req, timeout=65)


class Connector:
    def __init__(self, api, config, clipboard_reader=read_clipboard, clipboard_writer=write_clipboard):
        self.api, self.config = api, config
        self.clipboard_reader, self.clipboard_writer = clipboard_reader, clipboard_writer
        self.last_digest = None
        self.initialized = False
        self.write_generation = 0
        self.lock = threading.Lock()

    def health(self, status, **fields):
        try:
            self.api.report_health(status, **fields)
        except Exception as exc:
            print(f"Clipboard health update failed: {exc}", flush=True)

    def poll_once(self):
        with self.lock:
            generation_before_read = self.write_generation
        raw = self.clipboard_reader()
        digest = hashlib.sha256(raw).hexdigest()
        with self.lock:
            if digest == self.last_digest:
                return None
            # A reply write happened while pbpaste was sampling. Discard the
            # stale/ambiguous sample; a later poll will see the final value.
            if generation_before_read != self.write_generation:
                return None
            if not self.initialized and not self.config["includeCurrentClipboard"]:
                self.last_digest, self.initialized = digest, True
                return None
            generation = self.write_generation
        item = clipboard_item(raw)
        if item:
            self.api.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)
        with self.lock:
            # Do not overwrite the digest installed by a concurrent approved
            # reply; doing so would ingest that reply on the next poll.
            if generation == self.write_generation:
                self.last_digest = digest
            self.initialized = True
        self.health("healthy", detail="Clipboard poll completed")
        return item

    def deliver(self, reply):
        if reply.get("channel") not in (None, CHANNEL_ID):
            return
        self.api.begin_attempt(reply["id"])
        try:
            digest = self.clipboard_writer(str(reply.get("body", "")))
            with self.lock:
                # Never turn text this connector just wrote into incoming work.
                self.last_digest, self.initialized = digest, True
                self.write_generation += 1
        except Exception as exc:
            message = f"clipboard write is ambiguous: {exc}"
            self.health("degraded", error=message,
                        detail="Clipboard delivery needs user verification")
            self.api.verification_needed(reply["id"], message)
        else:
            self.health("healthy", detail="Approved reply was written to the clipboard")
            self.api.ack(reply["id"], True)

    def recover(self):
        for reply in self.api.request("/v1/channel-replies").get("replies", []):
            self.deliver(reply)

    def reply_loop(self):
        delay = 1.0
        while True:
            try:
                self.recover()
                with self.api.events() as events:
                    delay = 1.0
                    while True:
                        raw = events.readline(MAX_EVENT_BYTES + 1)
                        if not raw:
                            raise ConnectionError("event stream closed")
                        if len(raw) > MAX_EVENT_BYTES:
                            raise ValueError("event line exceeds 128 KiB")
                        if raw.startswith(b"data: "):
                            event = json.loads(raw[6:])
                            if event.get("kind") == "channel-reply":
                                self.deliver(event)
            except Exception as exc:
                print(f"Clipboard reply stream: {exc}; retrying in {delay:g}s", flush=True)
                self.health("retrying", error=str(exc), retry_in=delay,
                            detail="Clipboard reply stream disconnected")
                time.sleep(delay)
                delay = min(delay * 2, 30.0)


def main():
    import sys
    try:
        config = load_config()
        if sys.platform != "darwin":
            raise ValueError("Clipboard Inbox requires macOS pbpaste/pbcopy")
        port, token = os.environ["TERMITE_PORT"], os.environ["TERMITE_TOKEN"]
    except (KeyError, ValueError, OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Clipboard Inbox not started: {exc}")
    api = TermiteAPI(port, token)
    replies = ["reply"] if config["writeApprovedReplies"] else []
    registration = api.request("/v1/channels", {
        "id": CHANNEL_ID,
        "name": "Clipboard Inbox",
        "service": "macOS Clipboard",
        "account": "local",
        "description": "Opt-in clipboard changes",
        "replyCapabilities": replies,
    })
    connector = Connector(api, config)
    if config["writeApprovedReplies"]:
        for pending in registration.get("pendingReplies", []):
            connector.deliver(pending)
        threading.Thread(target=connector.reply_loop, daemon=True, name="clipboard-replies").start()
    delay = config["pollIntervalSeconds"]
    while True:
        try:
            connector.poll_once()
            delay = config["pollIntervalSeconds"]
        except Exception as exc:
            print(f"Clipboard poll: {exc}; retrying in {delay:g}s", flush=True)
            connector.health("retrying", error=str(exc), retry_in=delay,
                             detail="Clipboard poll failed")
            delay = min(delay * 2, 30.0)
        time.sleep(delay)


if __name__ == "__main__":
    main()
