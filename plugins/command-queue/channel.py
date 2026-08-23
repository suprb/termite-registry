#!/usr/bin/env python3
"""Command Queue: explicit argv-only JSON producer and approved-reply consumer."""

from collections import deque
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


CHANNEL_ID = "dev.termite.command-queue.queue"
CONFIG_PATH = Path(__file__).with_name("config.json")
MAX_BODY_BYTES = 64 * 1024
MAX_PRODUCER_BYTES = 512 * 1024
MAX_HTTP_BYTES = 1024 * 1024
MAX_EVENT_BYTES = 128 * 1024
SAFE_ENV = {"PATH": "/usr/bin:/bin:/usr/local/bin", "LANG": "C.UTF-8"}


def as_bool(value):
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "on"}


def utf8_prefix(value, max_bytes):
    return str(value).encode("utf-8")[:max_bytes].decode("utf-8", "ignore")


def field(value, default, max_bytes):
    text = str(value) if value is not None else ""
    return utf8_prefix(text if text.strip() else default, max_bytes)


def iso_now(offset_seconds=0):
    value = datetime.now(timezone.utc) + timedelta(seconds=max(0, offset_seconds))
    return value.isoformat().replace("+00:00", "Z")


def validate_argv(value, name):
    if not isinstance(value, list) or not value or len(value) > 32:
        raise ValueError(f"{name} must be a non-empty JSON argv array (maximum 32 elements)")
    if any(not isinstance(part, str) or not part or len(part.encode()) > 4096 for part in value):
        raise ValueError(f"{name} argv elements must be non-empty strings no larger than 4 KiB")
    return list(value)


def load_config(path=CONFIG_PATH, environ=None):
    environ = os.environ if environ is None else environ
    config = {
        "enabled": False, "producer": [], "consumer": [], "pollIntervalSeconds": 5,
        "producerTimeoutSeconds": 5, "consumerTimeoutSeconds": 10, "maxItemsPerPoll": 16,
    }
    if path.exists():
        if path.stat().st_size > 64 * 1024:
            raise ValueError("config.json exceeds 64 KiB")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("config.json must contain an object")
        config.update(value)
    if environ.get("TERMITE_COMMAND_QUEUE_ENABLED") not in (None, ""):
        config["enabled"] = environ["TERMITE_COMMAND_QUEUE_ENABLED"]
    for key, env_key in (("producer", "TERMITE_COMMAND_QUEUE_PRODUCER_JSON"),
                         ("consumer", "TERMITE_COMMAND_QUEUE_CONSUMER_JSON")):
        if environ.get(env_key) not in (None, ""):
            config[key] = json.loads(environ[env_key])
    numeric = {
        "pollIntervalSeconds": "TERMITE_COMMAND_QUEUE_INTERVAL",
        "producerTimeoutSeconds": "TERMITE_COMMAND_QUEUE_PRODUCER_TIMEOUT",
        "consumerTimeoutSeconds": "TERMITE_COMMAND_QUEUE_CONSUMER_TIMEOUT",
        "maxItemsPerPoll": "TERMITE_COMMAND_QUEUE_MAX_ITEMS",
    }
    for key, env_key in numeric.items():
        if environ.get(env_key) not in (None, ""):
            config[key] = environ[env_key]
    config["enabled"] = as_bool(config.get("enabled"))
    if not config["enabled"]:
        raise ValueError("Command Queue is disabled; copy config.example.json and opt in")
    config["producer"] = validate_argv(config.get("producer"), "producer")
    config["consumer"] = validate_argv(config.get("consumer"), "consumer")
    try:
        config["pollIntervalSeconds"] = min(300.0, max(1.0, float(config["pollIntervalSeconds"])))
        config["producerTimeoutSeconds"] = min(60.0, max(0.5, float(config["producerTimeoutSeconds"])))
        config["consumerTimeoutSeconds"] = min(60.0, max(0.5, float(config["consumerTimeoutSeconds"])))
        config["maxItemsPerPoll"] = min(64, max(1, int(config["maxItemsPerPoll"])))
    except (TypeError, ValueError) as exc:
        raise ValueError("queue interval, timeout, or item limit is invalid") from exc
    return config


def run_bounded(argv, input_bytes=b"", timeout=5, max_stdout=MAX_PRODUCER_BYTES):
    """Execute argv literally with bounded I/O, time, and inherited authority."""
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


def parse_producer_output(raw, max_items):
    if len(raw) > MAX_PRODUCER_BYTES:
        raise ValueError("producer output exceeds 512 KiB")
    value = json.loads(raw.decode("utf-8"))
    if isinstance(value, dict) and "items" in value:
        value = value["items"]
    elif isinstance(value, dict):
        value = [value]
    if not isinstance(value, list):
        raise ValueError("producer must output a JSON item, array, or {items:[...]}")
    if len(value) > max_items:
        raise ValueError(f"producer returned more than {max_items} items")
    return [queue_item(item) for item in value]


def queue_item(value):
    if not isinstance(value, dict):
        raise ValueError("each producer item must be an object")
    delivery = value.get("deliveryID")
    if not isinstance(delivery, str) or not delivery or len(delivery.encode()) > 512:
        raise ValueError("each producer item needs a stable deliveryID no larger than 512 bytes")
    body = value.get("body")
    if not isinstance(body, str) or not body.strip():
        raise ValueError("each producer item needs a non-empty string body")
    if len(body.encode("utf-8")) > MAX_BODY_BYTES:
        raise ValueError("producer item body exceeds 64 KiB")
    digest = hashlib.sha256(delivery.encode("utf-8")).hexdigest()
    result = {
        "id": "queue-" + digest[:24],
        "deliveryID": "command-queue:" + digest,
        "conversationID": field(value.get("conversationID"), "command-queue", 512),
        "senderID": field(value.get("senderID"), "configured-producer", 512),
        "senderName": field(value.get("senderName"), "Command Queue", 256),
        "title": field(value.get("title"), "Queued work", 512),
        "body": body,
    }
    for key in ("replyToID", "createdAt", "projectHint"):
        if value.get(key) is not None:
            result[key] = utf8_prefix(value[key], 128 if key == "createdAt" else
                                      (512 if key == "replyToID" else 4096))
    return result


def reply_payload(reply):
    body = str(reply.get("body", ""))
    if len(body.encode("utf-8")) > MAX_BODY_BYTES:
        raise ValueError("approved reply exceeds 64 KiB")
    return {
        "version": 1,
        "id": str(reply["id"]),
        "channel": CHANNEL_ID,
        "conversationID": str(reply.get("conversationID", "")),
        "replyToID": reply.get("replyToID"),
        "kind": str(reply.get("replyKind", reply.get("kind", "result"))),
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
    def __init__(self, api, config, runner=run_bounded):
        self.api, self.config, self.runner = api, config, runner
        self.seen, self.order = set(), deque()

    def health(self, status, **fields):
        try:
            self.api.report_health(status, **fields)
        except Exception as exc:
            print(f"Command Queue health update failed: {exc}", flush=True)

    def remember(self, delivery_id):
        if delivery_id in self.seen:
            return False
        self.seen.add(delivery_id)
        self.order.append(delivery_id)
        while len(self.order) > 4096:
            self.seen.discard(self.order.popleft())
        return True

    def poll_once(self):
        raw = self.runner(
            self.config["producer"], timeout=self.config["producerTimeoutSeconds"],
            max_stdout=MAX_PRODUCER_BYTES,
        )
        submitted = []
        for item in parse_producer_output(raw, self.config["maxItemsPerPoll"]):
            if item["deliveryID"] in self.seen:
                continue
            self.api.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)
            self.remember(item["deliveryID"])
            submitted.append(item)
        self.health("healthy", detail="Command producer poll completed")
        return submitted

    def deliver(self, reply):
        if reply.get("channel") not in (None, CHANNEL_ID):
            return
        try:
            encoded = (json.dumps(
                reply_payload(reply), separators=(",", ":"), ensure_ascii=False
            ) + "\n").encode()
        except Exception as exc:
            self.api.ack(reply["id"], False, f"invalid approved reply: {exc}")
            return
        self.api.begin_attempt(reply["id"])
        try:
            self.runner(
                self.config["consumer"], input_bytes=encoded,
                timeout=self.config["consumerTimeoutSeconds"], max_stdout=4096,
            )
        except Exception as exc:
            message = f"consumer execution is ambiguous: {exc}"
            self.health("degraded", error=message,
                        detail="Consumer delivery needs user verification")
            self.api.verification_needed(reply["id"], message)
        else:
            self.health("healthy", detail="Approved reply consumer completed")
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
                print(f"Command Queue reply stream: {exc}; retrying in {delay:g}s", flush=True)
                self.health("retrying", error=str(exc), retry_in=delay,
                            detail="Command Queue reply stream disconnected")
                time.sleep(delay)
                delay = min(delay * 2, 30.0)


def main():
    try:
        config = load_config()
        port, token = os.environ["TERMITE_PORT"], os.environ["TERMITE_TOKEN"]
    except (KeyError, ValueError, OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Command Queue not started: {exc}")
    api = TermiteAPI(port, token)
    registration = api.request("/v1/channels", {
        "id": CHANNEL_ID,
        "name": "Command Queue",
        "service": "Local JSON commands",
        "account": utf8_prefix(Path(config["producer"][0]).name, 256),
        "description": "Explicit producer in; reviewed consumer out",
        "replyCapabilities": ["reply"],
    })
    connector = Connector(api, config)
    for pending in registration.get("pendingReplies", []):
        connector.deliver(pending)
    threading.Thread(target=connector.reply_loop, daemon=True, name="command-queue-replies").start()
    delay = config["pollIntervalSeconds"]
    while True:
        try:
            connector.poll_once()
            delay = config["pollIntervalSeconds"]
        except Exception as exc:
            print(f"Command Queue producer: {exc}; retrying in {delay:g}s", flush=True)
            connector.health("retrying", error=str(exc), retry_in=delay,
                             detail="Command producer poll failed")
            delay = min(delay * 2, 60.0)
        time.sleep(delay)


if __name__ == "__main__":
    main()
