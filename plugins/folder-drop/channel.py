#!/usr/bin/env python3
"""Folder Drop: files in, user-approved JSON replies out."""

from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json
import os
import re
import stat
import threading
import time
import urllib.parse
import urllib.request


CHANNEL_ID = "dev.termite.folder-drop.inbox"
MAX_FILE_BYTES = 64 * 1024
MAX_REPLY_FILE_BYTES = 512 * 1024
MAX_HTTP_BYTES = 1024 * 1024
MAX_EVENT_BYTES = 128 * 1024
CONFIG_PATH = Path(__file__).with_name("config.json")


def as_bool(value):
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "on"}


def utf8_prefix(value, max_bytes):
    raw = str(value).encode("utf-8")
    return raw[:max_bytes].decode("utf-8", "ignore")


def field(value, default, max_bytes):
    text = str(value) if value is not None else ""
    return utf8_prefix(text if text.strip() else default, max_bytes)


def iso_now(offset_seconds=0):
    value = datetime.now(timezone.utc) + timedelta(seconds=max(0, offset_seconds))
    return value.isoformat().replace("+00:00", "Z")


def read_regular_file(path, max_bytes, dir_fd=None):
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, dir_fd=dir_fd)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError(f"{Path(path).name}: not a regular file")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            raw = handle.read(max_bytes + 1)
    finally:
        os.close(fd)
    if len(raw) > max_bytes:
        raise ValueError(f"{Path(path).name}: file exceeds 64 KiB")
    return raw


def load_config(path=CONFIG_PATH, environ=None):
    environ = os.environ if environ is None else environ
    config = {"enabled": False, "pollIntervalSeconds": 2}
    if path.exists():
        if path.stat().st_size > 64 * 1024:
            raise ValueError("config.json exceeds 64 KiB")
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("config.json must contain an object")
        config.update(loaded)
    overrides = {
        "enabled": environ.get("TERMITE_FOLDER_DROP_ENABLED"),
        "inbox": environ.get("TERMITE_FOLDER_DROP_INBOX"),
        "outbox": environ.get("TERMITE_FOLDER_DROP_OUTBOX"),
        "pollIntervalSeconds": environ.get("TERMITE_FOLDER_DROP_INTERVAL"),
    }
    for key, value in overrides.items():
        if value not in (None, ""):
            config[key] = value
    config["enabled"] = as_bool(config.get("enabled"))
    try:
        config["pollIntervalSeconds"] = min(300.0, max(0.5, float(config["pollIntervalSeconds"])))
    except (TypeError, ValueError) as exc:
        raise ValueError("pollIntervalSeconds must be a number") from exc
    if not config["enabled"]:
        raise ValueError("Folder Drop is disabled; copy config.example.json to config.json and set enabled")
    if not config.get("inbox") or not config.get("outbox"):
        raise ValueError("inbox and outbox must both be explicitly configured")
    base = path.parent
    for key in ("inbox", "outbox"):
        candidate = Path(str(config[key])).expanduser()
        config[key] = (base / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    if config["inbox"] == config["outbox"]:
        raise ValueError("inbox and outbox must be different directories")
    return config


class TermiteAPI:
    def __init__(self, port, token, timeout=10):
        self.base = f"http://127.0.0.1:{int(port)}"
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self.timeout = timeout

    def request(self, path, body=None):
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
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
        return urllib.request.urlopen(
            urllib.request.Request(self.base + "/v1/events", headers=self.headers), timeout=65
        )


def file_to_work_item(path):
    if path.is_symlink() or not path.is_file():
        return None
    raw = read_regular_file(path, MAX_FILE_BYTES)
    digest = hashlib.sha256(path.name.encode("utf-8") + b"\0" + raw).hexdigest()
    supplied = {}
    if path.suffix.lower() == ".json":
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"{path.name}: JSON drop must be an object")
        supplied = value
        body = str(value.get("body", ""))
    else:
        body = raw.decode("utf-8")
    if not body.strip():
        raise ValueError(f"{path.name}: body is empty")
    if len(body.encode("utf-8")) > MAX_FILE_BYTES:
        raise ValueError(f"{path.name}: body exceeds 64 KiB")
    item = {
        "id": "drop-" + digest[:24],
        "deliveryID": "folder-drop:" + digest,
        "conversationID": field(supplied.get("conversationID"), "folder-drop", 512),
        "senderID": field(supplied.get("senderID"), "local-folder", 512),
        "senderName": field(supplied.get("senderName"), "Folder Drop", 256),
        "title": field(supplied.get("title"), path.name, 512),
        "body": body,
    }
    if supplied.get("replyToID"):
        item["replyToID"] = str(supplied["replyToID"])
    if supplied.get("projectHint"):
        item["projectHint"] = str(supplied["projectHint"])
    limits = {"replyToID": 512, "projectHint": 4096}
    for key, limit in limits.items():
        if key in item:
            item[key] = utf8_prefix(item[key], limit)
    return item


def reply_filename(reply_id):
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", str(reply_id))[:120]
    suffix = hashlib.sha256(str(reply_id).encode("utf-8")).hexdigest()[:10]
    return f"reply-{safe or 'item'}-{suffix}.json"


def write_reply(outbox, reply):
    body = str(reply.get("body", ""))
    if len(body.encode("utf-8")) > MAX_FILE_BYTES:
        raise ValueError("approved reply exceeds 64 KiB")
    payload = {
        "version": 1,
        "id": str(reply["id"]),
        "conversationID": str(reply.get("conversationID", "")),
        "replyToID": reply.get("replyToID"),
        "kind": str(reply.get("replyKind", reply.get("kind", "result"))),
        "body": body,
    }
    target_name = reply_filename(reply["id"])
    target = outbox / target_name
    encoded = (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    if len(encoded) > MAX_REPLY_FILE_BYTES:
        raise ValueError("serialized approved reply exceeds 512 KiB")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
    directory_fd = os.open(outbox, directory_flags)
    try:
        try:
            fd = os.open(target_name, flags, 0o600, dir_fd=directory_fd)
        except FileExistsError:
            existing = json.loads(read_regular_file(
                target_name, MAX_REPLY_FILE_BYTES, dir_fd=directory_fd
            ))
            if existing.get("id") == payload["id"] and existing.get("body") == payload["body"]:
                return target
            raise ValueError(f"outbox collision at {target.name}")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            # A successful acknowledgement must not precede durable directory
            # metadata for the newly created reply file.
            os.fsync(directory_fd)
        except Exception:
            try:
                os.unlink(target_name, dir_fd=directory_fd)
            except OSError:
                pass
            raise
    finally:
        os.close(directory_fd)
    return target


class Connector:
    def __init__(self, api, config):
        self.api, self.config = api, config
        self.seen = set()
        self.seen_order = deque()

    def remember(self, delivery_id):
        if delivery_id in self.seen:
            return False
        self.seen.add(delivery_id)
        self.seen_order.append(delivery_id)
        while len(self.seen_order) > 4096:
            self.seen.discard(self.seen_order.popleft())
        return True

    def health(self, status, **fields):
        try:
            self.api.report_health(status, **fields)
        except Exception as exc:
            print(f"Folder Drop health update failed: {exc}", flush=True)

    def deliver(self, reply):
        if reply.get("channel") not in (None, CHANNEL_ID):
            return
        self.api.begin_attempt(reply["id"])
        try:
            write_reply(self.config["outbox"], reply)
        except Exception as exc:
            message = f"folder outbox delivery is ambiguous: {exc}"
            self.health("degraded", error=message,
                        detail="Outbox delivery needs user verification")
            self.api.verification_needed(reply["id"], message)
        else:
            self.health("healthy", detail="Approved reply is durable in the outbox")
            self.api.ack(reply["id"], True)

    def recover(self):
        payload = self.api.request("/v1/channel-replies")
        for reply in payload.get("replies", []):
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
                print(f"Folder Drop reply stream: {exc}; retrying in {delay:g}s", flush=True)
                time.sleep(delay)
                delay = min(delay * 2, 30.0)

    def scan_once(self):
        for path in sorted(self.config["inbox"].iterdir(), key=lambda value: value.name):
            if path.name.startswith("."):
                continue
            try:
                item = file_to_work_item(path)
            except Exception as exc:
                print(f"Folder Drop skipped {path.name}: {exc}", flush=True)
                continue
            if item and item["deliveryID"] not in self.seen:
                # Let host/network failures reach the outer exponential backoff.
                self.api.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)
                self.remember(item["deliveryID"])
        self.health("healthy", detail="Folder inbox scan completed")


def main():
    try:
        config = load_config()
        port, token = os.environ["TERMITE_PORT"], os.environ["TERMITE_TOKEN"]
    except (KeyError, ValueError, OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Folder Drop not started: {exc}")
    config["inbox"].mkdir(mode=0o700, parents=True, exist_ok=True)
    config["outbox"].mkdir(mode=0o700, parents=True, exist_ok=True)
    api = TermiteAPI(port, token)
    registration = api.request("/v1/channels", {
        "id": CHANNEL_ID,
        "name": "Folder Drop",
        "service": "Local Files",
        "account": utf8_prefix(config["inbox"].name, 256),
        "description": utf8_prefix(f"Files from {config['inbox']}", 1024),
        "replyCapabilities": ["reply"],
    })
    connector = Connector(api, config)
    for pending in registration.get("pendingReplies", []):
        connector.deliver(pending)
    threading.Thread(target=connector.reply_loop, daemon=True, name="folder-replies").start()
    delay = config["pollIntervalSeconds"]
    while True:
        try:
            connector.scan_once()
            delay = config["pollIntervalSeconds"]
        except Exception as exc:
            print(f"Folder Drop poll: {exc}; retrying in {delay:g}s", flush=True)
            connector.health("retrying", error=str(exc), retry_in=delay,
                             detail="Folder inbox scan failed")
            delay = min(delay * 2, 60.0)
        time.sleep(delay)


if __name__ == "__main__":
    main()
