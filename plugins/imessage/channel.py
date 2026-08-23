#!/usr/bin/env python3
"""Allowlisted, local-only Messages database Channel for macOS."""

from pathlib import Path
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import selectors
import signal
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request


CHANNEL_ID = "dev.termite.imessage.messages"
CONFIG_PATH = Path(__file__).with_name("config.json")
MAX_BODY_BYTES = 64 * 1024
MAX_DB_ROWS = 100
MAX_HTTP_BYTES = 1024 * 1024
MAX_EVENT_BYTES = 128 * 1024
SAFE_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
APPLE_EPOCH_OFFSET = 978307200

SEND_SCRIPT = r'''
on run argv
    if (count of argv) is not 3 then error "expected kind, target, and body"
    set targetKind to item 1 of argv
    set targetID to item 2 of argv
    set replyText to item 3 of argv
    tell application "Messages"
        if targetKind is "chat" then
            repeat with candidate in chats
                if (id of candidate as text) is targetID then
                    send replyText to candidate
                    return "sent"
                end if
            end repeat
            error "allowlisted chat is not available in Messages"
        else if targetKind is "handle" then
            set targetService to first service whose service type is iMessage
            set targetBuddy to buddy targetID of targetService
            send replyText to targetBuddy
            return "sent"
        else
            error "invalid target kind"
        end if
    end tell
end run
'''.strip()


def as_bool(value):
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "on"}


def utf8_prefix(value, max_bytes):
    return str(value).encode("utf-8")[:max_bytes].decode("utf-8", "ignore")


def iso_now(offset_seconds=0):
    value = datetime.now(timezone.utc) + timedelta(seconds=max(0, offset_seconds))
    return value.isoformat().replace("+00:00", "Z")


def string_list(value, name):
    if not isinstance(value, list) or len(value) > 16:
        raise ValueError(f"{name} must be a JSON array with at most 16 values")
    result = []
    for item in value:
        if not isinstance(item, str) or not item.strip() or len(item.encode()) > 480:
            raise ValueError(f"{name} values must be non-empty strings no larger than 480 bytes")
        result.append(item.strip())
    return result


def load_config(path=CONFIG_PATH, environ=None):
    environ = os.environ if environ is None else environ
    config = {
        "enabled": False, "databasePath": "~/Library/Messages/chat.db",
        "allowedHandles": [], "allowedChatGUIDs": [], "sendApprovedReplies": False,
        "includeExistingMessages": False, "pollIntervalSeconds": 3,
        "maxMessagesPerPoll": 50,
    }
    if path.exists():
        if path.stat().st_size > 64 * 1024:
            raise ValueError("config.json exceeds 64 KiB")
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("config.json must contain an object")
        config.update(loaded)
    mapping = {
        "enabled": "TERMITE_IMESSAGE_ENABLED",
        "databasePath": "TERMITE_IMESSAGE_DATABASE",
        "sendApprovedReplies": "TERMITE_IMESSAGE_SEND_REPLIES",
        "includeExistingMessages": "TERMITE_IMESSAGE_INCLUDE_EXISTING",
        "pollIntervalSeconds": "TERMITE_IMESSAGE_INTERVAL",
        "maxMessagesPerPoll": "TERMITE_IMESSAGE_MAX_MESSAGES",
    }
    for key, env_key in mapping.items():
        if environ.get(env_key) not in (None, ""):
            config[key] = environ[env_key]
    for key, env_key in (("allowedHandles", "TERMITE_IMESSAGE_HANDLES_JSON"),
                         ("allowedChatGUIDs", "TERMITE_IMESSAGE_CHATS_JSON")):
        if environ.get(env_key) not in (None, ""):
            config[key] = json.loads(environ[env_key])
    for key in ("enabled", "sendApprovedReplies", "includeExistingMessages"):
        config[key] = as_bool(config.get(key))
    config["allowedHandles"] = string_list(config.get("allowedHandles"), "allowedHandles")
    config["allowedChatGUIDs"] = string_list(config.get("allowedChatGUIDs"), "allowedChatGUIDs")
    try:
        config["pollIntervalSeconds"] = min(300.0, max(1.0, float(config["pollIntervalSeconds"])))
        config["maxMessagesPerPoll"] = min(MAX_DB_ROWS, max(1, int(config["maxMessagesPerPoll"])))
    except (TypeError, ValueError) as exc:
        raise ValueError("pollIntervalSeconds/maxMessagesPerPoll are invalid") from exc
    if not config["enabled"]:
        raise ValueError("iMessage is disabled; copy config.example.json and opt in")
    if not config["allowedHandles"] and not config["allowedChatGUIDs"]:
        raise ValueError("at least one handle or chat GUID must be explicitly allowlisted")
    database = Path(str(config["databasePath"])).expanduser()
    if not database.is_absolute():
        database = path.parent / database
    config["databasePath"] = database.resolve()
    return config


def run_bounded(argv, input_bytes=b"", timeout=15, max_stdout=4096):
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
            raise RuntimeError(f"osascript exited {return_code}: {error}")
        return bytes(chunks["stdout"])


def message_timestamp(value):
    try:
        seconds = float(value or 0)
        if abs(seconds) > 10_000_000_000:
            seconds /= 1_000_000_000
        return datetime.fromtimestamp(seconds + APPLE_EPOCH_OFFSET, timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OSError, OverflowError):
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class MessageSource:
    def __init__(self, database, allowed_handles, allowed_chats):
        self.database = Path(database)
        self.allowed_handles = {value.casefold(): value for value in allowed_handles}
        self.allowed_chats = set(allowed_chats)

    def connect(self):
        uri = "file:" + urllib.parse.quote(str(self.database), safe="/") + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=2)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=1000")
        deadline = time.monotonic() + 2
        connection.set_progress_handler(lambda: 1 if time.monotonic() > deadline else 0, 1000)
        return connection

    def boundary(self):
        with closing(self.connect()) as database:
            return int(database.execute("SELECT COALESCE(MAX(ROWID), 0) FROM message").fetchone()[0])

    def rows(self, after_rowid, limit, upper_rowid=None, newest=False):
        handle_values = list(self.allowed_handles)
        clauses, arguments = ["COALESCE(m.is_from_me, 0) = 0", "m.text IS NOT NULL", "m.ROWID > ?"], [after_rowid]
        allowed = []
        if handle_values:
            allowed.append("lower(h.id) IN (" + ",".join("?" for _ in handle_values) + ")")
            arguments.extend(handle_values)
        if self.allowed_chats:
            allowed.append("c.guid IN (" + ",".join("?" for _ in self.allowed_chats) + ")")
            arguments.extend(sorted(self.allowed_chats))
        clauses.append("(" + " OR ".join(allowed) + ")")
        if upper_rowid is not None:
            clauses.append("m.ROWID <= ?")
            arguments.append(upper_rowid)
        direction = "DESC" if newest else "ASC"
        query = f"""
            SELECT m.ROWID, m.guid,
                   CASE WHEN length(CAST(m.text AS BLOB)) > {MAX_BODY_BYTES}
                        THEN substr(m.text, 1, 16000) || '\n[message truncated]'
                        ELSE m.text END,
                   m.date, h.id, c.guid, c.display_name
              FROM message AS m
              LEFT JOIN handle AS h ON h.ROWID = m.handle_id
              JOIN chat_message_join AS cmj ON cmj.message_id = m.ROWID
              JOIN chat AS c ON c.ROWID = cmj.chat_id
             WHERE {" AND ".join(clauses)}
             ORDER BY m.ROWID {direction}
             LIMIT ?
        """
        arguments.append(limit)
        with closing(self.connect()) as database:
            result = list(database.execute(query, arguments))
        return list(reversed(result)) if newest else result


def message_to_work_item(row, source):
    rowid, guid, body, date_value, handle, chat_guid, chat_name = row
    if not guid or not isinstance(body, str) or not body.strip():
        return None
    body = utf8_prefix(body, MAX_BODY_BYTES)
    if chat_guid in source.allowed_chats:
        conversation = "chat:" + chat_guid
        subject = chat_name or handle or "iMessage chat"
    elif handle and handle.casefold() in source.allowed_handles:
        conversation = "handle:" + source.allowed_handles[handle.casefold()]
        subject = handle
    else:
        return None
    if len(conversation.encode()) > 512:
        return None
    digest = hashlib.sha256(guid.encode("utf-8")).hexdigest()
    delivery = "imessage:" + guid
    if len(delivery.encode("utf-8")) > 512:
        delivery = "imessage-sha256:" + digest
    return {
        "id": "message-" + digest[:24],
        "deliveryID": delivery,
        "conversationID": conversation,
        "replyToID": utf8_prefix(guid, 512),
        "senderID": utf8_prefix(handle or "imessage-sender", 512),
        "senderName": utf8_prefix(handle or "iMessage sender", 256),
        "title": utf8_prefix("Message from " + subject, 512),
        "body": body,
        "createdAt": message_timestamp(date_value),
    }


def resolve_address(conversation_id, config):
    if conversation_id.startswith("chat:"):
        value = conversation_id[5:]
        if value not in set(config["allowedChatGUIDs"]):
            raise ValueError("reply chat is no longer allowlisted")
        return "chat", value
    if conversation_id.startswith("handle:"):
        value = conversation_id[7:]
        handles = {item.casefold(): item for item in config["allowedHandles"]}
        if value.casefold() not in handles:
            raise ValueError("reply handle is no longer allowlisted")
        return "handle", handles[value.casefold()]
    raise ValueError("reply has an invalid Messages address")


def send_approved_reply(reply, config, runner=run_bounded):
    body = str(reply.get("body", ""))
    if not body.strip() or len(body.encode()) > MAX_BODY_BYTES:
        raise ValueError("approved reply is empty or exceeds 64 KiB")
    kind, target = resolve_address(str(reply.get("conversationID", "")), config)
    # The script is constant. Target and untrusted body are separate argv values.
    runner(["/usr/bin/osascript", "-e", SEND_SCRIPT, kind, target, body], timeout=15, max_stdout=4096)


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
        return urllib.request.urlopen(
            urllib.request.Request(self.base + "/v1/events", headers=self.headers), timeout=65
        )


class Connector:
    def __init__(self, api, source, config, sender=send_approved_reply):
        self.api, self.source, self.config, self.sender = api, source, config, sender
        self.cursor, self.initialized = 0, False

    def health(self, status, **fields):
        try:
            self.api.report_health(status, **fields)
        except Exception as exc:
            print(f"iMessage health update failed: {exc}", flush=True)

    def poll_once(self):
        if not self.initialized:
            boundary = self.source.boundary()
            if not self.config["includeExistingMessages"]:
                self.cursor = boundary
                self.initialized = True
                self.health("healthy", detail="Messages database baseline completed")
                return []
            rows = self.source.rows(0, self.config["maxMessagesPerPoll"], boundary, newest=True)
            initial_boundary = boundary
        else:
            rows = self.source.rows(self.cursor, self.config["maxMessagesPerPoll"])
            initial_boundary = None
        submitted = []
        for row in rows:
            item = message_to_work_item(row, self.source)
            if item:
                self.api.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)
                submitted.append(item)
            # Advance only after the host accepts a valid item. Invalid rows
            # are safe to skip, but a failed POST must be retried.
            if initial_boundary is None:
                self.cursor = max(self.cursor, int(row[0]))
        if initial_boundary is not None:
            # Existing-history mode intentionally selects only the newest
            # bounded page, then baselines the rest once every POST succeeds.
            self.cursor = initial_boundary
        self.initialized = True
        self.health("healthy", detail="Messages database poll completed")
        return submitted

    def deliver(self, reply):
        if reply.get("channel") not in (None, CHANNEL_ID):
            return
        self.api.begin_attempt(reply["id"])
        try:
            self.sender(reply, self.config)
        except Exception as exc:
            message = f"Messages Apple Event result is ambiguous: {exc}"
            self.health("degraded", error=message,
                        detail="iMessage delivery needs user verification")
            self.api.verification_needed(reply["id"], message)
        else:
            self.health("healthy", detail="Messages accepted the approved reply")
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
                print(f"iMessage reply stream: {exc}; retrying in {delay:g}s", flush=True)
                self.health("retrying", error=str(exc), retry_in=delay,
                            detail="iMessage reply stream disconnected")
                time.sleep(delay)
                delay = min(delay * 2, 30.0)


def main():
    import sys
    try:
        config = load_config()
        if sys.platform != "darwin":
            raise ValueError("iMessage Channel requires macOS")
        port, token = os.environ["TERMITE_PORT"], os.environ["TERMITE_TOKEN"]
    except (KeyError, ValueError, OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"iMessage not started: {exc}")
    source = MessageSource(config["databasePath"], config["allowedHandles"], config["allowedChatGUIDs"])
    api = TermiteAPI(port, token)
    replies = ["reply"] if config["sendApprovedReplies"] else []
    registration = api.request("/v1/channels", {
        "id": CHANNEL_ID,
        "name": "iMessage",
        "service": "Messages",
        "account": "allowlisted chats",
        "description": "Allowlisted incoming Messages only",
        "replyCapabilities": replies,
    })
    connector = Connector(api, source, config)
    if config["sendApprovedReplies"]:
        for pending in registration.get("pendingReplies", []):
            connector.deliver(pending)
        threading.Thread(target=connector.reply_loop, daemon=True, name="imessage-replies").start()
    delay = config["pollIntervalSeconds"]
    while True:
        try:
            connector.poll_once()
            delay = config["pollIntervalSeconds"]
        except Exception as exc:
            print(f"iMessage poll: {exc}; retrying in {delay:g}s", flush=True)
            connector.health("retrying", error=str(exc), retry_in=delay,
                             detail="Messages database poll failed")
            delay = min(delay * 2, 60.0)
        time.sleep(delay)


if __name__ == "__main__":
    main()
