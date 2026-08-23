#!/usr/bin/env python3
"""Read-only, allowlisted Apple Reminders Channel."""

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
import time
import urllib.request


CHANNEL_ID = "dev.termite.apple-reminders.incomplete"
CONFIG_PATH = Path(__file__).with_name("config.json")
MAX_BODY_BYTES = 64 * 1024
MAX_OUTPUT_BYTES = 512 * 1024
MAX_HTTP_BYTES = 1024 * 1024
SAFE_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}

READ_SCRIPT = r'''
function safeString(object, property) {
    try {
        const value = object[property]();
        return value === null || value === undefined ? "" : String(value);
    } catch (_) { return ""; }
}
function safeDate(object, property) {
    try {
        const value = object[property]();
        return value ? new Date(value).toISOString() : "";
    } catch (_) { return ""; }
}
function run(argv) {
    if (argv.length !== 4) throw new Error("expected list names, ids, limit, and offset");
    const allowedNames = new Set(JSON.parse(argv[0]));
    const allowedIDs = new Set(JSON.parse(argv[1]));
    const limit = Math.max(1, Math.min(100, Number(argv[2])));
    const offset = Math.max(0, Number(argv[3]));
    const app = Application("Reminders");
    const result = [];
    let skipped = 0;
    let more = false;
    outer: for (const list of app.lists()) {
        const listName = safeString(list, "name");
        const listID = safeString(list, "id");
        if (!allowedNames.has(listName) && !allowedIDs.has(listID)) continue;
        for (const reminder of list.reminders()) {
            let completed = false;
            try { completed = Boolean(reminder.completed()); } catch (_) { continue; }
            if (completed) continue;
            if (skipped < offset) { skipped += 1; continue; }
            if (result.length >= limit) { more = true; break outer; }
            result.push({
                id: safeString(reminder, "id"),
                name: safeString(reminder, "name").slice(0, 20000),
                body: safeString(reminder, "body").slice(0, 20000),
                dueDate: safeDate(reminder, "dueDate"),
                modificationDate: safeDate(reminder, "modificationDate"),
                listID: listID,
                listName: listName
            });
        }
    }
    return JSON.stringify({items: result, more: more});
}
'''.strip()


def as_bool(value):
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "on"}


def utf8_prefix(value, max_bytes):
    return str(value).encode("utf-8")[:max_bytes].decode("utf-8", "ignore")


def iso_now(offset_seconds=0):
    value = datetime.now(timezone.utc) + timedelta(seconds=max(0, offset_seconds))
    return value.isoformat().replace("+00:00", "Z")


def string_list(value, name):
    if not isinstance(value, list) or len(value) > 32:
        raise ValueError(f"{name} must be a JSON array with at most 32 values")
    result = []
    for item in value:
        if not isinstance(item, str) or not item.strip() or len(item.encode()) > 512:
            raise ValueError(f"{name} values must be non-empty strings no larger than 512 bytes")
        result.append(item.strip())
    return result


def load_config(path=CONFIG_PATH, environ=None):
    environ = os.environ if environ is None else environ
    config = {"enabled": False, "allowedListNames": [], "allowedListIDs": [],
              "pollIntervalSeconds": 10, "maxRemindersPerPoll": 64}
    if path.exists():
        if path.stat().st_size > 64 * 1024:
            raise ValueError("config.json exceeds 64 KiB")
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("config.json must contain an object")
        config.update(loaded)
    if environ.get("TERMITE_REMINDERS_ENABLED") not in (None, ""):
        config["enabled"] = environ["TERMITE_REMINDERS_ENABLED"]
    for key, env_key in (("allowedListNames", "TERMITE_REMINDERS_LIST_NAMES_JSON"),
                         ("allowedListIDs", "TERMITE_REMINDERS_LIST_IDS_JSON")):
        if environ.get(env_key) not in (None, ""):
            config[key] = json.loads(environ[env_key])
    if environ.get("TERMITE_REMINDERS_INTERVAL") not in (None, ""):
        config["pollIntervalSeconds"] = environ["TERMITE_REMINDERS_INTERVAL"]
    if environ.get("TERMITE_REMINDERS_MAX_ITEMS") not in (None, ""):
        config["maxRemindersPerPoll"] = environ["TERMITE_REMINDERS_MAX_ITEMS"]
    config["enabled"] = as_bool(config.get("enabled"))
    config["allowedListNames"] = string_list(config.get("allowedListNames"), "allowedListNames")
    config["allowedListIDs"] = string_list(config.get("allowedListIDs"), "allowedListIDs")
    try:
        config["pollIntervalSeconds"] = min(300.0, max(2.0, float(config["pollIntervalSeconds"])))
        config["maxRemindersPerPoll"] = min(100, max(1, int(config["maxRemindersPerPoll"])))
    except (TypeError, ValueError) as exc:
        raise ValueError("pollIntervalSeconds/maxRemindersPerPoll are invalid") from exc
    if not config["enabled"]:
        raise ValueError("Apple Reminders is disabled; copy config.example.json and opt in")
    if not config["allowedListNames"] and not config["allowedListIDs"]:
        raise ValueError("at least one Reminders list name or id must be explicitly allowlisted")
    return config


def run_bounded(argv, timeout=15, max_stdout=MAX_OUTPUT_BYTES):
    with tempfile.TemporaryFile() as stdin_file:
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


class ReminderSource:
    def __init__(self, list_names, list_ids, runner=run_bounded):
        self.list_names, self.list_ids, self.runner = list_names, list_ids, runner

    def fetch(self, limit, offset):
        argv = [
            "/usr/bin/osascript", "-l", "JavaScript", "-e", READ_SCRIPT,
            json.dumps(self.list_names, separators=(",", ":")),
            json.dumps(self.list_ids, separators=(",", ":")),
            str(limit), str(offset),
        ]
        raw = self.runner(argv, timeout=15, max_stdout=MAX_OUTPUT_BYTES)
        return parse_output(raw, limit)


def parse_output(raw, limit):
    if len(raw) > MAX_OUTPUT_BYTES:
        raise ValueError("Reminders output exceeds 512 KiB")
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict) or not isinstance(value.get("items"), list):
        raise ValueError("Reminders returned malformed JSON")
    if len(value["items"]) > limit:
        raise ValueError("Reminders returned more than the configured limit")
    return value["items"], value.get("more") is True


def reminder_to_work_item(value):
    if not isinstance(value, dict):
        raise ValueError("reminder must be an object")
    reminder_id = value.get("id")
    name = value.get("name")
    if not isinstance(reminder_id, str) or not reminder_id.strip():
        raise ValueError("reminder has no stable id")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("reminder has no name")
    digest = hashlib.sha256(reminder_id.encode()).hexdigest()
    parts = [f"List: {value.get('listName') or value.get('listID') or 'Reminders'}",
             f"Reminder: {name}"]
    if value.get("dueDate"):
        parts.append(f"Due: {value['dueDate']}")
    if value.get("body"):
        parts.extend(["", str(value["body"])])
    body = utf8_prefix("\n".join(parts), MAX_BODY_BYTES)
    delivery = "reminder:" + reminder_id
    if len(delivery.encode()) > 512:
        delivery = "reminder-sha256:" + digest
    item = {
        "id": "reminder-" + digest[:24],
        "deliveryID": delivery,
        "conversationID": "reminders-list-" + hashlib.sha256(
            str(value.get("listID") or value.get("listName") or "unknown").encode()
        ).hexdigest()[:24],
        "senderID": "apple-reminders",
        "senderName": "Apple Reminders",
        "title": utf8_prefix(name, 512),
        "body": body,
    }
    if value.get("modificationDate"):
        item["createdAt"] = utf8_prefix(value["modificationDate"], 128)
    return item


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


class Connector:
    def __init__(self, api, source, config):
        self.api, self.source, self.config = api, source, config
        self.seen, self.order, self.offset = set(), deque(), 0

    def health(self, status, **fields):
        try:
            self.api.report_health(status, **fields)
        except Exception as exc:
            print(f"Apple Reminders health update failed: {exc}", flush=True)

    def remember(self, delivery_id):
        self.seen.add(delivery_id)
        self.order.append(delivery_id)
        while len(self.order) > 4096:
            self.seen.discard(self.order.popleft())

    def poll_once(self):
        values, more = self.source.fetch(self.config["maxRemindersPerPoll"], self.offset)
        submitted = []
        for value in values:
            try:
                item = reminder_to_work_item(value)
            except ValueError as exc:
                print(f"Apple Reminders skipped an item: {exc}", flush=True)
                continue
            if item["deliveryID"] in self.seen:
                continue
            self.api.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)
            self.remember(item["deliveryID"])
            submitted.append(item)
        self.offset = self.offset + len(values) if more else 0
        self.health("healthy", detail="Apple Reminders poll completed")
        return submitted


def main():
    import sys
    try:
        config = load_config()
        if sys.platform != "darwin":
            raise ValueError("Apple Reminders Channel requires macOS")
        port, token = os.environ["TERMITE_PORT"], os.environ["TERMITE_TOKEN"]
    except (KeyError, ValueError, OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Apple Reminders not started: {exc}")
    source = ReminderSource(config["allowedListNames"], config["allowedListIDs"])
    api = TermiteAPI(port, token)
    api.request("/v1/channels", {
        "id": CHANNEL_ID,
        "name": "Apple Reminders",
        "service": "Reminders",
        "account": "allowlisted lists",
        "description": "Read-only incomplete reminders",
        "replyCapabilities": [],
    })
    connector = Connector(api, source, config)
    delay = config["pollIntervalSeconds"]
    while True:
        try:
            connector.poll_once()
            delay = config["pollIntervalSeconds"]
        except Exception as exc:
            print(f"Apple Reminders poll: {exc}; retrying in {delay:g}s", flush=True)
            connector.health("retrying", error=str(exc), retry_in=delay,
                             detail="Apple Reminders poll failed")
            delay = min(delay * 2, 60.0)
        time.sleep(delay)


if __name__ == "__main__":
    main()
