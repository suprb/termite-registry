#!/usr/bin/env python3
"""Allowlisted Telegram Bot API connector for Termite Channels."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request


EXTENSION_ID = "dev.termite.telegram"
CHANNEL_ID = EXTENSION_ID
KEYCHAIN_SERVICE = "termite.telegram"
TELEGRAM_API_HOST = "https://api.telegram.org"
MAX_HTTP_BYTES = 8 * 1024 * 1024
MAX_SSE_LINE_BYTES = 256 * 1024
HEALTH_STATUSES = {"healthy", "degraded", "retrying", "offline"}
HEALTH_FIELD_BYTES = {"lastSuccessAt": 128, "lastErrorAt": 128, "error": 1024,
                      "nextRetryAt": 128, "detail": 2048}


class ConnectorError(RuntimeError):
    pass


class UncertainDeliveryError(ConnectorError): pass
def iso_now(): return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url): return None


def rejecting_opener(): return urllib.request.build_opener(RejectRedirects())


def read_json(response, source):
    payload = response.read(MAX_HTTP_BYTES + 1)
    if len(payload) > MAX_HTTP_BYTES: raise ConnectorError(f"{source} response exceeded {MAX_HTTP_BYTES} bytes")
    try: return json.loads(payload) if payload else {}
    except json.JSONDecodeError as exc: raise ConnectorError(f"{source} returned invalid JSON") from exc


def utf8_prefix(value, max_bytes): return str(value).encode("utf-8")[:max_bytes].decode("utf-8", "ignore")


def truncate(text, limit=60000):
    encoded = str(text).encode("utf-8")
    if len(encoded) <= limit:
        return str(text)
    return encoded[:limit].decode("utf-8", "ignore") + "\n[truncated by connector]"


def load_file_config(path: Path | None = None) -> dict:
    path = path or Path(__file__).with_name("config.json")
    if not path.exists(): return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConnectorError(f"cannot read {path.name}: {exc}") from exc
    if not isinstance(value, dict): raise ConnectorError(f"{path.name} must contain a JSON object")
    if value.get("botToken") and stat.S_IMODE(path.stat().st_mode) & 0o077:
        print(f"warning: chmod 600 {path} because it contains a token", file=sys.stderr)
    return value


def keychain_secret(service: str = KEYCHAIN_SERVICE) -> str | None:
    security = Path("/usr/bin/security")
    if not security.exists(): return None
    try:
        result = subprocess.run([str(security), "find-generic-password", "-s", service, "-w"],
                                check=False, capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError): return None
    return result.stdout.strip() if result.returncode == 0 else None


def setting(config, key, env, default=None):
    return os.environ.get(env, config.get(key, default))


def parse_allowed(value) -> set[str]:
    values = value if isinstance(value, list) else str(value or "").split(",")
    result = {str(item).strip() for item in values if str(item).strip()}
    if not result: raise ConnectorError("allowedChatIds must explicitly list at least one Telegram chat")
    for item in result:
        try: int(item)
        except ValueError as exc: raise ConnectorError(f"invalid Telegram chat id: {item}") from exc
    return result


class TermiteClient:
    def __init__(self, port, token):
        self.base_url = f"http://127.0.0.1:{port}"
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self.opener = rejecting_opener()
    def request(self, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.base_url + path, data=data, headers=self.headers,
                                         method="GET" if data is None else "POST")
        with self.opener.open(request, timeout=30) as response: return read_json(response, "Termite")
    def register(self, account):
        return self.request("/v1/channels", {"id": CHANNEL_ID, "name": "Telegram", "service": "Telegram",
            "account": account, "description": "Reviewed work from allowlisted Telegram chats",
            "replyCapabilities": ["reply"]})
    def ingest(self, item): return self.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)
    def acknowledge(self, reply_id, delivered, error=None):
        body = {"delivered": delivered}
        if error: body["error"] = utf8_prefix(error, 1024)
        return self.request(f"/v1/channel-replies/{reply_id}/ack", body)
    def begin_reply_attempt(self, reply_id): return self.request(f"/v1/channel-replies/{reply_id}/attempt", {})
    def verification_needed(self, reply_id, error):
        return self.request(f"/v1/channel-replies/{reply_id}/ack", {
            "state": "verification-needed", "error": utf8_prefix(error, 1024)})
    def report_health(self, status, **fields):
        if status not in HEALTH_STATUSES: raise ConnectorError(f"invalid provider health status: {status}")
        unknown = set(fields) - set(HEALTH_FIELD_BYTES)
        if unknown: raise ConnectorError(f"invalid provider health field: {sorted(unknown)[0]}")
        body = {"status": status}; body.update({key: utf8_prefix(value, HEALTH_FIELD_BYTES[key])
                                                for key, value in fields.items() if value is not None})
        return self.request(f"/v1/channels/{CHANNEL_ID}/health", body)
    def pending_replies(self): return self.request("/v1/channel-replies").get("replies", [])
    def reply_is_queued(self, reply_id):
        return any(str(reply.get("id", "")) == reply_id for reply in self.pending_replies())
    def events(self):
        request = urllib.request.Request(self.base_url + "/v1/events", headers=self.headers)
        with self.opener.open(request, timeout=90) as response:
            while True:
                raw = response.readline(MAX_SSE_LINE_BYTES + 1)
                if not raw: break
                if len(raw) > MAX_SSE_LINE_BYTES: raise ConnectorError("Termite SSE line exceeded the safety bound")
                if raw.startswith(b"data: "): yield json.loads(raw[6:])


class TelegramClient:
    def __init__(self, token, long_poll_seconds=25):
        self.base_url = f"{TELEGRAM_API_HOST}/bot{token}"
        self.long_poll_seconds = int(long_poll_seconds)
        self.opener = rejecting_opener()
    def call(self, method, body=None):
        data = json.dumps(body or {}).encode()
        request = urllib.request.Request(f"{self.base_url}/{method}", data=data,
                                         headers={"Content-Type": "application/json"}, method="POST")
        try:
            with self.opener.open(request, timeout=self.long_poll_seconds + 10) as response:
                result = read_json(response, "Telegram")
        except urllib.error.HTTPError as exc: raise ConnectorError(f"Telegram HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            error = f"Telegram request failed: {type(exc).__name__}"
            if method == "sendMessage": raise UncertainDeliveryError(error) from exc
            raise ConnectorError(error) from exc
        if not result.get("ok"):
            raise ConnectorError(f"Telegram API rejected request: {result.get('description', 'unknown error')}")
        return result.get("result")
    def identity(self):
        user = self.call("getMe")
        return str(user.get("id", "")), user.get("username") or user.get("first_name") or "Telegram bot"
    def updates(self, offset=None):
        body = {"timeout": self.long_poll_seconds, "limit": 100,
                "allowed_updates": ["message"]}
        if offset is not None: body["offset"] = offset
        return self.call("getUpdates", body)
    def send(self, reply):
        body = {"chat_id": reply["conversationID"], "text": reply["body"],
                "disable_notification": False}
        if reply.get("replyToID"):
            body["reply_parameters"] = {"message_id": int(reply["replyToID"]), "allow_sending_without_reply": True}
        self.call("sendMessage", body)


class TelegramConnector:
    def __init__(self, termite, telegram, allowed_chat_ids, account=""):
        self.termite, self.telegram = termite, telegram
        self.allowed_chat_ids, self.account = allowed_chat_ids, account
        self.offset, self.own_user_id = None, ""
        self._delivering, self._lock = set(), threading.Lock()
    def _health(self, status, **fields):
        try: self.termite.report_health(status, **fields)
        except Exception as exc: print(f"Telegram health report failed: {exc}", file=sys.stderr)
    def poll_once(self):
        try: updates = self.telegram.updates(self.offset)
        except Exception as exc:
            self._health("retrying", error=str(exc), lastErrorAt=iso_now(), detail="Telegram provider poll failed")
            raise
        for update in sorted(updates, key=lambda item: int(item.get("update_id", -1))):
            update_id = int(update.get("update_id", -1))
            if update_id < 0: continue
            message = update.get("message") or {}
            chat, sender = message.get("chat") or {}, message.get("from") or {}
            chat_id = str(chat.get("id", ""))
            if chat_id not in self.allowed_chat_ids or str(sender.get("id", "")) == self.own_user_id or sender.get("is_bot"):
                self.offset = max(self.offset or 0, update_id + 1)
                continue
            text = message.get("text") or message.get("caption")
            if not text:
                self.offset = max(self.offset or 0, update_id + 1)
                continue
            name = " ".join(filter(None, [sender.get("first_name"), sender.get("last_name")])).strip()
            name = name or sender.get("username") or str(sender.get("id", "Telegram user"))
            title = chat.get("title") or chat.get("username") or name
            self.termite.ingest({"id": f"telegram-update-{update_id}", "deliveryID": f"telegram:{update_id}",
                "conversationID": chat_id, "replyToID": str(message.get("message_id", "")),
                "senderID": str(sender.get("id", "")), "senderName": truncate(name, 256),
                "title": truncate(f"Telegram · {title} · {name}", 512), "body": truncate(text),
                "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(message.get("date", 0))))})
            self.offset = max(self.offset or 0, update_id + 1)
        self._health("healthy", lastSuccessAt=iso_now(), detail="Telegram poll completed")
    def deliver(self, reply):
        reply_id = str(reply.get("id", ""))
        if not reply_id: return
        with self._lock:
            if reply_id in self._delivering: return
            self._delivering.add(reply_id)
        try:
            try:
                if not self.termite.reply_is_queued(reply_id): return
            except Exception:
                return
            if str(reply.get("conversationID", "")) not in self.allowed_chat_ids:
                self.termite.acknowledge(reply_id, False, "Telegram reply targeted a chat outside this connector's allowlist")
                return
            try: self.termite.begin_reply_attempt(reply_id)
            except Exception: return
            try: self.telegram.send(reply)
            except UncertainDeliveryError as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="Telegram delivery needs verification")
                self.termite.verification_needed(reply_id, str(exc))
            except Exception as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="Telegram delivery failed")
                self.termite.acknowledge(reply_id, False, str(exc))
            else:
                self._health("healthy", lastSuccessAt=iso_now(), detail="Telegram delivery completed")
                self.termite.acknowledge(reply_id, True)
        finally:
            with self._lock: self._delivering.discard(reply_id)
    def listen(self):
        delay = 1
        while True:
            try:
                for event in self.termite.events():
                    delay = 1
                    if event.get("kind") == "channel-reply": self.deliver(event)
            except Exception as exc:
                print(f"Termite event stream disconnected: {exc}; retrying", file=sys.stderr)
                time.sleep(delay); delay = min(delay * 2, 30)
    def run(self):
        try: self.own_user_id, detected = self.telegram.identity()
        except Exception as exc:
            self._health("offline", error=str(exc), lastErrorAt=iso_now(), detail="Telegram identity failed")
            raise
        registration = self.termite.register(self.account or detected)
        self._health("healthy", lastSuccessAt=iso_now(), detail="Telegram identity verified")
        for reply in registration.get("pendingReplies", []): self.deliver(reply)
        threading.Thread(target=self.listen, name="termite-events", daemon=True).start()
        delay = 1
        while True:
            try:
                for reply in self.termite.pending_replies(): self.deliver(reply)
                self.poll_once(); delay = 1
            except Exception as exc:
                print(f"Telegram poll failed: {exc}; retrying", file=sys.stderr)
                time.sleep(delay); delay = min(delay * 2, 60)


def build_connector(config_path=None):
    config = load_file_config(config_path)
    token = setting(config, "botToken", "TERMITE_TELEGRAM_BOT_TOKEN") or keychain_secret()
    if not token: raise ConnectorError("Telegram token missing; use Keychain service termite.telegram, config.json, or TERMITE_TELEGRAM_BOT_TOKEN")
    allowed = parse_allowed(setting(config, "allowedChatIds", "TERMITE_TELEGRAM_ALLOWED_CHAT_IDS"))
    try: long_poll = int(setting(config, "longPollSeconds", "TERMITE_TELEGRAM_LONG_POLL_SECONDS", 25))
    except (TypeError, ValueError) as exc: raise ConnectorError("longPollSeconds must be an integer") from exc
    if not 5 <= long_poll <= 50: raise ConnectorError("longPollSeconds must be between 5 and 50")
    port, termite_token = os.environ.get("TERMITE_PORT"), os.environ.get("TERMITE_TOKEN")
    if not port or not termite_token: raise ConnectorError("Termite did not provide TERMITE_PORT and TERMITE_TOKEN")
    return TelegramConnector(TermiteClient(port, termite_token), TelegramClient(str(token), long_poll), allowed,
                             str(setting(config, "account", "TERMITE_TELEGRAM_ACCOUNT", "")))


if __name__ == "__main__":
    try: build_connector().run()
    except ConnectorError as exc:
        print(f"Telegram Channel: {exc}", file=sys.stderr); raise SystemExit(2)
