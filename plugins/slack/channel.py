#!/usr/bin/env python3
"""Slack REST polling connector for Termite Channels (standard library only)."""

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
import urllib.parse
import urllib.request
import uuid


EXTENSION_ID = "dev.termite.slack"
CHANNEL_ID = EXTENSION_ID
KEYCHAIN_SERVICE = "termite.slack"
SLACK_API = "https://slack.com/api"
MAX_HTTP_BYTES = 8 * 1024 * 1024
MAX_SSE_LINE_BYTES = 256 * 1024
HEALTH_STATUSES = {"healthy", "degraded", "retrying", "offline"}
HEALTH_FIELD_BYTES = {"lastSuccessAt": 128, "lastErrorAt": 128, "error": 1024,
                      "nextRetryAt": 128, "detail": 2048}


class ConnectorError(RuntimeError):
    pass


class UncertainDeliveryError(ConnectorError):
    pass


def iso_now(): return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def rejecting_opener(): return urllib.request.build_opener(RejectRedirects())


def read_json(response, source):
    payload = response.read(MAX_HTTP_BYTES + 1)
    if len(payload) > MAX_HTTP_BYTES:
        raise ConnectorError(f"{source} response exceeded {MAX_HTTP_BYTES} bytes")
    try:
        return json.loads(payload) if payload else {}
    except json.JSONDecodeError as exc:
        raise ConnectorError(f"{source} returned invalid JSON") from exc


def utf8_prefix(value, max_bytes):
    return str(value).encode("utf-8")[:max_bytes].decode("utf-8", "ignore")


def truncate(text, limit=60000):
    encoded = str(text).encode("utf-8")
    if len(encoded) <= limit:
        return str(text)
    return encoded[:limit].decode("utf-8", "ignore") + "\n[truncated by connector]"


def load_file_config(path: Path | None = None) -> dict:
    path = path or Path(__file__).with_name("config.json")
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConnectorError(f"cannot read {path.name}: {exc}") from exc
    if not isinstance(value, dict):
        raise ConnectorError(f"{path.name} must contain a JSON object")
    if value.get("botToken") and stat.S_IMODE(path.stat().st_mode) & 0o077:
        print(f"warning: chmod 600 {path} because it contains a token", file=sys.stderr)
    return value


def keychain_secret(service: str = KEYCHAIN_SERVICE) -> str | None:
    security = Path("/usr/bin/security")
    if not security.exists():
        return None
    try:
        result = subprocess.run(
            [str(security), "find-generic-password", "-s", service, "-w"],
            check=False, capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def setting(config: dict, key: str, env: str, default=None):
    return os.environ.get(env, config.get(key, default))


def bounded_number(value, name: str, minimum: float, maximum: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConnectorError(f"{name} must be a number") from exc
    if not minimum <= number <= maximum:
        raise ConnectorError(f"{name} must be between {minimum:g} and {maximum:g}")
    return number


class TermiteClient:
    def __init__(self, port: str, token: str):
        self.base_url = f"http://127.0.0.1:{port}"
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self.opener = rejecting_opener()

    def request(self, path: str, body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        method = "GET" if body is None else "POST"
        request = urllib.request.Request(self.base_url + path, data=data, headers=self.headers, method=method)
        with self.opener.open(request, timeout=30) as response:
            return read_json(response, "Termite")

    def register(self, account: str):
        return self.request("/v1/channels", {
            "id": CHANNEL_ID, "name": "Slack", "service": "Slack", "account": account,
            "description": "Reviewed work from one selected Slack channel",
            "replyCapabilities": ["reply"],
        })

    def ingest(self, item: dict):
        return self.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)

    def acknowledge(self, reply_id: str, delivered: bool, error: str | None = None):
        body = {"delivered": delivered}
        if error:
            body["error"] = utf8_prefix(error, 1024)
        return self.request(f"/v1/channel-replies/{reply_id}/ack", body)

    def begin_reply_attempt(self, reply_id: str):
        return self.request(f"/v1/channel-replies/{reply_id}/attempt", {})

    def verification_needed(self, reply_id: str, error: str):
        return self.request(f"/v1/channel-replies/{reply_id}/ack", {
            "state": "verification-needed", "error": utf8_prefix(error, 1024),
        })

    def report_health(self, status: str, **fields):
        if status not in HEALTH_STATUSES:
            raise ConnectorError(f"invalid provider health status: {status}")
        unknown = set(fields) - set(HEALTH_FIELD_BYTES)
        if unknown:
            raise ConnectorError(f"invalid provider health field: {sorted(unknown)[0]}")
        body = {"status": status}
        body.update({key: utf8_prefix(value, HEALTH_FIELD_BYTES[key])
                     for key, value in fields.items() if value is not None})
        return self.request(f"/v1/channels/{CHANNEL_ID}/health", body)

    def pending_replies(self):
        return self.request("/v1/channel-replies").get("replies", [])

    def reply_is_queued(self, reply_id: str) -> bool:
        return any(str(reply.get("id", "")) == reply_id for reply in self.pending_replies())

    def events(self):
        request = urllib.request.Request(self.base_url + "/v1/events", headers=self.headers)
        with self.opener.open(request, timeout=90) as response:
            while True:
                raw = response.readline(MAX_SSE_LINE_BYTES + 1)
                if not raw: break
                if len(raw) > MAX_SSE_LINE_BYTES: raise ConnectorError("Termite SSE line exceeded the safety bound")
                if raw.startswith(b"data: "):
                    yield json.loads(raw[6:])


class SlackClient:
    def __init__(self, token: str, timeout: float = 20):
        self.token = token
        self.timeout = timeout
        self._names: dict[str, str] = {}
        self.opener = rejecting_opener()

    def call(self, method: str, body: dict | None = None, query: dict | None = None):
        url = f"{SLACK_API}/{method}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        data = None if body is None else json.dumps(body).encode("utf-8")
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json; charset=utf-8"}
        request = urllib.request.Request(url, data=data, headers=headers, method="GET" if data is None else "POST")
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                result = read_json(response, "Slack")
        except urllib.error.HTTPError as exc:
            raise ConnectorError(f"Slack HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            error = f"Slack request failed: {type(exc).__name__}"
            if method == "chat.postMessage": raise UncertainDeliveryError(error) from exc
            raise ConnectorError(error) from exc
        if not result.get("ok"):
            raise ConnectorError(f"Slack API rejected request: {result.get('error', 'unknown_error')}")
        return result

    def identity(self) -> tuple[str, str]:
        result = self.call("auth.test")
        return str(result.get("user_id", "")), str(result.get("team", "Slack workspace"))

    def history(self, channel_id: str, oldest: str, max_pages: int = 4) -> list[dict]:
        messages: list[dict] = []
        cursor = ""
        for _ in range(max_pages):
            query = {"channel": channel_id, "oldest": oldest, "inclusive": "false", "limit": 100}
            if cursor:
                query["cursor"] = cursor
            result = self.call("conversations.history", query=query)
            messages.extend(result.get("messages", []))
            cursor = result.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break
        if cursor:
            raise ConnectorError("Slack poll exceeded 400 messages; reduce lookback or poll more often")
        return sorted(messages, key=lambda message: float(message.get("ts", 0)))

    def user_name(self, user_id: str) -> str:
        if not user_id:
            return "Slack user"
        if user_id not in self._names:
            result = self.call("users.info", query={"user": user_id})
            user = result.get("user", {})
            profile = user.get("profile", {})
            self._names[user_id] = profile.get("display_name") or profile.get("real_name") or user.get("name") or user_id
        return self._names[user_id]

    def send(self, reply: dict):
        client_message_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "termite:" + str(reply["id"])))
        body = {"channel": reply["conversationID"], "text": reply["body"], "client_msg_id": client_message_id}
        if reply.get("replyToID"):
            body["thread_ts"] = reply["replyToID"]
        self.call("chat.postMessage", body=body)


class SlackConnector:
    def __init__(self, termite: TermiteClient, slack: SlackClient, channel_id: str,
                 account: str, poll_seconds: float, initial_lookback: float):
        self.termite, self.slack, self.channel_id, self.account = termite, slack, channel_id, account
        self.poll_seconds = poll_seconds
        self.oldest = f"{time.time() - initial_lookback:.6f}"
        self.own_user_id = ""
        self._delivering: set[str] = set()
        self._delivery_lock = threading.Lock()

    def _health(self, status, **fields):
        try: self.termite.report_health(status, **fields)
        except Exception as exc: print(f"Slack health report failed: {exc}", file=sys.stderr)

    def poll_once(self):
        try: messages = self.slack.history(self.channel_id, self.oldest)
        except Exception as exc:
            self._health("retrying", error=str(exc), lastErrorAt=iso_now(), detail="Slack provider poll failed")
            raise
        for message in messages:
            ts = str(message.get("ts", ""))
            if not ts:
                continue
            if message.get("user") == self.own_user_id or message.get("bot_id") or message.get("subtype"):
                self.oldest = max(self.oldest, ts, key=float)
                continue
            text = str(message.get("text", "")).strip()
            if not text:
                self.oldest = max(self.oldest, ts, key=float)
                continue
            user_id = str(message.get("user", ""))
            try:
                sender = self.slack.user_name(user_id)
            except ConnectorError:
                sender = user_id or "Slack user"
            self.termite.ingest({
                "id": "slack-" + ts.replace(".", "-"),
                "deliveryID": f"slack:{self.channel_id}:{ts}",
                "conversationID": self.channel_id,
                "replyToID": str(message.get("thread_ts") or ts),
                "senderID": user_id,
                "senderName": truncate(sender, 256),
                "title": truncate(f"Slack message from {sender}", 512),
                "body": truncate(text),
                "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(ts))),
            })
            self.oldest = max(self.oldest, ts, key=float)
        self._health("healthy", lastSuccessAt=iso_now(), detail="Slack poll completed")

    def deliver(self, reply: dict):
        reply_id = str(reply.get("id", ""))
        if not reply_id:
            return
        with self._delivery_lock:
            if reply_id in self._delivering:
                return
            self._delivering.add(reply_id)
        try:
            try:
                if not self.termite.reply_is_queued(reply_id):
                    return
            except Exception:
                return  # The queued-reply poll will retry after a transient local API failure.
            if str(reply.get("conversationID", "")) != self.channel_id:
                self.termite.acknowledge(reply_id, False, "Slack reply targeted a channel outside this connector's configuration")
                return
            try: self.termite.begin_reply_attempt(reply_id)
            except Exception: return
            try:
                self.slack.send(reply)
            except UncertainDeliveryError as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(),
                             detail="Slack delivery needs verification")
                self.termite.verification_needed(reply_id, str(exc))
            except Exception as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="Slack delivery failed")
                self.termite.acknowledge(reply_id, False, str(exc))
            else:
                self._health("healthy", lastSuccessAt=iso_now(), detail="Slack delivery completed")
                self.termite.acknowledge(reply_id, True)
        finally:
            with self._delivery_lock:
                self._delivering.discard(reply_id)

    def listen(self):
        delay = 1.0
        while True:
            try:
                for event in self.termite.events():
                    delay = 1.0
                    if event.get("kind") == "channel-reply":
                        self.deliver(event)
            except Exception as exc:
                print(f"Termite event stream disconnected: {exc}; retrying", file=sys.stderr)
                time.sleep(delay)
                delay = min(delay * 2, 30)

    def run(self):
        try: self.own_user_id, detected_account = self.slack.identity()
        except Exception as exc:
            self._health("offline", error=str(exc), lastErrorAt=iso_now(), detail="Slack identity failed")
            raise
        if not self.account:
            self.account = detected_account
        registration = self.termite.register(self.account)
        self._health("healthy", lastSuccessAt=iso_now(), detail="Slack identity verified")
        for reply in registration.get("pendingReplies", []):
            self.deliver(reply)
        threading.Thread(target=self.listen, name="termite-events", daemon=True).start()
        delay = 1.0
        while True:
            try:
                for reply in self.termite.pending_replies():
                    self.deliver(reply)
                self.poll_once()
                delay = 1.0
                time.sleep(self.poll_seconds)
            except Exception as exc:
                print(f"Slack poll failed: {exc}; retrying", file=sys.stderr)
                time.sleep(delay)
                delay = min(delay * 2, 60)


def build_connector(config_path: Path | None = None) -> SlackConnector:
    config = load_file_config(config_path)
    token = setting(config, "botToken", "TERMITE_SLACK_BOT_TOKEN") or keychain_secret()
    channel_id = setting(config, "channelId", "TERMITE_SLACK_CHANNEL_ID")
    if not token:
        raise ConnectorError("Slack token missing; use Keychain service termite.slack, config.json, or TERMITE_SLACK_BOT_TOKEN")
    if not channel_id:
        raise ConnectorError("Slack channelId missing in config.json or TERMITE_SLACK_CHANNEL_ID")
    poll = bounded_number(setting(config, "pollSeconds", "TERMITE_SLACK_POLL_SECONDS", 5), "pollSeconds", 2, 300)
    lookback = bounded_number(setting(config, "initialLookbackSeconds", "TERMITE_SLACK_INITIAL_LOOKBACK_SECONDS", 300), "initialLookbackSeconds", 0, 86400)
    port, termite_token = os.environ.get("TERMITE_PORT"), os.environ.get("TERMITE_TOKEN")
    if not port or not termite_token:
        raise ConnectorError("Termite did not provide TERMITE_PORT and TERMITE_TOKEN")
    return SlackConnector(TermiteClient(port, termite_token), SlackClient(str(token)), str(channel_id),
                          str(setting(config, "account", "TERMITE_SLACK_ACCOUNT", "")), poll, lookback)


if __name__ == "__main__":
    try:
        build_connector().run()
    except ConnectorError as exc:
        print(f"Slack Channel: {exc}", file=sys.stderr)
        raise SystemExit(2)
