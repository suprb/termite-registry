#!/usr/bin/env python3
"""Discord REST polling connector for Termite Channels (no gateway dependency)."""

from __future__ import annotations

import hashlib
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


EXTENSION_ID = "dev.termite.discord"
CHANNEL_ID = EXTENSION_ID
KEYCHAIN_SERVICE = "termite.discord"
DISCORD_API = "https://discord.com/api/v10"
MAX_HTTP_BYTES = 8 * 1024 * 1024
MAX_SSE_LINE_BYTES = 256 * 1024
HEALTH_STATUSES = {"healthy", "degraded", "retrying", "offline"}
HEALTH_FIELD_BYTES = {"lastSuccessAt": 128, "lastErrorAt": 128, "error": 1024,
                      "nextRetryAt": 128, "detail": 2048}


class ConnectorError(RuntimeError): pass
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
    return str(text) if len(encoded) <= limit else encoded[:limit].decode("utf-8", "ignore") + "\n[truncated by connector]"


def load_file_config(path=None):
    path = path or Path(__file__).with_name("config.json")
    if not path.exists(): return {}
    try: value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc: raise ConnectorError(f"cannot read {path.name}: {exc}") from exc
    if not isinstance(value, dict): raise ConnectorError(f"{path.name} must contain a JSON object")
    if value.get("botToken") and stat.S_IMODE(path.stat().st_mode) & 0o077:
        print(f"warning: chmod 600 {path} because it contains a token", file=sys.stderr)
    return value


def keychain_secret(service=KEYCHAIN_SERVICE):
    security = Path("/usr/bin/security")
    if not security.exists(): return None
    try:
        result = subprocess.run([str(security), "find-generic-password", "-s", service, "-w"],
            check=False, capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError): return None
    return result.stdout.strip() if result.returncode == 0 else None


def setting(config, key, env, default=None): return os.environ.get(env, config.get(key, default))


def parse_channel_ids(value):
    values = value if isinstance(value, list) else str(value or "").split(",")
    result = {str(item).strip() for item in values if str(item).strip()}
    if not result: raise ConnectorError("channelIds must explicitly list at least one Discord channel")
    if any(not item.isdigit() for item in result): raise ConnectorError("Discord channel IDs must be numeric snowflakes")
    return sorted(result, key=int)


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
        return self.request("/v1/channels", {"id": CHANNEL_ID, "name": "Discord", "service": "Discord",
            "account": account, "description": "Reviewed work from allowlisted Discord channels",
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


class DiscordClient:
    def __init__(self, token, timeout=20): self.token, self.timeout, self.opener = token, timeout, rejecting_opener()
    def call(self, method, path, body=None, query=None):
        url = DISCORD_API + path
        if query: url += "?" + urllib.parse.urlencode(query)
        data = None if body is None else json.dumps(body).encode()
        headers = {"Authorization": f"Bot {self.token}", "Content-Type": "application/json",
                   "User-Agent": "TermiteChannel/1.0"}
        for attempt in range(3):
            request = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with self.opener.open(request, timeout=self.timeout) as response:
                    return read_json(response, "Discord")
            except urllib.error.HTTPError as exc:
                if exc.code == 429 and attempt < 2:
                    try: wait = float(read_json(exc, "Discord rate limit").get("retry_after", 1))
                    except Exception: wait = 1
                    time.sleep(min(max(wait, 0.25), 30)); continue
                raise ConnectorError(f"Discord HTTP {exc.code}") from exc
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                error = f"Discord request failed: {type(exc).__name__}"
                if method == "POST" and path.endswith("/messages"): raise UncertainDeliveryError(error) from exc
                raise ConnectorError(error) from exc
        raise ConnectorError("Discord rate limit retry exhausted")
    def identity(self):
        user = self.call("GET", "/users/@me")
        return str(user.get("id", "")), user.get("global_name") or user.get("username") or "Discord bot"
    def messages(self, channel_id, after=None, initial_limit=25, max_pages=4):
        found, cursor = [], after
        for _ in range(max_pages):
            query = {"limit": 100 if cursor else initial_limit}
            if cursor: query["after"] = cursor
            page = self.call("GET", f"/channels/{channel_id}/messages", query=query)
            if not isinstance(page, list): raise ConnectorError("Discord returned an invalid messages response")
            found.extend(page)
            if len(page) < query["limit"]: break
            newest = max((str(item.get("id", "0")) for item in page), key=int)
            if cursor == newest: break
            cursor = newest
        if page and len(page) == query["limit"]:
            raise ConnectorError("Discord poll exceeded the 400-message bound; reduce initial history or poll more often")
        return sorted(found, key=lambda item: int(item.get("id", 0)))
    def send(self, reply):
        digest = hashlib.sha256(("termite:" + str(reply["id"])).encode()).digest()
        nonce = str(int.from_bytes(digest[:8], "big"))
        body = {"content": reply["body"], "nonce": nonce, "enforce_nonce": True,
                "allowed_mentions": {"parse": [], "replied_user": False}}
        if reply.get("replyToID"):
            body["message_reference"] = {"message_id": reply["replyToID"],
                "channel_id": reply["conversationID"], "fail_if_not_exists": False}
        self.call("POST", f"/channels/{reply['conversationID']}/messages", body=body)


class DiscordConnector:
    def __init__(self, termite, discord, channel_ids, account, poll_seconds, initial_limit):
        self.termite, self.discord, self.channel_ids, self.account = termite, discord, channel_ids, account
        self.poll_seconds, self.initial_limit = poll_seconds, initial_limit
        self.after = {item: None for item in channel_ids}; self.own_user_id = ""
        self._delivering, self._lock = set(), threading.Lock()
    def _health(self, status, **fields):
        try: self.termite.report_health(status, **fields)
        except Exception as exc: print(f"Discord health report failed: {exc}", file=sys.stderr)
    def poll_once(self):
        for channel_id in self.channel_ids:
            try: messages = self.discord.messages(channel_id, self.after[channel_id], self.initial_limit)
            except Exception as exc:
                self._health("retrying", error=str(exc), lastErrorAt=iso_now(), detail="Discord provider poll failed")
                raise
            for message in messages:
                message_id = str(message.get("id", ""))
                if not message_id: continue
                author = message.get("author") or {}
                if str(author.get("id", "")) == self.own_user_id or author.get("bot"):
                    self.after[channel_id] = message_id; continue
                text = str(message.get("content", "")).strip()
                if not text:
                    self.after[channel_id] = message_id; continue
                sender = author.get("global_name") or author.get("username") or str(author.get("id", "Discord user"))
                self.termite.ingest({"id": f"discord-message-{message_id}",
                    "deliveryID": f"discord:{channel_id}:{message_id}", "conversationID": channel_id,
                    "replyToID": message_id, "senderID": str(author.get("id", "")),
                    "senderName": truncate(sender, 256), "title": truncate(f"Discord message from {sender}", 512),
                    "body": truncate(text), "createdAt": message.get("timestamp")})
                self.after[channel_id] = message_id
        self._health("healthy", lastSuccessAt=iso_now(), detail="Discord poll completed")
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
            if str(reply.get("conversationID", "")) not in self.channel_ids:
                self.termite.acknowledge(reply_id, False, "Discord reply targeted a channel outside this connector's allowlist")
                return
            try: self.termite.begin_reply_attempt(reply_id)
            except Exception: return
            try: self.discord.send(reply)
            except UncertainDeliveryError as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="Discord delivery needs verification")
                self.termite.verification_needed(reply_id, str(exc))
            except Exception as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="Discord delivery failed")
                self.termite.acknowledge(reply_id, False, str(exc))
            else:
                self._health("healthy", lastSuccessAt=iso_now(), detail="Discord delivery completed")
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
        try: self.own_user_id, detected = self.discord.identity()
        except Exception as exc:
            self._health("offline", error=str(exc), lastErrorAt=iso_now(), detail="Discord identity failed")
            raise
        registration = self.termite.register(self.account or detected)
        self._health("healthy", lastSuccessAt=iso_now(), detail="Discord identity verified")
        for reply in registration.get("pendingReplies", []): self.deliver(reply)
        threading.Thread(target=self.listen, name="termite-events", daemon=True).start()
        delay = 1
        while True:
            try:
                for reply in self.termite.pending_replies(): self.deliver(reply)
                self.poll_once(); delay = 1; time.sleep(self.poll_seconds)
            except Exception as exc:
                print(f"Discord poll failed: {exc}; retrying", file=sys.stderr)
                time.sleep(delay); delay = min(delay * 2, 60)


def build_connector(config_path=None):
    config = load_file_config(config_path)
    token = setting(config, "botToken", "TERMITE_DISCORD_BOT_TOKEN") or keychain_secret()
    if not token: raise ConnectorError("Discord token missing; use Keychain service termite.discord, config.json, or TERMITE_DISCORD_BOT_TOKEN")
    ids = parse_channel_ids(setting(config, "channelIds", "TERMITE_DISCORD_CHANNEL_IDS"))
    try:
        poll = float(setting(config, "pollSeconds", "TERMITE_DISCORD_POLL_SECONDS", 5))
        limit = int(setting(config, "initialFetchLimit", "TERMITE_DISCORD_INITIAL_FETCH_LIMIT", 25))
    except (TypeError, ValueError) as exc: raise ConnectorError("pollSeconds and initialFetchLimit must be numbers") from exc
    if not 2 <= poll <= 300: raise ConnectorError("pollSeconds must be between 2 and 300")
    if not 1 <= limit <= 100: raise ConnectorError("initialFetchLimit must be between 1 and 100")
    port, termite_token = os.environ.get("TERMITE_PORT"), os.environ.get("TERMITE_TOKEN")
    if not port or not termite_token: raise ConnectorError("Termite did not provide TERMITE_PORT and TERMITE_TOKEN")
    return DiscordConnector(TermiteClient(port, termite_token), DiscordClient(str(token)), ids,
        str(setting(config, "account", "TERMITE_DISCORD_ACCOUNT", "")), poll, limit)


if __name__ == "__main__":
    try: build_connector().run()
    except ConnectorError as exc:
        print(f"Discord Channel: {exc}", file=sys.stderr); raise SystemExit(2)
