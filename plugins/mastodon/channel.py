#!/usr/bin/env python3
"""Mastodon mention/reply connector for Termite Channels (stdlib only)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html import unescape
from html.parser import HTMLParser
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from typing import Any
import urllib.error
import urllib.parse
import urllib.request


PLUGIN_DIR = Path(__file__).resolve().parent
CHANNEL_ID = "dev.termite.mastodon.mentions"
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_last_success_at: str | None = None
_last_error_at: str | None = None


def bounded(value: Any, limit: int) -> str:
    return str(value).encode("utf-8")[:limit].decode("utf-8", "ignore")


def delivery_uncertain(exc: Exception) -> bool:
    return not isinstance(exc, urllib.error.HTTPError) and isinstance(
        exc, (urllib.error.URLError, TimeoutError, OSError)
    )


def report_health(client: Any, status: str, *, error: str = "",
                  retry_in: int | None = None, detail: str = "") -> None:
    global _last_success_at, _last_error_at
    now = datetime.now(timezone.utc)
    if status == "healthy":
        _last_success_at = now.isoformat().replace("+00:00", "Z")
    if error:
        _last_error_at = now.isoformat().replace("+00:00", "Z")
    body: dict[str, Any] = {"status": status}
    if _last_success_at:
        body["lastSuccessAt"] = _last_success_at
    if _last_error_at:
        body["lastErrorAt"] = _last_error_at
    if error:
        body["error"] = bounded(error, 1024)
    if retry_in is not None:
        body["nextRetryAt"] = (now + timedelta(seconds=max(0, retry_in))).isoformat().replace("+00:00", "Z")
    if detail:
        body["detail"] = bounded(detail, 2048)
    try:
        client.request(f"/v1/channels/{CHANNEL_ID}/health", body)
    except Exception as exc:
        print(f"health update failed: {type(exc).__name__}", file=sys.stderr, flush=True)


def origin(url: str) -> tuple[str, str, int | None]:
    parsed = urllib.parse.urlsplit(url)
    return parsed.scheme.lower(), (parsed.hostname or "").lower(), parsed.port


class SameOriginRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if req.get_method() not in {"GET", "HEAD"} or origin(req.full_url) != origin(newurl):
            raise urllib.error.HTTPError(newurl, code, "unsafe provider redirect", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


HTTP = urllib.request.build_opener(SameOriginRedirect())


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def plain_text(html: str) -> str:
    parser = TextExtractor()
    try:
        parser.feed(html)
        return " ".join(" ".join(parser.parts).split())
    except Exception:
        return " ".join(unescape(html).split())


def keychain(service: str) -> str:
    if not Path("/usr/bin/security").exists():
        return ""
    try:
        return subprocess.run(
            ["/usr/bin/security", "find-generic-password", "-s", service, "-w"],
            check=True, capture_output=True, text=True, timeout=5
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def load_config() -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "base_url": "", "access_token": "", "poll_seconds": 60,
        "max_notifications": 40, "reply_visibility": "unlisted",
        "max_reply_characters": 500, "channel_name": "Mastodon Mentions",
        "account": "Mastodon", "allow_insecure_local": False, "state_file": "",
    }
    path = PLUGIN_DIR / "config.json"
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("config.json must contain a JSON object")
        cfg.update(loaded)
    env = {
        "base_url": "MASTODON_BASE_URL", "access_token": "MASTODON_ACCESS_TOKEN",
        "poll_seconds": "MASTODON_POLL_SECONDS", "max_notifications": "MASTODON_MAX_NOTIFICATIONS",
        "reply_visibility": "MASTODON_REPLY_VISIBILITY",
        "max_reply_characters": "MASTODON_MAX_REPLY_CHARACTERS",
        "channel_name": "MASTODON_CHANNEL_NAME", "account": "MASTODON_ACCOUNT",
        "allow_insecure_local": "MASTODON_ALLOW_INSECURE_LOCAL", "state_file": "MASTODON_STATE_FILE",
    }
    for key, name in env.items():
        if name in os.environ:
            cfg[key] = os.environ[name]
    cfg["base_url"] = str(cfg["base_url"]).rstrip("/")
    cfg["allow_insecure_local"] = str(cfg["allow_insecure_local"]).lower() in {"1", "true", "yes"}
    parsed = urllib.parse.urlparse(cfg["base_url"])
    if not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("base_url must have a host and must not contain credentials")
    local = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    if parsed.scheme != "https" and not (parsed.scheme == "http" and local and cfg["allow_insecure_local"]):
        raise ValueError("base_url must use HTTPS; loopback HTTP requires allow_insecure_local")
    if not cfg["access_token"]:
        cfg["access_token"] = keychain("termite.mastodon")
    if not cfg["access_token"]:
        raise ValueError("Set access_token/MASTODON_ACCESS_TOKEN or Keychain service termite.mastodon")
    cfg["poll_seconds"] = min(3600, max(30, int(cfg["poll_seconds"])))
    cfg["max_notifications"] = min(80, max(1, int(cfg["max_notifications"])))
    cfg["max_reply_characters"] = min(5000, max(1, int(cfg["max_reply_characters"])))
    if cfg["reply_visibility"] not in {"public", "unlisted", "private", "direct"}:
        raise ValueError("reply_visibility must be public, unlisted, private, or direct")
    if not cfg["state_file"]:
        root = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
        cfg["state_file"] = str(root / "termite" / "mastodon.json")
    return cfg


class TermiteClient:
    def __init__(self) -> None:
        self.base = f"http://127.0.0.1:{os.environ['TERMITE_PORT']}"
        self.headers = {"Authorization": f"Bearer {os.environ['TERMITE_TOKEN']}",
                        "Content-Type": "application/json"}

    def request(self, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.base + path, data=data, headers=self.headers,
                                         method="GET" if data is None else "POST")
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
        return json.loads(raw) if raw else {}

    def events(self):
        request = urllib.request.Request(self.base + "/v1/events", headers=self.headers)
        with urllib.request.urlopen(request, timeout=90) as response:
            for raw in response:
                if raw.startswith(b"data: "):
                    yield json.loads(raw[6:])


class MastodonAPI:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.headers = {"Authorization": f"Bearer {cfg['access_token']}",
                        "User-Agent": "Termite-Mastodon-Channel/1.0"}

    def notification_page(self, since_id: str | None, max_id: str | None = None) -> list[dict[str, Any]]:
        query: list[tuple[str, str]] = [("types[]", "mention"), ("limit", str(self.cfg["max_notifications"]))]
        if since_id:
            query.append(("since_id", since_id))
        if max_id:
            query.append(("max_id", max_id))
        url = self.cfg["base_url"] + "/api/v1/notifications?" + urllib.parse.urlencode(query)
        with HTTP.open(urllib.request.Request(url, headers=self.headers), timeout=30) as response:
            data = response.read(MAX_RESPONSE_BYTES + 1)
        if len(data) > MAX_RESPONSE_BYTES:
            raise ValueError("Mastodon response exceeds 2 MiB")
        value = json.loads(data)
        if not isinstance(value, list):
            raise ValueError("Mastodon notifications response is not a list")
        return value

    def notification_pages(self, since_id: str | None):
        """Page to the old cursor; each provider response remains byte-bounded."""
        max_id: str | None = None
        cursors: set[str] = set()
        while True:
            page = self.notification_page(since_id, max_id)
            if not page:
                return
            yield page
            if len(page) < self.cfg["max_notifications"]:
                return
            cursor = str(page[-1].get("id") or "") if isinstance(page[-1], dict) else ""
            if not cursor or cursor in cursors:
                raise ValueError("Mastodon pagination cursor did not advance")
            cursors.add(cursor)
            max_id = cursor

    def own_account_id(self) -> str:
        url = self.cfg["base_url"] + "/api/v1/accounts/verify_credentials"
        with HTTP.open(urllib.request.Request(url, headers=self.headers), timeout=30) as response:
            data = response.read(65537)
        if len(data) > 65536:
            raise ValueError("Mastodon credentials response exceeds 64 KiB")
        value = json.loads(data)
        account_id = str(value.get("id") or "") if isinstance(value, dict) else ""
        if not account_id:
            raise ValueError("Mastodon credentials response lacks account id")
        return account_id

    def reply(self, reply: dict[str, Any]) -> None:
        if len(reply["body"]) > self.cfg["max_reply_characters"]:
            raise ValueError(f"reply exceeds configured {self.cfg['max_reply_characters']} character limit")
        status_id = str(reply.get("replyToID") or reply["conversationID"])
        data = urllib.parse.urlencode({
            "status": reply["body"], "in_reply_to_id": status_id,
            "visibility": self.cfg["reply_visibility"],
        }).encode()
        headers = dict(self.headers)
        headers.update({"Content-Type": "application/x-www-form-urlencoded",
                        "Idempotency-Key": reply["id"]})
        request = urllib.request.Request(self.cfg["base_url"] + "/api/v1/statuses",
                                         data=data, headers=headers, method="POST")
        with HTTP.open(request, timeout=30) as response:
            # A 2xx response establishes acceptance; its body is not required.
            response.read(MAX_RESPONSE_BYTES + 1)


def work_item(notification: dict[str, Any], own_account_id: str = "",
              base_url: str = "") -> dict[str, Any] | None:
    notification_id = str(notification.get("id", "")).strip()
    status = notification.get("status") or {}
    account = notification.get("account") or status.get("account") or {}
    status_id = str(status.get("id", "")).strip()
    if not notification_id or not status_id:
        raise ValueError("mention notification lacks immutable notification/status id")
    if own_account_id and str(account.get("id") or "") == own_account_id:
        return None
    scope = hashlib.sha256(base_url.encode()).hexdigest()[:16] if base_url else ""
    delivery_prefix = f"mastodon:{scope}:notification" if scope else "mastodon-notification"
    item_prefix = f"mastodon-{scope}" if scope else "mastodon"
    handle = str(account.get("acct") or account.get("username") or "unknown")
    name = plain_text(str(account.get("display_name") or "")) or f"@{handle}"
    body = plain_text(str(status.get("content") or ""))
    url = str(status.get("url") or status.get("uri") or "")
    if url:
        body = (body + "\n\n" if body else "") + url
    return {
        "id": bounded(f"{item_prefix}-{notification_id}", 256),
        "deliveryID": bounded(f"{delivery_prefix}:{notification_id}", 512),
        "conversationID": bounded(status_id, 512), "replyToID": bounded(status_id, 512),
        "senderID": bounded(account.get("id") or handle, 256), "senderName": bounded(name, 256),
        "title": bounded(f"Mention from @{handle}", 512),
        "body": body.encode("utf-8")[:65536].decode("utf-8", "ignore") or "(Empty mention)",
        **({"createdAt": bounded(notification["created_at"], 128)} if notification.get("created_at") else {}),
    }


def load_since_id(path: str) -> str | None:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        return str(value.get("since_id")) if value.get("since_id") else None
    except (OSError, ValueError, TypeError):
        return None


def save_since_id(path: str, since_id: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps({"since_id": since_id}) + "\n", encoding="utf-8")
    temporary.replace(target)


def deliver(client: TermiteClient, api: MastodonAPI, reply: dict[str, Any]) -> None:
    client.request(f"/v1/channel-replies/{reply['id']}/attempt", {})
    try:
        api.reply(reply)
    except Exception as exc:
        message = bounded(f"Mastodon delivery failed: {exc}", 512)
        report_health(client, "degraded", error=message, detail="Mastodon reply failed")
        if delivery_uncertain(exc):
            client.request(f"/v1/channel-replies/{reply['id']}/ack", {
                "state": "verification-needed", "error": message
            })
        else:
            client.request(f"/v1/channel-replies/{reply['id']}/ack", {
                "delivered": False, "error": message
            })
        return
    report_health(client, "healthy", detail="Mastodon reply accepted")
    client.request(f"/v1/channel-replies/{reply['id']}/ack", {"delivered": True})


def recover_pending(client: TermiteClient, api: MastodonAPI) -> None:
    for reply in client.request("/v1/channel-replies").get("replies", []):
        if reply.get("channel") == CHANNEL_ID:
            deliver(client, api, reply)


def reply_loop(client: TermiteClient, api: MastodonAPI) -> None:
    delay = 1
    while True:
        try:
            recover_pending(client, api)
            for event in client.events():
                delay = 1
                if event.get("kind") == "channel-reply" and event.get("channel") == CHANNEL_ID:
                    deliver(client, api, event)
        except Exception as exc:
            print(f"reply stream disconnected: {exc}", file=sys.stderr, flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def largest_id(ids: list[str]) -> str:
    return max(ids, key=lambda value: (len(value), value))


def main() -> None:
    cfg = load_config()
    client = TermiteClient()
    api = MastodonAPI(cfg)
    own_account_id = api.own_account_id()
    registration = client.request("/v1/channels", {
        "id": CHANNEL_ID, "name": cfg["channel_name"], "service": "Mastodon",
        "account": cfg["account"], "description": "Mentions in; approved replies out",
        "replyCapabilities": ["reply"],
    })
    report_health(client, "healthy", detail="Mastodon credentials verified")
    for pending in registration.get("pendingReplies", []):
        deliver(client, api, pending)
    threading.Thread(target=reply_loop, args=(client, api), daemon=True).start()
    since_id = load_since_id(cfg["state_file"])
    failures = 0
    delay = 0
    while True:
        if delay:
            time.sleep(delay)
        try:
            ingested: list[str] = []
            for notifications in api.notification_pages(since_id):
                for notification in reversed(notifications):
                    item = work_item(notification, own_account_id, cfg["base_url"])
                    if item is not None:
                        client.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)
                    ingested.append(str(notification["id"]))
            if ingested:
                since_id = largest_id(ingested)
                save_since_id(cfg["state_file"], since_id)
            failures = 0
            delay = cfg["poll_seconds"]
            report_health(client, "healthy", detail="Mastodon notifications poll succeeded")
        except Exception as exc:
            failures += 1
            delay = min(cfg["poll_seconds"] * (2 ** min(failures, 5)), 3600)
            report_health(client, "retrying", error=f"Mastodon poll failed: {exc}",
                          retry_in=delay, detail="Mastodon notifications poll will retry")
            print(f"Mastodon poll failed: {exc}; retrying in {delay}s", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
