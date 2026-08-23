#!/usr/bin/env python3
"""Allowlisted Matrix room connector for Termite Channels."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from typing import Any
import urllib.parse
import urllib.error
import urllib.request


PLUGIN_DIR = Path(__file__).resolve().parent
CHANNEL_ID = "dev.termite.matrix.rooms"
MAX_SYNC_BYTES = 4 * 1024 * 1024
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


def keychain() -> str:
    if not Path("/usr/bin/security").exists():
        return ""
    try:
        return subprocess.run(
            ["/usr/bin/security", "find-generic-password", "-s", "termite.matrix", "-w"],
            check=True, capture_output=True, text=True, timeout=5
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def load_config() -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "homeserver": "", "access_token": "", "room_ids": [], "own_user_id": "",
        "sync_timeout_seconds": 30, "timeline_limit": 25, "channel_name": "Matrix Rooms",
        "account": "Matrix", "allow_insecure_local": False, "state_file": "",
    }
    path = PLUGIN_DIR / "config.json"
    if path.exists():
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("config.json must contain a JSON object")
        cfg.update(value)
    env = {
        "homeserver": "MATRIX_HOMESERVER", "access_token": "MATRIX_ACCESS_TOKEN",
        "room_ids": "MATRIX_ROOM_IDS", "own_user_id": "MATRIX_OWN_USER_ID",
        "sync_timeout_seconds": "MATRIX_SYNC_TIMEOUT_SECONDS", "timeline_limit": "MATRIX_TIMELINE_LIMIT",
        "channel_name": "MATRIX_CHANNEL_NAME", "account": "MATRIX_ACCOUNT",
        "allow_insecure_local": "MATRIX_ALLOW_INSECURE_LOCAL", "state_file": "MATRIX_STATE_FILE",
    }
    for key, name in env.items():
        if name in os.environ:
            cfg[key] = os.environ[name]
    if isinstance(cfg["room_ids"], str):
        cfg["room_ids"] = [item.strip() for item in cfg["room_ids"].replace("\n", ",").split(",") if item.strip()]
    if not isinstance(cfg["room_ids"], list) or not cfg["room_ids"]:
        raise ValueError("room_ids/MATRIX_ROOM_IDS must explicitly allow at least one room")
    if len(cfg["room_ids"]) > 16 or any(not str(room).startswith("!") for room in cfg["room_ids"]):
        raise ValueError("room_ids must contain 1–16 Matrix room IDs")
    if not str(cfg["own_user_id"]).startswith("@") or ":" not in str(cfg["own_user_id"]):
        raise ValueError("own_user_id/MATRIX_OWN_USER_ID is required and must be a Matrix user ID")
    cfg["homeserver"] = str(cfg["homeserver"]).rstrip("/")
    cfg["allow_insecure_local"] = str(cfg["allow_insecure_local"]).lower() in {"1", "true", "yes"}
    parsed = urllib.parse.urlparse(cfg["homeserver"])
    if not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("homeserver must have a host and must not contain credentials")
    local = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    if parsed.scheme != "https" and not (parsed.scheme == "http" and local and cfg["allow_insecure_local"]):
        raise ValueError("homeserver must use HTTPS; loopback HTTP requires allow_insecure_local")
    if not cfg["access_token"]:
        cfg["access_token"] = keychain()
    if not cfg["access_token"]:
        raise ValueError("Set MATRIX_ACCESS_TOKEN/access_token or Keychain service termite.matrix")
    cfg["sync_timeout_seconds"] = min(60, max(5, int(cfg["sync_timeout_seconds"])))
    cfg["timeline_limit"] = min(100, max(1, int(cfg["timeline_limit"])))
    if not cfg["state_file"]:
        root = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
        cfg["state_file"] = str(root / "termite" / "matrix.json")
    return cfg


class TermiteClient:
    def __init__(self) -> None:
        self.base = f"http://127.0.0.1:{os.environ['TERMITE_PORT']}"
        self.headers = {"Authorization": f"Bearer {os.environ['TERMITE_TOKEN']}", "Content-Type": "application/json"}

    def request(self, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, headers=self.headers,
                                     method="GET" if data is None else "POST")
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read()
        return json.loads(raw) if raw else {}

    def events(self):
        req = urllib.request.Request(self.base + "/v1/events", headers=self.headers)
        with urllib.request.urlopen(req, timeout=90) as response:
            for raw in response:
                if raw.startswith(b"data: "):
                    yield json.loads(raw[6:])


class MatrixAPI:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.headers = {"Authorization": f"Bearer {cfg['access_token']}", "User-Agent": "Termite-Matrix-Channel/1.0"}

    def sync(self, since: str | None) -> dict[str, Any]:
        room_filter = {"room": {"rooms": self.cfg["room_ids"], "timeline": {"limit": self.cfg["timeline_limit"]}}}
        query = {"timeout": str(self.cfg["sync_timeout_seconds"] * 1000),
                 "filter": json.dumps(room_filter, separators=(",", ":"))}
        if since:
            query["since"] = since
        url = self.cfg["homeserver"] + "/_matrix/client/v3/sync?" + urllib.parse.urlencode(query)
        with HTTP.open(urllib.request.Request(url, headers=self.headers),
                       timeout=self.cfg["sync_timeout_seconds"] + 15) as response:
            raw = response.read(MAX_SYNC_BYTES + 1)
        if len(raw) > MAX_SYNC_BYTES:
            raise ValueError("Matrix sync exceeds 4 MiB")
        value = json.loads(raw)
        if not isinstance(value, dict) or not value.get("next_batch"):
            raise ValueError("Matrix sync response lacks next_batch")
        return value

    def own_user_id(self) -> str:
        url = self.cfg["homeserver"] + "/_matrix/client/v3/account/whoami"
        with HTTP.open(urllib.request.Request(url, headers=self.headers), timeout=30) as response:
            raw = response.read(65537)
        if len(raw) > 65536:
            raise ValueError("Matrix whoami response exceeds 64 KiB")
        value = json.loads(raw)
        user_id = str(value.get("user_id") or "") if isinstance(value, dict) else ""
        if not user_id:
            raise ValueError("Matrix whoami response lacks user_id")
        return user_id

    def reply(self, reply: dict[str, Any]) -> None:
        room = reply["conversationID"]
        if room not in self.cfg["room_ids"]:
            raise ValueError("reply targets a room outside the configured allowlist")
        transaction = hashlib.sha256(reply["id"].encode()).hexdigest()
        path = "/_matrix/client/v3/rooms/{}/send/m.room.message/{}".format(
            urllib.parse.quote(room, safe=""), transaction)
        content: dict[str, Any] = {"msgtype": "m.text", "body": reply["body"]}
        if reply.get("replyToID"):
            content["m.relates_to"] = {"m.in_reply_to": {"event_id": reply["replyToID"]}}
        headers = dict(self.headers)
        headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.cfg["homeserver"] + path, data=json.dumps(content).encode(),
                                     headers=headers, method="PUT")
        with HTTP.open(req, timeout=30) as response:
            # A 2xx response establishes acceptance; its body is not required.
            response.read(65537)


def work_items(sync: dict[str, Any], allowed: set[str], own_user: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    joined = ((sync.get("rooms") or {}).get("join") or {})
    for room_id, room in joined.items():
        if room_id not in allowed:
            continue
        events = ((room or {}).get("timeline") or {}).get("events") or []
        for event in events:
            if not isinstance(event, dict):
                continue
            content = event.get("content") or {}
            if not isinstance(content, dict):
                continue
            event_id = str(event.get("event_id") or "")
            sender = str(event.get("sender") or "")
            if event.get("type") != "m.room.message" or content.get("msgtype") not in {"m.text", "m.emote"}:
                continue
            relates = content.get("m.relates_to") or {}
            if isinstance(relates, dict) and relates.get("rel_type") == "m.replace":
                continue
            if not event_id or not sender or sender == own_user:
                continue
            body = str(content.get("body") or "").encode("utf-8")[:65536].decode("utf-8", "ignore")
            if not body:
                continue
            results.append({
                "id": f"matrix-{hashlib.sha256(event_id.encode()).hexdigest()[:32]}",
                "deliveryID": bounded(event_id, 512), "conversationID": bounded(room_id, 512),
                "replyToID": bounded(event_id, 512), "senderID": bounded(sender, 256), "senderName": bounded(sender, 256),
                "title": bounded(f"Matrix message in {room_id}", 512), "body": body,
            })
    return results


def load_since(path: str) -> str | None:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        return str(value.get("next_batch")) if value.get("next_batch") else None
    except (OSError, ValueError, TypeError):
        return None


def save_since(path: str, token: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps({"next_batch": token}) + "\n", encoding="utf-8")
    temporary.replace(target)


def deliver(client: TermiteClient, api: MatrixAPI, reply: dict[str, Any]) -> None:
    client.request(f"/v1/channel-replies/{reply['id']}/attempt", {})
    try:
        api.reply(reply)
    except Exception as exc:
        message = bounded(f"Matrix delivery failed: {exc}", 512)
        report_health(client, "degraded", error=message, detail="Matrix reply failed")
        if delivery_uncertain(exc):
            client.request(f"/v1/channel-replies/{reply['id']}/ack", {
                "state": "verification-needed", "error": message
            })
        else:
            client.request(f"/v1/channel-replies/{reply['id']}/ack", {
                "delivered": False, "error": message
            })
        return
    report_health(client, "healthy", detail="Matrix reply accepted")
    client.request(f"/v1/channel-replies/{reply['id']}/ack", {"delivered": True})


def recover_pending(client: TermiteClient, api: MatrixAPI) -> None:
    for reply in client.request("/v1/channel-replies").get("replies", []):
        if reply.get("channel") == CHANNEL_ID:
            deliver(client, api, reply)


def reply_loop(client: TermiteClient, api: MatrixAPI) -> None:
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


def main() -> None:
    cfg = load_config()
    client = TermiteClient()
    api = MatrixAPI(cfg)
    authenticated_user = api.own_user_id()
    if authenticated_user != cfg["own_user_id"]:
        raise ValueError("own_user_id does not match the authenticated Matrix token")
    registration = client.request("/v1/channels", {
        "id": CHANNEL_ID, "name": cfg["channel_name"], "service": "Matrix",
        "account": cfg["account"], "description": f"Messages from {len(cfg['room_ids'])} allowlisted room(s)",
        "replyCapabilities": ["reply"],
    })
    report_health(client, "healthy", detail="Matrix credentials verified")
    for pending in registration.get("pendingReplies", []):
        deliver(client, api, pending)
    threading.Thread(target=reply_loop, args=(client, api), daemon=True).start()
    since = load_since(cfg["state_file"])
    failures = 0
    while True:
        try:
            response = api.sync(since)
            for item in work_items(response, set(cfg["room_ids"]), authenticated_user):
                client.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)
            since = str(response["next_batch"])
            save_since(cfg["state_file"], since)
            failures = 0
            report_health(client, "healthy", detail="Matrix sync succeeded")
        except Exception as exc:
            failures += 1
            delay = min(2 ** min(failures, 6), 60)
            report_health(client, "retrying", error=f"Matrix sync failed: {exc}",
                          retry_in=delay, detail="Matrix sync will retry")
            print(f"Matrix sync failed: {exc}; retrying in {delay}s", file=sys.stderr, flush=True)
            time.sleep(delay)


if __name__ == "__main__":
    main()
