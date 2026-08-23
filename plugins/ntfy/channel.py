#!/usr/bin/env python3
"""Allowlisted ntfy subscriber and optional fixed-topic publisher."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
from typing import Any
import urllib.parse
import urllib.error
import urllib.request


PLUGIN_DIR = Path(__file__).resolve().parent
CHANNEL_ID = "dev.termite.ntfy.topics"
MAX_POLL_BYTES = 2 * 1024 * 1024
TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
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
            ["/usr/bin/security", "find-generic-password", "-s", "termite.ntfy", "-w"],
            check=True, capture_output=True, text=True, timeout=5
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def load_config() -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "server_url": "https://ntfy.sh", "topics": [], "access_token": "",
        "poll_seconds": 60, "initial_since": "10m", "max_messages": 100,
        "reply_topic": "", "max_publish_bytes": 4096, "channel_name": "ntfy Topics",
        "account": "ntfy", "allow_insecure_local": False, "state_file": "",
    }
    path = PLUGIN_DIR / "config.json"
    if path.exists():
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("config.json must contain a JSON object")
        cfg.update(value)
    env = {
        "server_url": "NTFY_SERVER_URL", "topics": "NTFY_TOPICS", "access_token": "NTFY_ACCESS_TOKEN",
        "poll_seconds": "NTFY_POLL_SECONDS", "initial_since": "NTFY_INITIAL_SINCE",
        "max_messages": "NTFY_MAX_MESSAGES", "reply_topic": "NTFY_REPLY_TOPIC",
        "max_publish_bytes": "NTFY_MAX_PUBLISH_BYTES", "channel_name": "NTFY_CHANNEL_NAME",
        "account": "NTFY_ACCOUNT", "allow_insecure_local": "NTFY_ALLOW_INSECURE_LOCAL",
        "state_file": "NTFY_STATE_FILE",
    }
    for key, name in env.items():
        if name in os.environ:
            cfg[key] = os.environ[name]
    if isinstance(cfg["topics"], str):
        cfg["topics"] = [item.strip() for item in cfg["topics"].replace("\n", ",").split(",") if item.strip()]
    if not isinstance(cfg["topics"], list) or not cfg["topics"]:
        raise ValueError("topics/NTFY_TOPICS must explicitly allow at least one topic")
    if len(cfg["topics"]) > 16 or any(not TOPIC_PATTERN.fullmatch(str(topic)) for topic in cfg["topics"]):
        raise ValueError("topics must contain 1–16 valid ntfy topic names")
    if cfg["reply_topic"] and not TOPIC_PATTERN.fullmatch(str(cfg["reply_topic"])):
        raise ValueError("reply_topic must be one explicit valid ntfy topic")
    if cfg["reply_topic"] and cfg["reply_topic"] in cfg["topics"]:
        raise ValueError("reply_topic must differ from subscribed topics to prevent reply loops")
    cfg["server_url"] = str(cfg["server_url"]).rstrip("/")
    cfg["allow_insecure_local"] = str(cfg["allow_insecure_local"]).lower() in {"1", "true", "yes"}
    parsed = urllib.parse.urlparse(cfg["server_url"])
    if not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("server_url must have a host and must not contain credentials")
    local = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    if parsed.scheme != "https" and not (parsed.scheme == "http" and local and cfg["allow_insecure_local"]):
        raise ValueError("server_url must use HTTPS; loopback HTTP requires allow_insecure_local")
    if not cfg["access_token"]:
        cfg["access_token"] = keychain()
    cfg["poll_seconds"] = min(3600, max(30, int(cfg["poll_seconds"])))
    cfg["max_messages"] = min(500, max(1, int(cfg["max_messages"])))
    cfg["max_publish_bytes"] = min(65536, max(1, int(cfg["max_publish_bytes"])))
    if cfg["initial_since"] != "all" and not re.fullmatch(r"[1-9][0-9]*[smhd]", str(cfg["initial_since"])):
        raise ValueError("initial_since must be 'all' or a duration such as 10m or 2h")
    if not cfg["state_file"]:
        root = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
        cfg["state_file"] = str(root / "termite" / "ntfy.json")
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


class NtfyAPI:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.headers = {"User-Agent": "Termite-ntfy-Channel/1.0"}
        if cfg["access_token"]:
            self.headers["Authorization"] = f"Bearer {cfg['access_token']}"

    def poll(self, since: str) -> list[dict[str, Any]]:
        topic_path = ",".join(urllib.parse.quote(topic, safe="") for topic in self.cfg["topics"])
        query = urllib.parse.urlencode({"poll": "1", "since": since})
        url = f"{self.cfg['server_url']}/{topic_path}/json?{query}"
        with HTTP.open(urllib.request.Request(url, headers=self.headers), timeout=30) as response:
            raw = response.read(MAX_POLL_BYTES + 1)
        if len(raw) > MAX_POLL_BYTES:
            raise ValueError("ntfy poll response exceeds 2 MiB")
        messages = []
        for line in raw.splitlines():
            if not line.strip():
                continue
            value = json.loads(line)
            if value.get("event") == "message":
                messages.append(value)
                if len(messages) >= self.cfg["max_messages"]:
                    break
        return messages

    def publish(self, reply: dict[str, Any]) -> None:
        topic = str(self.cfg["reply_topic"])
        if not topic:
            raise ValueError("no reply_topic is configured")
        data = reply["body"].encode("utf-8")
        if len(data) > self.cfg["max_publish_bytes"]:
            raise ValueError(f"reply exceeds configured {self.cfg['max_publish_bytes']} byte limit")
        headers = dict(self.headers)
        headers.update({"Content-Type": "text/plain; charset=utf-8", "X-Title": "Termite reply",
                        "X-Termite-Reply-ID": reply["id"]})
        url = f"{self.cfg['server_url']}/{urllib.parse.quote(topic, safe='')}"
        with HTTP.open(urllib.request.Request(url, data=data, headers=headers, method="POST"),
                       timeout=30) as response:
            # A 2xx response establishes acceptance; its body is not required.
            response.read(65537)


def work_item(message: dict[str, Any], server_url: str, allowed: set[str]) -> dict[str, Any]:
    message_id = str(message.get("id") or "")
    topic = str(message.get("topic") or "")
    if not message_id or topic not in allowed:
        raise ValueError("ntfy message lacks an id or belongs to a non-allowlisted topic")
    server = hashlib.sha256(server_url.encode()).hexdigest()[:16]
    body = str(message.get("message") or "")
    click = str(message.get("click") or "")
    if click:
        body = (body + "\n\n" if body else "") + click
    body = body.encode("utf-8")[:65536].decode("utf-8", "ignore") or "(Empty ntfy message)"
    identity = f"ntfy:{server}:{message_id}"
    if len(identity.encode("utf-8")) > 512:
        identity = f"ntfy:{server}:sha256:{hashlib.sha256(message_id.encode()).hexdigest()}"
    item_digest = hashlib.sha256((server_url + "\0" + message_id).encode()).hexdigest()[:32]
    result: dict[str, Any] = {
        "id": f"ntfy-{item_digest}",
        "deliveryID": identity, "conversationID": topic,
        "replyToID": bounded(message_id, 512), "senderID": topic, "senderName": bounded(f"ntfy / {topic}", 256),
        "title": bounded(message.get("title") or f"ntfy message in {topic}", 512), "body": body,
    }
    if isinstance(message.get("time"), (int, float)):
        try:
            result["createdAt"] = datetime.fromtimestamp(message["time"], timezone.utc).isoformat().replace("+00:00", "Z")
        except (OverflowError, OSError, ValueError):
            pass
    return result


def load_since(path: str, fallback: str) -> str:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        return str(value.get("last_id")) if value.get("last_id") else fallback
    except (OSError, ValueError, TypeError):
        return fallback


def save_since(path: str, message_id: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps({"last_id": message_id}) + "\n", encoding="utf-8")
    temporary.replace(target)


def deliver(client: TermiteClient, api: NtfyAPI, reply: dict[str, Any]) -> None:
    client.request(f"/v1/channel-replies/{reply['id']}/attempt", {})
    try:
        api.publish(reply)
    except Exception as exc:
        message = bounded(f"ntfy publish failed: {exc}", 512)
        report_health(client, "degraded", error=message, detail="ntfy publish failed")
        if delivery_uncertain(exc):
            client.request(f"/v1/channel-replies/{reply['id']}/ack", {
                "state": "verification-needed", "error": message
            })
        else:
            client.request(f"/v1/channel-replies/{reply['id']}/ack", {
                "delivered": False, "error": message
            })
        return
    report_health(client, "healthy", detail="ntfy publish accepted")
    client.request(f"/v1/channel-replies/{reply['id']}/ack", {"delivered": True})


def recover_pending(client: TermiteClient, api: NtfyAPI) -> None:
    for reply in client.request("/v1/channel-replies").get("replies", []):
        if reply.get("channel") == CHANNEL_ID:
            deliver(client, api, reply)


def reply_loop(client: TermiteClient, api: NtfyAPI) -> None:
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
    api = NtfyAPI(cfg)
    can_reply = bool(cfg["reply_topic"])
    registration = client.request("/v1/channels", {
        "id": CHANNEL_ID, "name": cfg["channel_name"], "service": "ntfy",
        "account": cfg["account"], "description": f"Messages from {len(cfg['topics'])} allowlisted topic(s)",
        "replyCapabilities": ["reply"] if can_reply else [],
    })
    if can_reply:
        for pending in registration.get("pendingReplies", []):
            deliver(client, api, pending)
        threading.Thread(target=reply_loop, args=(client, api), daemon=True).start()
    since = load_since(cfg["state_file"], cfg["initial_since"])
    failures = 0
    delay = 0
    while True:
        if delay:
            time.sleep(delay)
        try:
            messages = api.poll(since)
            ingested = []
            for message in messages:
                client.request(f"/v1/channels/{CHANNEL_ID}/work-items",
                               work_item(message, cfg["server_url"], set(cfg["topics"])))
                ingested.append(str(message["id"]))
            if ingested:
                since = ingested[-1]
                save_since(cfg["state_file"], since)
            failures = 0
            delay = cfg["poll_seconds"]
            report_health(client, "healthy", detail="ntfy poll succeeded")
        except Exception as exc:
            failures += 1
            delay = min(cfg["poll_seconds"] * (2 ** min(failures, 5)), 3600)
            report_health(client, "retrying", error=f"ntfy poll failed: {exc}",
                          retry_in=delay, detail="ntfy poll will retry")
            print(f"ntfy poll failed: {exc}; retrying in {delay}s", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
