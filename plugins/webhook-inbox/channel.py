#!/usr/bin/env python3
"""Authenticated JSON webhook receiver for Termite Channels (stdlib only)."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


PLUGIN_DIR = Path(__file__).resolve().parent
CHANNEL_ID = "dev.termite.webhook-inbox.inbox"
_last_success_at: str | None = None
_last_error_at: str | None = None


def bounded(value: Any, limit: int) -> str:
    return str(value).encode("utf-8")[:limit].decode("utf-8", "ignore")


def report_health(client: Any, status: str, *, error: str = "",
                  retry_in: int | None = None, detail: str = "") -> None:
    """Best-effort provider health; telemetry must never block connector work."""
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
    """Keep credentials on one origin and never redirect a provider POST."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if req.get_method() not in {"GET", "HEAD"} or origin(req.full_url) != origin(newurl):
            raise urllib.error.HTTPError(newurl, code, "unsafe cross-origin/provider redirect", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


HTTP = urllib.request.build_opener(SameOriginRedirect())


def provider_error(exc: Exception) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        return f"HTTP {exc.code}"
    if isinstance(exc, urllib.error.URLError):
        return "network error"
    return type(exc).__name__


def delivery_uncertain(exc: Exception) -> bool:
    """A transport failure after POST may hide provider acceptance."""
    return not isinstance(exc, urllib.error.HTTPError) and isinstance(
        exc, (urllib.error.URLError, TimeoutError, OSError)
    )


def _keychain(service: str) -> str:
    tool = Path("/usr/bin/security")
    if not service or not tool.exists():
        return ""
    try:
        return subprocess.run(
            [str(tool), "find-generic-password", "-s", service, "-w"],
            check=True, capture_output=True, text=True, timeout=5
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def load_config() -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "listen_host": "127.0.0.1", "listen_port": 8787,
        "inbound_secret": "", "callback_url": "", "callback_bearer_token": "",
        "allow_insecure_local_callback": False,
        "max_body_bytes": 65536, "channel_name": "Webhook Inbox", "account": "local",
    }
    path = PLUGIN_DIR / "config.json"
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("config.json must contain a JSON object")
        cfg.update(loaded)
    env = {
        "listen_host": "WEBHOOK_LISTEN_HOST", "listen_port": "WEBHOOK_LISTEN_PORT",
        "inbound_secret": "WEBHOOK_SECRET", "callback_url": "WEBHOOK_REPLY_URL",
        "callback_bearer_token": "WEBHOOK_REPLY_TOKEN",
        "allow_insecure_local_callback": "WEBHOOK_ALLOW_INSECURE_LOCAL_CALLBACK",
        "max_body_bytes": "WEBHOOK_MAX_BODY_BYTES", "channel_name": "WEBHOOK_CHANNEL_NAME",
        "account": "WEBHOOK_ACCOUNT",
    }
    for key, name in env.items():
        if name in os.environ:
            cfg[key] = os.environ[name]
    cfg["listen_port"] = int(cfg["listen_port"])
    cfg["max_body_bytes"] = min(65536, max(1024, int(cfg["max_body_bytes"])))
    cfg["allow_insecure_local_callback"] = str(cfg["allow_insecure_local_callback"]).lower() in {"1", "true", "yes"}
    if not 1 <= cfg["listen_port"] <= 65535:
        raise ValueError("listen_port must be between 1 and 65535")
    if not cfg["inbound_secret"]:
        cfg["inbound_secret"] = _keychain("termite.webhook-inbox")
    if not cfg["inbound_secret"]:
        raise ValueError("Set WEBHOOK_SECRET/inbound_secret or Keychain service termite.webhook-inbox")
    if not cfg["callback_bearer_token"]:
        cfg["callback_bearer_token"] = _keychain("termite.webhook-inbox.callback")
    if cfg["callback_url"]:
        parsed = urllib.parse.urlparse(str(cfg["callback_url"]))
        if not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("callback_url must have a host and must not contain credentials")
        local = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        if parsed.scheme != "https" and not (
            parsed.scheme == "http" and local and cfg["allow_insecure_local_callback"]
        ):
            raise ValueError("callback_url must use HTTPS; loopback HTTP requires allow_insecure_local_callback")
    return cfg


class TermiteClient:
    def __init__(self) -> None:
        self.base = f"http://127.0.0.1:{os.environ['TERMITE_PORT']}"
        self.headers = {
            "Authorization": f"Bearer {os.environ['TERMITE_TOKEN']}",
            "Content-Type": "application/json",
        }

    def request(self, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, headers=self.headers,
                                     method="GET" if body is None else "POST")
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read()
        return json.loads(raw) if raw else {}

    def events(self):
        req = urllib.request.Request(self.base + "/v1/events", headers=self.headers)
        with urllib.request.urlopen(req, timeout=90) as response:
            for raw in response:
                if raw.startswith(b"data: "):
                    yield json.loads(raw[6:])


def normalize_event(payload: dict[str, Any]) -> dict[str, Any]:
    delivery = str(payload.get("deliveryID", "")).strip()
    if not delivery:
        raise ValueError("deliveryID is required and must be the sender's stable event id")
    body = str(payload.get("body", "")).strip()
    if not body:
        raise ValueError("body is required")
    if len(body.encode("utf-8")) > 65536:
        raise ValueError("body exceeds Termite's 64 KiB limit")
    digest = hashlib.sha256(delivery.encode()).hexdigest()
    stable = digest[:24]
    bounded_delivery = bounded(delivery, 512)
    if bounded_delivery != delivery:
        bounded_delivery = f"sha256:{digest}"
    result = {
        "id": bounded(payload.get("id") or f"webhook-{stable}", 256),
        "deliveryID": bounded_delivery,
        "conversationID": bounded(payload.get("conversationID") or delivery, 512),
        "senderID": bounded(payload.get("senderID") or "webhook", 256),
        "senderName": bounded(payload.get("senderName") or "Webhook", 256),
        "title": bounded(payload.get("title") or "Incoming webhook", 512),
        "body": body,
    }
    for key, limit in (("replyToID", 512), ("createdAt", 128), ("projectHint", 1024)):
        if payload.get(key):
            result[key] = bounded(payload[key], limit)
    return result


def deliver_reply(client: TermiteClient, cfg: dict[str, Any], reply: dict[str, Any]) -> None:
    url = str(cfg["callback_url"])
    if not url:
        client.request(f"/v1/channel-replies/{reply['id']}/ack", {
            "delivered": False, "error": "No callback_url configured"
        })
        return
    payload = json.dumps({
        "id": reply["id"], "kind": reply.get("replyKind", "result"),
        "conversationID": reply["conversationID"], "replyToID": reply.get("replyToID"),
        "body": reply["body"],
    }).encode()
    headers = {"Content-Type": "application/json", "Idempotency-Key": reply["id"]}
    if cfg["callback_bearer_token"]:
        headers["Authorization"] = f"Bearer {cfg['callback_bearer_token']}"
    client.request(f"/v1/channel-replies/{reply['id']}/attempt", {})
    try:
        request = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with HTTP.open(request, timeout=30) as response:
            if not 200 <= response.status < 300:
                raise RuntimeError(f"callback returned HTTP {response.status}")
    except Exception as exc:
        message = f"Callback failed: {provider_error(exc)}"
        report_health(client, "degraded", error=message, detail="Outbound callback failed")
        if delivery_uncertain(exc):
            client.request(f"/v1/channel-replies/{reply['id']}/ack", {
                "state": "verification-needed", "error": message
            })
        else:
            client.request(f"/v1/channel-replies/{reply['id']}/ack", {
                "delivered": False, "error": message
            })
        return
    # Do not misreport an accepted provider delivery if the host ack fails. The
    # host keeps the attempt in delivering until restart requires verification.
    report_health(client, "healthy", detail="Outbound callback accepted")
    client.request(f"/v1/channel-replies/{reply['id']}/ack", {"delivered": True})


def make_handler(client: TermiteClient, cfg: dict[str, Any]):
    class Handler(BaseHTTPRequestHandler):
        server_version = "TermiteWebhook/1"

        def reply(self, status: int, value: dict[str, Any]) -> None:
            data = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            if self.path == "/health":
                self.reply(200, {"ok": True, "channel": CHANNEL_ID})
            else:
                self.reply(404, {"error": "not found"})

        def do_POST(self) -> None:
            if self.path not in {"/", "/events", "/webhook"}:
                self.reply(404, {"error": "not found"})
                return
            secret = str(cfg["inbound_secret"])
            provided = self.headers.get("Authorization", "")
            if secret and not secrets.compare_digest(provided, f"Bearer {secret}"):
                self.reply(401, {"error": "invalid bearer token"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 1 or length > cfg["max_body_bytes"]:
                    raise ValueError("invalid Content-Length")
                raw = self.rfile.read(length)
                payload = json.loads(raw)
                if not isinstance(payload, dict):
                    raise ValueError("request JSON must be an object")
                work = normalize_event(payload)
                result = client.request(f"/v1/channels/{CHANNEL_ID}/work-items", work)
                report_health(client, "healthy", detail="Webhook listener accepted an event")
                self.reply(202, {"accepted": True, "id": result.get("id", work["id"])})
            except (ValueError, json.JSONDecodeError) as exc:
                self.reply(400, {"error": str(exc)})
            except Exception as exc:
                print(f"webhook ingest failed: {exc}", file=sys.stderr, flush=True)
                self.reply(502, {"error": "Termite rejected the event"})

        def log_message(self, fmt: str, *args: Any) -> None:
            print(f"webhook: {fmt % args}", file=sys.stderr, flush=True)
    return Handler


def recover_pending(client: TermiteClient, cfg: dict[str, Any]) -> None:
    for reply in client.request("/v1/channel-replies").get("replies", []):
        if reply.get("channel") == CHANNEL_ID:
            deliver_reply(client, cfg, reply)


def reply_loop(client: TermiteClient, cfg: dict[str, Any]) -> None:
    delay = 1
    while True:
        try:
            recover_pending(client, cfg)
            for event in client.events():
                delay = 1
                if event.get("kind") == "channel-reply" and event.get("channel") == CHANNEL_ID:
                    deliver_reply(client, cfg, event)
        except Exception as exc:
            print(f"reply stream disconnected: {exc}; retrying", file=sys.stderr, flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def main() -> None:
    cfg = load_config()
    client = TermiteClient()
    registration = client.request("/v1/channels", {
        "id": CHANNEL_ID, "name": cfg["channel_name"], "service": "Webhook",
        "account": cfg["account"], "description": "Authenticated JSON webhook receiver",
        "replyCapabilities": ["reply"] if cfg["callback_url"] else [],
    })
    for pending in registration.get("pendingReplies", []):
        deliver_reply(client, cfg, pending)
    if cfg["callback_url"]:
        threading.Thread(target=reply_loop, args=(client, cfg), daemon=True).start()
    server = ThreadingHTTPServer((cfg["listen_host"], cfg["listen_port"]), make_handler(client, cfg))
    report_health(client, "healthy", detail="Webhook listener is ready")
    print(f"Webhook Inbox listening on http://{cfg['listen_host']}:{cfg['listen_port']}/events", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
