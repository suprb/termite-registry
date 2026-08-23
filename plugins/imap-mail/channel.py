#!/usr/bin/env python3
"""TLS IMAP inbox with optional TLS SMTP replies for Termite Channels."""

from __future__ import annotations

from email import policy
from email.header import decode_header, make_header
from email.message import EmailMessage, Message
from email.parser import BytesParser
from email.utils import parseaddr
from datetime import datetime, timedelta, timezone
import hashlib
import imaplib
import json
import os
from pathlib import Path
import re
import smtplib
import ssl
import subprocess
import sys
import threading
import time
from typing import Any
import urllib.request


PLUGIN_DIR = Path(__file__).resolve().parent
CHANNEL_ID = "dev.termite.imap-mail.inbox"
MAX_MESSAGE_BYTES = 512 * 1024
MAX_BODY_BYTES = 64 * 1024
_last_success_at: str | None = None
_last_error_at: str | None = None


def bounded(value: Any, limit: int) -> str:
    return str(value).encode("utf-8")[:limit].decode("utf-8", "ignore")


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
        "imap_host": "", "imap_port": 993, "username": "", "password": "",
        "mailbox": "INBOX", "search": "ALL", "poll_seconds": 120, "max_messages": 50,
        "smtp_host": "", "smtp_port": 465, "smtp_mode": "ssl", "smtp_username": "",
        "smtp_password": "", "from_address": "", "channel_name": "Email Inbox",
        "allow_insecure_local": False,
    }
    path = PLUGIN_DIR / "config.json"
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("config.json must contain a JSON object")
        cfg.update(loaded)
    env = {
        "imap_host": "IMAP_HOST", "imap_port": "IMAP_PORT", "username": "IMAP_USERNAME",
        "password": "IMAP_PASSWORD", "mailbox": "IMAP_MAILBOX", "search": "IMAP_SEARCH",
        "poll_seconds": "IMAP_POLL_SECONDS", "max_messages": "IMAP_MAX_MESSAGES",
        "smtp_host": "SMTP_HOST", "smtp_port": "SMTP_PORT", "smtp_mode": "SMTP_MODE",
        "smtp_username": "SMTP_USERNAME", "smtp_password": "SMTP_PASSWORD",
        "from_address": "SMTP_FROM", "channel_name": "IMAP_CHANNEL_NAME",
        "allow_insecure_local": "IMAP_ALLOW_INSECURE_LOCAL",
    }
    for key, name in env.items():
        if name in os.environ:
            cfg[key] = os.environ[name]
    cfg["imap_port"] = int(cfg["imap_port"])
    cfg["smtp_port"] = int(cfg["smtp_port"])
    if not 1 <= cfg["imap_port"] <= 65535 or not 1 <= cfg["smtp_port"] <= 65535:
        raise ValueError("IMAP and SMTP ports must be between 1 and 65535")
    cfg["poll_seconds"] = min(86400, max(30, int(cfg["poll_seconds"])))
    cfg["max_messages"] = min(200, max(1, int(cfg["max_messages"])))
    cfg["allow_insecure_local"] = str(cfg["allow_insecure_local"]).lower() in {"1", "true", "yes"}
    cfg["search"] = str(cfg["search"]).upper()
    if cfg["search"] not in {"ALL", "UNSEEN"}:
        raise ValueError("search must be ALL or UNSEEN")
    if not cfg["imap_host"] or not cfg["username"]:
        raise ValueError("imap_host and username are required")
    if not cfg["password"]:
        cfg["password"] = keychain("termite.imap-mail")
    if not cfg["password"]:
        raise ValueError("Set IMAP_PASSWORD/password or Keychain service termite.imap-mail")
    if cfg["smtp_host"]:
        if cfg["smtp_mode"] not in {"ssl", "starttls", "plain"}:
            raise ValueError("smtp_mode must be ssl, starttls, or loopback-only plain")
        local = cfg["smtp_host"] in {"127.0.0.1", "localhost", "::1"}
        if cfg["smtp_mode"] == "plain" and not (local and cfg["allow_insecure_local"]):
            raise ValueError("plain SMTP is allowed only on loopback with allow_insecure_local")
        if not cfg["smtp_password"]:
            cfg["smtp_password"] = keychain("termite.imap-mail.smtp") or cfg["password"]
        cfg["smtp_username"] = cfg["smtp_username"] or cfg["username"]
        cfg["from_address"] = cfg["from_address"] or cfg["smtp_username"]
    return cfg


class TermiteClient:
    def __init__(self) -> None:
        self.base = f"http://127.0.0.1:{os.environ['TERMITE_PORT']}"
        self.headers = {"Authorization": f"Bearer {os.environ['TERMITE_TOKEN']}",
                        "Content-Type": "application/json"}

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


def decoded(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except (LookupError, UnicodeError):
        return value


def message_body(message: Message) -> str:
    candidates: list[str] = []
    parts = message.walk() if message.is_multipart() else [message]
    for part in parts:
        if part.get_content_disposition() == "attachment" or part.get_content_type() != "text/plain":
            continue
        try:
            content = part.get_content()
        except (LookupError, UnicodeError):
            payload = part.get_payload(decode=True) or b""
            content = payload.decode("utf-8", "replace")
        if isinstance(content, str):
            candidates.append(content)
    value = "\n\n".join(candidates).strip() or "(No plain-text body; attachments and HTML are not imported.)"
    return value.encode("utf-8")[:MAX_BODY_BYTES].decode("utf-8", "ignore")


def work_item(raw: bytes, server: str, account: str, mailbox: str,
              uid_validity: str, uid: str) -> dict[str, Any]:
    message = BytesParser(policy=policy.default).parsebytes(raw[:MAX_MESSAGE_BYTES])
    sender_name, sender_address = parseaddr(decoded(message.get("From")))
    provider_id = f"{server}\0{account}\0{mailbox}\0{uid_validity}\0{uid}"
    digest = hashlib.sha256(provider_id.encode()).hexdigest()
    message_id = str(message.get("Message-ID", "")).strip()
    result: dict[str, Any] = {
        "id": f"mail-{digest[:32]}", "deliveryID": f"imap:{digest}",
        "conversationID": bounded(sender_address or account, 512),
        "senderID": bounded(sender_address or "unknown", 256),
        "senderName": bounded(sender_name or sender_address or "Email sender", 256),
        "title": bounded(decoded(message.get("Subject")), 512) or "Email message", "body": message_body(message),
    }
    if message_id:
        result["replyToID"] = bounded(message_id, 512)
    return result


def is_own_message(item: dict[str, Any], cfg: dict[str, Any]) -> bool:
    sender = parseaddr(str(item.get("senderID") or ""))[1].casefold()
    own = {
        parseaddr(str(cfg.get(key) or ""))[1].casefold()
        for key in ("username", "from_address", "smtp_username")
    }
    own.discard("")
    return bool(sender and sender in own)


def poll_mailbox(client: TermiteClient, cfg: dict[str, Any]) -> None:
    context = ssl.create_default_context()
    connection = imaplib.IMAP4_SSL(cfg["imap_host"], cfg["imap_port"], ssl_context=context, timeout=30)
    try:
        connection.login(cfg["username"], cfg["password"])
        status, _ = connection.select(cfg["mailbox"], readonly=True)
        if status != "OK":
            raise RuntimeError(f"cannot select mailbox {cfg['mailbox']}")
        response = connection.response("UIDVALIDITY")
        values = response[1] or []
        uid_validity = values[0].decode() if values and isinstance(values[0], bytes) else str(values[0] if values else "unknown")
        status, data = connection.uid("search", None, cfg["search"])
        if status != "OK":
            raise RuntimeError("IMAP UID search failed")
        uids = (data[0] or b"").split()[-cfg["max_messages"]:]
        for raw_uid in uids:
            uid = raw_uid.decode("ascii")
            status, chunks = connection.uid("fetch", uid, f"(BODY.PEEK[]<0.{MAX_MESSAGE_BYTES}>)")
            if status != "OK":
                raise RuntimeError(f"IMAP fetch failed for UID {uid}")
            raw = next((chunk[1] for chunk in chunks if isinstance(chunk, tuple) and isinstance(chunk[1], bytes)), b"")
            if raw:
                item = work_item(raw, cfg["imap_host"], cfg["username"], cfg["mailbox"], uid_validity, uid)
                if not is_own_message(item, cfg):
                    client.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)
    finally:
        try:
            connection.logout()
        except imaplib.IMAP4.error:
            pass


def send_reply(cfg: dict[str, Any], reply: dict[str, Any]) -> None:
    recipient = parseaddr(reply["conversationID"])[1]
    if not recipient or "@" not in recipient:
        raise ValueError("original conversation does not contain a reply email address")
    message = EmailMessage()
    message["From"] = cfg["from_address"]
    message["To"] = recipient
    message["Subject"] = "Re: Termite result"
    message["Message-ID"] = f"<termite-{re.sub(r'[^A-Za-z0-9.-]', '-', reply['id'])}@localhost>"
    if reply.get("replyToID"):
        message["In-Reply-To"] = reply["replyToID"]
        message["References"] = reply["replyToID"]
    message.set_content(bounded(reply["body"], MAX_BODY_BYTES))
    context = ssl.create_default_context()
    if cfg["smtp_mode"] == "ssl":
        smtp: smtplib.SMTP = smtplib.SMTP_SSL(cfg["smtp_host"], cfg["smtp_port"], context=context, timeout=30)
    else:
        smtp = smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"], timeout=30)
    try:
        if cfg["smtp_mode"] == "starttls":
            smtp.starttls(context=context)
        if cfg["smtp_username"]:
            smtp.login(cfg["smtp_username"], cfg["smtp_password"])
        smtp.send_message(message)
    finally:
        try:
            smtp.quit()
        except smtplib.SMTPException:
            smtp.close()


def delivery_uncertain(exc: Exception) -> bool:
    """SMTP transport loss can occur after DATA was accepted."""
    return isinstance(exc, (smtplib.SMTPServerDisconnected, TimeoutError, OSError))


def deliver(client: TermiteClient, cfg: dict[str, Any], reply: dict[str, Any]) -> None:
    client.request(f"/v1/channel-replies/{reply['id']}/attempt", {})
    try:
        send_reply(cfg, reply)
    except Exception as exc:
        message = bounded(f"SMTP delivery failed: {exc}", 512)
        report_health(client, "degraded", error=message, detail="SMTP delivery failed")
        if delivery_uncertain(exc):
            client.request(f"/v1/channel-replies/{reply['id']}/ack", {
                "state": "verification-needed", "error": message
            })
        else:
            client.request(f"/v1/channel-replies/{reply['id']}/ack", {
                "delivered": False, "error": message
            })
        return
    report_health(client, "healthy", detail="SMTP delivery accepted")
    client.request(f"/v1/channel-replies/{reply['id']}/ack", {"delivered": True})


def recover_pending(client: TermiteClient, cfg: dict[str, Any]) -> None:
    for reply in client.request("/v1/channel-replies").get("replies", []):
        if reply.get("channel") == CHANNEL_ID:
            deliver(client, cfg, reply)


def reply_loop(client: TermiteClient, cfg: dict[str, Any]) -> None:
    delay = 1
    while True:
        try:
            recover_pending(client, cfg)
            for event in client.events():
                delay = 1
                if event.get("kind") == "channel-reply" and event.get("channel") == CHANNEL_ID:
                    deliver(client, cfg, event)
        except Exception as exc:
            print(f"reply stream disconnected: {exc}", file=sys.stderr, flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def main() -> None:
    cfg = load_config()
    client = TermiteClient()
    can_reply = bool(cfg["smtp_host"])
    registration = client.request("/v1/channels", {
        "id": CHANNEL_ID, "name": cfg["channel_name"], "service": "Email",
        "account": cfg["username"], "description": f"Read-only IMAP polling of {cfg['mailbox']}",
        "replyCapabilities": ["reply"] if can_reply else [],
    })
    if can_reply:
        for pending in registration.get("pendingReplies", []):
            deliver(client, cfg, pending)
        threading.Thread(target=reply_loop, args=(client, cfg), daemon=True).start()
    delay = 0
    failures = 0
    while True:
        if delay:
            time.sleep(delay)
        try:
            poll_mailbox(client, cfg)
            failures = 0
            delay = cfg["poll_seconds"]
            report_health(client, "healthy", detail="IMAP poll succeeded")
        except Exception as exc:
            failures += 1
            delay = min(cfg["poll_seconds"] * (2 ** min(failures, 5)), 3600)
            report_health(client, "retrying", error=f"IMAP poll failed: {exc}",
                          retry_in=delay, detail="IMAP poll will retry")
            print(f"IMAP poll failed: {exc}; retrying in {delay}s", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
