#!/usr/bin/env python3
"""Bounded, read-only RSS/Atom connector for Termite Channels."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


PLUGIN_DIR = Path(__file__).resolve().parent
CHANNEL_ID = "dev.termite.rss-feed.inbox"
MAX_DOWNLOAD = 2 * 1024 * 1024
_last_success_at: str | None = None
_last_error_at: str | None = None


def origin(url: str) -> tuple[str, str, int | None]:
    parsed = urllib.parse.urlsplit(url)
    return parsed.scheme.lower(), (parsed.hostname or "").lower(), parsed.port


class SameOriginRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if origin(req.full_url) != origin(newurl):
            raise urllib.error.HTTPError(newurl, code, "unsafe cross-origin redirect", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


HTTP = urllib.request.build_opener(SameOriginRedirect())


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def plain_text(value: str, limit: int = 65536) -> str:
    parser = _TextExtractor()
    try:
        parser.feed(value)
        text = " ".join(" ".join(parser.parts).split())
    except Exception:
        text = " ".join(unescape(value).split())
    return text.encode("utf-8")[:limit].decode("utf-8", "ignore")


def bounded(value: str, limit: int) -> str:
    return value.encode("utf-8")[:limit].decode("utf-8", "ignore")


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
        client.post(f"/v1/channels/{CHANNEL_ID}/health", body)
    except Exception as exc:
        print(f"health update failed: {type(exc).__name__}", file=sys.stderr, flush=True)


def safe_url_label(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _keychain(service: str) -> str:
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
        "feed_urls": [], "poll_seconds": 300, "max_entries_per_feed": 25,
        "request_timeout": 30, "bearer_token": "", "channel_name": "RSS and Atom Feed",
        "account": "feeds", "allow_insecure_local": False,
    }
    path = PLUGIN_DIR / "config.json"
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("config.json must contain a JSON object")
        cfg.update(loaded)
    env = {
        "feed_urls": "RSS_FEED_URLS", "poll_seconds": "RSS_POLL_SECONDS",
        "max_entries_per_feed": "RSS_MAX_ENTRIES", "request_timeout": "RSS_REQUEST_TIMEOUT",
        "bearer_token": "RSS_BEARER_TOKEN", "channel_name": "RSS_CHANNEL_NAME",
        "account": "RSS_ACCOUNT", "allow_insecure_local": "RSS_ALLOW_INSECURE_LOCAL",
    }
    for key, name in env.items():
        if name in os.environ:
            cfg[key] = os.environ[name]
    if isinstance(cfg["feed_urls"], str):
        cfg["feed_urls"] = [item.strip() for item in cfg["feed_urls"].replace("\n", ",").split(",") if item.strip()]
    if not isinstance(cfg["feed_urls"], list) or not cfg["feed_urls"]:
        raise ValueError("Configure at least one feed_urls entry or RSS_FEED_URLS")
    if len(cfg["feed_urls"]) > 16:
        raise ValueError("At most 16 feeds are supported by one Channel")
    cfg["allow_insecure_local"] = str(cfg["allow_insecure_local"]).lower() in {"1", "true", "yes"}
    for url in cfg["feed_urls"]:
        parsed = urllib.parse.urlparse(str(url))
        if not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("feed URLs must have a host and must not contain credentials")
        local = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        if parsed.scheme != "https" and not (
            parsed.scheme == "http" and local and cfg["allow_insecure_local"]
        ):
            raise ValueError("feed URLs must use HTTPS; loopback HTTP requires allow_insecure_local")
    cfg["poll_seconds"] = min(86400, max(30, int(cfg["poll_seconds"])))
    cfg["max_entries_per_feed"] = min(100, max(1, int(cfg["max_entries_per_feed"])))
    cfg["request_timeout"] = min(120, max(5, int(cfg["request_timeout"])))
    if not cfg["bearer_token"]:
        cfg["bearer_token"] = _keychain("termite.rss-feed")
    return cfg


class TermiteClient:
    def __init__(self) -> None:
        self.base = f"http://127.0.0.1:{os.environ['TERMITE_PORT']}"
        self.headers = {"Authorization": f"Bearer {os.environ['TERMITE_TOKEN']}",
                        "Content-Type": "application/json"}

    def post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        req = urllib.request.Request(self.base + path, data=json.dumps(body).encode(),
                                     headers=self.headers, method="POST")
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read()
        return json.loads(raw) if raw else {}


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def child_text(node: ET.Element, names: tuple[str, ...]) -> str:
    for child in node:
        if local_name(child.tag) in names:
            return "".join(child.itertext()).strip()
    return ""


def entry_link(node: ET.Element) -> str:
    for child in node:
        if local_name(child.tag) == "link":
            href = child.attrib.get("href", "").strip()
            rel = child.attrib.get("rel", "alternate")
            if href and rel in {"alternate", ""}:
                return href
            if child.text and child.text.strip():
                return child.text.strip()
    return ""


def iso_date(value: str) -> str | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_feed(data: bytes, feed_url: str, maximum: int) -> list[dict[str, Any]]:
    root = ET.fromstring(data)
    feed_title = child_text(root, ("title",))
    container = root
    if local_name(root.tag) == "rss":
        container = next((x for x in root if local_name(x.tag) == "channel"), root)
        feed_title = child_text(container, ("title",)) or feed_title
    # RSS and Atom conventionally return newest-first; bound the initial import
    # to the head so a large archive cannot bury current work under old entries.
    nodes = [x for x in container if local_name(x.tag) in {"item", "entry"}][:maximum]
    feed_key = hashlib.sha256(feed_url.encode()).hexdigest()[:16]
    results = []
    for node in nodes:
        title = child_text(node, ("title",)) or "Untitled feed item"
        link = entry_link(node)
        identity = child_text(node, ("guid", "id")) or link
        body = child_text(node, ("content", "encoded", "description", "summary"))
        if not identity:
            identity = hashlib.sha256((title + "\0" + body).encode()).hexdigest()
        delivery = hashlib.sha256((feed_url + "\0" + identity).encode()).hexdigest()
        text = plain_text(body)
        if link:
            text = (text + "\n\n" if text else "") + link
        text = bounded(text, 65536)
        safe_title = plain_text(title, 512) or "Untitled feed item"
        item: dict[str, Any] = {
            "id": f"feed-{delivery[:32]}", "deliveryID": f"feed:{delivery}",
            "conversationID": f"feed:{feed_key}", "senderID": bounded(feed_url, 256),
            "senderName": plain_text(feed_title, 256) or "Feed", "title": safe_title,
            "body": text or bounded(safe_title, 65536),
        }
        created = iso_date(child_text(node, ("published", "updated", "pubdate", "date")))
        if created:
            item["createdAt"] = created
        results.append(item)
    return results


def fetch(url: str, cfg: dict[str, Any], validators: dict[str, str]) -> tuple[bytes | None, dict[str, str]]:
    headers = {"User-Agent": "Termite-RSS-Channel/1.0", "Accept": "application/atom+xml, application/rss+xml, application/xml, text/xml"}
    if cfg["bearer_token"]:
        headers["Authorization"] = f"Bearer {cfg['bearer_token']}"
    if validators.get("etag"):
        headers["If-None-Match"] = validators["etag"]
    if validators.get("last_modified"):
        headers["If-Modified-Since"] = validators["last_modified"]
    try:
        with HTTP.open(urllib.request.Request(url, headers=headers), timeout=cfg["request_timeout"]) as response:
            data = response.read(MAX_DOWNLOAD + 1)
            if len(data) > MAX_DOWNLOAD:
                raise ValueError("feed exceeds 2 MiB download limit")
            return data, {"etag": response.headers.get("ETag", ""),
                          "last_modified": response.headers.get("Last-Modified", "")}
    except urllib.error.HTTPError as exc:
        if exc.code == 304:
            return None, validators
        raise


def main() -> None:
    cfg = load_config()
    client = TermiteClient()
    client.post("/v1/channels", {
        "id": CHANNEL_ID, "name": cfg["channel_name"], "service": "RSS / Atom",
        "account": cfg["account"], "description": f"Read-only updates from {len(cfg['feed_urls'])} feed(s)",
        "replyCapabilities": [],
    })
    validators: dict[str, dict[str, str]] = {url: {} for url in cfg["feed_urls"]}
    delay = 0
    failures = 0
    while True:
        if delay:
            time.sleep(delay)
        failed = False
        errors: list[str] = []
        for url in cfg["feed_urls"]:
            try:
                data, validators[url] = fetch(url, cfg, validators[url])
                if data:
                    for item in parse_feed(data, url, cfg["max_entries_per_feed"]):
                        client.post(f"/v1/channels/{CHANNEL_ID}/work-items", item)
            except Exception as exc:
                failed = True
                errors.append(f"{safe_url_label(url)}: {type(exc).__name__}")
                print(f"feed {safe_url_label(url)} failed: {exc}", file=sys.stderr, flush=True)
        failures = failures + 1 if failed else 0
        delay = min(cfg["poll_seconds"] * (2 ** min(failures, 5)), 3600)
        if failed:
            report_health(client, "retrying", error="; ".join(errors), retry_in=delay,
                          detail="One or more feeds failed; successful feeds were still imported")
        else:
            report_health(client, "healthy", detail=f"Polled {len(cfg['feed_urls'])} feed(s)")


if __name__ == "__main__":
    main()
