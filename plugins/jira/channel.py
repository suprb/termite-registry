#!/usr/bin/env python3
"""Strictly scoped Jira Cloud REST connector for Termite Channels."""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


EXTENSION_ID = "dev.termite.jira"
CHANNEL_ID = EXTENSION_ID
KEYCHAIN_SERVICE = "termite.jira"
JIRA_HOST = re.compile(r"^[a-z0-9][a-z0-9-]*\.atlassian\.net$")
PROJECT_KEY = re.compile(r"^[A-Z][A-Z0-9_]{1,63}$")
ISSUE_KEY = re.compile(r"^([A-Z][A-Z0-9_]{1,63})-[1-9][0-9]*$")
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
    encoded = str(text or "").encode("utf-8")
    return str(text or "") if len(encoded) <= limit else encoded[:limit].decode("utf-8", "ignore") + "\n[truncated by connector]"


def load_file_config(path=None):
    path = path or Path(__file__).with_name("config.json")
    if not path.exists(): return {}
    try: value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc: raise ConnectorError(f"cannot read {path.name}: {exc}") from exc
    if not isinstance(value, dict): raise ConnectorError(f"{path.name} must contain a JSON object")
    if value.get("apiToken") and stat.S_IMODE(path.stat().st_mode) & 0o077:
        print(f"warning: chmod 600 {path} because it contains an API token", file=sys.stderr)
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


def validate_base_url(value):
    parsed = urllib.parse.urlsplit(str(value or ""))
    if parsed.scheme != "https" or not parsed.hostname or not JIRA_HOST.fullmatch(parsed.hostname):
        raise ConnectorError("baseUrl must be an HTTPS *.atlassian.net Jira Cloud site")
    if parsed.username or parsed.password or parsed.port not in {None, 443} or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ConnectorError("baseUrl must contain only the Jira Cloud HTTPS origin")
    return f"https://{parsed.hostname}"


def parse_projects(value):
    values = value if isinstance(value, list) else str(value or "").split(",")
    projects = sorted({str(item).strip().upper() for item in values if str(item).strip()})
    if not projects: raise ConnectorError("projectKeys must explicitly allow at least one Jira project")
    invalid = [item for item in projects if not PROJECT_KEY.fullmatch(item)]
    if invalid: raise ConnectorError(f"invalid Jira project key: {invalid[0]}")
    return projects


def adf_text(node):
    if isinstance(node, str): return node
    if isinstance(node, list): return "".join(adf_text(child) for child in node)
    if not isinstance(node, dict): return ""
    kind = node.get("type")
    if kind == "text": return str(node.get("text", ""))
    if kind == "hardBreak": return "\n"
    value = "".join(adf_text(child) for child in node.get("content", []))
    if kind in {"paragraph", "heading", "blockquote", "listItem", "codeBlock"}: value += "\n"
    return value


def adf_document(text):
    lines = str(text).splitlines() or [""]
    return {"type": "doc", "version": 1, "content": [
        {"type": "paragraph", "content": ([{"type": "text", "text": line}] if line else [])}
        for line in lines
    ]}


def jira_time(value):
    try: return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except (TypeError, ValueError): return None


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
        return self.request("/v1/channels", {"id": CHANNEL_ID, "name": "Jira Cloud", "service": "Jira",
            "account": account, "description": "Reviewed issues and comments from allowlisted Jira projects",
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


class JiraClient:
    def __init__(self, base_url, email, api_token, timeout=20):
        self.base_url, self.timeout = validate_base_url(base_url), timeout
        credential = base64.b64encode(f"{email}:{api_token}".encode()).decode()
        self.headers = {"Authorization": f"Basic {credential}", "Accept": "application/json",
            "Content-Type": "application/json", "User-Agent": "TermiteChannel/1.0"}
        self.opener = rejecting_opener()
    def call(self, method, path, body=None, query=None):
        if not path.startswith("/rest/api/3/"): raise ConnectorError("invalid Jira API path")
        url = self.base_url + path
        if query: url += "?" + urllib.parse.urlencode(query)
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(url, data=data, headers=self.headers, method=method)
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                return read_json(response, "Jira")
        except urllib.error.HTTPError as exc:
            detail = "permission or rate limit" if exc.code in {403, 429} else "provider rejected request"
            raise ConnectorError(f"Jira HTTP {exc.code} ({detail})") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            error = f"Jira request failed: {type(exc).__name__}"
            if method == "POST" and path.endswith("/comment"): raise UncertainDeliveryError(error) from exc
            raise ConnectorError(error) from exc
    def identity(self):
        user = self.call("GET", "/rest/api/3/myself")
        return str(user.get("accountId", "")), user.get("displayName") or user.get("emailAddress") or "Jira user"
    def issues(self, projects, since, max_pages=4):
        jql_projects = ", ".join(f'"{key}"' for key in projects)
        jql = f"project in ({jql_projects}) AND updated >= \"{since}\" ORDER BY updated ASC, key ASC"
        found, token = [], None
        for _ in range(max_pages):
            body = {"jql": jql, "maxResults": 50,
                "fields": ["summary", "description", "updated", "created", "reporter", "project"]}
            if token: body["nextPageToken"] = token
            result = self.call("POST", "/rest/api/3/search/jql", body=body)
            found.extend(result.get("issues") or [])
            token = result.get("nextPageToken")
            if not token or result.get("isLast") is True: break
        if token: raise ConnectorError("Jira result exceeded the 200-issue poll bound; narrow project scope or poll more often")
        return found
    def recent_comments(self, issue_key, since, max_pages=4):
        path = f"/rest/api/3/issue/{urllib.parse.quote(issue_key)}/comment"
        found, start_at = [], 0
        for _ in range(max_pages):
            result = self.call("GET", path,
                query={"maxResults": 100, "startAt": start_at, "orderBy": "-created"})
            if not isinstance(result, dict) or not isinstance(result.get("comments", []), list):
                raise ConnectorError("Jira returned an invalid comments response")
            page = result.get("comments") or []
            reached_cutoff = False
            for comment in page:
                created = jira_time(comment.get("created")) if isinstance(comment, dict) else None
                if created is not None and created < since:
                    reached_cutoff = True
                    continue
                found.append(comment)
            start_at += len(page)
            total_value = result.get("total")
            try: has_more = int(total_value) > start_at if total_value is not None else len(page) == 100
            except (TypeError, ValueError): raise ConnectorError("Jira returned an invalid comment total")
            if reached_cutoff or not page or len(page) < 100 or not has_more:
                return found
        raise ConnectorError("Jira comment poll exceeded the 400-comment overlap bound; poll more often")
    def send_comment(self, issue_key, body):
        self.call("POST", f"/rest/api/3/issue/{urllib.parse.quote(issue_key)}/comment",
                  body={"body": adf_document(truncate(body, 63000))})


class JiraConnector:
    def __init__(self, termite, jira, projects, account, poll_seconds, lookback, hints=None):
        self.termite, self.jira, self.projects, self.account = termite, jira, set(projects), account
        self.poll_seconds, self.hints = poll_seconds, hints or {}
        self.since = datetime.now(timezone.utc) - timedelta(seconds=lookback)
        self.own_account_id = ""
        self._delivering, self._lock = set(), threading.Lock()
    def _health(self, status, **fields):
        try: self.termite.report_health(status, **fields)
        except Exception as exc: print(f"Jira health report failed: {exc}", file=sys.stderr)
    def poll_once(self):
        since_jql = self.since.strftime("%Y-%m-%d %H:%M")
        try: issues = self.jira.issues(sorted(self.projects), since_jql)
        except Exception as exc:
            self._health("retrying", error=str(exc), lastErrorAt=iso_now(), detail="Jira provider poll failed")
            raise
        for issue in issues:
            issue_id, key = str(issue.get("id", "")), str(issue.get("key", ""))
            match = ISSUE_KEY.fullmatch(key)
            if not issue_id or not match or match.group(1) not in self.projects: continue
            fields = issue.get("fields") or {}; reporter = fields.get("reporter") or {}
            sender = reporter.get("displayName") or "Jira user"
            item = {"id": f"jira-issue-{issue_id}", "deliveryID": f"jira:issue:{issue_id}",
                "conversationID": key, "replyToID": key, "senderID": str(reporter.get("accountId", "")),
                "senderName": truncate(sender, 256), "title": truncate(f"{key}: {fields.get('summary') or 'Untitled issue'}", 512),
                "body": truncate(adf_text(fields.get("description")).strip() or fields.get("summary") or "(empty issue)"),
                "createdAt": fields.get("created")}
            if self.hints.get(match.group(1)): item["projectHint"] = truncate(self.hints[match.group(1)], 1024)
            self.termite.ingest(item)
            try: comments = self.jira.recent_comments(key, self.since)
            except Exception as exc:
                self._health("retrying", error=str(exc), lastErrorAt=iso_now(), detail="Jira provider poll failed")
                raise
            for comment in comments:
                comment_id = str(comment.get("id", "")); author = comment.get("author") or {}
                if (not comment_id or str(author.get("accountId", "")) == self.own_account_id
                        or str(author.get("accountType", "")).casefold() == "app"):
                    continue
                body = adf_text(comment.get("body")).strip()
                if not body: continue
                commenter = author.get("displayName") or "Jira user"
                comment_item = {"id": f"jira-comment-{comment_id}", "deliveryID": f"jira:comment:{comment_id}",
                    "conversationID": key, "replyToID": key, "senderID": str(author.get("accountId", "")),
                    "senderName": truncate(commenter, 256), "title": truncate(f"{key}: comment from {commenter}", 512),
                    "body": truncate(body), "createdAt": comment.get("created")}
                if self.hints.get(match.group(1)): comment_item["projectHint"] = truncate(self.hints[match.group(1)], 1024)
                self.termite.ingest(comment_item)
        self.since = datetime.now(timezone.utc) - timedelta(minutes=5)
        self._health("healthy", lastSuccessAt=iso_now(), detail="Jira poll completed")
    def deliver(self, reply):
        reply_id, issue_key = str(reply.get("id", "")), str(reply.get("conversationID", ""))
        if not reply_id: return
        with self._lock:
            if reply_id in self._delivering: return
            self._delivering.add(reply_id)
        try:
            try:
                if not self.termite.reply_is_queued(reply_id): return
            except Exception:
                return
            match = ISSUE_KEY.fullmatch(issue_key)
            if not match or match.group(1) not in self.projects:
                self.termite.acknowledge(reply_id, False, "Jira issue is outside this connector's project allowlist"); return
            try: self.termite.begin_reply_attempt(reply_id)
            except Exception: return
            try: self.jira.send_comment(issue_key, reply["body"])
            except UncertainDeliveryError as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="Jira delivery needs verification")
                self.termite.verification_needed(reply_id, str(exc))
            except Exception as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="Jira delivery failed")
                self.termite.acknowledge(reply_id, False, str(exc))
            else:
                self._health("healthy", lastSuccessAt=iso_now(), detail="Jira delivery completed")
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
        try: self.own_account_id, detected = self.jira.identity()
        except Exception as exc:
            self._health("offline", error=str(exc), lastErrorAt=iso_now(), detail="Jira identity failed")
            raise
        registration = self.termite.register(self.account or detected)
        self._health("healthy", lastSuccessAt=iso_now(), detail="Jira identity verified")
        for reply in registration.get("pendingReplies", []): self.deliver(reply)
        threading.Thread(target=self.listen, name="termite-events", daemon=True).start()
        delay = 1
        while True:
            try:
                for reply in self.termite.pending_replies(): self.deliver(reply)
                self.poll_once(); delay = 1; time.sleep(self.poll_seconds)
            except Exception as exc:
                print(f"Jira poll failed: {exc}; retrying", file=sys.stderr)
                time.sleep(delay); delay = min(delay * 2, 120)


def build_connector(config_path=None):
    config = load_file_config(config_path)
    api_token = setting(config, "apiToken", "TERMITE_JIRA_API_TOKEN") or keychain_secret()
    email = setting(config, "email", "TERMITE_JIRA_EMAIL")
    if not api_token: raise ConnectorError("Jira API token missing; use Keychain service termite.jira, config.json, or TERMITE_JIRA_API_TOKEN")
    if not email or "@" not in str(email): raise ConnectorError("Jira account email missing in config.json or TERMITE_JIRA_EMAIL")
    base_url = validate_base_url(setting(config, "baseUrl", "TERMITE_JIRA_BASE_URL"))
    projects = parse_projects(setting(config, "projectKeys", "TERMITE_JIRA_PROJECT_KEYS"))
    try:
        poll = float(setting(config, "pollSeconds", "TERMITE_JIRA_POLL_SECONDS", 60))
        lookback = float(setting(config, "initialLookbackSeconds", "TERMITE_JIRA_INITIAL_LOOKBACK_SECONDS", 86400))
    except (TypeError, ValueError) as exc: raise ConnectorError("pollSeconds and initialLookbackSeconds must be numbers") from exc
    if not 30 <= poll <= 3600: raise ConnectorError("pollSeconds must be between 30 and 3600")
    if not 0 <= lookback <= 2592000: raise ConnectorError("initialLookbackSeconds must be between 0 and 2592000")
    hints = config.get("projectHints", {})
    if not isinstance(hints, dict) or any(key not in projects for key in hints):
        raise ConnectorError("projectHints must map only allowlisted Jira project keys to local paths")
    port, termite_token = os.environ.get("TERMITE_PORT"), os.environ.get("TERMITE_TOKEN")
    if not port or not termite_token: raise ConnectorError("Termite did not provide TERMITE_PORT and TERMITE_TOKEN")
    return JiraConnector(TermiteClient(port, termite_token), JiraClient(base_url, str(email), str(api_token)), projects,
        str(setting(config, "account", "TERMITE_JIRA_ACCOUNT", "")), poll, lookback, hints)


if __name__ == "__main__":
    try: build_connector().run()
    except ConnectorError as exc:
        print(f"Jira Channel: {exc}", file=sys.stderr); raise SystemExit(2)
