#!/usr/bin/env python3
"""Allowlisted GitHub Issues connector for Termite Channels."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
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


EXTENSION_ID = "dev.termite.github-issues"
CHANNEL_ID = EXTENSION_ID
KEYCHAIN_SERVICE = "termite.github-issues"
GITHUB_API = "https://api.github.com"
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
MAX_HTTP_BYTES = 8 * 1024 * 1024
MAX_SSE_LINE_BYTES = 256 * 1024
HEALTH_STATUSES = {"healthy", "degraded", "retrying", "offline"}
HEALTH_FIELD_BYTES = {"lastSuccessAt": 128, "lastErrorAt": 128, "error": 1024,
                      "nextRetryAt": 128, "detail": 2048}


class ConnectorError(RuntimeError): pass
class UncertainDeliveryError(ConnectorError): pass


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
    if value.get("token") and stat.S_IMODE(path.stat().st_mode) & 0o077:
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


def parse_repositories(value):
    values = value if isinstance(value, list) else str(value or "").split(",")
    repositories = sorted({str(item).strip() for item in values if str(item).strip()})
    if not repositories: raise ConnectorError("repositories must explicitly allow at least one owner/repository")
    invalid = [item for item in repositories if not REPOSITORY.fullmatch(item)]
    if invalid: raise ConnectorError(f"invalid GitHub repository: {invalid[0]}")
    return repositories


def parse_bool(value, name):
    if isinstance(value, bool): return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes"}: return True
    if normalized in {"0", "false", "no"}: return False
    raise ConnectorError(f"{name} must be true or false")


def parse_time(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def iso_time(value):
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
def iso_now(): return iso_time(datetime.now(timezone.utc))


def ignored_actor(user, own_login=""):
    login = str((user or {}).get("login", ""))
    return ((own_login and login.casefold() == str(own_login).casefold())
            or str((user or {}).get("type", "")).casefold() == "bot"
            or login.casefold().endswith("[bot]"))


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
        return self.request("/v1/channels", {"id": CHANNEL_ID, "name": "GitHub Issues", "service": "GitHub",
            "account": account, "description": "Reviewed issues and comments from allowlisted repositories",
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


class GitHubClient:
    def __init__(self, token, timeout=20): self.token, self.timeout, self.opener = token, timeout, rejecting_opener()
    def call(self, method, path, body=None, query=None, include_headers=False):
        if not path.startswith("/"): raise ConnectorError("invalid GitHub API path")
        url = GITHUB_API + path
        if query: url += "?" + urllib.parse.urlencode(query)
        data = None if body is None else json.dumps(body).encode()
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "TermiteChannel/1.0",
            "Content-Type": "application/json"}
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                result = read_json(response, "GitHub")
                return (result, dict(response.headers)) if include_headers else result
        except urllib.error.HTTPError as exc:
            detail = "permission or rate limit" if exc.code in {403, 429} else "provider rejected request"
            raise ConnectorError(f"GitHub HTTP {exc.code} ({detail})") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            error = f"GitHub request failed: {type(exc).__name__}"
            if method == "POST" and path.endswith("/comments"): raise UncertainDeliveryError(error) from exc
            raise ConnectorError(error) from exc
    def identity(self):
        user = self.call("GET", "/user")
        return str(user.get("id", "")), user.get("login") or "GitHub user"
    def _repo_path(self, repository):
        if not REPOSITORY.fullmatch(repository): raise ConnectorError("reply targeted a repository outside the configured syntax")
        owner, name = repository.split("/", 1)
        return f"/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(name)}"
    def issues(self, repository, since, max_pages=4):
        found = []
        result = []
        for page in range(1, max_pages + 1):
            result = self.call("GET", self._repo_path(repository) + "/issues", query={"state": "open",
                "sort": "updated", "direction": "asc", "since": since, "per_page": 100, "page": page})
            if not isinstance(result, list): raise ConnectorError("GitHub returned an invalid issues response")
            found.extend(result)
            if len(result) < 100: break
        if len(result) == 100: raise ConnectorError("GitHub issue poll exceeded the 400-item bound; narrow repositories or poll more often")
        return found
    def comments(self, repository, since, max_pages=4):
        found = []
        result = []
        for page in range(1, max_pages + 1):
            result = self.call("GET", self._repo_path(repository) + "/issues/comments", query={
                "sort": "updated", "direction": "asc", "since": since, "per_page": 100, "page": page})
            if not isinstance(result, list): raise ConnectorError("GitHub returned an invalid comments response")
            found.extend(result)
            if len(result) < 100: break
        if len(result) == 100: raise ConnectorError("GitHub comment poll exceeded the 400-item bound; narrow repositories or poll more often")
        return found
    @staticmethod
    def marker(reply_id): return "termite-reply:" + hashlib.sha256(str(reply_id).encode()).hexdigest()[:32]
    def recent_comment_bodies(self, repository, issue_number):
        path = self._repo_path(repository) + f"/issues/{int(issue_number)}/comments"
        first, headers = self.call("GET", path, query={"per_page": 100, "page": 1}, include_headers=True)
        link = next((value for key, value in headers.items() if key.lower() == "link"), "")
        pages = [1]
        matches = re.findall(r"[?&]page=(\d+)>; rel=\"last\"", link)
        if matches:
            last = int(matches[-1]); pages = list(range(max(1, last - 2), last + 1))
        bodies = []
        for page in pages:
            result = first if page == 1 else self.call("GET", path, query={"per_page": 100, "page": page})
            bodies.extend(str(item.get("body", "")) for item in result if isinstance(item, dict))
        return bodies
    def send(self, reply):
        repository, issue_number = str(reply["conversationID"]), str(reply.get("replyToID", ""))
        marker = self.marker(reply["id"])
        if any(f"<!-- {marker} -->" in body for body in self.recent_comment_bodies(repository, issue_number)):
            return
        body = truncate(reply["body"], 63000) + f"\n\n<!-- {marker} -->"
        self.call("POST", self._repo_path(repository) + f"/issues/{int(issue_number)}/comments", body={"body": body})


class GitHubConnector:
    def __init__(self, termite, github, repositories, account, poll_seconds, lookback,
                 include_issues=True, include_comments=True, project_hints=None):
        self.termite, self.github, self.repositories, self.account = termite, github, repositories, account
        self.poll_seconds, self.include_issues, self.include_comments = poll_seconds, include_issues, include_comments
        start = datetime.now(timezone.utc) - timedelta(seconds=lookback)
        self.since = {repo: iso_time(start) for repo in repositories}
        self.project_hints = project_hints or {}; self.own_login = ""
        self._delivering, self._lock = set(), threading.Lock()
    def _health(self, status, **fields):
        try: self.termite.report_health(status, **fields)
        except Exception as exc: print(f"GitHub health report failed: {exc}", file=sys.stderr)
    def base_item(self, repository, number, sender, sender_id, created_at):
        item = {"conversationID": repository, "replyToID": str(number), "senderID": str(sender_id or ""),
                "senderName": truncate(sender, 256), "createdAt": created_at}
        hint = self.project_hints.get(repository)
        if hint: item["projectHint"] = truncate(hint, 1024)
        return item
    def poll_once(self):
        for repository in self.repositories:
            newest = parse_time(self.since[repository])
            if self.include_issues:
                try: issues = self.github.issues(repository, self.since[repository])
                except Exception as exc:
                    self._health("retrying", error=str(exc), lastErrorAt=iso_now(), detail="GitHub provider poll failed")
                    raise
                for issue in issues:
                    if "pull_request" in issue: continue
                    updated = issue.get("updated_at")
                    if updated: newest = max(newest, parse_time(updated))
                    user = issue.get("user") or {}; sender = user.get("login") or "GitHub user"
                    if ignored_actor(user, self.own_login): continue
                    number, database_id = issue.get("number"), issue.get("id")
                    if number is None or database_id is None: continue
                    item = self.base_item(repository, number, sender, user.get("id"), issue.get("created_at"))
                    item.update({"id": f"github-issue-{database_id}", "deliveryID": f"github:issue:{database_id}",
                        "title": truncate(f"{repository} #{number}: {issue.get('title') or 'Untitled issue'}", 512),
                        "body": truncate(issue.get("body") or issue.get("title") or "(empty issue)")})
                    self.termite.ingest(item)
            if self.include_comments:
                try: comments = self.github.comments(repository, self.since[repository])
                except Exception as exc:
                    self._health("retrying", error=str(exc), lastErrorAt=iso_now(), detail="GitHub provider poll failed")
                    raise
                for comment in comments:
                    updated = comment.get("updated_at")
                    if updated: newest = max(newest, parse_time(updated))
                    user = comment.get("user") or {}; sender = user.get("login") or "GitHub user"
                    if ignored_actor(user, self.own_login): continue
                    match = re.search(r"/issues/(\d+)$", str(comment.get("issue_url", "")))
                    database_id = comment.get("id")
                    if not match or database_id is None: continue
                    number = match.group(1); item = self.base_item(repository, number, sender, user.get("id"), comment.get("created_at"))
                    item.update({"id": f"github-comment-{database_id}", "deliveryID": f"github:comment:{database_id}",
                        "title": truncate(f"{repository} #{number}: comment from {sender}", 512),
                        "body": truncate(comment.get("body") or "(empty comment)")})
                    self.termite.ingest(item)
            self.since[repository] = iso_time(newest - timedelta(seconds=2))
        self._health("healthy", lastSuccessAt=iso_now(), detail="GitHub poll completed")
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
            if reply.get("conversationID") not in self.repositories:
                self.termite.acknowledge(reply_id, False, "repository is not in this connector's allowlist"); return
            try: self.termite.begin_reply_attempt(reply_id)
            except Exception: return
            try: self.github.send(reply)
            except UncertainDeliveryError as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="GitHub delivery needs verification")
                self.termite.verification_needed(reply_id, str(exc))
            except Exception as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="GitHub delivery failed")
                self.termite.acknowledge(reply_id, False, str(exc))
            else:
                self._health("healthy", lastSuccessAt=iso_now(), detail="GitHub delivery completed")
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
        try: _, detected = self.github.identity(); self.own_login = detected
        except Exception as exc:
            self._health("offline", error=str(exc), lastErrorAt=iso_now(), detail="GitHub identity failed")
            raise
        registration = self.termite.register(self.account or detected)
        self._health("healthy", lastSuccessAt=iso_now(), detail="GitHub identity verified")
        for reply in registration.get("pendingReplies", []): self.deliver(reply)
        threading.Thread(target=self.listen, name="termite-events", daemon=True).start()
        delay = 1
        while True:
            try:
                for reply in self.termite.pending_replies(): self.deliver(reply)
                self.poll_once(); delay = 1; time.sleep(self.poll_seconds)
            except Exception as exc:
                print(f"GitHub poll failed: {exc}; retrying", file=sys.stderr)
                time.sleep(delay); delay = min(delay * 2, 120)


def build_connector(config_path=None):
    config = load_file_config(config_path)
    token = setting(config, "token", "TERMITE_GITHUB_TOKEN") or keychain_secret()
    if not token: raise ConnectorError("GitHub token missing; use Keychain service termite.github-issues, config.json, or TERMITE_GITHUB_TOKEN")
    repositories = parse_repositories(setting(config, "repositories", "TERMITE_GITHUB_REPOSITORIES"))
    try:
        poll = float(setting(config, "pollSeconds", "TERMITE_GITHUB_POLL_SECONDS", 30))
        lookback = float(setting(config, "initialLookbackSeconds", "TERMITE_GITHUB_INITIAL_LOOKBACK_SECONDS", 86400))
    except (TypeError, ValueError) as exc: raise ConnectorError("pollSeconds and initialLookbackSeconds must be numbers") from exc
    if not 10 <= poll <= 3600: raise ConnectorError("pollSeconds must be between 10 and 3600")
    if not 0 <= lookback <= 2592000: raise ConnectorError("initialLookbackSeconds must be between 0 and 2592000")
    include_issues = parse_bool(setting(config, "includeIssues", "TERMITE_GITHUB_INCLUDE_ISSUES", True), "includeIssues")
    include_comments = parse_bool(setting(config, "includeComments", "TERMITE_GITHUB_INCLUDE_COMMENTS", True), "includeComments")
    if not include_issues and not include_comments: raise ConnectorError("includeIssues and includeComments cannot both be false")
    hints = config.get("projectHints", {})
    if not isinstance(hints, dict) or any(key not in repositories for key in hints):
        raise ConnectorError("projectHints must map only allowlisted repositories to local paths")
    port, termite_token = os.environ.get("TERMITE_PORT"), os.environ.get("TERMITE_TOKEN")
    if not port or not termite_token: raise ConnectorError("Termite did not provide TERMITE_PORT and TERMITE_TOKEN")
    return GitHubConnector(TermiteClient(port, termite_token), GitHubClient(str(token)), repositories,
        str(setting(config, "account", "TERMITE_GITHUB_ACCOUNT", "")), poll, lookback,
        include_issues, include_comments, hints)


if __name__ == "__main__":
    try: build_connector().run()
    except ConnectorError as exc:
        print(f"GitHub Issues Channel: {exc}", file=sys.stderr); raise SystemExit(2)
