#!/usr/bin/env python3
"""Scoped Linear GraphQL connector for Termite Channels."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid


EXTENSION_ID = "dev.termite.linear"
CHANNEL_ID = EXTENSION_ID
KEYCHAIN_SERVICE = "termite.linear"
LINEAR_API = "https://api.linear.app/graphql"
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
    if value.get("apiKey") and stat.S_IMODE(path.stat().st_mode) & 0o077:
        print(f"warning: chmod 600 {path} because it contains an API key", file=sys.stderr)
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


def parse_ids(value, name):
    values = value if isinstance(value, list) else str(value or "").split(",")
    result = sorted({str(item).strip() for item in values if str(item).strip()})
    for item in result:
        try: uuid.UUID(item)
        except ValueError as exc: raise ConnectorError(f"{name} must contain Linear model UUIDs; invalid: {item}") from exc
    return result


def parse_bool(value, name):
    if isinstance(value, bool): return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes"}: return True
    if normalized in {"0", "false", "no"}: return False
    raise ConnectorError(f"{name} must be true or false")


def iso_time(value): return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
def iso_now(): return iso_time(datetime.now(timezone.utc))


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
        return self.request("/v1/channels", {"id": CHANNEL_ID, "name": "Linear", "service": "Linear",
            "account": account, "description": "Reviewed issues from allowlisted Linear teams and projects",
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


class LinearClient:
    def __init__(self, api_key, timeout=20): self.api_key, self.timeout, self.opener = api_key, timeout, rejecting_opener()
    def graphql(self, query, variables=None):
        request = urllib.request.Request(LINEAR_API,
            data=json.dumps({"query": query, "variables": variables or {}}).encode(),
            headers={"Authorization": self.api_key, "Content-Type": "application/json",
                     "User-Agent": "TermiteChannel/1.0"}, method="POST")
        try:
            with self.opener.open(request, timeout=self.timeout) as response: result = read_json(response, "Linear")
        except urllib.error.HTTPError as exc:
            detail = "rate limited" if exc.code == 429 else "provider rejected request"
            raise ConnectorError(f"Linear HTTP {exc.code} ({detail})") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            error = f"Linear request failed: {type(exc).__name__}"
            if "mutation Reply" in query: raise UncertainDeliveryError(error) from exc
            raise ConnectorError(error) from exc
        if result.get("errors"):
            error = result["errors"][0]
            code = (error.get("extensions") or {}).get("code")
            raise ConnectorError(f"Linear GraphQL error{f' ({code})' if code else ''}")
        return result.get("data") or {}
    def identity(self):
        viewer = self.graphql("query { viewer { id name email } }").get("viewer") or {}
        return str(viewer.get("id", "")), viewer.get("name") or viewer.get("email") or "Linear user"
    def issues(self, since, team_ids, project_ids, assignee_id, max_pages=4):
        query = """
        query Issues($first: Int!, $after: String, $filter: IssueFilter) {
          issues(first: $first, after: $after, orderBy: updatedAt, filter: $filter) {
            nodes { id identifier title description createdAt updatedAt url
              team { id name } project { id name } creator { id name } assignee { id name } }
            pageInfo { hasNextPage endCursor }
          }
        }"""
        issue_filter = {"updatedAt": {"gte": since}}
        if team_ids: issue_filter["team"] = {"id": {"in": team_ids}}
        if project_ids: issue_filter["project"] = {"id": {"in": project_ids}}
        if assignee_id: issue_filter["assignee"] = {"id": {"eq": assignee_id}}
        found, cursor = [], None
        page = {}
        for _ in range(max_pages):
            connection = (self.graphql(query, {"first": 50, "after": cursor, "filter": issue_filter}).get("issues") or {})
            found.extend(connection.get("nodes") or [])
            page = connection.get("pageInfo") or {}
            if not page.get("hasNextPage") or not page.get("endCursor"): break
            cursor = page["endCursor"]
        if page.get("hasNextPage"):
            raise ConnectorError("Linear poll exceeded the 200-issue bound; narrow team/project scope or poll more often")
        return found
    def issue_scope_and_comments(self, issue_id):
        query = """query Scope($id: String!) { issue(id: $id) {
          id identifier team { id } project { id } assignee { id }
          comments(last: 100) { nodes { body } }
        } }"""
        return self.graphql(query, {"id": issue_id}).get("issue")
    @staticmethod
    def marker(reply_id): return "termite-reply:" + hashlib.sha256(str(reply_id).encode()).hexdigest()[:32]
    def send_comment(self, issue_id, body, reply_id):
        marker = self.marker(reply_id)
        mutation = """mutation Reply($input: CommentCreateInput!) {
          commentCreate(input: $input) { success comment { id } }
        }"""
        result = self.graphql(mutation, {"input": {"issueId": issue_id,
            "body": truncate(body, 63000) + f"\n\n<!-- {marker} -->"}}).get("commentCreate") or {}
        if not result.get("success") or not (result.get("comment") or {}).get("id"):
            raise ConnectorError("Linear did not confirm comment creation")


class LinearConnector:
    def __init__(self, termite, linear, team_ids, project_ids, assigned_only, account, poll_seconds, lookback, hints=None):
        self.termite, self.linear = termite, linear
        self.team_ids, self.project_ids, self.assigned_only = set(team_ids), set(project_ids), assigned_only
        self.account, self.poll_seconds = account, poll_seconds
        self.since = iso_time(datetime.now(timezone.utc) - timedelta(seconds=lookback))
        self.hints, self.viewer_id = hints or {}, ""
        self._delivering, self._lock = set(), threading.Lock()
    def _health(self, status, **fields):
        try: self.termite.report_health(status, **fields)
        except Exception as exc: print(f"Linear health report failed: {exc}", file=sys.stderr)
    def allowed_scope(self, issue):
        team_id = str((issue.get("team") or {}).get("id", ""))
        project_id = str((issue.get("project") or {}).get("id", ""))
        assignee_id = str((issue.get("assignee") or {}).get("id", ""))
        return ((not self.team_ids or team_id in self.team_ids)
                and (not self.project_ids or project_id in self.project_ids)
                and (not self.assigned_only or (self.viewer_id and assignee_id == self.viewer_id)))
    def poll_once(self):
        poll_started_at = datetime.now(timezone.utc)
        try:
            issues = self.linear.issues(self.since, sorted(self.team_ids), sorted(self.project_ids),
                                        self.viewer_id if self.assigned_only else "")
        except Exception as exc:
            self._health("retrying", error=str(exc), lastErrorAt=iso_now(), detail="Linear provider poll failed")
            raise
        for issue in issues:
            if not self.allowed_scope(issue): continue
            issue_id, identifier = str(issue.get("id", "")), str(issue.get("identifier", ""))
            if not issue_id or not identifier: continue
            creator = issue.get("creator") or {}; sender = creator.get("name") or "Linear user"
            project = issue.get("project") or {}; team = issue.get("team") or {}
            body = issue.get("description") or issue.get("title") or "(empty issue)"
            item = {"id": f"linear-issue-{issue_id}", "deliveryID": f"linear:issue:{issue_id}",
                "conversationID": issue_id, "replyToID": identifier, "senderID": str(creator.get("id", "")),
                "senderName": truncate(sender, 256), "title": truncate(f"{identifier}: {issue.get('title') or 'Untitled issue'}", 512),
                "body": truncate(body), "createdAt": issue.get("createdAt")}
            hint = self.hints.get(str(project.get("id", ""))) or self.hints.get(str(team.get("id", "")))
            if hint: item["projectHint"] = truncate(hint, 1024)
            self.termite.ingest(item)
        # Advance only to the beginning of the completed query. An issue updated
        # while pages are being read must remain inside the next overlap window.
        self.since = iso_time(poll_started_at - timedelta(seconds=2))
        self._health("healthy", lastSuccessAt=iso_now(), detail="Linear poll completed")
    def deliver(self, reply):
        reply_id, issue_id = str(reply.get("id", "")), str(reply.get("conversationID", ""))
        if not reply_id or not issue_id: return
        with self._lock:
            if reply_id in self._delivering: return
            self._delivering.add(reply_id)
        try:
            try:
                if not self.termite.reply_is_queued(reply_id): return
            except Exception:
                return
            try:
                issue = self.linear.issue_scope_and_comments(issue_id)
                if not issue or not self.allowed_scope(issue): raise ConnectorError("Linear issue is outside this connector's allowlist")
                marker = self.linear.marker(reply_id)
                comments = ((issue.get("comments") or {}).get("nodes") or [])
                if not any(f"<!-- {marker} -->" in str(comment.get("body", "")) for comment in comments):
                    try: self.termite.begin_reply_attempt(reply_id)
                    except Exception: return
                    self.linear.send_comment(issue_id, reply["body"], reply_id)
            except UncertainDeliveryError as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="Linear delivery needs verification")
                self.termite.verification_needed(reply_id, str(exc))
            except Exception as exc:
                self._health("degraded", error=str(exc), lastErrorAt=iso_now(), detail="Linear delivery failed")
                self.termite.acknowledge(reply_id, False, str(exc))
            else:
                self._health("healthy", lastSuccessAt=iso_now(), detail="Linear delivery completed")
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
        try: self.viewer_id, detected = self.linear.identity()
        except Exception as exc:
            self._health("offline", error=str(exc), lastErrorAt=iso_now(), detail="Linear identity failed")
            raise
        registration = self.termite.register(self.account or detected)
        self._health("healthy", lastSuccessAt=iso_now(), detail="Linear identity verified")
        for reply in registration.get("pendingReplies", []): self.deliver(reply)
        threading.Thread(target=self.listen, name="termite-events", daemon=True).start()
        delay = 1
        while True:
            try:
                for reply in self.termite.pending_replies(): self.deliver(reply)
                self.poll_once(); delay = 1; time.sleep(self.poll_seconds)
            except Exception as exc:
                print(f"Linear poll failed: {exc}; retrying", file=sys.stderr)
                time.sleep(delay); delay = min(delay * 2, 120)


def build_connector(config_path=None):
    config = load_file_config(config_path)
    api_key = setting(config, "apiKey", "TERMITE_LINEAR_API_KEY") or keychain_secret()
    if not api_key: raise ConnectorError("Linear API key missing; use Keychain service termite.linear, config.json, or TERMITE_LINEAR_API_KEY")
    teams = parse_ids(setting(config, "teamIds", "TERMITE_LINEAR_TEAM_IDS"), "teamIds")
    projects = parse_ids(setting(config, "projectIds", "TERMITE_LINEAR_PROJECT_IDS"), "projectIds")
    if not teams and not projects: raise ConnectorError("teamIds or projectIds must explicitly allow at least one Linear scope")
    assigned = parse_bool(setting(config, "assignedToMeOnly", "TERMITE_LINEAR_ASSIGNED_TO_ME_ONLY", True), "assignedToMeOnly")
    try:
        poll = float(setting(config, "pollSeconds", "TERMITE_LINEAR_POLL_SECONDS", 30))
        lookback = float(setting(config, "initialLookbackSeconds", "TERMITE_LINEAR_INITIAL_LOOKBACK_SECONDS", 86400))
    except (TypeError, ValueError) as exc: raise ConnectorError("pollSeconds and initialLookbackSeconds must be numbers") from exc
    if not 10 <= poll <= 3600: raise ConnectorError("pollSeconds must be between 10 and 3600")
    if not 0 <= lookback <= 2592000: raise ConnectorError("initialLookbackSeconds must be between 0 and 2592000")
    hints = config.get("projectHints", {})
    if not isinstance(hints, dict) or any(key not in set(teams + projects) for key in hints):
        raise ConnectorError("projectHints must map only allowlisted team/project UUIDs to local paths")
    port, termite_token = os.environ.get("TERMITE_PORT"), os.environ.get("TERMITE_TOKEN")
    if not port or not termite_token: raise ConnectorError("Termite did not provide TERMITE_PORT and TERMITE_TOKEN")
    return LinearConnector(TermiteClient(port, termite_token), LinearClient(str(api_key)), teams, projects, assigned,
        str(setting(config, "account", "TERMITE_LINEAR_ACCOUNT", "")), poll, lookback, hints)


if __name__ == "__main__":
    try: build_connector().run()
    except ConnectorError as exc:
        print(f"Linear Channel: {exc}", file=sys.stderr); raise SystemExit(2)
