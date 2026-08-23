#!/usr/bin/env python3
"""Credential-free integration smoke tests for first-party Channels.

The suite runs the real package entrypoints as imported modules against a
loopback Termite HTTP probe and controlled provider/source fakes. It never
reads package config.json files, credentials, Keychain, or external URLs.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from email.message import EmailMessage
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock
import urllib.parse


ROOT = Path(__file__).resolve().parent.parent
PLUGINS = ROOT / "plugins"
TOKEN = "channel-integration-token"
CHANNELS = (
    "apple-reminders", "clipboard-inbox", "command-queue", "discord",
    "folder-drop", "git-watch", "github-issues", "imap-mail", "imessage",
    "jira", "linear", "mastodon", "matrix", "ntfy", "rss-feed", "slack",
    "telegram", "webhook-inbox",
)
READ_ONLY = {"apple-reminders", "git-watch", "rss-feed"}
LOCAL_CHANNELS = {
    "apple-reminders", "clipboard-inbox", "command-queue",
    "folder-drop", "git-watch", "imessage",
}
LOCAL_REPLY_CHANNELS = LOCAL_CHANNELS - READ_ONLY
ENV_CLIENTS = {"imap-mail", "mastodon", "matrix", "ntfy", "rss-feed", "webhook-inbox"}
PROVIDER_CLASS_CLIENTS = {"discord", "github-issues", "jira", "linear", "slack", "telegram"}


def fail(message: str) -> None:
    raise AssertionError(message)


def load_channel(name: str):
    path = PLUGINS / name / "channel.py"
    spec = importlib.util.spec_from_file_location("termite_integration_" + name.replace("-", "_"), path)
    if spec is None or spec.loader is None:
        fail(f"{name}: cannot load entrypoint")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TermiteState:
    def __init__(self) -> None:
        self.registrations: list[dict] = []
        self.items: list[dict] = []
        self.queued: dict[str, dict] = {}
        self.acks: list[tuple[str, dict]] = []
        self.attempts: list[str] = []
        self.health: list[tuple[str, dict]] = []
        self.timeline: list[str] = []
        self.auth_failures = 0


class TermiteHandler(BaseHTTPRequestHandler):
    state: TermiteState

    def log_message(self, *_args) -> None:
        pass

    def read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        value = json.loads(self.rfile.read(length) or b"{}")
        if not isinstance(value, dict):
            fail("Termite probe received a non-object request")
        return value

    def send_json(self, value: dict) -> None:
        data = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def check_auth(self) -> None:
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            self.state.auth_failures += 1

    def do_GET(self) -> None:
        self.check_auth()
        if self.path == "/v1/channel-replies":
            self.send_json({"replies": list(self.state.queued.values())})
            return
        self.send_error(404)

    def do_POST(self) -> None:
        self.check_auth()
        body = self.read_body()
        if self.path == "/v1/channels":
            self.state.registrations.append(body)
            self.send_json({"pendingReplies": list(self.state.queued.values())})
            return
        if self.path.startswith("/v1/channels/") and self.path.endswith("/health"):
            channel_id = urllib.parse.unquote(self.path.split("/")[-2])
            self.state.health.append((channel_id, body))
            self.send_json({"ok": True})
            return
        if self.path.endswith("/work-items"):
            self.state.items.append(body)
            self.send_json({"id": body.get("id"), "workItem": body})
            return
        if self.path.endswith("/ack"):
            reply_id = urllib.parse.unquote(self.path.split("/")[-2])
            self.state.acks.append((reply_id, body))
            self.state.timeline.append("ack:" + reply_id)
            self.state.queued.pop(reply_id, None)
            self.send_json({"ok": True})
            return
        if self.path.endswith("/attempt"):
            reply_id = urllib.parse.unquote(self.path.split("/")[-2])
            self.state.attempts.append(reply_id)
            self.state.timeline.append("attempt:" + reply_id)
            self.send_json({"ok": True})
            return
        self.send_error(404)


@contextmanager
def termite_probe():
    state = TermiteState()
    handler = type("BoundTermiteHandler", (TermiteHandler,), {"state": state})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port, state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def termite_client(name: str, module, port: int):
    if name in ENV_CLIENTS:
        with mock.patch.dict(os.environ, {"TERMITE_PORT": str(port), "TERMITE_TOKEN": TOKEN}):
            return module.TermiteClient()
    if name in PROVIDER_CLASS_CLIENTS:
        return module.TermiteClient(str(port), TOKEN)
    return module.TermiteAPI(str(port), TOKEN)


def register(name: str, module, client, can_reply: bool) -> dict:
    if hasattr(client, "register"):
        return client.register("integration")
    body = {
        "id": module.CHANNEL_ID,
        "name": f"{name} integration",
        "service": "Controlled integration",
        "account": "loopback",
        "description": "Credential-free integration probe",
        "replyCapabilities": ["reply"] if can_reply else [],
    }
    return client.request("/v1/channels", body) if hasattr(client, "request") else client.post("/v1/channels", body)


def submit(client, module, item: dict) -> None:
    path = f"/v1/channels/{module.CHANNEL_ID}/work-items"
    if hasattr(client, "request"):
        client.request(path, item)
    else:
        client.post(path, item)


def check_item(name: str, item: dict) -> None:
    for key in ("id", "deliveryID", "conversationID", "senderID", "senderName", "title", "body"):
        if not isinstance(item.get(key), str) or not item[key]:
            fail(f"{name}: normalized item lacks non-empty {key}")
    for key, limit in (("id", 256), ("deliveryID", 512), ("conversationID", 512),
                       ("senderID", 512), ("senderName", 256), ("title", 512), ("body", 65536)):
        if len(item[key].encode()) > limit:
            fail(f"{name}: {key} exceeds {limit} bytes")


@dataclass
class Exercise:
    recover: object | None = None
    sends: list | None = None
    expected_items: int = 1


class FakeSlack:
    def __init__(self, state) -> None: self.state, self.sent = state, []
    def history(self, _channel, _oldest):
        return [{"ts": "2000000000.100", "user": "U1", "text": "Ship safely"}]
    def user_name(self, _user): return "Mira"
    def send(self, reply):
        if reply["id"] not in self.state.attempts: fail("slack: provider send preceded host attempt")
        self.state.timeline.append("provider:" + reply["id"]); self.sent.append(reply)


class FakeTelegram:
    def __init__(self, state) -> None: self.state, self.sent = state, []
    def updates(self, _offset):
        return [{"update_id": 10, "message": {"message_id": 4, "date": 1784368800,
            "chat": {"id": -7, "title": "Ops"}, "from": {"id": 2, "first_name": "Mira"},
            "text": "Check deploy"}}]
    def send(self, reply):
        if reply["id"] not in self.state.attempts: fail("telegram: provider send preceded host attempt")
        self.state.timeline.append("provider:" + reply["id"]); self.sent.append(reply)


class FakeDiscord:
    def __init__(self, state) -> None: self.state, self.sent = state, []
    def messages(self, _channel, _after, _limit):
        return [{"id": "100", "timestamp": "2026-07-18T10:00:00Z",
            "author": {"id": "U1", "username": "Mira"}, "content": "Review release"}]
    def send(self, reply):
        if reply["id"] not in self.state.attempts: fail("discord: provider send preceded host attempt")
        self.state.timeline.append("provider:" + reply["id"]); self.sent.append(reply)


class FakeGitHub:
    def __init__(self, state) -> None: self.state, self.sends = state, []
    def issues(self, _repository, _since):
        return [{"id": 10, "number": 1, "title": "Fix release", "body": "Details",
            "created_at": "2026-07-18T10:00:00Z", "updated_at": "2026-07-18T10:00:00Z",
            "user": {"id": 2, "login": "mira", "type": "User"}}]
    def comments(self, _repository, _since): return []
    def send(self, reply):
        if reply["id"] not in self.state.attempts: fail("github-issues: provider send preceded host attempt")
        self.state.timeline.append("provider:" + reply["id"]); self.sends.append(reply)


class FakeLinear:
    def __init__(self, team: str, state) -> None: self.team, self.state, self.sent = team, state, []
    def issues(self, *_args):
        return [{"id": "i1", "identifier": "ENG-1", "title": "Fix release", "description": "Details",
            "createdAt": "2026-07-18T10:00:00Z", "team": {"id": self.team}, "project": None,
            "creator": {"id": "U1", "name": "Mira"}, "assignee": {"id": "SELF"}}]
    def issue_scope_and_comments(self, _issue):
        return {"team": {"id": self.team}, "project": None, "assignee": {"id": "SELF"},
                "comments": {"nodes": []}}
    def marker(self, reply_id): return "integration-" + reply_id
    def send_comment(self, issue_id, body, reply_id):
        if reply_id not in self.state.attempts: fail("linear: provider send preceded host attempt")
        self.state.timeline.append("provider:" + reply_id); self.sent.append((issue_id, body, reply_id))


class FakeJira:
    def __init__(self, state) -> None: self.state, self.sent = state, []
    def issues(self, _projects, _since):
        return [{"id": "10", "key": "ENG-1", "fields": {"summary": "Fix release",
            "description": {"type": "doc", "content": [{"type": "paragraph",
                "content": [{"type": "text", "text": "Details"}]}]},
            "created": "2026-07-18T10:00:00Z", "reporter": {"accountId": "U1", "displayName": "Mira"}}}]
    def recent_comments(self, _issue, _since):
        return [{"id": "20", "created": "2026-07-18T10:01:00Z",
            "author": {"accountId": "U2", "displayName": "Lee", "accountType": "atlassian"},
            "body": {"type": "doc", "content": [{"type": "paragraph",
                "content": [{"type": "text", "text": "More detail"}]}]}}]
    def send_comment(self, issue, body):
        if not self.state.attempts: fail("jira: provider send preceded host attempt")
        self.state.timeline.append("provider:" + self.state.attempts[-1]); self.sent.append((issue, body))


def provider_class_exercise(name: str, module, client, state: TermiteState) -> Exercise:
    if name == "slack":
        provider = FakeSlack(state); connector = module.SlackConnector(client, provider, "C1", "", 2, 0)
        connector.own_user_id, connector.oldest = "SELF", "0"; connector.poll_once()
        return Exercise(lambda: [connector.deliver(reply) for reply in client.pending_replies()], provider.sent)
    if name == "telegram":
        provider = FakeTelegram(state); connector = module.TelegramConnector(client, provider, {"-7"})
        connector.own_user_id = "SELF"; connector.poll_once()
        return Exercise(lambda: [connector.deliver(reply) for reply in client.pending_replies()], provider.sent)
    if name == "discord":
        provider = FakeDiscord(state); connector = module.DiscordConnector(client, provider, ["7"], "", 2, 25)
        connector.own_user_id = "SELF"; connector.poll_once()
        return Exercise(lambda: [connector.deliver(reply) for reply in client.pending_replies()], provider.sent)
    if name == "github-issues":
        provider = FakeGitHub(state); connector = module.GitHubConnector(client, provider, ["a/b"], "", 10, 86400)
        connector.own_login = "termite-test"; connector.poll_once()
        return Exercise(lambda: [connector.deliver(reply) for reply in client.pending_replies()], provider.sends)
    if name == "linear":
        team = "11111111-1111-1111-1111-111111111111"; provider = FakeLinear(team, state)
        connector = module.LinearConnector(client, provider, [team], [], True, "", 10, 86400)
        connector.viewer_id = "SELF"; connector.poll_once()
        return Exercise(lambda: [connector.deliver(reply) for reply in client.pending_replies()], provider.sent)
    provider = FakeJira(state); connector = module.JiraConnector(client, provider, ["ENG"], "", 30, 86400)
    connector.own_account_id = "SELF"; connector.poll_once()
    return Exercise(lambda: [connector.deliver(reply) for reply in client.pending_replies()], provider.sent, 2)


class ReplyAPI:
    def __init__(self) -> None: self.sent = []
    def reply(self, reply): self.sent.append(reply)
    def publish(self, reply): self.sent.append(reply)


def function_exercise(name: str, module, client, temporary: Path) -> Exercise:
    if name == "matrix":
        sync = {"rooms": {"join": {"!ops:test": {"timeline": {"events": [{
            "type": "m.room.message", "event_id": "$event", "sender": "@mira:test",
            "content": {"msgtype": "m.text", "body": "Check staging"}}]}}}}}
        item = module.work_items(sync, {"!ops:test"}, "@self:test")[0]; submit(client, module, item)
        provider = ReplyAPI(); return Exercise(lambda: module.recover_pending(client, provider), provider.sent)
    if name == "mastodon":
        item = module.work_item({"id": "900", "created_at": "2026-07-18T10:00:00Z",
            "account": {"id": "7", "acct": "mira@example.test", "display_name": "Mira"},
            "status": {"id": "800", "content": "<p>Check staging</p>"}}, "self", "https://social.test")
        submit(client, module, item); provider = ReplyAPI()
        return Exercise(lambda: module.recover_pending(client, provider), provider.sent)
    if name == "imap-mail":
        message = EmailMessage(); message["From"] = "Mira <mira@example.test>"
        message["Subject"] = "Release"; message["Message-ID"] = "<m1@example.test>"
        message.set_content("Please verify staging")
        item = module.work_item(message.as_bytes(), "imap.test", "me", "INBOX", "9", "42")
        submit(client, module, item); sent = []
        module.send_reply = lambda _cfg, reply: sent.append(reply)
        return Exercise(lambda: module.recover_pending(client, {}), sent)
    if name == "webhook-inbox":
        item = module.normalize_event({"deliveryID": "evt-42", "body": "Build it"})
        submit(client, module, item); sent = []
        response = mock.MagicMock(status=204); context = mock.MagicMock(); context.__enter__.return_value = response
        module.HTTP.open = mock.Mock(side_effect=lambda request, timeout=30: (sent.append(request) or context))
        cfg = {"callback_url": "https://callback.test/hook", "callback_bearer_token": ""}
        return Exercise(lambda: module.recover_pending(client, cfg), sent)
    if name == "ntfy":
        item = module.work_item({"id": "m1", "topic": "ops", "message": "Check deploy"},
                                "https://ntfy.test", {"ops"})
        submit(client, module, item); provider = ReplyAPI()
        return Exercise(lambda: module.recover_pending(client, provider), provider.sent)
    fail(f"{name}: no function adapter")


def local_exercise(name: str, module, client, temporary: Path) -> Exercise:
    if name == "folder-drop":
        inbox, outbox = temporary / "inbox", temporary / "outbox"; inbox.mkdir(); outbox.mkdir()
        (inbox / "task.txt").write_text("Review release", encoding="utf-8")
        connector = module.Connector(client, {"inbox": inbox, "outbox": outbox}); connector.scan_once()
        return Exercise(connector.recover, [])
    if name == "clipboard-inbox":
        writes = []
        def write(body): writes.append(body); return module.hashlib.sha256(body.encode()).hexdigest()
        connector = module.Connector(client, {"includeCurrentClipboard": True}, lambda: b"Review clipboard", write)
        connector.poll_once(); return Exercise(connector.recover, writes)
    if name == "command-queue":
        sends = []
        def runner(_argv, input_bytes=b"", **_kwargs):
            if input_bytes: sends.append(json.loads(input_bytes)); return b""
            return b'{"deliveryID":"ticket-42","title":"Review","body":"Check it"}'
        cfg = {"producer": ["/producer"], "consumer": ["/consumer"], "producerTimeoutSeconds": 2,
               "consumerTimeoutSeconds": 3, "maxItemsPerPoll": 16}
        connector = module.Connector(client, cfg, runner); connector.poll_once()
        return Exercise(connector.recover, sends)
    if name == "imessage":
        class Source:
            allowed_chats = set(); allowed_handles = {"friend@example.test": "friend@example.test"}
            def boundary(self): return 1
            def rows(self, *_args, **_kwargs):
                return [(1, "guid-1", "hello", 100, "friend@example.test", "chat", None)]
        sends = []
        cfg = {"includeExistingMessages": True, "maxMessagesPerPoll": 20,
               "allowedHandles": ["friend@example.test"], "allowedChatGUIDs": []}
        connector = module.Connector(client, Source(), cfg, lambda reply, _cfg: sends.append(reply))
        connector.poll_once(); return Exercise(connector.recover, sends)
    if name == "git-watch":
        class Source:
            repository = temporary
            def recent_hashes(self, _count): return ["1" * 40]
            def commit(self, value): return {"hash": value, "author": "Ada", "email": "ada@example.test",
                "date": "2026-07-18T10:00:00Z", "subject": "Ship", "body": "Verified"}
        connector = module.Connector(client, Source(), {"maxCommitsPerPoll": 20, "includeExistingCommits": True})
        connector.poll_once(); return Exercise()
    if name == "apple-reminders":
        class Source:
            def fetch(self, _limit, _offset):
                return ([{"id": "r1", "name": "Review release", "listID": "work"}], False)
        connector = module.Connector(client, Source(), {"maxRemindersPerPoll": 10})
        connector.poll_once(); return Exercise()
    fail(f"{name}: no local adapter")


def make_reply(name: str, module, item: dict) -> dict:
    return {"kind": "channel-reply", "channel": module.CHANNEL_ID, "id": f"{name}-reply-1",
            "conversationID": item["conversationID"], "replyToID": item.get("replyToID"),
            "body": "Approved result"}


def run_one(name: str) -> tuple[int, int, int]:
    module = load_channel(name)
    with termite_probe() as (port, state), tempfile.TemporaryDirectory(prefix=f"termite-{name}-") as directory:
        client = termite_client(name, module, port)
        can_reply = name not in READ_ONLY
        initial = register(name, module, client, can_reply)
        if initial.get("pendingReplies") != []:
            fail(f"{name}: fresh registration unexpectedly recovered replies")
        if name in PROVIDER_CLASS_CLIENTS:
            exercise = provider_class_exercise(name, module, client, state)
        elif name in ENV_CLIENTS and name != "rss-feed":
            exercise = function_exercise(name, module, client, Path(directory))
        elif name == "rss-feed":
            feed = b'<rss><channel><title>Ops</title><item><guid>x1</guid><title>Deploy</title><description>Done</description></item></channel></rss>'
            submit(client, module, module.parse_feed(feed, "https://feed.test/rss", 10)[0]); exercise = Exercise()
        else:
            exercise = local_exercise(name, module, client, Path(directory))
        if len(state.items) != exercise.expected_items:
            fail(f"{name}: expected {exercise.expected_items} inbound item(s), got {len(state.items)}")
        for item in state.items:
            check_item(name, item)
        if state.auth_failures:
            fail(f"{name}: Termite requests omitted the bearer token")
        registration = state.registrations[-1]
        if registration.get("id") != module.CHANNEL_ID:
            fail(f"{name}: registration used the wrong Channel id")
        expected_caps = ["reply"] if can_reply else []
        if registration.get("replyCapabilities") != expected_caps:
            fail(f"{name}: registration reply capabilities do not match integration mode")
        if name in LOCAL_CHANNELS and not any(
            body.get("status") == "healthy" for _, body in state.health
        ):
            fail(f"{name}: successful local poll did not report healthy")
        if can_reply:
            reply = make_reply(name, module, state.items[0]); state.queued[reply["id"]] = reply
            recovered = register(name, module, client, can_reply).get("pendingReplies", [])
            if [item.get("id") for item in recovered] != [reply["id"]]:
                fail(f"{name}: reconnect did not expose its queued reply")
            exercise.recover()
            if len(state.acks) != 1 or state.acks[0][1].get("delivered") is not True:
                fail(f"{name}: approved reply was not acknowledged exactly once")
            if state.queued:
                fail(f"{name}: acknowledged reply remained queued")
            if name in PROVIDER_CLASS_CLIENTS:
                if state.attempts != [reply["id"]]:
                    fail(f"{name}: provider delivery did not record exactly one attempt")
                expected_timeline = ["attempt:" + reply["id"], "provider:" + reply["id"], "ack:" + reply["id"]]
                if state.timeline != expected_timeline:
                    fail(f"{name}: reliability order was {state.timeline}, expected {expected_timeline}")
                if not state.health or state.health[-1][1].get("status") != "healthy":
                    fail(f"{name}: successful poll did not report healthy")
            elif name in LOCAL_REPLY_CHANNELS:
                if state.attempts != [reply["id"]]:
                    fail(f"{name}: local delivery did not record exactly one attempt")
                expected_timeline = ["attempt:" + reply["id"], "ack:" + reply["id"]]
                if state.timeline != expected_timeline:
                    fail(f"{name}: local reliability order was {state.timeline}, expected {expected_timeline}")
                if not state.health or state.health[-1][1].get("status") != "healthy":
                    fail(f"{name}: successful local delivery did not report healthy")
            if name == "folder-drop":
                sends = list((Path(directory) / "outbox").glob("reply-*.json"))
            else:
                sends = exercise.sends or []
            if len(sends) != 1:
                fail(f"{name}: approved reply reached the fake provider {len(sends)} times")
        elif state.acks:
            fail(f"{name}: read-only connector acknowledged an outbound reply")
        return len(state.items), len(state.acks), 1 if can_reply else 0


def main() -> None:
    discovered = {
        path.parent.name for path in PLUGINS.glob("*/manifest.json")
        if path.parent.name != "demo-inbox"
        and "channels" in json.loads(path.read_text(encoding="utf-8")).get("capabilities", [])
    }
    if discovered != set(CHANNELS):
        fail(f"integration inventory drift: expected {sorted(CHANNELS)}, found {sorted(discovered)}")
    totals = [0, 0, 0]
    for name in CHANNELS:
        inbound, acks, replies = run_one(name)
        totals[0] += inbound; totals[1] += acks; totals[2] += replies
        print(f"  ok {name}: inbound={inbound} reply={'yes' if replies else 'read-only'} ack={acks}")
    print(f"integrated {len(CHANNELS)} Channels: {totals[0]} inbound items, "
          f"{totals[2]} approved provider deliveries, {totals[1]} acknowledgements")


if __name__ == "__main__":
    main()
