import io
import unittest
from unittest import mock
import urllib.error
import channel


T1 = "11111111-1111-1111-1111-111111111111"
P1 = "22222222-2222-2222-2222-222222222222"


class FakeTermite:
    def __init__(self): self.items, self.acks, self.queued, self.attempts, self.health = [], [], None, [], []
    def ingest(self, item): self.items.append(item)
    def reply_is_queued(self, reply_id): return self.queued is None or reply_id in self.queued
    def acknowledge(self, *args):
        self.acks.append(args)
        if self.queued is not None: self.queued.discard(args[0])
    def begin_reply_attempt(self, reply_id): self.attempts.append(reply_id)
    def verification_needed(self, reply_id, error): self.acks.append((reply_id, "verification-needed", error))
    def report_health(self, status, **fields): self.health.append((status, fields))
class FakeLinear:
    def __init__(self, issues=None, scoped=None, error=None): self.value, self.scoped, self.error, self.sent = issues or [], scoped, error, []
    def issues(self, *args): return self.value
    def issue_scope_and_comments(self, issue_id): return self.scoped
    def marker(self, reply_id): return "termite-reply:marker"
    def send_comment(self, issue_id, body, reply_id):
        if self.error: raise channel.ConnectorError(self.error)
        self.sent.append((issue_id, body, reply_id))


class LinearTests(unittest.TestCase):
    def test_oversized_http_response_is_rejected(self):
        with mock.patch.object(channel, "MAX_HTTP_BYTES", 4):
            with self.assertRaises(channel.ConnectorError): channel.read_json(io.BytesIO(b"12345"), "test")

    def test_scope_ids_are_explicit_uuids(self):
        with self.assertRaises(channel.ConnectorError): channel.parse_ids(["ENG"], "teamIds")
        self.assertEqual(channel.parse_ids(T1, "teamIds"), [T1])
    def test_scoped_issue_becomes_work(self):
        issue = {"id": "i1", "identifier": "ENG-7", "title": "Fix", "description": "Details",
            "createdAt": "2026-01-01T00:00:00Z", "team": {"id": T1}, "project": {"id": P1},
            "creator": {"id": "u1", "name": "Mira"}}
        termite = FakeTermite(); connector = channel.LinearConnector(termite, FakeLinear([issue]), [T1], [P1], False, "", 30, 10)
        connector.poll_once()
        self.assertEqual(len(termite.items), 1)
        self.assertEqual(termite.items[0]["deliveryID"], "linear:issue:i1")
        self.assertEqual(termite.health[-1][0], "healthy")
    def test_provider_poll_failure_reports_retrying(self):
        termite = FakeTermite(); linear = FakeLinear(); linear.issues = mock.Mock(side_effect=channel.ConnectorError("offline"))
        connector = channel.LinearConnector(termite, linear, [T1], [P1], False, "", 30, 10)
        with self.assertRaises(channel.ConnectorError): connector.poll_once()
        self.assertEqual(termite.health[-1][0], "retrying")
    def test_event_stream_failure_does_not_change_provider_health(self):
        termite = FakeTermite(); termite.events = mock.Mock(side_effect=channel.ConnectorError("Termite unavailable"))
        connector = channel.LinearConnector(termite, FakeLinear(), [T1], [P1], False, "", 30, 10)
        with mock.patch.object(channel.time, "sleep", side_effect=RuntimeError("stop")):
            with self.assertRaisesRegex(RuntimeError, "stop"): connector.listen()
        self.assertEqual(termite.health, [])
    def test_outbound_scope_and_existing_marker(self):
        scoped = {"team": {"id": T1}, "project": {"id": P1},
            "comments": {"nodes": [{"body": "<!-- termite-reply:marker -->"}]}}
        termite = FakeTermite(); linear = FakeLinear(scoped=scoped)
        connector = channel.LinearConnector(termite, linear, [T1], [P1], False, "", 30, 10)
        connector.deliver({"id": "r1", "conversationID": "i1", "body": "done"})
        self.assertFalse(linear.sent); self.assertEqual(termite.acks, [("r1", True)])
    def test_assignee_scope_is_revalidated_before_reply(self):
        scoped = {"team": {"id": T1}, "project": {"id": P1}, "assignee": {"id": "someone-else"},
            "comments": {"nodes": []}}
        termite = FakeTermite(); linear = FakeLinear(scoped=scoped)
        connector = channel.LinearConnector(termite, linear, [T1], [P1], True, "", 30, 10)
        connector.viewer_id = "viewer"
        connector.deliver({"id": "r1", "conversationID": "i1", "body": "done"})
        self.assertFalse(linear.sent)
        self.assertEqual(termite.acks[0][1], False)
    def test_outbound_failure_is_acknowledged(self):
        scoped = {"team": {"id": T1}, "project": {"id": P1}, "comments": {"nodes": []}}
        termite = FakeTermite(); connector = channel.LinearConnector(termite, FakeLinear(scoped=scoped, error="denied"), [T1], [P1], False, "", 30, 10)
        connector.deliver({"id": "r2", "conversationID": "i1", "body": "done"})
        self.assertEqual(termite.attempts, ["r2"])
        self.assertEqual(termite.acks, [("r2", False, "denied")])
        self.assertEqual(termite.health[-1][0], "degraded")
    def test_uncertain_delivery_requires_verification(self):
        scoped = {"team": {"id": T1}, "project": {"id": P1}, "comments": {"nodes": []}}
        termite = FakeTermite(); linear = FakeLinear(scoped=scoped)
        linear.send_comment = mock.Mock(side_effect=channel.UncertainDeliveryError("timeout after send"))
        connector = channel.LinearConnector(termite, linear, [T1], [P1], False, "", 30, 10)
        connector.deliver({"id": "r1", "conversationID": "i1", "body": "done"})
        self.assertEqual(termite.attempts, ["r1"])
        self.assertEqual(termite.acks, [("r1", "verification-needed", "timeout after send")])
        self.assertEqual(termite.health[-1][0], "degraded")
    def test_send_network_failure_is_uncertain(self):
        linear = channel.LinearClient("secret")
        linear.opener.open = mock.Mock(side_effect=urllib.error.URLError("timeout"))
        with self.assertRaises(channel.UncertainDeliveryError):
            linear.graphql("mutation Reply { commentCreate(input: {}) { success } }")
    def test_stale_recovery_copy_is_not_acked_twice(self):
        scoped = {"team": {"id": T1}, "project": {"id": P1}, "comments": {"nodes": []}}
        termite = FakeTermite(); termite.queued = {"r1"}; linear = FakeLinear(scoped=scoped)
        connector = channel.LinearConnector(termite, linear, [T1], [P1], False, "", 30, 10)
        reply = {"id": "r1", "conversationID": "i1", "body": "done"}
        connector.deliver(reply); connector.deliver(reply)
        self.assertEqual(len(linear.sent), 1)
        self.assertEqual(termite.attempts, ["r1"])
        self.assertEqual(termite.acks, [("r1", True)])


if __name__ == "__main__": unittest.main()
