import io
import unittest
from unittest import mock
import urllib.error
import channel


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
class FakeDiscord:
    def __init__(self, messages=None, error=None): self.value, self.error = messages or [], error
    def messages(self, channel_id, after, initial_limit): return self.value
    def send(self, reply):
        if self.error: raise channel.ConnectorError(self.error)


class DiscordTests(unittest.TestCase):
    def test_oversized_http_response_is_rejected(self):
        with mock.patch.object(channel, "MAX_HTTP_BYTES", 4):
            with self.assertRaises(channel.ConnectorError): channel.read_json(io.BytesIO(b"12345"), "test")

    def test_requires_numeric_allowlist(self):
        with self.assertRaises(channel.ConnectorError): channel.parse_channel_ids("")
        with self.assertRaises(channel.ConnectorError): channel.parse_channel_ids("general")
        self.assertEqual(channel.parse_channel_ids("20,10"), ["10", "20"])
    def test_rate_limit_error_body_is_bounded(self):
        error = urllib.error.HTTPError("https://discord.com", 429, "rate limited", {}, io.BytesIO(b"12345"))
        opener = mock.Mock(); opener.open.side_effect = error
        client = channel.DiscordClient("secret"); client.opener = opener
        with mock.patch.object(channel, "MAX_HTTP_BYTES", 4):
            with mock.patch.object(channel.time, "sleep"):
                with self.assertRaises(channel.ConnectorError): client.call("GET", "/users/@me")
    def test_human_text_becomes_work(self):
        messages = [
            {"id": "100", "channel_id": "7", "timestamp": "2026-01-01T00:00:00Z",
             "author": {"id": "2", "username": "mira"}, "content": "Review release"},
            {"id": "101", "author": {"id": "3", "bot": True}, "content": "ignore"},
        ]
        termite = FakeTermite(); connector = channel.DiscordConnector(termite, FakeDiscord(messages), ["7"], "", 5, 25)
        connector.poll_once()
        self.assertEqual(len(termite.items), 1)
        self.assertEqual(termite.items[0]["deliveryID"], "discord:7:100")
        self.assertEqual(connector.after["7"], "101")
        self.assertEqual(termite.health[-1][0], "healthy")
    def test_provider_poll_failure_reports_retrying(self):
        termite = FakeTermite(); discord = FakeDiscord(); discord.messages = mock.Mock(side_effect=channel.ConnectorError("offline"))
        connector = channel.DiscordConnector(termite, discord, ["7"], "", 5, 25)
        with self.assertRaises(channel.ConnectorError): connector.poll_once()
        self.assertEqual(termite.health[-1][0], "retrying")
    def test_event_stream_failure_does_not_change_provider_health(self):
        termite = FakeTermite(); termite.events = mock.Mock(side_effect=channel.ConnectorError("Termite unavailable"))
        connector = channel.DiscordConnector(termite, FakeDiscord(), ["7"], "", 5, 25)
        with mock.patch.object(channel.time, "sleep", side_effect=RuntimeError("stop")):
            with self.assertRaisesRegex(RuntimeError, "stop"): connector.listen()
        self.assertEqual(termite.health, [])
    def test_delivery_failure_is_acknowledged(self):
        termite = FakeTermite(); connector = channel.DiscordConnector(termite, FakeDiscord(error="forbidden"), ["7"], "", 5, 25)
        connector.deliver({"id": "r1", "conversationID": "7", "body": "done"})
        self.assertEqual(termite.attempts, ["r1"])
        self.assertEqual(termite.acks, [("r1", False, "forbidden")])
        self.assertEqual(termite.health[-1][0], "degraded")
    def test_uncertain_delivery_requires_verification(self):
        termite = FakeTermite(); discord = FakeDiscord()
        discord.send = mock.Mock(side_effect=channel.UncertainDeliveryError("timeout after send"))
        connector = channel.DiscordConnector(termite, discord, ["7"], "", 5, 25)
        connector.deliver({"id": "r1", "conversationID": "7", "body": "done"})
        self.assertEqual(termite.attempts, ["r1"])
        self.assertEqual(termite.acks, [("r1", "verification-needed", "timeout after send")])
        self.assertEqual(termite.health[-1][0], "degraded")
    def test_send_network_failure_is_uncertain(self):
        discord = channel.DiscordClient("secret")
        discord.opener.open = mock.Mock(side_effect=urllib.error.URLError("timeout"))
        with self.assertRaises(channel.UncertainDeliveryError):
            discord.call("POST", "/channels/7/messages", body={"content": "done"})
    def test_stale_recovery_copy_is_not_acked_twice(self):
        termite = FakeTermite(); termite.queued = {"r1"}
        connector = channel.DiscordConnector(termite, FakeDiscord(), ["7"], "", 5, 25)
        reply = {"id": "r1", "conversationID": "7", "body": "done"}
        connector.deliver(reply); connector.deliver(reply)
        self.assertEqual(termite.attempts, ["r1"])
        self.assertEqual(termite.acks, [("r1", True)])


if __name__ == "__main__": unittest.main()
