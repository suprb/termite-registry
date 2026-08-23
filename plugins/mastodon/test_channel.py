import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("mastodon_channel", Path(__file__).with_name("channel.py"))
channel = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(channel)


class MastodonTests(unittest.TestCase):
    def test_notification_identity_and_text(self):
        notification = {
            "id": "900", "created_at": "2026-01-02T03:04:05Z",
            "account": {"id": "7", "acct": "mira@example.test", "display_name": "<b>Mira</b>"},
            "status": {"id": "800", "content": "<p>Please <strong>check</strong> staging.</p>", "url": "https://example.test/@mira/800"},
        }
        first = channel.work_item(notification)
        again = channel.work_item(notification)
        self.assertIsNotNone(first)
        self.assertIsNotNone(again)
        self.assertEqual(first["deliveryID"], "mastodon-notification:900")
        self.assertEqual(first, again)
        self.assertIn("Please check staging.", first["body"])
        self.assertEqual(first["replyToID"], "800")

    def test_own_account_mentions_are_ignored(self):
        notification = {
            "id": "1", "account": {"id": "self", "acct": "me"},
            "status": {"id": "2", "content": "echo"},
        }
        self.assertIsNone(channel.work_item(notification, "self"))

    def test_notification_identity_is_scoped_to_instance(self):
        notification = {
            "id": "1", "account": {"id": "person", "acct": "mira"},
            "status": {"id": "2", "content": "hello"},
        }
        first = channel.work_item(notification, base_url="https://one.social")
        second = channel.work_item(notification, base_url="https://two.social")
        self.assertNotEqual(first["deliveryID"], second["deliveryID"])

    def test_authenticated_redirects_cannot_change_origin(self):
        handler = channel.SameOriginRedirect()
        request = channel.urllib.request.Request("https://social.test/api/v1/notifications")
        with self.assertRaises(channel.urllib.error.HTTPError):
            handler.redirect_request(request, None, 302, "redirect", {}, "https://attacker.test/steal")

    def test_recovery_only_delivers_owned_queued_replies(self):
        client = mock.Mock()
        client.request.return_value = {"replies": [
            {"id": "ours", "channel": channel.CHANNEL_ID},
            {"id": "other", "channel": "dev.other"},
        ]}
        api = mock.Mock()
        with mock.patch.object(channel, "deliver") as deliver:
            channel.recover_pending(client, api)
        deliver.assert_called_once_with(client, api, {"id": "ours", "channel": channel.CHANNEL_ID})

    def test_notification_pagination_reaches_older_burst_pages(self):
        api = channel.MastodonAPI({"access_token": "x", "max_notifications": 2})
        pages = [
            [{"id": "6"}, {"id": "5"}],
            [{"id": "4"}, {"id": "3"}],
            [{"id": "2"}],
        ]
        with mock.patch.object(api, "notification_page", side_effect=pages) as fetch:
            self.assertEqual(list(api.notification_pages("1")), pages)
        self.assertEqual(fetch.call_args_list, [
            mock.call("1", None), mock.call("1", "5"), mock.call("1", "3")
        ])

    def test_success_ack_failure_is_not_relabelled_provider_failure(self):
        client = mock.Mock()
        def request(path, body):
            if path.endswith("/ack"):
                raise RuntimeError("host temporarily unavailable")
            return {}
        client.request.side_effect = request
        api = mock.Mock()
        with self.assertRaisesRegex(RuntimeError, "host temporarily"):
            channel.deliver(client, api, {"id": "r1"})
        self.assertEqual(client.request.call_args_list[0], mock.call("/v1/channel-replies/r1/attempt", {}))
        self.assertEqual(client.request.call_args_list[1].args[0], f"/v1/channels/{channel.CHANNEL_ID}/health")
        self.assertEqual(client.request.call_args_list[1].args[1]["status"], "healthy")
        self.assertEqual(client.request.call_args_list[2], mock.call("/v1/channel-replies/r1/ack", {"delivered": True}))

    def test_attempt_failure_prevents_mastodon_send(self):
        client = mock.Mock()
        client.request.side_effect = RuntimeError("attempt rejected")
        api = mock.Mock()
        with self.assertRaisesRegex(RuntimeError, "attempt rejected"):
            channel.deliver(client, api, {"id": "r1"})
        api.reply.assert_not_called()
        client.request.assert_called_once_with("/v1/channel-replies/r1/attempt", {})

    def test_ambiguous_mastodon_transport_failure_requires_verification(self):
        client = mock.Mock()
        api = mock.Mock()
        api.reply.side_effect = channel.urllib.error.URLError("reset")
        channel.deliver(client, api, {"id": "r1"})
        self.assertEqual(client.request.call_args_list[0], mock.call("/v1/channel-replies/r1/attempt", {}))
        self.assertEqual(client.request.call_args_list[-1].args[1]["state"], "verification-needed")

    def test_state_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory, "state.json"))
            channel.save_since_id(path, "123")
            self.assertEqual(channel.load_since_id(path), "123")

    def test_remote_http_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "config.json").write_text(json.dumps({"base_url": "http://remote.test", "access_token": "x"}))
            with mock.patch.object(channel, "PLUGIN_DIR", Path(directory)):
                with self.assertRaisesRegex(ValueError, "HTTPS"):
                    channel.load_config()


if __name__ == "__main__":
    unittest.main()
