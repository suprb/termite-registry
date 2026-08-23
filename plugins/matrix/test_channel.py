import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("matrix_channel", Path(__file__).with_name("channel.py"))
channel = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(channel)


class MatrixTests(unittest.TestCase):
    def test_only_allowlisted_human_messages_are_ingested(self):
        sync = {"next_batch": "s2", "rooms": {"join": {
            "!ok:test": {"timeline": {"events": [
                {"type": "m.room.message", "event_id": "$one", "sender": "@mira:test", "content": {"msgtype": "m.text", "body": "Check staging"}},
                {"type": "m.room.message", "event_id": "$bot", "sender": "@bot:test", "content": {"msgtype": "m.notice", "body": "noise"}},
                {"type": "m.room.message", "event_id": "$edit", "sender": "@mira:test", "content": {"msgtype": "m.text", "body": "edited", "m.relates_to": {"rel_type": "m.replace", "event_id": "$one"}}},
            ]}},
            "!blocked:test": {"timeline": {"events": [
                {"type": "m.room.message", "event_id": "$two", "sender": "@x:test", "content": {"msgtype": "m.text", "body": "blocked"}}
            ]}}
        }}}
        items = channel.work_items(sync, {"!ok:test"}, "@me:test")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["deliveryID"], "$one")
        self.assertEqual(items[0]["conversationID"], "!ok:test")

    def test_state_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory, "matrix.json"))
            channel.save_since(path, "batch-9")
            self.assertEqual(channel.load_since(path), "batch-9")

    def test_remote_http_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "config.json").write_text(json.dumps({
                "homeserver": "http://matrix.test", "access_token": "x",
                "room_ids": ["!x:test"], "own_user_id": "@me:test"
            }))
            with mock.patch.object(channel, "PLUGIN_DIR", Path(directory)):
                with self.assertRaisesRegex(ValueError, "HTTPS"):
                    channel.load_config()

    def test_own_user_is_required_to_prevent_reply_echo(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "config.json").write_text(json.dumps({
                "homeserver": "https://matrix.test", "access_token": "x", "room_ids": ["!x:test"]
            }))
            with mock.patch.object(channel, "PLUGIN_DIR", Path(directory)):
                with self.assertRaisesRegex(ValueError, "own_user_id"):
                    channel.load_config()

    def test_authenticated_redirects_cannot_change_origin(self):
        handler = channel.SameOriginRedirect()
        request = channel.urllib.request.Request("https://matrix.test/_matrix/client/v3/sync")
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

    def test_whoami_requires_a_bounded_authenticated_user(self):
        api = channel.MatrixAPI({"access_token": "x", "homeserver": "https://matrix.test"})
        response = mock.MagicMock()
        response.read.return_value = json.dumps({"user_id": "@me:test"}).encode()
        context = mock.MagicMock()
        context.__enter__.return_value = response
        with mock.patch.object(channel.HTTP, "open", return_value=context):
            self.assertEqual(api.own_user_id(), "@me:test")

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

    def test_attempt_failure_prevents_matrix_send(self):
        client = mock.Mock()
        client.request.side_effect = RuntimeError("attempt rejected")
        api = mock.Mock()
        with self.assertRaisesRegex(RuntimeError, "attempt rejected"):
            channel.deliver(client, api, {"id": "r1"})
        api.reply.assert_not_called()
        client.request.assert_called_once_with("/v1/channel-replies/r1/attempt", {})

    def test_ambiguous_matrix_transport_failure_requires_verification(self):
        client = mock.Mock()
        api = mock.Mock()
        api.reply.side_effect = channel.urllib.error.URLError("reset")
        channel.deliver(client, api, {"id": "r1"})
        self.assertEqual(client.request.call_args_list[0], mock.call("/v1/channel-replies/r1/attempt", {}))
        self.assertEqual(client.request.call_args_list[-1].args[1]["state"], "verification-needed")


if __name__ == "__main__":
    unittest.main()
