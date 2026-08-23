from email.message import EmailMessage
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("imap_channel", Path(__file__).with_name("channel.py"))
channel = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(channel)


class MailTests(unittest.TestCase):
    def test_message_becomes_stable_work_item(self):
        message = EmailMessage()
        message["From"] = "Mira <mira@example.test>"
        message["Subject"] = "Release check"
        message["Message-ID"] = "<m1@example.test>"
        message.set_content("Please verify staging.")
        a = channel.work_item(message.as_bytes(), "imap.test", "me", "INBOX", "9", "42")
        b = channel.work_item(message.as_bytes(), "imap.test", "me", "INBOX", "9", "42")
        self.assertEqual(a["deliveryID"], b["deliveryID"])
        self.assertEqual(a["conversationID"], "mira@example.test")
        self.assertEqual(a["replyToID"], "<m1@example.test>")

    def test_attachment_content_is_not_imported(self):
        message = EmailMessage()
        message["From"] = "mira@example.test"
        message.set_content("visible")
        message.add_attachment(b"SECRET-BINARY", maintype="application", subtype="octet-stream", filename="x")
        self.assertEqual(channel.message_body(message), "visible")

    def test_environment_overrides_file(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "config.json").write_text(json.dumps({"imap_host": "old", "username": "me", "password": "x"}))
            with mock.patch.object(channel, "PLUGIN_DIR", Path(directory)), mock.patch.dict(
                os.environ, {"IMAP_HOST": "new"}, clear=False
            ):
                self.assertEqual(channel.load_config()["imap_host"], "new")

    def test_own_sender_is_filtered_to_prevent_reply_echo(self):
        message = EmailMessage()
        message["From"] = "Termite <me@example.test>"
        message.set_content("a sent-message copy")
        item = channel.work_item(message.as_bytes(), "imap.test", "me@example.test", "INBOX", "1", "2")
        self.assertTrue(channel.is_own_message(item, {"username": "me@example.test"}))
        self.assertFalse(channel.is_own_message(item, {"username": "other@example.test"}))

    def test_multibyte_headers_are_byte_bounded(self):
        message = EmailMessage()
        message["From"] = "mira@example.test"
        message["Subject"] = "é" * 1000
        message.set_content("body")
        item = channel.work_item(message.as_bytes(), "imap.test", "me", "INBOX", "1", "3")
        self.assertLessEqual(len(item["title"].encode()), 512)

    def test_recovery_only_delivers_owned_queued_replies(self):
        client = mock.Mock()
        client.request.return_value = {"replies": [
            {"id": "ours", "channel": channel.CHANNEL_ID},
            {"id": "other", "channel": "dev.other"},
        ]}
        with mock.patch.object(channel, "deliver") as deliver:
            channel.recover_pending(client, {})
        deliver.assert_called_once_with(client, {}, {"id": "ours", "channel": channel.CHANNEL_ID})

    def test_success_ack_failure_is_not_relabelled_provider_failure(self):
        client = mock.Mock()
        def request(path, body):
            if path.endswith("/ack"):
                raise RuntimeError("host temporarily unavailable")
            return {}
        client.request.side_effect = request
        with mock.patch.object(channel, "send_reply"):
            with self.assertRaisesRegex(RuntimeError, "host temporarily"):
                channel.deliver(client, {}, {"id": "r1"})
        self.assertEqual(client.request.call_args_list[0], mock.call("/v1/channel-replies/r1/attempt", {}))
        self.assertEqual(client.request.call_args_list[1].args[0], f"/v1/channels/{channel.CHANNEL_ID}/health")
        self.assertEqual(client.request.call_args_list[1].args[1]["status"], "healthy")
        self.assertEqual(client.request.call_args_list[2], mock.call("/v1/channel-replies/r1/ack", {"delivered": True}))

    def test_attempt_failure_prevents_smtp_send(self):
        client = mock.Mock()
        client.request.side_effect = RuntimeError("attempt rejected")
        with mock.patch.object(channel, "send_reply") as provider:
            with self.assertRaisesRegex(RuntimeError, "attempt rejected"):
                channel.deliver(client, {}, {"id": "r1"})
        provider.assert_not_called()
        client.request.assert_called_once_with("/v1/channel-replies/r1/attempt", {})

    def test_ambiguous_smtp_disconnect_requires_verification(self):
        client = mock.Mock()
        with mock.patch.object(
            channel, "send_reply", side_effect=channel.smtplib.SMTPServerDisconnected("reset")
        ):
            channel.deliver(client, {}, {"id": "r1"})
        self.assertEqual(client.request.call_args_list[0], mock.call("/v1/channel-replies/r1/attempt", {}))
        self.assertEqual(client.request.call_args_list[-1].args[1]["state"], "verification-needed")


if __name__ == "__main__":
    unittest.main()
