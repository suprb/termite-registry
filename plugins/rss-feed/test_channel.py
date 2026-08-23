import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("rss_channel", Path(__file__).with_name("channel.py"))
channel = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(channel)


class FeedTests(unittest.TestCase):
    def test_rss_and_atom_have_stable_delivery_ids(self):
        rss = b'''<rss><channel><title>Ops</title><item><guid>x1</guid><title>Deploy</title><description><![CDATA[<b>Done</b>]]></description></item></channel></rss>'''
        atom = b'''<feed xmlns="http://www.w3.org/2005/Atom"><title>News</title><entry><id>a1</id><title>Hello</title><summary>World</summary><link href="https://example.test/a1"/></entry></feed>'''
        a = channel.parse_feed(rss, "https://example.test/rss", 10)[0]
        b = channel.parse_feed(rss, "https://example.test/rss", 10)[0]
        self.assertEqual(a["deliveryID"], b["deliveryID"])
        self.assertEqual(a["body"], "Done")
        self.assertIn("https://example.test/a1", channel.parse_feed(atom, "https://example.test/atom", 10)[0]["body"])

    def test_environment_overrides_file(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "config.json").write_text(json.dumps({"feed_urls": ["https://old.test/feed"]}))
            with mock.patch.object(channel, "PLUGIN_DIR", Path(directory)), mock.patch.dict(
                os.environ, {"RSS_FEED_URLS": "https://new.test/a,https://new.test/b"}, clear=False
            ), mock.patch.object(channel, "_keychain", return_value=""):
                self.assertEqual(len(channel.load_config()["feed_urls"]), 2)

    def test_empty_body_fallback_and_metadata_are_byte_bounded(self):
        title = "é" * 70000
        feed = f"<rss><channel><title>{title}</title><item><guid>x</guid><title>{title}</title></item></channel></rss>".encode()
        item = channel.parse_feed(feed, "https://example.test/feed", 1)[0]
        self.assertLessEqual(len(item["senderName"].encode()), 256)
        self.assertLessEqual(len(item["title"].encode()), 512)
        self.assertLessEqual(len(item["body"].encode()), 65536)

    def test_redirects_cannot_move_feed_credentials_to_another_origin(self):
        handler = channel.SameOriginRedirect()
        request = channel.urllib.request.Request("https://feeds.test/a")
        with self.assertRaises(channel.urllib.error.HTTPError):
            handler.redirect_request(request, None, 302, "redirect", {}, "https://attacker.test/a")

    def test_feed_log_label_drops_signed_query(self):
        label = channel.safe_url_label("https://feeds.test/private?token=do-not-log")
        self.assertEqual(label, "https://feeds.test/private")

    def test_read_only_health_reports_are_bounded(self):
        client = mock.Mock()
        channel.report_health(client, "retrying", error="x" * 5000, retry_in=30,
                              detail="A feed poll will retry")
        path, body = client.post.call_args.args
        self.assertEqual(path, f"/v1/channels/{channel.CHANNEL_ID}/health")
        self.assertEqual(body["status"], "retrying")
        self.assertLessEqual(len(body["error"].encode()), 1024)
        self.assertIn("lastErrorAt", body)
        self.assertIn("nextRetryAt", body)


if __name__ == "__main__":
    unittest.main()
