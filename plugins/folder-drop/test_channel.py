import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).with_name("channel.py")
SPEC = importlib.util.spec_from_file_location("folder_drop_channel", MODULE_PATH)
channel = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(channel)


class FakeAPI:
    def __init__(self):
        self.timeline, self.health_updates = [], []

    def begin_attempt(self, reply_id):
        self.timeline.append(("attempt", reply_id))

    def ack(self, reply_id, delivered, error=None):
        self.timeline.append(("ack", reply_id, delivered, error))

    def verification_needed(self, reply_id, error):
        self.timeline.append(("verification", reply_id, error))

    def report_health(self, status, **fields):
        self.health_updates.append((status, fields))


class FolderDropTests(unittest.TestCase):
    def test_text_drop_has_stable_content_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "task.txt"
            path.write_text("ship it", encoding="utf-8")
            first = channel.file_to_work_item(path)
            second = channel.file_to_work_item(path)
            self.assertEqual(first["deliveryID"], second["deliveryID"])
            self.assertEqual(first["body"], "ship it")

    def test_changed_content_gets_new_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "task.txt"
            path.write_text("one", encoding="utf-8")
            first = channel.file_to_work_item(path)
            path.write_text("two", encoding="utf-8")
            self.assertNotEqual(first["deliveryID"], channel.file_to_work_item(path)["deliveryID"])

    def test_json_metadata_and_outbox_are_bounded_and_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "task.json"
            source.write_text(json.dumps({"title": "Release", "body": "verify", "conversationID": "r1"}))
            item = channel.file_to_work_item(source)
            self.assertEqual(item["title"], "Release")
            reply = {"id": "reply/1", "conversationID": "r1", "body": "done"}
            target = channel.write_reply(root, reply)
            self.assertEqual(target, channel.write_reply(root, reply))
            self.assertEqual(json.loads(target.read_text())["body"], "done")

    def test_disabled_without_explicit_opt_in(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "disabled"):
                channel.load_config(Path(tmp) / "missing.json", {})

    def test_reply_outbox_directory_must_not_be_a_symlink(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            actual = root / "actual"
            actual.mkdir()
            link = root / "link"
            link.symlink_to(actual, target_is_directory=True)
            with self.assertRaises(OSError):
                channel.write_reply(link, {"id": "r1", "body": "done"})
            self.assertEqual(list(actual.iterdir()), [])

    def test_escaped_large_reply_remains_idempotent_on_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            outbox = Path(tmp)
            reply = {"id": "r-control", "body": "\u0001" * channel.MAX_FILE_BYTES}
            target = channel.write_reply(outbox, reply)
            self.assertGreater(target.stat().st_size, channel.MAX_FILE_BYTES)
            self.assertEqual(channel.write_reply(outbox, reply), target)

    def test_reply_attempt_precedes_durable_outbox_and_ack(self):
        with tempfile.TemporaryDirectory() as tmp:
            api = FakeAPI()
            connector = channel.Connector(api, {"outbox": Path(tmp)})
            connector.deliver({"id": "r1", "channel": channel.CHANNEL_ID, "body": "done"})
            self.assertEqual(api.timeline, [("attempt", "r1"), ("ack", "r1", True, None)])
            self.assertEqual(len(list(Path(tmp).glob("reply-*.json"))), 1)
            self.assertEqual(api.health_updates[-1][0], "healthy")

    def test_uncertain_outbox_failure_requires_verification(self):
        api = FakeAPI()
        connector = channel.Connector(api, {"outbox": Path("/unused")})
        with mock.patch.object(channel, "write_reply", side_effect=OSError("ack lost")):
            connector.deliver({"id": "r2", "channel": channel.CHANNEL_ID, "body": "done"})
        self.assertEqual(api.timeline[0], ("attempt", "r2"))
        self.assertEqual(api.timeline[1][0:2], ("verification", "r2"))
        self.assertNotIn(("ack", "r2", False, None), api.timeline)
        self.assertEqual(api.health_updates[-1][0], "degraded")


if __name__ == "__main__":
    unittest.main()
