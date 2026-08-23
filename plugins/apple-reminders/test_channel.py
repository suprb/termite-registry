import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("reminders_channel", Path(__file__).with_name("channel.py"))
channel = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(channel)


class FakeAPI:
    def __init__(self):
        self.items, self.health = [], []

    def request(self, path, body=None):
        if body:
            self.items.append(body)
        return {}

    def report_health(self, status, **fields):
        self.health.append((status, fields))


class ReminderTests(unittest.TestCase):
    def test_requires_opt_in_and_allowlisted_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            with self.assertRaisesRegex(ValueError, "disabled"):
                channel.load_config(path, {})
            path.write_text('{"enabled":true,"allowedListNames":[],"allowedListIDs":[]}')
            with self.assertRaisesRegex(ValueError, "allowlisted"):
                channel.load_config(path, {})

    def test_jxa_receives_allowlist_as_arguments(self):
        calls = []

        def runner(argv, **kwargs):
            calls.append((argv, kwargs))
            return b'{"items":[],"more":false}'

        dangerous = 'Work\"); Application("Finder").quit(); //'
        source = channel.ReminderSource([dangerous], [], runner)
        self.assertEqual(source.fetch(10, 0), ([], False))
        argv = calls[0][0]
        self.assertEqual(argv[:5], ["/usr/bin/osascript", "-l", "JavaScript", "-e", channel.READ_SCRIPT])
        self.assertEqual(json.loads(argv[5]), [dangerous])
        self.assertNotIn(dangerous, channel.READ_SCRIPT)

    def test_stable_reminder_identity_and_bounds(self):
        value = {"id": "x-apple-reminder://123", "name": "Review release",
                 "body": "x" * 70000, "listID": "list-1", "listName": "Work"}
        first = channel.reminder_to_work_item(value)
        second = channel.reminder_to_work_item(value)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["deliveryID"], "reminder:x-apple-reminder://123")
        self.assertLessEqual(len(first["body"].encode()), channel.MAX_BODY_BYTES)

    def test_poll_pages_and_deduplicates(self):
        class Source:
            def __init__(self):
                self.calls = []

            def fetch(self, limit, offset):
                self.calls.append(offset)
                value = {"id": "r1", "name": "One", "listID": "l1"}
                return ([value], len(self.calls) == 1)

        api, source = FakeAPI(), Source()
        connector = channel.Connector(api, source, {"maxRemindersPerPoll": 10})
        self.assertEqual(len(connector.poll_once()), 1)
        self.assertEqual(connector.poll_once(), [])
        self.assertEqual(source.calls, [0, 1])
        self.assertEqual(len(api.items), 1)
        self.assertEqual([value[0] for value in api.health], ["healthy", "healthy"])

    def test_manifest_is_read_only(self):
        manifest = json.loads(Path(__file__).with_name("manifest.json").read_text())
        self.assertEqual(manifest["capabilities"], ["channels"])
        self.assertNotIn("send", channel.READ_SCRIPT)


if __name__ == "__main__":
    unittest.main()
