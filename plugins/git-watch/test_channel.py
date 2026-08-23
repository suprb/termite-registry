import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("git_watch_channel", Path(__file__).with_name("channel.py"))
channel = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(channel)


HASH1 = "1" * 40
HASH2 = "2" * 40


class FakeAPI:
    def __init__(self):
        self.items, self.health = [], []

    def request(self, path, body=None):
        if body:
            self.items.append(body)
        return {}

    def report_health(self, status, **fields):
        self.health.append((status, fields))


class FakeSource:
    repository = Path("/tmp/narrow-repo")

    def __init__(self):
        self.hashes = [HASH1]

    def recent_hashes(self, count):
        return self.hashes[:count]

    def commit(self, value):
        return {"hash": value, "author": "Ada", "email": "ada@example.test",
                "date": "2026-07-18T10:00:00Z", "subject": "Ship", "body": "Verified"}


class GitWatchTests(unittest.TestCase):
    def test_disabled_and_missing_repository_are_safe(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            with self.assertRaisesRegex(ValueError, "disabled"):
                channel.load_config(path, {})
            path.write_text('{"enabled":true,"repository":""}')
            with self.assertRaisesRegex(ValueError, "explicitly"):
                channel.load_config(path, {})

    def test_commit_identity_is_stable(self):
        commit = FakeSource().commit(HASH1)
        one = channel.commit_to_work_item(commit, "/tmp/narrow-repo")
        two = channel.commit_to_work_item(commit, "/tmp/narrow-repo")
        self.assertEqual(one["deliveryID"], two["deliveryID"])

    def test_default_baselines_then_reports_new_commit(self):
        api, source = FakeAPI(), FakeSource()
        connector = channel.Connector(api, source, {
            "maxCommitsPerPoll": 20, "includeExistingCommits": False,
        })
        self.assertEqual(connector.poll_once(), [])
        source.hashes = [HASH2, HASH1]
        submitted = connector.poll_once()
        self.assertEqual([item["id"] for item in submitted], ["commit-" + HASH2])
        self.assertEqual(len(api.items), 1)
        self.assertEqual([value[0] for value in api.health], ["healthy", "healthy"])

    def test_manifest_has_no_reply_event_capability(self):
        import json
        manifest = json.loads(Path(__file__).with_name("manifest.json").read_text())
        self.assertEqual(manifest["capabilities"], ["channels"])


if __name__ == "__main__":
    unittest.main()
