import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("clipboard_channel", Path(__file__).with_name("channel.py"))
channel = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(channel)


class FakeAPI:
    def __init__(self):
        self.items, self.acks, self.attempts = [], [], []
        self.verifications, self.health = [], []

    def request(self, path, body=None):
        if body is not None:
            self.items.append(body)
        return {"replies": []}

    def ack(self, *args):
        self.acks.append(args)

    def begin_attempt(self, reply_id):
        self.attempts.append(reply_id)

    def verification_needed(self, reply_id, error):
        self.verifications.append((reply_id, error))

    def report_health(self, status, **fields):
        self.health.append((status, fields))


class ClipboardTests(unittest.TestCase):
    def test_disabled_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "requires"):
                channel.load_config(Path(tmp) / "missing.json", {})

    def test_identity_is_stable(self):
        one = channel.clipboard_item(b"review this")
        two = channel.clipboard_item(b"review this")
        self.assertEqual(one["deliveryID"], two["deliveryID"])

    def test_initial_clipboard_is_only_a_baseline_by_default(self):
        api = FakeAPI()
        connector = channel.Connector(api, {"includeCurrentClipboard": False}, lambda: b"existing")
        self.assertIsNone(connector.poll_once())
        self.assertEqual(api.items, [])

    def test_approved_reply_is_not_reingested(self):
        api = FakeAPI()
        current = [b"incoming"]

        def read():
            return current[0]

        def write(body):
            current[0] = body.encode()
            return channel.hashlib.sha256(current[0]).hexdigest()

        connector = channel.Connector(api, {"includeCurrentClipboard": True}, read, write)
        connector.poll_once()
        connector.deliver({"id": "r1", "channel": channel.CHANNEL_ID, "body": "approved"})
        self.assertIsNone(connector.poll_once())
        self.assertEqual(len(api.items), 1)
        self.assertEqual(api.attempts, ["r1"])
        self.assertEqual(api.acks, [("r1", True)])
        self.assertEqual(api.health[-1][0], "healthy")

    def test_concurrent_reply_write_does_not_replace_self_write_digest(self):
        current = [b"incoming"]

        def read():
            return current[0]

        def write(body):
            current[0] = body.encode()
            return channel.hashlib.sha256(current[0]).hexdigest()

        class RacingAPI(FakeAPI):
            def request(api_self, path, body=None):
                result = super().request(path, body)
                if body is not None:
                    connector.deliver({"id": "r-race", "channel": channel.CHANNEL_ID,
                                       "body": "approved"})
                return result

        api = RacingAPI()
        connector = channel.Connector(api, {"includeCurrentClipboard": True}, read, write)
        connector.poll_once()
        self.assertIsNone(connector.poll_once())
        self.assertEqual(len(api.items), 1)
        self.assertEqual(api.attempts, ["r-race"])
        self.assertEqual(api.acks, [("r-race", True)])

    def test_failed_clipboard_write_is_not_automatically_retryable(self):
        api = FakeAPI()

        def fail(_body):
            raise TimeoutError("pbcopy acknowledgement lost")

        connector = channel.Connector(
            api, {"includeCurrentClipboard": True}, lambda: b"", fail)
        connector.deliver({"id": "r2", "channel": channel.CHANNEL_ID, "body": "approved"})
        self.assertEqual(api.attempts, ["r2"])
        self.assertEqual(api.acks, [])
        self.assertEqual(api.verifications[0][0], "r2")
        self.assertEqual(api.health[-1][0], "degraded")

    def test_output_limit_is_enforced(self):
        with self.assertRaisesRegex(ValueError, "64 KiB"):
            channel.clipboard_item(b"x" * (channel.MAX_CLIPBOARD_BYTES + 1))


if __name__ == "__main__":
    unittest.main()
