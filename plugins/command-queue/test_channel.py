import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("command_queue_channel", Path(__file__).with_name("channel.py"))
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


class CommandQueueTests(unittest.TestCase):
    def test_disabled_and_string_commands_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            with self.assertRaisesRegex(ValueError, "disabled"):
                channel.load_config(path, {})
            path.write_text('{"enabled":true,"producer":"echo bad","consumer":["/bin/true"]}')
            with self.assertRaisesRegex(ValueError, "argv array"):
                channel.load_config(path, {})

    def test_producer_identity_uses_declared_delivery_id(self):
        raw = b'{"deliveryID":"ticket-42","title":"Review","body":"Check it"}'
        first = channel.parse_producer_output(raw, 16)[0]
        second = channel.parse_producer_output(raw, 16)[0]
        self.assertEqual(first["deliveryID"], second["deliveryID"])
        self.assertEqual(first["id"], second["id"])

    def test_consumer_gets_json_stdin_and_fixed_argv(self):
        api, calls = FakeAPI(), []

        def runner(argv, input_bytes=b"", **kwargs):
            calls.append((list(argv), input_bytes, kwargs))
            return b""

        config = {"producer": ["/producer", "--json"], "consumer": ["/consumer", "--stdin"],
                  "producerTimeoutSeconds": 2, "consumerTimeoutSeconds": 3, "maxItemsPerPoll": 16}
        connector = channel.Connector(api, config, runner)
        connector.deliver({"id": "reply-1", "channel": channel.CHANNEL_ID,
                           "conversationID": "ticket-42", "body": "approved; $(unsafe)"})
        self.assertEqual(api.attempts, ["reply-1"])
        self.assertEqual(calls[0][0], ["/consumer", "--stdin"])
        self.assertEqual(json.loads(calls[0][1])["body"], "approved; $(unsafe)")
        self.assertEqual(api.acks, [("reply-1", True)])
        self.assertEqual(api.health[-1][0], "healthy")

    def test_consumer_failure_after_attempt_needs_verification(self):
        api = FakeAPI()

        def runner(_argv, **_kwargs):
            raise TimeoutError("consumer result unknown")

        config = {"producer": ["/producer"], "consumer": ["/consumer"],
                  "producerTimeoutSeconds": 2, "consumerTimeoutSeconds": 3,
                  "maxItemsPerPoll": 16}
        connector = channel.Connector(api, config, runner)
        connector.deliver({"id": "reply-2", "channel": channel.CHANNEL_ID,
                           "conversationID": "ticket-42", "body": "approved"})
        self.assertEqual(api.attempts, ["reply-2"])
        self.assertEqual(api.acks, [])
        self.assertEqual(api.verifications[0][0], "reply-2")
        self.assertEqual(api.health[-1][0], "degraded")

    def test_poll_deduplicates_repeated_producer_output(self):
        api = FakeAPI()
        raw = b'[{"deliveryID":"one","body":"First"}]'

        def runner(argv, **kwargs):
            return raw

        config = {"producer": ["/producer"], "consumer": ["/consumer"],
                  "producerTimeoutSeconds": 2, "consumerTimeoutSeconds": 3, "maxItemsPerPoll": 16}
        connector = channel.Connector(api, config, runner)
        self.assertEqual(len(connector.poll_once()), 1)
        self.assertEqual(connector.poll_once(), [])
        self.assertEqual(len(api.items), 1)

    def test_item_count_and_body_limits(self):
        too_many = json.dumps([{"deliveryID": str(i), "body": "x"} for i in range(3)]).encode()
        with self.assertRaisesRegex(ValueError, "more than 2"):
            channel.parse_producer_output(too_many, 2)
        with self.assertRaisesRegex(ValueError, "64 KiB"):
            channel.queue_item({"deliveryID": "x", "body": "x" * (channel.MAX_BODY_BYTES + 1)})

    def test_timeout_kills_the_entire_spawned_process_group(self):
        script = (
            "import subprocess,sys; "
            "subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)'])"
        )
        with self.assertRaises(TimeoutError):
            channel.run_bounded([sys.executable, "-c", script], timeout=0.1, max_stdout=64)


if __name__ == "__main__":
    unittest.main()
