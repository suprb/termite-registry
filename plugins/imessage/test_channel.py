import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("imessage_channel", Path(__file__).with_name("channel.py"))
channel = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(channel)


class FakeAPI:
    def __init__(self):
        self.attempts, self.acks, self.verifications, self.health = [], [], [], []

    def begin_attempt(self, reply_id):
        self.attempts.append(reply_id)

    def ack(self, *args):
        self.acks.append(args)

    def verification_needed(self, reply_id, error):
        self.verifications.append((reply_id, error))

    def report_health(self, status, **fields):
        self.health.append((status, fields))


class IMessageTests(unittest.TestCase):
    def test_requires_opt_in_and_an_allowlist(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            with self.assertRaisesRegex(ValueError, "disabled"):
                channel.load_config(path, {})
            path.write_text('{"enabled":true,"allowedHandles":[],"allowedChatGUIDs":[]}')
            with self.assertRaisesRegex(ValueError, "allowlisted"):
                channel.load_config(path, {})

    def test_query_filters_own_and_unallowlisted_messages(self):
        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "chat.db"
            db = sqlite3.connect(database)
            db.executescript("""
                CREATE TABLE message (ROWID INTEGER PRIMARY KEY, guid TEXT, text TEXT, date INTEGER,
                                      is_from_me INTEGER, handle_id INTEGER);
                CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT);
                CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, display_name TEXT);
                CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
                INSERT INTO handle VALUES (1, 'friend@example.test'), (2, 'stranger@example.test');
                INSERT INTO chat VALUES (1, 'iMessage;-;friend', NULL), (2, 'iMessage;-;stranger', NULL);
                INSERT INTO message VALUES (1, 'guid-in', 'hello', 100, 0, 1);
                INSERT INTO message VALUES (2, 'guid-own', 'mine', 101, 1, 1);
                INSERT INTO message VALUES (3, 'guid-no', 'nope', 102, 0, 2);
                INSERT INTO chat_message_join VALUES (1, 1), (1, 2), (2, 3);
            """)
            db.commit(); db.close()
            source = channel.MessageSource(database, ["friend@example.test"], [])
            rows = source.rows(0, 20)
            self.assertEqual([row[1] for row in rows], ["guid-in"])
            item = channel.message_to_work_item(rows[0], source)
            self.assertEqual(item["conversationID"], "handle:friend@example.test")
            self.assertEqual(item["deliveryID"], "imessage:guid-in")

    def test_chat_allowlist_sets_a_stable_reply_address(self):
        source = channel.MessageSource("/tmp/no.db", [], ["iMessage;+;group"])
        row = (7, "guid-7", "status?", 100, "+46123", "iMessage;+;group", "Release team")
        one = channel.message_to_work_item(row, source)
        two = channel.message_to_work_item(row, source)
        self.assertEqual(one["id"], two["id"])
        self.assertEqual(one["conversationID"], "chat:iMessage;+;group")

    def test_applescript_body_is_an_argument_not_source(self):
        calls = []

        def runner(argv, **kwargs):
            calls.append((argv, kwargs))
            return b"sent"

        config = {"allowedHandles": ["friend@example.test"], "allowedChatGUIDs": []}
        dangerous = 'hello"\nend tell\ndo shell script "touch /tmp/no"'
        reply = {"conversationID": "handle:friend@example.test", "body": dangerous}
        channel.send_approved_reply(reply, config, runner)
        argv = calls[0][0]
        self.assertEqual(argv[:3], ["/usr/bin/osascript", "-e", channel.SEND_SCRIPT])
        self.assertEqual(argv[-1], dangerous)
        self.assertNotIn(dangerous, channel.SEND_SCRIPT)

    def test_outbound_address_is_rechecked(self):
        config = {"allowedHandles": ["friend@example.test"], "allowedChatGUIDs": []}
        with self.assertRaisesRegex(ValueError, "no longer allowlisted"):
            channel.resolve_address("handle:attacker@example.test", config)

    def test_reply_attempt_precedes_messages_send(self):
        api, calls = FakeAPI(), []
        connector = channel.Connector(
            api, None, {}, lambda reply, _config: calls.append(reply["id"]))
        connector.deliver({"id": "r1", "channel": channel.CHANNEL_ID, "body": "done"})
        self.assertEqual(api.attempts, ["r1"])
        self.assertEqual(calls, ["r1"])
        self.assertEqual(api.acks, [("r1", True)])
        self.assertEqual(api.health[-1][0], "healthy")

    def test_uncertain_apple_event_needs_verification(self):
        api = FakeAPI()

        def uncertain(_reply, _config):
            raise TimeoutError("Messages reply unknown")

        connector = channel.Connector(api, None, {}, uncertain)
        connector.deliver({"id": "r2", "channel": channel.CHANNEL_ID, "body": "done"})
        self.assertEqual(api.attempts, ["r2"])
        self.assertEqual(api.acks, [])
        self.assertEqual(api.verifications[0][0], "r2")
        self.assertEqual(api.health[-1][0], "degraded")

    def test_failed_ingest_does_not_advance_cursor(self):
        class Source:
            allowed_chats = set()
            allowed_handles = {"friend@example.test": "friend@example.test"}

            def boundary(self):
                return 0

            def rows(self, after, limit, upper_rowid=None, newest=False):
                return [(1, "guid-1", "hello", 100, "friend@example.test",
                         "iMessage;-;friend", None)] if after < 1 else []

        class FailingAPI:
            def request(self, path, body=None):
                raise ConnectionError("offline")

        connector = channel.Connector(
            FailingAPI(), Source(), {"includeExistingMessages": False, "maxMessagesPerPoll": 20}
        )
        self.assertEqual(connector.poll_once(), [])
        with self.assertRaises(ConnectionError):
            connector.poll_once()
        self.assertEqual(connector.cursor, 0)


if __name__ == "__main__":
    unittest.main()
