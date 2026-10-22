"""Offline fixtures only: no real sessions, settings, messages, or model turns."""
import copy
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

import collector
import events
import stream_store
from stream_store import Store, CapacityError
import test_adapter as fixture

TARGET = fixture.TARGET
EVENT_ID = fixture.OTHER
BINDING = {"target": TARGET, "cli": "/fixture/claude", "root": "/fixture/root",
           "include_text": False, "peer_inbox": False}
IDENTITY = [TARGET, 12345, "fixture-start", "/fixture/producer.sock"]
CURSOR = {"offset": 100, "inode": 1, "device": 2}


def structured(state="running", **changes):
    value = {"version": 1, "event_id": EVENT_ID, "kind": "status", "origin": "claude",
             "hops": 0, "state": state, "text": "fixture progress"}
    value.update(changes)
    return events.PREFIX + json.dumps(value)


def transcript(text="fixture", uid=EVENT_ID):
    return {"type": "assistant", "uuid": uid, "text": text, "tools": [], "text_truncated": False}


def peer(text="fixture", uid=EVENT_ID, sequence=1):
    return {"kind": "peer_reply", "peer_event_id": uid, "text": text, "sequence": sequence}


def snapshot(rows=(), cursor=None, stream="fixture-stream"):
    return {"identity": IDENTITY[:], "event_stream": stream, "events": list(rows),
            "cursor": cursor or CURSOR.copy(), "gaps": [], "warnings": []}


def wait_reply(rows=(), peers=(), cursor=None, stream="fixture-stream", cancelled=True):
    return {"transcript": snapshot(rows, cursor, stream), "events": list(peers), "sequence": len(peers),
            "event_stream": stream, "event_gap": None, "cancelled": cancelled}


class FakeClient:
    def __init__(self, first=None, replies=()):
        self.first = first or snapshot()
        self.replies = iter(replies or [wait_reply()])
        self.calls, self.closed = [], False

    def call(self, name, args, **_):
        self.calls.append((name, copy.deepcopy(args)))
        if name == "read_claude_session":
            return copy.deepcopy(self.first)
        if name == "open_claude_inbox":
            return {"identity": self.first["identity"], "event_stream": self.first["event_stream"], "address": "uds:/fixture/private.sock"}
        if name != "wait_claude_session":
            raise AssertionError("Forbidden dispatch: " + name)
        value = next(self.replies)
        if isinstance(value, Exception):
            raise value
        return copy.deepcopy(value)

    def close(self):
        self.closed = True


class EventTests(unittest.TestCase):
    def test_metadata_default_and_redaction_opt_in(self):
        secret = "password=hidden token=secret Bearer abcdef sk-123456789xyz person@example.com /Users/someone/private"
        default = events.normalize(TARGET, transcript(secret), "transcript")
        self.assertNotIn("hidden", json.dumps(default))
        visible = events.normalize(TARGET, transcript(secret), "transcript", include_text=True)
        self.assertNotIn("hidden", visible["content"]["text"])
        for raw in ("secret", "abcdef", "sk-123456789xyz", "person@example.com", "/Users/someone"):
            self.assertNotIn(raw, visible["content"]["text"])

    def test_ignore_user_echo_and_delivery_receipt(self):
        self.assertIsNone(events.normalize(TARGET, {"type": "user", "text": "incoming"}, "transcript"))
        self.assertIsNone(events.normalize(TARGET, {"kind": "delivery_receipt"}, "peer"))

    def test_structured_cross_route_id_and_semantics_match(self):
        a = events.normalize(TARGET, transcript(structured()), "transcript")
        b = events.normalize(TARGET, peer(structured()), "peer", stream="a")
        self.assertEqual(a["event_id"], b["event_id"])
        self.assertEqual(events.canonical(a), events.canonical(b))
        self.assertEqual("running", b["content"]["reported_state"])

    def test_closed_schema_loop_marker_and_false_verification_rejected(self):
        for changes in ({"origin": "codex"}, {"hops": 1}, {"callback": "https://invalid.example"},
                        {"state": "verified"}, {"state": []}, {"event_id": []}, {"version": True}):
            result = events.normalize(TARGET, transcript(structured(**changes)), "transcript", include_text=True)
            self.assertEqual("rejected_structured_event", result["kind"])
            self.assertNotIn("text", result["content"])

    def test_updates_distinct_exact_duplicates_stable(self):
        a = events.normalize(TARGET, transcript("a"), "transcript")
        self.assertEqual(a, events.normalize(TARGET, transcript("a"), "transcript"))
        self.assertNotEqual(a["event_id"], events.normalize(TARGET, transcript("b"), "transcript")["event_id"])

    def test_missing_uuid_uses_record_cursor(self):
        a, b = transcript(uid=None), transcript(uid=None)
        a["record_cursor"], b["record_cursor"] = {"offset": 1}, {"offset": 2}
        self.assertNotEqual(events.normalize(TARGET, a, "transcript")["event_id"], events.normalize(TARGET, b, "transcript")["event_id"])

    def test_peer_without_id_dedup_is_limited_to_helper(self):
        a = events.normalize(TARGET, peer(uid=None), "peer", stream="one")
        b = events.normalize(TARGET, peer(uid=None), "peer", stream="two")
        self.assertNotEqual(a["event_id"], b["event_id"])
        self.assertEqual("helper_lifetime", a["source"]["dedupe"])

    def test_unicode_text_fits_journal_and_invalid_unicode_is_inert(self):
        value = events.normalize(TARGET, transcript("😀" * 2000), "transcript", include_text=True)
        self.assertLess(len(events.packed(value).encode()), stream_store.MAX_EVENT_BYTES)
        self.assertTrue(value["content"]["display_text_may_be_shortened"])
        surrogate = events.normalize(TARGET, transcript("\ud800"), "transcript", include_text=True)
        events.packed(surrogate).encode("utf-8")

    def test_arbitrary_source_id_cannot_bypass_metadata_filter(self):
        value = events.normalize(TARGET, transcript("fixture", uid="token=fixture-secret"), "transcript")
        self.assertNotIn("fixture-secret", json.dumps(value))

    def test_json_escape_overhead_fits_event_budget(self):
        for char in ("\x01", "\n", "\"", "\\"):
            value = events.normalize(TARGET, transcript(char * 2000), "transcript", include_text=True)
            self.assertLess(len(events.packed(value).encode()), stream_store.MAX_EVENT_BYTES)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="collector-test-")
        self.path = Path(self.tmp.name)
        self.store = Store(self.path)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def seed(self, value=None):
        state = self.store.bind(BINDING, IDENTITY)
        state.update(collection="collecting", last_verified=100, lease_seconds=30, cursor=CURSOR.copy())
        event = value or events.normalize(TARGET, transcript(), "transcript")
        self.store.commit(state, [event])
        return event

    def test_atomic_backpressure_retains_previous_cursor_and_dedup(self):
        self.seed()
        state = self.store.state()
        state["cursor"]["offset"] += 100
        with mock.patch.object(stream_store, "MAX_EVENTS", 1), self.assertRaises(CapacityError):
            self.store.commit(state, [events.normalize(TARGET, transcript("second"), "transcript")])
        self.assertEqual(CURSOR, self.store.state()["cursor"])
        self.assertEqual(1, len(self.store.replay()))

    def test_collision_rolls_back_whole_batch(self):
        first = self.seed(events.normalize(TARGET, peer("one"), "peer"))
        collision = events.normalize(TARGET, peer("two"), "peer")
        self.assertEqual(first["event_id"], collision["event_id"])
        with self.assertRaisesRegex(ValueError, "collision"):
            self.store.commit(self.store.state(), [events.normalize(TARGET, transcript("new"), "transcript"), collision])
        self.assertEqual(1, len(self.store.replay()))

    def test_duplicate_status_has_idempotent_presentation(self):
        first = events.normalize(TARGET, transcript(structured()), "transcript")
        self.seed(first)
        before = self.store.status(now=101)
        state = self.store.state()
        state["last_verified"] = 102
        other_route = events.normalize(TARGET, peer(structured()), "peer")
        self.assertEqual([], self.store.commit(state, [other_route]))
        after = self.store.status(now=103)
        self.assertEqual(before["presentation_id"], after["presentation_id"])
        self.assertEqual("running", after["state"]["reported_state"])
        self.assertEqual("unavailable_unqualified", after["parent_delivery"])

    def test_no_inferred_completion_and_stale_producer(self):
        self.seed()
        fresh = self.store.status(now=105)
        stale = self.store.status(now=131)
        self.assertEqual("fresh", fresh["producer_observation"])
        self.assertEqual("stale", stale["producer_observation"])
        self.assertNotEqual(fresh["presentation_id"], stale["presentation_id"])
        self.assertNotIn("reported_state", stale["state"])

    def test_restart_replay_and_read_lane_while_writer_owns_lock(self):
        event = self.seed()
        with Store(self.path, "reader") as read:
            self.assertEqual(event, read.replay()[0]["event"])
            self.assertEqual(1, read.status(now=105)["last_seq"])
        with self.assertRaisesRegex(ValueError, "Another collector"):
            Store(self.path)
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(CURSOR, self.store.state()["cursor"])
        self.assertEqual(event, self.store.replay()[0]["event"])

    def test_ack_is_local_monotonic_and_idempotent(self):
        self.seed()
        with Store(self.path, "consumer") as consumer:
            first = consumer.acknowledge("fixture", 1)
            self.assertEqual(first, consumer.acknowledge("fixture", 1))
            self.assertFalse(first["parent_presentation_proven"])
            with self.assertRaises(ValueError):
                consumer.acknowledge("fixture", 0)
            with self.assertRaises(ValueError):
                consumer.acknowledge("fixture", 2)
        self.assertEqual({"fixture": 1}, self.store.status()["consumer_cursors"])

    def test_private_permissions_and_missing_reader(self):
        self.assertEqual(0o600, (self.path / "stream.sqlite3").stat().st_mode & 0o777)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                Store(directory, "reader")
            self.assertEqual([], list(Path(directory).iterdir()))

    def test_symlink_sidecar_is_rejected_without_touching_destination(self):
        target = self.path / "do-not-touch"
        target.write_text("sentinel")
        target.chmod(0o600)
        (self.path / "stream.sqlite3-journal").symlink_to(target)
        with self.assertRaisesRegex(ValueError, "sidecar"):
            Store(self.path, "reader")
        self.assertEqual("sentinel", target.read_text())
        (self.path / "stream.sqlite3-journal").unlink()

    def test_reader_cannot_mutate_collection_or_ack(self):
        self.seed()
        with Store(self.path, "reader") as read:
            with self.assertRaises(ValueError):
                read.commit({})
            with self.assertRaises(ValueError):
                read.acknowledge("test", 1)

    def test_wrong_scope_or_identity_rejected(self):
        self.seed()
        with self.assertRaises(ValueError):
            self.store.bind({**BINDING, "target": fixture.OTHER}, IDENTITY)
        with self.assertRaises(ValueError):
            self.store.bind(BINDING, ["replacement"])

    def test_byte_limit_and_consumer_capacity_are_bounded(self):
        self.seed()
        with mock.patch.object(stream_store, "MAX_BYTES", 1), self.assertRaises(CapacityError):
            self.store.commit(self.store.state(), [events.normalize(TARGET, transcript("new"), "transcript")])
        with Store(self.path, "consumer") as consumer, mock.patch.object(stream_store, "MAX_CONSUMERS", 1):
            consumer.acknowledge("first", 1)
            with self.assertRaises(CapacityError):
                consumer.acknowledge("second", 1)

    def test_crashed_writer_reports_recovery_then_offline_replay_restores_commit(self):
        self.seed()
        self.store.close()
        # Force real SQLite cache spill before abrupt exit, leaving a hot journal.
        script = """import os, sqlite3, sys
db=sqlite3.connect(sys.argv[1])
db.execute('PRAGMA cache_size=1')
db.execute('BEGIN IMMEDIATE')
db.execute(\"UPDATE metadata SET value=? WHERE key='state'\", ('x'*200000,))
os._exit(0)
"""
        subprocess.run([sys.executable, "-B", "-c", script, str(self.path / "stream.sqlite3")], check=True)
        with Store(self.path, "reader") as reader:
            status = reader.status()
            self.assertEqual("recovery_required", status["state"]["collection"])
            self.assertEqual("unverified", status["producer_observation"])
            self.assertIsNone(status["last_seq"])
            self.assertEqual(status["presentation_id"], reader.status()["presentation_id"])
            with self.assertRaises(stream_store.RecoveryRequired):
                reader.replay()
        recovered = Store.recover(self.path)
        self.assertEqual("recovered_observer_stopped", recovered["state"]["collection"])
        self.assertEqual("observer_stopped", recovered["producer_observation"])
        self.assertEqual(CURSOR, recovered["state"]["cursor"])
        with Store(self.path, "reader") as reader:
            self.assertEqual(1, len(reader.replay()))

    def test_recovery_refuses_missing_journal_without_creating_it(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                Store.recover(directory)
            self.assertEqual([], list(Path(directory).iterdir()))

    def test_reader_open_before_crash_also_reports_recovery_required(self):
        self.seed()
        self.store.close()
        with Store(self.path, "reader") as reader:
            script = """import os, sqlite3, sys
db=sqlite3.connect(sys.argv[1])
db.execute('PRAGMA cache_size=1')
db.execute('BEGIN IMMEDIATE')
db.execute(\"UPDATE metadata SET value=? WHERE key='state'\", ('x'*200000,))
os._exit(0)
"""
            subprocess.run([sys.executable, "-B", "-c", script, str(self.path / "stream.sqlite3")], check=True)
            self.assertEqual("recovery_required", reader.status()["state"]["collection"])
            with self.assertRaises(stream_store.RecoveryRequired):
                reader.replay()
        Store.recover(self.path)

    def test_publication_intent_survives_restart_and_cannot_be_retried(self):
        self.seed()
        self.store.publication(1, "uncertain")
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual("uncertain", self.store.replay()[0]["publication"])
        with self.assertRaises(Exception):
            self.store.publication(1, "uncertain")


class CollectorTests(unittest.TestCase):
    setUp = StoreTests.setUp
    tearDown = StoreTests.tearDown
    seed = StoreTests.seed
    def test_unsolicited_event_persisted_without_send(self):
        fake = FakeClient(replies=[wait_reply(peers=[peer(structured("done"))])])
        result = collector.collect(lambda: fake, self.store, {**BINDING, "peer_inbox": True})
        self.assertEqual("done", result["state"]["reported_state"])
        self.assertEqual(0, result["send_calls"])
        self.assertTrue(fake.closed)
        self.assertIn("open_claude_inbox", [name for name, _ in fake.calls])
        self.assertNotIn("send_claude_message", [name for name, _ in fake.calls])
        self.assertIsNone(result["state"]["endpoint"])

    def test_disconnect_restart_resumes_cursor_and_retains_peer_gap(self):
        binding = {**BINDING, "peer_inbox": True}
        first = FakeClient(snapshot([transcript()]), [RuntimeError("private failure")])
        failed = collector.collect(lambda: first, self.store, binding)
        self.assertEqual("producer_or_transport_error", failed["outcome"])
        self.assertNotIn("private failure", json.dumps(failed))
        self.store.close()
        self.store = Store(self.path)
        second = FakeClient(snapshot([transcript()], stream="new"), [wait_reply(stream="new")])
        result = collector.collect(lambda: second, self.store, binding)
        self.assertEqual(CURSOR, second.calls[0][1]["cursor"])
        rows = self.store.replay()
        self.assertEqual(1, sum(r["event"]["kind"] == "assistant_message" for r in rows))
        self.assertTrue(any(r["event"]["kind"] == "peer_listener_restart_gap" for r in rows))
        self.assertEqual("new", result["state"]["event_stream"])

    def test_scope_mismatch_makes_no_client(self):
        self.seed()
        factory = mock.Mock()
        with self.assertRaises(ValueError):
            collector.collect(factory, self.store, {**BINDING, "target": fixture.OTHER})
        factory.assert_not_called()

    def test_missing_producer_has_no_claim_of_liveness(self):
        result = collector.collect(mock.Mock(side_effect=RuntimeError("missing")), self.store, BINDING)
        self.assertEqual("unverified", result["producer_observation"])
        self.assertEqual("producer_or_transport_error", result["outcome"])

    def test_replacement_producer_preserves_cursor(self):
        self.seed()
        first = snapshot([transcript("new")], {**CURSOR, "offset": 200})
        first["identity"] = ["replacement"]
        fake = FakeClient(first)
        result = collector.collect(lambda: fake, self.store, BINDING)
        self.assertEqual("producer_or_transport_error", result["outcome"])
        self.assertEqual(CURSOR, result["state"]["cursor"])
        self.assertTrue(fake.closed)

    def test_blocked_publication_durable_and_never_automatically_republished(self):
        published = []
        def failing(entry, **_):
            published.append(entry)
            raise BrokenPipeError("private")
        fake = FakeClient(snapshot([transcript()]))
        first = collector.collect(lambda: fake, self.store, BINDING, publisher=failing)
        self.assertEqual("publication_uncertain", first["outcome"])
        saved = {row["event"]["event_id"] for row in self.store.replay()}
        next_output = []
        collector.collect(lambda: FakeClient(snapshot([transcript()])), self.store, BINDING, publisher=lambda entry, **_: next_output.append(entry))
        self.assertTrue(all(row["event"]["event_id"] not in saved for row in next_output))
        self.assertEqual(1, len(published))
        self.assertEqual("uncertain", self.store.replay()[0]["publication"])

    def test_quota_status_remains_readable_and_cursor_does_not_advance(self):
        self.seed()
        fake = FakeClient(snapshot([transcript("new")], {**CURSOR, "offset": 200}))
        with mock.patch.object(stream_store, "MAX_EVENTS", 1):
            result = collector.collect(lambda: fake, self.store, BINDING)
        self.assertEqual("backpressure", result["outcome"])
        with Store(self.path, "reader") as reader:
            self.assertEqual(CURSOR, reader.status()["state"]["cursor"])

    def test_quiet_duration_is_bounded_and_helper_closed(self):
        fake = FakeClient(replies=[wait_reply(cancelled=False)] * 20)
        start = time.monotonic()
        result = collector.collect(lambda: fake, self.store, BINDING, duration=0.25)
        self.assertLess(time.monotonic() - start, 1)
        self.assertEqual("stopped", result["outcome"])
        self.assertTrue(fake.closed)

    def test_cancel_before_contact(self):
        cancel, factory = threading.Event(), mock.Mock()
        cancel.set()
        result = collector.collect(factory, self.store, BINDING, cancel=cancel)
        factory.assert_not_called()
        self.assertEqual("cancelled", result["outcome"])

    def test_cancellation_stops_publication_between_entries(self):
        cancel, output = threading.Event(), []
        def publish(entry, **_):
            output.append(entry)
            cancel.set()
        fake = FakeClient(snapshot([transcript("a"), transcript("b")]))
        result = collector.collect(lambda: fake, self.store, BINDING, cancel=cancel, publisher=publish)
        self.assertEqual("cancelled", result["outcome"])
        self.assertEqual(1, len(output))
        self.assertEqual("pipe_written_no_parent_ack", self.store.replay()[0]["publication"])
        self.assertTrue(all(row["publication"] == "not_attempted" for row in self.store.replay()[1:]))

    def test_gap_evidence_is_bounded_replayable_and_separate_from_warnings(self):
        first = snapshot()
        first.update(gaps=[{"reason": "malformed_record", "start_offset": 3, "end_offset": 80, "record_complete": True}], warnings=["repeated_uuid: update retained"])
        reply = wait_reply()
        reply["event_gap"] = {"after_sequence": 0, "oldest_available": 4}
        collector.collect(lambda: FakeClient(first, [reply]), self.store, BINDING)
        by_kind = {row["event"]["kind"]: row["event"]["content"]["details"] for row in self.store.replay()}
        self.assertEqual(3, by_kind["transcript_gap"]["ranges"][0]["start_offset"])
        self.assertEqual(["repeated_uuid"], by_kind["transcript_warning"]["codes"])
        self.assertEqual(4, by_kind["peer_retention_gap"]["oldest_available"])


class PipeTests(unittest.TestCase):
    def test_slow_consumer_hits_deadline_without_retry(self):
        read, write = os.pipe()
        try:
            os.set_blocking(write, False)
            while True:
                try:
                    os.write(write, b"x" * 4096)
                except BlockingIOError:
                    break
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                collector.PipePublisher(write, timeout=0.05)({"seq": 1, "event": {}})
            self.assertLess(time.monotonic() - started, 0.5)
        finally:
            os.close(read)
            os.close(write)

    def test_structured_output_contains_no_parent_delivery_claim(self):
        read, write = os.pipe()
        try:
            collector.PipePublisher(write)({"seq": 1, "event": {"event_id": "fixture"}})
            value = json.loads(os.read(read, 4096))
            self.assertEqual("unavailable_unqualified", value["parent_delivery"])
        finally:
            os.close(read)
            os.close(write)


class ReceiveOnlyInboxTests(unittest.TestCase):
    setUp = fixture.AdapterTests.setUp
    tearDown = fixture.AdapterTests.tearDown

    def test_verified_local_peer_can_publish_without_outstanding_send(self):
        with tempfile.TemporaryDirectory(prefix="ce-", dir="/tmp") as folder:
            self.data.update(pid=os.getpid(), messagingSocketPath=str(Path(folder) / "fixture.sock"))
            with mock.patch.object(self.adapter, "verify_target", return_value=self.data), mock.patch.object(self.adapter, "send_message", side_effect=AssertionError("No send allowed")):
                opened = self.adapter.open_inbox(TARGET)
                self.assertEqual(0, opened["send_calls"])
                self.assertEqual({}, self.adapter.outstanding)
                self.assertEqual(0o700, self.adapter.inbox.stat().st_mode & 0o777)
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                    sock.connect(opened["address"][4:])
                    sock.sendall(fixture.line({"type": "user", "msg_id": EVENT_ID, "message": {"content": "unsolicited fixture"}}))
                    end = time.monotonic() + 1
                    while not self.adapter.events and time.monotonic() < end:
                        time.sleep(0.01)
                self.assertEqual(EVENT_ID, self.adapter.events[0]["peer_event_id"])
                self.assertEqual("unsolicited fixture", self.adapter.events[0]["text"])
                self.adapter.close()
                self.assertFalse(Path(opened["address"][4:]).exists())

    def test_receive_only_open_still_enforces_exact_allowlist(self):
        with self.assertRaises(fixture.mod.AdapterError) as error:
            self.adapter.open_inbox(fixture.OTHER)
        self.assertEqual("session_not_allowed", error.exception.code)


class CollectorDiscardRestartTests(unittest.TestCase):
    setUp = fixture.AdapterTests.setUp
    tearDown = fixture.AdapterTests.tearDown
    cursor = fixture.AdapterTests.cursor

    def test_committed_discard_cursor_survives_disconnect_and_reopen(self):
        body = fixture.line({"type": "user", "sessionId": TARGET,
                             "message": {"content": [{"type": "tool_result", "content": "HIDDEN_TOOL_BODY" * 70000}]}})
        self.transcript.write_bytes(body + fixture.line(fixture.event("VISIBLE_AFTER")))
        state_path = self.root / "collector-state"
        state_path.mkdir(mode=0o700)
        binding = {**BINDING, "include_text": True}
        owner = self

        class Bridge:
            def __init__(self, fail):
                self.fail = fail
                self.closed = False
                self.calls = []

            def call(self, name, args, **_):
                self.calls.append(name)
                if name not in ("read_claude_session", "wait_claude_session"):
                    raise AssertionError("No sends or inbox permitted in this fixture")
                if name == "wait_claude_session" and self.fail:
                    raise RuntimeError("synthetic disconnect")
                args = dict(args)
                if name == "wait_claude_session":
                    args["timeout"] = 0
                with mock.patch.object(owner.adapter, "verify_target", return_value=owner.data):
                    result = owner.adapter.call(name, args)
                if name == "wait_claude_session" and not result["transcript"]["has_more"]:
                    result["cancelled"] = True
                return result

            def close(self):
                self.closed = True

        with Store(state_path) as store:
            state = store.bind(binding, list(self.adapter.identity(self.data)))
            state["cursor"] = self.cursor()
            store.commit(state)
            first = Bridge(True)
            failed = collector.collect(lambda: first, store, binding)
            self.assertEqual("producer_or_transport_error", failed["outcome"])
            saved = store.state()["cursor"]
            self.assertTrue(saved["discarding"])
            self.assertEqual(fixture.mod.MAX_SCAN, saved["offset"])
        self.adapter.close()
        self.adapter = fixture.mod.Adapter(self.root, sys.executable, TARGET, fixture.SOURCE)
        with Store(state_path) as store:
            self.assertEqual(saved, store.state()["cursor"])
            second = Bridge(False)
            result = collector.collect(lambda: second, store, binding)
            rows = store.replay()
            visible = [r for r in rows if r["event"]["kind"] == "assistant_message"]
            self.assertEqual(["VISIBLE_AFTER"], [r["event"]["content"]["text"] for r in visible])
            self.assertNotIn("HIDDEN_TOOL_BODY", json.dumps(rows))
            self.assertEqual(self.transcript.stat().st_size, result["state"]["cursor"]["offset"])
            self.assertNotIn("discarding", result["state"]["cursor"])
            self.assertGreaterEqual(sum(r["event"]["kind"] == "transcript_gap" for r in rows), 2)
            self.assertTrue(first.closed and second.closed)
            self.assertNotIn("send_claude_message", first.calls + second.calls)


if __name__ == "__main__":
    unittest.main()
