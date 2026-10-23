"""Deterministic fake MCP workflows. No real session, runtime or provider."""
import copy
import os
from pathlib import Path
import tempfile
import threading
import unittest
import subprocess
import sys
from unittest import mock

import journal as jm
from journal import Journal, digest
from workflow import run_operation
from client import bound_operation

TARGET = "11111111-1111-4111-8111-111111111111"
REQUEST = "22222222-2222-4222-8222-222222222222"
MESSAGE = "33333333-3333-4333-8333-333333333333"
ACK = f"CLAUDE-ADAPTER-{REQUEST} ACK"
IDENTITY = [TARGET, 12345, "synthetic-start", "/fixture", "/fixture.sock", "2.1.299", 1]
CURSOR = {"offset": 100, "inode": 123, "device": 456}


def observed(text, kind="assistant", offset=200, **extra):
    return {"type": kind, "uuid": str(offset), "timestamp": "fixture", "text": text, **extra}


def poll(events=(), *, offset=200, peers=(), sequence=0, gaps=(), event_gap=None):
    return {"events": list(peers), "sequence": sequence, "event_stream": "fixture-stream",
            "event_gap": event_gap,
            "transcript": {"events": list(events), "cursor": {**CURSOR, "offset": offset}, "gaps": list(gaps)}}


class FakeClient:
    def __init__(self, polls=(), *, failure=None, transport="sent", identity=None):
        self.polls = list(polls)
        self.failure = failure
        self.transport = transport
        self.identity = identity or IDENTITY
        self.calls = []
        self.closed = False

    def call(self, name, args, **options):
        self.calls.append((name, copy.deepcopy(args)))
        if name == "read_claude_session":
            return {"identity": self.identity, "cursor": CURSOR.copy(), "event_stream": "fixture-stream"}
        if name == "send_claude_message":
            if self.failure:
                raise self.failure
            return {"message_id": MESSAGE, "transport": self.transport, "delivery": "unknown",
                    "bytes_written": 10, "after_sequence": 0, "event_stream": "fixture-stream"}
        if self.polls:
            result = self.polls.pop(0)
            if isinstance(result, BaseException):
                raise result
            if callable(result):
                result = result()
            return result
        return poll(offset=args["cursor"]["offset"])

    def close(self):
        self.closed = True


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)
        self.path.chmod(0o700)
        self.journal = Journal(self.path)
        self.binding = {"target": TARGET, "cli": "/fixture/claude", "root": "/fixture/root", "message_hash": digest("hello")}

    def tearDown(self):
        self.journal.close()
        self.temp.cleanup()

    def run_fake(self, fake, **options):
        return run_operation(lambda: fake, self.journal, REQUEST, self.binding, "hello", budget=options.pop("budget", 1), **options)

    def sends(self, fake):
        return [args for name, args in fake.calls if name == "send_claude_message"]

    def test_echo_unrelated_then_delayed_ack_same_client(self):
        fake = FakeClient([poll([observed(ACK, "user")]), poll([observed("unrelated", offset=300)], offset=300),
                           poll([observed(ACK, offset=400)], offset=400)])
        state = self.run_fake(fake)
        self.assertEqual("acknowledged", state["outcome"])
        self.assertEqual(1, len(self.sends(fake)))
        waits = [a for n, a in fake.calls if n == "wait_claude_session"]
        self.assertEqual([100, 200, 300], [a["cursor"]["offset"] for a in waits])
        self.assertEqual([0, 0, 0], [a["after_sequence"] for a in waits])
        self.assertTrue(fake.closed)

    def test_user_echo_alone_never_acknowledges(self):
        state = self.run_fake(FakeClient([poll([observed(ACK, "user")])]), budget=0.08)
        self.assertEqual("observation_timeout", state["outcome"])
        self.assertIsNone(state["ack"])
        self.assertIsNotNone(state["arrival"])

    def test_exact_ack_without_inbound_event_preserves_missing_arrival(self):
        state = self.run_fake(FakeClient([poll([observed(ACK)])]))
        self.assertEqual("acknowledged", state["outcome"])
        self.assertIsNone(state["arrival"])
        self.assertEqual([], state["receipts"])

    def test_wrong_nonce_does_not_match(self):
        state = self.run_fake(FakeClient([poll([observed("other ACK")])]), budget=0.08)
        self.assertIsNone(state["ack"])

    def test_helper_cancelled_wait_ends_observation(self):
        state = self.run_fake(FakeClient([{**poll(), "cancelled": True}]))
        self.assertEqual("cancelled", state["outcome"])

    def test_saved_replay_binding_rejects_other_target(self):
        self.run_fake(FakeClient([poll([observed(ACK)])]))
        with self.assertRaisesRegex(ValueError, "No matching operation"):
            bound_operation(self.journal, REQUEST, {**self.binding, "target": MESSAGE})
        self.assertEqual("acknowledged", bound_operation(self.journal, REQUEST, self.binding)["state"]["outcome"])

    def test_process_exit_keeps_committed_intent_and_releases_lock(self):
        self.run_fake(FakeClient(), budget=0.02)
        self.journal.close()
        # Owned child exits without close()/finally, as after a process crash.
        program = "from journal import Journal; import os,sys; j=Journal(sys.argv[1]); j.begin(sys.argv[2], {}, {'outcome':'prepared_unknown'}); os._exit(17)"
        child = subprocess.run([sys.executable, "-B", "-c", program, str(self.path), MESSAGE],
                               cwd=Path(__file__).parent, capture_output=True, timeout=10)
        self.assertEqual(17, child.returncode, child.stderr.decode())
        self.journal = Journal(self.path)
        self.assertEqual("prepared_unknown", self.journal.get(MESSAGE)["state"]["outcome"])
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.journal.begin(MESSAGE, {}, {})

    def test_peer_reply_and_sequence_are_independent_of_transcript_cursor(self):
        reply = {"kind": "peer_reply", "sequence": 7, "text": ACK, "verified_pid": 12345}
        state = self.run_fake(FakeClient([poll(peers=[reply], sequence=7, offset=100)]))
        self.assertEqual("verified_peer_reply", state["ack"]["route"])
        self.assertEqual(7, state["sequence"])
        self.assertEqual(100, state["cursor"]["offset"])

    def test_held_stops_without_resend_or_ack(self):
        receipt = {"kind": "delivery_receipt", "sequence": 1, "message_id": MESSAGE, "status": "held"}
        fake = FakeClient([poll(peers=[receipt], sequence=1)])
        state = self.run_fake(fake)
        self.assertEqual("held_or_refused", state["outcome"])
        self.assertIsNone(state["ack"])
        self.assertEqual(1, len(self.sends(fake)))

    def test_unrelated_receipt_does_not_stop(self):
        receipt = {"kind": "delivery_receipt", "sequence": 1, "message_id": "another", "status": "held"}
        state = self.run_fake(FakeClient([poll(peers=[receipt], sequence=1), poll([observed(ACK)], sequence=1)]))
        self.assertEqual("acknowledged", state["outcome"])

    def test_uncertain_partial_write_is_observed_not_resent(self):
        fake = FakeClient([poll([observed(ACK)])], transport="uncertain")
        state = self.run_fake(fake)
        self.assertEqual("uncertain", state["transport"])
        self.assertEqual("acknowledged", state["outcome"])
        self.assertEqual(1, len(self.sends(fake)))

    def test_helper_exit_during_send_consumes_intent_and_resume_never_sends(self):
        first = FakeClient(failure=RuntimeError("helper exited after possible write"))
        state = self.run_fake(first)
        self.assertEqual("observation_error", state["outcome"])
        self.assertIsNone(state["message_id"])
        self.journal.close()
        self.journal = Journal(self.path)
        resumed = FakeClient([poll([observed(ACK)])])
        state = self.run_fake(resumed)
        self.assertEqual([], self.sends(resumed))
        self.assertEqual("acknowledged", state["outcome"])
        self.assertEqual(1, state["listener_gaps"])

    def test_crash_after_intent_before_send_remains_consumed(self):
        fake = FakeClient()
        def fail_progress(_):
            raise SystemExit("fixture process boundary")
        with self.assertRaises(SystemExit):
            self.run_fake(fake, progress=fail_progress)
        self.assertEqual([], self.sends(fake))
        resumed = FakeClient()
        state = self.run_fake(resumed, budget=0.03)
        self.assertEqual([], self.sends(resumed))
        self.assertEqual("unknown", state["transport"])

    def test_duplicate_completed_operation_replays_without_starting_helper(self):
        self.run_fake(FakeClient([poll([observed(ACK)])]))
        def forbidden():
            self.fail("must not start another helper")
        state = run_operation(forbidden, self.journal, REQUEST, self.binding, "hello")
        self.assertTrue(state["replayed"])
        self.assertFalse(state["live_check"])

    def test_duplicate_id_with_changed_message_or_target_refused(self):
        self.run_fake(FakeClient(), budget=0.02)
        for binding in ({**self.binding, "message_hash": digest("changed")}, {**self.binding, "cli": "/other"}):
            with self.assertRaises(ValueError):
                run_operation(lambda: self.fail("no helper"), self.journal, REQUEST, binding, "changed")

    def test_identity_change_on_resume_refuses_observation_and_send(self):
        self.run_fake(FakeClient(), budget=0.02)
        changed = FakeClient(identity=[TARGET, 99999, "different"])
        state = self.run_fake(changed)
        self.assertIn("identity changed", state["error"])
        self.assertEqual([], self.sends(changed))

    def test_cancel_wait_preserves_evidence_and_closes_owned_helper(self):
        cancel = threading.Event()
        def cancelling():
            cancel.set()
            return poll([observed("before cancellation")])
        fake = FakeClient([cancelling])
        state = self.run_fake(fake, cancel=cancel)
        self.assertEqual("cancelled", state["outcome"])
        self.assertTrue(fake.closed)
        self.assertEqual(1, len(self.sends(fake)))

    def test_preflight_failure_has_no_committed_send_intent(self):
        fake = FakeClient()
        fake.call = mock.Mock(side_effect=RuntimeError("process_visibility_unavailable"))
        with self.assertRaises(RuntimeError):
            self.run_fake(fake)
        self.assertIsNone(self.journal.get(REQUEST))
        self.assertTrue(fake.closed)

    def test_gap_and_truncation_indicators_survive_ack(self):
        state = self.run_fake(FakeClient([poll([observed(ACK, text_truncated=True)], gaps=[{"reason": "oversized", "start_offset": 1, "end_offset": 50}], event_gap={"oldest_available": 10})]))
        self.assertEqual(1, state["transcript_gap_count"])
        self.assertEqual(1, state["event_gap_count"])
        self.assertTrue(state["text_truncated"])

    def test_journal_deduplicates_repeated_transcript_evidence(self):
        repeat = observed("same", offset=200)
        self.run_fake(FakeClient([poll([repeat]), poll([repeat]), poll([observed(ACK, offset=300)], offset=300)]))
        entries = self.journal.replay(REQUEST)
        self.assertEqual(1, sum(e["event"].get("event") == repeat for e in entries))

    def test_event_capacity_rolls_back_cursor_with_evidence(self):
        self.run_fake(FakeClient(), budget=0.02)
        before = self.journal.get(REQUEST)["state"]
        with mock.patch.object(jm, "MAX_EVENTS", 0):
            with self.assertRaises(ValueError):
                self.journal.checkpoint(REQUEST, {**before, "cursor": {**CURSOR, "offset": 999}}, [{"new": "evidence"}])
        self.assertEqual(before, self.journal.get(REQUEST)["state"])

    def test_exclusive_journal_owner(self):
        with self.assertRaisesRegex(ValueError, "Another workflow"):
            Journal(self.path)

    def test_private_file_permissions_and_symlinks(self):
        self.journal.close()
        db = self.path / "operations.sqlite3"
        db.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "private"):
            Journal(self.path)
        db.chmod(0o600)
        backup = self.path / "saved.sqlite3"
        db.rename(backup)
        db.symlink_to(backup)
        with self.assertRaises(OSError):
            Journal(self.path)

    def test_replay_pagination_is_stable_after_restart(self):
        self.run_fake(FakeClient([poll([observed(ACK)])]))
        first = self.journal.replay(REQUEST, limit=1)
        self.journal.close()
        self.journal = Journal(self.path)
        following = self.journal.replay(REQUEST, after=first[0]["ordinal"])
        self.assertTrue(following)
        self.assertGreater(following[0]["ordinal"], first[0]["ordinal"])

    def test_dispatch_handoff_closes_owned_helper_and_duplicate_never_sends(self):
        fake = FakeClient([poll([observed(ACK)])])
        state = self.run_fake(fake, mode="dispatch")
        self.assertEqual("dispatched", state["outcome"])
        self.assertEqual("handed_off", state["observation_status"])
        self.assertEqual("closed_on_exit", state["peer_inbox_continuity"])
        self.assertFalse(any(n == "wait_claude_session" for n, _ in fake.calls))
        self.assertTrue(fake.closed)
        state = run_operation(lambda: self.fail("no duplicate helper"), self.journal, REQUEST,
                              self.binding, "hello", mode="dispatch")
        self.assertTrue(state["replayed"])
        self.assertFalse(state["live_check"])
        resumed = FakeClient([poll([observed(ACK)])])
        state = self.run_fake(resumed, mode="observe")
        self.assertEqual("acknowledged", state["outcome"])
        self.assertEqual([], self.sends(resumed))
        self.assertEqual(1, state["listener_gaps"])

    def test_observe_without_existing_intent_never_starts_helper(self):
        with self.assertRaisesRegex(ValueError, "existing durable intent"):
            self.run_fake(FakeClient(), mode="observe")
        self.assertIsNone(self.journal.get(REQUEST))

    def test_explicit_follow_after_ack_saves_later_candidate_without_claiming_result(self):
        self.run_fake(FakeClient([poll([observed(ACK)])]))
        fake = FakeClient([poll([observed("The work is done", offset=300)], offset=300)])
        state = self.run_fake(fake, mode="observe", follow=True, budget=0.1)
        self.assertEqual([], self.sends(fake))
        self.assertTrue(state["ack"])
        self.assertEqual("observation_timeout", state["outcome"])
        self.assertEqual("unverified", state["work_completion"])
        self.assertEqual("unverified", state["latest_assistant_evidence"]["correlation"])
        self.assertEqual("The work is done", state["latest_assistant_evidence"]["event"]["text"])
        self.assertEqual(300, self.journal.get(REQUEST)["state"]["cursor"]["offset"])
        self.assertEqual("transcript_only", state["observation_route"])
        self.assertEqual("original_inbox_unavailable", state["peer_inbox_continuity"])

    def test_ack_and_candidate_in_same_transcript_message_are_separate_facts(self):
        state = self.run_fake(FakeClient([poll([observed(ACK + "\nThe requested analysis result")])]))
        self.assertTrue(state["ack"])
        candidate = state["latest_assistant_evidence"]
        self.assertEqual("The requested analysis result", candidate["event"]["text"])
        self.assertEqual("unverified", candidate["correlation"])
        self.assertEqual("unverified", candidate["work_completion"])

    def test_ack_and_candidate_in_same_peer_message_are_separate_facts(self):
        event = {"kind": "peer_reply", "sequence": 1, "text": ACK + "\nThe requested analysis result"}
        state = self.run_fake(FakeClient([poll(peers=[event], sequence=1)]))
        self.assertTrue(state["ack"])
        self.assertEqual("The requested analysis result", state["latest_assistant_evidence"]["event"]["text"])
        self.assertEqual("verified_peer_reply", state["latest_assistant_evidence"]["route"])
        self.assertEqual("unverified", state["work_completion"])

    def test_cancelled_wait_preserves_ack_without_emitting_cancelled_progress(self):
        cancel, updates = threading.Event(), []
        def cancel_with_ack():
            cancel.set()
            return poll([observed(ACK)])
        state = self.run_fake(FakeClient([cancel_with_ack]), cancel=cancel, progress=updates.append)
        self.assertTrue(state["ack"])
        self.assertEqual("cancelled", state["outcome"])
        self.assertEqual("cancelled", state["observation_status"])
        self.assertFalse(any(p["stage"] == "observation" for p in updates))

    def test_progress_callback_failure_cannot_abort_observation_or_erase_ack(self):
        callback = mock.Mock(side_effect=RuntimeError("PRIVATE output fault"))
        fake = FakeClient([poll([observed(ACK)])])
        state = self.run_fake(fake, progress=callback)
        self.assertEqual("acknowledged", state["outcome"])
        self.assertTrue(state["ack"])
        self.assertEqual(1, callback.call_count)
        self.assertEqual(1, len(self.sends(fake)))
        self.assertEqual("RuntimeError", state["output_error"]["kind"])
        self.assertNotIn("PRIVATE", str(state["output_error"]))

    def test_arrival_progress_does_not_expose_producer_metadata(self):
        updates = []
        arrival = {"type": "user", "text": REQUEST, "uuid": "PRIVATE_UUID",
                   "timestamp": {"token": "PRIVATE_TIMESTAMP"}}
        state = self.run_fake(FakeClient([poll([arrival]), poll([observed(ACK)])]), progress=updates.append)
        self.assertTrue(state["arrival"])
        self.assertTrue(any(p.get("arrival_observed") for p in updates))
        self.assertNotIn("PRIVATE", str(updates))

    def test_follow_held_retains_historical_receipt_and_collects_transcript_only(self):
        held = {"kind": "delivery_receipt", "sequence": 1, "message_id": MESSAGE, "status": "held"}
        self.run_fake(FakeClient([poll(peers=[held], sequence=1)]))
        cancel = threading.Event()
        def finish():
            cancel.set()
            return poll(sequence=1)
        fake = FakeClient([poll([observed("Later candidate text", offset=300)], offset=300), finish])
        updates = []
        state = self.run_fake(fake, mode="observe", follow=True, progress=updates.append, cancel=cancel)
        self.assertEqual([held], state["receipts"])
        self.assertTrue(any(p.get("stage") == "observation" and p.get("candidate_assistant_evidence") for p in updates))
        self.assertEqual("transcript_only", state["observation_route"])
        self.assertEqual("original_inbox_unavailable", state["peer_inbox_continuity"])
        self.assertEqual("Later candidate text", state["latest_assistant_evidence"]["event"]["text"])
        self.assertIsNone(state["ack"])
        self.assertEqual([], self.sends(fake))

    def test_rpc_overhead_reserved_instead_of_final_expired_zero_poll(self):
        now, budgets = [0.0], []
        fake = FakeClient()
        original = fake.call
        def costly(name, args, **options):
            if name == "wait_claude_session":
                available = options["deadline"] - now[0]
                budgets.append((args["timeout"], available))
                now[0] += args["timeout"] + 0.2
                if now[0] > options["deadline"]:
                    raise TimeoutError("synthetic response deadline")
            return original(name, args, **options)
        fake.call = costly
        cancel = threading.Event()
        with mock.patch("workflow.time.monotonic", side_effect=lambda: now[0]), \
             mock.patch.object(cancel, "wait", side_effect=lambda seconds: now.__setitem__(0, now[0] + seconds)):
            state = self.run_fake(fake, budget=1.4, cancel=cancel)
        self.assertEqual([(1, 1.4)], budgets)
        self.assertEqual("observation_timeout", state["outcome"])
        self.assertNotIn("error", state)
        self.assertEqual(1, len(self.sends(fake)))

    def test_observation_deadline_preserves_cursor_and_send_deadline_stays_uncertain(self):
        fake = FakeClient([poll([observed("saved")], offset=300), TimeoutError("fixture response deadline")])
        state = self.run_fake(fake)
        self.assertEqual("observation_timeout", state["outcome"])
        self.assertEqual("wait_claude_session", state["error_stage"])
        self.assertEqual(300, state["cursor"]["offset"])
        self.assertEqual(1, len(self.sends(fake)))
        self.journal.close()
        self.temp.cleanup()
        self.setUp()
        state = self.run_fake(FakeClient(failure=TimeoutError("fixture uncertain send")))
        self.assertEqual("observation_error", state["outcome"])
        self.assertEqual("send_claude_message", state["error_stage"])
        self.assertEqual("unknown", state["transport"])
        self.assertIsNone(state["message_id"])

    def test_quiet_observation_emits_progress_without_extending_budget(self):
        now, updates = [0.0], []
        fake = FakeClient()
        original = fake.call
        def quiet(name, args, **options):
            if name == "wait_claude_session":
                now[0] += args["timeout"] + 0.1
            return original(name, args, **options)
        fake.call = quiet
        cancel = threading.Event()
        with mock.patch("workflow.time.monotonic", side_effect=lambda: now[0]), \
             mock.patch.object(cancel, "wait", side_effect=lambda seconds: now.__setitem__(0, now[0] + seconds)):
            state = self.run_fake(fake, budget=20, cancel=cancel, progress=updates.append)
        observations = [p for p in updates if p["stage"] == "observation"]
        self.assertGreaterEqual(len(observations), 2)
        self.assertLessEqual(len(observations), 4)
        self.assertLessEqual(now[0], 20)
        self.assertEqual("observation_timeout", state["outcome"])


if __name__ == "__main__":
    unittest.main()
