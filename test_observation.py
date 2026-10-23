import json
import time
import unittest
from unittest import mock
import test_adapter as fixture

mod = fixture.mod


class ObservationTests(unittest.TestCase):
    setUp = fixture.AdapterTests.setUp
    tearDown = fixture.AdapterTests.tearDown
    read = fixture.AdapterTests.read
    cursor = fixture.AdapterTests.cursor
    assert_code = fixture.AdapterTests.assert_code

    def test_oversized_record_spanning_many_reads_advances_with_explicit_gaps(self):
        oversized = fixture.line(fixture.event("PRIVATE_OMITTED" * 60000))
        self.transcript.write_bytes(oversized + fixture.line(fixture.event("ACK", uuid="next")))
        cursor = self.cursor()
        gaps, events = [], []
        for _ in range(10):
            result = self.read(cursor=cursor)
            self.assertLessEqual(result["bytes_scanned"], mod.MAX_SCAN)
            self.assertGreater(result["cursor"]["offset"], cursor["offset"])
            self.assertNotIn("PRIVATE_OMITTED", json.dumps(result))
            gaps.extend(result["gaps"])
            events.extend(result["events"])
            cursor = result["cursor"]
            if not result["has_more"]:
                break
        self.assertEqual(["ACK"], [e["text"] for e in events])
        self.assertTrue(gaps)
        self.assertEqual(len(oversized), gaps[-1]["end_offset"])
        self.assertTrue(gaps[-1]["record_complete"])
        self.assertNotIn("discarding", cursor)

    def test_unfinished_oversized_record_never_parses_its_continuation(self):
        self.transcript.write_bytes(b"x" * (mod.MAX_SCAN + 50))
        first = self.read(cursor=self.cursor())
        self.assertTrue(first["cursor"]["discarding"])
        with self.transcript.open("ab") as stream:
            stream.write(b"\n" + fixture.line(fixture.event("later")))
        second = self.read(cursor=first["cursor"])
        self.assertEqual(["later"], [e["text"] for e in second["events"]])
        self.assertTrue(second["gaps"])

    def test_hidden_oversized_tool_body_advances_across_budget_boundary(self):
        prefix = fixture.line({"type": "system", "padding": "x" * (mod.MAX_SCAN - 70)})
        hidden = fixture.line({"type": "user", "sessionId": fixture.TARGET,
                               "message": {"content": [{"type": "tool_result", "content": "HIDDEN_TOOL_BODY" * 70000}]}})
        self.transcript.write_bytes(prefix + hidden + fixture.line(fixture.event("VISIBLE_AFTER")))
        cursor, found, gaps = self.cursor(), [], []
        for _ in range(8):
            result = self.read(cursor=cursor)
            self.assertLessEqual(result["bytes_scanned"], mod.MAX_SCAN)
            self.assertGreater(result["cursor"]["offset"], cursor["offset"])
            self.assertNotIn("HIDDEN_TOOL_BODY", json.dumps(result))
            found.extend(e["text"] for e in result["events"])
            gaps.extend(result["gaps"])
            cursor = result["cursor"]
            if not result["has_more"]:
                break
        self.assertEqual(["VISIBLE_AFTER"], found)
        self.assertEqual(self.transcript.stat().st_size, cursor["offset"])
        self.assertTrue(gaps)

    def test_discarded_continuation_cannot_masquerade_as_visible_json(self):
        self.transcript.write_bytes(b"x" * mod.MAX_SCAN)
        first = self.read(cursor=self.cursor())
        self.assertTrue(first["cursor"]["discarding"])
        with self.transcript.open("ab") as stream:
            stream.write(fixture.line(fixture.event("HIDDEN_CONTINUATION")))
            stream.write(fixture.line(fixture.event("VISIBLE_AFTER", uuid="next")))
        second = self.read(cursor=first["cursor"])
        self.assertEqual(["VISIBLE_AFTER"], [e["text"] for e in second["events"]])
        self.assertNotIn("HIDDEN_CONTINUATION", json.dumps(second))
        self.assertTrue(second["gaps"][0]["record_complete"])

    def test_tail_inside_large_record_reports_gap_and_finds_later_event(self):
        self.transcript.write_bytes(fixture.line(fixture.event("x" * (mod.MAX_SCAN * 2))) + fixture.line(fixture.event("last")))
        result = self.read()
        self.assertEqual(["last"], [e["text"] for e in result["events"]])
        self.assertTrue(result["gaps"])

    def test_malformed_record_gap_details_are_bounded(self):
        self.transcript.write_bytes(b"x\n" * 100000)
        result = self.read(cursor=self.cursor())
        self.assertEqual(1, len(result["gaps"]))
        self.assertEqual(self.transcript.stat().st_size, result["cursor"]["offset"])
        self.assertLess(len(json.dumps(result)), 4000)

    def test_recent_event_limit_is_visible_even_when_scan_reaches_eof(self):
        self.transcript.write_bytes(b"".join(fixture.line(fixture.event(str(i), uuid=str(i))) for i in range(4)))
        result = self.read(limit=1)
        self.assertFalse(result["has_more"])
        self.assertIn("event_limit: earlier scanned events omitted", result["warnings"])

    def test_memory_event_retention_gap_is_explicit(self):
        for i in range(205):
            self.adapter.record({"kind": "fixture", "number": i})
        with mock.patch.object(self.adapter, "verify_target", return_value=self.data):
            result = self.adapter.wait_session(fixture.TARGET, timeout=0)
        self.assertEqual({"after_sequence": 0, "oldest_available": 6}, result["event_gap"])
        self.assertEqual(200, len(result["events"]))
        self.assertEqual(205, result["sequence"])

    def test_helper_restart_stream_is_rejected_even_with_zero_sequence(self):
        self.assert_code("event_stream_changed", self.adapter.wait_session, fixture.TARGET,
                         event_stream="another-helper", timeout=0)

    def test_nonzero_sequence_requires_its_stream(self):
        self.assert_code("event_stream_required", self.adapter.wait_session, fixture.TARGET,
                         after_sequence=1, timeout=0)

    def test_cancellation_returns_without_waiting_for_timeout(self):
        self.adapter.stopping.set()
        with mock.patch.object(self.adapter, "verify_target", return_value=self.data):
            started = time.monotonic()
            result = self.adapter.wait_session(fixture.TARGET, timeout=55)
        self.assertTrue(result["cancelled"])
        self.assertLess(time.monotonic() - started, 0.5)

    def test_cursor_and_gap_progress_are_not_timeouts(self):
        for payload in (fixture.line({"type": "system", "padding": "hidden"}),
                        fixture.line({"type": "api-request", "padding": "x" * (mod.MAX_RECORD + 5)})):
            with self.subTest(size=len(payload)):
                self.transcript.write_bytes(payload)
                cursor = self.cursor()
                with mock.patch.object(self.adapter, "verify_target", return_value=self.data):
                    result = self.adapter.wait_session(fixture.TARGET, timeout=55, cursor=cursor)
                self.assertGreater(result["transcript"]["cursor"]["offset"], cursor["offset"])
                self.assertTrue(result["progress"])
                self.assertFalse(result["timed_out"])
                self.assertEqual([], result["transcript"]["events"])

    def test_wait_does_not_rescan_after_deadline(self):
        cursor = self.cursor()
        now = [0.0]
        snapshot = {"events": [], "gaps": [], "cursor": cursor}
        def scan(*_):
            now[0] += 0.9
            return snapshot
        def pause(seconds):
            now[0] += seconds
        with mock.patch.object(mod.time, "monotonic", side_effect=lambda: now[0]), \
             mock.patch.object(self.adapter, "read_session", side_effect=scan) as read, \
             mock.patch.object(self.adapter.changed, "wait", side_effect=pause), \
             mock.patch.object(self.adapter, "verify_target", return_value=self.data):
            result = self.adapter.wait_session(fixture.TARGET, timeout=1, cursor=cursor)
        self.assertEqual(1, read.call_count)
        self.assertTrue(result["timed_out"])
        self.assertFalse(result["progress"])


if __name__ == "__main__":
    unittest.main()
