"""Concurrent saved-state fixtures; never open production state or helpers."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import client as cm

from client import saved_report
from journal import Journal, JournalReader, encode
from test_workflow import TARGET, REQUEST, CURSOR


class JournalReaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.path.chmod(0o700)
        self.writer = Journal(self.path)
        self.addCleanup(self.writer.close)
        self.binding = {"target": TARGET, "cli": "/fixture/claude", "root": "/fixture/root"}
        self.state = {"outcome": "acknowledged", "transport": "sent", "cursor": CURSOR,
                      "ack": {"event": {"text": "token=PRIVATE_SECRET"}}, "receipts": [],
                      "latest_assistant_evidence": {"event": {"text": "PRIVATE_PAYLOAD"}}}
        self.writer.begin(REQUEST, self.binding, self.state)
        self.event = {"kind": "transcript", "event": {"type": "assistant", "text": "token=PRIVATE_SECRET"}}
        self.writer.checkpoint(REQUEST, self.state, [self.event])

    def test_snapshot_while_writer_owns_lock_and_second_writer_stays_refused(self):
        before = {p.name: p.read_bytes() for p in self.path.iterdir()}
        with JournalReader(self.path) as reader:
            snapshot = reader.snapshot(REQUEST)
            self.assertEqual(self.state, snapshot["operation"]["state"])
            self.assertEqual(self.event, snapshot["evidence"][0]["event"])
            with self.assertRaisesRegex(ValueError, "Another workflow"):
                Journal(self.path)
            with self.assertRaisesRegex(ValueError, "Read-only"):
                reader.checkpoint(REQUEST, {})
            with self.assertRaises(sqlite3.OperationalError):
                reader.db.execute("DELETE FROM operations")
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.path.iterdir()})

    def test_snapshot_never_pairs_new_cursor_with_uncommitted_evidence(self):
        updated = {**self.state, "cursor": {**CURSOR, "offset": 999}}
        with JournalReader(self.path) as reader:
            self.writer.db.execute("BEGIN IMMEDIATE")
            self.writer.db.execute("UPDATE operations SET state=? WHERE id=?", (encode(updated), REQUEST))
            self.writer.db.execute("INSERT INTO events(operation,key,payload) VALUES (?,?,?)", (REQUEST, "fixture-new", encode({"kind": "new"})))
            old = reader.snapshot(REQUEST)
            self.assertEqual(CURSOR, old["operation"]["state"]["cursor"])
            self.assertEqual(1, len(old["evidence"]))
            self.assertFalse(reader.db.in_transaction)
            self.writer.db.commit()
            new = reader.snapshot(REQUEST)
            self.assertEqual(updated["cursor"], new["operation"]["state"]["cursor"])
            self.assertEqual(2, len(new["evidence"]))

    def test_readonly_status_cli_runs_during_writer_without_payload_or_helper(self):
        result = subprocess.run([sys.executable, "-B", str(Path(__file__).with_name("client.py")),
            "--cli", self.binding["cli"], "--target", TARGET, "--claude-root", self.binding["root"],
            "--state-dir", str(self.path), "status", "--request-id", REQUEST], capture_output=True, timeout=3)
        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout)
        self.assertTrue(report["acknowledged"])
        self.assertFalse(report["live_check"])
        self.assertEqual("unverified", report["work_completion"])
        self.assertNotIn(b"PRIVATE", result.stdout)
        self.assertNotIn(b"/fixture", result.stdout)

    def test_replay_text_is_opt_in_and_redacted_and_binding_still_required(self):
        with JournalReader(self.path) as reader:
            snapshot = reader.snapshot(REQUEST, limit=1)
        report = saved_report(snapshot, REQUEST, self.binding, replay=True)
        self.assertNotIn("PRIVATE", json.dumps(report))
        included = saved_report(snapshot, REQUEST, self.binding, replay=True, include_text=True)
        self.assertIn("[REDACTED]", json.dumps(included))
        self.assertNotIn("PRIVATE_SECRET", json.dumps(included))
        with self.assertRaisesRegex(ValueError, "No matching"):
            saved_report(snapshot, REQUEST, {**self.binding, "target": REQUEST})

    def test_report_separates_transcript_backlog_replay_pagination_and_unknown_liveness(self):
        state = {**self.state, "has_more": True, "observation_status": "running",
                 "peer_inbox_continuity": "original_inbox_unavailable", "observation_route": "transcript_only",
                 "preflight_coverage": {"warnings": ["PRIVATE_WARNING"], "gaps": [{"private": "gap"}], "has_more": True}}
        self.writer.checkpoint(REQUEST, state)
        with JournalReader(self.path) as reader:
            snapshot = reader.snapshot(REQUEST)
        report = saved_report(snapshot, REQUEST, self.binding)
        self.assertTrue(report["transcript_has_more"])
        self.assertFalse(report["replay_has_more"])
        self.assertEqual("unknown", report["owner_liveness"])
        self.assertEqual("original_inbox_unavailable", report["peer_inbox_continuity"])
        self.assertEqual("transcript_only", report["observation_route"])
        self.assertEqual({"warning_count": 1, "gap_count": 1, "has_more": True}, report["preflight_coverage"])
        self.assertNotIn("PRIVATE", json.dumps(report))
        self.assertIsNone(cm.state_report(self.state, REQUEST)["transcript_has_more"])

    def test_status_and_replay_failed_output_exit_3_without_helper_or_writes(self):
        before = {p.name: p.read_bytes() for p in self.path.iterdir()}
        for command in ("status", "replay"):
            argv = ["client.py", "--cli", self.binding["cli"], "--target", TARGET,
                    "--claude-root", self.binding["root"], "--state-dir", str(self.path), command,
                    "--request-id", REQUEST]
            with self.subTest(command=command), mock.patch.object(cm.sys, "argv", argv), \
                 mock.patch.object(cm, "emit", return_value=False), \
                 mock.patch.object(cm, "Client", side_effect=AssertionError("no helper")):
                with self.assertRaises(SystemExit) as result:
                    cm.main()
                self.assertEqual(3, result.exception.code)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.path.iterdir()})

    def test_hot_journal_is_refused_without_recovery_writes(self):
        self.writer.checkpoint(REQUEST, {**self.state, "padding": "a" * 200000})
        self.writer.close()
        program = """import json, os, sqlite3, sys
os.umask(0o077)
db = sqlite3.connect(sys.argv[1])
db.execute('PRAGMA cache_size=1')
db.execute('BEGIN IMMEDIATE')
db.execute('UPDATE operations SET state=? WHERE id=?',
           (json.dumps({'outcome': 'uncommitted', 'padding': 'b' * 200000}), sys.argv[2]))
os._exit(17)
"""
        result = subprocess.run([sys.executable, "-B", "-c", program,
            str(self.path / "operations.sqlite3"), REQUEST], capture_output=True, timeout=5)
        self.assertEqual(17, result.returncode, result.stderr)
        sidecar = self.path / "operations.sqlite3-journal"
        self.assertGreater(sidecar.stat().st_size, 512)
        self.assertEqual(bytes.fromhex("d9d505f920a163d7"), sidecar.read_bytes()[:8])
        before = {p.name: (p.stat().st_mode, p.read_bytes()) for p in self.path.iterdir()}
        with self.assertRaisesRegex(ValueError, "recovery required"):
            with JournalReader(self.path) as reader:
                reader.snapshot(REQUEST)
        self.assertEqual(before, {p.name: (p.stat().st_mode, p.read_bytes()) for p in self.path.iterdir()})

    def test_missing_and_unsafe_reader_state_never_created_or_repaired(self):
        missing = self.path / "missing"
        missing.mkdir(mode=0o700)
        with self.assertRaises(FileNotFoundError):
            JournalReader(missing)
        self.assertEqual([], list(missing.iterdir()))
        sidecar = self.path / "operations.sqlite3-journal"
        sidecar.write_bytes(b"fixture")
        sidecar.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            JournalReader(self.path)
        sidecar.unlink()
        database = self.path / "operations.sqlite3"
        database.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            JournalReader(self.path)
        database.chmod(0o600)


if __name__ == "__main__":
    unittest.main()
