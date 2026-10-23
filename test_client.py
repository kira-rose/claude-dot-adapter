"""Owned Python stub helpers only; no Claude executable is ever started."""
import json
import os
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock
import client as cm

from client import BoundedOutput, Cancelled, Client

TARGET = "11111111-1111-4111-8111-111111111111"
INIT = '''import sys,json,time
first=json.loads(sys.stdin.readline())
print(json.dumps({"jsonrpc":"2.0","id":first["id"],"result":{}}),flush=True)
sys.stdin.readline()
request=json.loads(sys.stdin.readline())
'''


class ClientTests(unittest.TestCase):
    def cli_cancel_fixture(self, *, blocked, cancel_in_report=False):
        import tempfile
        from pathlib import Path
        from journal import JournalReader
        from test_workflow import FakeClient, REQUEST, ACK, poll, observed
        read_fd, write_fd = os.pipe()
        try:
            if blocked:
                os.set_blocking(write_fd, False)
                while True:
                    try:
                        os.write(write_fd, b"x" * 8192)
                    except BlockingIOError:
                        break
                os.set_blocking(write_fd, True)
            with tempfile.TemporaryDirectory() as folder:
                path = Path(folder)
                path.chmod(0o700)
                message = path / "message.txt"
                message.write_text("fixture request")
                stdout = mock.Mock()
                stdout.fileno.return_value = write_fd
                fake = FakeClient()
                class CancelOnAckOutput(BoundedOutput):
                    def __call__(self, value):
                        if value.get("stage") == "observation":
                            self.cancel.set()
                        return super().__call__(value)
                def factory(*args, **kwargs):
                    def cancel_with_ack():
                        if not cancel_in_report:
                            kwargs["cancel"].set()
                        return poll([observed(ACK + "\nPRIVATE_RESULT")])
                    fake.polls = [cancel_with_ack]
                    return fake
                argv = ["client.py", "--cli", "/fixture/claude", "--target", TARGET,
                        "--claude-root", "/fixture/root", "--state-dir", folder, "send",
                        "--request-id", REQUEST, "--authorized-one-message", "--message-file", str(message), "--wait", "1"]
                with mock.patch.object(cm.sys, "argv", argv), mock.patch.object(cm.sys, "stdout", stdout), \
                     mock.patch.object(cm, "Client", side_effect=factory), mock.patch.object(cm, "_emitter", None), \
                     mock.patch.object(cm, "BoundedOutput", CancelOnAckOutput if cancel_in_report else BoundedOutput):
                    # Keep the real readiness implementation while replacing the helper factory.
                    cm.Client._ready = Client._ready
                    with self.assertRaises(SystemExit) as result:
                        cm.main()
                    self.assertEqual(130, result.exception.code)
                with JournalReader(path) as reader:
                    state = reader.get(REQUEST)["state"]
                self.assertEqual("cancelled", state["outcome"])
                self.assertTrue(state["ack"])
                self.assertEqual(1, sum(n == "send_claude_message" for n, _ in fake.calls))
                if blocked:
                    self.assertTrue(state["output_error"])
                else:
                    os.set_blocking(read_fd, False)
                    data = os.read(read_fd, 65536)
                    final = json.loads(data.splitlines()[-1])
                    self.assertEqual("cancelled", final["outcome"])
                    self.assertTrue(final["acknowledged"])
                    self.assertTrue(final["candidate_assistant_evidence"])
                    self.assertNotIn(b"PRIVATE", data)
                    self.assertNotIn(b"/fixture", data)
        finally:
            os.close(write_fd)
            os.close(read_fd)

    def test_cli_cancellation_emits_final_metadata_and_exits_130(self):
        self.cli_cancel_fixture(blocked=False)

    def test_cli_cancellation_with_blocked_stdout_still_exits_130(self):
        self.cli_cancel_fixture(blocked=True)

    def test_cli_cancellation_during_progress_before_first_byte_can_emit_final(self):
        self.cli_cancel_fixture(blocked=False, cancel_in_report=True)

    def stub(self, program, **options):
        real = subprocess.Popen
        self.children = []
        def launch(_args, **kwargs):
            proc = real([sys.executable, "-B", "-u", "-c", program], **kwargs)
            self.children.append(proc)
            return proc
        with mock.patch("client.subprocess.Popen", side_effect=launch):
            return Client(sys.executable, TARGET, **options)

    def test_partial_line_obeys_entire_response_deadline(self):
        client = self.stub(INIT + 'sys.stdout.write("{\\\"id\\\":");sys.stdout.flush();time.sleep(30)')
        client.timeout = 0.2  # Test response timing independently of process startup.
        try:
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                client.request("fixture", {})
            self.assertLess(time.monotonic() - started, 0.8)
            with self.assertRaisesRegex(RuntimeError, "unusable"):
                client.request("fixture", {})
        finally:
            client.close()
        self.assertIsNotNone(self.children[0].poll())

    def test_initialization_failure_reaps_helper(self):
        with self.assertRaises(TimeoutError):
            self.stub('import sys,time;sys.stdin.readline();sys.stdout.write("{");sys.stdout.flush();time.sleep(30)', timeout=0.15)
        self.assertIsNotNone(self.children[0].poll())

    def test_helper_exit_before_reply_is_not_retried(self):
        client = self.stub(INIT + 'sys.exit(0)')
        try:
            with self.assertRaisesRegex(RuntimeError, "exited"):
                client.request("fixture", {})
            self.assertEqual(1, len(self.children))
        finally:
            client.close()

    def test_cancellation_interrupts_partial_response_and_cleanup(self):
        cancel = threading.Event()
        client = self.stub(INIT + 'sys.stdout.write("{");sys.stdout.flush();time.sleep(30)', cancel=cancel)
        timer = threading.Timer(0.08, cancel.set)
        timer.start()
        try:
            with self.assertRaises(Cancelled):
                client.request("fixture", {})
        finally:
            client.close()
            timer.join()
        self.assertIsNotNone(self.children[0].poll())

    def test_fragmented_successful_response(self):
        client = self.stub(INIT + 'payload=json.dumps({"id":request["id"],"result":{"ok":True}})+"\\n"\nfor ch in payload:\n sys.stdout.write(ch);sys.stdout.flush()')
        try:
            self.assertEqual({"ok": True}, client.request("fixture", {}))
        finally:
            client.close()

    def test_oversized_response_is_bounded(self):
        client = self.stub(INIT + 'sys.stdout.write("x"*1100000);sys.stdout.flush();time.sleep(30)')
        try:
            with self.assertRaisesRegex(RuntimeError, "oversized"):
                client.request("fixture", {})
        finally:
            client.close()

    def test_wrong_response_id_marks_client_unusable(self):
        client = self.stub(INIT + 'print(json.dumps({"id":999,"result":{}}),flush=True)')
        try:
            with self.assertRaisesRegex(RuntimeError, "ID mismatch"):
                client.request("fixture", {})
        finally:
            client.close()

    def test_concurrent_request_rejected_before_write_and_first_stays_usable(self):
        client = self.stub(INIT + 'print(json.dumps({"id":request["id"],"result":{"first":True}}),flush=True)')
        entered, release = threading.Event(), threading.Event()
        real_write = client._write
        def paused_write(*args):
            entered.set()
            self.assertTrue(release.wait(2))
            return real_write(*args)
        replies = []
        with mock.patch.object(client, "_write", side_effect=paused_write):
            worker = threading.Thread(target=lambda: replies.append(client.request("first", {})))
            worker.start()
            try:
                self.assertTrue(entered.wait(2))
                with self.assertRaisesRegex(RuntimeError, "Another request"):
                    client.request("second", {})
                self.assertEqual(2, client.next_id)
                release.set()
                worker.join(2)
                self.assertFalse(worker.is_alive())
                self.assertEqual([{"first": True}], replies)
                self.assertFalse(client.broken)
            finally:
                release.set()
                worker.join(2)
                client.close()

    def test_full_stdout_pipe_cannot_block_workflow_or_destroy_ack(self):
        from journal import Journal, digest
        from test_workflow import FakeClient, REQUEST, ACK, TARGET, poll, observed
        from workflow import run_operation
        import tempfile
        from pathlib import Path
        read_fd, write_fd = os.pipe()
        try:
            os.set_blocking(write_fd, False)
            while True:
                try:
                    os.write(write_fd, b"x" * 8192)
                except BlockingIOError:
                    break
            os.set_blocking(write_fd, True)
            output = BoundedOutput(write_fd, timeout=0.02)
            with tempfile.TemporaryDirectory() as folder:
                Path(folder).chmod(0o700)
                with Journal(folder) as journal:
                    fake = FakeClient([poll([observed(ACK)])])
                    binding = {"target": TARGET, "message_hash": digest("hello")}
                    started = time.monotonic()
                    state = run_operation(lambda: fake, journal, REQUEST, binding, "hello", budget=0.3, progress=output)
                    self.assertLess(time.monotonic() - started, 0.8)
                    self.assertEqual("acknowledged", state["outcome"])
                    self.assertTrue(journal.get(REQUEST)["state"]["ack"])
                    self.assertIn("output_error", journal.get(REQUEST)["state"])
                    self.assertEqual(1, sum(n == "send_claude_message" for n, _ in fake.calls))
                    self.assertFalse(output({"later": "never appended"}))
        finally:
            os.close(write_fd)
            os.close(read_fd)

    def test_partial_output_disables_stream_and_cancellation_is_bounded(self):
        read_fd, write_fd = os.pipe()
        try:
            output = BoundedOutput(write_fd, timeout=0.03)
            self.assertFalse(output({"large": "x" * 200000}))
            self.assertTrue(output.error["partial_line"])
            os.set_blocking(read_fd, False)
            fragments = bytearray()
            while True:
                try:
                    fragments.extend(os.read(read_fd, 65536))
                except BlockingIOError:
                    break
            self.assertFalse(fragments.endswith(b"\n"))
            self.assertFalse(output({"second": True}))
            with self.assertRaises(BlockingIOError):
                os.read(read_fd, 65536)
            cancel = threading.Event()
            cancel.set()
            cancelled = BoundedOutput(write_fd, cancel=cancel)
            self.assertFalse(cancelled({"cancelled": True}))
            self.assertEqual("Cancelled", cancelled.error["kind"])
        finally:
            os.close(write_fd)
            os.close(read_fd)

    def test_closed_output_and_restore_failure_never_escape(self):
        read_fd, write_fd = os.pipe()
        os.close(write_fd)
        try:
            output = BoundedOutput(write_fd)
            self.assertFalse(output({"closed": True}))
            self.assertEqual("OSError", output.error["kind"])
            self.assertFalse(output({"later": True}))
        finally:
            os.close(read_fd)
        read_fd, write_fd = os.pipe()
        try:
            real = os.set_blocking
            def fail_restore(fd, blocking):
                if blocking:
                    raise OSError("synthetic restore failure")
                return real(fd, blocking)
            output = BoundedOutput(write_fd)
            with mock.patch("client.os.set_blocking", side_effect=fail_restore):
                self.assertFalse(output({"written": True}))
            self.assertFalse(output({"never_append": True}))
        finally:
            os.close(write_fd)
            os.close(read_fd)


if __name__ == "__main__":
    unittest.main()
