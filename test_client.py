"""Owned Python stub helpers only; no Claude executable is ever started."""
import json
import os
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

from client import Cancelled, Client

TARGET = "11111111-1111-4111-8111-111111111111"
INIT = '''import sys,json,time
first=json.loads(sys.stdin.readline())
print(json.dumps({"jsonrpc":"2.0","id":first["id"],"result":{}}),flush=True)
sys.stdin.readline()
request=json.loads(sys.stdin.readline())
'''


class ClientTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
