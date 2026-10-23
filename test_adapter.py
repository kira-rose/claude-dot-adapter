"""Offline security/correctness checks; never contact a real Claude session.

Run from this directory: python3 -B -m unittest discover -s . -p test_adapter.py
"""
from __future__ import annotations

import importlib.util
from contextlib import contextmanager
import io
import json
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

MODULE_PATH = Path(__file__).with_name("adapter.py")
SPEC = importlib.util.spec_from_file_location("claude_direct_adapter_tested", MODULE_PATH)
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)
CLIENT_SPEC = importlib.util.spec_from_file_location("claude_direct_client_tested", MODULE_PATH.with_name("client.py"))
client_mod = importlib.util.module_from_spec(CLIENT_SPEC)
with mock.patch.dict(sys.modules, {"adapter": mod}):
    CLIENT_SPEC.loader.exec_module(client_mod)

TARGET = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
SOURCE = "33333333-3333-4333-8333-333333333333"


def event(text="visible", **extra):
    return {"type": "assistant", "sessionId": TARGET,
            "uuid": "44444444-4444-4444-8444-444444444444",
            "message": {"content": [{"type": "text", "text": text}]}, **extra}


def line(row):
    return (json.dumps(row, ensure_ascii=False) + "\n").encode()


class FakeSocket:
    def __init__(self, fail_after=None):
        self.written = bytearray()
        self.fail_after = fail_after
        self.connected = None
        self.shutdown_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def settimeout(self, timeout):
        self.timeout = timeout

    def connect(self, path):
        self.connected = path

    def send(self, data):
        if self.fail_after is not None:
            remaining = self.fail_after - len(self.written)
            if remaining <= 0:
                raise OSError("fixture write failure")
            data = data[:remaining]
        self.written.extend(data)
        return len(data)

    def shutdown(self, how):
        self.shutdown_calls.append(how)


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="claude-adapter-test-")
        self.root = Path(self.tmp.name)
        (self.root / "sessions").mkdir(mode=0o700)
        (self.root / "projects").mkdir(mode=0o700)
        self.project = self.root / "projects" / "fixture"
        self.project.mkdir(mode=0o700)
        self.transcript = self.project / (TARGET + ".jsonl")
        self.transcript.write_bytes(b"")
        self.adapter = mod.Adapter(self.root, sys.executable, TARGET, SOURCE)
        self.data = {"sessionId": TARGET, "pid": 424242, "procStart": "fixture-start",
                     "cwd": str(self.project), "messagingSocketPath": str(self.root / "fixture.sock"),
                     "version": "2.1.299", "peerProtocol": 1, "status": "idle", "name": "Fixture"}

    def tearDown(self):
        self.adapter.close()
        self.tmp.cleanup()

    def assert_code(self, code, fn, *args, **kwargs):
        with self.assertRaises(mod.AdapterError) as caught:
            fn(*args, **kwargs)
        self.assertEqual(code, caught.exception.code)

    def read(self, *args, **kwargs):
        with mock.patch.object(self.adapter, "verify_target", return_value=self.data):
            return self.adapter.read_session(TARGET, *args, **kwargs)

    def cursor(self, offset=0):
        info = self.transcript.stat()
        return {"offset": offset, "inode": info.st_ino, "device": info.st_dev}

    def send(self, sock=None, pid=None, identity_updates=None, text="Hello fixture"):
        sock = sock or FakeSocket()
        def inbox(_):
            self.adapter.inbox = self.root / "fake-inbox.sock"
        verify = ({"side_effect": identity_updates} if identity_updates else {"return_value": self.data})
        with mock.patch.object(self.adapter, "verify_target", **verify), \
             mock.patch.object(self.adapter, "start_inbox", side_effect=inbox), \
             mock.patch.object(mod.socket, "socket", return_value=sock), \
             mock.patch.object(mod, "peer_pid", return_value=self.data["pid"] if pid is None else pid):
            return self.adapter.send_message(TARGET, text), sock

    @contextmanager
    def background_identity(self, version="2.1.299", kind="bg"):
        """Synthetic official background peer; never inspect or contact Claude."""
        self.adapter.cli = Path("/Applications/Claude.app/Contents/MacOS/claude")
        self.data.update(version=version, kind=kind)
        self.data["messagingSocketPath"] = f"/tmp/cc-socks/{self.data['pid']}.sock"
        registry = self.root / "sessions" / f"{self.data['pid']}.json"
        registry.write_text(json.dumps(self.data))
        socket_path = Path(self.data["messagingSocketPath"])
        socket_info = SimpleNamespace(st_mode=stat.S_IFSOCK | 0o600, st_uid=os.getuid())
        real_lstat, real_owned_dir = Path.lstat, mod.owned_dir
        def lstat(path, *args, **kwargs):
            return socket_info if path == socket_path else real_lstat(path, *args, **kwargs)
        def owned_dir(path):
            if path != socket_path.parent:
                real_owned_dir(path)
        def pidpath(pid, buffer, size):
            self.assertEqual(self.data["pid"], pid)
            self.assertEqual(4096, size)
            buffer.value = os.fsencode(self.adapter.cli)
            return len(buffer.value)
        library = mock.Mock()
        library.proc_pidpath.side_effect = pidpath
        display_name = subprocess.CompletedProcess([], 0, stdout="claude bg-spare\n")
        with mock.patch.object(mod.sys, "platform", "darwin"), \
             mock.patch.object(mod.ctypes, "CDLL", return_value=library), \
             mock.patch.object(mod.subprocess, "run", return_value=display_name) as ps, \
             mock.patch.object(self.adapter, "process_start", return_value=self.data["procStart"]) as start, \
             mock.patch.object(mod, "owned_dir", side_effect=owned_dir), \
             mock.patch.object(Path, "lstat", lstat):
            yield SimpleNamespace(library=library, ps=ps, start=start,
                                  registry=registry, socket_info=socket_info)

    def test_qualified_macos_versions_keep_interactive_and_background_identity(self):
        for version in ("2.1.299", "2.1.308"):
            for kind in ("interactive", "bg"):
                with self.subTest(version=version, kind=kind), self.background_identity(version, kind) as fixture:
                    self.adapter.pinned = None
                    before = fixture.registry.read_bytes()
                    self.assertEqual(self.data, self.adapter.verify_target(pin=True))
                    self.assertEqual(self.adapter.identity(self.data), self.adapter.pinned)
                    self.assertEqual(before, fixture.registry.read_bytes())
                    fixture.ps.assert_not_called()  # No Claude or lifecycle command.

    def test_runtime_protocol_allowlist_rejects_unknown_and_malformed_pairs(self):
        pairs = [(version, 1) for version in (None, True, 308, [], {}, "2.1.300", "2.1.307", "2.1.309", "2.1.308-dev", " 2.1.308")]
        pairs += [(version, protocol) for version in ("2.1.299", "2.1.308")
                  for protocol in (None, True, False, 1.0, "1", [], {}, 0, 2)]
        for version, protocol in pairs:
            with self.subTest(version=version, protocol=protocol), self.background_identity() as fixture:
                fixture.registry.write_text(json.dumps({**self.data, "version": version, "peerProtocol": protocol}))
                self.assert_code("unsupported_protocol", self.adapter.verify_target)
                fixture.start.assert_not_called()
                fixture.library.proc_pidpath.assert_not_called()

    def test_new_runtime_not_implicitly_qualified_on_linux_or_other_platforms(self):
        for platform, version, allowed in (("linux", "2.1.299", True),
                                           ("linux", "2.1.308", False),
                                           ("win32", "2.1.308", False),
                                           ("freebsd", "2.1.299", False)):
            with self.subTest(platform=platform, version=version), self.background_identity(version) as fixture, \
                 mock.patch.object(mod.sys, "platform", platform), \
                 mock.patch.object(self.adapter, "verify_executable") as executable:
                if allowed:
                    self.assertEqual(self.data, self.adapter.verify_target())
                    executable.assert_called_once_with(self.data["pid"])
                else:
                    self.assert_code("unsupported_protocol", self.adapter.verify_target)
                    fixture.start.assert_not_called()
                    executable.assert_not_called()

    def test_pid_type_and_signed_pid_t_bounds_rejected_before_process_queries(self):
        for pid in (None, True, False, 2.0, "2", [], {}, -1, 0, 1, 2**31, 2**64):
            with self.subTest(pid=pid), mock.patch.object(self.adapter, "registry", return_value={**self.data, "pid": pid}), \
                 mock.patch.object(self.adapter, "process_start") as start, \
                 mock.patch.object(self.adapter, "verify_executable") as executable:
                self.assert_code("invalid_pid", self.adapter.verify_target)
                start.assert_not_called()
                executable.assert_not_called()

    def test_new_runtime_preserves_registry_start_executable_and_socket_guards(self):
        mutations = [({"sessionId": OTHER}, "target_not_unique"),
                     ({"pid": 424243}, "registry_identity_mismatch"),
                     ({"procStart": "replacement-start"}, "process_identity_changed"),
                     ({"messagingSocketPath": "/tmp/untrusted/424242.sock"}, "unsafe_socket_path"),
                     ({"messagingSocketPath": "/tmp/cc-socks/424243.sock"}, "unsafe_socket_path")]
        for change, code in mutations:
            with self.subTest(change=change), self.background_identity("2.1.308", "interactive") as fixture:
                fixture.registry.write_text(json.dumps({**self.data, **change}))
                self.assert_code(code, self.adapter.verify_target)
        with self.background_identity("2.1.308", "interactive") as fixture:
            fixture.socket_info.st_uid = os.getuid() + 1
            self.assert_code("unsafe_socket", self.adapter.verify_target)
        with self.background_identity("2.1.308", "interactive") as fixture:
            fixture.socket_info.st_mode = stat.S_IFSOCK | 0o666
            self.assert_code("unsafe_socket", self.adapter.verify_target)
        with self.background_identity("2.1.308", "interactive") as fixture:
            def other_executable(pid, buffer, size):
                buffer.value = b"/untrusted/claude"
                return len(buffer.value)
            fixture.library.proc_pidpath.side_effect = other_executable
            self.assert_code("executable_mismatch", self.adapter.verify_target)

    def test_runtime_switch_after_pin_rejected_even_between_qualified_versions(self):
        with self.background_identity("2.1.299", "interactive") as fixture:
            self.adapter.verify_target(pin=True)
            fixture.registry.write_text(json.dumps({**self.data, "version": "2.1.308"}))
            self.assert_code("session_identity_changed", self.adapter.verify_target)

    def test_new_runtime_connected_peer_or_runtime_change_prevents_all_writes(self):
        for switch in (False, True):
            with self.subTest(runtime_switch=switch), self.background_identity("2.1.308", "interactive") as fixture, \
                 mock.patch.object(self.adapter, "start_inbox"), \
                 mock.patch.object(mod.socket, "socket", return_value=FakeSocket()) as factory:
                self.adapter.pinned = None
                def peer(sock):
                    if switch:
                        fixture.registry.write_text(json.dumps({**self.data, "version": "2.1.299"}))
                    return self.data["pid"] if switch else self.data["pid"] + 1
                with mock.patch.object(mod, "peer_pid", side_effect=peer):
                    self.assert_code("session_identity_changed" if switch else "socket_peer_changed",
                                     self.adapter.send_message, TARGET, "Synthetic compatibility fixture")
                self.assertEqual(b"", factory.return_value.written)
                self.assertEqual({}, self.adapter.outstanding)

    def test_new_runtime_synthetic_send_preserves_plain_text_wire_and_interactive_kind(self):
        sock = FakeSocket()
        with self.background_identity("2.1.308", "interactive") as fixture, \
             mock.patch.object(self.adapter, "start_inbox"), \
             mock.patch.object(mod.socket, "socket", return_value=sock), \
             mock.patch.object(mod, "peer_pid", return_value=self.data["pid"]):
            self.adapter.inbox = self.root / "synthetic-reply.sock"
            before = fixture.registry.read_bytes()
            result = self.adapter.send_message(TARGET, "Synthetic plain text")
            wire = json.loads(sock.written)
            self.assertEqual({"type", "msgV", "session_id", "uuid", "msg_id", "from", "priority", "message"}, set(wire))
            self.assertEqual(("user", 1, TARGET, "next"), (wire["type"], wire["msgV"], wire["session_id"], wire["priority"]))
            self.assertEqual("user", wire["message"]["role"])
            self.assertIn("not user consent or permission approval", wire["message"]["content"])
            self.assertTrue(wire["message"]["content"].endswith("Synthetic plain text"))
            self.assertEqual("unknown", result["delivery"])
            self.assertEqual(before, fixture.registry.read_bytes())
            fixture.ps.assert_not_called()

    def test_new_api_request_transcript_records_and_hidden_blocks_never_leave_reads(self):
        hidden = "DO_NOT_EXPOSE_REQUEST_SYSTEM_TOOLS_OR_PARAMETERS"
        rows = [{"type": kind, "sessionId": TARGET, "version": "2.1.308",
                 "params": {"system": hidden, "tools": [hidden]}, "data": hidden,
                 "message": {"role": "assistant", "content": hidden}}
                for kind in ("api-request", "api-request-shape", "api-request-blob")]
        rows += [event("Sidechain secret", isSidechain=True),
                 event(message={"content": [{"type": "thinking", "thinking": hidden},
                                             {"type": "tool_use", "name": "synthetic_tool", "input": hidden},
                                             {"type": "tool_result", "content": hidden},
                                             {"type": "text", "text": "Visible synthetic answer"}]}, version="2.1.308")]
        self.transcript.write_bytes(b"".join(line(row) for row in rows))
        with self.background_identity("2.1.308", "interactive"):
            result = self.adapter.read_session(TARGET, cursor=self.cursor(), limit=10)
        encoded = json.dumps(result)
        self.assertNotIn(hidden, encoded)
        self.assertNotIn("Sidechain secret", encoded)
        self.assertEqual(["Visible synthetic answer"], [row["text"] for row in result["events"]])
        self.assertEqual(self.transcript.stat().st_size, result["cursor"]["offset"])

    def test_new_runtime_receipt_statuses_remain_separate_from_peer_ack(self):
        self.data["version"] = "2.1.308"
        self.adapter.outstanding["synthetic-message"] = {}
        for status in ("held", "denied", "expired", "delivered", "refused", "dropped"):
            self.adapter.receive_frame({"type": "control", "action": "peer_message_status",
                                       "orig_msg_id": "synthetic-message", "status": status}, self.data)
        self.adapter.receive_frame({"type": "control", "action": "peer_message_status", "orig_msg_id": "synthetic-message",
                                    "status": "expired", "status_detail": "refused"}, self.data)
        self.assertEqual(["held", "denied", "expired", "delivered", "refused", "dropped", "refused"],
                         [row["status"] for row in self.adapter.events])
        self.assertTrue(all(row["kind"] == "delivery_receipt" for row in self.adapter.events))

    def test_background_display_name_differs_but_os_executable_matches(self):
        with self.background_identity() as fixture:
            self.assertEqual(self.data, self.adapter.verify_target(pin=True))
            self.assertEqual(self.adapter.identity(self.data), self.adapter.pinned)
            fixture.ps.assert_not_called()
            self.assertEqual(1, fixture.library.proc_pidpath.call_count)

    def test_darwin_mismatched_executable_rejected_even_if_display_name_matches(self):
        with self.background_identity() as fixture:
            fixture.ps.return_value.stdout = str(self.adapter.cli)
            def wrong_path(pid, buffer, size):
                buffer.value = b"/untrusted/claude"
                return len(buffer.value)
            fixture.library.proc_pidpath.side_effect = wrong_path
            self.assert_code("executable_mismatch", self.adapter.verify_target)
            fixture.ps.assert_not_called()

    def test_darwin_failed_truncated_or_inconsistent_path_fails_closed(self):
        cases = [(b"", 0), (b"", -1), (b"/claude", 4096), (b"/claude", 4097),
                 (b"/claude", 6), (b"/claude\0suffix", 14),
                 (b"x" * 4096, 4095), (b"claude", 6)]
        for value, length in cases:
            with self.subTest(value=value[:30], length=length), self.background_identity() as fixture:
                def broken_path(pid, buffer, size):
                    buffer.raw = value.ljust(size, b"\0")
                    return length
                fixture.library.proc_pidpath.side_effect = broken_path
                self.assert_code("process_visibility_unavailable", self.adapter.verify_target)
                fixture.ps.assert_not_called()

    def test_darwin_library_symbol_or_call_unavailable_fails_closed(self):
        for error in (OSError("unavailable"), AttributeError("missing symbol")):
            with self.subTest(error=type(error).__name__), self.background_identity() as fixture, \
                 mock.patch.object(mod.ctypes, "CDLL", side_effect=error):
                self.assert_code("process_visibility_unavailable", self.adapter.verify_target)
                fixture.ps.assert_not_called()
        with self.background_identity() as fixture:
            fixture.library.proc_pidpath.side_effect = PermissionError("denied")
            self.assert_code("process_visibility_unavailable", self.adapter.verify_target)
            fixture.ps.assert_not_called()

    def test_linux_uses_exact_proc_executable_without_display_name_fallback(self):
        for path, code in ((str(self.adapter.cli), None), ("/untrusted/claude", "executable_mismatch"),
                           (str(self.adapter.cli) + " (deleted)", "executable_mismatch"),
                           ("", "process_visibility_unavailable"), ("claude", "process_visibility_unavailable")):
            with self.subTest(path=path), mock.patch.object(mod.sys, "platform", "linux"), \
                 mock.patch.object(mod.os, "readlink", return_value=path) as readlink, \
                 mock.patch.object(mod.subprocess, "run") as ps:
                if code:
                    self.assert_code(code, self.adapter.verify_executable, self.data["pid"])
                else:
                    self.adapter.verify_executable(self.data["pid"])
                readlink.assert_called_once_with(f"/proc/{self.data['pid']}/exe")
                ps.assert_not_called()

    def test_linux_unavailable_executable_fails_closed(self):
        for error in (FileNotFoundError("exited"), PermissionError("denied")):
            with self.subTest(error=type(error).__name__), mock.patch.object(mod.sys, "platform", "linux"), \
                 mock.patch.object(mod.os, "readlink", side_effect=error), \
                 mock.patch.object(mod.subprocess, "run") as ps:
                self.assert_code("process_visibility_unavailable", self.adapter.verify_executable, self.data["pid"])
                ps.assert_not_called()

    def test_executable_identity_fails_closed_on_unsupported_platform(self):
        with mock.patch.object(mod.sys, "platform", "unsupported"), mock.patch.object(mod.subprocess, "run") as ps:
            self.assert_code("unsupported_platform", self.adapter.verify_executable, self.data["pid"])
            ps.assert_not_called()

    @unittest.skipUnless(sys.platform == "darwin" or sys.platform.startswith("linux"), "OS path API required")
    def test_native_executable_api_resolves_only_this_owned_python_process(self):
        self.adapter.verify_executable(os.getpid())

    def test_background_pid_reuse_rejected_before_executable_lookup(self):
        with self.background_identity() as fixture:
            fixture.start.return_value = "replacement-start"
            self.assert_code("process_identity_changed", self.adapter.verify_target)
            fixture.library.proc_pidpath.assert_not_called()

    def test_background_wrong_registry_session_or_filename_mapping_rejected(self):
        for change, code in (({"sessionId": OTHER}, "target_not_unique"),
                             ({"pid": self.data["pid"] + 1}, "registry_identity_mismatch")):
            with self.subTest(change=change), self.background_identity() as fixture:
                fixture.registry.write_text(json.dumps({**self.data, **change}))
                self.assert_code(code, self.adapter.verify_target)
                fixture.library.proc_pidpath.assert_not_called()

    def test_background_wrong_registry_owner_rejected(self):
        real_fstat = os.fstat
        def wrong_owner(fd):
            values = list(real_fstat(fd))
            values[4] = os.getuid() + 1
            return os.stat_result(values)
        with self.background_identity() as fixture, mock.patch.object(mod.os, "fstat", side_effect=wrong_owner):
            self.assert_code("target_not_unique", self.adapter.verify_target)
            fixture.library.proc_pidpath.assert_not_called()

    def test_background_socket_owner_type_and_permissions_still_required(self):
        for mode, uid in ((stat.S_IFSOCK | 0o600, os.getuid() + 1),
                          (stat.S_IFSOCK | 0o666, os.getuid()), (stat.S_IFREG | 0o600, os.getuid())):
            with self.subTest(mode=mode, uid=uid), self.background_identity() as fixture:
                fixture.socket_info.st_mode, fixture.socket_info.st_uid = mode, uid
                self.assert_code("unsafe_socket", self.adapter.verify_target)

    def test_background_socket_path_binding_still_required(self):
        for path in ("/tmp/untrusted/424242.sock", "/tmp/cc-socks/999.sock"):
            with self.subTest(path=path), self.background_identity() as fixture:
                fixture.registry.write_text(json.dumps({**self.data, "messagingSocketPath": path}))
                self.assert_code("unsafe_socket_path", self.adapter.verify_target)

    def test_background_socket_peer_mismatch_or_post_connect_pid_reuse_prevents_write(self):
        for reused, code in ((False, "socket_peer_changed"), (True, "process_identity_changed")):
            sock = FakeSocket()
            with self.subTest(reused=reused), self.background_identity() as fixture, \
                 mock.patch.object(self.adapter, "start_inbox"), \
                 mock.patch.object(mod.socket, "socket", return_value=sock), \
                 mock.patch.object(mod, "peer_pid", return_value=self.data["pid"] if reused else self.data["pid"] + 1):
                if reused:
                    fixture.start.side_effect = [self.data["procStart"], "replacement-start"]
                self.assert_code(code, self.adapter.send_message, TARGET, "Synthetic test only")
                self.assertEqual(b"", sock.written)
                self.assertEqual({}, self.adapter.outstanding)

    def test_background_executable_mismatch_prevents_socket_creation(self):
        with self.background_identity() as fixture, mock.patch.object(mod.socket, "socket") as socket_factory:
            def wrong_path(pid, buffer, size):
                buffer.value = b"/untrusted/claude"
                return len(buffer.value)
            fixture.library.proc_pidpath.side_effect = wrong_path
            self.assert_code("executable_mismatch", self.adapter.send_message, TARGET, "Synthetic test only")
            socket_factory.assert_not_called()
            self.assertEqual({}, self.adapter.outstanding)

    def test_linux_socket_peer_owned_by_other_user_rejected(self):
        sock = mock.Mock()
        sock.getsockopt.return_value = mod.struct.pack("3i", self.data["pid"], os.getuid() + 1, 0)
        with mock.patch.object(mod.sys, "platform", "linux"), \
             mock.patch.object(mod.socket, "SO_PEERCRED", 17, create=True):
            self.assert_code("wrong_peer_uid", mod.peer_pid, sock)

    def test_uuid_validation_rejects_paths_and_non_strings(self):
        for value in ("../settings.json", str(self.transcript), TARGET.upper().replace("1", "A", 1), None, [], {}):
            with self.subTest(value=value):
                self.assert_code("invalid_session_id", self.adapter.authorize, value)

    def test_default_sender_names_only_its_adapter_instance(self):
        self.adapter.close()
        self.adapter = mod.Adapter(self.root, sys.executable, TARGET)
        result, sock = self.send()
        body = json.loads(sock.written)["message"]["content"]
        self.assertIsNone(self.adapter.source)
        self.assertIn("local Codex adapter instance " + self.adapter.instance_id, body)
        self.assertNotIn("source chat", body)
        self.assertNotIn(SOURCE, body)
        self.assertEqual("unknown", result["delivery"])

    def test_explicit_source_attribution_is_preserved_and_validated(self):
        _, sock = self.send()
        self.assertIn("source chat " + SOURCE, json.loads(sock.written)["message"]["content"])
        self.assert_code("invalid_session_id", mod.Adapter, self.root, sys.executable, TARGET, source="not-a-chat")

    def test_client_omits_absent_source_and_forwards_only_explicit_source(self):
        for source in (None, SOURCE):
            with self.subTest(source=source), \
                 mock.patch.object(client_mod.subprocess, "Popen") as popen, \
                 mock.patch.object(client_mod.Client, "request", return_value={}), \
                 mock.patch.object(client_mod.Client, "_configure_transport"), \
                 mock.patch.object(client_mod.Client, "_write"):
                client = client_mod.Client(cli=sys.executable, target=TARGET, source=source)
                command = popen.call_args.args[0]
                self.assertNotIn(None, command)
                if source is None:
                    self.assertNotIn("--source-thread", command)
                else:
                    self.assertEqual(SOURCE, command[command.index("--source-thread") + 1])
                client.close()

    def test_both_command_entry_points_leave_source_unset_by_default(self):
        with mock.patch.object(sys, "argv", ["adapter.py", "--claude-root", str(self.root), "--cli", sys.executable, "--target", TARGET]), \
             mock.patch.object(mod, "Adapter") as constructor, \
             mock.patch.object(mod, "run_server"):
            mod.main()
            self.assertIsNone(constructor.call_args.args[3])
        with mock.patch.object(sys, "argv", ["client.py", "--cli", sys.executable, "--target", TARGET, "tools"]), \
             mock.patch.object(client_mod, "Client") as constructor, \
             mock.patch.object(client_mod, "emit"):
            client_mod.main()
            self.assertIsNone(constructor.call_args.args[2])

    def test_review_entry_points_require_explicit_cli_and_target(self):
        for module, program in ((mod, "adapter.py"), (client_mod, "client.py")):
            for arguments in ([], ["--cli", sys.executable], ["--target", TARGET]):
                with self.subTest(program=program, arguments=arguments), \
                     mock.patch.object(sys, "argv", [program, *arguments]), \
                     mock.patch.object(sys, "stderr", io.StringIO()), \
                     mock.patch.object(module, "Adapter" if module is mod else "Client") as constructor:
                    with self.assertRaises(SystemExit) as caught:
                        module.main()
                    self.assertEqual(2, caught.exception.code)
                    constructor.assert_not_called()

    def test_inventory_survives_target_exit_without_widening_scope(self):
        rows = [{"sessionId": OTHER, "pid": 12345, "name": "Other fixture", "cwd": "/fixture"}]
        completed = subprocess.CompletedProcess([], 0, stdout=json.dumps(rows))
        with mock.patch.object(mod.subprocess, "run", return_value=completed), \
             mock.patch.object(self.adapter, "process_start", return_value="helper-start") as visibility, \
             mock.patch.object(self.adapter, "verify_target", side_effect=AssertionError("Inventory must not bind a target")):
            result = self.adapter.list_sessions()
        visibility.assert_called_once_with(os.getpid())
        self.assertEqual(OTHER, result["sessions"][0]["sessionId"])
        self.assertFalse(result["sessions"][0]["read_send_allowed"])
        self.assertFalse(result["target_present_in_inventory"])
        self.assertFalse(result["protocol_verified"])
        self.assertIsNone(self.adapter.pinned)
        self.assert_code("session_not_allowed", self.adapter.read_session, OTHER)
        self.assert_code("session_not_allowed", self.adapter.send_message, OTHER, "not authorized")
        self.assert_code("session_not_allowed", self.adapter.wait_session, OTHER)

    def test_empty_inventory_is_not_a_claim_no_sessions_exist(self):
        completed = subprocess.CompletedProcess([], 0, stdout="[]")
        with mock.patch.object(mod.subprocess, "run", return_value=completed), \
             mock.patch.object(self.adapter, "process_start", return_value="helper-start"):
            result = self.adapter.list_sessions()
        self.assertEqual([], result["sessions"])
        self.assertEqual("not_independently_verified", result["inventory_completeness"])
        self.assertIn("not proof", result["note"])

    def test_hidden_process_view_is_a_named_failure_even_with_empty_inventory(self):
        completed = subprocess.CompletedProcess([], 0, stdout="[]")
        denied = mod.AdapterError("process_visibility_unavailable", "fixture visibility denied")
        with mock.patch.object(mod.subprocess, "run", return_value=completed), \
             mock.patch.object(self.adapter, "process_start", side_effect=denied):
            self.assert_code("process_visibility_unavailable", self.adapter.list_sessions)

    def test_send_annotations_disclose_side_effects_while_reads_remain_read_only(self):
        for tool in mod.tool_specs():
            with self.subTest(tool=tool["name"]):
                sending = tool["name"] == "send_claude_message"
                self.assertEqual(tool["name"] not in ("send_claude_message", "open_claude_inbox"), tool["annotations"]["readOnlyHint"])
                self.assertEqual(sending, tool["annotations"]["destructiveHint"])
                self.assertEqual(sending, tool["annotations"]["openWorldHint"])

    def test_close_leaves_a_replacement_file_at_its_old_socket_path(self):
        with tempfile.TemporaryDirectory(prefix="csa-", dir="/tmp") as folder:
            self.adapter.start_inbox({"messagingSocketPath": str(Path(folder) / "fixture.sock")})
            path = self.adapter.inbox
            path.unlink()
            path.write_text("replacement must survive")
            self.adapter.close()
            self.assertEqual("replacement must survive", path.read_text())
            self.assertFalse(self.adapter.worker.is_alive())

    def test_owned_helper_signals_cleanup_socket_without_contacting_claude(self):
        # The subprocess owns only a temporary listener. It never queries a
        # registry, opens a transcript, executes Claude, or connects to a peer.
        fixture = r'''
import importlib.util, sys
spec = importlib.util.spec_from_file_location("fixture_adapter", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
adapter = module.Adapter(sys.argv[2], sys.executable, sys.argv[3])
adapter.start_inbox({"messagingSocketPath": sys.argv[2] + "/fixture.sock"})
original_serve = module.serve
def ready_and_serve(instance):
    print(str(instance.inbox), flush=True)
    original_serve(instance)
module.serve = ready_and_serve
module.run_server(adapter)
'''
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=signum), tempfile.TemporaryDirectory(prefix="csa-", dir="/tmp") as folder:
                command = [sys.executable, "-B", "-u", "-c", fixture, str(MODULE_PATH), folder, TARGET]
                with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, text=True) as process:
                    try:
                        with selectors.DefaultSelector() as selector:
                            selector.register(process.stdout, selectors.EVENT_READ)
                            self.assertTrue(selector.select(10), "Fixture helper did not become ready")
                        socket_path = Path(process.stdout.readline().strip())
                        self.assertTrue(socket_path.is_socket())
                        process.send_signal(signum)  # This exact owned fixture PID only.
                        self.assertEqual(128 + signum, process.wait(timeout=8))
                        self.assertFalse(socket_path.exists())
                        self.assertEqual("", process.stderr.read())
                    finally:
                        if process.poll() is None:
                            process.kill()  # Cleanup of this owned test fixture only.
                            process.wait(timeout=3)

    def test_other_session_denied_before_any_lookup(self):
        with mock.patch.object(self.adapter, "verify_target") as verify:
            for fn, args in ((self.adapter.read_session, (OTHER,)),
                             (self.adapter.send_message, (OTHER, "hello")),
                             (self.adapter.wait_session, (OTHER,))):
                self.assert_code("session_not_allowed", fn, *args)
            verify.assert_not_called()

    def test_limits_reject_bool_float_zero_and_oversize(self):
        for value in (True, 1.5, 0, -1, 101, "20"):
            with self.subTest(value=value):
                self.assert_code("invalid_argument", self.adapter.read_session, TARGET, limit=value)
        self.assert_code("invalid_argument", self.adapter.wait_session, TARGET, timeout=56)
        self.assert_code("invalid_argument", self.adapter.wait_session, TARGET, after_sequence=True)

    def test_visible_text_omits_thinking_tool_arguments_results_and_system(self):
        self.transcript.write_bytes(b"".join([
            line(event(message={"content": [
                {"type": "thinking", "thinking": "PRIVATE_THOUGHT"},
                {"type": "text", "text": "VISIBLE"},
                {"type": "tool_use", "id": "tool-1", "name": "Read", "input": {"path": "PRIVATE_INPUT"}},
                {"type": "tool_result", "tool_use_id": "tool-1", "content": "PRIVATE_RESULT"},
            ]})),
            line(event("PRIVATE_SYSTEM", type="system")),
            line(event("PRIVATE_SIDECHAIN", isSidechain=True)),
        ]))
        result = self.read()
        rendered = json.dumps(result)
        for private in ("PRIVATE_THOUGHT", "PRIVATE_INPUT", "PRIVATE_RESULT", "PRIVATE_SYSTEM", "PRIVATE_SIDECHAIN"):
            self.assertNotIn(private, rendered)
        self.assertEqual(["VISIBLE"], [e["text"] for e in result["events"]])
        self.assertEqual(2, len(result["events"][0]["tools"]))
        self.assertNotIn("status", result["events"][0]["tools"][0])

    def test_text_and_tool_counts_are_bounded(self):
        row = event("x" * (mod.MAX_TEXT + 1))
        row["message"]["content"].extend({"type": "tool_use", "id": str(i), "name": "n" * 200} for i in range(30))
        self.transcript.write_bytes(line(row))
        result = self.read()["events"][0]
        self.assertEqual(mod.MAX_TEXT, len(result["text"]))
        self.assertTrue(result["text_truncated"])
        self.assertEqual(20, len(result["tools"]))
        self.assertTrue(all(len(t["name"]) <= 100 for t in result["tools"]))

    def test_partial_record_keeps_cursor_and_reads_once_when_completed(self):
        first, second = line(event("first")), line(event("second", uuid="second"))
        self.transcript.write_bytes(first + second[:-2])
        initial = self.read(cursor=self.cursor())
        self.assertEqual(["first"], [e["text"] for e in initial["events"]])
        self.assertEqual(len(first), initial["cursor"]["offset"])
        self.assertTrue(initial["has_more"])
        with self.transcript.open("ab") as out:
            out.write(second[-2:])
        following = self.read(cursor=initial["cursor"])
        self.assertEqual(["second"], [e["text"] for e in following["events"]])
        self.assertFalse(following["has_more"])

    def test_partial_utf8_record_is_not_decoded_early(self):
        full = line(event("first 🎈"))
        cut = full.index("🎈".encode()) + 1
        self.transcript.write_bytes(full[:cut])
        result = self.read(cursor=self.cursor())
        self.assertEqual([], result["events"])
        self.assertEqual(0, result["cursor"]["offset"])
        self.assertTrue(result["has_more"])

    def test_malformed_completed_line_advances_and_later_valid_record_survives(self):
        self.transcript.write_bytes(b"not json\n\xff\n" + line(event("valid")))
        result = self.read(cursor=self.cursor())
        self.assertEqual(["valid"], [e["text"] for e in result["events"]])
        self.assertIn("malformed_completed_record", result["warnings"])
        self.assertEqual(self.transcript.stat().st_size, result["cursor"]["offset"])

    def test_duplicate_uuid_keeps_update_and_distinct_uuid_keeps_equal_text(self):
        self.transcript.write_bytes(line(event("a")) + line(event("b")) + line(event("b", uuid="different")))
        result = self.read()
        self.assertEqual(["a", "b", "b"], [e["text"] for e in result["events"]])
        self.assertIn("repeated_uuid: update retained", result["warnings"])

    def test_large_history_is_bounded_and_returns_recent_limit(self):
        self.transcript.write_bytes(b"".join(line(event(str(i), uuid=str(i))) for i in range(3000)))
        result = self.read(limit=3)
        self.assertEqual(["2997", "2998", "2999"], [e["text"] for e in result["events"]])
        self.assertLessEqual(result["bytes_scanned"], mod.MAX_SCAN)
        self.assertIn("tail_window: earlier history was not scanned", result["warnings"])

    def test_incremental_event_limit_preserves_unread_records(self):
        self.transcript.write_bytes(b"".join(line(event(str(i), uuid=str(i))) for i in range(5)))
        first = self.read(cursor=self.cursor(), limit=2)
        second = self.read(cursor=first["cursor"], limit=2)
        self.assertEqual(["0", "1"], [e["text"] for e in first["events"]])
        self.assertEqual(["2", "3"], [e["text"] for e in second["events"]])
        self.assertTrue(second["has_more"])

    def test_oversized_record_is_not_parsed_or_skipped_silently(self):
        self.transcript.write_bytes(line(event("x" * (mod.MAX_RECORD + 10))))
        result = self.read(cursor=self.cursor())
        self.assertEqual([], result["events"])
        self.assertEqual(self.transcript.stat().st_size, result["cursor"]["offset"])
        self.assertFalse(result["has_more"])
        self.assertTrue(result["gaps"][0]["record_complete"])
        self.assertIn("transcript_gap: bytes discarded without parsing", result["warnings"])

    def test_invalid_cursor_shape_and_non_boundary_are_rejected(self):
        self.transcript.write_bytes(line(event()))
        for cursor in ({}, {**self.cursor(), "extra": 1}, {**self.cursor(), "offset": 1}):
            with self.subTest(cursor=cursor):
                self.assert_code("invalid_cursor", self.read, cursor=cursor)
        for offset in (-1, True, self.transcript.stat().st_size + 1):
            with self.subTest(offset=offset):
                self.assert_code("invalid_argument", self.read, cursor={**self.cursor(), "offset": offset})

    def test_inode_rotation_rejected(self):
        cursor = self.cursor()
        replacement = self.project / "replacement"
        replacement.write_bytes(line(event()))
        replacement.replace(self.transcript)
        self.assert_code("transcript_rotated", self.read, cursor=cursor)

    def test_truncation_below_cursor_rejected(self):
        self.transcript.write_bytes(line(event()))
        cursor = self.cursor(self.transcript.stat().st_size)
        self.transcript.write_bytes(b"")
        self.assert_code("invalid_argument", self.read, cursor=cursor)

    def test_other_session_record_rejected(self):
        self.transcript.write_bytes(line(event(sessionId=OTHER)))
        self.assert_code("transcript_session_mismatch", self.read)

    def test_transcript_final_symlink_rejected(self):
        secret = self.root / "credentials.json"
        secret.write_text("DO_NOT_READ")
        self.transcript.unlink()
        self.transcript.symlink_to(secret)
        self.assert_code("unsafe_transcript", self.read)

    def test_project_symlink_not_followed(self):
        self.transcript.unlink()
        self.project.rmdir()
        outside = self.root / "outside"
        outside.mkdir(mode=0o700)
        (outside / (TARGET + ".jsonl")).write_bytes(line(event("DO_NOT_READ")))
        self.project.symlink_to(outside, target_is_directory=True)
        self.assert_code("transcript_not_unique", self.read)

    def test_registry_file_symlink_is_not_followed(self):
        external = self.root / "external-registry.json"
        external.write_text(json.dumps(self.data))
        (self.root / "sessions" / f"{self.data['pid']}.json").symlink_to(external)
        self.assert_code("target_not_unique", self.adapter.registry)

    def test_registry_directory_symlink_rejected(self):
        folder = self.root / "sessions"
        folder.rmdir()
        outside = self.root / "outside-sessions"
        outside.mkdir(mode=0o700)
        folder.symlink_to(outside, target_is_directory=True)
        self.assert_code("unsafe_directory", self.adapter.registry)

    def test_registry_reads_no_key_or_configuration_files(self):
        registry = self.root / "sessions" / f"{self.data['pid']}.json"
        registry.write_text(json.dumps(self.data))
        for name in ("secret.key", "settings.json", "credentials.json", "123.key"):
            (self.root / "sessions" / name).write_text("DO_NOT_READ")
        paths = []
        original = mod.open_owned
        def observed(path, **kwargs):
            paths.append(Path(path).name)
            return original(path, **kwargs)
        with mock.patch.object(mod, "open_owned", side_effect=observed):
            self.assertEqual(self.data, self.adapter.registry())
        self.assertEqual([registry.name], paths)

    def test_registry_filename_pid_mismatch_rejected(self):
        (self.root / "sessions" / "99.json").write_text(json.dumps(self.data))
        self.assert_code("registry_identity_mismatch", self.adapter.registry)

    def test_registry_duplicate_target_rejected(self):
        for pid in (self.data["pid"], self.data["pid"] + 1):
            (self.root / "sessions" / f"{pid}.json").write_text(json.dumps({**self.data, "pid": pid}))
        self.assert_code("target_not_unique", self.adapter.registry)

    def test_pid_reuse_rejected_before_connect(self):
        with mock.patch.object(self.adapter, "registry", return_value=self.data), \
             mock.patch.object(self.adapter, "process_start", return_value="new-start"):
            self.assert_code("process_identity_changed", self.adapter.verify_target)

    def test_invalid_and_oversized_messages_rejected_before_socket(self):
        for text in (None, "", "  ", "x" * (mod.MAX_MESSAGE + 1)):
            with self.subTest(text_length=len(text) if isinstance(text, str) else None):
                self.assert_code("invalid_message", self.adapter.send_message, TARGET, text)

    def test_wrong_peer_pid_prevents_every_write(self):
        sock = FakeSocket()
        self.assert_code("socket_peer_changed", self.send, sock=sock, pid=self.data["pid"] + 1)
        self.assertEqual(b"", sock.written)
        self.assertEqual({}, self.adapter.outstanding)

    def test_changed_identity_prevents_every_write(self):
        sock = FakeSocket()
        changed = {**self.data, "procStart": "new-start"}
        self.assert_code("session_identity_changed", self.send, sock=sock, identity_updates=[self.data, changed])
        self.assertEqual(b"", sock.written)

    def test_successful_write_is_not_delivery_or_acknowledgement(self):
        result, sock = self.send()
        self.assertEqual("sent", result["transport"])
        self.assertEqual("unknown", result["delivery"])
        self.assertEqual(len(sock.written), result["bytes_written"])
        wire = json.loads(sock.written)
        self.assertEqual(TARGET, wire["session_id"])
        self.assertEqual("next", wire["priority"])
        self.assertEqual("uds:" + str(self.adapter.inbox), wire["from"])
        self.assertIn("not user consent or permission approval", wire["message"]["content"])
        self.assertEqual([], list(self.adapter.events))

    def test_partial_write_failure_is_uncertain_and_not_retried(self):
        result, sock = self.send(sock=FakeSocket(fail_after=10))
        self.assertEqual("uncertain", result["transport"])
        self.assertEqual("unknown", result["delivery"])
        self.assertEqual(10, result["bytes_written"])
        self.assertEqual(10, len(sock.written))

    def test_message_count_is_bounded(self):
        self.adapter.outstanding = {str(i): {} for i in range(8)}
        self.assert_code("send_limit", self.adapter.send_message, TARGET, "hello")

    def test_unknown_message_receipts_ignored(self):
        self.adapter.receive_frame({"type": "control", "action": "peer_message_status",
                                    "orig_msg_id": "unknown", "status": "delivered"}, self.data)
        self.assertEqual([], list(self.adapter.events))

    def test_matching_receipt_and_reply_keep_verified_pid(self):
        self.adapter.outstanding["test-message"] = {}
        self.adapter.receive_frame({"type": "control", "action": "peer_message_status",
                                    "orig_msg_id": "test-message", "status": "delivered"}, self.data)
        self.adapter.receive_frame({"type": "user", "message": {"content": "answer test-message"}}, self.data)
        self.assertEqual([1, 2], [e["sequence"] for e in self.adapter.events])
        self.assertTrue(all(e["verified_pid"] == self.data["pid"] for e in self.adapter.events))
        self.assertEqual(["test-message"], self.adapter.events[-1]["message_ids"])

    def test_refused_receipt_normalizes_native_expired_status(self):
        self.adapter.outstanding["test-message"] = {}
        self.adapter.receive_frame({"type": "control", "action": "peer_message_status",
                                    "orig_msg_id": "test-message", "status": "expired",
                                    "status_detail": "refused"}, self.data)
        self.assertEqual("refused", self.adapter.events[-1]["status"])

    def test_inbox_rejects_reply_from_wrong_process_before_reading(self):
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        listener = mock.MagicMock()
        listener.accept.side_effect = [(connection, None), OSError("fixture finished")]
        self.adapter.listener = listener
        with mock.patch.object(self.adapter, "verify_target", return_value=self.data), \
             mock.patch.object(mod, "peer_pid", return_value=self.data["pid"] + 1):
            self.adapter.receive_loop()
        connection.recv.assert_not_called()
        self.assertEqual([], list(self.adapter.events))

    def test_inbox_reassembles_fragmented_frames_from_verified_process(self):
        self.adapter.outstanding["test-message"] = {}
        frame = line({"type": "control", "action": "peer_message_status",
                      "orig_msg_id": "test-message", "status": "delivered"})
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.recv.side_effect = [frame[:15], frame[15:], b""]
        listener = mock.MagicMock()
        listener.accept.side_effect = [(connection, None), OSError("fixture finished")]
        self.adapter.listener = listener
        with mock.patch.object(self.adapter, "verify_target", return_value=self.data), \
             mock.patch.object(mod, "peer_pid", return_value=self.data["pid"]):
            self.adapter.receive_loop()
        self.assertEqual(1, len(self.adapter.events))
        self.assertEqual("delivered", self.adapter.events[0]["status"])
        self.assertEqual(self.data["pid"], self.adapter.events[0]["verified_pid"])

    def test_wait_cursor_does_not_skip_reply_arriving_during_transcript_read(self):
        def racing_read(*_):
            self.adapter.record({"kind": "peer_reply", "text": "late acknowledgement"})
            return {"events": [], "cursor": self.cursor()}
        with mock.patch.object(self.adapter, "verify_target", return_value=self.data), \
             mock.patch.object(self.adapter, "read_session", side_effect=racing_read):
            first = self.adapter.wait_session(TARGET, timeout=0, cursor=self.cursor())
        self.assertEqual([], first["events"])
        self.assertEqual(0, first["sequence"], "Return the event snapshot cursor, not a newer unseen sequence")
        with mock.patch.object(self.adapter, "verify_target", return_value=self.data):
            following = self.adapter.wait_session(TARGET, after_sequence=first["sequence"], timeout=0)
        self.assertEqual("late acknowledgement", following["events"][0]["text"])

    def test_malformed_receipt_identifier_is_ignored_without_thread_crash(self):
        for mid in ([], {}, ["bad"]):
            with self.subTest(mid=mid):
                self.adapter.receive_frame({"type": "control", "action": "peer_message_status",
                                            "orig_msg_id": mid, "status": "delivered"}, self.data)
        self.assertEqual([], list(self.adapter.events))

    def test_malformed_transcript_uuid_does_not_crash_reader(self):
        self.transcript.write_bytes(line(event("first", uuid={"bad": 1})) + line(event("second", uuid=["bad"])))
        result = self.read()
        self.assertEqual(["first", "second"], [e["text"] for e in result["events"]])
        self.assertTrue(all(e.get("uuid") is None or isinstance(e["uuid"], str) for e in result["events"]))

    def test_tool_schema_rejects_unknown_missing_and_approval_arguments(self):
        for name, args in (("approve_claude_permission", {}),
                           ("read_claude_session", {}),
                           ("send_claude_message", {"session_id": TARGET, "text": "x", "approve": True}),
                           ("list_claude_sessions", {"session_id": TARGET})):
            with self.subTest(name=name, args=args):
                self.assert_code("unknown_tool" if name.startswith("approve") else "invalid_arguments", self.adapter.call, name, args)
        self.assert_code("invalid_arguments", self.adapter.call, "list_claude_sessions", [])

    def test_stdio_schema_errors_do_not_terminate_following_request(self):
        requests = [
            {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "read_claude_session", "arguments": {"session_id": OTHER}}},
            {"jsonrpc": "2.0", "id": 2, "method": "ping"},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
        ]
        output = io.StringIO()
        mod.serve(self.adapter, io.BytesIO(b"".join(line(r) for r in requests)), output)
        replies = [json.loads(s) for s in output.getvalue().splitlines()]
        self.assertEqual([1, 2], [r["id"] for r in replies])
        self.assertTrue(replies[0]["result"]["isError"])
        self.assertEqual({}, replies[1]["result"])

    def test_stdio_oversized_request_is_not_executed(self):
        output = io.StringIO()
        with mock.patch.object(self.adapter, "call") as call:
            mod.serve(self.adapter, io.BytesIO(b"x" * (mod.MAX_RPC + 1) + b"\n"), output)
            call.assert_not_called()
        self.assertEqual("", output.getvalue())


if __name__ == "__main__":
    unittest.main()
