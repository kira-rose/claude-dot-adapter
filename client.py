#!/usr/bin/env python3
"""Explicit local MCP client. No automatic sends, retries or permission changes."""
import argparse
import json
import math
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import threading
import time


class Cancelled(RuntimeError):
    pass


class Client:
    def __init__(self, cli, target, source=None, *, root=None, timeout=75, cancel=None):
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be finite and positive")
        self.timeout = timeout
        self.cancel = cancel or threading.Event()
        self.next_id = 0
        self.buffer = bytearray()
        self.broken = False
        self.closed = False
        command = [sys.executable, "-B", "-u", str(Path(__file__).with_name("adapter.py")),
                   "--cli", str(cli), "--target", target]
        if source is not None:
            command.extend(["--source-thread", source])
        if root is not None:
            command.extend(["--claude-root", str(root)])
        self.proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.DEVNULL, bufsize=0)
        try:
            self._configure_transport()
            self.request("initialize", {"protocolVersion": "2024-11-05", "capabilities": {},
                         "clientInfo": {"name": "claude-adapter-client", "version": "0.2.0"}})
            self._write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}).encode() + b"\n",
                        time.monotonic() + self.timeout, self.cancel)
        except BaseException:
            self.close()
            raise

    def _configure_transport(self):
        os.set_blocking(self.proc.stdin.fileno(), False)
        os.set_blocking(self.proc.stdout.fileno(), False)

    @staticmethod
    def _ready(fd, kind, deadline, cancel):
        with selectors.DefaultSelector() as selector:
            selector.register(fd, kind)
            while True:
                if cancel.is_set():
                    raise Cancelled("Observation cancelled; no automatic resend")
                left = deadline - time.monotonic()
                if left <= 0:
                    raise TimeoutError("MCP response deadline exceeded; delivery may be unknown")
                if selector.select(min(left, 0.05)):
                    return

    def _write(self, payload, deadline, cancel):
        offset = 0
        while offset < len(payload):
            self._ready(self.proc.stdin, selectors.EVENT_WRITE, deadline, cancel)
            try:
                count = os.write(self.proc.stdin.fileno(), payload[offset:])
            except BlockingIOError:
                continue
            if not count:
                raise RuntimeError("MCP input closed")
            offset += count

    def request(self, method, params, *, deadline=None, cancel=None):
        if self.broken or self.closed:
            raise RuntimeError("Client is unusable; never retry an uncertain send")
        deadline = min(deadline if deadline is not None else float("inf"), time.monotonic() + self.timeout)
        cancel = cancel or self.cancel
        self.next_id += 1
        payload = json.dumps({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params}).encode() + b"\n"
        if len(payload) > 65536:
            raise ValueError("MCP request exceeds the adapter limit")
        try:
            self._write(payload, deadline, cancel)
            while b"\n" not in self.buffer:
                if len(self.buffer) > 1048576:
                    raise RuntimeError("MCP response oversized")
                self._ready(self.proc.stdout, selectors.EVENT_READ, deadline, cancel)
                try:
                    chunk = os.read(self.proc.stdout.fileno(), 65536)
                except BlockingIOError:
                    continue
                if not chunk:
                    raise RuntimeError("MCP helper exited before a complete response; delivery may be unknown")
                self.buffer.extend(chunk)
            line, _, rest = self.buffer.partition(b"\n")
            self.buffer = bytearray(rest)
            if len(line) > 1048576:
                raise RuntimeError("MCP response oversized")
            value = json.loads(line)
            if not isinstance(value, dict) or value.get("id") != self.next_id:
                raise RuntimeError("MCP response ID mismatch")
            if "error" in value:
                raise RuntimeError(json.dumps(value["error"]))
            return value["result"]
        except BaseException:
            self.broken = True
            raise

    def call(self, name, args, **options):
        value = self.request("tools/call", {"name": name, "arguments": args}, **options)
        content = json.loads(value["content"][0]["text"])
        if value.get("isError"):
            raise RuntimeError(json.dumps(content))
        return content

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.proc.stdin.close()
            try:
                self.proc.wait(timeout=0.3)
            except subprocess.TimeoutExpired:
                self.proc.terminate()  # Owned helper only, never Claude.
                try:
                    self.proc.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait(timeout=1)
        finally:
            self.proc.stdout.close()


def emit(value):
    print(json.dumps(value, ensure_ascii=False), flush=True)


def has_ack_line(text, nonce):
    return any(line.strip().strip("`") == nonce + " ACK" for line in text.splitlines())


def bound_operation(journal, request_id, binding):
    old = journal.get(request_id)
    if not old or any(old["binding"].get(k) != v for k, v in binding.items()):
        raise ValueError("No matching operation for the selected target/runtime")
    return old


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", required=True, help="Absolute verified Claude executable")
    parser.add_argument("--target", required=True, help="Exact authorized session UUID")
    parser.add_argument("--claude-root", default=str(Path.home() / ".claude"))
    parser.add_argument("--source-thread")
    parser.add_argument("--state-dir", help="Existing private 0700 directory; required for durable operations")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list")
    sub.add_parser("tools")
    read = sub.add_parser("read")
    read.add_argument("--limit", type=int, default=5)
    read.add_argument("--cursor")
    for name in ("send", "probe", "observe", "replay"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--request-id", required=True, help="Stable UUID; reuse only to observe the same operation")
        if name in ("send", "probe"):
            cmd.add_argument("--authorized-one-message", action="store_true", required=True)
        if name == "send":
            cmd.add_argument("--message-file", required=True, help="UTF-8 message data, not executable Python or shell")
        if name != "replay":
            cmd.add_argument("--wait", type=float, default=45, help="Total operation deadline, 1..3600 seconds")
        else:
            cmd.add_argument("--after", type=int, default=0)
    args = parser.parse_args()
    if not Path(args.cli).is_absolute():
        parser.error("--cli must be absolute")
    from adapter import session_id
    session_id(args.target)
    if args.command in ("send", "probe", "observe", "replay"):
        if not args.state_dir:
            parser.error("--state-dir is required for durable operations")
        session_id(args.request_id)
        if args.command != "replay" and (not math.isfinite(args.wait) or not 1 <= args.wait <= 3600):
            parser.error("--wait must be finite and within 1..3600 seconds")
    cancel = threading.Event()
    previous = {sig: signal.signal(sig, lambda *_: cancel.set()) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        if args.command in ("send", "probe", "observe", "replay"):
            from journal import Journal, digest
            from workflow import run_operation
            with Journal(args.state_dir) as journal:
                binding = {"target": args.target, "cli": str(Path(args.cli).resolve()),
                           "root": str(Path(args.claude_root).expanduser().resolve())}
                if args.command == "replay":
                    old = bound_operation(journal, args.request_id, binding)
                    emit({"request_id": args.request_id, "saved_state": old["state"],
                          "saved_evidence": journal.replay(args.request_id, args.after), "live_check": False})
                    return
                message = None
                if args.command == "send":
                    with open(args.message_file, "rb") as source:
                        raw = source.read(16001)
                    if len(raw) > 16000:
                        parser.error("Message file exceeds the bounded input limit")
                    message = raw.decode("utf-8")
                elif args.command == "probe":
                    message = "Authorized no-work connectivity check. Make no file changes, run no commands, start no task, and change no settings or permissions."
                if args.command == "observe":
                    binding = bound_operation(journal, args.request_id, binding)["binding"]
                else:
                    binding["message_hash"] = digest(message)
                factory = lambda: Client(args.cli, args.target, args.source_thread, root=args.claude_root,
                                         timeout=min(75, args.wait), cancel=cancel)
                result = run_operation(factory, journal, args.request_id, binding, message,
                                       budget=args.wait, cancel=cancel, progress=emit)
                emit(result)
                if result["outcome"] not in ("acknowledged",):
                    raise SystemExit(130 if result["outcome"] == "cancelled" else 2)
        else:
            client = Client(args.cli, args.target, args.source_thread, root=args.claude_root, cancel=cancel)
            try:
                if args.command == "tools":
                    emit(client.request("tools/list", {}))
                elif args.command == "list":
                    emit(client.call("list_claude_sessions", {}))
                else:
                    call = {"session_id": args.target, "limit": args.limit}
                    if args.cursor:
                        call["cursor"] = json.loads(args.cursor)
                    emit(client.call("read_claude_session", call))
            finally:
                client.close()
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        emit({"error": str(error), "resends": 0})
        sys.exit(1)
