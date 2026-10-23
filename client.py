#!/usr/bin/env python3
"""Explicit local MCP client. No automatic sends, retries or permission changes."""
import argparse
import json
import math
import os
from pathlib import Path
import selectors
import signal
import stat
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
        self.request_lock = threading.Lock()
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
                         "clientInfo": {"name": "claude-adapter-client", "version": "0.4.0-dev"}})
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
        if not self.request_lock.acquire(blocking=False):
            raise RuntimeError("Another request owns this client; concurrent calls are not supported")
        try:
            return self._request(method, params, deadline=deadline, cancel=cancel)
        finally:
            self.request_lock.release()

    def _request(self, method, params, *, deadline=None, cancel=None):
        if self.broken or self.closed:
            raise RuntimeError("Client is unusable; never retry an uncertain send")
        deadline = min(deadline if deadline is not None else float("inf"), time.monotonic() + self.timeout)
        cancel = cancel or self.cancel
        self.next_id += 1
        request_id = self.next_id
        payload = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}).encode() + b"\n"
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
            if not isinstance(value, dict) or value.get("id") != request_id:
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


class BoundedOutput:
    """Best-effort JSONL output; journal evidence survives a blocked consumer.

    Once output is partial or unavailable, never append another frame to it.
    This object has no thread, queue, retries, or workflow ownership of its own.
    """
    def __init__(self, fd, *, timeout=0.1, deadline=None, cancel=None):
        self.fd, self.timeout, self.deadline = fd, timeout, deadline
        self.cancel = cancel or threading.Event()
        self.error = None

    def __call__(self, value):
        if self.error:
            return False
        offset, blocking, success = 0, None, False
        deadline = min(self.deadline if self.deadline is not None else float("inf"), time.monotonic() + self.timeout)
        try:
            payload = (json.dumps(value, ensure_ascii=False) + "\n").encode()
            if len(payload) > 1048576:
                raise ValueError("Output frame exceeds its bound")
            blocking = os.get_blocking(self.fd)
            os.set_blocking(self.fd, False)
            regular = stat.S_ISREG(os.fstat(self.fd).st_mode)
            while offset < len(payload):
                if self.cancel.is_set():
                    raise Cancelled("Output cancelled")
                if time.monotonic() >= deadline:
                    raise TimeoutError("Output deadline")
                if not regular:
                    Client._ready(self.fd, selectors.EVENT_WRITE, deadline, self.cancel)
                try:
                    count = os.write(self.fd, payload[offset:])
                except BlockingIOError:
                    continue
                if not count:
                    raise BrokenPipeError("Output closed")
                offset += count
            success = True
        except (OSError, TimeoutError, Cancelled, ValueError, UnicodeError) as error:
            self.error = {"kind": type(error).__name__, "partial_line": offset > 0,
                          "detail": "Output unavailable; inspect committed state with read-only status/replay"}
        finally:
            if blocking is not None:
                try:
                    os.set_blocking(self.fd, blocking)
                except OSError as error:
                    self.error = {"kind": type(error).__name__, "partial_line": offset > 0,
                                  "detail": "Output descriptor unavailable; inspect committed state"}
                    success = False
        return success


_emitter = None


def emit(value):
    global _emitter
    if _emitter is None:
        _emitter = BoundedOutput(sys.stdout.fileno())
    return _emitter(value)


def emit_required(value):
    if not emit(value):
        raise SystemExit(3)


def state_report(state, request_id):
    """Safe metadata projection of committed state, never a fresh liveness claim."""
    preflight = state.get("preflight_coverage", {})
    result = {"request_id": request_id, "outcome": state.get("outcome"),
              "transport": state.get("transport", "unknown"),
              "delivery_status": state.get("receipts", [{}])[-1].get("status", "unknown") if state.get("receipts") else "unknown",
              "arrival_observed": bool(state.get("arrival")), "acknowledged": bool(state.get("ack")),
              "work_completion": "unverified", "candidate_assistant_evidence": bool(state.get("latest_assistant_evidence")),
              "observation_status": state.get("observation_status", "unknown"),
              "cursor": state.get("cursor"), "sequence": state.get("sequence"),
              "last_observed_at": state.get("last_observed_at"),
              "transcript_gap_count": state.get("transcript_gap_count", 0),
              "event_gap_count": state.get("event_gap_count", 0), "listener_gaps": state.get("listener_gaps", 0),
              "text_truncated": state.get("text_truncated"),
              "transcript_has_more": state.get("has_more"),
              "preflight_coverage": {"warning_count": len(preflight["warnings"]) if "warnings" in preflight else None,
                                     "gap_count": len(preflight["gaps"]) if "gaps" in preflight else None,
                                     "has_more": preflight.get("has_more")},
              "peer_inbox_continuity": state.get("peer_inbox_continuity", "unknown"),
              "observation_route": state.get("observation_route", "unknown"),
              "owner_liveness": "unknown", "error_present": bool(state.get("error")),
              "error_stage": state.get("error_stage"),
              "output_unavailable": bool(state.get("output_error")),
              "replayed": bool(state.get("replayed")), "live_check": False, "resends": 0}
    return result


def saved_report(snapshot, request_id, binding, *, replay=False, include_text=False):
    """Metadata by default; visible evidence text requires an explicit opt-in."""
    old = snapshot["operation"]
    if not old or any(old["binding"].get(k) != v for k, v in binding.items()):
        raise ValueError("No matching operation for the selected target/runtime")
    result = state_report(old["state"], request_id)
    result.update(last_ordinal=snapshot["last_ordinal"], replay_has_more=snapshot["has_more"])
    if replay:
        from events import redact
        evidence = []
        for row in snapshot["evidence"]:
            saved = row["event"]
            event = saved.get("event", {})
            item = {"ordinal": row["ordinal"], "kind": saved.get("kind"),
                    "event_type": event.get("type", event.get("kind")),
                    "has_text": bool(event.get("text")), "text_truncated": bool(event.get("text_truncated"))}
            if include_text and isinstance(event.get("text"), str):
                item["text"] = redact(event["text"])
                item["trust"] = "untrusted_peer_data"
            evidence.append(item)
        result["saved_evidence"] = evidence
    return result


def has_ack_line(text, nonce):
    return any(line.strip().strip("`") == nonce + " ACK" for line in text.splitlines())


def bound_operation(journal, request_id, binding):
    old = journal.get(request_id)
    if not old or any(old["binding"].get(k) != v for k, v in binding.items()):
        raise ValueError("No matching operation for the selected target/runtime")
    return old


def main():
    global _emitter
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", required=True, help="Absolute verified Claude executable")
    parser.add_argument("--target", required=True, help="Exact authorized session UUID")
    parser.add_argument("--claude-root", default=str(Path.home() / ".claude"))
    parser.add_argument("--source-thread")
    parser.add_argument("--state-dir", help="Existing private 0700 directory; required for durable operations")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list")
    sessions = sub.add_parser("sessions", help="Metadata catalog with explicit saved-history limits")
    sessions.add_argument("--limit", type=int, default=50)
    sessions.add_argument("--cursor")
    sessions.add_argument("--saved-offset", type=int, default=0)
    sub.add_parser("tools")
    read = sub.add_parser("read")
    read.add_argument("--limit", type=int, default=5)
    read.add_argument("--cursor")
    for name in ("send", "probe", "observe", "replay", "status"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--request-id", required=True, help="Stable UUID; reuse only to observe the same operation")
        if name in ("send", "probe"):
            cmd.add_argument("--authorized-one-message", action="store_true", required=True)
        if name == "send":
            cmd.add_argument("--message-file", required=True, help="UTF-8 message data, not executable Python or shell")
        if name in ("send", "probe"):
            cmd.add_argument("--dispatch-only", action="store_true", help="Send once and release ownership; original peer reply inbox closes")
        if name == "observe":
            cmd.add_argument("--follow", action="store_true", help="Continue bounded observation after ACK/held; never infer task completion")
        if name not in ("replay", "status"):
            cmd.add_argument("--wait", type=float, default=45, help="Total operation deadline, 1..3600 seconds")
        else:
            cmd.add_argument("--after", type=int, default=0)
            cmd.add_argument("--limit", type=int, default=100)
            if name == "replay":
                cmd.add_argument("--include-text", action="store_true", help="Include bounded best-effort-redacted visible text")
    args = parser.parse_args()
    if not Path(args.cli).is_absolute():
        parser.error("--cli must be absolute")
    from adapter import session_id
    session_id(args.target)
    if args.command in ("send", "probe", "observe", "replay", "status"):
        if not args.state_dir:
            parser.error("--state-dir is required for durable operations")
        session_id(args.request_id)
        if args.command not in ("replay", "status") and (not math.isfinite(args.wait) or not 1 <= args.wait <= 3600):
            parser.error("--wait must be finite and within 1..3600 seconds")
    cancel = threading.Event()
    previous = {sig: signal.signal(sig, lambda *_: cancel.set()) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        if args.command in ("send", "probe", "observe", "replay", "status"):
            from journal import Journal, JournalReader, digest
            from workflow import run_operation
            journal_type = JournalReader if args.command in ("replay", "status") else Journal
            with journal_type(args.state_dir) as journal:
                binding = {"target": args.target, "cli": str(Path(args.cli).resolve()),
                           "root": str(Path(args.claude_root).expanduser().resolve())}
                if args.command in ("replay", "status"):
                    snapshot = journal.snapshot(args.request_id, after=args.after, limit=args.limit)
                    emit_required(saved_report(snapshot, args.request_id, binding, replay=args.command == "replay",
                                               include_text=getattr(args, "include_text", False)))
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
                _emitter = BoundedOutput(sys.stdout.fileno(), deadline=time.monotonic() + args.wait, cancel=cancel)
                mode = "observe" if args.command == "observe" else "dispatch" if args.dispatch_only else "send_and_observe"
                result = run_operation(factory, journal, args.request_id, binding, message,
                                       budget=args.wait, cancel=cancel, progress=_emitter,
                                       mode=mode, follow=getattr(args, "follow", False))
                if (_emitter.error and _emitter.error["kind"] == "Cancelled"
                        and not _emitter.error["partial_line"]):
                    # A cancellation between report()'s check and its first
                    # byte leaves a clean stream for a distinct final frame.
                    _emitter = BoundedOutput(sys.stdout.fileno())
                _emitter.deadline = time.monotonic() + 0.1
                # Cancellation stops observation, but a healthy output stream
                # can still receive the final committed cancellation state.
                _emitter.cancel = threading.Event()
                if not emit(state_report(result, args.request_id)):
                    saved = journal.get(args.request_id)
                    if saved:
                        saved["state"]["output_error"] = _emitter.error
                        journal.checkpoint(args.request_id, saved["state"])
                    raise SystemExit(130 if result["outcome"] == "cancelled" else 3)
                if result["outcome"] not in ("acknowledged", "dispatched"):
                    raise SystemExit(130 if result["outcome"] == "cancelled" else 2)
        else:
            client = Client(args.cli, args.target, args.source_thread, root=args.claude_root, cancel=cancel)
            try:
                if args.command == "tools":
                    emit_required(client.request("tools/list", {}))
                elif args.command == "list":
                    emit_required(client.call("list_claude_sessions", {}))
                elif args.command == "sessions":
                    call = {"limit": args.limit, "saved_offset": args.saved_offset}
                    if args.cursor is not None:
                        call["cursor"] = args.cursor
                    emit_required(client.call("enumerate_claude_sessions", call))
                else:
                    call = {"session_id": args.target, "limit": args.limit}
                    if args.cursor:
                        call["cursor"] = json.loads(args.cursor)
                    emit_required(client.call("read_claude_session", call))
            finally:
                client.close()
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        sys.exit(1 if emit({"error": str(error), "resends": 0}) else 3)
