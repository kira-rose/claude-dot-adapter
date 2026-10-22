#!/usr/bin/env python3
"""Bounded receive-only collection. No daemon, dispatch, or parent wakeup."""
import argparse
import json
import math
import os
from pathlib import Path
import selectors
import signal
import stat
import sys
import threading
import time
import uuid

from client import Client, Cancelled
from events import diagnostic, normalize, packed
from stream_store import CapacityError, Store


class BudgetEnded(Exception):
    pass


class PipePublisher:
    """Explicit NDJSON pipe output, with a bounded write deadline.

    Persistence precedes publication. A partial write is uncertain, never retried.
    No destination URL, process launch, or parent connection is accepted here.
    """
    def __init__(self, fd, timeout=1):
        if not stat.S_ISFIFO(os.fstat(fd).st_mode) and not stat.S_ISSOCK(os.fstat(fd).st_mode):
            raise ValueError("Publication requires an explicitly connected pipe or socket")
        self.fd, self.timeout = fd, timeout

    def __call__(self, event, *, deadline=None, cancel=None):
        payload = (packed({"type": "claude_session_event", "parent_delivery": "unavailable_unqualified", **event}) + "\n").encode()
        deadline, offset = min(deadline if deadline is not None else float("inf"), time.monotonic() + self.timeout), 0
        blocking = os.get_blocking(self.fd)
        os.set_blocking(self.fd, False)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(self.fd, selectors.EVENT_WRITE)
                while offset < len(payload):
                    if cancel is not None and cancel.is_set():
                        raise Cancelled("Publication cancelled; output may be partial")
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("Publication deadline; output may be partial")
                    if not selector.select(min(remaining, 0.05)):
                        continue
                    try:
                        count = os.write(self.fd, payload[offset:])
                    except BlockingIOError:
                        continue
                    if not count:
                        raise BrokenPipeError("Publication pipe closed")
                    offset += count
        finally:
            os.set_blocking(self.fd, blocking)


def batch_events(target, transcript, peer=(), *, stream=None, include_text=False):
    result = []
    for route, rows in (("transcript", transcript.get("events", [])), ("peer", peer)):
        for row in rows:
            event = normalize(target, row, route, stream=stream, include_text=include_text)
            if event:
                result.append(event)
    gaps = transcript.get("gaps", [])
    if gaps:
        safe = [{k: v for k, v in gap.items() if
                 (k in ("start_offset", "end_offset") and type(v) is int and v >= 0) or
                 (k == "record_complete" and type(v) is bool) or
                 (k == "reason" and v in ("oversized_or_tail_record", "malformed_record", "additional_gaps"))}
                for gap in gaps[:20]]
        result.append(diagnostic(target, "transcript_gap", [transcript.get("cursor"), gaps],
                                 {"ranges": safe, "range_count": len(gaps), "details_truncated": len(gaps) > 20}))
    warnings = transcript.get("warnings", [])
    if warnings:
        allowed = {"tail_window", "transcript_gap", "incomplete_record", "malformed_completed_record", "gap_details_coalesced", "repeated_uuid", "event_limit"}
        codes = sorted({s.split(":")[0] for s in warnings if isinstance(s, str) and s.split(":")[0] in allowed})
        result.append(diagnostic(target, "transcript_warning", [transcript.get("cursor"), codes], {"codes": codes}))
    return result


def collect(factory, store, binding, *, duration=60, lease=30, cancel=None, publisher=None):
    if not math.isfinite(duration) or not 0 < duration <= 3600 or not 5 <= lease <= 300:
        raise ValueError("Invalid bounded duration or liveness lease")
    cancel = cancel or threading.Event()
    old = store.state()
    if old.get("binding") and old["binding"] != binding:
        raise ValueError("Journal scope changed; no target was contacted")
    deadline = time.monotonic() + duration
    target = binding["target"]
    client = None
    epoch = str(uuid.uuid4())
    result = "stopped"
    publication_attempt = False

    def save(state, events):
        nonlocal publication_attempt
        inserted = store.commit(state, events)
        if publisher:
            for entry in inserted:
                if cancel.is_set():
                    raise Cancelled("Publication cancelled before next event")
                if time.monotonic() >= deadline:
                    raise BudgetEnded()
                store.publication(entry["seq"], "uncertain")
                publication_attempt = True
                publisher(entry, deadline=deadline, cancel=cancel)
                store.publication(entry["seq"], "pipe_written_no_parent_ack")
                publication_attempt = False
            if inserted:
                published = store.state()
                published["publication"] = "pipe_written_no_parent_ack"
                store.commit(published)

    try:
        if cancel.is_set():
            raise Cancelled("Collector cancelled")
        client = factory()
        args = {"session_id": target, "limit": 100}
        if old.get("cursor") is not None:
            args["cursor"] = old["cursor"]
        first = client.call("read_claude_session", args, deadline=deadline, cancel=cancel)
        state = store.bind(binding, first["identity"])
        state.update(collection="collecting", error=None, epoch=epoch, lease_seconds=lease,
                     last_verified=time.time(), event_stream=first["event_stream"], sequence=0,
                     endpoint=None, publication="pipe_enabled_no_parent_ack" if publisher else "disabled")
        extra = [diagnostic(target, "collector_started", epoch)]
        if not old.get("cursor"):
            extra.append(diagnostic(target, "initial_bounded_snapshot", epoch))
        if old.get("epoch") and binding["peer_inbox"]:
            extra.append(diagnostic(target, "peer_listener_restart_gap", [old["epoch"], epoch]))
        if binding["peer_inbox"]:
            opened = client.call("open_claude_inbox", {"session_id": target}, deadline=deadline, cancel=cancel)
            if opened["identity"] != first["identity"] or opened["event_stream"] != first["event_stream"]:
                raise ValueError("Receive-only inbox identity changed")
            state["endpoint"] = opened["address"]
        state["cursor"] = first["cursor"]
        save(state, extra + batch_events(target, first, include_text=binding["include_text"]))
        while not cancel.is_set() and deadline - time.monotonic() > 0.15:
            state = store.state()
            remaining = deadline - time.monotonic()
            wait_seconds = max(0, min(5, int(remaining - 0.1)))
            reply = client.call("wait_claude_session", {
                "session_id": target, "cursor": state["cursor"],
                "event_stream": state["event_stream"], "after_sequence": state["sequence"],
                "timeout": wait_seconds}, deadline=deadline, cancel=cancel)
            transcript = reply.get("transcript")
            if not transcript or transcript["identity"] != state["identity"] or reply["event_stream"] != state["event_stream"]:
                raise ValueError("Producer or helper identity changed")
            if type(reply["sequence"]) is not int or reply["sequence"] < state["sequence"]:
                raise ValueError("Peer cursor moved backwards")
            batch = batch_events(target, transcript, reply["events"], stream=state["event_stream"], include_text=binding["include_text"])
            if reply.get("event_gap"):
                gap = {k: v for k, v in reply["event_gap"].items() if k in ("after_sequence", "oldest_available") and type(v) is int and v >= 0}
                batch.append(diagnostic(target, "peer_retention_gap", [state["event_stream"], gap], {"stream": state["event_stream"], **gap}))
            state.update(cursor=transcript["cursor"], sequence=reply["sequence"], last_verified=time.time())
            save(state, batch)
            if reply.get("cancelled"):
                result = "cancelled"
                break
            if wait_seconds == 0:
                cancel.wait(min(0.05, max(0, deadline - time.monotonic())))
        if cancel.is_set():
            result = "cancelled"
    except BudgetEnded:
        result = "stopped"
    except Cancelled:
        result = "cancelled"
    except Exception as error:
        # Preserve committed cursors, not the failed candidate batch. Never store
        # arbitrary exception text, which can contain private producer content.
        result = "publication_uncertain" if publication_attempt else "backpressure" if isinstance(error, CapacityError) else "producer_or_transport_error"
        state = store.state()
        state.update(collection=result, error=type(error).__name__, endpoint=None)
        if publication_attempt:
            state["publication"] = "uncertain_no_automatic_republication"
        store.commit(state)
    finally:
        if client is not None:
            client.close()  # Only this helper; never signal the producer.
        state = store.state()
        if result in ("stopped", "cancelled"):
            state.update(collection=result, endpoint=None)
            if publication_attempt:
                state["publication"] = "uncertain_no_automatic_republication"
            store.commit(state)
    return {"outcome": result, **store.status()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True, help="Existing private 0700 directory")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("collect")
    run.add_argument("--cli", required=True)
    run.add_argument("--target", required=True)
    run.add_argument("--claude-root", default=str(Path.home() / ".claude"))
    run.add_argument("--duration", type=float, default=60)
    run.add_argument("--lease", type=int, default=30)
    run.add_argument("--peer-inbox", action="store_true", help="Open an ephemeral receive-only inbox; does not announce it")
    run.add_argument("--include-text", action="store_true", help="Opt into best-effort redacted visible text")
    run.add_argument("--publish", action="store_true", help="Write newly committed events to stdout pipe, never a parent turn")
    sub.add_parser("status")
    sub.add_parser("recover", help="Offline locked SQLite recovery; no producer contact")
    replay = sub.add_parser("replay")
    replay.add_argument("--after", type=int, default=0)
    replay.add_argument("--limit", type=int, default=100)
    ack = sub.add_parser("ack")
    ack.add_argument("--consumer", required=True)
    ack.add_argument("--cursor", type=int, required=True)
    args = parser.parse_args()
    if args.command == "recover":
        print(packed(Store.recover(args.state_dir)))
        return 0
    if args.command != "collect":
        with Store(args.state_dir, "consumer" if args.command == "ack" else "reader") as store:
            value = store.status() if args.command == "status" else store.replay(args.after, args.limit) if args.command == "replay" else store.acknowledge(args.consumer, args.cursor)
        print(packed(value))
        return 0
    from adapter import session_id
    session_id(args.target)
    if not Path(args.cli).is_absolute() or not math.isfinite(args.duration) or not 1 <= args.duration <= 3600:
        parser.error("Use an absolute CLI path and a duration of 1..3600 seconds")
    if not 5 <= args.lease <= 300:
        parser.error("Lease must be 5..300 seconds")
    binding = {"target": args.target, "cli": str(Path(args.cli).resolve()),
               "root": str(Path(args.claude_root).expanduser().resolve()),
               "peer_inbox": args.peer_inbox, "include_text": args.include_text}
    cancel = threading.Event()
    previous = {sig: signal.signal(sig, lambda *_: cancel.set()) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        publisher = PipePublisher(sys.stdout.fileno()) if args.publish else None
        with Store(args.state_dir) as store:
            factory = lambda: Client(args.cli, args.target, root=args.claude_root, timeout=min(75, args.duration), cancel=cancel)
            result = collect(factory, store, binding, duration=args.duration, lease=args.lease, cancel=cancel, publisher=publisher)
        # The independent status command remains available even if this pipe blocks.
        if not args.publish:
            print(packed(result))
        return 0 if result["outcome"] == "stopped" else 130 if result["outcome"] == "cancelled" else 2
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(packed({"error": type(error).__name__, "send_calls": 0}), file=sys.stderr)
        sys.exit(1)
