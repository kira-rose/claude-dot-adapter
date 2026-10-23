"""Single-send, caller-driven observation. Saved evidence is not a live callback."""
import copy
import math
import threading
import time

from journal import digest

BLOCKED = {"held", "denied", "refused", "expired", "dropped"}
TERMINAL = {"acknowledged", "held_or_refused", "send_failed"}


def exact_line(text, expected):
    return isinstance(text, str) and any(line.strip().strip("`") == expected for line in text.splitlines())


def run_operation(factory, journal, request_id, binding, message=None, *, budget=45,
                  cancel=None, progress=lambda _value: None, mode="send_and_observe", follow=False):
    """Create one durable intent or resume observation of that same intent.

    A crash anywhere after begin() consumes the operation's send permission.
    Resuming never calls send, even if no message ID/result was returned.
    Intent uncertainty is deliberately preferred to a duplicate side effect.
    """
    if not math.isfinite(budget) or not 0 < budget <= 3600:
        raise ValueError("Observation budget must be finite and within (0,3600]")
    if mode not in ("send_and_observe", "dispatch", "observe") or (follow and mode != "observe"):
        raise ValueError("Follow requires explicit observation; dispatch and observation have one owner")
    from adapter import session_id
    session_id(request_id)
    session_id(binding["target"])
    cancel = cancel or threading.Event()
    deadline = time.monotonic() + budget
    old = journal.get(request_id)
    if mode == "observe" and not old:
        raise ValueError("Observation requires an existing durable intent; no send")
    if old and old["binding"] != binding:
        raise ValueError("Operation ID belongs to different intent/target/runtime; no send")
    ack_line = f"CLAUDE-ADAPTER-{request_id} ACK"
    if not old:
        if not isinstance(message, str) or not message.strip() or digest(message) != binding.get("message_hash"):
            raise ValueError("New operation requires its exact authorized message")
        outgoing = (message + "\n\nRequest marker: " + request_id +
                    "\nPlease acknowledge receipt with exactly this line:\n" + ack_line +
                    "\nThis acknowledgement confirms receipt only, not acceptance or completion of work.")
        if len(outgoing) > 4000:
            raise ValueError("Message plus correlation instructions exceeds 4000 characters")
    state = copy.deepcopy(old["state"]) if old else None
    if state and (mode == "dispatch" or (state["outcome"] in TERMINAL and
                                         not (follow and state["outcome"] != "send_failed"))):
        return {**state, "replayed": True, "live_check": False, "resends": 0}
    client = None
    stage = "preflight"
    last_progress = time.monotonic()
    progress_unavailable = False
    # Two process identity queries can each take five seconds. Keep their
    # response budget separate from long-poll waiting, including on short runs.
    reserve = min(11, budget / 4)

    def report(value):
        nonlocal last_progress, progress_unavailable
        if cancel.is_set() or progress_unavailable:
            return  # Reserve a healthy output stream for the final cancelled state.
        try:
            delivered = progress(value)
            output_error = getattr(progress, "error", None)
            if delivered is False and not output_error:
                output_error = {"kind": "OutputUnavailable", "detail": "Progress callback rejected output"}
        except Exception as error:
            output_error = {"kind": type(error).__name__, "detail": "Progress callback unavailable; observation continues"}
        last_progress = time.monotonic()
        progress_unavailable = bool(output_error)
        if state is not None and output_error and state.get("output_error") != output_error:
            state["output_error"] = output_error
            journal.checkpoint(request_id, state)

    def assistant_evidence(event, route):
        text = event.get("text", "")
        if exact_line(text, ack_line):
            state["ack"] = {"route": route, "event": event}
        if isinstance(text, str):
            remaining = "\n".join(line for line in text.splitlines() if line.strip().strip("`") != ack_line).strip()
            if remaining:
                state["latest_assistant_evidence"] = {"route": route, "event": {**event, "text": remaining},
                                                      "correlation": "unverified", "work_completion": "unverified"}

    try:
        if cancel.is_set():
            raise InterruptedError("Cancelled before preflight")
        client = factory()
        def call(name, args):
            nonlocal stage
            stage = name
            return client.call(name, args, deadline=deadline, cancel=cancel)
        target = binding["target"]
        before = call("read_claude_session", {"session_id": target, "limit": 1})
        if state:
            if state["identity"] != before["identity"]:
                raise ValueError("Target identity changed since the recorded operation; no retarget or send")
            # There is no listener resurrection. Retain already observed peer
            # evidence, declare the interval, and reconcile via the old transcript cursor.
            gap = {"kind": "listener_gap", "old_stream": state["event_stream"],
                   "new_stream": before["event_stream"],
                   "detail": "Unobserved peer replies during helper exit cannot be recovered"}
            state["event_stream"] = before["event_stream"]
            state["sequence"] = 0
            state["listener_gaps"] += 1
            state["outcome"] = "observing"
            state["observation_status"] = "running"
            state["observation_route"] = "transcript_only"
            state["peer_inbox_continuity"] = "original_inbox_unavailable"
            state.pop("error", None)
            journal.checkpoint(request_id, state, [gap])
        else:
            state = {"request_id": request_id, "ack_line": ack_line, "outcome": "prepared_unknown",
                     "message_id": None, "transport": "unknown", "send_result": None,
                     "identity": before["identity"], "cursor": before["cursor"],
                     "event_stream": before["event_stream"], "sequence": 0,
                     "arrival": None, "ack": None, "receipts": [], "listener_gaps": 0,
                     "transcript_gap_count": 0, "event_gap_count": 0, "text_truncated": False,
                     "resends": 0, "created_at": time.time(), "observation_status": "running",
                     "observation_route": "transcript_and_peer", "peer_inbox_continuity": "closes_on_exit",
                     "work_completion": "unverified", "latest_assistant_evidence": None,
                     "preflight_coverage": {"warnings": before.get("warnings", []),
                                            "gaps": before.get("gaps", []), "has_more": before.get("has_more", False)}}
            journal.begin(request_id, binding, state)  # Durable BEFORE every possible write.
            report({"stage": "intent_committed", "request_id": request_id})
            sent = call("send_claude_message", {"session_id": target, "text": outgoing})
            state.update(message_id=sent["message_id"], transport=sent["transport"], send_result=sent,
                         sequence=sent["after_sequence"], event_stream=sent["event_stream"], outcome="observing")
            journal.checkpoint(request_id, state, [{"kind": "send_result", "result": sent}])
            report({"stage": "send_result", "request_id": request_id,
                    "transport": state["transport"], "delivery_status": "unknown"})
            if sent["transport"] == "failed":
                state["outcome"] = "send_failed"
                journal.checkpoint(request_id, state)
                return state
            if mode == "dispatch":
                state.update(outcome="dispatched" if sent["transport"] == "sent" else "dispatch_uncertain",
                             observation_status="handed_off", peer_inbox_continuity="closed_on_exit")
                journal.checkpoint(request_id, state, [{"kind": "observation_handoff",
                    "detail": "The original reply inbox closes on exit; later observation recovers transcript evidence only"}])
                return state

        report({"stage": "observing", "request_id": request_id, "cursor": state["cursor"],
                "sequence": state["sequence"], "acknowledged": bool(state["ack"]), "follow": follow})
        while deadline - time.monotonic() > reserve and not cancel.is_set():
            remaining = deadline - time.monotonic()
            previous_facts = (bool(state["ack"]), bool(state["arrival"]),
                              state["receipts"][-1]["status"] if state["receipts"] else None,
                              bool(state["transcript_gap_count"]), bool(state["event_gap_count"]),
                              digest(state.get("latest_assistant_evidence")), state.get("target_status"))
            out = call("wait_claude_session", {"session_id": target,
                       "after_sequence": state["sequence"], "event_stream": state["event_stream"],
                       "cursor": state["cursor"], "timeout": max(0, min(5, int(remaining - reserve)))})
            if out["event_stream"] != state["event_stream"]:
                raise ValueError("Event stream changed unexpectedly; explicit reconciliation required")
            if out.get("cancelled"):
                cancel.set()
            evidence = []
            for event in out["events"]:
                evidence.append({"kind": "peer", "stream": out["event_stream"], "event": event})
                state["text_truncated"] |= bool(event.get("text_truncated"))
                if event.get("kind") == "delivery_receipt" and event.get("message_id") == state["message_id"]:
                    if event not in state["receipts"]:
                        if len(state["receipts"]) >= 128:
                            raise ValueError("Receipt bound reached; reconcile saved evidence")
                        state["receipts"].append(event)
                if event.get("kind") == "peer_reply":
                    assistant_evidence(event, "verified_peer_reply")
            if out.get("event_gap"):
                evidence.append({"kind": "event_gap", "gap": out["event_gap"]})
                state["event_gap_count"] += 1
            transcript = out.get("transcript")
            if transcript:
                for event in transcript["events"]:
                    evidence.append({"kind": "transcript", "event": event})
                    state["text_truncated"] |= bool(event.get("text_truncated"))
                    if event.get("type") == "user" and (request_id in event.get("text", "") or
                            (state["message_id"] and state["message_id"] in event.get("text", ""))):
                        state["arrival"] = {"uuid": event.get("uuid"), "timestamp": event.get("timestamp")}
                    if event.get("type") == "assistant":
                        assistant_evidence(event, "target_assistant_transcript")
                for gap in transcript.get("gaps", []):
                    evidence.append({"kind": "transcript_gap", "gap": gap})
                    state["transcript_gap_count"] += 1
                state["cursor"] = transcript["cursor"]
                state["has_more"] = transcript.get("has_more", False)
            state["sequence"] = out["sequence"]
            state["last_observed_at"] = time.time()
            state["target_status"] = out.get("status") if out.get("status") in ("idle", "busy", "waiting") else "unknown"
            state["waiting_for"] = out.get("waiting_for")
            state["work_completion"] = "unverified"
            if state["ack"]:
                state["outcome"] = "acknowledged"
            elif state["receipts"] and state["receipts"][-1]["status"] in BLOCKED:
                state["outcome"] = "held_or_refused"
            else:
                state["outcome"] = "observing"
            journal.checkpoint(request_id, state, evidence)
            facts = (bool(state["ack"]), bool(state["arrival"]),
                     state["receipts"][-1]["status"] if state["receipts"] else None,
                     bool(state["transcript_gap_count"]), bool(state["event_gap_count"]),
                     digest(state.get("latest_assistant_evidence")), state.get("target_status"))
            if facts != previous_facts or time.monotonic() - last_progress >= 5:
                report({"stage": "observation", "request_id": request_id, "arrival_observed": bool(state["arrival"]),
                          "acknowledged": state["ack"] is not None, "outcome": state["outcome"],
                          "transcript_gap_count": state["transcript_gap_count"], "event_gap_count": state["event_gap_count"],
                          "cursor": state["cursor"], "sequence": state["sequence"], "transcript_has_more": state.get("has_more"),
                          "target_status": state.get("target_status"), "candidate_assistant_evidence": bool(state.get("latest_assistant_evidence")),
                          "work_completion": "unverified", "remaining_seconds": max(0, deadline - time.monotonic())})
            if cancel.is_set():
                break
            if state["outcome"] in TERMINAL and not follow:
                state["observation_status"] = "stopped_at_receipt"
                journal.checkpoint(request_id, state)
                return state
            # Prevent zero-time waits and unrelated-event bursts from spinning.
            cancel.wait(min(0.05, max(0, deadline - time.monotonic())))
        state["outcome"] = "cancelled" if cancel.is_set() else "observation_timeout"
        state["observation_status"] = "cancelled" if cancel.is_set() else "budget_exhausted"
        journal.checkpoint(request_id, state)
        return state
    except (Exception, KeyboardInterrupt) as error:
        # Reload the LAST committed state: a failed journal transaction must
        # never expose an advanced cursor as durable progress.
        saved = journal.get(request_id)
        if saved:
            state = saved["state"]
            cancelled = cancel.is_set() or isinstance(error, (KeyboardInterrupt, InterruptedError))
            timed_out = isinstance(error, TimeoutError) and stage in ("wait_claude_session", "read_claude_session")
            state["outcome"] = "cancelled" if cancelled else "observation_timeout" if timed_out else "observation_error"
            state["observation_status"] = "cancelled" if cancelled else "budget_exhausted" if timed_out else "error"
            state["error_stage"] = stage
            state["error"] = str(error)[:2000]
            journal.checkpoint(request_id, state)
            return state
        raise
    finally:
        if client is not None:
            client.close()
