"""Small, inert event envelopes. Peer content is data, never executable routing."""
import hashlib
import json
import re
import uuid

PREFIX = "CLAUDE-ADAPTER-EVENT "
STATES = {"accepted", "running", "blocked", "done", "failed"}


def packed(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def fingerprint(value):
    return hashlib.sha256(packed(value).encode("utf-8", "backslashreplace")).hexdigest()


def redact(text):
    """Best-effort convenience filter; metadata-only remains the safe default."""
    text = re.sub(r"(?i)\bBearer\s+[^\s]+", "Bearer [REDACTED]", text)
    text = re.sub(r"\b(?:sk-|ghp_|github_pat_)[A-Za-z0-9_-]{8,}", "[REDACTED]", text)
    text = re.sub(r"(?i)\b(password|secret|token|api[_-]?key)\s*[:=]\s*[^\s,;]+", r"\1=[REDACTED]", text)
    text = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "[EMAIL]", text)
    text = re.sub(r"/(?:Users|home)/[^\s/]+", "/[HOME]", text)
    result, remaining = [], 4000
    for char in text[:2000]:
        # Budget JSON escapes as well as UTF-8; a control character costs six
        # bytes in an envelope even though it occupies one byte in raw text.
        cost = len(json.dumps(char, ensure_ascii=False).encode("utf-8", "replace")) - 2
        if cost > remaining:
            break
        result.append(char)
        remaining -= cost
    return "".join(result)


def structured(text):
    if len(text.encode()) > 1900:
        raise ValueError("structured_size")
    value = json.loads(text[len(PREFIX):])
    if not isinstance(value, dict) or set(value) != {"version", "event_id", "kind", "origin", "hops", "state", "text"}:
        raise ValueError("structured_fields")
    if type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("structured_version")
    if value["origin"] != "claude" or type(value["hops"]) is not int or value["hops"] != 0:
        raise ValueError("loop_marker")
    if value["kind"] not in ("message", "status"):
        raise ValueError("structured_kind")
    if not isinstance(value["event_id"], str) or str(uuid.UUID(value["event_id"])) != value["event_id"]:
        raise ValueError("structured_id")
    if not isinstance(value["text"], str) or len(value["text"]) > 1200:
        raise ValueError("structured_text")
    state = value["state"]
    if (value["kind"] == "status" and (not isinstance(state, str) or state not in STATES)) or (value["kind"] == "message" and state is not None):
        raise ValueError("structured_state")
    return value


def normalize(target, event, route, *, stream=None, include_text=False):
    if route == "transcript":
        if event.get("type") != "assistant":
            return None  # Never reflect incoming prompts/user echoes back to their sender.
        source_id = event.get("uuid") or event.get("record_cursor")
    elif route == "peer":
        if event.get("kind") != "peer_reply":
            return None
        source_id = event.get("peer_event_id")
    else:
        raise ValueError("Unknown event route")
    text = event.get("text", "")
    if not isinstance(text, str):
        raise ValueError("Invalid event text")
    text = text[:2000].encode("utf-8", "replace").decode("utf-8")
    # Transcript UUIDs are producer data. Do not leak arbitrary strings through
    # metadata fields, even when no visible text was requested.
    if source_id is not None:
        try:
            if not isinstance(source_id, str) or str(uuid.UUID(source_id)) != source_id:
                raise ValueError("Not a UUID")
        except (ValueError, AttributeError):
            source_id = fingerprint(source_id)
    content = {"text_hash": fingerprint(text), "characters": len(text),
               "truncated": bool(event.get("text_truncated")), "trust": "untrusted_peer_data"}
    key = [target, route, source_id, fingerprint([text, event.get("tools", []), content["truncated"]])]
    if route == "peer":
        key = [target, "peer", source_id] if source_id else [target, "peer_ephemeral", stream, event.get("sequence")]
    kind = "assistant_message" if route == "transcript" else "peer_message"
    if text.startswith(PREFIX):
        try:
            if content["truncated"]:
                raise ValueError("structured_truncated")
            value = structured(text)
        except (ValueError, TypeError, AttributeError, RecursionError):
            # Do not propagate exception strings or invalid peer-controlled fields.
            kind = "rejected_structured_event"
            content["reason"] = "invalid_schema_or_loop_marker"
        else:
            key = [target, "structured", value["event_id"]]
            kind = "producer_" + value["kind"]
            text = value["text"].encode("utf-8", "replace").decode("utf-8")
            content = {"text_hash": fingerprint(text), "characters": len(text), "truncated": False,
                       "trust": "untrusted_peer_data", "producer_event_id": value["event_id"],
                       "reported_state": value["state"]}
    if include_text and kind != "rejected_structured_event":
        content["text"] = redact(text)
        content["display_text_may_be_shortened"] = content["text"] != text
    return {"version": 1, "event_id": fingerprint(key), "target": target, "kind": kind,
            "content": content, "source": {"route": route, "id": source_id,
            "dedupe": "structured_id" if kind in ("producer_message", "producer_status") else "producer_id" if route == "peer" and source_id else
                      "helper_lifetime" if route == "peer" else "transcript_content_version"}}


def diagnostic(target, kind, key, details=None):
    return {"version": 1, "event_id": fingerprint([target, kind, key]), "target": target,
            "kind": kind, "content": {"trust": "collector_metadata", "details": details}, "source": {"route": "collector"}}


def canonical(event):
    # The same structured publication can arrive through either route.
    return fingerprint({k: v for k, v in event.items() if k != "source"})
