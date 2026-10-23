# Receive-only event collection

The portable local collector introduced in 0.3.0 remains part of 0.4.0-dev. It can observe a selected running Claude session's assistant transcript and optionally receive unsolicited peer messages. It journals those observations without first sending a message. It does not wake Codex, announce itself to Claude, launch a daemon or install a scheduled service.

## Run and inspect

Use one existing user-owned directory with mode `0700`, separate from source control. Keep the same directory and exact target/runtime for restarts. The collector freezes its text and inbox options as part of that journal's binding.

```sh
mkdir -m 700 /absolute/private/collector-state
python3 -B collector.py --state-dir /absolute/private/collector-state collect \
  --cli /absolute/path/to/claude --target exact-authorized-session-uuid \
  --duration 300
```

`--duration` is 1–3,600 seconds. Initialization, reads, waits and optional publication share this deadline; owned-helper cleanup adds a short bounded period. The foreground process exits afterwards. SIGINT/SIGTERM cancel collection and close only its helper. Claude continues independently.

A separate process can inspect committed status without waiting on the MCP helper or publication pipe:

```sh
python3 -B collector.py --state-dir /absolute/private/collector-state status
python3 -B collector.py --state-dir /absolute/private/collector-state replay --after 0 --limit 100
```

Replay returns stable `seq` values, immutable event IDs, normalized envelopes and per-event publication state. Continue after the last returned sequence. It never creates a helper or checks a live producer. `status.presentation_id` stays stable while the material status and freshness class remain unchanged, permitting an idempotent display. `verification_age_seconds` can increase without creating a new presentation ID.

`reported_state` is a producer claim taken only from a valid structured status event. It is separate from collector health and never implies independent verification. `producer_observation: fresh` means that this collector verified the bound process within its lease; `stale`, `observer_stopped` and `unverified` explicitly limit that evidence. The default lease is 30 seconds; `--lease` accepts 5–300 seconds. None of these fields proves the producer accepted or completed an assigned task.

Consumer progress is local bookkeeping:

```sh
python3 -B collector.py --state-dir /absolute/private/collector-state ack \
  --consumer example-viewer --cursor 12
```

Acknowledgements are monotonic and idempotent, limited to committed sequence numbers. They do not prove a parent chat received or presented anything. Consumer cursors appear in status.

## Unsolicited peer inbox

Add `--peer-inbox` to open the helper's private ephemeral Unix socket. While collecting, its address is available as `status.state.endpoint`. The new MCP tool `open_claude_inbox` exposes the same receive-only capability. Both paths enforce the existing exact session allowlist, runtime guard and process identity checks; accepted connections must come from the verified target PID.

The address is valid only for that helper's lifetime. The collector does not publish a fake Claude registry entry or send the address to Claude. Making the target aware of it is a separate, explicitly authorized interaction. A caller must keep the connection alive through peer verification; this is a local compatibility protocol, not a durable message broker. The new inbox has synthetic macOS socket coverage, but no fresh live Claude acceptance result.

A helper restart records `peer_listener_restart_gap`: replies sent while its listener was absent cannot be recovered. Stable producer event IDs permit deduplication across routes and restarts. Legacy peer frames with no event ID can only be deduplicated within their helper stream, which their envelope declares. Transcript events use UUID/content-version identity, with a record-position fallback when a UUID is absent. The initial read is a bounded tail snapshot and explicitly declares that it does not recover all earlier history.

## Optional structured producer events

The entire visible assistant text or verified peer message may consist of the prefix `CLAUDE-ADAPTER-EVENT ` followed by this JSON shape:

```json
{"version":1,"event_id":"11111111-1111-4111-8111-111111111111","kind":"status","origin":"claude","hops":0,"state":"running","text":"Synthetic progress example"}
```

Use a new lowercase UUID for each logical event and reuse it only for the identical event. `kind` is `status` or `message`; status permits `accepted`, `running`, `blocked`, `done` or `failed`, while message requires `state: null`. All seven fields are required and extra fields are rejected. Text is at most 1,200 characters; the full frame is at most 1,900 bytes. Invalid/truncated frames and loop markers produce bounded rejection metadata, never routing or execution. A conflicting payload under an existing ID stops collection without advancing its cursor.

No structured fields can choose a destination or perform work. A host must treat these envelopes as untrusted peer data, even when the producer PID is verified. A `done` event remains an unverified producer report.

## Privacy and bounded storage

Default envelopes contain content length/hash, source identity and event kind. `--include-text` opts into best-effort redacted visible text. The filter handles some token assignments, bearer tokens, common key prefixes, email addresses and home-directory prefixes; it does not guarantee secrecy. Display text is capped at 2,000 characters and 4,000 serialized UTF-8 bytes including JSON escape overhead, with shortening marked. Thinking, tool input/result bodies and user transcript echoes are not collected.

The event journal supports at most 5,000 events, 8 MiB of envelope payload, 8 KiB per envelope, 32 KiB of state and 16 consumers. SQLite has an additional page-count cap. Retention does not silently discard events or deduplication history. Capacity failures leave the last committed cursor intact and expose `backpressure`; retain the journal and explicitly reconcile any discontinuity before changing scope or storage.

Transcript byte gaps retain bounded reason/range/completeness metadata; up to 20 detailed ranges are kept with an explicit total and truncation flag. Warning codes are separate from gaps. Peer retention gaps preserve their stream and sequence boundary. Evidence and observation cursors commit atomically.

SQLite uses synchronous transactions and an exclusive collector lock. Readers and consumer acknowledgements use separate connections. If a process crash leaves a hot rollback journal, read-only status reports `recovery_required` without claiming freshness or inventing a cursor. Resume the collector normally, or recover saved state without producer contact:

```sh
python3 -B collector.py --state-dir /absolute/private/collector-state recover
```

Recovery requires an existing journal, takes the collector lock and marks the observer stopped. It preserves committed events/cursors and clears the expired inbox address. Reopen any existing reader connection after recovery; replay is then available. Do not delete the journal to clear an uncertain send or publication.

## Optional pipe publication

`--publish` writes newly committed NDJSON envelopes to an explicitly connected stdout pipe or socket. It accepts no callback URL or command. Pipe output uses a one-second per-event limit bounded by the remaining collection deadline and checks cancellation. A slow or disconnected receiver stops publication with an explicit uncertain outcome; independent status/replay remain available.

The journal records publication intent before pipe I/O. Each event is `not_attempted`, `uncertain` or `pipe_written_no_parent_ack`. A crash after event persistence but before publication leaves a replayable event; a crash during publication remains uncertain. Restarting does not automatically republish old events. A downstream receiver must explicitly replay and deduplicate by immutable event ID, then record its own durable acceptance before claiming success. An uncertain attempt cannot be converted into an automatic repeat.

Every live output and status identifies parent delivery as `unavailable_unqualified`. Writing a pipe is not parent presentation. The required host-side connection is described in [host-integration.md](host-integration.md).
