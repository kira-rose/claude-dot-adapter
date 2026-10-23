# Saved and live session metadata catalog

The metadata catalog is part of the 0.4.0-dev development source. An earlier catalog build was qualified in an isolated runtime with the pinned SDK and a fresh native MCP host on macOS. That established bounded saved metadata enumeration, not universal saved-history coverage or access from every caller. A subsequent installed release containing the executable-path fix has separate read/send/ACK and fresh app-server read evidence, detailed in [PROVENANCE.md](PROVENANCE.md). This does not establish existing desktop helper reload or activation. Installation and tool exposure must be checked in the actual calling context.

## Sources and data boundary

`enumerate_claude_sessions` uses two independent sources, scoped to the configured local Claude root:

- [`claude agents --json --all`](https://code.claude.com/docs/en/agent-view#list-sessions-as-json) reports live interactive and retained background session metadata. It does not enumerate all saved interactive history.
- The optional [`claude_agent_sdk.list_sessions`](https://code.claude.com/docs/en/agent-sdk/python#list_sessions) API supplies saved metadata. The worker accepts exactly `claude-agent-sdk==0.2.172`; missing or different versions produce an explicit unavailable source. It calls with `directory=None`, `include_worktrees=False`, a limit and an offset. `CLAUDE_CONFIG_DIR` binds the configured root.

The [pinned SDK implementation](https://github.com/anthropics/claude-agent-sdk-python/blob/v0.2.172/src/claude_agent_sdk/_internal/sessions.py) reads up to 65,536 bytes at each file end and scans candidates before applying pagination. The adapter has no ad hoc transcript scan. Only UUID, custom title, workspace, creation timestamp and file modification timestamp leave the isolated worker. SDK `summary`, `first_prompt`, message bodies and exception text are excluded; Python and file-descriptor logs are suppressed. This is metadata-only output, but the SDK internally reads bounded transcript payloads. Coverage reports that distinction.

The SDK worker has a 20-second deadline. Both source commands have a 2 MiB captured-output cap; the worker's metadata budget is 1 MiB and 4,096 consumed SDK rows per window. The SDK may read many files before returning; these are elapsed-time and output bounds, not a fixed aggregate disk-read budget. A timeout reports unavailable coverage instead of an empty successful inventory.

Nothing in this path starts or resumes Claude, sends a message, reads session messages through the SDK, or changes permissions. It does not create a peer registry entry. Existing transcript reads and sends retain their exact configured target and all process/socket identity checks.

## Meaning of each row

| Field | Meaning |
| --- | --- |
| `session_id` | Stable full UUID; merge key across both sources |
| `title` | SDK custom title when available, otherwise CLI name; never summary or prompt fallback |
| `project_workspace` | Reported working directory, or null |
| `live_status` | `running`, `busy`, `idle`, `stopped` or `unknown`, with separate evidence |
| `created_at` | SDK's first timestamp from its bounded head, or null; UTC and provenance included |
| `last_activity_at` | Null: neither selected source establishes semantic last activity |
| `process_started_at` | CLI-reported start time only; not creation or activity |
| `storage_modified_at` | SDK file mtime only; not semantic activity |
| `read_send_allowed` | Whether UUID matches the configured allowlist; actual access still requires normal identity verification |

A unique PID with the configured executable corroborates live metadata only. It does not verify peer identity. Missing PIDs, inaccessible process views, multiple matching PIDs, or one PID claimed by different UUIDs do not establish liveness. A saved-only row stays `unknown`. A completed or failed background task does not prove the process stopped; only an explicit CLI stopped state without a reported PID is labeled stopped.

Known limitation: this catalog's process corroboration compares `ps comm` to the configured executable path. A legitimate background process renamed to `claude bg-spare` can therefore remain `unknown`. The separate read/send target verifier now uses the OS-resolved executable path and retains all UUID, process and socket identity gates. Catalog status neither overrides nor replaces that verifier.

Titles can be generated or user-set. A rename outside the SDK's bounded windows can be missed, so `title` is the latest title observed through these sources, not guaranteed current truth. Conflicting metadata and duplicate UUID observations carry warnings. File mtime may select between competing saved observations without being relabeled activity.

## Pagination and completeness

There are two distinct continuation mechanisms:

1. Call with `saved_offset=0` and `limit` from 1 through 50. Follow `pagination.next_cursor` with the same saved offset until it is null. The opaque cursor binds the configured scope and the full bounded merged snapshot. A changed snapshot fails with `catalog_changed_restart_pagination` instead of silently skipping rows.
2. If `coverage.saved_source.next_offset` is present, begin the next SDK window with that `saved_offset` and no catalog cursor. Drain its catalog pages in the same way. Live rows are repeated in each window; deduplicate accumulated rows by UUID.

The worker advances by consumed SDK rows, including rejected UUID rows, and never advances past an unreturned valid row at its output-byte cap. Saved offsets are bounded at 1,000,000. If a further continuation would exceed that ceiling, `continuation_limit_reached=true`, `truncated=true`, and `next_offset=null` explicitly preserve the gap.

The SDK supplies no stable cross-window snapshot or total. Concurrent writes can move entries between offset windows, and unreadable, empty, sidechain, unparseable or summary-less files may be omitted. Exhausting all continuations therefore does not prove complete saved history. `all_saved_sessions_complete` stays false. A source failure is reported separately while usable rows from the other source remain available. `target_present` concerns the current bounded merged window, not all possible history.

## Source invocation

After a separately reviewed dependency and release installation, a catalog request has this form:

```sh
python3 -B client.py --cli /absolute/path/to/qualified/claude \
  --target exact-configured-session-uuid \
  sessions --limit 50 --saved-offset 0
```

The host's tool allowlist must include `enumerate_claude_sessions` for native access. Copying this source or loading its skill does not install the optional SDK or update registration. No broadened read/send allowlist is needed for metadata enumeration.

## Validation

Offline tests cover saved-only history after reboot, unavailable sources, missing timestamps, title changes, duplicate/stale records, shared-PID ambiguity, bounded worker output, suppressed SDK logs, both pagination mechanisms and their limit boundaries. All use synthetic metadata and temporary fixtures. The pre-existing read/send/collector tests remain part of the full regression suite.

The recorded native catalog qualification returned saved metadata after reviewed SDK installation. It did not establish a live peer for every saved row. A live transcript read still requires normal registry and endpoint identity checks; successful discovery alone does not authorize or prove a successful read or message.
