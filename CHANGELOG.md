# Changelog

## Unreleased observation reliability — source candidate, 2026-10-23

- Distinguish cursor/gap progress from actual wait timeout and stop additional transcript scans after the deadline while retaining final identity verification.
- Add concurrent read-only operation `status`/`replay`, strict snapshot privacy and recovery refusal, with distinct transcript backlog, replay pagination and saved coverage metadata.
- Add explicit dispatch-only handoff and post-ACK/held observation follow, retaining at-most-once intent and documenting the lost original peer inbox.
- Preserve ACK and candidate answer text independently; correlation and work completion remain unverified.
- Bound progress pipe writes, preserve cancellation and committed evidence on output failure, and reject overlapping RPC calls before writing.
- This work is qualified only with isolated fixtures; it has not changed the installed adapter or contacted a live Claude session.

## 0.4.0-dev — development candidate, 2026-10-23

- Add metadata-only `enumerate_claude_sessions` and client `sessions`, merging stable UUIDs from documented live/background CLI and optional saved-session SDK sources.
- Isolate the exactly pinned SDK metadata call, suppress library logs, and return only UUID, title, workspace and timestamp metadata. Never use summary or prompt text as a title fallback.
- Preserve unknown status and semantic activity, distinguish file modification from activity and process start, and report stale or ambiguous process mappings.
- Add snapshot-checked merged pagination plus explicit saved-source continuation and coverage ceilings.
- Replace target executable display-name comparison with macOS `proc_pidpath` and Linux `/proc/<pid>/exe`. Fail closed on unavailable or mismatched OS paths while preserving UUID/PID, start-time, owner, socket and peer checks.
- Add 16 executable-identity regressions; the local suite passes 208 tests. The fix's native API check uses the owned Python test process; Linux and Claude background cases are synthetic.
- Add a reusable Dot skill and reconcile current MCP/client operation, metadata limits, dependency, provenance and public repository documentation. No runtime configuration or access scope is changed by these docs.
- Record separate qualification scopes: a predecessor catalog build was verified for native metadata enumeration; a subsequent installed release containing the executable-path fix passed bounded target reads and one authorized no-work send with transcript arrival and matching assistant ACK, without resend. A fresh isolated native app-server read also succeeded. Existing desktop helper reload/activation is not established. This remains development source, not a tagged stable release. The catalog's separate `ps comm` limitation remains.

## 0.3.0 — collector source candidate, 2026-10-22

- Add bounded receive-only collection, a separate event journal, status/replay and optional pipe output without dispatching a message.
- Preserve producer/session identity, explicit listener/retention gaps and no automatic parent wake-up claim.
- Qualify hidden oversized-record recovery across collector restart with synthetic regression fixtures.

## 0.2.0 — 2026-10-22

- Keep one client/helper alive through one send and repeated bounded observation.
- Commit operation intent before sending; replay saved evidence and resume observation without resending after restart or uncertain transport.
- Keep transcript offsets separate from helper-specific peer stream/sequence cursors; report retention and listener gaps.
- Advance across oversized transcript records only with explicit byte-range gaps; retain normal incomplete records until complete.
- Enforce deadlines across partial stdio reads/writes, handle cancellation and reap owned helpers after initialization failure.
- Preserve target, process, executable, socket and peer identity controls.
- Add portable documentation and 92 offline tests; package independently of the installed adapter.
