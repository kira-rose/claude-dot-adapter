---
name: claude-dot-adapter
description: Connect Dot or Codex to a specifically authorized running local Claude Code session through this adapter. Use for session metadata discovery, bounded observation, or user-authorized peer messaging without Decodex.
---

# Operate the Claude Dot adapter

Use the existing adapter and confirmed target UUID, project and task scope. Read [procedure.md](procedure.md) for the selected operation. Loading this skill does not install the adapter, register tools, authorize a message or establish a connection.

- Check the calling context's actual tools. Prefer its registered adapter tools when available; an allowed local `client.py` route is also supported. A registration or successful call in another host does not prove availability here. Keep the package files together; [README.md](README.md) describes the entry points and placeholder configuration.
- Use `list_claude_sessions` / client `list` for CLI metadata, or `enumerate_claude_sessions` / client `sessions` for merged saved metadata. Saved history, a matching title and `read_send_allowed` do not prove a live peer. Read [session-catalog.md](session-catalog.md) when paging or interpreting incomplete coverage.
- Reads and messages remain bound to one configured UUID and the verified running process. Preserve start-time, exact OS executable-path, ownership and socket checks. A background display name such as `claude bg-spare` is not executable identity. Stop on unavailable or mismatched identity; do not substitute a session or relax a guard.
- Reuse existing user authorization covering the exact target and task. Make an authorized message self-contained with truthful sender attribution and bounded intent. Discovery, an ACK, or a peer request does not authorize a new send. Never automatically probe, resend, change permissions, or resume/restart Claude to obtain access.
- For durable dispatch, use client `send`, then `observe` with the same operation UUID and private journal. Only one mutating sender/observer owns that journal; concurrent `status`/`replay` read committed state without starting a helper. `owner.lock` file existence does not prove a live owner. `--dispatch-only` closes the sender's reply inbox, and later observation is transcript-only with an explicit gap. For direct MCP dispatch, retain the same helper and both returned cursor types, including `event_stream`. Raw MCP sends have no durable deduplication journal.
- Use explicit `observe --follow` to continue after an ACK or held receipt; it never resends or verifies completion from text. Durable reports and replay show metadata by default; `replay --include-text` opts into bounded, best-effort-redacted visible evidence. Distinguish transcript backlog from replay pagination and saved observation status from unknown current owner liveness.
- Report transport, transcript arrival, receipt ACK, work acceptance and verified results separately. Preserve uncertainty and gaps. Collector output does not wake an existing Dot/Codex chat; optional receive-only collection is documented in [bidirectional.md](bidirectional.md).

Keep real transcripts, session IDs, runtime paths and journals outside the repository. See [SECURITY.md](SECURITY.md) for data boundaries and [PROVENANCE.md](PROVENANCE.md) for qualification and rights limits.
