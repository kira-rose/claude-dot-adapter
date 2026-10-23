# Claude Dot Adapter

A local adapter that lets Dot or Codex observe and send peer messages to one explicitly selected, already running Claude Code session. It connects directly through Claude Code's local Unix socket and visible transcript; no Decodex service is required. The public source repository is [kira-rose/claude-dot-adapter](https://github.com/kira-rose/claude-dot-adapter). [SKILL.md](SKILL.md) is the reusable skill entry point; keep it with this package and its linked procedure.

Version **0.4.0-dev**, a development candidate rather than a tagged stable release. Observation, messaging and collection use the Python standard library; saved-session discovery optionally requires exactly `claude-agent-sdk==0.2.172`, pinned in `requirements-catalog.txt`. The protocol guard accepts **Claude Code 2.1.299 on macOS/Linux and 2.1.308 on macOS, with peer protocol 1**. Other combinations fail closed until separately verified. Windows is unsupported. There is no qualified route for waking an existing Dot/Codex chat.

Qualification recorded on 23 October 2026: an earlier catalog build was installed in an isolated runtime and verified through a fresh native MCP host for saved metadata enumeration. The background executable-path fix passed the 208-test local suite and was subsequently installed and qualified through a bounded target read, one authorized unique no-work send, transcript arrival and matching assistant ACK, with no resend. A later read confirmed the same process and ACK; a fresh isolated native app-server call also read the target. This qualifies the installed release and tested local routes. It does not prove that existing desktop helpers reloaded or that a fresh app-server call activated them. See [PROVENANCE.md](PROVENANCE.md).

The macOS 2.1.308 update subsequently passed 218 tests in both source and staged isolated runtime, was installed by a guarded versioned cutover, and completed a bounded read and one authorized receipt marker through the installed local client. The matching assistant transcript ACK was observed with zero resends; the same process remained interactive. A separate native call in the existing desktop chat still returned the old 2.1.299-only guard, establishing that helper had not reloaded. Local-client qualification does not establish desktop helper activation.

The subsequent observation-reliability changes are source-only fixture work. They have not been installed, published or tested against a live session. Earlier live qualification does not qualify this working revision.

## Entry points

| Entry point | Purpose | Lifetime and persistence |
| --- | --- | --- |
| `adapter.py` | Six MCP tools: list, enumerate metadata, read, send, wait, open receive-only inbox | One read/send target per helper; peer events and send tracking are in memory |
| `client.py` | Single send followed by repeated bounded observation | One helper per invocation; private SQLite intent, evidence and cursors survive invocation exit |
| `collector.py` | Receive-only collection, status, replay and optional pipe publication | Foreground bounded run; a separate private event journal and consumer cursors survive exit |

A host may expose only a subset of the six server tools. Loading the skill or copying the source does not install dependencies, register MCP tools or activate them in a later chat.

The collector observes assistant transcript events without an outstanding send. Opting into `--peer-inbox` also opens an ephemeral Unix inbox for unsolicited messages from the same verified target process. It does not announce the address, dispatch work or wake a parent. See [bidirectional.md](bidirectional.md) for usage and [host-integration.md](host-integration.md) for the precise missing host contract.

Use the client workflow when the operation must remain identifiable after a crash. It commits an operation ID before sending and never sends that ID again, even if the helper exits before returning a message ID. Retain the same state directory and operation ID to preserve this guarantee. Direct MCP sends do not use the durable journal.

The adapter does not start/resume Claude, change its settings, answer approvals or retry a failed/uncertain send. A peer message is explicitly labelled as a peer message, not human consent. The caller must already have authorization to send the particular message to the selected target. Command flags express that authorization; they do not authenticate a human decision.

## Run the client

Requires Python 3.10 or newer, an absolute path to the verified Claude executable, and the exact authorized session UUID. Keep the Python implementation files together. Run these examples from the repository directory, replacing the placeholders with your authorized values:

```sh
python3 -B client.py --cli /absolute/path/to/claude \
  --target exact-authorized-session-uuid read --limit 5
```

This verifies the target's registry, PID/start time, executable and private socket, then reads bounded visible transcript events. Executable identity uses macOS `proc_pidpath` or Linux `/proc/<pid>/exe`, with exact matching to the configured path and no display-name fallback. Connected peer identity is checked when sending or receiving, not by a transcript-only read. `list` obtains Claude's read-only session metadata inventory; it does not authorize other sessions or prove that the inventory is complete.

`sessions --limit 50` merges saved metadata with live and retained background metadata. It reports unknown liveness and semantic activity explicitly. Its separate catalog process check still compares `ps comm` and can leave renamed background processes as `unknown`; the target access check uses the OS executable path. Enumeration does not expand the read/send target or verify a peer endpoint. See [session-catalog.md](session-catalog.md) for the optional dependency, data boundaries, two pagination levels and coverage limits. Existing installed tool allowlists do not automatically expose this tool.

For a durable operation, create a private state directory outside the repository, prepare the authorized message as UTF-8 data, and choose a new UUID once:

```sh
mkdir -m 700 /absolute/private/adapter-state
python3 -c 'import uuid; print(uuid.uuid4())'

python3 -B client.py --cli /absolute/path/to/claude \
  --target exact-authorized-session-uuid \
  --state-dir /absolute/private/adapter-state \
  send --request-id stable-operation-uuid --authorized-one-message \
  --message-file /absolute/private/message.txt --wait 45
```

The client appends a unique receipt-acknowledgement line. It keeps the same helper alive through the send and all observation polls, emitting bounded JSON metadata progress and a final saved-state report. An inbound user transcript echo establishes arrival only. A matching target assistant line or verified peer reply establishes acknowledgement of receipt only. Additional assistant text, including text beside the ACK, is retained as candidate evidence with unverified correlation and work completion.

`send --dispatch-only` sends once, records the result and releases the workflow owner. It closes the original peer reply inbox on exit. A later observer can continue transcript reads, but cannot recover missed replies sent to that inbox. A repeated dispatch with the same UUID returns saved state without starting a helper or sending again.

After timeout or helper exit, resume observation with the same identity and journal:

```sh
python3 -B client.py --cli /absolute/path/to/claude \
  --target exact-authorized-session-uuid \
  --state-dir /absolute/private/adapter-state \
  observe --request-id stable-operation-uuid --wait 45
```

`observe` requires an existing durable operation and never sends. It resumes the saved transcript cursor and records a listener gap: peer replies missed while the original helper was absent cannot be recovered. A new helper stream does not open a replacement reply inbox; resumed observation is labelled `transcript_only`. Already saved peer evidence remains replayable. A changed target identity is refused, including a replacement process using the same session UUID. Plain `observe` replays a saved ACK or held/refused outcome; explicit `observe --follow` continues bounded observation after it. Follow never infers completion from later text or an idle target.

`status --request-id ...` and `replay --request-id ... --after 0 --limit 100` read committed state without starting a helper, taking the workflow owner lock or checking live status. They can run while the single mutating observer owns the journal. A short snapshot releases its database read transaction before output. Hot-journal recovery, unexpected WAL state and unsafe files are refused without repair. The persistent `owner.lock` file is not proof of an active owner; the kernel lock ends when its descriptor closes. Never delete it to bypass ownership.

Durable final reports, status and replay omit message text by default. `replay --include-text` explicitly includes bounded visible text with best-effort redaction; this is not a complete secret or PII filter. Continue replay with the last returned evidence `ordinal`, not the snapshot's maximum `last_ordinal`. `replay_has_more` describes saved-event pagination; `transcript_has_more` describes the last observed transcript backlog, or is null for unknown legacy state. Preflight coverage is separate from later gap counts. These reports have `live_check: false` and unknown owner liveness: saved `running` state can survive a crash. Matching configured target/runtime paths are still required.

Exit codes: `0` for acknowledged observation, successful dispatch-only or successful read/status/replay; `2` for an unacknowledged, held, failed or timed-out operation; `3` when output is unavailable; `130` for cancelled observation, including when output also fails; `1` for other errors. Follow ends at its budget with code `2` even when an earlier ACK remains saved. Cancelling observation preserves received evidence and stops the owned helper, not the Claude session or previously requested work.

Blocked or broken stdout cannot hold a workflow indefinitely: each pipe-output attempt has a short deadline, and partial output disables further frames. Committed evidence remains in the journal. Ordinary progress callback failures are recorded separately and do not abort observation. Quiet progress is emitted at roughly five-second intervals when polls return. Client waits reserve response overhead inside the operation budget and do not begin a final poll after that reserve is exhausted; cleanup and filesystem latency are not a hard real-time guarantee. A client rejects overlapping RPCs before the second write.

## Register the MCP tools in Codex

Codex supports a local stdio command with explicit arguments in an MCP server entry. Registration and tool timeout fields follow the [official MCP configuration documentation](https://learn.chatgpt.com/docs/extend/mcp?surface=cli).

The following is a template, not an installed configuration:

```toml
[mcp_servers.claude_sessions]
command = "/absolute/path/to/python3"
args = ["-B", "-u", "/absolute/path/to/claude-code-adapter/adapter.py", "--cli", "/absolute/path/to/claude", "--target", "exact-authorized-session-uuid"]
enabled_tools = ["list_claude_sessions", "enumerate_claude_sessions", "read_claude_session", "send_claude_message", "wait_claude_session"]
tool_timeout_sec = 75

[mcp_servers.claude_sessions.tools.send_claude_message]
approval_mode = "prompt"
```

Both server and client accept `--claude-root /absolute/path/to/config-root` to select a nondefault Claude configuration directory. An optional `--source-thread actual-originating-chat-uuid` identifies the true sender; omit it when unknown. Do not invent a chat ID. The example deliberately excludes the separate receive-only `open_claude_inbox` tool. Preserve an existing host's approved tool scope and send controls; a native tool approval setting does not govern independent shell clients.

MCP operating sequence: read once, retain the transcript cursor, send exactly once when authorized, then repeatedly wait on the **same helper** using the send result's `after_sequence` and `event_stream` plus the independent transcript `cursor`. Update both cursors from every result. A nonzero peer sequence without its stream ID is rejected. Native waits last at most 55 seconds each. Raw MCP callers must implement their own durable operation tracking if needed.

## Evidence and limits

| Observation | What it establishes |
| --- | --- |
| `transport: sent` | All bytes were written to the verified local socket |
| `transport: uncertain` | Some bytes were written; delivery is unknown |
| Matching delivery receipt | Claude's reported delivery status for that message |
| Inbound user transcript echo | The message appeared in the target transcript |
| Correlated assistant/peer ACK | Receipt was acknowledged |
| Work acceptance/completion | Requires separate explicit target evidence and result verification |

Transcript reads scan at most 256 KiB, return at most 100 events and cap visible text at 2,000 characters. Thinking, tool inputs/results and system records are omitted. Oversized records are discarded in bounded chunks with explicit byte-range gaps; partial JSON is never parsed. A cursor may carry `discarding: true` until the skipped record ends. Transcript rotation and invalid cursors fail explicitly. Bounded tail snapshots can omit earlier history and report warnings.

Wait results distinguish cursor/gap progress from timeout, even when no visible event is returned. Subsequent transcript scans stop at the wait deadline; final target identity verification remains required. The reliability audit reproduced misleading timeout flags and an extra expired scan, not a completed-record cursor stall.

Peer events have a separate helper-specific stream and sequence. The in-memory buffer retains 200 events and reports retention gaps. A new helper cannot replay an old stream. The journal atomically commits observed evidence with its cursors; it cannot recover events never observed. It holds at most 128 operations and 5,000 events, with a 256 KiB bound per stored JSON value. Capacity errors preserve the last committed cursor and do not silently prune deduplication state. Do not delete or replace a journal to retry an uncertain operation.

The original client workflow is caller-driven bounded observation. The separate collector can remain active for its explicit foreground budget and publish newly committed envelopes to a connected pipe. Neither path provides a daemon or automatic wake-up/delivery to a disconnected Codex chat. Local output and state can contain private conversation metadata and paths: retain them privately and exclude them from source control.

## Validation and maintenance

```sh
python3 -B -m unittest discover -s . -p 'test*.py' -q
```

The suite uses synthetic sessions, temporary Unix sockets and owned Python helpers. It never starts or contacts Claude or a provider. The current observation-reliability source candidate passes **247 tests on macOS with Python 3.14.7**. It has no new live qualification or installed-runtime changes. The historical installed 2.1.308 compatibility revision passed 218 tests, including 16 executable-identity regressions and 10 compatibility regressions. That earlier qualification also compared the signed installed macOS binaries' registry, transcript and plain-text inbox contracts with 2.1.299. Source and fixture checks do not establish live delivery. Linux path/peer cases were synthetic on that host, and Linux 2.1.308 remains rejected. CI is configured for Linux and macOS with Python 3.10 and 3.14; this configuration is not a passing CI result. Earlier remote runs did not execute the test jobs, and no green remote result is claimed here.

See [procedure.md](procedure.md) for operation and uncertainty handling, [SECURITY.md](SECURITY.md) for trust boundaries, and [PROVENANCE.md](PROVENANCE.md) for origin and license status.
