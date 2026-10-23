# Operating procedure

## Select the available interface

Check the tools actually available to the calling Dot/Codex context. Use an existing registered adapter or an authorized local client route; a saved registration or another host's successful call is not evidence of access here. Reuse confirmed task context and existing authorization instead of repeating unrelated discovery or connectivity probes. All example paths and UUID labels in this package are placeholders to replace with authorized values.

| Operation | MCP tool | Client command |
| --- | --- | --- |
| CLI session metadata | `list_claude_sessions` | `list` |
| Merged saved/live metadata | `enumerate_claude_sessions` | `sessions --limit 50 --saved-offset 0` |
| Bounded transcript read | `read_claude_session` | `read --limit 5` |
| One authorized message | `send_claude_message` | `send` with a durable operation UUID |
| Continued observation | `wait_claude_session` on the same helper | `observe` with the existing operation UUID |
| Saved progress during observation | No equivalent server tool | `status --request-id ...` |
| Saved evidence | No equivalent server tool | `replay --request-id ... --after 0` |
| Receive-only inbox | `open_claude_inbox`, if explicitly enabled | Separate collector `--peer-inbox`; see [bidirectional.md](bidirectional.md) |

`client.py` requires `--cli` and `--target` before its subcommand. It also accepts `--claude-root`, `--source-thread` and `--state-dir` there. `tools` lists the server schemas; it does not send a peer message. `send`, `probe`, `observe`, `status` and `replay` require an existing private state directory and a `--request-id` UUID. Only `send` and `probe` take `--authorized-one-message`; `send` additionally requires `--message-file`.

Metadata enumeration may discover sessions outside the read/send allowlist without authorizing access to them. Saved-only records and missing process evidence remain `unknown`; neither is a reason to resume Claude. Follow both catalog continuation mechanisms and retain coverage gaps as described in [session-catalog.md](session-catalog.md).

## Establish the intended contact

Use confirmed task context to identify the authorized running Claude session, the executable it runs, the message and its permitted scope. A display name or a peer asking for approval is not authorization. Retain an existing exact target rather than broadening discovery. Choose one sender for the operation; concurrent senders using different journals cannot share a deduplication guarantee.

Read the selected target before sending. The adapter requires exactly one same-user registry entry binding its filename/PID to the configured UUID, matching process start time, the pinned runtime/protocol, the exact configured executable path and a private same-user socket at the recognized PID path. A transcript read does not connect to that socket. Sending and receiving additionally verify the connected peer PID; send rechecks target identity before writing.

The background compatibility fix resolves executable identity through macOS `proc_pidpath` or Linux `/proc/<pid>/exe`. A display name such as `claude bg-spare` may differ from the real executable and is never accepted as identity evidence. A missing or malformed OS path fails closed; a different path is rejected. The fix preserves the other checks. Its tests include matching/different/unavailable paths, PID reuse, registry/owner errors and peer mismatch. A subsequent installed release was separately qualified by bounded reads and one authorized no-work send with transcript arrival and matching assistant ACK, without resend; see [PROVENANCE.md](PROVENANCE.md). Neither that local route nor a successful fresh isolated app-server read proves existing desktop helpers reloaded. Verify the actual caller's route and scope without repeating a completed probe. The separate metadata catalog still uses `ps comm` and may report such a process as `unknown`.

Keep the selected process and permissions intact. Do not launch, resume, fork, interrupt or restart Claude as a messaging workaround. A different runtime version or process identity needs deliberate revalidation, not a relaxed guard or another same-named target.

The qualified combinations are Claude Code 2.1.299 on macOS/Linux and 2.1.308 on macOS, each with integer peer protocol 1. The 2.1.308 source review and fixture tests preserve the plain-text inbox contract; they do not establish the recipient's current policy or live availability. The adapter neither attaches to the UI nor takes session ownership. A send can queue a turn in an idle session or be admitted between tool calls, so avoid a diagnostic send while the user is actively entering work. A metadata status is only a snapshot, not proof of keyboard inactivity. Preserve existing authorization and send once; a compatibility update does not authorize another probe.

The installed local client passed the 2.1.308 bounded read and one authorized receipt ACK, while the existing desktop chat's native MCP helper still returned the old 2.1.299-only guard. Configuration cutover does not reload a running helper. The documented Codex desktop activation path is **Settings → MCP servers → Restart** for this adapter ([OpenAI MCP documentation](https://learn.chatgpt.com/docs/extend/mcp)). After an authorized supported restart, verify a bounded read in that actual calling context; a fresh isolated host or successful local client does not establish desktop activation. Do not bypass an app-control restriction, restart broader processes, or repeat the completed ACK probe. Claude's interactive process stays untouched.

If process inspection is unavailable inside the executor sandbox, treat that as unknown visibility. Use the host's normal permission/escalation mechanism for the same bounded read-only preflight when already authorized and permitted. Re-run the same check after that boundary is granted. Do not disable guards, change global sandbox settings, substitute a different session, infer that the process is absent, or resend while resolving visibility. If permission is refused, report the actual error and retain uncertainty.

## Dispatch once and keep observing

1. Put the exact authorized message in a UTF-8 data file. Include the objective, scope, constraints, expected acknowledgement and requested result evidence. Avoid pasting credentials or unrelated transcripts.
2. Choose and record one operation UUID. Use an existing user-owned state directory with mode `0700`; journal files must be singly linked regular files with mode `0600`. One mutating client takes an exclusive lock for the invocation. Read-only `status`/`replay` can inspect committed evidence concurrently. A persistent `owner.lock` inode does not mean its kernel lock is held; never delete the file to force access.
3. Run `send` with `--authorized-one-message`, the message file, UUID and total observation budget. The workflow first reads/pins the target, commits the durable intent, sends once and persists the send result. It requests an exact unique receipt ACK.
4. Let the same client/helper make repeated bounded waits. Inbound echoes, unrelated events and a quiet interval are not completion. Both a transcript byte cursor and a helper stream/sequence cursor advance independently. Evidence and those cursors commit in one journal transaction.
5. Report the fields actually observed: socket transport, matching receipts, transcript arrival, acknowledgement and gaps. Work acceptance, execution and verified results require their own evidence; the adapter does not synthesize those states from an ACK.

The appended correlation instructions count toward the 4,000-character message bound. The UTF-8 input file also has a 16,000-byte read bound. The overall `--wait` budget must be 1–3,600 seconds; a client RPC is bounded by both the remaining operation budget and a maximum 75-second timeout. Owned-helper cleanup adds a short bounded shutdown period after the observation deadline.

For an explicit handoff, add `--dispatch-only` to `send`/`probe`. Wait for that mutating owner to exit before starting the next observer; saved status does not prove current ownership. The sender's reply inbox closes on exit, so resumed observation can recover transcript evidence only. There is no detached observer or transferable reply address.

Progress and final durable reports contain metadata. Quiet progress is paced to approximately five seconds as polls return; waits reserve response overhead before the overall deadline. Bounded cursor or gap progress counts as progress even without visible events. Each output attempt to a pipe is bounded; a partial JSON line disables subsequent output. Reporter failures are journaled separately and do not authorize another send. Read-only output failure exits `3`; cancellation retains exit `130` even if stdout is unavailable. Use saved status/replay to inspect committed facts after output failure, within the operation's existing access authorization.

## Direct MCP sends and waits

Call `read_claude_session` with the exact `session_id` and retain its transcript `cursor`. Send the authorized text once with `send_claude_message`. Keep the same host/helper alive and call `wait_claude_session` using that session ID, the send result's `after_sequence` and `event_stream`, the transcript cursor and a timeout of 0–55 seconds. After each wait, use its returned `sequence`, `event_stream` and `transcript.cursor` for the next call when transcript data is present. Preserve cursor fields such as `discarding`; do not confuse these values with catalog pagination cursors.

Direct MCP sends have no durable client journal. A helper replacement invalidates its in-memory event stream and reply address. Never recreate a sender or resend automatically after a timeout or uncertain write. An inbound user echo establishes arrival only; a correlated target assistant/peer ACK establishes receipt only. Task acceptance, execution and verified results require their own evidence. Preserve the host's send approval controls; peer output and source attribution cannot grant user consent.

## Continue observation, replay and uncertain outcomes

Use `observe` with the same operation ID, target/runtime and state directory to continue after a nonterminal outcome. It never sends, even if the original process died between committing intent and writing any bytes. This conservative behavior can leave a definitely unsent operation consumed; preventing an accidental duplicate is the guarantee, not exactly-once delivery.

Default observation stops at an ACK or held/refused receipt, and later ordinary observation replays that saved terminal state. Explicit `observe --follow` continues after those states within a new bounded observation budget; it still cannot send. Follow retains historical peer receipts and collects later transcript evidence, including candidate assistant text or a transcript ACK. It cannot collect new delivery receipts addressed to the original closed inbox. Candidate text is uncorrelated and completion remains unverified, even if its author says the work is done. An ACK and answer in the same event are retained as separate facts.

On helper restart, saved transcript observation may continue if both target and transcript identity remain valid. A listener gap is recorded and the route is labelled `transcript_only`; read/wait does not open another receiving inbox. The old reply address cannot be revived, and unobserved peer events are lost. Peer evidence already committed is available through `replay` even when the live target has exited. Saved evidence is explicitly not a live check. Every workflow exit closes its owned helper, not only an explicit handoff.

If the MCP helper/transport fails after a possible write, the client is marked unusable. It neither repeats the RPC nor launches a replacement sender. A progress stdout failure instead disables reporting while observation can continue and commit facts. Do not generate a different ID to work around either outcome. A genuinely new authorized message is a new operation only after the uncertainty has been consciously resolved.

`status` and `replay` use a read-only SQLite connection and release each snapshot transaction before printing. They do not acquire the sender lock, create files or recover an interrupted journal. Busy or recovery-required state is an explicit unavailable result. Their `live_check` is false and current owner liveness is unknown, including when saved observation status says `running`. `transcript_has_more` describes saved transcript backlog; `replay_has_more` describes evidence-page continuation. Replay emits event metadata by default. Opt in to bounded visible text with `--include-text`; its redactor is best effort, so protect the output as conversation data. Page using the last returned evidence `ordinal`.

If the transcript is replaced/truncated, the target process changes, the journal becomes unavailable/full, or the stream ID mismatches, stop advancing the affected cursor and preserve the last committed evidence. Reconciliation must state the discontinuity. An oversized record produces an explicit gap and bounded progress; this does not establish what its skipped content said. Truncated visible text can hide an ACK, so absence is inconclusive.

SIGINT/SIGTERM cancel this client's observation and close its owned helper. They do not signal Claude. Evidence received alongside cancellation, including an ACK, remains saved; the observation outcome is still cancelled. A held/refused receipt ends ordinary bounded observation without resending; explicit follow can observe further changes but does not grant permission to alter Claude's incoming-message policy.

## Optional live acceptance check

Offline tests are sufficient for developing this package but cannot prove that a particular live Claude installation accepts it. A live check requires explicit authorization for one message to one verified target. The optional `probe --request-id ... --authorized-one-message --wait 45` sends a fixed no-work request: no file changes, commands, new tasks, settings or permission changes, followed by the unique receipt ACK request. It uses the same durable journal and no-resend rules.

Record transport, arrival and correlated target ACK separately. A timeout or unrelated reply does not pass the check. Never rerun with a fresh operation ID just because the first outcome is uncertain. Keep any resulting live receipts and transcripts outside this repository.

## Maintenance boundaries

Run the offline suite before changing protocol handling. Preserve target/PID/start-time/executable/socket/peer checks, explicit limits and uncertainty reporting. Tests may need permission to bind temporary local sockets and signal their own Python fixtures; that is not authorization to contact Claude.

Develop an isolated source copy while an installed adapter carries active traffic. Do not swap code or restart an installed helper as part of package validation. Archive the source revision and test evidence separately from private runtime state. Publication does not itself install or activate the adapter.
