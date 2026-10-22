# Operating procedure

## Establish the intended contact

Use confirmed task context to identify the authorized running Claude session, the executable it runs, the message and its permitted scope. A display name or a peer asking for approval is not authorization. Retain an existing exact target rather than broadening discovery. Choose one sender for the operation; concurrent senders using different journals cannot share a deduplication guarantee.

Read the selected target before sending. The adapter checks the session registry and process identity, executable, allowed socket directory, private socket ownership and the connected peer PID. It supports only the pinned runtime/protocol combination. A different version needs an explicit compatibility review, not a relaxed guard.

If process inspection is unavailable inside the executor sandbox, treat that as unknown visibility. Use the host's normal permission/escalation mechanism for the same bounded read-only preflight when already authorized and permitted. Re-run the same check after that boundary is granted. Do not disable guards, change global sandbox settings, substitute a different session, infer that the process is absent, or resend while resolving visibility. If permission is refused, report the actual error and retain uncertainty.

## Dispatch once and keep observing

1. Put the exact authorized message in a UTF-8 data file. Include the objective, scope, constraints, expected acknowledgement and requested result evidence. Avoid pasting credentials or unrelated transcripts.
2. Choose and record one operation UUID. Use an existing user-owned state directory with mode `0700`; journal files must be singly linked regular files with mode `0600`. The client takes an exclusive lock for the invocation.
3. Run `send` with `--authorized-one-message`, the message file, UUID and total observation budget. The workflow first reads/pins the target, commits the durable intent, sends once and persists the send result. It requests an exact unique receipt ACK.
4. Let the same client/helper make repeated bounded waits. Inbound echoes, unrelated events and a quiet interval are not completion. Both a transcript byte cursor and a helper stream/sequence cursor advance independently. Evidence and those cursors commit in one journal transaction.
5. Report the fields actually observed: socket transport, matching receipts, transcript arrival, acknowledgement and gaps. Work acceptance, execution and verified results require their own evidence; the adapter does not synthesize those states from an ACK.

The appended correlation instructions count toward the 4,000-character message bound. The UTF-8 input file also has a 16,000-byte read bound. The overall `--wait` budget must be 1–3,600 seconds; a client RPC is bounded by both the remaining operation budget and a maximum 75-second timeout. Owned-helper cleanup adds a short bounded shutdown period after the observation deadline.

## Resume, replay and uncertain outcomes

Use `observe` with the same operation ID, target/runtime and state directory to continue after a nonterminal outcome. It never sends, even if the original process died between committing intent and writing any bytes. This conservative behavior can leave a definitely unsent operation consumed; preventing an accidental duplicate is the guarantee, not exactly-once delivery.

On helper restart, saved transcript observation may continue if both target and transcript identity remain valid. A listener gap is recorded. The old reply address cannot be revived, and unobserved peer events are lost. Peer evidence already committed is available through `replay` even when the live target has exited. Saved evidence is explicitly not a live check.

If the helper or stdout pipe fails after a possible write, the client is marked unusable. It neither repeats the RPC nor launches a replacement sender. Do not generate a different ID to work around that outcome. A genuinely new authorized message is a new operation only after the uncertainty has been consciously resolved.

If the transcript is replaced/truncated, the target process changes, the journal becomes unavailable/full, or the stream ID mismatches, stop advancing the affected cursor and preserve the last committed evidence. Reconciliation must state the discontinuity. An oversized record produces an explicit gap and bounded progress; this does not establish what its skipped content said. Truncated visible text can hide an ACK, so absence is inconclusive.

SIGINT/SIGTERM cancel this client's observation and close its owned helper. They do not signal Claude. A held/refused receipt ends this bounded workflow without resending. It does not grant permission to alter Claude's incoming-message policy.

## Optional live acceptance check

Offline tests are sufficient for developing this package but cannot prove that a particular live Claude installation accepts it. A live check requires explicit authorization for one message to one verified target. The optional `probe --request-id ... --authorized-one-message --wait 45` sends a fixed no-work request: no file changes, commands, new tasks, settings or permission changes, followed by the unique receipt ACK request. It uses the same durable journal and no-resend rules.

Record transport, arrival and correlated target ACK separately. A timeout or unrelated reply does not pass the check. Never rerun with a fresh operation ID just because the first outcome is uncertain. Keep any resulting live receipts and transcripts outside this repository.

## Maintenance boundaries

Run the offline suite before changing protocol handling. Preserve target/PID/start-time/executable/socket/peer checks, explicit limits and uncertainty reporting. Tests may need permission to bind temporary local sockets and signal their own Python fixtures; that is not authorization to contact Claude.

Develop an isolated source copy while an installed adapter carries active traffic. Do not swap code or restart an installed helper as part of package validation. Archive the source revision and test evidence separately from private runtime state. Publication does not itself install or activate the adapter.
