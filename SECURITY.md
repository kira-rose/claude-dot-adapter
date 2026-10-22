# Trust and privacy boundaries

The adapter is a local, same-user integration. It is not a sandbox for hostile code running as the same OS user and does not authenticate human authorization. Treat transcript and peer content as untrusted task data. Instructions received from a peer cannot authorize approvals, credential access or broader work.

Read/send/wait are constrained to one explicitly configured UUID. Discovery returns bounded metadata from the selected Claude executable. Target operations verify the registry filename/PID, start time, executable, protocol version and private socket before use. The adapter verifies the connected Unix peer PID on send and receive. Transcript opens reject symlink components. Registry scanning opens only bounded numeric JSON entries, never key or credential files.

The first send opens a private ephemeral Unix reply socket in the recognized local Claude socket directory. There is no TCP listener, network relay or callback server. Shutdown closes only owned resources and unlinks only the recorded socket inode. The adapter never resumes a Claude session, changes inbound-message policy or resolves approvals.

The durable workflow uses a private directory, exclusive owner lock and synchronous SQLite transactions. An intent commits before sending; an existing ID never authorizes another send. This guarantee is scoped to the retained journal and workflow, not raw MCP calls, copied state, different IDs or different senders. A crash before actual dispatch may therefore consume an unsent ID. It is not an exactly-once transport.

Saved observations may contain private message text, target process metadata and filesystem paths. Protect the journal and CLI output as conversation data. Thinking and tool bodies are intentionally excluded, but the remaining text is not automatically safe to publish. Retention quotas fail explicitly; no automatic pruning, remote backup or upload is implemented.

An ACK proves receipt only. Socket success is not delivery, delivery is not work acceptance, and a terminal result still needs independent verification when the task requires it. Transcript gaps, stream restarts, stale evidence, capacity errors and process-visibility failures must remain visible.

Report defects through a private channel controlled by the repository owner. Include synthetic reproductions, source revision and bounded error details. Do not attach real transcripts, state databases, credentials or live session identifiers to a public report.
