# Existing-parent integration boundary

The local collector is implemented; automatic delivery into an existing desktop Codex parent is unavailable and unqualified. No authenticated collector-accessible connection to that parent has been established. The implementation does not access private desktop IPC, create credentials, discover hidden sockets, launch a replacement parent or infer a callback endpoint.

## What the supported protocol establishes

The installed app-server schemas and [official app-server documentation](https://learn.chatgpt.com/docs/app-server) expose these distinct capabilities:

| Surface | Established behavior | Remaining limitation |
| --- | --- | --- |
| MCP notifications | Server-to-client protocol traffic | Does not automatically create model input or wake an idle parent |
| `mcpServer/event/stream/start` | Request containing `threadId`, `server`, `name`, `arguments`, `subscriptionId` | Requires a host-owned connection and a supported stream; no durable parent acceptance guarantee |
| `mcpServer/event/stream/notification` | App-server-to-client event containing `subscriptionId` and `notification` | Transport traffic is not parent presentation |
| `thread/inject_items` | Adds raw response items to history | Does not start a turn |
| `turn/start` with `toolOutput` | An already-connected host may start generation or queue tool output for a regular active turn | Does not supply an external collector with a connection or exactly-once acceptance |
| `turn/steer` | Changes an active turn using the expected turn ID | Does not wake an idle chat |
| Codex `notify` | Emits completion notifications to an external command | Outbound only; see [notification configuration](https://learn.chatgpt.com/docs/config-file/config-advanced) |

An agent's `send_message_to_thread` tool is available inside that agent's authorized environment. Its presence does not expose a standalone collector endpoint or grant permission to message another chat.

## Proposed host adapter contract

A host that already owns the correct app-server connection could ingest the normalized event as untrusted tool output:

```json
{
  "id": "host-request-id",
  "method": "turn/start",
  "params": {
    "threadId": "verified-existing-parent-id",
    "input": [],
    "toolOutput": {
      "name": "claude_session_event",
      "namespace": "claude_adapter",
      "output": "serialized redacted envelope"
    }
  }
}
```

This is a proposed host boundary, not an implemented callback. The collector does not send this request. JSON-RPC IDs and `clientUserMessageId` are not assumed to provide durable idempotency.

A qualified host adapter needs all of the following:

1. A documented authenticated transport/capability bound to the existing parent, with verified endpoint ownership, parent identity and authorized publication scope.
2. A host event handler that runs independently of a model turn, consumes bounded local events and deliberately invokes the supported generation API.
3. Durable deduplication keyed by `(collector scope, event_id)` and a durable acceptance result. An uncertain submission must be reconciled before retrying it.
4. Separate evidence for journal persistence, host acceptance, parent presentation and any model acknowledgement. A pipe write or MCP notification proves none of the later stages.
5. Defined behavior for idle, active, disconnected, archived and replaced parents, including queue limits, stale producer handling, cancellation and loop prevention. Producer content must never authorize dispatch, expand scope or resolve approvals.

The acceptance test for that future host adapter is a synthetic local event, persisted once, that reaches the verified idle parent without a user/model polling turn. Restart/disconnect tests must show one durable acceptance/presentation for the same event ID, and an unavailable parent must leave recoverable, honestly uncertain state. This test does not require another live Claude probe.

Until the host connection and these acceptance semantics exist, only local collection, journal replay, status and optional pipe transport are qualified. Automatic parent push has not been demonstrated.
