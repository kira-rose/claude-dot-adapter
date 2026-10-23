# Provenance and rights notice

Version **0.4.0-dev**, development source; qualification summary updated 23 October 2026. This is not a tagged stable release. Publication of source does not install it, change an existing runtime or establish that a caller has its tools.

## Origin and publication scope

This package continues the original local adapter implementation developed during the requesting user's Codex task. Its starting Python sources were the sanitized `0.1.0-review.3` review distribution. Subsequent work added the durable workflow/journal, bounded observation with explicit transcript gaps, receive-only collection, the saved-session metadata catalog and OS executable-path verification for background processes.

The repository owner explicitly authorized publishing the adapter and reusable Dot skill to the existing public [kira-rose/claude-dot-adapter](https://github.com/kira-rose/claude-dot-adapter) repository. The earlier repository name and private-only publication description are historical. This authorization concerns the reviewed source and portable documentation, not private operating evidence or organization-specific material.

The earlier review distribution stated: **“No license grant has been established for this snapshot.”** That limitation is preserved. Public visibility and this specific publication authorization do not imply an open-source or commercial reuse license, ownership assignment or third-party entitlement. No license file or additional grant has been invented.

## Included material and dependencies

Repository documentation and `SKILL.md` were written for this package from its implementation and authorized operating requirements. The organization-specific infrastructure skill master is not included. Examples use placeholders or synthetic fixtures. No real transcript, private session identity, credential, user configuration, installed environment, runtime journal or private live receipt is bundled. Product names describe interoperability and do not imply vendor affiliation or endorsement.

Observation, messaging, journaling and collection use Python's standard library. Saved-session metadata optionally requires exactly `claude-agent-sdk==0.2.172`, declared in `requirements-catalog.txt`; the SDK and its dependencies are not vendored. Its public `list_sessions` API internally reads bounded transcript ends, while this package returns only selected metadata; see [session-catalog.md](session-catalog.md). No vendor runtime implementation is copied into the adapter. The SDK's upstream terms and the terms for GitHub Actions referenced at fixed commits remain separate.

## Qualification and limits

- The guarded compatibility target remains Claude Code **2.1.299 / peer protocol 1**. Earlier local integration established a correlated receipt ACK through a local client route. That evidence does not prove acceptance of every subsequent change, another session, or a different host route.
- A predecessor 0.4.0-dev catalog build was installed in an isolated runtime with the pinned SDK and qualified through a fresh native MCP host for bounded saved metadata enumeration. This established metadata availability, not complete history, a live peer for every saved record, or universal Dot access.
- The background executable-path fix passed **208 local tests on macOS with Python 3.14.7**, including 16 new regressions. Within that suite, the native API case inspected only the owned Python test process; Claude background and Linux cases used synthetic fixtures.
- A subsequent release containing the fix was installed and cut over. Qualification included a bounded read of the exact target, one explicitly authorized unique no-work send, transcript arrival and a matching assistant ACK, without a resend. A later read confirmed the same process and ACK. A fresh isolated native app-server call also read the target. These observations qualify the installed release and the tested local routes; they do not prove that existing desktop helpers reloaded. A fresh app-server call is not evidence of desktop helper activation.
- The separate catalog process check still compares `ps comm` and may leave renamed background processes as `unknown`. Target read/send verification uses the OS-resolved executable path and preserves all identity gates.
- The CI matrix is configured for macOS/Linux and Python 3.10/3.14. Earlier remote jobs did not execute tests; this document does not claim a green remote run or native Linux qualification of the new path check.
- The reusable skill and procedure provide operating guidance. Loading them does not grant access, authorize a message or demonstrate new runtime behavior. Automatic delivery into an existing Dot/Codex parent remains unavailable and unqualified; see [host-integration.md](host-integration.md).

Private installation manifests, live receipts and host-specific evidence remain outside this repository. Keep later validation scoped to its exact revision, runtime and route instead of transferring an older success to a new operation.
