# Provenance and rights notice

Version **0.3.0 candidate**, prepared 22 October 2026.

This maintained package continues the original local adapter implementation developed during the requesting user's Codex task. Its starting Python sources were the sanitized `0.1.0-review.3` review distribution (`adapter.py`, `client.py`, `test_adapter.py`). This version adds the durable workflow/journal, bounded stream-aware observation, explicit transcript gaps and offline regression tests.

The 0.3.0 candidate adds a bounded receive-only collector, a separate event journal/status lane, optional pipe output, and a documented host integration boundary. It is developed on a feature branch while the qualified 0.2.0 source and installation remain separate. No live Claude probes were sent for the new collector; synthetic socket and process fixtures do not establish live peer publication or parent wake-up acceptance.

Documentation in this repository was written for this package from its implementation and authorized operating requirements. The earlier operational companion and its organization-specific skill text are not included. No vendor runtime implementation, SDK, credentials, user configuration, real transcript, private session identity, installation path or live integration receipt is bundled. Tests use synthetic fixtures. Product names describe interoperability and do not imply vendor affiliation or endorsement.

The earlier review distribution stated: **“No license grant has been established for this snapshot.”** That limitation is preserved: no open-source or commercial reuse license, ownership assignment or third-party entitlement is inferred here. The user explicitly authorized implementing this continuation and publishing it to a new private personal GitHub repository. That specific publication instruction is not a general redistribution license. No license file has been invented.

The original implementation had local macOS integration evidence for Claude Code 2.1.299 / peer protocol 1. This continuation has offline tests only; no fresh live Claude message was sent while implementing or validating it. Prior live evidence must not be represented as acceptance of the changed workflow. The installed adapter was left unchanged during this work.

The source uses only Python's standard library at runtime. GitHub Actions used for offline CI are referenced at fixed upstream commits and are not vendored. Their upstream terms remain separate.
