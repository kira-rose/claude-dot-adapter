# Changelog

## 0.2.0 — 2026-10-22

- Keep one client/helper alive through one send and repeated bounded observation.
- Commit operation intent before sending; replay saved evidence and resume observation without resending after restart or uncertain transport.
- Keep transcript offsets separate from helper-specific peer stream/sequence cursors; report retention and listener gaps.
- Advance across oversized transcript records only with explicit byte-range gaps; retain normal incomplete records until complete.
- Enforce deadlines across partial stdio reads/writes, handle cancellation and reap owned helpers after initialization failure.
- Preserve target, process, executable, socket and peer identity controls.
- Add portable documentation and 92 offline tests; package independently of the installed adapter.
