"""Isolated optional SDK metadata worker. Never calls query/resume/messages.

The pinned public API reads file stats and at most 64 KiB at each JSONL end.
Only selected metadata leaves this process; SDK summary/first_prompt/messages
are never serialized, persisted, logged, or used as fallback titles.
"""
from __future__ import annotations

import contextlib
import argparse
import importlib.metadata
import json
import os
import sys

from session_catalog import MAX_BYTES, MAX_ROWS, MAX_SAVED_OFFSET, UUID, packed, text_field, timestamp

SDK_VERSION = '0.2.172'
SDK_SOURCE = 'claude_agent_sdk.list_sessions'


def read_metadata(*, offset=0, version_getter=importlib.metadata.version, sdk_loader=None):
    if type(offset) is not int or not 0 <= offset <= MAX_SAVED_OFFSET:
        return {'status': 'unavailable', 'error': 'invalid_saved_offset'}
    try:
        version = version_getter('claude-agent-sdk')
    except importlib.metadata.PackageNotFoundError:
        return {'status': 'unavailable', 'error': 'sdk_not_installed', 'required_version': SDK_VERSION}
    if version != SDK_VERSION:
        return {'status': 'unavailable', 'error': 'sdk_version_not_qualified', 'required_version': SDK_VERSION}
    try:
        if sdk_loader is None:
            import claude_agent_sdk as sdk
        else:
            sdk = sdk_loader()
        # This API reads bounded portions per file, but scans across the local
        # root before slicing. The parent additionally bounds worker time/output.
        values = sdk.list_sessions(directory=None, limit=MAX_ROWS+1, offset=offset, include_worktrees=False)
        if not isinstance(values, list):
            raise ValueError('unexpected_sdk_result')
        selected = []
        rejected = 0
        output_bytes = 0
        output_limited = False
        consumed = 0
        for value in values[:MAX_ROWS]:
            sid = getattr(value, 'session_id', None)
            if not isinstance(sid, str) or not UUID.fullmatch(sid.lower()):
                rejected += 1
                consumed += 1
                continue
            item = {'session_id': sid.lower(), 'custom_title': text_field(getattr(value, 'custom_title', None),512),
                             'cwd': text_field(getattr(value, 'cwd', None),2048),
                             'created_at': timestamp(getattr(value, 'created_at', None)),
                             'last_modified': timestamp(getattr(value, 'last_modified', None))}
            size = len(packed(item).encode()) + 1
            if output_bytes + size > MAX_BYTES // 2:
                output_limited = True
                break
            selected.append(item)
            output_bytes += size
            consumed += 1
        truncated = len(values) > MAX_ROWS or output_limited
        continuation_limited = truncated and offset + consumed > MAX_SAVED_OFFSET
        return {'status': 'ok', 'sdk_version': version, 'rows': selected,
                'rejected_rows': rejected, 'truncated': truncated,
                'output_byte_limit_reached': output_limited,
                'returned_by_sdk': len(values), 'sdk_offset': offset, 'sdk_limit': MAX_ROWS+1,
                'consumed_sdk_rows': consumed,
                'next_offset': offset + consumed if truncated and not continuation_limited else None,
                'continuation_limit_reached': continuation_limited,
                'sdk_has_snapshot_or_total': False, 'head_bytes_per_file': 65536, 'tail_bytes_per_file': 65536}
    except Exception:
        # Never emit exception text: it may contain paths or transcript text.
        return {'status': 'unavailable', 'error': 'sdk_metadata_failed', 'required_version': SDK_VERSION}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--offset', type=int, default=0)
    args = parser.parse_args()
    # Preserve a dedicated output FD, suppress library stdout/stderr at the OS
    # level, and restore only after SDK objects/payloads have been discarded.
    output = os.dup(sys.stdout.fileno())
    try:
        with open(os.devnull, 'w') as sink:
            os.dup2(sink.fileno(), sys.stdout.fileno())
            os.dup2(sink.fileno(), sys.stderr.fileno())
            with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
                result = read_metadata(offset=args.offset)
        with os.fdopen(output, 'w') as channel:
            output = None
            channel.write(packed(result) + '\n')
    finally:
        if output is not None:
            os.close(output)


if __name__ == '__main__':
    main()
