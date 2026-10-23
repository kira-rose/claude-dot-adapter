"""Merge documented SDK saved metadata and CLI live/background metadata.

Only the isolated optional SDK worker reads bounded transcript head/tail data;
it emits allowlisted metadata and never prompt/summary text. No resume/send.
Absence from this catalog never proves a session is stopped.
"""
from __future__ import annotations

import base64
import collections
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import sys
import time

SOURCE = 'claude agents --json --all'
SOURCE_DOC = 'https://code.claude.com/docs/en/agent-view#list-sessions-as-json'
UUID = re.compile(r'^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$')
MAX_BYTES = 2 * 1024 * 1024
MAX_ROWS = 4096
MAX_PAGE = 50
MAX_SAVED_OFFSET = 1000000
SAVED_SOURCE = 'claude_agent_sdk.list_sessions'
SAVED_VERSION = '0.2.172'
SAVED_DOC = 'https://code.claude.com/docs/en/agent-sdk/python#list_sessions'
WAITING = {'permission prompt', 'input needed', 'sandbox request', 'worker request', 'dialog open'}
STATES = {'working', 'blocked', 'done', 'failed', 'stopped'}


class CatalogError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def packed(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)


def fingerprint(value):
    return hashlib.sha256(packed(value).encode()).hexdigest()


def bounded_output(command, *, env=None, timeout=15, maximum=MAX_BYTES):
    """Bound actual captured bytes and elapsed time, not just parsed results."""
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                               stderr=subprocess.DEVNULL, env=env)
    deadline = time.monotonic() + timeout
    output = bytearray()
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                left = deadline - time.monotonic()
                if left <= 0 or not selector.select(left):
                    raise CatalogError('source_timeout')
                block = os.read(process.stdout.fileno(), min(65536, maximum + 1 - len(output)))
                if not block:
                    break
                output.extend(block)
                if len(output) > maximum:
                    raise CatalogError('source_byte_limit')
        try:
            returncode = process.wait(timeout=max(0.001, deadline-time.monotonic()))
        except subprocess.TimeoutExpired:
            raise CatalogError('source_timeout') from None
        if returncode:
            raise CatalogError('source_failed')
        return bytes(output)
    finally:
        if process.poll() is None:
            process.terminate()  # Own short-lived metadata command only.
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1)
        process.stdout.close()


def text_field(value, byte_limit):
    if not isinstance(value, str) or not value or any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in value):
        return None
    # Reject, rather than silently truncate a title/path or emit control data.
    return value if len(packed(value).encode()) <= byte_limit else None


def timestamp(value):
    if type(value) is not int or value < 0:
        return None
    try:
        return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    except (ValueError, OverflowError, OSError):
        return None


def unknown_time():
    return {'value': None, 'timezone': None, 'provenance': None, 'reason': 'not_exposed_by_documented_source'}


def iso_timestamp(value):
    if not isinstance(value, str) or not value.endswith('Z') or len(value) > 30:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    except ValueError:
        return None


def saved_metadata(root, *, offset=0, runner=bounded_output):
    try:
        raw = runner([sys.executable, '-B', str(Path(__file__).with_name('saved_session_metadata.py')), '--offset', str(offset)],
                     env=dict(os.environ, CLAUDE_CONFIG_DIR=str(root)), timeout=20)
        if len(raw) > MAX_BYTES:
            raise CatalogError('source_byte_limit')
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise CatalogError('sdk_worker_invalid_response')
        if value.get('status') == 'unavailable':
            allowed = {'sdk_not_installed', 'sdk_version_not_qualified', 'sdk_metadata_failed'}
            error = value.get('error')
            return {'status': 'unavailable', 'error': error if isinstance(error,str) and error in allowed else 'sdk_worker_failed'}
        if value.get('status') != 'ok' or value.get('sdk_version') != SAVED_VERSION or not isinstance(value.get('rows'), list) or len(value['rows']) > MAX_ROWS or type(value.get('truncated')) is not bool:
            raise CatalogError('sdk_worker_invalid_response')
        next_offset = value.get('next_offset')
        continuation_limited = value.get('continuation_limit_reached', False)
        consumed = value.get('consumed_sdk_rows')
        if type(continuation_limited) is not bool:
            raise CatalogError('sdk_worker_invalid_continuation')
        if continuation_limited:
            if not value['truncated'] or next_offset is not None or type(consumed) is not int or not 0 < consumed <= MAX_ROWS or offset + consumed <= MAX_SAVED_OFFSET:
                raise CatalogError('sdk_worker_invalid_continuation')
        elif value['truncated'] and (type(next_offset) is not int or not offset < next_offset <= min(offset + MAX_ROWS, MAX_SAVED_OFFSET)):
            raise CatalogError('sdk_worker_invalid_continuation')
        elif not value['truncated'] and next_offset is not None:
            raise CatalogError('sdk_worker_invalid_continuation')
        # Rebuild even a worker response, so arbitrary added fields cannot leak.
        return {'status': 'ok', 'sdk_version': SAVED_VERSION, 'rows': value['rows'],
                'truncated': value['truncated'], 'returned_by_sdk': value.get('returned_by_sdk'),
                'rejected_rows': value.get('rejected_rows'),
                'sdk_offset': offset, 'next_offset': next_offset,
                'continuation_limit_reached': continuation_limited,
                'consumed_sdk_rows': value.get('consumed_sdk_rows'),
                'output_byte_limit_reached': value.get('output_byte_limit_reached', False)}
    except (OSError, ValueError, UnicodeError, CatalogError, RecursionError) as error:
        return {'status': 'unavailable', 'error': error.code if isinstance(error, CatalogError) else 'sdk_worker_failed'}


def merge_saved(sessions, rows, target):
    groups = collections.defaultdict(list)
    rejected = 0
    for raw in rows:
        if not isinstance(raw, dict) or not isinstance(raw.get('session_id'), str) or not UUID.fullmatch(raw['session_id']):
            rejected += 1
            continue
        groups[raw['session_id']].append({'title': text_field(raw.get('custom_title'), 512),
            'cwd': text_field(raw.get('cwd'), 2048), 'created': iso_timestamp(raw.get('created_at')),
            'modified': iso_timestamp(raw.get('last_modified'))})
    by_id = {s['session_id']: s for s in sessions}
    for sid, originals in groups.items():
        if sid not in by_id:
            # Same output shape, with no fabricated live/background membership.
            blank = merge_rows([{'sessionId': sid, 'kind': 'background'}], target, {})[0][0]
            blank.update(input_rows=0, metadata_sources=[])
            by_id[sid] = blank
        value = by_id[sid]
        unique = list({packed(r): r for r in originals}.values())
        selected = unique
        # Storage modification can resolve competing file observations, but is
        # never labeled semantic session activity or fed into last_activity_at.
        modifications = [r['modified'] for r in unique]
        if all(modifications):
            latest = max(modifications)
            selected = [r for r in unique if r['modified'] == latest]
        title, cwd = consensus(selected, 'title'), consensus(selected, 'cwd')
        if len({r['title'] for r in selected if r['title'] is not None}) > 1:
            value['warnings'].append('conflicting_saved_titles')
        if title is not None:
            if value['title'] is not None and value['title'] != title:
                value['warnings'].append('saved_title_differs_from_live_name')
            value['title'] = title
            value['provenance']['title'] = SAVED_SOURCE + '.custom_title'
            value['provenance']['title_scope'] = 'user_or_generated_title_in_bounded_windows_latest_rename_not_guaranteed'
        if value['project_workspace'] is None and cwd is not None:
            value['project_workspace'] = cwd
            value['provenance']['project_workspace'] = SAVED_SOURCE + '.cwd'
        created = consensus(unique, 'created')
        if len({r['created'] for r in unique if r['created'] is not None}) > 1:
            value['warnings'].append('conflicting_saved_creation_times')
            value['created_at']['reason'] = 'conflicting_saved_creation_times'
        if created is not None:
            value['created_at'] = {'value': created, 'timezone': 'UTC',
                'provenance': SAVED_SOURCE + '.created_at', 'semantics': 'sdk_first_timestamp_in_bounded_head'}
        modified = consensus(selected, 'modified')
        value['storage_modified_at'] = {'value': modified, 'timezone': 'UTC' if modified else None,
            'provenance': SAVED_SOURCE + '.last_modified' if modified else None,
            'semantics': 'filesystem_mtime_not_semantic_activity'}
        value['metadata_sources'] = sorted(set(value['metadata_sources']) | {'saved_sdk'})
        value['input_rows'] += len(originals)
        if len(originals) > 1:
            value['warnings'].append('duplicate_saved_uuid_rows')
        value['warnings'] = sorted(set(value['warnings']))
    return [by_id[sid] for sid in sorted(by_id)], rejected


def process_evidence(pids, cli, *, runner=bounded_output):
    """Process existence/executable corroboration, not session/socket identity.

A missing PID remains unknown: even a visible helper does not establish an
unrestricted process view. No signal is sent to any inventoried process.
"""
    pids = sorted(set(pids))
    observed = {}
    # At most MAX_ROWS PIDs from the bounded source. One bounded ps call keeps
    # worst-case latency independent of how many rows happen to be live.
    for start in range(0, len(pids), MAX_ROWS):
        group = pids[start:start+MAX_ROWS]
        own = os.getpid()
        try:
            data = runner(['/bin/ps', '-p', ','.join(map(str, [own] + group)), '-o', 'pid=,comm='],
                          timeout=5, maximum=MAX_BYTES).decode('utf-8')
            commands = {}
            for line in data.splitlines():
                parts = line.strip().split(None, 1)
                if len(parts) == 2 and parts[0].isdigit():
                    commands[int(parts[0])] = parts[1]
            if own not in commands:
                raise CatalogError('process_view_unavailable')
            for pid in group:
                observed[pid] = ('matching_executable' if commands.get(pid) == cli else
                                 'different_executable' if pid in commands else 'not_observed')
        except (OSError, UnicodeError, CatalogError):
            observed.update({pid: 'unavailable' for pid in group})
    return observed


def clean_row(row):
    if not isinstance(row, dict):
        return None, 'invalid_row'
    sid = row.get('sessionId')
    if not isinstance(sid, str) or not UUID.fullmatch(sid):
        return None, 'missing_or_invalid_session_uuid'
    if row.get('kind') not in ('interactive', 'background'):
        return None, 'unsupported_kind'
    pid = row.get('pid')
    pid = pid if type(pid) is int and 1 < pid <= 2**31-1 else None
    status = row.get('status') if row.get('status') in ('busy', 'idle', 'waiting') else None
    state = row.get('state') if isinstance(row.get('state'), str) and row['state'] in STATES else None
    value = {'session_id': sid, 'kind': row['kind'], 'pid': pid, 'status': status,
             'state': state, 'name': text_field(row.get('name'), 512),
             'cwd': text_field(row.get('cwd'), 2048), 'started_at': timestamp(row.get('startedAt')),
             'waiting_for': row.get('waitingFor') if isinstance(row.get('waitingFor'), str) and row['waitingFor'] in WAITING else None}
    return value, None


def consensus(rows, field):
    values = {row[field] for row in rows if row[field] is not None}
    return next(iter(values)) if len(values) == 1 else None


def merge_rows(rows, target, processes):
    groups = collections.defaultdict(list)
    pid_sessions = collections.defaultdict(set)
    dropped = collections.Counter()
    for raw in rows:
        row, reason = clean_row(raw)
        if row is None:
            dropped[reason] += 1
        else:
            groups[row['session_id']].append(row)
            if row['pid'] is not None:
                pid_sessions[row['pid']].add(row['session_id'])
    sessions = []
    for sid, originals in sorted(groups.items()):
        unique = {packed(row): row for row in originals}
        group = list(unique.values())
        warnings = []
        if len(originals) > len(group):
            warnings.append('duplicate_metadata_rows')
        conflicting_pids = {r['pid'] for r in group if r['pid'] is not None and len(pid_sessions[r['pid']]) > 1}
        if conflicting_pids:
            warnings.append('pid_claimed_by_multiple_session_ids')
        matching = [r for r in group if r['pid'] is not None and r['pid'] not in conflicting_pids and processes.get(r['pid']) == 'matching_executable']
        matching_pids = {r['pid'] for r in matching}
        stale = [r for r in group if r['pid'] is not None and processes.get(r['pid']) != 'matching_executable']
        if stale:
            warnings.append('unverified_or_stale_process_row')
        # A unique matching live process is stronger metadata than a retained
        # background row. Conflicting matching processes never choose a winner.
        selected = matching if len(matching_pids) == 1 else group
        title, cwd = consensus(selected, 'name'), consensus(selected, 'cwd')
        if len({r['name'] for r in selected if r['name'] is not None}) > 1:
            warnings.append('conflicting_titles')
        if len({r['cwd'] for r in selected if r['cwd'] is not None}) > 1:
            warnings.append('conflicting_workspaces')
        status = 'unknown'
        evidence = 'no_verified_live_process'
        if conflicting_pids:
            evidence = 'ambiguous_pid_session_mapping'
        pid = None
        reported_status = consensus(selected, 'status')
        waiting = consensus(selected, 'waiting_for')
        if len(matching_pids) > 1:
            warnings.append('multiple_matching_live_processes')
            evidence = 'ambiguous_live_processes'
            title, cwd = consensus(matching, 'name'), consensus(matching, 'cwd')
        elif len(matching_pids) == 1:
            pid = next(iter(matching_pids))
            evidence = 'pid_and_executable_corroborated_not_peer_identity'
            statuses = {r['status'] for r in matching if r['status'] is not None}
            if len(statuses) > 1:
                warnings.append('conflicting_live_status')
            else:
                status = reported_status if reported_status in ('busy', 'idle') else 'running'
        elif not any(r['pid'] is not None for r in group) and {r['state'] for r in group} == {'stopped'}:
            status, evidence = 'stopped', 'explicit_cli_stopped_state_no_pid'
        if any(r['state'] in ('done', 'failed') for r in group):
            warnings.append('task_state_is_not_process_liveness')
        started = consensus(selected, 'started_at')
        source_classes = sorted({'live_report' if r['pid'] is not None else 'retained_background' if r['kind'] == 'background' else 'unverified_interactive' for r in group})
        sessions.append({'session_id': sid, 'title': title, 'project_workspace': cwd,
                         'live_status': status, 'reported_status': reported_status, 'waiting_for': waiting,
                         'reported_task_states': sorted({r['state'] for r in group if r['state'] is not None}),
                         'created_at': unknown_time(), 'last_activity_at': unknown_time(),
                         'process_started_at': {'value': started, 'timezone': 'UTC' if started else None,
                                                'provenance': SOURCE + '.startedAt' if started else None,
                                                'semantics': 'reported_start_time_not_creation_or_activity'},
                         'provenance': {'title': SOURCE + '.name' if title else None,
                                        'project_workspace': SOURCE + '.cwd' if cwd else None,
                                        'title_scope': 'current_source_report_not_independent_saved_rename_verification',
                                        'selection': 'unique_matching_process' if len(matching_pids) == 1 else 'consensus'},
                         'live_evidence': {'pid': pid, 'basis': evidence, 'peer_identity_verified': False},
                         'metadata_sources': source_classes, 'input_rows': len(originals),
                         'warnings': sorted(warnings), 'read_send_allowed': sid == target})
    return sessions, dict(sorted(dropped.items()))


def decode_cursor(cursor):
    if cursor is None:
        return None
    if not isinstance(cursor, str) or not 1 <= len(cursor) <= 1024:
        raise CatalogError('invalid_catalog_cursor')
    try:
        value = json.loads(base64.b64decode(cursor.encode('ascii'), altchars=b'-_', validate=True))
        assert isinstance(value, dict) and set(value) == {'v', 'scope', 'snapshot', 'offset'}
        assert type(value['v']) is int and value['v'] == 1
        assert type(value['offset']) is int and 0 < value['offset'] <= MAX_ROWS * 2
        assert all(isinstance(value[k], str) and re.fullmatch('[0-9a-f]{64}', value[k]) for k in ('scope', 'snapshot'))
        return value
    except (ValueError, UnicodeError, AssertionError, TypeError, RecursionError):
        raise CatalogError('invalid_catalog_cursor') from None


def enumerate_sessions(cli, root, target, *, limit=50, cursor=None, saved_offset=0, runner=bounded_output, probe=None, saved_provider=None):
    if type(limit) is not int or not 1 <= limit <= MAX_PAGE:
        raise CatalogError('invalid_catalog_limit')
    if type(saved_offset) is not int or not 0 <= saved_offset <= MAX_SAVED_OFFSET:
        raise CatalogError('invalid_saved_offset')
    token = decode_cursor(cursor)
    scope = fingerprint({'cli': str(cli), 'root': str(root), 'target': target, 'source': SOURCE, 'saved_version': SAVED_VERSION, 'saved_offset': saved_offset})
    if token and token['scope'] != scope:
        raise CatalogError('catalog_cursor_scope_mismatch')
    observed_at = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    coverage = {'source': SOURCE, 'documentation': SOURCE_DOC, 'source_status': 'ok',
                'source_response_complete': False, 'source_has_pagination': False,
                'scope': 'live_and_retained_background_rows_in_configured_local_claude_root',
                'all_saved_sessions_complete': False,
                'saved_interactive_history': 'see_saved_source',
                'creation_and_last_activity': 'not_exposed',
                'missing_row_is_not_stopped': True, 'transcript_payloads_read': False,
                'transcript_text_returned_or_persisted': False, 'ad_hoc_transcript_scan': False,
                'max_source_bytes': MAX_BYTES, 'max_source_rows': MAX_ROWS}
    try:
        env = dict(os.environ, CLAUDE_CONFIG_DIR=str(root))
        raw = runner([str(cli), 'agents', '--json', '--all'], env=env)
        if len(raw) > MAX_BYTES:
            raise CatalogError('source_byte_limit')
        try:
            rows = json.loads(raw)
        except (ValueError, UnicodeError, RecursionError):
            raise CatalogError('source_invalid_json') from None
        if not isinstance(rows, list):
            raise CatalogError('source_not_array')
        if len(rows) > MAX_ROWS:
            raise CatalogError('source_row_limit')
    except (CatalogError, OSError) as error:
        coverage.update(source_status='unavailable', error=error.code if isinstance(error, CatalogError) else 'source_execution_failed')
        rows = []
    normalized = [clean_row(row)[0] for row in rows]
    pids = [r['pid'] for r in normalized if r is not None and r['pid'] is not None]
    processes = (probe(pids) if probe else process_evidence(pids, str(cli), runner=runner))
    sessions, dropped = merge_rows(rows, target, processes)
    coverage.update(source_response_complete=coverage['source_status']=='ok', source_rows=len(rows) if coverage['source_status']=='ok' else None, rejected_rows=dropped,
                    stable_id_rows_complete=not bool(dropped), process_verification='pid_and_executable_only')
    saved = saved_provider() if saved_provider else saved_metadata(root, offset=saved_offset, runner=runner)
    saved_ok = saved.get('status') == 'ok'
    saved_rows = saved.get('rows', []) if saved_ok else []
    sessions, saved_rejected = merge_saved(sessions, saved_rows, target)
    coverage['saved_source'] = {k: v for k, v in saved.items() if k in ('status', 'error', 'sdk_version', 'truncated', 'returned_by_sdk', 'rejected_rows', 'output_byte_limit_reached', 'next_offset', 'consumed_sdk_rows', 'continuation_limit_reached')}
    coverage['saved_source'].update(source=SAVED_SOURCE, documentation=SAVED_DOC,
        qualified_version=SAVED_VERSION, returned_metadata_rows=len(saved_rows), adapter_rejected_rows=saved_rejected,
        sdk_offset=saved_offset, prior_saved_rows_omitted=saved_offset > 0,
        maximum_saved_offset=MAX_SAVED_OFFSET,
        cross_window_uuid_deduplication_required=True,
        continuation='exhaust_catalog_pages_then_restart_with_saved_offset_equal_to_next_offset; no_cursor_on_new_window',
        per_file_reads='at_most_65536_head_bytes_plus_65536_tail_bytes',
        total_scan='SDK_scans_candidates_before_limit; worker_deadline_20_seconds',
        exclusions='SDK may omit unreadable, empty, sidechain or unparseable/summary-less files',
        titles='bounded_window_title_not_guaranteed_latest_rename',
        snapshot_or_total_provided_by_sdk=False)
    coverage['transcript_payloads_read'] = True if saved_ok else False if saved.get('error') in ('sdk_not_installed', 'sdk_version_not_qualified') else None
    coverage['creation_and_last_activity'] = 'SDK_creation_if_available; semantic_last_activity_not_exposed'
    coverage['combined_status'] = 'ok_with_coverage_limits' if saved_ok and coverage['source_status']=='ok' else 'partial' if saved_ok or coverage['source_status']=='ok' else 'unavailable'
    if coverage['combined_status'] == 'unavailable':
        if token:
            raise CatalogError('catalog_source_unavailable')
        return {'observed_at': observed_at, 'sessions': [], 'coverage': coverage,
                'pagination': {'limit': limit, 'next_cursor': None, 'total_sessions': None, 'returned': 0},
                'target': target, 'target_present': None, 'read_send_scope_unchanged': True}
    snapshot = fingerprint({'sessions': sessions, 'coverage': coverage})
    offset = token['offset'] if token else 0
    if token and (token['snapshot'] != snapshot or offset >= len(sessions)):
        raise CatalogError('catalog_changed_restart_pagination')
    page = sessions[offset:offset+limit]
    end = offset + len(page)
    next_cursor = (base64.urlsafe_b64encode(packed({'v': 1, 'scope': scope, 'snapshot': snapshot, 'offset': end}).encode()).decode()
                   if end < len(sessions) else None)
    return {'observed_at': observed_at, 'sessions': page, 'coverage': coverage,
            'pagination': {'limit': limit, 'next_cursor': next_cursor, 'total_sessions': len(sessions),
                           'returned': len(page), 'offset': offset, 'snapshot': snapshot,
                           'consistency': 'revalidate_full_bounded_catalog_each_page'},
            'target': target, 'target_present': any(s['session_id'] == target for s in sessions),
            'read_send_scope_unchanged': True}
