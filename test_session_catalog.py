"""Offline metadata fixtures; never inspect real Claude data or launch Claude."""
import base64
import io
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

import adapter
import client
import session_catalog as cat
import saved_session_metadata as saved

TARGET = '11111111-1111-4111-8111-111111111111'
OTHER = '22222222-2222-4222-8222-222222222222'
CLI = '/fixture/claude'
ROOT = '/fixture/metadata-root'


def row(sid=TARGET, **extra):
    return {'sessionId': sid, 'kind': 'interactive', 'pid': 123,
            'status': 'idle', 'name': 'Current title', 'cwd': '/fixture/project',
            'startedAt': 1792712416653, **extra}


def listing(rows, **options):
    options.setdefault('saved_provider',lambda: {'status':'unavailable','error':'sdk_not_installed'})
    return cat.enumerate_sessions(CLI, ROOT, TARGET,
                                  runner=lambda *a, **kw: json.dumps(rows).encode(),
                                  probe=lambda pids: {pid: 'matching_executable' for pid in pids}, **options)


class CatalogTests(unittest.TestCase):
    def test_live_required_fields_and_timestamp_provenance(self):
        value = listing([row()])['sessions'][0]
        self.assertEqual(TARGET, value['session_id'])
        self.assertEqual('Current title', value['title'])
        self.assertEqual('/fixture/project', value['project_workspace'])
        self.assertEqual('idle', value['live_status'])
        self.assertEqual('2026-10-22T23:40:16.653Z', value['process_started_at']['value'])
        self.assertEqual('UTC', value['process_started_at']['timezone'])
        self.assertIn('startedAt', value['process_started_at']['provenance'])
        for field in ('created_at', 'last_activity_at'):
            self.assertIsNone(value[field]['value'])
            self.assertEqual('not_exposed_by_documented_source', value[field]['reason'])
        self.assertFalse(value['live_evidence']['peer_identity_verified'])

    def test_saved_background_after_reboot_and_explicit_stop(self):
        result = listing([row(kind='background', pid=None, status=None, state='stopped')])
        self.assertEqual('stopped', result['sessions'][0]['live_status'])
        self.assertEqual(['retained_background'], result['sessions'][0]['metadata_sources'])
        self.assertFalse(result['coverage']['all_saved_sessions_complete'])
        self.assertEqual('sdk_not_installed', result['coverage']['saved_source']['error'])

    def test_completed_saved_background_is_not_assumed_stopped(self):
        for state in ('done', 'failed', 'blocked', 'working', None):
            with self.subTest(state=state):
                result = listing([row(kind='background', pid=None, status=None, state=state)])
                self.assertEqual('unknown', result['sessions'][0]['live_status'])

    def test_absence_and_empty_response_are_not_proof_of_stop(self):
        result = listing([])
        self.assertEqual([], result['sessions'])
        self.assertFalse(result['target_present'])
        self.assertTrue(result['coverage']['source_response_complete'])
        self.assertFalse(result['coverage']['all_saved_sessions_complete'])
        self.assertTrue(result['coverage']['missing_row_is_not_stopped'])

    def test_metadata_merge_by_uuid_not_title_or_workspace(self):
        result = listing([row(), row(OTHER, pid=456), row(kind='background', pid=None, status=None, state='done')])
        self.assertEqual(2, len(result['sessions']))
        self.assertEqual(2, result['sessions'][0]['input_rows'])
        self.assertTrue(result['sessions'][0]['read_send_allowed'])
        self.assertFalse(result['sessions'][1]['read_send_allowed'])

    def test_identical_duplicates_are_visible_and_do_not_duplicate_result(self):
        result = listing([row(), row()])
        self.assertEqual(1, len(result['sessions']))
        self.assertIn('duplicate_metadata_rows', result['sessions'][0]['warnings'])

    def test_stale_pid_is_unknown_and_does_not_hide_retained_row(self):
        for evidence in ('not_observed', 'different_executable', 'unavailable'):
            with self.subTest(evidence=evidence):
                rows = [row(), row(kind='background', pid=None, status=None, state='done')]
                result = cat.enumerate_sessions(CLI, ROOT, TARGET,
                    runner=lambda *a, **kw: json.dumps(rows).encode(), probe=lambda _: {123: evidence})
                value = result['sessions'][0]
                self.assertEqual('unknown', value['live_status'])
                self.assertIn('unverified_or_stale_process_row', value['warnings'])
                self.assertIn('retained_background', value['metadata_sources'])
                self.assertIsNone(value['live_evidence']['pid'])

    def test_multiple_matching_pids_are_ambiguous(self):
        value = listing([row(), row(pid=456, status='busy')])['sessions'][0]
        self.assertEqual('unknown', value['live_status'])
        self.assertIn('multiple_matching_live_processes', value['warnings'])

    def test_conflicting_same_pid_status_is_unknown(self):
        value = listing([row(), row(status='busy')])['sessions'][0]
        self.assertEqual('unknown', value['live_status'])
        self.assertIn('conflicting_live_status', value['warnings'])

    def test_waiting_is_live_running_with_bounded_reason(self):
        value = listing([row(status='waiting', waitingFor='permission prompt')])['sessions'][0]
        self.assertEqual('running', value['live_status'])
        self.assertEqual('permission prompt', value['waiting_for'])
        value = listing([row(status='waiting', waitingFor='SECRET PROMPT BODY')])['sessions'][0]
        self.assertIsNone(value['waiting_for'])

    def test_missing_or_invalid_timestamps_stay_unknown(self):
        for value in (None, True, -1, 1e20, 10**40, '2026-10-23', {}):
            with self.subTest(value=value):
                item = listing([row(startedAt=value, createdAt=1234, lastActivity=5678)])['sessions'][0]
                self.assertIsNone(item['process_started_at']['value'])
                self.assertIsNone(item['created_at']['value'])
                self.assertIsNone(item['last_activity_at']['value'])

    def test_renamed_title_appears_on_next_listing(self):
        old = listing([row(name='Before')])
        new = listing([row(name='After')])
        self.assertEqual('After', new['sessions'][0]['title'])
        self.assertNotEqual(old['pagination']['snapshot'], new['pagination']['snapshot'])

    def test_unique_live_title_preferred_over_retained_background_name(self):
        value = listing([row(name='Current renamed title'), row(kind='background', pid=None, status=None, name='Old name')])['sessions'][0]
        self.assertEqual('Current renamed title', value['title'])
        self.assertEqual('unique_matching_process', value['provenance']['selection'])

    def test_conflicting_titles_and_workspaces_are_not_chosen_arbitrarily(self):
        value = listing([row(name='A'), row(name='B', cwd='/other')])['sessions'][0]
        self.assertIsNone(value['title'])
        self.assertIsNone(value['project_workspace'])
        self.assertIn('conflicting_titles', value['warnings'])
        self.assertIn('conflicting_workspaces', value['warnings'])

    def test_missing_uuid_not_replaced_with_background_short_id(self):
        result = listing([row(sessionId=None, kind='background', id='abc12345'), row(sessionId='../secret'), None, row(kind='unsupported')])
        self.assertEqual([], result['sessions'])
        self.assertEqual(2, result['coverage']['rejected_rows']['missing_or_invalid_session_uuid'])
        self.assertFalse(result['coverage']['stable_id_rows_complete'])

    def test_unexpected_field_types_do_not_escape_parser(self):
        result = listing([row(state={}, status={}, waitingFor={}, cwd=[], name={})])
        self.assertIsNone(result['sessions'][0]['title'])
        self.assertIsNone(result['sessions'][0]['project_workspace'])
        self.assertEqual('running', result['sessions'][0]['live_status'])

    def test_prompt_payload_fields_and_undocumented_timestamps_are_discarded(self):
        result = listing([row(firstPrompt='PROMPT_SECRET', summary='SUMMARY_SECRET', message={'content':'BODY_SECRET'}, tools=['TOOL_SECRET'], lastActivity=123, createdAt=123)])
        output = json.dumps(result)
        for marker in ('PROMPT_SECRET','SUMMARY_SECRET','BODY_SECRET','TOOL_SECRET'):
            self.assertNotIn(marker, output)
        self.assertFalse(result['coverage']['transcript_payloads_read'])

    def test_no_files_or_sdk_opened_during_enumeration(self):
        with mock.patch('builtins.open', side_effect=AssertionError('file open forbidden')), \
             mock.patch.object(Path, 'open', side_effect=AssertionError('file open forbidden')):
            self.assertEqual(1, len(listing([row()])['sessions']))

    def test_pagination_merges_before_paging_and_survives_fresh_calls(self):
        rows = [row(OTHER, pid=456), row(), row()]
        first = listing(rows, limit=1)
        second = listing(list(reversed(rows)), limit=1, cursor=first['pagination']['next_cursor'])
        self.assertEqual([TARGET, OTHER], [first['sessions'][0]['session_id'], second['sessions'][0]['session_id']])
        self.assertIsNone(second['pagination']['next_cursor'])
        self.assertEqual(2, second['pagination']['total_sessions'])

    def test_changed_catalog_or_rename_requires_paging_restart(self):
        rows = [row(), row(OTHER, pid=456)]
        token = listing(rows, limit=1)['pagination']['next_cursor']
        for changed in ([row(name='Renamed'), rows[1]], [row(status='busy'), rows[1]], [rows[0]]):
            with self.assertRaisesRegex(cat.CatalogError, 'catalog_changed_restart_pagination'):
                listing(changed, limit=1, cursor=token)

    def test_scope_mismatch_rejected_before_source_call(self):
        token = listing([row(), row(OTHER)], limit=1)['pagination']['next_cursor']
        with self.assertRaisesRegex(cat.CatalogError, 'catalog_cursor_scope_mismatch'):
            cat.enumerate_sessions(CLI, '/different/root', TARGET, cursor=token,
                runner=lambda *a, **kw: self.fail('source should not run'))

    def test_invalid_limits_and_cursors_rejected_before_execution(self):
        for limit in (0, 51, True, '2', None):
            with self.assertRaises(cat.CatalogError):
                cat.enumerate_sessions(CLI, ROOT, TARGET, limit=limit)
        for cursor in ('', '!', {}, True, 'x'*1025, base64.urlsafe_b64encode(b'[]').decode()):
            with self.assertRaises(cat.CatalogError):
                cat.enumerate_sessions(CLI, ROOT, TARGET, cursor=cursor)

    def test_source_failure_does_not_claim_empty_complete_history(self):
        def unavailable(*a, **kw): raise cat.CatalogError('source_timeout')
        result = cat.enumerate_sessions(CLI, ROOT, TARGET, runner=unavailable)
        self.assertIsNone(result['target_present'])
        self.assertIsNone(result['pagination']['total_sessions'])
        self.assertEqual('unavailable', result['coverage']['source_status'])
        self.assertFalse(result['coverage']['source_response_complete'])

    def test_malformed_or_oversized_source_has_explicit_limits(self):
        for raw, error in ((b'{}','source_not_array'),(b'{','source_invalid_json'),
                           (b' '* (cat.MAX_BYTES+1),'source_byte_limit'),
                           (json.dumps([{}]*(cat.MAX_ROWS+1)).encode(),'source_row_limit')):
            with self.subTest(error=error):
                result = cat.enumerate_sessions(CLI,ROOT,TARGET,runner=lambda *a,**kw: raw)
                self.assertEqual(error, result['coverage']['error'])

    def test_production_command_is_documented_all_scope_with_root_binding(self):
        runner = mock.Mock(return_value=b'[]')
        cat.enumerate_sessions(CLI, ROOT, TARGET, runner=runner)
        self.assertEqual([CLI,'agents','--json','--all'],runner.call_args_list[0].args[0])
        self.assertEqual(ROOT,runner.call_args_list[0].kwargs['env']['CLAUDE_CONFIG_DIR'])
        self.assertEqual(ROOT,runner.call_args_list[1].kwargs['env']['CLAUDE_CONFIG_DIR'])
        self.assertTrue(runner.call_args_list[1].args[0][2].endswith('saved_session_metadata.py'))

    def test_process_inspection_has_no_signal_and_absence_stays_unverified(self):
        runner = mock.Mock(return_value=f'{os.getpid()} /fixture/python\n123 {CLI}\n456 /other/program\n'.encode())
        result = cat.process_evidence([123,456,789],CLI,runner=runner)
        self.assertEqual({123:'matching_executable',456:'different_executable',789:'not_observed'},result)
        self.assertEqual('/bin/ps',runner.call_args.args[0][0])
        runner.return_value=b'123 /fixture/claude\n'
        self.assertEqual({123:'unavailable'},cat.process_evidence([123],CLI,runner=runner))

    def test_large_process_catalog_uses_one_bounded_probe(self):
        runner = mock.Mock(return_value=f'{os.getpid()} /fixture/python\n'.encode())
        cat.process_evidence(range(10000,10000+cat.MAX_ROWS),CLI,runner=runner)
        self.assertEqual(1,runner.call_count)
        self.assertEqual(5,runner.call_args.kwargs['timeout'])

    def test_page_envelope_is_below_client_limit_even_with_json_escaping(self):
        rows=[row(f'{i:08x}-1111-4111-8111-111111111111',pid=1000+i,
                  name='"'*250,cwd='\\'*1000) for i in range(cat.MAX_PAGE)]
        result=listing(rows)
        frame=json.dumps({'result':{'content':[{'type':'text','text':json.dumps(result)}]}}).encode()
        self.assertLess(len(frame),1048576)

    def test_control_or_oversized_title_is_unknown_not_truncated(self):
        for name in ('\x1b[31m','\ud800','x'*513):
            self.assertIsNone(listing([row(name=name)])['sessions'][0]['title'])

    def test_bounded_capture_rejects_excess_output_and_deadline(self):
        with self.assertRaisesRegex(cat.CatalogError,'source_byte_limit'):
            cat.bounded_output([sys.executable,'-B','-c','print("x"*2000)'],maximum=100)
        with self.assertRaisesRegex(cat.CatalogError,'source_timeout'):
            cat.bounded_output([sys.executable,'-B','-c','import time; time.sleep(3)'],timeout=0.1)


class AdapterCatalogTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='catalog-fixture-')
        self.app=adapter.Adapter(self.tmp.name,sys.executable,TARGET)
    def tearDown(self):
        self.app.close();self.tmp.cleanup()

    def test_tool_dispatch_does_not_verify_target_or_read_transcripts(self):
        with mock.patch('session_catalog.enumerate_sessions',return_value={'fixture':True}) as enum, \
             mock.patch.object(self.app,'verify_target',side_effect=AssertionError('no live target needed')), \
             mock.patch.object(self.app,'read_session',side_effect=AssertionError('no transcripts')):
            self.assertEqual({'fixture':True},self.app.call('enumerate_claude_sessions',{'limit':2,'cursor':'opaque'}))
            self.assertEqual({'limit':2,'cursor':'opaque','saved_offset':0},enum.call_args.kwargs)
        self.assertIsNone(self.app.pinned)

    def test_enumeration_does_not_authorize_other_session(self):
        with mock.patch('session_catalog.enumerate_sessions',return_value=listing([row(OTHER)])):
            result=self.app.call('enumerate_claude_sessions',{})
        self.assertFalse(result['sessions'][0]['read_send_allowed'])
        for name,args in (('read_claude_session',{'session_id':OTHER}),
                          ('send_claude_message',{'session_id':OTHER,'text':'not sent'}),
                          ('wait_claude_session',{'session_id':OTHER,'timeout':0})):
            with self.assertRaises(adapter.AdapterError) as caught:self.app.call(name,args)
            self.assertEqual('session_not_allowed',caught.exception.code)

    def test_catalog_failure_is_a_named_tool_error(self):
        with mock.patch('session_catalog.enumerate_sessions',side_effect=cat.CatalogError('invalid_catalog_limit')):
            with self.assertRaises(adapter.AdapterError) as caught:self.app.call('enumerate_claude_sessions',{'limit':True})
            self.assertEqual('invalid_catalog_limit',caught.exception.code)

    def test_new_tool_schema_and_legacy_schema_remain_distinct(self):
        tools={t['name']:t for t in adapter.tool_specs()}
        self.assertEqual({},tools['list_claude_sessions']['inputSchema']['properties'])
        self.assertEqual({'limit','cursor','saved_offset'},set(tools['enumerate_claude_sessions']['inputSchema']['properties']))
        self.assertTrue(tools['enumerate_claude_sessions']['annotations']['readOnlyHint'])
        self.assertFalse(tools['enumerate_claude_sessions']['annotations']['openWorldHint'])
        with self.assertRaises(adapter.AdapterError):self.app.call('enumerate_claude_sessions',{'session_id':OTHER})

    def test_cli_sessions_selects_only_metadata_tool(self):
        with mock.patch.object(sys,'argv',['client.py','--cli',sys.executable,'--target',TARGET,'sessions','--limit','2','--cursor','opaque']), \
             mock.patch.object(client,'Client') as klass, mock.patch.object(client,'emit'):
            client.main()
        klass.return_value.call.assert_called_once_with('enumerate_claude_sessions',{'limit':2,'cursor':'opaque','saved_offset':0})
        klass.return_value.close.assert_called_once()




def saved_row(sid=TARGET, **extra):
    return {'session_id':sid,'custom_title':'Saved title','cwd':'/fixture/saved-project',
            'created_at':'2026-10-20T01:02:03.004Z','last_modified':'2026-10-23T00:00:00.000Z',**extra}


def saved_source(rows, **extra):
    return {'status':'ok','sdk_version':cat.SAVED_VERSION,'rows':rows,'truncated':False,
            'returned_by_sdk':len(rows),'rejected_rows':0,'next_offset':None,'consumed_sdk_rows':len(rows),**extra}


class SavedCatalogTests(unittest.TestCase):
    def test_saved_interactive_sessions_after_reboot_without_live_registry(self):
        result=listing([],saved_provider=lambda:saved_source([saved_row()]))
        self.assertEqual(1,len(result['sessions']))
        value=result['sessions'][0]
        self.assertEqual('Saved title',value['title'])
        self.assertEqual('unknown',value['live_status'])
        self.assertEqual(['saved_sdk'],value['metadata_sources'])
        self.assertEqual('2026-10-20T01:02:03.004Z',value['created_at']['value'])
        self.assertEqual('UTC',value['created_at']['timezone'])
        self.assertIn('first_timestamp',value['created_at']['semantics'])
        self.assertIsNone(value['last_activity_at']['value'])
        self.assertEqual('filesystem_mtime_not_semantic_activity',value['storage_modified_at']['semantics'])
        self.assertIsNone(value['process_started_at']['value'])
        self.assertTrue(result['coverage']['transcript_payloads_read'])
        self.assertFalse(result['coverage']['transcript_text_returned_or_persisted'])

    def test_saved_rows_survive_live_source_failure(self):
        def failed(*a,**kw):raise cat.CatalogError('source_timeout')
        result=cat.enumerate_sessions(CLI,ROOT,TARGET,runner=failed,saved_provider=lambda:saved_source([saved_row()]))
        self.assertEqual('partial',result['coverage']['combined_status'])
        self.assertEqual('unknown',result['sessions'][0]['live_status'])
        self.assertEqual('source_timeout',result['coverage']['error'])

    def test_saved_custom_title_beats_generic_live_name_and_rename_invalidates_cursor(self):
        first=listing([row(name='Generic name')],limit=1,saved_provider=lambda:saved_source([saved_row(),saved_row(OTHER)]))
        value=first['sessions'][0]
        self.assertEqual('Saved title',value['title'])
        self.assertIn('custom_title',value['provenance']['title'])
        self.assertEqual('/fixture/project',value['project_workspace'])
        self.assertIn('saved_title_differs_from_live_name',value['warnings'])
        renamed=lambda:saved_source([saved_row(custom_title='Renamed'),saved_row(OTHER)])
        with self.assertRaisesRegex(cat.CatalogError,'catalog_changed_restart_pagination'):
            listing([row()],limit=1,cursor=first['pagination']['next_cursor'],saved_provider=renamed)
        self.assertEqual('Renamed',listing([],saved_provider=renamed)['sessions'][0]['title'])

    def test_missing_saved_title_does_not_use_summary_or_prompt(self):
        result=listing([],saved_provider=lambda:saved_source([saved_row(custom_title=None,summary='PRIVATE',first_prompt='PRIVATE')]))
        self.assertIsNone(result['sessions'][0]['title'])
        self.assertNotIn('PRIVATE',json.dumps(result))

    def test_duplicate_saved_uuid_uses_latest_storage_metadata_with_limits(self):
        rows=[saved_row(custom_title='Old',last_modified='2026-10-22T00:00:00.000Z'),saved_row(custom_title='New')]
        value=listing([],saved_provider=lambda:saved_source(rows))['sessions'][0]
        self.assertEqual('New',value['title'])
        self.assertIn('duplicate_saved_uuid_rows',value['warnings'])
        self.assertIsNone(value['last_activity_at']['value'])
        conflict=[saved_row(custom_title='A'),saved_row(custom_title='B',created_at='2026-10-21T00:00:00.000Z')]
        value=listing([],saved_provider=lambda:saved_source(conflict))['sessions'][0]
        self.assertIsNone(value['title'])
        self.assertIsNone(value['created_at']['value'])
        self.assertIn('conflicting_saved_titles',value['warnings'])
        self.assertEqual('conflicting_saved_creation_times',value['created_at']['reason'])

    def test_missing_saved_timestamps_stay_null(self):
        value=listing([],saved_provider=lambda:saved_source([saved_row(created_at=None,last_modified=None)]))['sessions'][0]
        self.assertIsNone(value['created_at']['value'])
        self.assertIsNone(value['storage_modified_at']['value'])
        self.assertIsNone(value['last_activity_at']['value'])

    def test_saved_and_live_merge_before_stable_uuid_pagination(self):
        provider=lambda:saved_source([saved_row(),saved_row(OTHER)])
        first=listing([row()],saved_provider=provider,limit=1)
        second=listing([row()],saved_provider=provider,limit=1,cursor=first['pagination']['next_cursor'])
        self.assertEqual(2,second['pagination']['total_sessions'])
        self.assertEqual(OTHER,second['sessions'][0]['session_id'])
        self.assertFalse(second['sessions'][0]['read_send_allowed'])
        self.assertIsNone(second['pagination']['next_cursor'])

    def test_saved_truncation_and_failure_preserve_explicit_coverage(self):
        result=listing([],saved_provider=lambda:saved_source([saved_row()],truncated=True))
        self.assertTrue(result['coverage']['saved_source']['truncated'])
        self.assertFalse(result['coverage']['all_saved_sessions_complete'])
        self.assertFalse(result['coverage']['saved_source']['snapshot_or_total_provided_by_sdk'])
        result=listing([row()],saved_provider=lambda:{'status':'unavailable','error':'source_timeout'})
        self.assertEqual('partial',result['coverage']['combined_status'])
        self.assertIsNone(result['coverage']['transcript_payloads_read'])
        self.assertEqual(1,len(result['sessions']))

    def test_saved_worker_command_is_same_python_with_root_bound_environment(self):
        runner=mock.Mock(return_value=json.dumps(saved_source([saved_row()])).encode())
        value=cat.saved_metadata(ROOT,runner=runner)
        self.assertEqual('ok',value['status'])
        self.assertEqual(sys.executable,runner.call_args.args[0][0])
        self.assertEqual(ROOT,runner.call_args.kwargs['env']['CLAUDE_CONFIG_DIR'])
        self.assertEqual(20,runner.call_args.kwargs['timeout'])

    def test_sdk_missing_or_unqualified_version_is_not_imported(self):
        def missing(_):raise importlib.metadata.PackageNotFoundError()
        loader=mock.Mock(side_effect=AssertionError('must not import'))
        self.assertEqual('sdk_not_installed',saved.read_metadata(version_getter=missing,sdk_loader=loader)['error'])
        self.assertEqual('sdk_version_not_qualified',saved.read_metadata(version_getter=lambda _:'0.0.0',sdk_loader=loader)['error'])
        loader.assert_not_called()

    def test_sdk_api_arguments_and_whitelist_never_access_prompt_attributes(self):
        class Metadata:
            session_id=TARGET;custom_title='Title';cwd='/fixture/project';created_at=1000;last_modified=2000
            @property
            def summary(self):raise AssertionError('summary forbidden')
            @property
            def first_prompt(self):raise AssertionError('prompt forbidden')
        sdk=types.SimpleNamespace(list_sessions=mock.Mock(return_value=[Metadata()]))
        result=saved.read_metadata(version_getter=lambda _:saved.SDK_VERSION,sdk_loader=lambda:sdk)
        sdk.list_sessions.assert_called_once_with(directory=None,limit=cat.MAX_ROWS+1,offset=0,include_worktrees=False)
        self.assertEqual('ok',result['status'])
        self.assertEqual({'session_id','custom_title','cwd','created_at','last_modified'},set(result['rows'][0]))
        self.assertEqual('1970-01-01T00:00:01.000Z',result['rows'][0]['created_at'])
        self.assertEqual(saved.SDK_VERSION,cat.SAVED_VERSION)

    def test_sdk_failure_does_not_expose_exception_or_transcript_text(self):
        sdk=types.SimpleNamespace(list_sessions=mock.Mock(side_effect=ValueError('SECRET TRANSCRIPT')))
        result=saved.read_metadata(version_getter=lambda _:saved.SDK_VERSION,sdk_loader=lambda:sdk)
        self.assertEqual('sdk_metadata_failed',result['error'])
        self.assertNotIn('SECRET',json.dumps(result))

    def test_sdk_row_and_serialized_byte_caps_are_explicit(self):
        values=[types.SimpleNamespace(session_id=TARGET,custom_title='x'*250,cwd='x'*1500,created_at=None,last_modified=None)]*(cat.MAX_ROWS+1)
        sdk=types.SimpleNamespace(list_sessions=lambda **_:values)
        result=saved.read_metadata(version_getter=lambda _:saved.SDK_VERSION,sdk_loader=lambda:sdk)
        self.assertTrue(result['truncated'])
        self.assertTrue(result['output_byte_limit_reached'])
        self.assertLess(len(json.dumps(result).encode()),cat.MAX_BYTES)

    def test_worker_suppresses_python_and_fd_logs_with_temporary_fake_sdk(self):
        with tempfile.TemporaryDirectory(prefix='catalog-sdk-fixture-') as temp:
            folder=Path(temp);(folder/'claude_agent_sdk').mkdir()
            (folder/'claude_agent_sdk/__init__.py').write_text('''import os,sys
from types import SimpleNamespace
def list_sessions(**kwargs):
    assert kwargs == {"directory":None,"limit":4097,"offset":0,"include_worktrees":False}
    assert os.environ["CLAUDE_CONFIG_DIR"] == "/fixture/root"
    print("PRIVATE_PYTHON_LOG")
    os.write(1,b"PRIVATE_FD_LOG\\n")
    os.write(2,b"PRIVATE_STDERR\\n")
    return [SimpleNamespace(session_id="11111111-1111-4111-8111-111111111111",custom_title="Safe title",cwd="/fixture/project",created_at=1000,last_modified=2000,summary="PRIVATE_SUMMARY",first_prompt="PRIVATE_PROMPT")]
''')
            dist=folder/('claude_agent_sdk-'+saved.SDK_VERSION+'.dist-info');dist.mkdir()
            (dist/'METADATA').write_text('Metadata-Version: 2.1\nName: claude-agent-sdk\nVersion: '+saved.SDK_VERSION+'\n')
            env=dict(os.environ,PYTHONPATH=temp,CLAUDE_CONFIG_DIR='/fixture/root')
            result=subprocess.run([sys.executable,'-B',str(Path(saved.__file__))],env=env,capture_output=True,timeout=5)
            self.assertEqual(0,result.returncode,result.stderr)
            self.assertNotIn(b'PRIVATE',result.stdout+result.stderr)
            self.assertEqual('Safe title',json.loads(result.stdout)['rows'][0]['custom_title'])


class ContinuationAndStaleTests(unittest.TestCase):
    def test_one_pid_claimed_by_different_uuids_never_proves_both_live(self):
        result=listing([row(),row(OTHER)])
        for value in result['sessions']:
            self.assertEqual('unknown',value['live_status'])
            self.assertIsNone(value['live_evidence']['pid'])
            self.assertEqual('ambiguous_pid_session_mapping',value['live_evidence']['basis'])
            self.assertIn('pid_claimed_by_multiple_session_ids',value['warnings'])

    def test_saved_worker_continuation_covers_every_bounded_row_window(self):
        values=[types.SimpleNamespace(session_id=f'{i:08x}-1111-4111-8111-111111111111',custom_title='x'*250,cwd='x'*1500,created_at=1000,last_modified=2000) for i in range(1200)]
        offsets=[]
        def page(**kwargs):
            offsets.append(kwargs['offset'])
            return values[kwargs['offset']:kwargs['offset']+kwargs['limit']]
        sdk=types.SimpleNamespace(list_sessions=page)
        offset=0;seen=[]
        while True:
            response=saved.read_metadata(offset=offset,version_getter=lambda _:saved.SDK_VERSION,sdk_loader=lambda:sdk)
            seen.extend(x['session_id'] for x in response['rows'])
            next_offset=response['next_offset']
            if next_offset is None:break
            self.assertTrue(response['truncated'])
            self.assertGreater(next_offset,offset)
            self.assertEqual(offset+response['consumed_sdk_rows'],next_offset)
            offset=next_offset
        self.assertEqual(1200,len(seen))
        self.assertEqual(1200,len(set(seen)))
        self.assertGreater(len(offsets),1)

    def test_sdk_limit_continuation_and_invalid_rows_count_toward_offset(self):
        good=types.SimpleNamespace(session_id=TARGET,custom_title=None,cwd=None,created_at=None,last_modified=None)
        invalid=types.SimpleNamespace(session_id='invalid')
        sdk=types.SimpleNamespace(list_sessions=lambda **_:[invalid]+[good]*cat.MAX_ROWS)
        response=saved.read_metadata(offset=100,version_getter=lambda _:saved.SDK_VERSION,sdk_loader=lambda:sdk)
        self.assertEqual(100+cat.MAX_ROWS,response['next_offset'])
        self.assertEqual(1,response['rejected_rows'])
        self.assertEqual(cat.MAX_ROWS,response['consumed_sdk_rows'])

    def test_merged_paging_and_sdk_window_continuation_are_explicit_and_distinct(self):
        source=lambda:saved_source([saved_row(),saved_row(OTHER)],truncated=True,next_offset=2,consumed_sdk_rows=2)
        first=listing([],saved_provider=source,limit=1)
        self.assertIsNotNone(first['pagination']['next_cursor'])
        self.assertEqual(2,first['coverage']['saved_source']['next_offset'])
        last=listing([],saved_provider=source,limit=1,cursor=first['pagination']['next_cursor'])
        self.assertIsNone(last['pagination']['next_cursor'])
        self.assertEqual(2,last['coverage']['saved_source']['next_offset'])
        self.assertFalse(last['coverage']['all_saved_sessions_complete'])
        with self.assertRaisesRegex(cat.CatalogError,'scope_mismatch'):
            listing([],saved_provider=source,saved_offset=2,cursor=first['pagination']['next_cursor'])
        following=listing([],saved_provider=lambda:saved_source([]),saved_offset=2)
        self.assertTrue(following['coverage']['saved_source']['prior_saved_rows_omitted'])
        self.assertTrue(following['coverage']['saved_source']['cross_window_uuid_deduplication_required'])

    def test_invalid_saved_offset_rejected_before_source(self):
        for offset in (-1,True,'1',1000001):
            with self.assertRaisesRegex(cat.CatalogError,'invalid_saved_offset'):
                cat.enumerate_sessions(CLI,ROOT,TARGET,saved_offset=offset)
            self.assertEqual('invalid_saved_offset',saved.read_metadata(offset=offset)['error'])

    def test_saved_worker_must_offer_progress_when_truncated(self):
        for offset in (None,0,-1,True,'1',cat.MAX_ROWS+1):
            body=saved_source([],truncated=True,next_offset=offset)
            result=cat.saved_metadata(ROOT,runner=lambda *a,**kw:json.dumps(body).encode())
            self.assertEqual('sdk_worker_invalid_continuation',result['error'])

    def test_continuation_ceiling_is_explicit_and_never_returns_unusable_offset(self):
        good=types.SimpleNamespace(session_id=TARGET,custom_title=None,cwd=None,created_at=None,last_modified=None)
        sdk=types.SimpleNamespace(list_sessions=lambda **_:[good]*(cat.MAX_ROWS+1))
        for offset in (cat.MAX_SAVED_OFFSET-cat.MAX_ROWS+1,cat.MAX_SAVED_OFFSET):
            response=saved.read_metadata(offset=offset,version_getter=lambda _:saved.SDK_VERSION,sdk_loader=lambda:sdk)
            self.assertTrue(response['truncated'])
            self.assertTrue(response['continuation_limit_reached'])
            self.assertIsNone(response['next_offset'])
            parent=cat.saved_metadata(ROOT,offset=offset,runner=lambda *a,**kw:json.dumps(response).encode())
            self.assertEqual('ok',parent['status'])
            result=listing([],saved_offset=offset,saved_provider=lambda:parent)
            self.assertTrue(result['coverage']['saved_source']['continuation_limit_reached'])
            self.assertFalse(result['coverage']['all_saved_sessions_complete'])
        invalid=saved_source([],truncated=True,next_offset=cat.MAX_SAVED_OFFSET+1)
        self.assertEqual('sdk_worker_invalid_continuation',cat.saved_metadata(ROOT,offset=cat.MAX_SAVED_OFFSET,runner=lambda *a,**kw:json.dumps(invalid).encode())['error'])

    def test_exact_continuation_ceiling_remains_usable(self):
        good=types.SimpleNamespace(session_id=TARGET,custom_title=None,cwd=None,created_at=None,last_modified=None)
        sdk=types.SimpleNamespace(list_sessions=lambda **_:[good]*(cat.MAX_ROWS+1))
        offset=cat.MAX_SAVED_OFFSET-cat.MAX_ROWS
        response=saved.read_metadata(offset=offset,version_getter=lambda _:saved.SDK_VERSION,sdk_loader=lambda:sdk)
        self.assertEqual(cat.MAX_SAVED_OFFSET,response['next_offset'])
        self.assertFalse(response['continuation_limit_reached'])
        self.assertEqual('ok',cat.saved_metadata(ROOT,offset=offset,runner=lambda *a,**kw:json.dumps(response).encode())['status'])


if __name__=='__main__':unittest.main()
