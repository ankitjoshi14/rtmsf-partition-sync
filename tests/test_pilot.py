from experiments.aws.run import trial_matrix
from experiments.aws.workloads import partition_values, partition_path, replay_verified


def test_fixture_table_names_keep_experiments_and_reruns_separate():
    import re
    from experiments.aws.workloads import fixture_table_name
    from experiments.figures import RECOVERY_CASES

    trials = trial_matrix([100, 1000, 10000, 100000], [1, 10, 100, 1000, 10000],
                          1000, 5, layout_sizes=[100, 1000, 10000])
    fixtures = [(t['id'], t['method'], t['base'], t['x'] if t['graph'] == 4 else 1,
                 t.get('layout')) for t in trials]
    fixtures += [(f'{case}_r{r}', 'rtmsf', 0, 1, None)
                 for case in ('lifecycle', *RECOVERY_CASES, 'check_short_burst') for r in range(1, 4)]
    runs = []
    for run in ('run_20260929_090000_a1b2', 'run_20260929_100000_c3d4'):
        names = [fixture_table_name(name, method, run, base, files, layout)
                 for name, method, base, files, layout in fixtures]
        assert len(names) == len(set(names))
        assert all(re.fullmatch(r'(rtmsf|crawler|msck)_[a-z0-9_]{1,245}', name) for name in names)
        runs.append(set(names))
    assert runs[0].isdisjoint(runs[1])
    assert 'rtmsf_scale_p100_r1_20260929_090000_a1b2' in runs[0]


def test_graph5_uses_same_logical_data_in_both_orders_and_isolated_trials():
    from types import SimpleNamespace
    from experiments.aws.workloads import Pilot
    from experiments.aws.trial_buckets import bucket_name

    pilot = Pilot.__new__(Pilot)
    pilot.args = SimpleNamespace(layout='hierarchical')
    matrix = trial_matrix([], [], 1000, 2, layout_sizes=[100, 1000, 10000])
    assert len(matrix) == len({t['id'] for t in matrix}) == 24
    assert {t['graph'] for t in matrix} == {5}
    assert {t['method'] for t in matrix} == {'rtmsf', 'athena_msck'}
    names = [bucket_name(t, 'p20260926_120000_abcd') for t in matrix]
    assert len(set(names)) == len(names) and all(len(name) <= 63 for name in names)
    paths = {}
    for layout in ('day_first', 'tenant_first'):
        paths[layout] = [pilot.path(pilot.values(i, layout), layout) for i in range(101)]
        assert len(set(paths[layout])) == 101
    assert [sorted(p.split('/')) for p in paths['day_first']] == [sorted(p.split('/')) for p in paths['tenant_first']]
    assert len({p.split('/')[0] for p in paths['day_first']}) == 1
    assert len({p.split('/')[0] for p in paths['tenant_first']}) == 101


def test_main_layout_rejects_dimension_mismatch():
    import pytest
    from experiments.aws.workloads import Pilot
    pilot = Pilot.__new__(Pilot)
    assert pilot.path(pilot.values(1000)) == partition_path(partition_values(1000))
    with pytest.raises(ValueError):
        pilot.path(('p001000',))


def test_seed_upload_recovers_lost_response_only_after_verifying_bytes(monkeypatch):
    from io import BytesIO
    from types import SimpleNamespace
    import pytest
    from botocore.exceptions import ClientError, ConnectionClosedError
    import experiments.aws.workloads as module

    pilot = module.Pilot.__new__(module.Pilot)
    pilot.changes, records, calls = [], [], []
    pilot.record = lambda event, **data: records.append(dict(event=event, **data))
    monkeypatch.setattr(module, 'parquet_payload', lambda record_id: b'expected parquet')
    monkeypatch.setattr(module.time, 'sleep', lambda seconds: None)
    def put(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise ConnectionClosedError(endpoint_url='https://example.invalid')
        raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
    pilot.clients = {'s3': SimpleNamespace(put_object=put, get_object=lambda **kwargs: {
        'Body': BytesIO(b'expected parquet'), 'ResponseMetadata': {'RequestId': 'verification'}})}
    fixture = dict(name='seed', root='fixture/new/', bucket='owned', objects={})
    pilot.upload(fixture, pilot.values(0), 0, setup=True)
    assert len(fixture['objects']) == 1 and len(calls) == 2
    assert records[-1]['request_id'] is None
    assert records[-1]['verification_request_id'] == 'verification'
    calls.clear()
    with pytest.raises(ConnectionClosedError):
        pilot.upload(fixture, pilot.values(1), 1)
    assert len(calls) == 1, 'measured uploads must not gain hidden retries'
    calls.clear()
    pilot.clients['s3'].get_object = lambda **kwargs: {'Body': BytesIO(b'wrong bytes'), 'ResponseMetadata': {'RequestId': 'mismatch'}}
    with pytest.raises(ValueError, match='different bytes'):
        pilot.upload(fixture, pilot.values(2), 2, setup=True)


def test_processing_requires_exact_file_partition_and_successful_ack(monkeypatch):
    import pytest
    import experiments.aws.workloads as module
    pilot = module.Pilot.__new__(module.Pilot)
    pilot.timeout = 1
    pilot.outputs = {'GlueDatabaseName': 'db'}
    pilot.record = lambda *args, **kwargs: None
    pilot.changes = [dict(fixture='trial', at='2026-01-01T00:00:01Z', key_sha256='new_file', operation='CREATE', partition='dt=1')]
    fixture = {'name': 'trial', 'table': {'Name': 'target'}}
    records = [
        {'action': 'message_decoded', 'timestamp': '2026-01-01T00:00:02Z', 'message_id': 'm', 'events': [{'key_sha256': 'new_file', 'operation': 'CREATE'}]},
        {'action': 'pmcp_result', 'timestamp': '2026-01-01T00:00:03Z', 'table_id': 'db.target', 'partition': 'dt=1', 'status': 'NOOP'},
        {'action': 'message_acknowledged', 'timestamp': '2026-01-01T00:00:04Z', 'message_id': 'm'},
    ]
    clock = [0]
    monkeypatch.setattr(module.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(module.time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    pilot.telemetry = lambda: [{'record': r} for r in records]
    pilot.wait_processed(fixture, '2026-01-01T00:00:00Z')
    records[0]['events'][0]['key_sha256'] = 'old_seed_file'
    with pytest.raises(TimeoutError):
        pilot.wait_processed(fixture, '2026-01-01T00:00:00Z')
    records[0]['events'][0]['key_sha256'] = 'new_file'
    records[1]['partition'] = 'dt=other'
    with pytest.raises(TimeoutError):
        pilot.wait_processed(fixture, '2026-01-01T00:00:00Z')
    records[1]['partition'] = 'dt=1'
    records.append({'action': 'message_dead_lettered', 'timestamp': '2026-01-01T00:00:03Z', 'message_id': 'm'})
    with pytest.raises(AssertionError, match='dead-lettered'):
        pilot.wait_processed(fixture, '2026-01-01T00:00:00Z')


def test_explicit_delete_replay_requires_matching_decoded_event_and_ack(monkeypatch):
    import experiments.aws.workloads as module
    pilot = module.Pilot.__new__(module.Pilot)
    pilot.timeout, pilot.changes = 1, []
    pilot.outputs = {'GlueDatabaseName': 'db'}
    pilot.record = lambda *args, **kwargs: None
    fixture = {'name': 'trial', 'table': {'Name': 'target'}}
    expected = [dict(key_sha256='replayed-file', operation='DELETE', partition='dt=1')]
    records = [
        dict(action='message_decoded', message_id='replay', timestamp='2026-01-01T00:00:02Z',
             events=[dict(key_sha256='replayed-file', operation='DELETE')]),
        dict(action='pmcp_result', table_id='db.target', partition='dt=1', status='NOOP', timestamp='2026-01-01T00:00:03Z'),
        dict(action='message_acknowledged', message_id='replay', timestamp='2026-01-01T00:00:04Z'),
    ]
    clock = [0]
    monkeypatch.setattr(module.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(module.time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    pilot.telemetry = lambda: [{'record': record} for record in records]
    pilot.wait_processed(fixture, '2026-01-01T00:00:00Z', expected_changes=expected)
    records[0]['events'][0]['operation'] = 'CREATE'
    with pytest.raises(TimeoutError):
        pilot.wait_processed(fixture, '2026-01-01T00:00:00Z', expected_changes=expected)
    records[0]['events'][0]['operation'] = 'DELETE'
    records.pop()
    with pytest.raises(TimeoutError):
        pilot.wait_processed(fixture, '2026-01-01T00:00:00Z', expected_changes=expected)
import pytest


def test_layout_rolls_over_hours_sites_and_days_without_aliases():
    values = [partition_values(i) for i in range(10001)]
    assert len(set(values)) == 10001
    assert values[24] == ('2026-01-01', 'site-001', '00')
    assert values[2400] == ('2026-01-02', 'site-000', '00')
    assert partition_path(values[0]) == 'dt=2026-01-01/site=site-000/hour=00/'


def test_matrix_is_fresh_per_condition_and_rotates_methods():
    matrix = trial_matrix([100, 1000], [1, 10], 1000, 2)
    assert len({trial['id'] for trial in matrix}) == len(matrix)
    assert [t['method'] for t in matrix if t['graph'] == 2 and t['x'] == 100][:6] == [
        'rtmsf', 'glue_event_crawler', 'athena_msck',
        'glue_event_crawler', 'athena_msck', 'rtmsf']
    assert all(t['base'] <= 1000 for t in matrix)


def test_recovery_needs_same_message_received_again_and_acknowledged_in_successor():
    marker = {'pending_message_ids': ['original']}
    records = [
        {'log_stream': 'worker/worker/replacement', 'record': {'action': 'message_received', 'message_id': 'original', 'receive_count': '2'}},
        {'log_stream': 'worker/worker/replacement', 'record': {'action': 'message_acknowledged', 'message_id': 'original'}},
    ]
    assert replay_verified(marker, records, 'replacement')
    assert not replay_verified(marker, records[:1], 'replacement')
    assert not replay_verified(marker, records, 'different')
    assert not replay_verified({'pending_message_ids': ['unrelated']}, records, 'replacement')
    records[0]['record']['receive_count'] = None
    assert not replay_verified(marker, records, 'replacement')


@pytest.mark.parametrize('task_id,expected_status', [('arn:task/unrelated', 'incomplete'), ('arn:task/crashed', 'passed')])
@pytest.mark.parametrize('wait_for_successor', [False, True, 'transport_error'])
def test_recovery_fault_matches_launcher_path_and_rejects_marker_for_other_task(monkeypatch, task_id, expected_status, wait_for_successor):
    from datetime import datetime, timezone
    from io import BytesIO
    from types import SimpleNamespace
    import json

    from experiments.aws import workloads
    from experiments.aws.crash_worker import FaultPlan

    current = datetime.now(timezone.utc).timestamp()
    monkeypatch.setattr(workloads.time, 'time', lambda: current)
    monkeypatch.setattr(workloads.time, 'monotonic', lambda: current)
    def pause(seconds):
        if expected_status != 'passed':
            raise TimeoutError('test deadline')
    monkeypatch.setattr(workloads.time, 'sleep', pause)
    tasks = [
        {'taskArn': 'arn:task/crashed', 'createdAt': datetime.fromtimestamp(current + 1, timezone.utc),
         'stoppedAt': datetime.fromtimestamp(current + 2, timezone.utc), 'lastStatus': 'STOPPED',
         'containers': [{'name': 'worker', 'exitCode': 86}]},
        {'taskArn': 'arn:task/successor', 'createdAt': datetime.fromtimestamp(current + 3, timezone.utc),
         'startedAt': datetime.fromtimestamp(current + 4, timezone.utc), 'lastStatus': 'RUNNING', 'containers': []},
    ]
    marker = {
        'operation': 'ADD', 'point': 'before_processing',
        'partition_path': 'dt=2026-01-01/site=site-000/hour=00',
        'table_id': 'db.trial', 'task_id': task_id, 'pending_message_ids': ['original'],
    }
    run = object.__new__(workloads.Pilot)
    run.run_id, run.timeout = 'run_test', 60
    run.recovery_results = []
    run.outputs = {'GlueDatabaseName': 'db', 'ClusterName': 'cluster', 'ServiceName': 'service'}
    reads = []
    def read_marker(**kwargs):
        reads.append(kwargs)
        if len(reads) > 1:
            raise RuntimeError('connection closed on unnecessary marker reread')
        return {'Body': BytesIO(json.dumps(marker).encode())}
    run.clients = {
        's3': SimpleNamespace(get_object=read_marker),
        'ecs': object(),
    }
    fixture = {'name': 'test', 'table': {'Name': 'trial'}, 'bucket': 'data-bucket'}
    configured = []
    run.configure = lambda fault: configured.append(FaultPlan(**fault))
    run.stop_worker = run.start_worker = lambda: None
    run.upload = lambda *args: ('key', '', 0)
    run.remove = run.drain = run.verify = run.wait_presence = lambda *args: None
    run.presence = lambda *args: True
    run.record = lambda *args, **kwargs: None
    run.telemetry = lambda: [
        {'log_stream': 'worker/worker/successor', 'record': {'action': 'message_received', 'message_id': 'original', 'receive_count': '2'}},
        {'log_stream': 'worker/worker/successor', 'record': {'action': 'message_acknowledged', 'message_id': 'original'}},
    ]
    polls = []
    def task_poll(*args):
        polls.append(True)
        if wait_for_successor == 'transport_error' and len(polls) == 1:
            from botocore.exceptions import ConnectionClosedError
            raise ConnectionClosedError(endpoint_url='https://ecs.example.test/')
        return [] if wait_for_successor and len(polls) == 1 else tasks
    monkeypatch.setattr(workloads, 'service_tasks', task_poll)
    run.recovery('add_before_processing', fixture, 1)
    assert configured[0].partition_path == marker['partition_path']
    assert run.recovery_results[0]['crash_verified'] is (expected_status == 'passed')
    assert run.recovery_results[0]['status'] == expected_status
