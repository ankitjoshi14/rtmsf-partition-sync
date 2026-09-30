from datetime import datetime, timezone
import importlib
import importlib.util

import pytest


def checks():
    assert importlib.util.find_spec('experiments.aws.checks') is not None, 'supporting check module is required'
    return importlib.import_module('experiments.aws.checks')


class Clock:
    now = 0

    def sleep(self, seconds):
        self.now += seconds

    def utc(self, value=None):
        return datetime.fromtimestamp(self.now if value is None else value, timezone.utc).isoformat()


class Pilot:
    def __init__(self, clock):
        self.clock = clock
        self.fixtures = {}
        self.outputs = {'GlueDatabaseName': 'db'}
        self.calls, self.records = [], []
        self.deadline = 1000
        self.upload_seconds = 0.01
        self.timeout = 60

    def fixture(self, name, method, base=0, files=1):
        assert base == 0 and files == 1
        value = {'name': name, 'method': method, 'bucket': 'bucket', 'table': {'Name': name}, 'objects': {}}
        self.fixtures[name] = value
        return value

    def upload(self, fixture, values, record_id):
        key = (tuple(values), record_id)
        fixture['objects'][key] = [str(record_id), str(record_id * 10), *values]
        self.calls.append(('upload', fixture['name'], values, record_id, self.clock.now))
        self.clock.sleep(self.upload_seconds)
        return key, self.clock.utc(), self.clock.now

    def record(self, event, **fields):
        self.records.append({'event': event, **fields})

    def presence(self, fixture, values):
        return self.clock.now >= 31

    def wait_presence(self, *args):
        self.clock.now = max(self.clock.now, 31)

    def wait_processed(self, fixture, since):
        self.calls.append(('processed', fixture['name'], since))

    def drain(self, name):
        self.calls.append(('drain', name))

    def verify(self, fixture, name):
        self.calls.append(('verify', fixture['name'], len(fixture['objects'])))

    def telemetry(self):
        result = [{'record': {'action': 'message_received', 'timestamp': self.clock.utc(offset + 0.01),
                              'message_id': 'first' if offset == 0 else str(offset)}} for offset in range(0, 41, 5)]
        for name in self.fixtures:
            result.append({'record': {'action': 'pmcp_result', 'table_id': 'db.' + name,
                                      'timestamp': self.clock.utc(31), 'status': 'APPLIED'}})
        return result


def setup(monkeypatch):
    module = checks()
    clock = Clock()
    monkeypatch.setattr(module.time, 'monotonic', lambda: clock.now)
    monkeypatch.setattr(module.time, 'sleep', clock.sleep)
    monkeypatch.setattr(module, 'utc', clock.utc)
    pilot = Pilot(clock)
    module.prepare_checks(pilot)
    return module, pilot, clock


def test_checks_prepare_empty_tables_and_cover_exact_short_and_two_table_workloads(monkeypatch):
    module, pilot, _ = setup(monkeypatch)
    monkeypatch.setattr(module, '_burst_evidence', lambda *args: {})
    module.short_burst(pilot)
    module.two_tables(pilot)
    assert len(pilot.fixtures['check_short_burst_r1']['objects']) == 20
    for suffix in ('a', 'b'):
        objects = pilot.fixtures['check_two_tables_' + suffix + '_r1']['objects']
        partitions = {key[0] for key in objects}
        assert len(objects) == 15 and len(partitions) == 3
        assert all(sum(key[0] == values for key in objects) == 5 for values in partitions)
    assert len([call for call in pilot.calls if call[0] == 'processed']) == 3


def test_burst_evidence_counts_only_target_events_and_retains_split_windows(monkeypatch):
    import hashlib

    module, pilot, clock = setup(monkeypatch)
    fixture = pilot.fixtures['check_short_burst_r1']
    fixture['objects'] = {f'file-{index}.parquet': [] for index in range(20)}
    records = [{'action': 'message_decoded', 'message_id': str(index),
                'events': [{'key_sha256': hashlib.sha256(key.encode()).hexdigest(), 'operation': 'CREATE'}]}
               for index, key in enumerate(fixture['objects'])]
    records += [{'action': 'pmcp_result', 'table_id': 'db.' + fixture['table']['Name'],
                 'status': status, 'observation_count': count}
                for status, count in [('APPLIED', 15), ('NOOP', 5)]]
    records += [{'action': 'pmcp_result', 'table_id': 'db.unrelated', 'status': 'APPLIED', 'observation_count': 100},
                {'action': 'message_decoded', 'message_id': 'unrelated',
                 'events': [{'key_sha256': 'unrelated', 'operation': 'CREATE'}]}]
    pilot.telemetry = lambda: [{'record': dict(record, timestamp=clock.utc(1))} for record in records]
    result = module._burst_evidence(pilot, fixture, clock.utc())
    assert result['observed_events'] == result['distinct_files'] == 20
    assert result['pmcp_attempts'] == 2
    assert result['applied_catalog_changes'] == 1
    assert result['observations_per_pmcp_attempt'] == [15, 5]
    assert result['coalescing_observed'] is True
    missing = records.pop(20)
    with pytest.raises(TimeoutError, match='terminal PMCP'):
        module._burst_evidence(pilot, fixture, clock.utc())
    records.insert(20, missing)
    records.append(dict(missing, status='RETRY', observation_count=10))
    assert module._burst_evidence(pilot, fixture, clock.utc())['pmcp_attempts'] == 3
    records.pop(0)
    with pytest.raises(TimeoutError, match='all uploaded files'):
        module._burst_evidence(pilot, fixture, clock.utc())


def test_long_burst_retains_real_schedule_and_proves_progress_before_last_upload(monkeypatch):
    module, pilot, _ = setup(monkeypatch)
    result = module.long_burst(pilot)
    uploads = [call for call in pilot.calls if call[0] == 'upload']
    assert [round(call[-1]) for call in uploads] == list(range(0, 41, 5))
    assert result['progress_before_final_upload']
    assert result['first_processing_from_receive_s'] <= 31
    assert result['maximum_actual_upload_gap_s'] < 10
    assert any(item['event'] == 'supporting_check_trace' and item['trace'] for item in pilot.records)


def test_long_burst_cannot_claim_maximum_bound_when_actual_gaps_allow_quiet_flush(monkeypatch):
    module, pilot, _ = setup(monkeypatch)
    pilot.upload_seconds = 11
    with pytest.raises(TimeoutError, match='quiet period'):
        module.long_burst(pilot)
    assert any(item['event'] == 'supporting_check_trace' for item in pilot.records)


def test_expired_budget_retains_each_check_as_not_attempted(monkeypatch):
    module, pilot, clock = setup(monkeypatch)
    pilot.deadline = clock.now
    results = module.run_checks(pilot)
    assert {item['name'] for item in results} >= {'short_burst', 'long_burst', 'two_tables'}
    assert all(item['attempted'] is False and item['status'] == 'incomplete' for item in results)
    assert not pilot.calls


@pytest.mark.parametrize('count', [2, 6])
def test_retry_checks_restore_normal_worker_and_require_actual_injected_failure_count(monkeypatch, count):
    module, pilot, clock = setup(monkeypatch)
    from experiments.aws.crash_worker import FaultPlan

    configured, replayed = [], []
    pilot.stop_worker = pilot.start_worker = lambda: None
    pilot.configure = lambda fault=None: configured.append(FaultPlan(**fault) if fault else None)
    pilot.presence = lambda *args: count == 2 or configured[-1] is None
    base_records = pilot.telemetry
    name = 'retryable_failures' if count == 2 else 'retry_exhaustion'

    def telemetry():
        records = base_records()
        records += [{'record': {'action': 'catalog_failure_injected', 'timestamp': clock.utc(1)}} for _ in range(count)]
        if count == 6:
            records += [{'record': {'action': 'pmcp_result', 'table_id': 'db.check_' + name + '_r1',
                                    'timestamp': clock.utc(1), 'status': 'RETRY'}} for _ in range(6)]
            records += [{'record': {'action': action, 'message_id': 'first', 'timestamp': clock.utc(1)}}
                        for action in ('message_dead_lettered', 'message_acknowledged')]
        return records

    pilot.telemetry = telemetry
    monkeypatch.setattr(module, '_retain_expected_dlq', lambda *args: ['captured native notification'])
    monkeypatch.setattr(module, '_replay', lambda pilot, bodies, label: replayed.extend(bodies))
    result = module.retry_failures(pilot, count)
    assert result['injected_failures'] == count
    assert configured[0].fail_catalog_calls == count
    assert configured[-1] is None
    assert replayed == (['captured native notification'] if count == 6 else [])


def test_expected_dlq_body_is_retained_before_specific_message_is_consumed(monkeypatch):
    import json
    from types import SimpleNamespace

    module, pilot, _ = setup(monkeypatch)
    order = []
    key = 'root/dt=t/a.parquet'
    body = json.dumps({'Records': [{'eventName': 'ObjectCreated:Put', 's3': {'bucket': {'name': 'bucket'}, 'object': {'key': key}}}]})
    pilot.outputs['DeadLetterQueueUrl'] = 'dlq'
    pilot.bundle = SimpleNamespace(write=lambda name, data: order.append(('retained', data)))
    pilot.clients = {'sqs': SimpleNamespace(
        receive_message=lambda **kwargs: {'Messages': [{'MessageId': 'dlq-id', 'ReceiptHandle': 'private-receipt', 'Body': body}]},
        delete_message=lambda **kwargs: order.append(('deleted', kwargs)),
    )}
    result = module._retain_expected_dlq(pilot, pilot.fixtures['check_retry_exhaustion_r1'], key, 1)
    assert result == [body]
    assert [item[0] for item in order] == ['retained', 'deleted']
    assert 'private-receipt' not in json.dumps(order[0])


@pytest.mark.parametrize('settings', [{'repetitions': 2}, {'repetitions': 5, 'correctness_repetitions': 2}])
def test_each_requested_repetition_prepares_fresh_fixtures_and_retains_unattempted_checks(monkeypatch, settings):
    from types import SimpleNamespace

    module, pilot, clock = setup(monkeypatch)
    pilot.fixtures.clear()
    pilot.args = SimpleNamespace(**settings)
    module.prepare_checks(pilot)
    assert 'check_long_burst_r1' in pilot.fixtures and 'check_long_burst_r2' in pilot.fixtures
    assert len(pilot.fixtures) == 2 * len(module.CHECK_FIXTURES)
    pilot.deadline = clock.now
    result = module.run_checks(pilot)
    assert {item['repetition'] for item in result} == {1, 2}
    assert len(result) == 2 * len(module.CHECK_NAMES)


def test_selected_checks_prepare_only_their_tables_and_keep_requested_order(monkeypatch):
    module, pilot, clock = setup(monkeypatch)
    pilot.fixtures.clear()
    names = ['rapid_changes', 'duplicate_old_events']
    module.prepare_checks(pilot, names=names)
    assert set(pilot.fixtures) == {'check_rapid_changes_r1', 'check_duplicate_old_events_r1'}
    pilot.deadline = clock.now
    assert [result['name'] for result in module.run_checks(pilot, names=names)] == names
    with pytest.raises(ValueError):
        module.prepare_checks(pilot, names=['unknown'])
    with pytest.raises(ValueError):
        module.run_checks(pilot, names=['rapid_changes', 'rapid_changes'])


@pytest.mark.parametrize('release_failure', [False, True])
def test_native_capture_requires_stopped_worker_retains_only_match_and_releases_every_lease(monkeypatch, release_failure):
    import json
    from types import SimpleNamespace

    module, pilot, _ = setup(monkeypatch)
    fixture = pilot.fixtures['check_duplicate_old_events_r1']
    key = 'root/dt=t/a.parquet'
    def body(name, path=key):
        return json.dumps({'Records': [{'eventName': name, 's3': {'bucket': {'name': 'bucket'}, 'object': {'key': path}}}]})
    native = body('ObjectCreated:Put')
    messages = [{'MessageId': str(index), 'ReceiptHandle': 'private-' + str(index), 'Body': value}
                for index, value in enumerate([body('ObjectCreated:Put', 'unrelated'), body('ObjectRemoved:Delete'), native])]
    writes, released, guarded = [], [], []
    pilot.outputs['EventQueueUrl'] = 'event'
    pilot.require_worker_stopped = lambda: guarded.append(True)
    pilot.bundle = SimpleNamespace(write=lambda name, data: writes.append((name, data)))
    def release(**kwargs):
        released.append(kwargs)
        if release_failure and len(released) == 1:
            raise RuntimeError('provider error contains private-receipt-value')
    pilot.clients = {'sqs': SimpleNamespace(
        receive_message=lambda **kwargs: {'Messages': messages},
        change_message_visibility=release,
    )}
    if release_failure:
        with pytest.raises(RuntimeError, match='SQS receipt operation failed'):
            module._capture_native(pilot, fixture, key, 'CREATE', 'first_create')
        captured = writes[0][1]
    else:
        captured = module._capture_native(pilot, fixture, key, 'CREATE', 'first_create')
    assert guarded == [True]
    assert captured['body'] == native and captured['message_id'] == '2'
    assert [item['ReceiptHandle'] for item in released] == ['private-0', 'private-1', 'private-2']
    assert all(item['VisibilityTimeout'] == 0 for item in released)
    assert 'private-' not in json.dumps(writes)
    assert 'unrelated' not in json.dumps(writes)
