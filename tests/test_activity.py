"""Checks specific to the experiment control and retained evidence."""
import json
from types import SimpleNamespace

import pytest

from experiments.evidence import EvidenceBundle
from experiments.aws.workloads import Pilot
from rtmsf.models import MutationResult
from tests.test_worker import Clock, Source, Pmcp, delivery, event, worker


def test_immediate_control_keeps_same_partition_events_separate_and_waits_for_retry():
    from experiments.aws.activity_worker import ImmediateBuffer

    clock = Clock()
    source = Source(clock, [delivery('batch', event(), event(operation='DELETE'))])
    pmcp = Pmcp(source, [MutationResult('RETRY', 'temporary'), MutationResult('NOOP', 'empty')])
    control = worker(source, pmcp=pmcp, jitter_ms=lambda maximum: maximum)
    control._buffer = ImmediateBuffer()
    control.run_once()
    assert [intent.mutation for intent in pmcp.intents] == ['ADD', 'DROP']
    assert all(intent.observation_count == 1 for intent in pmcp.intents)
    assert source.acknowledged == []
    clock.advance(1000)
    control.run_once()
    assert source.acknowledged == ['batch']
    assert [intent.mutation for intent in pmcp.intents] == ['ADD', 'DROP', 'ADD']


def test_failed_catalog_check_retains_actual_extra_partition(tmp_path):
    pilot = Pilot.__new__(Pilot)
    pilot.bundle = EvidenceBundle(tmp_path / 'run', {})
    pilot.outputs = {'GlueDatabaseName': 'db'}
    extra = {'Values': ['2026-01-01', 'site-000', '00'], 'StorageDescriptor': {'Location': 's3://bucket/table/dt=2026-01-01/site=site-000/hour=00/', 'Columns': []}}
    pilot.clients = {'glue': SimpleNamespace(get_paginator=lambda name: SimpleNamespace(paginate=lambda **kw: [{'Partitions': [extra]}]))}
    fixture = {'objects': {}, 'table': {'Name': 'trial', 'StorageDescriptor': {'Location': 's3://bucket/table/', 'Columns': []}}}
    with pytest.raises(AssertionError, match='complete catalog membership'):
        pilot.verify(fixture, 'extra')
    saved = json.loads((pilot.bundle.root / 'catalog/extra.json').read_text())
    assert saved['expected'] == []
    assert saved['observed'][0]['Values'] == extra['Values']


def test_activity_schedules_preserve_live_creates_and_bound_existing_deletes():
    from experiments.aws.activity import schedule

    mixed = schedule('mixed', 100)
    assert len(mixed) == 20
    assert sum(operation == 'DELETE' for _, operation, _, _ in mixed) == 10
    for rate in (1, 3):
        for active in (10, 20):
            changes = schedule(f'continuous_{rate}ps_{active}p', 100)
            assert len(changes) == 120 * rate
            assert max(at for at, *_ in changes) < 120
            creates = {partition for _, operation, partition, _ in changes if operation == 'CREATE'}
            deletes = [partition for _, operation, partition, _ in changes if operation == 'DELETE']
            assert len(creates) == active
            assert len(deletes) == len(set(deletes)) <= 100
            assert creates.isdisjoint(deletes)


def test_activity_outputs_keep_missing_counts_and_regenerate(tmp_path):
    from experiments.activity_reports import write_activity_reports
    from experiments.aws.trial_buckets import bucket_name

    trial = dict(condition='continuous_3ps_20p', handler='rtmsf', method='rtmsf',
                 graph='activity', base=100, x=100, repetition=1)
    assert len(bucket_name(trial, 'run_20260930_120000_a1b2')) <= 63
    assert bucket_name(trial, 'one') != bucket_name(trial, 'two')
    assert bucket_name(trial, 'one') != bucket_name(trial | {'handler': 'immediate'}, 'one')
    rows = [dict(condition='quick', method='rtmsf', repetition=1, status='incomplete',
                 s3_head_requests=None, first_partition_visibility_s=11.123456)]
    original, regenerated = tmp_path / 'original', tmp_path / 'regenerated'
    write_activity_reports(original, rows, [], ['measurement unavailable'])
    payload = json.loads((original / 'report_inputs.json').read_text())
    write_activity_reports(regenerated, payload['rows'], payload['timeline'], payload['errors'])
    for file in original.iterdir():
        assert file.read_bytes() == (regenerated / file.name).read_bytes()
    assert 'unknown / unknown / unknown' in (original / 'SUMMARY.md').read_text()


def test_buffer_comparison_keeps_resource_identity_and_output_settings(tmp_path):
    from experiments.activity_reports import write_activity_reports
    from experiments.aws.trial_buckets import bucket_name

    trial = dict(condition='load_20ps_1000p', handler='rtmsf', method='rtmsf',
                 graph='activity', base=1000, x=100, repetition=1, buffer_size=100)
    assert bucket_name(trial, 'same-run') != bucket_name(trial | {'buffer_size': 1000}, 'same-run')
    assert len(bucket_name(trial | {'buffer_size': 1000}, 'same-run')) <= 63
    row = dict(condition=trial['condition'], method='rtmsf', repetition=1, status='passed',
               max_pending_deliveries=1000, max_buffer_bytes=16 * 1024 ** 2,
               queue_visibility_seconds=1000, peak_rss_bytes=80 * 1024 ** 2,
               peak_pending_messages=650, peak_pending_body_bytes=800000)
    write_activity_reports(tmp_path, [row], [], [])
    assert 'max_pending_deliveries' in (tmp_path / 'activity_data.csv').read_text().splitlines()[0]
    assert '80.000' in (tmp_path / 'SUMMARY.md').read_text()


def test_memory_summary_requires_final_sample_from_the_exact_worker():
    from experiments.aws.activity import memory_summary

    def sample(task, rss, final=False):
        return dict(log_stream='worker/main/' + task, record=dict(action='memory_sample',
                    final=final, peak_rss_bytes=rss, pending_messages=10, pending_body_bytes=2000))
    records = [sample('first', 100), sample('second', 999, True)]
    with pytest.raises(ValueError, match='final'):
        memory_summary(records, 'first')
    records.append(sample('first', 150, True))
    assert memory_summary(records, 'first')['peak_rss_bytes'] == 150


def test_load_schedule_keeps_seed_files_and_deletes_only_completed_prior_creates():
    from experiments.aws.activity import LOADS, schedule

    for condition in LOADS:
        rate, active = (int(part[:-2]) if part.endswith('ps') else int(part[:-1])
                        for part in condition.split('_')[1:])
        changes = schedule(condition)
        assert len(changes) == rate * 120
        live = set()
        for at, operation, partition, record_id in changes:
            assert 0 <= partition < active <= 1000 and 0 <= at < 120
            assert record_id >= 1000000
            identity = (partition, record_id)
            if operation == 'CREATE':
                assert identity not in live
                live.add(identity)
            else:
                live.remove(identity)
        assert len(live) == len(changes) // 2


def test_activity_backlog_includes_operations_without_delivered_notifications():
    from experiments.aws.activity import annotate_backlog

    operations = [dict(key_sha256='a', operation='CREATE', completed_s=0.5),
                  dict(key_sha256='b', operation='DELETE', completed_s=1.5)]
    records = [dict(action='message_decoded', message_id='m', events=[dict(key_sha256='a', operation='CREATE')]),
               dict(action='message_acknowledged', message_id='m', timestamp='2026-01-01T00:00:02+00:00'),
               dict(action='message_acknowledged', message_id='m', timestamp='2026-01-01T00:00:10+00:00')]
    samples = [dict(elapsed_s=s) for s in (1, 3, 5)]
    annotate_backlog(samples, operations, records, '2026-01-01T00:00:00+00:00')
    assert [s['unacknowledged_operations'] for s in samples] == [1, 1, 1]
    assert samples[-1]['acknowledged_operations'] == 1
    assert samples[-1]['oldest_unacknowledged_s'] == 3.5


def test_activity_retries_uncertain_writes_without_reading_or_changing_payload(monkeypatch):
    from botocore.exceptions import ConnectionClosedError
    import experiments.aws.activity as activity
    import experiments.aws.workloads as workloads

    pilot = Pilot.__new__(Pilot)
    pilot.changes, records, puts, deletes = [], [], [], []
    pilot.record = lambda event, **data: records.append(dict(event=event, **data))
    monkeypatch.setattr(workloads, 'parquet_payload', lambda record_id: b'fixed parquet')
    monkeypatch.setattr(activity.time, 'sleep', lambda seconds: None)
    objects = {}
    def put(**kw):
        puts.append(kw)
        if kw['Key'] in objects:
            assert 'IfNoneMatch' not in kw, 'retry must tolerate an already accepted identical PUT'
        objects[kw['Key']] = kw['Body']
        if len(puts) == 1:
            raise ConnectionClosedError(endpoint_url='https://example.invalid')
        return {'ResponseMetadata': {'RequestId': 'put'}}
    def delete(**kw):
        deletes.append(kw)
        objects.pop(kw['Key'], None)
        if len(deletes) == 1:
            raise ConnectionClosedError(endpoint_url='https://example.invalid')
    pilot.clients = {'s3': SimpleNamespace(put_object=put, delete_object=delete)}
    fixture = dict(name='owned_trial', root='fixture/new/', bucket='owned', objects={})
    key, _, attempts = activity.write_operation(pilot, fixture, 'CREATE', 0, 1000000)
    assert attempts == 2 and len(objects) == len(fixture['objects']) == 1
    assert puts[0]['IfNoneMatch'] == '*' and puts[0]['Body'] == puts[1]['Body']
    with pytest.raises(ValueError, match='already recorded'):
        pilot.upload(fixture, pilot.values(0), 1000000, retry_write=True)
    assert activity.write_operation(pilot, fixture, 'DELETE', 0, 1000000)[2] == 2
    assert not objects and not fixture['objects'] and deletes[0] == deletes[1]
    assert len([r for r in records if r['event'] == 'activity_write_retry']) == 2
    def disconnected(**kw):
        raise ConnectionClosedError(endpoint_url='https://example.invalid')
    pilot.clients['s3'].put_object = disconnected
    with pytest.raises(ConnectionClosedError):
        activity.write_operation(pilot, fixture, 'CREATE', 1, 1000001)
    assert len([r for r in records if r['event'] == 'activity_write_retry']) == 4


def test_activity_visibility_is_unknown_when_first_upload_retried(monkeypatch):
    from threading import Event
    from botocore.exceptions import ConnectionClosedError
    import experiments.aws.activity as activity
    import experiments.aws.workloads as workloads

    accepted, observed = Event(), Event()
    records, timeline = [], []
    def record(event, **data):
        records.append(dict(event=event, **data))
        if event == 'activity_sample' and data.get('first_partition_present'):
            observed.set()
    def put(**kwargs):
        if not accepted.is_set():
            accepted.set()
            assert observed.wait(2), 'the accepted PUT must become visible before its response fails'
            raise ConnectionClosedError(endpoint_url='https://example.invalid')
        return {'ResponseMetadata': {'RequestId': 'retry'}}
    pilot = Pilot.__new__(Pilot)
    fixture = dict(name='trial', root='fixture/trial/', bucket='owned', objects={}, table={'Name': 'trial'})
    pilot.fixtures, pilot.windows, pilot.changes = {'trial': fixture}, [], []
    pilot.outputs = dict(TaskRoleArn='task-role', GlueDatabaseName='db', EventQueueUrl='queue')
    pilot.deadline = activity.time.monotonic() + 10
    pilot.record = record
    pilot.presence = lambda *args: accepted.wait(2)
    pilot.wait_processed = pilot.drain = lambda *args: None
    pilot.clients = dict(s3=SimpleNamespace(put_object=put), sqs=SimpleNamespace(
        get_queue_attributes=lambda **kwargs: {'Attributes': {
            'ApproximateNumberOfMessages': '0', 'ApproximateNumberOfMessagesNotVisible': '0'}}))
    pilot.telemetry = lambda: [{'record': dict(action='pmcp_result', table_id='db.trial',
        timestamp=activity.utc(), status='APPLIED', observation_count=2, partition='p')}]
    monkeypatch.setattr(workloads, 'parquet_payload', lambda record_id: b'fixed parquet')
    monkeypatch.setattr(activity, 'schedule', lambda condition: [(0, 'CREATE', 100, 1000000)])
    row = activity.run_trial(pilot, dict(id='trial', condition='quick', handler='rtmsf', repetition=1), timeline)
    assert row['first_partition_visibility_s'] is None
    assert row['first_partition_visibility_note'] == 'first upload retried; original completion unknown'
    assert row['producer_retry_attempts'] == 1
    operation = next(r for r in records if r['event'] == 'activity_operation')
    assert operation['completed_s'] > timeline[0]['observed_elapsed_s']
