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
