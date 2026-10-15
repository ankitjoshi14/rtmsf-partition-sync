"""Bounded real-OCI burst checks; catalog changes use an in-memory test double."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import time

import oci

from experiments.oci.run import MemoryCatalog, client_options, normalize_report
from rtmsf.adapters.oci import OciObjectStore, OciQueueEventSource, enqueue_event
from rtmsf.models import TableDefinition
from rtmsf.pmcp import PartitionMutationCommitProtocol
from rtmsf.routing import RoutingIndex
from rtmsf.worker import RTMSFWorker


def summarize(records, partitions, mutation):
    buffered = sum(r.get('event_count', 0) for r in records if r.get('action') == 'delivery_buffered')
    results = [r for r in records if r.get('action') == 'pmcp_result']
    assert results and {r['partition'] for r in results} == partitions, 'all affected partitions must be reconciled'
    assert all(r['status'] in {'APPLIED', 'NOOP'} and r['mutation'] == mutation for r in results), 'unexpected reconciliation result'
    counts = dict.fromkeys(partitions, 0)
    for result in results:
        assert result['observation_count'] > 0, 'nonempty reconciliation required'
        counts[result['partition']] += result['observation_count']
    assert sum(counts.values()) == buffered, 'all notifications must be reconciled'
    assert len(results) < buffered, 'at least one window must group notifications'
    seen = 0
    acknowledgements = 0
    for record in records:
        if record.get('action') == 'pmcp_result':
            seen += record['observation_count']
        elif record.get('action') == 'delivery_acknowledged':
            acknowledgements += record['event_count']
            assert not record['dead_lettered'], 'unexpected quarantine'
            assert acknowledgements <= seen, 'acknowledged before reconciliation'
    assert acknowledgements == buffered, 'all notifications must be acknowledged'
    return {'notifications': buffered, 'reconciliations': len(results),
            'acknowledgements': acknowledgements, 'observations_per_partition': counts}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--resources', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--config-file', default='~/.oci/config')
    parser.add_argument('--profile', default='RTMSF_OCI')
    parser.add_argument('--instance-principal', action='store_true')
    parser.add_argument('--external-rule-control', action='store_true',
                        help='Controller must enable the dedicated rule before this run and disable it afterward')
    args = parser.parse_args()
    root = args.output_dir
    root.mkdir(parents=True, exist_ok=True)
    if (root / 'burst-report.json').exists():
        raise ValueError('refuse to overwrite previous burst evidence')
    resource = json.loads(args.resources.read_text())
    rs = resource['resources']
    bucket, namespace = rs['bucket'], resource['namespace']
    if bucket != 'rtmsf-events-20261012' or resource['region'] != 'us-sanjose-1':
        raise ValueError('refuse unrelated resources')
    config, options = client_options(args, resource['region'])
    storage_client = oci.object_storage.ObjectStorageClient(config, **options)
    queue = oci.queue.QueueClient(config, service_endpoint=rs['queue']['endpoint'], **options)
    quarantine = oci.queue.QueueClient(config, service_endpoint=rs['quarantine']['endpoint'], **options)
    if not args.external_rule_control:
        events = oci.events.EventsClient(config, **options)
        rule = events.get_rule(rs['rule']['id']).data
        if rule.display_name != 'rtmsf-object-events-20261012' or rule.is_enabled:
            raise ValueError('expected the dedicated disabled Events rule')
    stats = queue.get_stats(rs['queue']['id']).data
    if stats.queue.visible_messages or stats.queue.in_flight_messages or stats.dlq.visible_messages or stats.dlq.in_flight_messages:
        raise ValueError('test requires empty primary queue and DLQ')
    started = time.monotonic()
    prefix = 'burst-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '/'
    sources = {'demo': (namespace, bucket)}
    storage = OciObjectStore(storage_client, sources)
    if storage.prefix_non_empty('demo', prefix):
        raise ValueError('test prefix must be fresh')
    records, raw, checks = [], [], []
    catalog = MemoryCatalog()
    report = {'provider': 'OCI', 'region': resource['region'], 'prefix': prefix,
              'authentication': 'instance principal' if args.instance_principal else 'user session',
              'rule_control': 'external controller' if args.external_rule_control else 'runner',
              'catalog': 'in-memory test double', 'passed': False, 'checks': checks,
              'settings': {'settling_ms': 5000, 'maximum_ms': 20000, 'queue_lease_seconds': 120,
                           'max_pending_deliveries': 20, 'poll_wait_seconds': 0},
              'started_at': datetime.now(timezone.utc).isoformat()}

    class Source(OciQueueEventSource):
        def receive(self, *args):
            deliveries = super().receive(*args)
            for d in deliveries:
                raw.append({'received_elapsed_seconds': round(time.monotonic()-started, 3),
                            'event': json.loads(self._pending[d.handle].body)})
            return deliveries

    source = Source(queue, rs['queue']['id'], quarantine, rs['quarantine']['id'], sources)

    def emit(record):
        records.append(dict(normalize_report(record), elapsed_seconds=round(time.monotonic()-started, 3)))

    worker = RTMSFWorker(
        source=source, pmcp=PartitionMutationCommitProtocol(storage, catalog),
        telemetry=type('Telemetry', (), {'emit': staticmethod(emit)})(),
        routing=RoutingIndex([TableDefinition('demo.bursts', 'demo', prefix, ('dt',))]),
        settling_ms=5000, maximum_ms=20000, queue_lease_seconds=120,
        pmcp_budget_ms=20000, source_call_budget_ms=20000,
        poll_wait_seconds=0, max_pending_deliveries=20)

    def put(key):
        storage_client.put_object(namespace, bucket, key, b'rtmsf-burst', content_type='application/octet-stream')

    def delete(key):
        storage_client.delete_object(namespace, bucket, key)

    def phase(name, operation, items, expected, touched, mutation, event_suffix):
        first_record, first_raw = len(records), len(raw)
        begin = time.monotonic()
        def produce():
            for item in items:
                operation(item)
            return round(time.monotonic()-begin, 3)
        with ThreadPoolExecutor(max_workers=1) as pool:
            producer = pool.submit(produce)
            deadline = begin+100
            while time.monotonic() < deadline:
                worker.run_once()
                if producer.done():
                    duration = producer.result()
                    phase_raw = raw[first_raw:]
                    matching = [r['event'] for r in phase_raw if r['event'].get('eventType', '').endswith(event_suffix)]
                    keys = {e['data']['resourceName'] for e in matching}
                    complete = len(phase_raw) >= len(items) if event_suffix == 'replay' else set(items) <= keys
                    if complete and not source._pending:
                        actual = {key[2] for key in catalog.locations}
                        assert actual == expected, 'unexpected catalog membership'
                        summary = summarize(records[first_record:], touched, mutation)
                        checks.append(dict(case=name, status='passed', producer_seconds=duration,
                                           partitions=sorted(actual), **summary))
                        print(json.dumps(checks[-1]), flush=True)
                        return
                time.sleep(0.25)
        raise RuntimeError('phase_timeout_'+name)

    try:
        if not args.external_rule_control:
            events.update_rule(rule.id, oci.events.models.UpdateRuleDetails(is_enabled=True))
            assert events.get_rule(rule.id).data.is_enabled
        a = [prefix+'dt=a/f'+str(i) for i in range(6)]
        b = [prefix+'dt=a/g'+str(i) for i in range(3)] + [prefix+'dt=b/f'+str(i) for i in range(3)]
        phase('six_creates_one_partition', put, a, {'dt=a'}, {'dt=a'}, 'ADD', 'createobject')
        phase('six_creates_two_partitions', put, b, {'dt=a', 'dt=b'}, {'dt=a', 'dt=b'}, 'ADD', 'createobject')
        original = raw[0]['event']
        phase('three_duplicate_notifications', lambda _: enqueue_event(queue, rs['queue']['id'], json.dumps(original)),
              range(3), {'dt=a', 'dt=b'}, {'dt=a'}, 'ADD', 'replay')
        phase('twelve_deletes_two_partitions', delete, a+b, set(), {'dt=a', 'dt=b'}, 'DROP', 'deleteobject')
        assert not storage.prefix_non_empty('demo', prefix), 'test objects remain unexpectedly'
        report['passed'] = True
    except Exception as error:
        report['error_type'] = type(error).__name__
        raise
    finally:
        try:
            if not args.external_rule_control:
                events.update_rule(rule.id, oci.events.models.UpdateRuleDetails(is_enabled=False))
                report['rule_disabled'] = not events.get_rule(rule.id).data.is_enabled
            report['primary_queue_final'] = oci.util.to_dict(queue.get_stats(rs['queue']['id']).data)
            report['quarantine_final'] = oci.util.to_dict(quarantine.get_stats(rs['quarantine']['id']).data)
        except Exception as error:
            report['cleanup_error_type'] = type(error).__name__
            report['passed'] = False
        report['duration_seconds'] = round(time.monotonic()-started, 3)
        report['worker_records'] = records
        (root/'burst-report.json').write_text(json.dumps(report, indent=2)+'\n')
        (root/'burst-raw-events.json').write_text(json.dumps(raw, indent=2)+'\n')
        print(json.dumps({'passed': report['passed'], 'rule_disabled': report.get('rule_disabled'),
                          'checks': len(checks), 'report': str(root/'burst-report.json')}), flush=True)
    if not report['passed'] or (not args.external_rule_control and not report.get('rule_disabled')):
        raise RuntimeError('burst proof or rule shutdown incomplete')


if __name__ == '__main__':
    main()
