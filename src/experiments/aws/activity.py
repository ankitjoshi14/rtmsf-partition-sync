"""Bounded activity experiments; reuse AWS fixtures, accounting and cleanup.

Run from the repository root. Results are separate from the five-graph matrix.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tarfile
from threading import Event, Thread
import time
import uuid

from experiments.aws.deploy import validate_target
from experiments.aws.request_audit import RequestAudit
from experiments.aws.request_metrics import METRICS, RequestMetrics, wait_until
from experiments.aws.trial_buckets import TrialBuckets
from experiments.aws.workloads import Pilot
from experiments.evidence import EvidenceBundle, source_snapshot, utc

BURSTS = ('quick', 'spaced', 'distributed', 'mixed')
CONDITIONS = BURSTS + tuple(f'continuous_{rate}ps_{active}p' for rate in (1, 3) for active in (10, 20))


def schedule(condition, base=100):
    if condition not in CONDITIONS or base < 100:
        raise ValueError('unknown condition or insufficient starting partitions')
    continuous = condition.startswith('continuous_')
    rate, active = (int(condition.split('_')[1][:-2]), int(condition.split('_')[2][:-1])) if continuous else (0, 1)
    changes, created, deleted = [], 0, 0
    for index in range(120 * rate if continuous else 20):
        at = index / rate if continuous else index * 2 if condition == 'spaced' else 0
        if (continuous and index % 4 == 3) or (condition == 'mixed' and index % 2):
            changes.append((at, 'DELETE', deleted, deleted))
            deleted += 1
        else:
            partition = base + (created % active if continuous else created if condition == 'distributed' else 0)
            changes.append((at, 'CREATE', partition, 1000000 + index))
            created += 1
    return changes


def run_trial(pilot, trial, timeline):
    fixture = pilot.fixtures[trial['id']]
    window = dict(id=trial['id'], bucket=fixture['bucket'], started_at=utc(), requester_arn=pilot.outputs['TaskRoleArn'])
    pilot.windows.append(window)
    start, stop = time.monotonic(), Event()
    samples, first_upload, completed = [], None, 0

    def observe():
        while not stop.is_set():
            sample = {'elapsed_s': time.monotonic() - start, 'completed_operations': completed}
            try:
                sample['first_partition_present'] = pilot.presence(fixture, pilot.values(100))
                sample['observed_elapsed_s'] = time.monotonic() - start
                attrs = pilot.clients['sqs'].get_queue_attributes(QueueUrl=pilot.outputs['EventQueueUrl'],
                    AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'])['Attributes']
                sample.update(queue_visible=int(attrs['ApproximateNumberOfMessages']),
                              queue_inflight=int(attrs['ApproximateNumberOfMessagesNotVisible']))
            except Exception as error:
                sample['error'] = type(error).__name__
            samples.append(sample)
            pilot.record('activity_sample', trial=trial['id'], **sample)
            stop.wait(1)

    observer = Thread(target=observe, daemon=True)
    observer.start()
    try:
        for at, operation, partition, record_id in schedule(trial['condition']):
            if time.monotonic() >= pilot.deadline:
                raise TimeoutError('activity run deadline reached')
            time.sleep(max(0, start + at - time.monotonic()))
            began = time.monotonic()
            if operation == 'CREATE':
                _, _, done = pilot.upload(fixture, pilot.values(partition), record_id)
                if first_upload is None:
                    first_upload = done
            else:
                key = fixture['root'] + pilot.path(pilot.values(partition)) + f'part-{record_id:06d}.parquet'
                pilot.remove(fixture, key)
                done = time.monotonic()
            completed += 1
            pilot.record('activity_operation', trial=trial['id'], operation=operation, partition=partition,
                         scheduled_s=at, started_s=began - start, completed_s=done - start)
        writes_done, writes_done_at = time.monotonic(), utc()
        pilot.wait_processed(fixture, window['started_at'])
        pilot.drain(trial['id'] + '_after')
        drained = time.monotonic()
        records = [item['record'] for item in pilot.telemetry()
                   if window['started_at'] <= item['record'].get('timestamp', '')]
        table_id = pilot.outputs['GlueDatabaseName'] + '.' + fixture['table']['Name']
        attempts = [r for r in records if r.get('action') == 'pmcp_result' and r.get('table_id') == table_id]
        calls = Counter()
        for record in records:
            if record.get('action') == 'api_call' and record.get('phase') == 'maintenance':
                calls[record['operation']] += record.get('attempts', 1)
        first_present = next((s['observed_elapsed_s'] for s in samples if s.get('first_partition_present') is True), None)
        if first_upload is None or first_present is None or not attempts or any(s.get('error') for s in samples):
            raise RuntimeError('activity observation evidence is incomplete')
        if trial['handler'] == 'immediate' and any(r['observation_count'] != 1 for r in attempts):
            raise AssertionError('immediate control combined events')
        return dict(condition=trial['condition'], method=trial['handler'], repetition=trial['repetition'],
                    completed_operations=completed, write_duration_s=writes_done - start,
                    pmcp_attempts=len(attempts), catalog_api_attempts=sum(n for op, n in calls.items() if op.startswith('Glue.')),
                    first_partition_visibility_s=first_present - (first_upload - start),
                    attempts_during_writes=sum(r['timestamp'] < writes_done_at for r in attempts),
                    processed_events_during_writes=sum(r['observation_count'] for r in attempts
                        if r['timestamp'] < writes_done_at and r['status'] in ('APPLIED', 'NOOP')),
                    partitions_processed_during_writes=len({r['partition'] for r in attempts
                        if r['timestamp'] < writes_done_at and r['status'] in ('APPLIED', 'NOOP')}),
                    peak_queue_visible=max(s.get('queue_visible', 0) for s in samples),
                    peak_queue_inflight=max(s.get('queue_inflight', 0) for s in samples),
                    drain_s=drained - writes_done, status='workload_passed', adapter_calls=dict(calls),
                    retry_attempts=sum(r['status'] == 'RETRY' for r in attempts),
                    terminal_observations=sum(r['observation_count'] for r in attempts if r['status'] in ('APPLIED', 'NOOP')))
    finally:
        stop.set()
        observer.join(timeout=45)
        window['ended_at'] = utc()
        timeline.extend(dict(condition=trial['condition'], method=trial['handler'], repetition=trial['repetition'], **s) for s in samples)
        if observer.is_alive():
            raise RuntimeError('activity observer did not stop')


def main():
    import boto3
    from experiments.activity_reports import write_activity_reports

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--expected-account', required=True)
    parser.add_argument('--stack', required=True)
    parser.add_argument('--region', default='us-west-2')
    parser.add_argument('--build-manifest', type=Path, required=True)
    parser.add_argument('--conditions', nargs='+', choices=CONDITIONS, default=list(CONDITIONS))
    parser.add_argument('--repetitions', type=int, choices=range(1, 6), default=1)
    parser.add_argument('--max-minutes', type=int, default=65)
    args = parser.parse_args()
    if args.max_minutes < 1 or len(set(args.conditions)) != len(args.conditions):
        parser.error('positive time budget and distinct conditions required')
    # Existing fixture/runtime helpers expect these shared settings.
    args.output_dir, args.timeout_seconds = Path('runs'), 240
    args.correctness_repetitions, args.graph4_partitions = args.repetitions, 100
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    validate_target(session.client('sts').get_caller_identity()['Account'], args.expected_account, args.stack, args.region)
    stack = session.client('cloudformation').describe_stacks(StackName=args.stack)['Stacks'][0]
    if stack['StackStatus'] not in ('CREATE_COMPLETE', 'UPDATE_COMPLETE'):
        raise ValueError('stack is not ready')
    outputs = {item['OutputKey']: item['OutputValue'] for item in stack['Outputs']}
    source = source_snapshot(Path.cwd())
    build = json.loads(args.build_manifest.read_text())
    runtime_names = [name for name in source['files'] if name.startswith('src/rtmsf/')]
    runtime_names += ['src/experiments/__init__.py', 'src/experiments/aws/__init__.py',
                      'src/experiments/aws/crash_worker.py', 'src/experiments/aws/activity_worker.py']
    if build.get('files') != {name: source['files'][name] for name in runtime_names}:
        raise ValueError('image runtime differs from current source; rebuild the experiment image')
    run_id = 'run_' + time.strftime('%Y%m%d_%H%M%S', time.gmtime()) + '_' + uuid.uuid4().hex[:4]
    root = args.output_dir / run_id
    bundle = EvidenceBundle(root, dict(config=vars(args), outputs=outputs, source=source, kind='bounded_activity_validation'))
    bundle.write('image-build.json', build)
    with tarfile.open(root / 'source.tar.gz', 'w:gz') as archive:
        for name in source['files']:
            archive.add(name)
    pilot = Pilot(session, outputs, bundle, run_id, args)
    original = pilot.service()['taskDefinition']
    definition = pilot.clients['ecs'].describe_task_definition(taskDefinition=original)['taskDefinition']
    if next(c for c in definition['containerDefinitions'] if c['name'] == 'worker')['image'] != build['image']:
        raise ValueError('deployed image does not match build manifest')
    pilot.require_worker_stopped()
    attrs = pilot.clients['sqs'].get_queue_attributes(QueueUrl=outputs['DeadLetterQueueUrl'],
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'])['Attributes']
    if any(int(v) for v in attrs.values()):
        raise ValueError('existing DLQ entries must be handled separately before this run')
    buckets = TrialBuckets(session, outputs, args.expected_account, args.region, run_id, root)
    pilot.trial_buckets = buckets
    audit = RequestAudit(session, outputs, args.region, root / 'request_audit')
    trail = session.client('cloudtrail')
    was_logging = trail.get_trail_status(Name=outputs['TrailName'])['IsLogging']
    matrix = [dict(id=f'{condition}_{handler}_r{repetition}', condition=condition, handler=handler,
                   method='rtmsf', graph='activity', base=100, x=100, repetition=repetition)
              for repetition in range(1, args.repetitions + 1)
              for handler in (('rtmsf', 'immediate') if repetition % 2 else ('immediate', 'rtmsf'))
              for condition in args.conditions if handler == 'rtmsf' or condition in BURSTS]
    rows = {t['id']: dict(condition=t['condition'], method=t['handler'], repetition=t['repetition'], status='not_attempted') for t in matrix}
    timeline, errors, stopped, current_handler = [], [], False, None
    print(f'Activity evidence: {root.resolve()} ({len(matrix)} conditions)', flush=True)
    try:
        buckets.provision(matrix)
        audit.buckets = list(buckets.buckets.values())
        trail.start_logging(Name=outputs['TrailName'])
        for trial in matrix:
            fixture = pilot.fixture(trial['id'], 'rtmsf', base=100)
            pilot.verify(fixture, trial['id'] + '_baseline')
        pilot.drain('before_activity')
        buckets.enable_notifications()
        ready = max(datetime.fromisoformat(t['metrics_enabled_at']).timestamp() for t in buckets.state['trials'].values()) + 900
        wait_until(ready, 'CloudWatch activation')
        audit.calibrate()
        ready = (int(time.time()) // 60 + 3) * 60
        pilot.record('activity_setup_complete', measurement_not_before=datetime.fromtimestamp(ready, timezone.utc).isoformat())
        wait_until(ready, 'Two complete quiet minutes after all setup reads')
        for trial in matrix:
            if time.monotonic() >= pilot.deadline:
                raise TimeoutError('no further conditions started after activity budget')
            if trial['handler'] != current_handler:
                pilot.stop_worker()
                pilot.configure()
                if trial['handler'] == 'immediate':
                    task = pilot.clients['ecs'].describe_task_definition(taskDefinition=pilot.task_definition)['taskDefinition']
                    for field in ('taskDefinitionArn', 'revision', 'status', 'requiresAttributes', 'compatibilities', 'registeredAt', 'registeredBy', 'deregisteredAt'):
                        task.pop(field, None)
                    next(c for c in task['containerDefinitions'] if c['name'] == 'worker')['entryPoint'] = ['python', '-m', 'experiments.aws.activity_worker']
                    pilot.task_definition = pilot.clients['ecs'].register_task_definition(**task)['taskDefinition']['taskDefinitionArn']
                    pilot.clients['ecs'].update_service(cluster=outputs['ClusterName'], service=outputs['ServiceName'],
                                                       taskDefinition=pilot.task_definition, desiredCount=0)
                pilot.start_worker()
                current_handler = trial['handler']
            print(f'Running {trial["id"]}', flush=True)
            try:
                rows[trial['id']] = run_trial(pilot, trial, timeline)
            except Exception as error:
                rows[trial['id']].update(status='failed', error=f'{type(error).__name__}: {error}')
                raise
    except BaseException as error:
        errors.append(f'{type(error).__name__}: {error}')
        print(f'Activity interrupted: {errors[-1]}', flush=True)
    finally:
        try:
            pilot.stop_worker()
            stopped = True
            pilot.clients['ecs'].update_service(cluster=outputs['ClusterName'], service=outputs['ServiceName'], taskDefinition=original, desiredCount=0)
        except Exception as error:
            errors.append(f'worker cleanup: {error}')
        try:
            if pilot.windows:
                wait_until(time.time() + 600, 'CloudWatch delivery; trial buckets remain idle')
                cutoff = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
                for window in pilot.windows:
                    window['quiet_until'] = cutoff
                counts = RequestMetrics(session, args.region, root / 'request_metrics').collect(pilot.windows)
                corroboration = audit.collect(pilot.windows, wait_seconds=0)
                for key, metrics in counts.items():
                    row = rows[key]
                    row.update(metrics)
                    expected = dict(zip(METRICS, (row.get('adapter_calls', {}).get('S3.HeadObject', 0),
                                                  row.get('adapter_calls', {}).get('S3.GetObject', 0),
                                                  row.get('adapter_calls', {}).get('S3.ListObjectsV2', 0))))
                    row['request_counts_agree'] = all(value is not None and value == corroboration.get(key, {}).get(metric) == expected[metric]
                                                    for metric, value in metrics.items())
                    if row['status'] == 'workload_passed' and not row['request_counts_agree']:
                        row.update(status='incomplete', error='S3 accounting sources do not agree')
            for trial in matrix:
                row = rows[trial['id']]
                if row['status'] == 'not_attempted':
                    continue
                try:
                    pilot.verify(pilot.fixtures[trial['id']], trial['id'] + '_final')
                    if row['status'] == 'workload_passed':
                        row['status'] = 'passed'
                except Exception as error:
                    row.update(status='failed', error=f'Final verification: {error}')
        except BaseException as error:
            errors.append(f'collection: {type(error).__name__}: {error}')
        try:
            pilot.collect_runtime()
        except Exception as error:
            errors.append(f'runtime evidence: {error}')
        if stopped:
            try:
                buckets.cleanup()
            except Exception as error:
                errors.append(f'bucket cleanup: {error}')
        try:
            if not was_logging:
                trail.stop_logging(Name=outputs['TrailName'])
        except Exception as error:
            errors.append(f'trail cleanup: {error}')
        bundle.write('request-windows.json', pilot.windows)
        bundle.write('worker-lifecycle.json', pilot.lifecycle)
        write_activity_reports(root, list(rows.values()), timeline, errors)
        bundle.write('artifact-hashes.json', {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()})
    complete = not errors and all(row['status'] == 'passed' for row in rows.values())
    print(f'{"passed" if complete else "incomplete_or_failed"}: {root.resolve()}', flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
