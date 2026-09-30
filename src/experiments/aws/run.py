"""Configure and run AWS experiments, preserving one evidence bundle per execution.

Use --help for settings. Workload steps live in experiments.aws.workloads;
report generation can also run offline from the retained bundle.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import time
import tarfile
import uuid

from experiments.aws.deploy import validate_target, service_tasks, require_stopped, require_tasks_stopped
from experiments.evidence import EvidenceBundle, source_snapshot, utc
from experiments.aws.workloads import METHODS, Pilot
from experiments.aws.request_audit import RequestAudit, METRICS
from experiments.aws.request_metrics import RequestMetrics
from experiments.aws.trial_buckets import TrialBuckets
from experiments.aws.request_metrics import wait_until
from experiments.aws.checks import CHECK_NAMES, prepare_checks, run_checks
from experiments.figures import graph_row, write_reports, RECOVERY_CASES


def trial_matrix(sizes, files, graph4_partitions, repetitions, layout_sizes=()):
    trials = []
    for repetition in range(1, repetitions + 1):
        shift = (repetition - 1) % len(METHODS)
        methods = METHODS[shift:] + METHODS[:shift]
        for graph, xs in ((2, sizes), (4, files)):
            for x in xs:
                for method in methods:
                    if graph == 4 and method == 'athena_msck':
                        continue
                    trials.append(dict(id=f'g{graph}_{method}_{x}_r{repetition}', graph=graph,
                                       method=method, x=x, repetition=repetition,
                                       base=x if graph == 2 else graph4_partitions))
        layouts = ('day_first', 'tenant_first') if repetition % 2 else ('tenant_first', 'day_first')
        for size in layout_sizes:
            for layout in layouts:
                for method in methods:
                    if method != 'glue_event_crawler':
                        trials.append(dict(id=f'g5_{method}_{layout}_{size}_r{repetition}', graph=5,
                                           method=method, layout=layout, x=size, base=size, repetition=repetition))
    return trials


def main() -> int:
    import boto3
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stack', required=True)
    parser.add_argument('--expected-account', required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--region', default='us-west-2')
    parser.add_argument('--sizes', type=int, nargs='+', default=[100, 1000])
    parser.add_argument('--files', type=int, nargs='+', default=[1, 10, 100, 1000])
    parser.add_argument('--graph4-partitions', type=int, default=1000)
    parser.add_argument('--layout-sizes', type=int, nargs='+', default=[100, 1000],
                        help='Graph 5 starting partitions; paper matrix: 100 1000 10000')
    parser.add_argument('--repetitions', type=int, default=1, help='repetitions per comparison condition (default: 1)')
    parser.add_argument('--correctness-repetitions', type=int,
                        help='Graph 1, recovery and supporting check repetitions (default: --repetitions)')
    parser.add_argument('--timeout-seconds', type=int, default=600)
    parser.add_argument('--max-minutes', type=int, default=85,
                        help='stop starting trials after this many minutes; collection and cleanup can finish later (default: 85)')
    parser.add_argument('--audit-wait-seconds', type=int, default=600)
    parser.add_argument('--request-source', choices=('cloudwatch', 'cloudtrail'), default='cloudwatch',
                        help='CloudWatch is the experiment protocol; CloudTrail-only is a diagnostic mode')
    parser.add_argument('--output-dir', type=Path, default=Path('runs'))
    parser.add_argument('--cleanup-run', type=Path, help='Clean this run\'s measurement buckets after its worker/crawlers have stopped')
    parser.add_argument('--build-manifest', type=Path, help='image-contents.json containing immutable image and runtime file hashes')
    parser.add_argument('--comparisons-only', action='store_true', help='only Graphs 2/3: new-partition comparisons')
    parser.add_argument('--graph4-only', action='store_true', help='only Graph 4 file-count comparisons on fresh fixtures')
    parser.add_argument('--graph5-only', action='store_true', help='only Graph 5: RTMSF/Athena, both key orders')
    parser.add_argument('--checks-only', nargs='+', choices=CHECK_NAMES, help='run only selected supporting checks on fresh fixtures')
    parser.add_argument('--recovery-only', nargs='+', choices=tuple(RECOVERY_CASES), help='rerun selected crash cases on fresh fixtures')
    args = parser.parse_args()
    if args.correctness_repetitions is None:
        args.correctness_repetitions = args.repetitions
    if sum(bool(mode) for mode in (args.checks_only, args.comparisons_only, args.recovery_only, args.graph4_only, args.graph5_only)) > 1:
        parser.error('choose only one of --checks-only, --comparisons-only, --recovery-only, --graph4-only, or --graph5-only')
    full_pilot = not (args.checks_only or args.comparisons_only or args.recovery_only or args.graph4_only or args.graph5_only)
    if not args.cleanup_run and (full_pilot or args.graph5_only) and args.request_source != 'cloudwatch':
        parser.error('Graph 5 requires isolated CloudWatch buckets; use cloudtrail only with an earlier-graph mode')
    recovery_cases = args.recovery_only or tuple(RECOVERY_CASES)
    if (any(not 1 <= x <= 100000 for x in args.sizes + [args.graph4_partitions])
            or any(not 1 <= x <= 10000 for x in args.files + args.layout_sizes)
            or any(not 1 <= count <= 5 for count in (args.repetitions, args.correctness_repetitions))):
        parser.error('partition counts must be 1..100000; files/layout sizes 1..10000; repetitions 1..5')
    if any(len(set(values)) != len(values) for values in (args.sizes, args.files, args.layout_sizes)):
        parser.error('sizes, files and layout sizes must not repeat the same condition')
    if args.max_minutes < 1 or not 30 <= args.timeout_seconds <= 900 or not 0 <= args.audit_wait_seconds <= 1200:
        parser.error('use max-minutes >= 1, timeout-seconds 30..900, audit-wait-seconds 0..1200')
    if args.request_source == 'cloudwatch' and args.audit_wait_seconds < 600:
        parser.error('CloudWatch collection requires at least 600 quiet seconds')
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    validate_target(session.client('sts').get_caller_identity()['Account'], args.expected_account, args.stack, args.region)
    stack = session.client('cloudformation').describe_stacks(StackName=args.stack)['Stacks'][0]
    if stack['StackStatus'] not in ('CREATE_COMPLETE', 'UPDATE_COMPLETE'):
        raise ValueError('stack is not ready')
    outputs = {item['OutputKey']: item['OutputValue'] for item in stack['Outputs']}
    if args.cleanup_run:
        original = json.loads((args.cleanup_run / 'manifest.json').read_text())
        if original['config']['stack'] != args.stack or original['outputs'] != outputs:
            raise ValueError('cleanup run does not match the current stack outputs')
        ecs, glue = session.client('ecs'), session.client('glue')
        service = ecs.describe_services(cluster=outputs['ClusterName'], services=[outputs['ServiceName']])['services'][0]
        require_stopped(service)
        require_tasks_stopped(service_tasks(ecs, outputs['ClusterName'], outputs['ServiceName']))
        for path in (args.cleanup_run / 'crawler').glob('*.json'):
            name = json.loads(path.read_text())['crawler_name']
            try:
                if glue.get_crawler(Name=name)['Crawler']['State'] != 'READY':
                    raise ValueError('stop the run\'s crawler before cleanup: ' + name)
                glue.delete_crawler(Name=name)
            except glue.exceptions.EntityNotFoundException:
                pass
        buckets = TrialBuckets(session, outputs, args.expected_account, args.region, args.cleanup_run.name, args.cleanup_run)
        buckets.path = args.cleanup_run / f'cleanup-{uuid.uuid4().hex}.json'
        buckets.cleanup()
        print(f'Cleanup receipt: {buckets.path}', flush=True)
        return 0
    run_id = 'run_' + time.strftime('%Y%m%d_%H%M%S', time.gmtime()) + '_' + uuid.uuid4().hex[:4]
    root = args.output_dir / run_id
    print(f'Experiment evidence: {root.resolve()}', flush=True)
    project = Path.cwd()
    source = source_snapshot(project)
    bundle = EvidenceBundle(root, dict(started_at=utc(), config=vars(args), outputs=outputs, source=source))
    archived = io.BytesIO()
    with tarfile.open(fileobj=archived, mode='w:gz') as archive:
        for name in source['files']:
            archive.add(project / name, arcname=name)
    bundle.write_bytes('source.tar.gz', archived.getvalue())
    pilot = Pilot(session, outputs, bundle, run_id, args)
    pilot.metadata['graphs'] = [1, 2, 3, 4, 5]
    if args.checks_only or args.recovery_only:
        pilot.metadata['graphs'] = []
    elif args.comparisons_only:
        pilot.metadata['graphs'] = [2, 3]
    elif args.graph4_only:
        pilot.metadata['graphs'] = [4]
    elif args.graph5_only:
        pilot.metadata['graphs'] = [5]
    if args.build_manifest:
        build = json.loads(args.build_manifest.read_text())
        project = Path.cwd()
        runtime_files = sorted((project / 'src/rtmsf').rglob('*.py')) + [
            project / name for name in ('src/experiments/__init__.py',
                                       'src/experiments/aws/__init__.py',
                                       'src/experiments/aws/crash_worker.py',
                                       'src/experiments/aws/activity_worker.py')]
        expected = {str(path.relative_to(project)): hashlib.sha256(path.read_bytes()).hexdigest() for path in runtime_files}
        if not isinstance(build.get('files'), dict) or not isinstance(build.get('image'), str) or '@sha256:' not in build['image']:
            raise ValueError('build manifest needs files and an immutable image digest; rebuild using docs/aws.md')
        if build['files'] != expected:
            raise ValueError('runtime files differ from the recorded worker image')
        definition = pilot.clients['ecs'].describe_task_definition(taskDefinition=pilot.service()['taskDefinition'])['taskDefinition']
        container = next(item for item in definition['containerDefinitions'] if item['name'] == 'worker')
        if container['image'] != build['image']:
            raise ValueError('deployed image differs from build manifest')
        bundle.write('image-build.json', build)
    matrix = trial_matrix(args.sizes, args.files, args.graph4_partitions, args.repetitions, args.layout_sizes)
    if args.comparisons_only:
        matrix = [trial for trial in matrix if trial['graph'] == 2]
    elif args.graph4_only:
        matrix = [trial for trial in matrix if trial['graph'] == 4]
    elif args.graph5_only:
        matrix = [trial for trial in matrix if trial['graph'] == 5]
    elif args.checks_only or args.recovery_only:
        matrix = []
    audit = RequestAudit(session, outputs, args.region, root / 'request_audit')
    owned = False
    try:
        pilot.require_worker_stopped()
        owned = True
        if matrix and args.request_source == 'cloudwatch':
            pilot.trial_buckets = TrialBuckets(session, outputs, args.expected_account, args.region, run_id, root)
            pilot.trial_buckets.provision(matrix)
            audit.buckets = list(pilot.trial_buckets.buckets.values())
        session.client('cloudtrail').start_logging(Name=outputs['TrailName'])
        if matrix and not pilot.trial_buckets:
            audit.calibrate()
        if full_pilot or args.checks_only:
            if args.checks_only:
                prepare_checks(pilot, names=args.checks_only)
            else:
                prepare_checks(pilot)
        # Pre-create RTMSF tables so routing is fixed for this execution.
        for repetition in range(1, args.correctness_repetitions + 1):
            if full_pilot:
                pilot.fixture(f'lifecycle_r{repetition}', 'rtmsf')
            if full_pilot or args.recovery_only:
                for case in recovery_cases:
                    pilot.fixture(f'{case}_r{repetition}', 'rtmsf')
        for trial in matrix:
            if trial['method'] == 'rtmsf' or pilot.trial_buckets:
                pilot.fixture(trial['id'], trial['method'], trial['base'], trial['x'] if trial['graph'] == 4 else 1,
                              **({'layout': trial['layout']} if trial.get('layout') else {}))
        # Only seed notifications exist at this point, and the worker is stopped.
        pilot.clients['sqs'].purge_queue(QueueUrl=outputs['EventQueueUrl'])
        pilot.clients['sqs'].purge_queue(QueueUrl=outputs['CrawlerEventQueueUrl'])
        pilot.record('setup_queue_purged', queue=outputs['EventQueueUrl'], wait_seconds=60)
        time.sleep(60)
        if pilot.trial_buckets:
            for trial in matrix:
                if trial['method'] == 'rtmsf':
                    pilot.trial_buckets.enable_notifications(trial['id'])
            ready = max(datetime.fromisoformat(t['metrics_enabled_at']).timestamp()
                        for t in pilot.trial_buckets.state['trials'].values()) + 900
            wait_until(ready, 'CloudWatch activation')
            # Let new selectors propagate before known-request calibration.
            # Each trial's setup verification and quiet minutes exclude these reads.
            audit.calibrate()
        if not args.recovery_only:
            pilot.configure()
            pilot.start_worker()
            pilot.drain('setup')
        for repetition in range(1, args.correctness_repetitions + 1):
            if full_pilot:
                name = f'lifecycle_r{repetition}'
                pilot.attempt(dict(id=name, graph=1, method='rtmsf', repetition=repetition), lambda: pilot.lifecycle_trial(pilot.fixtures[name], repetition))
        for repetition in range(1, args.correctness_repetitions + 1):
            for case in recovery_cases:
                if (full_pilot or args.recovery_only) and time.monotonic() < pilot.deadline:
                    pilot.recovery(case, pilot.fixtures[f'{case}_r{repetition}'], repetition)
        if pilot.recovery_results and not args.recovery_only:
            pilot.stop_worker()
            pilot.configure()
            pilot.start_worker()
        for trial in matrix:
            pilot.attempt(trial, lambda trial=trial: pilot.comparison(trial))
        if full_pilot or args.checks_only:
            pilot.checks.extend(run_checks(pilot, names=args.checks_only) if args.checks_only else run_checks(pilot))
    except BaseException as error:
        pilot.metadata['execution_error'] = f'{type(error).__name__}: {error}'
        pilot.record('execution_error', error=pilot.metadata['execution_error'])
    finally:
        if owned:
            try:
                pilot.stop_worker()
                pilot.metadata['worker_stopped'] = True
            except Exception as error:
                pilot.metadata['worker_stopped'] = False
                pilot.metadata['shutdown_error'] = str(error)
        if pilot.crawler is not None:
            try:
                pilot.crawler.delete()
                pilot.crawler = None
                pilot.metadata['crawler_stopped'] = True
            except Exception as error:
                pilot.metadata['crawler_stopped'] = False
                pilot.metadata['crawler_shutdown_error'] = str(error)
        counts = {}
        try:
            if matrix:
                if pilot.trial_buckets:
                    if pilot.windows:
                        wait_until(time.time() + args.audit_wait_seconds, 'CloudWatch delivery; measured buckets stay idle')
                    quiet = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
                    for window in pilot.windows:
                        window['quiet_until'] = quiet
                    collector = RequestMetrics(session, args.region, root / 'request_metrics')
                    counts = collector.collect(pilot.windows)
                    pilot.metadata['request_coverage'] = collector.coverage
                    audit.collect(pilot.windows, wait_seconds=0)
                    pilot.metadata['supporting_cloudtrail_coverage'] = audit.coverage
                else:
                    counts = audit.collect(pilot.windows, wait_seconds=args.audit_wait_seconds)
                    pilot.metadata['request_coverage'] = audit.coverage
        except BaseException as error:
            pilot.metadata['audit_error'] = f'{type(error).__name__}: {error}'
        # Athena verification reads must start strictly after frozen metric bounds.
        for trial_id in pilot.pending_verifications:
            try:
                pilot.verify(pilot.fixtures[trial_id], trial_id + '_final')
            except Exception as error:
                trial = next(t for t in matrix if t['id'] == trial_id)
                for result in pilot.graph_results:
                    if result.get('trial_id') == trial_id:
                        result.update(status='failed', error=f'Final verification: {type(error).__name__}: {error}')
        if pilot.trial_buckets and pilot.metadata.get('worker_stopped') and pilot.crawler is None:
            try:
                pilot.trial_buckets.cleanup()
                pilot.metadata['trial_buckets_deleted'] = True
            except Exception as error:
                pilot.metadata['bucket_cleanup_error'] = str(error)
        # Emit every planned point even when collection itself failed. Unknowns
        # are blank values; an incomplete count dictionary cannot establish a pass.
        for trial in matrix:
            for metric in METRICS:
                value = counts.get(trial['id'], {}).get(metric)
                pilot.graph_rows.append(graph_row(trial['graph'], trial['method'], metric, trial['x'], value, trial['repetition'], layout=trial.get('layout', '')))
            if trial['graph'] == 2 and not any(r['graph'] == 3 and r['method'] == trial['method'] and r['x_value'] == trial['x'] and r['repetition'] == trial['repetition'] for r in pilot.graph_rows):
                pilot.graph_rows.append(graph_row(3, trial['method'], 'visibility_delay_s', trial['x'], None, trial['repetition']))
        planned = ([dict(id=f'lifecycle_r{repetition}', graph=1, method='rtmsf', repetition=repetition) for repetition in range(1, args.correctness_repetitions + 1)] if full_pilot else []) + matrix
        for trial in planned:
            for graph in ([2, 3] if trial['graph'] == 2 else [trial['graph']]):
                if not any(result['graph'] == graph and result.get('trial_id') == trial['id'] for result in pilot.graph_results):
                    pilot.graph_results.append(dict(graph=graph, trial_id=trial['id'], method=trial['method'], layout=trial.get('layout', ''), x_value=trial.get('x'), repetition=trial['repetition'], status='incomplete', attempted=False,
                                                    error='Not attempted: ' + pilot.metadata.get('execution_error', 'pilot time budget exhausted')))
        for result in pilot.graph_results:
            if result['graph'] in (2, 4, 5) and result['status'] == 'passed':
                trial_id = result['trial_id']
                if any(counts.get(trial_id, {}).get(metric) is None for metric in METRICS):
                    result.update(status='incomplete', error='Request instrumentation unavailable; workload checks passed.')
        if full_pilot or args.recovery_only:
            for case in recovery_cases:
                for repetition in range(1, args.correctness_repetitions + 1):
                    if not any(result['case'] == case and result['repetition'] == repetition for result in pilot.recovery_results):
                        pilot.recovery_results.append(dict(case=case, repetition=repetition, status='incomplete', attempted=False,
                                                           error='Not attempted: ' + pilot.metadata.get('execution_error', 'pilot time budget exhausted')))
        planned_checks = CHECK_NAMES if full_pilot else args.checks_only or ()
        for name in planned_checks:
            for repetition in range(1, args.correctness_repetitions + 1):
                if not any(check['name'] == name and check.get('repetition', 1) == repetition for check in pilot.checks):
                    pilot.checks.append(dict(name=name, repetition=repetition, status='incomplete', attempted=False, error='Not run in this bounded pilot; follow-up validation required.'))
        try:
            if owned:
                pilot.collect_runtime()
        except Exception as error:
            pilot.metadata['collection_error'] = str(error)
        if owned:
            try:
                session.client('cloudtrail').stop_logging(Name=outputs['TrailName'])
                pilot.metadata['trail_stopped'] = True
            except Exception as error:
                pilot.metadata['trail_stopped'] = False
                pilot.metadata['trail_shutdown_error'] = str(error)
        bundle.write('request-windows.json', pilot.windows)
        bundle.write('worker-lifecycle.json', pilot.lifecycle)
        outcomes = pilot.graph_results + pilot.recovery_results + pilot.checks
        complete = (bool(outcomes)
                    and all(result.get('status') == 'passed' and result.get('attempted', True) for result in outcomes)
                    and not any(key.endswith('_error') for key in pilot.metadata))
        pilot.metadata['run_status'] = 'passed' if complete else 'incomplete_or_failed'
        write_reports(root, pilot.graph_rows, pilot.recovery_results, pilot.checks, pilot.metadata)
        hashes = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
        bundle.write('artifact-hashes.json', hashes)
        print(f'{pilot.metadata["run_status"]}: {root.resolve()}', flush=True)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
