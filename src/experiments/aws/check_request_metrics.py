"""Validate CloudWatch S3 counters with known requests in isolated trial buckets.

Run with --help. No worker, crawler, billing export or existing bucket is needed.
Only buckets created by this check are cleaned up; raw observations are retained.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import tarfile
import uuid

from experiments.aws.request_metrics import RequestMetrics, METRICS, wait_until


def utc():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + '\n')


def cleanup(s3, trials):
    for trial in trials:
        if trial.get('cleaned_up'):
            continue
        try:
            s3.delete_bucket_metrics_configuration(Bucket=trial['bucket'], Id='trial')
            for key in ('fixture/one', 'fixture/two'):
                s3.delete_object(Bucket=trial['bucket'], Key=key)
            s3.delete_bucket(Bucket=trial['bucket'])
            trial['cleaned_up'] = True
        except Exception as error:
            if getattr(error, 'response', {}).get('Error', {}).get('Code') == 'NoSuchBucket':
                trial['cleaned_up'] = True
            else:
                trial['cleanup_error'] = type(error).__name__
        if trial.get('cleaned_up'):
            trial.pop('cleanup_error', None)


def issue_requests(s3, trial, root):
    """No SDK retries: each successful response is one known request."""
    record_path = root / f'{trial["bucket"]}.json'
    if record_path.exists():
        raise ValueError('request record already exists; do not repeat an uncertain trial')
    records = []
    window = dict(id=trial['bucket'], bucket=trial['bucket'], filter_id='trial', started_at=utc())
    save(record_path, {'trial': trial, 'started_at': window['started_at'], 'requests': []})
    token = None
    try:
        for index in range(trial['count']):
            head = 'head_object' if trial['repetition'] % 2 else 'head_bucket'
            listing = 'list_objects_v2' if trial['repetition'] % 2 else 'list_objects'
            params = {'Bucket': trial['bucket'], 'Prefix': 'fixture/', 'MaxKeys': 1}
            if token:
                params['ContinuationToken' if listing.endswith('_v2') else 'Marker'] = token
            calls = [(head, {'Bucket': trial['bucket']} | ({'Key': 'fixture/one'} if head == 'head_object' else {})),
                     ('get_object', {'Bucket': trial['bucket'], 'Key': 'fixture/one'}), (listing, params)]
            if trial.get('list_only'):
                calls = calls[-1:]
            for operation, arguments in calls:
                started = utc()
                response = getattr(s3, operation)(**arguments)
                metadata = response['ResponseMetadata']
                records.append(dict(operation=operation, started_at=started, ended_at=utc(),
                                    request_id=metadata['RequestId'], status=metadata['HTTPStatusCode'],
                                    retries=metadata.get('RetryAttempts', 0)))
                if operation == 'get_object':
                    try:
                        response['Body'].read()
                    finally:
                        response['Body'].close()
                if metadata['HTTPStatusCode'] != 200 or metadata.get('RetryAttempts', 0):
                    raise ValueError('known-request check requires successful requests without retries')
                if operation.startswith('list_objects'):
                    token = (response.get('NextContinuationToken') if listing.endswith('_v2') else 'fixture/one') if response.get('IsTruncated') else None
        trial['requests_succeeded'] = True
    except Exception as error:
        trial['request_error'] = type(error).__name__
        trial['requests_succeeded'] = False
    finally:
        window['ended_at'] = utc()
        trial['window'] = window
        save(record_path, {'trial': trial, 'requests': records})
    return window


def main(argv=None):
    import boto3
    from botocore.config import Config
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile')
    parser.add_argument('--region', default='us-west-2')
    parser.add_argument('--expected-account', required=True)
    parser.add_argument('--output-dir', type=Path, default=Path('runs'))
    parser.add_argument('--resume', type=Path, help='Resume this check\'s provisioned manifest; never resend recorded trials')
    parser.add_argument('--cleanup-only', action='store_true', help='Clean up --resume buckets without issuing requests')
    parser.add_argument('--list-only', action='store_true', help='Check LIST counts and whether unused HEAD/GET counters report zero')
    parser.add_argument('--warmup-seconds', type=int, default=900)
    parser.add_argument('--collection-seconds', type=int, default=600)
    args = parser.parse_args(argv)
    if args.warmup_seconds < 900 or args.collection_seconds < 60:
        parser.error('allow at least 900 seconds for activation and 60 for collection')
    if args.cleanup_only and not args.resume:
        parser.error('--cleanup-only requires --resume')
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    config = Config(connect_timeout=3, read_timeout=15, retries={'total_max_attempts': 1})
    account = session.client('sts', config=config).get_caller_identity()['Account']
    if account != args.expected_account:
        raise ValueError('AWS account differs from --expected-account')
    s3 = session.client('s3', config=config)
    suffix = datetime.now(timezone.utc).strftime('%m%d%H%M') + '-' + uuid.uuid4().hex[:6]
    root = args.resume or args.output_dir / ('cwcheck_' + suffix)
    root.mkdir(parents=True, exist_ok=bool(args.resume))
    path = root / 'manifest.json'
    manifest = json.loads(path.read_text()) if args.resume else dict(author='Ankit Joshi', account=account,
                region=args.region, created_at=utc(), trials=[])
    if manifest['account'] != account or manifest['region'] != args.region:
        raise ValueError('resume manifest belongs to another AWS account or region')
    for trial in manifest['trials']:
        if not trial['bucket'].startswith('rtmsf-cw-check-') or not trial.get('created'):
            raise ValueError('refusing an unowned bucket')
    if args.cleanup_only:
        cleanup(s3, manifest['trials'])
        manifest['cleanup_at'] = utc()
        save(root / f'cleanup-{uuid.uuid4().hex}.json', manifest)
        return 0 if all(t.get('cleaned_up') for t in manifest['trials']) else 1
    for trial in manifest['trials']:
        if trial.get('cleaned_up'):
            raise ValueError('completed check cannot be resumed')
        record_path = root / f'{trial["bucket"]}.json'
        if record_path.exists():
            recorded = json.loads(record_path.read_text())['trial']
            if not recorded.get('window'):
                raise ValueError('interrupted trial has uncertain request counts; retain it and start a fresh check')
            trial.update(recorded)
    manifest['settings'] = dict(warmup_seconds=args.warmup_seconds, collection_seconds=args.collection_seconds)
    if not (root / 'source.tar.gz').exists():
        project = Path.cwd()
        names = ('src/experiments/__init__.py', 'src/experiments/aws/__init__.py',
                 'src/experiments/aws/request_metrics.py', 'src/experiments/aws/check_request_metrics.py',
                 'pyproject.toml', 'uv.lock')
        with tarfile.open(root / 'source.tar.gz', 'w:gz') as archive:
            for name in names:
                archive.add(project / name, arcname=name)
        save(root / 'source-files.json', {name: hashlib.sha256((project / name).read_bytes()).hexdigest() for name in names})
    save(path, manifest)
    try:
        if not args.resume:
            for count in (1, 10, 100):
                for repetition in (1, 2):
                    bucket = f'rtmsf-cw-check-{"list-" if args.list_only else ""}n{count}-r{repetition}-{suffix}'
                    s3.create_bucket(Bucket=bucket, **({'CreateBucketConfiguration': {'LocationConstraint': args.region}} if args.region != 'us-east-1' else {}))
                    trial = dict(bucket=bucket, count=count, repetition=repetition, filter_id='trial', created=True, list_only=args.list_only)
                    manifest['trials'].append(trial)
                    save(path, manifest)
                    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={name: True for name in ('BlockPublicAcls', 'IgnorePublicAcls', 'BlockPublicPolicy', 'RestrictPublicBuckets')})
                    for key in ('fixture/one', 'fixture/two'):
                        s3.put_object(Bucket=bucket, Key=key, Body=b'request metrics check\n')
                    s3.put_bucket_metrics_configuration(Bucket=bucket, Id='trial', MetricsConfiguration={'Id': 'trial'})
                    trial['metrics_enabled_at'] = utc()
                    save(path, manifest)
        ready = max(datetime.fromisoformat(t['metrics_enabled_at']).timestamp() for t in manifest['trials']) + args.warmup_seconds
        wait_until(ready, 'CloudWatch activation')
        # Start after a full quiet minute; setup traffic stays outside the window.
        if not all(t.get('window') for t in manifest['trials']):
            wait_until((int(time.time()) // 60 + 2) * 60, 'Quiet minute before requests')
            pending = [t for t in manifest['trials'] if not t.get('window')]
            with ThreadPoolExecutor(max_workers=6) as pool:
                list(pool.map(lambda t: issue_requests(s3, t, root), pending))
            save(path, manifest)
        collector = RequestMetrics(session, args.region, root / 'request_metrics')
        windows = [t['window'] for t in manifest['trials']]
        # CloudWatch does not finalize a minute immediately. Retain every snapshot.
        wait_until((int(time.time()) // 60 + 1) * 60, 'Measurement minute')
        deadline = time.time() + args.collection_seconds
        while True:
            quiet = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
            counts = collector.collect([window | {'quiet_until': quiet} for window in windows])
            available = sum(v is not None for values in counts.values() for v in values.values())
            print(f'CloudWatch values available: {available}/{len(windows) * 3}', flush=True)
            if time.time() >= deadline:
                break
            time.sleep(min(60, max(0, deadline - time.time())))
        rows = []
        for trial in manifest['trials']:
            for metric in METRICS:
                value = counts[trial['bucket']][metric]
                expected = 0 if trial.get('list_only') and metric != 's3_list_requests' else trial['count']
                status = 'passed' if trial.get('requests_succeeded') and value == expected else 'incomplete' if value is None else 'failed'
                rows.append(dict(requests=trial['count'], repetition=trial['repetition'], measurement=metric,
                                 expected=expected, observed=value, status=status, bucket=trial['bucket']))
        with (root / 'calibration.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        manifest['passed'] = all(r['status'] == 'passed' for r in rows)
        print(f'Known-request validation passed: {manifest["passed"]}', flush=True)
    finally:
        cleanup(s3, manifest['trials'])
        manifest['finished_at'] = utc()
        save(path, manifest)
        save(root / 'artifact-hashes.json', {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in root.rglob('*') if p.is_file() and p.name != 'artifact-hashes.json'})
        print(f'Evidence: {root.resolve()}', flush=True)
    return 0 if manifest.get('passed') and all(t.get('cleaned_up') for t in manifest['trials']) else 1


if __name__ == '__main__':
    raise SystemExit(main())
