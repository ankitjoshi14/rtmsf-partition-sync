"""Fresh experiment fixtures and workloads for five graphs and crash recovery."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from threading import Event, Thread
import time

from experiments.aws.deploy import service_tasks
from experiments.aws.runtime import AwsRuntime, assert_crash_trace, parquet_payload
from experiments.evidence import utc
from experiments.aws.request_metrics import wait_until
from experiments.aws.crawler import EventCrawler
from experiments.figures import graph_row


METHODS = ('rtmsf', 'glue_event_crawler', 'athena_msck')
BUCKETS = dict(zip(METHODS, ('DataBucketName', 'CrawlerBucketName', 'MsckBucketName')))
PARTITION_KEYS = {'hierarchical': ('dt', 'site', 'hour'),
                  'day_first': ('day', 'tenant'), 'tenant_first': ('tenant', 'day')}


def fixture_table_name(name, method, run_id, base=0, files=1, layout=None):
    """Readable AWS names; internal trial IDs stay stable in evidence records."""
    prefix = {'rtmsf': 'rtmsf', 'glue_event_crawler': 'crawler', 'athena_msck': 'msck'}[method]
    repetition = name.rsplit('_', 1)[-1]
    if name.startswith('g2_'):
        label = f'scale_p{base}_{repetition}'
    elif name.startswith('g4_'):
        label = f'files_p{base}_f{files}_{repetition}'
    elif name.startswith('g5_'):
        label = f'layout_{layout}_p{base}_{repetition}'
    elif name.startswith(('add_', 'drop_')):
        label = f'recovery_{name}'
    else:
        label = name
    return f'{prefix}_{label}_{run_id.removeprefix("run_")}'


def partition_values(index):
    return (str(date(2026, 1, 1) + timedelta(days=index // 2400)),
            f'site-{index // 24 % 100:03d}', f'{index % 24:02d}')


def partition_path(values):
    return '/'.join(f'{key}={value}' for key, value in zip(('dt', 'site', 'hour'), values)) + '/'


def replay_verified(marker, telemetry, successor_id):
    records = [item['record'] for item in telemetry if item['log_stream'].endswith('/' + successor_id)]
    pending = set(marker.get('pending_message_ids', []))
    received = set()
    for item in records:
        if item.get('action') != 'message_received':
            continue
        try:
            count = int(item.get('receive_count') or 0)
        except (TypeError, ValueError):
            continue
        if count > 1:
            received.add(item.get('message_id'))
    acknowledged = {item.get('message_id') for item in records if item.get('action') == 'message_acknowledged'}
    return bool(pending & received & acknowledged)


class Pilot(AwsRuntime):
    """Run the agreed workloads using shared AWS operations and evidence."""

    def __init__(self, session, outputs, bundle, run_id, args):
        super().__init__(session, outputs, bundle, run_id, args.timeout_seconds)
        self.args = args
        self.fixtures = {}
        self.graph_rows, self.graph_results, self.recovery_results, self.checks, self.windows = [], [], [], [], []
        self.crawler = None
        self.trial_buckets = None
        self.pending_verifications = []
        self.changes = []
        self.deadline = time.monotonic() + args.max_minutes * 60
        self.metadata = dict(config=vars(args) | {'output_dir': str(args.output_dir)},
                             repetitions=args.repetitions, correctness_repetitions=args.correctness_repetitions,
                             graph4_partitions=args.graph4_partitions,
                             data_kind='aws_pilot', graph_results=self.graph_results,
                             request_count_semantics='Observed request counts; delivery completeness is not guaranteed.')

    def values(self, index, layout=None):
        layout = layout or 'hierarchical'
        if layout in ('day_first', 'tenant_first'):
            values = {'day': '2026-09-25', 'tenant': f't{index:06d}'}
            return tuple(values[key] for key in PARTITION_KEYS[layout])
        return partition_values(index)

    def path(self, values, layout=None):
        keys = PARTITION_KEYS[layout or 'hierarchical']
        return '/'.join(f'{key}={value}' for key, value in zip(keys, values, strict=True)) + '/'

    def fixture(self, name, method, base=0, files=1, layout=None):
        layout = layout or 'hierarchical'
        table_name = fixture_table_name(name, method, self.run_id, base, files, layout)
        table = self.clients['glue'].get_table(DatabaseName=self.outputs['GlueDatabaseName'], Name='rtmsf')['Table']
        table = {key: deepcopy(table[key]) for key in ('TableType', 'Parameters', 'PartitionKeys', 'StorageDescriptor')}
        # Glue's crawler can probe the root directory's basename even for an
        # existing catalog target. Keep that name equal to the existing table.
        root = f'fixture/{self.run_id}/{table_name}/'
        bucket = (self.trial_buckets.buckets.get(name) if self.trial_buckets else None) or self.outputs[BUCKETS[method]]
        table['Name'] = table_name
        table['PartitionKeys'] = [{'Name': key, 'Type': 'string'} for key in PARTITION_KEYS[layout]]
        table['StorageDescriptor']['Location'] = f's3://{bucket}/{root}'
        self.clients['glue'].create_table(DatabaseName=self.outputs['GlueDatabaseName'], TableInput=table)
        if self.trial_buckets and name in self.trial_buckets.state['trials']:
            self.trial_buckets.state['trials'][name]['table'] = table_name
            self.trial_buckets._save()
        fixture = dict(name=name, method=method, layout=layout, table=table, bucket=bucket, root=root, objects={}, partitions={})
        self.fixtures[name] = fixture
        for index in range(base):
            values = self.values(index, layout)
            sd = deepcopy(table['StorageDescriptor'])
            sd['Location'] += self.path(values, layout)
            fixture['partitions'][values] = {'Values': list(values), 'StorageDescriptor': sd}
        jobs = [(self.values(i, layout), i) for i in range(base)]
        jobs += [(self.values(0, layout), base + i) for i in range(max(0, files - 1))]
        with ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(lambda job: self.upload(fixture, *job, setup=True), jobs))
        parts = list(fixture['partitions'].values())
        for offset in range(0, len(parts), 100):
            response = self.clients['glue'].batch_create_partition(DatabaseName=self.outputs['GlueDatabaseName'], TableName=table_name, PartitionInputList=parts[offset:offset + 100])
            if response.get('Errors'):
                raise RuntimeError(f'fixture registration failed: {response["Errors"]}')
        columns = [column['Name'] for column in table['PartitionKeys']]
        layout_info = dict(partition_columns=columns, starting_partitions=base,
                      occupied_prefixes=[len({values[:depth] for values in fixture['partitions']})
                                         for depth in range(1, len(columns) + 1)])
        if base:
            self.metadata.setdefault('fixture_layouts', {})[name] = layout_info
        self.record('fixture_ready', name=name, table=table, base_partitions=base, existing_files=files, layout=layout_info)
        print(f'Fixture ready: {name} ({base} registered partitions)', flush=True)
        return fixture

    def upload(self, fixture, values, record_id, *, setup=False, retry_write=False):
        from botocore.exceptions import ClientError, ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError

        payload = parquet_payload(record_id)
        path = self.path(values, fixture.get('layout'))
        key = fixture['root'] + path + f'part-{record_id:06d}.parquet'
        if retry_write and (setup or key in fixture['objects']):
            raise ValueError('cannot retry an already recorded object or setup upload')
        started = utc()
        verification_request_id = None
        for attempt in range(3 if setup else 1):
            try:
                # Activity retries repeat the same owned key and payload after an
                # uncertain response, without a measurement-contaminating GET.
                response = self.clients['s3'].put_object(Bucket=fixture['bucket'], Key=key, Body=payload,
                                                        **({} if retry_write else {'IfNoneMatch': '*'}))
                break
            except (ClientError, ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError) as error:
                code = error.response.get('Error', {}).get('Code') if isinstance(error, ClientError) else None
                if setup and attempt > 0 and code == 'PreconditionFailed':
                    existing = self.clients['s3'].get_object(Bucket=fixture['bucket'], Key=key)
                    with existing['Body'] as stream:
                        if stream.read() != payload:
                            raise ValueError('recovered seed object contains different bytes') from error
                    verification_request_id = existing['ResponseMetadata']['RequestId']
                    response = {'ResponseMetadata': {'RequestId': None}}
                    break
                if (not setup or attempt == 2 or (code is not None and code not in
                        {'SlowDown', 'RequestTimeout', 'InternalError', 'ServiceUnavailable', '500', '503'})):
                    raise
                self.record('fixture_upload_retry', fixture=fixture['name'], key=key,
                            attempt=attempt + 1, error=type(error).__name__)
                time.sleep(0.25 * 2 ** attempt)
        completed, mono = utc(), time.monotonic()
        fixture['objects'][key] = [str(record_id), str(record_id * 10), *values]
        self.changes.append(dict(fixture=fixture['name'], at=completed, key_sha256=hashlib.sha256(key.encode()).hexdigest(), operation='CREATE', partition=path.rstrip('/')))
        self.record('upload', fixture=fixture['name'], key=key, started_at=started, completed_at=completed,
                    request_id=response['ResponseMetadata']['RequestId'], verification_request_id=verification_request_id,
                    sha256=hashlib.sha256(payload).hexdigest())
        return key, completed, mono

    def remove(self, fixture, key):
        if key not in fixture['objects'] or not key.startswith(fixture['root']):
            raise ValueError('only this trial\'s own objects may be deleted')
        values = fixture['objects'][key][2:]
        self.clients['s3'].delete_object(Bucket=fixture['bucket'], Key=key)
        self.changes.append(dict(fixture=fixture['name'], at=utc(), key_sha256=hashlib.sha256(key.encode()).hexdigest(), operation='DELETE', partition=self.path(values, fixture.get('layout')).rstrip('/')))
        del fixture['objects'][key]
        return self.record('delete', fixture=fixture['name'], key=key)

    def presence(self, fixture, values):
        started = utc()
        try:
            response = self.clients['glue'].get_partition(DatabaseName=self.outputs['GlueDatabaseName'], TableName=fixture['table']['Name'], PartitionValues=list(values))
            present = True
            detail = response['Partition']
        except self.clients['glue'].exceptions.EntityNotFoundException:
            present, detail = False, None
        except Exception as error:
            present, detail = None, str(error)
        self.record('catalog_observation', fixture=fixture['name'], values=values, started_at=started,
                    present=present, detail=detail)
        return present

    def wait_presence(self, fixture, values, expected):
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            if self.presence(fixture, values) is expected:
                return time.monotonic()
            time.sleep(1)
        raise TimeoutError(f'{fixture["name"]}: catalog never observed {expected}')

    def verify(self, fixture, label):
        expected_values = sorted({tuple(row[2:]) for row in fixture['objects'].values()})
        parts = []
        for page in self.clients['glue'].get_paginator('get_partitions').paginate(DatabaseName=self.outputs['GlueDatabaseName'], TableName=fixture['table']['Name']):
            parts.extend(page.get('Partitions', []))
        expected = [{'Values': list(values), 'Location': fixture['table']['StorageDescriptor']['Location'] + self.path(values, fixture.get('layout')),
                     'Columns': fixture['table']['StorageDescriptor']['Columns']} for values in expected_values]
        observed = [{'Values': part['Values'], 'Location': part['StorageDescriptor'].get('Location'),
                     'Columns': part['StorageDescriptor'].get('Columns')} for part in parts]
        self.bundle.write(f'catalog/{label}.json', {'expected': expected, 'observed': observed})
        if sorted(tuple(part['Values']) for part in parts) != expected_values:
            raise AssertionError(f'{label}: complete catalog membership differs')
        for part in parts:
            descriptor = part['StorageDescriptor']
            expected_location = fixture['table']['StorageDescriptor']['Location'] + self.path(part['Values'], fixture.get('layout'))
            if descriptor['Location'].rstrip('/') != expected_location.rstrip('/') or descriptor['Columns'] != fixture['table']['StorageDescriptor']['Columns']:
                raise AssertionError(f'{label}: partition location or column schema differs')
        keys = ', '.join(key['Name'] for key in fixture['table']['PartitionKeys'])
        rows, _ = self.query(f'SELECT record_id, amount, {keys} FROM "{fixture["table"]["Name"]}" ORDER BY record_id', label)
        if sorted(rows[1:]) != sorted(fixture['objects'].values()):
            raise AssertionError(f'{label}: exact query rows differ')
        self.record('verified', fixture=fixture['name'], label=label, partitions=len(parts), rows=len(rows) - 1)

    def configure(self, fault=None, *, worker_settings=None):
        self.require_worker_stopped()
        ecs = self.clients['ecs']
        original = ecs.describe_task_definition(taskDefinition=self.service()['taskDefinition'])['taskDefinition']
        fields = ('family', 'taskRoleArn', 'executionRoleArn', 'networkMode', 'containerDefinitions', 'volumes', 'placementConstraints', 'requiresCompatibilities', 'cpu', 'memory', 'runtimePlatform', 'ephemeralStorage')
        task = {key: deepcopy(original[key]) for key in fields if key in original}
        container = next(item for item in task['containerDefinitions'] if item['name'] == 'worker')
        if not container['image'].startswith(self.outputs['EcrRepositoryUri'] + '@sha256:'):
            raise ValueError('pilot requires an immutable image from this stack')
        env = {item['name']: item['value'] for item in container['environment'] if not item['name'].startswith('RTMSF_EXPERIMENT')}
        env.update(SETTLING_MS='10000', MAXIMUM_MS='31000', QUEUE_VISIBILITY_SECONDS='180')
        env.update(worker_settings or {})
        env['RTMSF_TABLE_IDS'] = json.dumps([self.outputs['GlueDatabaseName'] + '.' + f['table']['Name'] for f in self.fixtures.values() if f['method'] == 'rtmsf'])
        sources = {'fixture': self.outputs['DataBucketName']}
        sources.update({name: f['bucket'] for name, f in self.fixtures.items()
                        if f['method'] == 'rtmsf' and f['bucket'] != self.outputs['DataBucketName']})
        env['RTMSF_SOURCES'] = json.dumps(sources)
        if fault:
            env['RTMSF_EXPERIMENT_FAULT'] = json.dumps(fault)
        container['environment'] = [{'name': key, 'value': value} for key, value in sorted(env.items())]
        container['entryPoint'] = ['python', '-m', 'experiments.aws.crash_worker' if fault else 'rtmsf.aws_app']
        registered = ecs.register_task_definition(**task)['taskDefinition']
        self.task_definition = registered['taskDefinitionArn']
        ecs.update_service(cluster=self.outputs['ClusterName'], service=self.outputs['ServiceName'], taskDefinition=self.task_definition, desiredCount=0)
        self.record('worker_configuration', task_definition=registered)
        self.wait_worker(False, configured=True)

    def wait_processed(self, fixture, since, *, expected_changes=None):
        """Match this action's exact files to decoded messages and their ACKs."""
        deadline = time.monotonic() + self.timeout
        table_id = self.outputs['GlueDatabaseName'] + '.' + fixture['table']['Name']
        changes = (list(expected_changes) if expected_changes is not None else
                   [change for change in self.changes if change['fixture'] == fixture['name'] and change['at'] >= since])
        if not changes and expected_changes is None:
            # Supporting checks can replay a captured CREATE without uploading.
            changes = [dict(key_sha256=hashlib.sha256(key.encode()).hexdigest(), operation='CREATE', partition=self.path(row[2:], fixture.get('layout')).rstrip('/')) for key, row in fixture['objects'].items()]
        expected = {(change['key_sha256'], change['operation']) for change in changes}
        partitions = {change['partition'] for change in changes}
        partition_by_change = {(change['key_sha256'], change['operation']): change['partition'] for change in changes}
        if not expected:
            raise ValueError('processing check requires exact changed files')
        while time.monotonic() < deadline:
            records = [entry['record'] for entry in self.telemetry()
                       if entry['record'].get('timestamp', '') >= since]
            decoded = [r for r in records if r.get('action') == 'message_decoded'
                       and any((event.get('key_sha256'), event.get('operation')) in expected for event in r.get('events', []))]
            received = {r['message_id'] for r in decoded}
            observed = {(event.get('key_sha256'), event.get('operation')) for r in decoded for event in r['events']}
            acked = {r.get('message_id') for r in records if r.get('action') == 'message_acknowledged'}
            dead = {r.get('message_id') for r in records if r.get('action') == 'message_dead_lettered'}
            if received & dead:
                raise AssertionError('a measured file event was dead-lettered')
            latest_receipt = dict.fromkeys(partitions, since)
            for record in decoded:
                for event in record['events']:
                    partition = partition_by_change.get((event.get('key_sha256'), event.get('operation')))
                    if partition is not None:
                        latest_receipt[partition] = max(latest_receipt[partition], record['timestamp'])
            processed = {r.get('partition') for r in records if r.get('action') == 'pmcp_result'
                         and r.get('table_id') == table_id and r.get('status') in ('APPLIED', 'NOOP')
                         and r.get('timestamp', '') >= latest_receipt.get(r.get('partition'), since)}
            if expected <= observed and partitions <= processed and received and received <= acked:
                self.record('processing_confirmed', fixture=fixture['name'], since=since,
                            message_ids=sorted(received), changed_files=sorted(expected), partitions=sorted(partitions))
                return
            time.sleep(3)
        raise TimeoutError('target processing and message acknowledgement not confirmed')

    def lifecycle_trial(self, fixture, repetition):
        values = partition_values(0)
        stop = Event()
        baseline_ready = Event()
        observed = []
        def watch():
            while not stop.is_set():
                value = self.presence(fixture, values)
                observed.append((time.monotonic(), value))
                baseline_ready.set()
                stop.wait(1)
        observer = Thread(target=watch, daemon=True)
        observer.start()
        origin = None
        expected = []
        try:
            if not baseline_ready.wait(timeout=self.timeout):
                raise TimeoutError('pre-upload baseline observation did not complete')
            stage_started = utc()
            key, _, origin = self.upload(fixture, values, 0)
            expected.extend([(observed[0][0], 0, ''), (origin, 1, 'Upload first file: 1 file')])
            keys = [key]
            for stage in range(4):
                if stage:
                    stage_started = utc()
                if stage == 1:
                    keys.extend(self.upload(fixture, values, i)[0] for i in range(1, 5))
                    expected.append((time.monotonic(), 1, 'Append four: 5 files'))
                elif stage == 2:
                    self.remove(fixture, keys.pop(0))
                    expected.append((time.monotonic(), 1, 'Delete one: 4 files'))
                elif stage == 3:
                    for key in keys:
                        self.remove(fixture, key)
                    expected.append((time.monotonic(), 0, 'Delete remaining four: empty'))
                self.wait_presence(fixture, values, stage != 3)
                self.wait_processed(fixture, stage_started)
                self.drain(f'{fixture["name"]}_stage{stage}')
                self.verify(fixture, f'{fixture["name"]}_stage{stage}')
        finally:
            stop.set()
            observer.join(timeout=20)
            if origin is not None:
                expected.append((time.monotonic(), int(bool(fixture['objects'])), ''))
                self.graph_rows.extend(graph_row(1, 'rtmsf', 'expected_partition_presence', t - origin, value, repetition, label) for t, value, label in expected)
                self.graph_rows.extend(graph_row(1, 'rtmsf', 'observed_partition_presence', t - origin, value, repetition) for t, value in observed)

    def comparison(self, trial):
        fixture = self.fixtures.get(trial['id'])
        if fixture is None:
            fixture = self.fixture(trial['id'], trial['method'], trial['base'], trial['x'] if trial['graph'] == 4 else 1,
                                   layout=trial.get('layout'))
        method = trial['method']
        isolated = getattr(self, 'trial_buckets', None) is not None
        self.verify(fixture, trial['id'] + '_baseline')
        if method == 'glue_event_crawler':
            crawler_outputs = self.outputs | {'CrawlerBucketName': fixture['bucket']}
            if isolated:
                self.trial_buckets.enable_notifications(trial['id'])
                crawler_outputs['CrawlerEventQueueUrl'] = self.trial_buckets.queue_url(trial['id'])
            self.crawler = EventCrawler(self.clients['glue'], self.clients['sqs'], self.clients['logs'],
                                        crawler_outputs, self.bundle.root / 'crawler')
            self.crawler.initialize(fixture['table']['Name'], trial['id'], timeout_seconds=self.timeout)
        elif method == 'rtmsf':
            self.drain(trial['id'] + '_before')
        # CloudTrail timestamps have second precision. Separate surrounding
        # query reads from the conservatively rounded maintenance window.
        if isolated:
            # AWS assigned setup reads to the second following minute in a real
            # trial. Exclude both complete minutes; delivery remains best effort.
            ready = (int(time.time()) // 60 + 3) * 60
            self.record('setup_quiet_period', trial=trial['id'], setup_completed_at=utc(),
                        measurement_not_before=datetime.fromtimestamp(ready, timezone.utc).isoformat(),
                        complete_quiet_minutes=2)
            wait_until(ready, 'Two quiet minutes after setup reads')
        else:
            time.sleep(1.1)
        index = trial['base'] if trial['graph'] in (2, 5) else 0
        values = self.values(index, trial['layout']) if trial.get('layout') else self.values(index)
        if trial['graph'] == 5 and self.presence(fixture, values) is not False:
            raise AssertionError('Graph 5 requires an absent new partition before upload')
        action_started = utc()
        uploaded_key, uploaded_at, upload_mono = self.upload(fixture, values, 1000000)
        # Observer starts before either managed-service submission, and runs
        # concurrently with its start API and entire execution.
        first = []
        stop = Event()
        def observe():
            while not stop.is_set():
                if self.presence(fixture, values) is True:
                    first.append(time.monotonic())
                    return
                stop.wait(1)
        observer = Thread(target=observe, daemon=True)
        trigger, start = uploaded_at, upload_mono
        trigger_known = method == 'rtmsf'
        if method == 'glue_event_crawler':
            self.crawler.notification_ready(timeout_seconds=120, expected_key=uploaded_key)
        if method != 'rtmsf':
            trigger, start = utc(), time.monotonic()
        if trial['graph'] == 2:
            observer.start()
        window = dict(id=trial['id'], bucket=fixture['bucket'], started_at=trigger,
                      graph=trial['graph'], method=method, x=trial['x'], repetition=trial['repetition'], layout=trial.get('layout', ''))
        if method == 'rtmsf':
            window['requester_arn'] = self.outputs['TaskRoleArn']
        elif method == 'glue_event_crawler':
            window['requester_arn'] = self.outputs['CrawlerRoleArn']
        try:
            if method == 'athena_msck':
                self.last_query_start = None
                try:
                    _, query_record = self.query(f'MSCK REPAIR TABLE `{fixture["table"]["Name"]}`', trial['id'] + '_msck')
                finally:
                    if self.last_query_start is not None:
                        trigger = self.last_query_start['started_at']
                        start = self.last_query_start['started_monotonic_seconds']
                        window['started_at'] = trigger
                        trigger_known = True
                trigger, start = query_record['started_at'], query_record['started_monotonic_seconds']
                window['started_at'] = trigger
                trigger_known = True
            elif method == 'glue_event_crawler':
                trigger = self.crawler.start()
                start = self.crawler.started_monotonic
                window['started_at'] = trigger
                trigger_known = True
                evidence = self.crawler.completion(timeout_seconds=self.timeout)
                self.record('crawler_completion', trial=trial['id'], evidence=evidence)
            else:
                if trial['graph'] in (2, 5):
                    self.wait_presence(fixture, values, True)
                self.wait_processed(fixture, action_started)
                self.drain(trial['id'] + '_after')
            window['ended_at'] = utc()
            self.windows.append(window)
            if trial['graph'] == 5 and self.presence(fixture, values) is not True:
                raise AssertionError('Graph 5 new partition was not registered')
            if trial['graph'] == 2:
                observer.join(timeout=self.timeout)
                if not first:
                    raise TimeoutError('no independent observation of new partition')
            if isolated:
                self.pending_verifications.append(trial['id'])
            else:
                time.sleep(1.1)
                self.verify(fixture, trial['id'] + '_final')
        finally:
            stop.set()
            if observer.is_alive():
                observer.join(timeout=20)
            if trial['graph'] == 2 and first and trigger_known:
                self.graph_rows.append(graph_row(3, method, 'visibility_delay_s', trial['x'], first[0] - start, trial['repetition']))
                self.record('visibility', trial=trial['id'], upload_completed_at=uploaded_at,
                            trigger_at=trigger, visibility_delay_s=first[0] - start)
            if 'ended_at' not in window:
                window['ended_at'] = utc()
                window['incomplete'] = True
                self.windows.append(window)
            if isolated:
                # Notifications may precede the upload response. The bucket has
                # been idle since setup; include the entire triggering PUT minute.
                window['trigger_at'] = trigger
                window['started_at'] = action_started
            if self.crawler:
                self.crawler.delete()
                self.crawler = None

    def recovery(self, case, fixture, repetition):
        from botocore.exceptions import ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError

        operation, point = case.split('_', 1)
        values = partition_values(0)
        target_path = partition_path(values).rstrip('/')
        table_id = self.outputs['GlueDatabaseName'] + '.' + fixture['table']['Name']
        marker_key = f'_experiment/crash/{self.run_id}_{fixture["name"]}.json'
        outcome = dict(case=case, repetition=repetition, expected_present=operation == 'add', observed_present=None,
                       crash_verified=False, replay_ack_verified=False, status='incomplete')
        self.recovery_results.append(outcome)
        self.stop_worker()
        self.configure(dict(operation=operation.upper(), point=point, partition_path=target_path,
                            table_id=table_id,
                            marker_bucket=fixture['bucket'], marker_key=marker_key))
        started = time.time()
        self.start_worker()
        try:
            key, _, _ = self.upload(fixture, values, 0)
            for phase, present in (('add', True), ('drop', False)):
                if phase == 'drop':
                    self.remove(fixture, key)
                if phase == operation:
                    deadline = time.monotonic() + self.timeout
                    marker, successor, trace = None, None, None
                    while time.monotonic() < deadline:
                        try:
                            if marker is None:
                                try:
                                    marker = json.loads(self.clients['s3'].get_object(Bucket=fixture['bucket'], Key=marker_key)['Body'].read())
                                except self.clients['s3'].exceptions.NoSuchKey:
                                    pass
                            tasks = service_tasks(self.clients['ecs'], self.outputs['ClusterName'], self.outputs['ServiceName'])
                            selected = [t for t in tasks if t['createdAt'].timestamp() >= started]
                            crashed = [t for t in selected if any(c.get('name') == 'worker' and c.get('exitCode') == 86 for c in t.get('containers', []))]
                            running = [t for t in selected if t['lastStatus'] == 'RUNNING']
                            if marker and len(crashed) == 1 and len(running) == 1:
                                assert_crash_trace(crashed, running)
                                outcome['crash_verified'] = (
                                    marker.get('point') == point and marker.get('operation') == operation.upper()
                                    and marker.get('partition_path') == target_path and marker.get('table_id') == table_id
                                    and marker.get('task_id') == crashed[0]['taskArn']
                                )
                                successor = running[0]['taskArn'].split('/')[-1]
                                trace = {'marker': marker, 'crashed': crashed, 'successor': running}
                                if outcome['crash_verified'] and replay_verified(marker, self.telemetry(), successor):
                                    outcome['replay_ack_verified'] = True
                                    break
                        except (ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError) as error:
                            self.record('recovery_observation_retry', case=case, error=type(error).__name__)
                        time.sleep(5)
                    self.record('recovery_evidence', case=case, trace=trace)
                    outcome['observed_present'] = self.presence(fixture, values)
                    if not outcome['crash_verified'] or not outcome['replay_ack_verified']:
                        raise TimeoutError('crash, nonoverlap, redelivery and acknowledgement evidence incomplete')
                    if outcome['observed_present'] is not present:
                        raise AssertionError('catalog state after replay differs')
                else:
                    self.wait_presence(fixture, values, present)
                self.drain(fixture['name'] + '_' + phase)
                self.verify(fixture, fixture['name'] + '_' + phase)
            outcome['status'] = 'passed'
        except Exception as error:
            outcome['status'] = 'incomplete' if isinstance(error, TimeoutError) else 'failed'
            outcome['error'] = f'{type(error).__name__}: {error}'
        self.record('recovery_result', **outcome)

    def attempt(self, trial, action):
        status, error, attempted = 'incomplete', 'Trial interrupted before completion', False
        print(f'Running {trial["id"]}', flush=True)
        try:
            if time.monotonic() >= self.deadline:
                raise TimeoutError('pilot time budget reached before trial started')
            attempted = True
            action()
            status, error = 'passed', None
        except Exception as exc:
            status, error = ('incomplete' if isinstance(exc, TimeoutError) else 'failed'), f'{type(exc).__name__}: {exc}'
        finally:
            cleanup_error = None
            if self.crawler is not None:
                try:
                    self.crawler.delete()
                    self.crawler = None
                except Exception as exc:
                    cleanup_error = exc
                    if status == 'passed':
                        status = 'incomplete'
                    error = (error + '; ' if error else '') + f'Crawler cleanup failed: {type(exc).__name__}: {exc}'
            for graph in ([2, 3] if trial['graph'] == 2 else [trial['graph']]):
                self.graph_results.append(dict(graph=graph, trial_id=trial['id'], method=trial['method'], layout=trial.get('layout', ''),
                                               x_value=trial.get('x'), repetition=trial['repetition'], status=status, error=error, attempted=attempted))
            self.record('trial_result', trial=trial, status=status, error=error, attempted=attempted)
            print(f'{trial["id"]}: {status}' + (f' — {error}' if error else ''), flush=True)
            if cleanup_error is not None:
                raise RuntimeError('Cannot continue after crawler cleanup failed') from cleanup_error
