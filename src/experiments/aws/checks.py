"""Bounded supporting workloads using the pilot's real AWS worker and evidence."""

from datetime import datetime
import hashlib
import json
import time
from urllib.parse import unquote_plus

from experiments.evidence import utc


CHECK_NAMES = ('short_burst', 'long_burst', 'two_tables', 'retryable_failures', 'retry_exhaustion', 'duplicate_old_events', 'rapid_changes')
CHECK_FIXTURES = ('short_burst', 'long_burst', 'two_tables_a', 'two_tables_b', 'retryable_failures', 'retry_exhaustion', 'duplicate_old_events', 'rapid_changes')


def _selected_names(names):
    selected = CHECK_NAMES if names is None else tuple(names)
    if len(set(selected)) != len(selected) or any(name not in CHECK_NAMES for name in selected):
        raise ValueError('check names must be unique supported names')
    return selected


def prepare_checks(pilot, names=None):
    """Create empty tables before the worker resolves its fixed routing index."""
    repetitions = getattr(getattr(pilot, 'args', None), 'repetitions', 1)
    repetitions = getattr(getattr(pilot, 'args', None), 'correctness_repetitions', None) or repetitions
    fixtures = [fixture for name in _selected_names(names)
                for fixture in (('two_tables_a', 'two_tables_b') if name == 'two_tables' else (name,))]
    return {f'{name}_r{repetition}': pilot.fixture(f'check_{name}_r{repetition}', 'rtmsf')
            for repetition in range(1, repetitions + 1) for name in fixtures}


def _records(pilot, since):
    return [item['record'] for item in pilot.telemetry() if item['record'].get('timestamp', '') >= since]


def _finish(pilot, fixture, since):
    from experiments.aws.workloads import partition_values

    pilot.wait_presence(fixture, partition_values(0), True)
    pilot.wait_processed(fixture, since)
    pilot.drain(fixture['name'])
    pilot.verify(fixture, fixture['name'])


def _burst_evidence(pilot, fixture, since):
    """Explain actual coalescing without assuming one window or disabling WOERA."""
    records = _records(pilot, since)
    expected = {hashlib.sha256(key.encode()).hexdigest() for key in fixture['objects']}
    events = [event for item in records if item.get('action') == 'message_decoded'
              for event in item.get('events', [])
              if event.get('key_sha256') in expected and event.get('operation') == 'CREATE']
    table_id = pilot.outputs['GlueDatabaseName'] + '.' + fixture['table']['Name']
    results = [item for item in records if item.get('action') == 'pmcp_result'
               and item.get('table_id') == table_id]
    if not expected or {event['key_sha256'] for event in events} != expected:
        raise TimeoutError('burst telemetry does not cover all uploaded files')
    if not results or any(type(item.get('observation_count')) is not int or item['observation_count'] < 1
                          or item.get('status') not in ('APPLIED', 'NOOP', 'RETRY') for item in results):
        raise TimeoutError('burst PMCP observation counts are unavailable')
    terminal = [item for item in results if item['status'] in ('APPLIED', 'NOOP')]
    if sum(item['observation_count'] for item in terminal) != len(events):
        raise TimeoutError('burst terminal PMCP records do not account for the observed events')
    counts = [item['observation_count'] for item in results]
    evidence = dict(distinct_files=len(expected), observed_events=len(events),
                    pmcp_attempts=len(results), observations_per_pmcp_attempt=counts,
                    applied_catalog_changes=sum(item.get('status') == 'APPLIED' for item in results),
                    coalescing_observed=any(count > 1 for count in counts))
    pilot.record('burst_work_evidence', fixture=fixture['name'], since=since,
                 processing_records=results, **evidence)
    return evidence


def short_burst(pilot, repetition=1):
    from experiments.aws.workloads import partition_values

    fixture = pilot.fixtures[f'check_short_burst_r{repetition}']
    since, start = utc(), time.monotonic()
    completed = [pilot.upload(fixture, partition_values(0), index)[2] for index in range(20)]
    duration = completed[-1] - start
    pilot.record('supporting_check_trace', name='short_burst', repetition=repetition, fixture=fixture['name'], since=since,
                 trace=[{'index': index, 'completed_offset_s': value - start} for index, value in enumerate(completed)])
    _finish(pilot, fixture, since)
    if duration >= 10:
        raise TimeoutError('uploads exceeded the 10-second quiet period; short-burst condition not established')
    return {'files': 20, 'partitions': 1, 'upload_duration_s': duration,
            **_burst_evidence(pilot, fixture, since)}


def long_burst(pilot, repetition=1):
    from experiments.aws.workloads import partition_values

    fixture = pilot.fixtures[f'check_long_burst_r{repetition}']
    values = partition_values(0)
    since, start = utc(), time.monotonic()
    trace, uploads, observations = [], [], []
    first_present = None
    next_upload = 0
    try:
        while next_upload < 9:
            if time.monotonic() >= pilot.deadline:
                raise TimeoutError('pilot deadline interrupted long burst')
            if time.monotonic() >= start + 5 * next_upload:
                began = time.monotonic()
                _, completed_at, completed = pilot.upload(fixture, values, next_upload)
                uploads.append(completed)
                trace.append({'kind': 'upload', 'index': next_upload, 'scheduled_offset_s': 5 * next_upload,
                              'started_offset_s': began - start, 'completed_offset_s': completed - start,
                              'completed_at': completed_at})
                next_upload += 1
            observed = time.monotonic()
            present = pilot.presence(fixture, values)
            observations.append(observed)
            trace.append({'kind': 'observation', 'offset_s': observed - start, 'present': present,
                          'request_duration_s': time.monotonic() - observed})
            if present and first_present is None:
                first_present = observed
            if next_upload < 9:
                time.sleep(max(0, min(1, start + 5 * next_upload - time.monotonic())))
    finally:
        pilot.record('supporting_check_trace', name='long_burst', repetition=repetition, fixture=fixture['name'], since=since, trace=trace)
    gaps = [later - earlier for earlier, later in zip(uploads, uploads[1:])]
    if max(gaps) >= 10:
        raise TimeoutError('actual upload gaps reached the quiet period; maximum-buffer condition not established')
    last_upload = next(item['started_offset_s'] for item in reversed(trace) if item['kind'] == 'upload') + start
    if first_present is None or first_present >= last_upload:
        raise AssertionError('no catalog progress observed before the final long-burst upload')
    _finish(pilot, fixture, since)
    records = _records(pilot, since)
    table_id = pilot.outputs['GlueDatabaseName'] + '.' + fixture['table']['Name']
    received = [item for item in records if item.get('action') == 'message_received']
    processed = [item for item in records if item.get('action') == 'pmcp_result' and item.get('table_id') == table_id]
    if not received or not processed:
        raise TimeoutError('long-burst receive and processing timestamps unavailable')
    first_receive = min(item['timestamp'] for item in received)
    first_result = min(processed, key=lambda item: item['timestamp'])
    arrival_times = sorted(datetime.fromisoformat(item['timestamp']).timestamp() for item in received
                           if item['timestamp'] <= first_result['timestamp'])
    result_time = datetime.fromisoformat(first_result['timestamp']).timestamp()
    arrival_gaps = [later - earlier for earlier, later in zip(arrival_times, arrival_times[1:] + [result_time])]
    pilot.record('long_burst_arrival_evidence', repetition=repetition, fixture=fixture['name'],
                 receive_records=received, processing_records=processed, arrival_gaps_s=arrival_gaps)
    if max(arrival_gaps) >= 10:
        raise TimeoutError('native receive gaps reached the quiet period; maximum-buffer condition not established')
    elapsed = (datetime.fromisoformat(first_result['timestamp']) - datetime.fromisoformat(first_receive)).total_seconds()
    provider_seconds = sum(float(item.get('duration_ms', 0)) / 1000 for item in records
                           if item.get('action') == 'api_call' and not item.get('operation', '').startswith('SQS.')
                           and first_receive <= item['timestamp'] <= first_result['timestamp'])
    observer_gap = max((later - earlier for earlier, later in zip(observations, observations[1:])), default=0)
    measurements = {'files': 9, 'repetition': repetition, 'scheduled_span_s': 40, 'actual_upload_gaps_s': gaps,
                    'maximum_actual_upload_gap_s': max(gaps), 'progress_before_final_upload': True,
                    'first_visibility_offset_s': first_present - start, 'first_receive': first_receive,
                    'first_pmcp_result': first_result, 'first_processing_from_receive_s': elapsed,
                    'measured_provider_duration_s': provider_seconds, 'maximum_observer_gap_s': observer_gap,
                    'measured_tolerance_s': provider_seconds + observer_gap}
    pilot.record('long_burst_bound_evidence', **measurements)
    if elapsed > 31 + measurements['measured_tolerance_s']:
        raise AssertionError('first processing exceeded 31 seconds plus measured provider and observer time')
    return measurements


def two_tables(pilot, repetition=1):
    from experiments.aws.workloads import partition_values

    fixtures = [pilot.fixtures[f'check_two_tables_{suffix}_r{repetition}'] for suffix in ('a', 'b')]
    since = utc()
    for partition_index in range(3):
        for file_index in range(5):
            for fixture in fixtures:
                pilot.upload(fixture, partition_values(partition_index), partition_index * 5 + file_index)
    for fixture in fixtures:
        for partition_index in range(3):
            pilot.wait_presence(fixture, partition_values(partition_index), True)
        _finish(pilot, fixture, since)
    return {'tables': 2, 'partitions_per_table': 3, 'files_per_partition': 5, 'files': 30}


def _body_matches(body, fixture, key, operation=None):
    try:
        records = json.loads(body)['Records']
        prefix = {'CREATE': 'ObjectCreated:', 'DELETE': 'ObjectRemoved:'}.get(operation)
        return bool(records) and all(record['s3']['bucket']['name'] == fixture['bucket']
                                     and unquote_plus(record['s3']['object']['key']) == key
                                     and (prefix is None or record.get('eventName', '').startswith(prefix)) for record in records)
    except (KeyError, TypeError, ValueError):
        return False


def _receipt_call(operation, **request):
    try:
        return operation(**request)
    except Exception as error:
        # Provider error text can repeat the receipt handle. The harness retains
        # the error class while keeping acknowledgement capabilities out of logs.
        raise RuntimeError(f'SQS receipt operation failed ({type(error).__name__})') from None


def _capture_native(pilot, fixture, key, operation, label):
    """Copy one matching native body while leaving every source message queued."""
    pilot.require_worker_stopped()
    client = pilot.clients['sqs']
    queue = pilot.outputs['EventQueueUrl']
    deadline = min(pilot.deadline, time.monotonic() + pilot.timeout)
    while time.monotonic() < deadline:
        messages = client.receive_message(QueueUrl=queue, MaxNumberOfMessages=10, WaitTimeSeconds=1,
                                          VisibilityTimeout=30, MessageSystemAttributeNames=['ApproximateReceiveCount']).get('Messages', [])
        try:
            for message in messages:
                if _body_matches(message['Body'], fixture, key, operation):
                    captured = {'message_id': message['MessageId'], 'body': message['Body'],
                                'body_sha256': hashlib.sha256(message['Body'].encode()).hexdigest(),
                                'operation': operation, 'classification': 'captured_native_s3_notification',
                                'fixture': fixture['name'], 'captured_while_worker_stopped': True}
                    pilot.bundle.write(f'checks/{fixture["name"]}_{label}_native.json', captured)
                    pilot.record('native_notification_captured', label=label,
                                 **{key: value for key, value in captured.items() if key != 'body'})
                    return captured
        finally:
            release_error = None
            for message in messages:
                try:
                    _receipt_call(client.change_message_visibility, QueueUrl=queue,
                                  ReceiptHandle=message['ReceiptHandle'], VisibilityTimeout=0)
                except RuntimeError as error:
                    release_error = release_error or error
            if release_error is not None:
                raise release_error
    raise TimeoutError('matching native notification was not captured')


def _retain_expected_dlq(pilot, fixture, key, expected_count, *, label=None, expected_bodies=None):
    """Persist expected native bodies before consuming only this check's DLQ input."""
    queue = pilot.outputs['DeadLetterQueueUrl']
    client = pilot.clients['sqs']
    messages = []
    deadline = min(pilot.deadline, time.monotonic() + pilot.timeout)
    while len(messages) < expected_count and time.monotonic() < deadline:
        received = client.receive_message(QueueUrl=queue, MaxNumberOfMessages=10,
                                          WaitTimeSeconds=1, VisibilityTimeout=180).get('Messages', [])
        for message in received:
            if not _body_matches(message['Body'], fixture, key) or (expected_bodies is not None and message['Body'] not in expected_bodies):
                _receipt_call(client.change_message_visibility, QueueUrl=queue, ReceiptHandle=message['ReceiptHandle'], VisibilityTimeout=0)
                raise AssertionError('DLQ contains an unrelated message; left unconsumed')
            messages.append(message)
    if len(messages) != expected_count:
        raise TimeoutError('expected durable DLQ copies were not all available')
    filename = fixture['name'] + ('_' + label if label else '') + '_dlq.json'
    pilot.bundle.write('checks/' + filename, {
        'fixture': fixture['name'], 'messages': [{'message_id': item['MessageId'], 'body': item['Body']} for item in messages],
    })
    for message in messages:
        _receipt_call(client.delete_message, QueueUrl=queue, ReceiptHandle=message['ReceiptHandle'])
    return [message['Body'] for message in messages]


def _replay(pilot, bodies, label):
    message_ids = []
    for body in bodies:
        response = pilot.clients['sqs'].send_message(QueueUrl=pilot.outputs['EventQueueUrl'], MessageBody=body)
        message_ids.append(response['MessageId'])
        pilot.record('captured_native_notification_replayed', label=label,
                     message_id=response['MessageId'], body_sha256=hashlib.sha256(body.encode()).hexdigest())
    return message_ids


def _await_ids(pilot, since, message_ids):
    expected = set(message_ids)
    deadline = min(pilot.deadline, time.monotonic() + pilot.timeout)
    while time.monotonic() < deadline:
        records = _records(pilot, since)
        received = {item.get('message_id') for item in records if item.get('action') == 'message_received'}
        acked = {item.get('message_id') for item in records if item.get('action') == 'message_acknowledged'}
        copied = {item.get('message_id') for item in records if item.get('action') == 'message_dead_lettered'}
        if expected and expected <= received & acked and received <= acked:
            return records, received & copied
        time.sleep(2)
    raise TimeoutError('captured or replayed message IDs were not received and acknowledged')


def _verified_phase(pilot, fixture, values, since, phase, present, message_ids=(), expected_changes=None):
    label = fixture['name'] + '_' + phase
    pilot.wait_presence(fixture, values, present)
    pilot.wait_processed(fixture, since, **({'expected_changes': expected_changes} if expected_changes is not None else {}))
    if message_ids:
        records, copied = _await_ids(pilot, since, message_ids)
        pilot.record('notification_processing_evidence', label=label, message_ids=list(message_ids), records=records)
        if copied:
            raise AssertionError('ordinary notification unexpectedly reached the DLQ')
    pilot.drain(label)
    pilot.verify(fixture, label)


def _empty_terminal(pilot, fixture, key, values, since, phase, records, copied, bodies):
    table_id = pilot.outputs['GlueDatabaseName'] + '.' + fixture['table']['Name']
    retries = [item for item in records if item.get('action') == 'pmcp_result'
               and item.get('table_id') == table_id and item.get('status') == 'RETRY']
    if len(retries) != 6 or any(item.get('reason') != 'not_visible' for item in retries):
        raise AssertionError('empty CREATE did not follow the normal not-visible retry exhaustion path')
    pilot.wait_presence(fixture, values, False)
    _retain_expected_dlq(pilot, fixture, key, len(copied), label=phase, expected_bodies=set(bodies))
    pilot.record('stale_create_terminal_evidence', label=fixture['name'] + '_' + phase,
                 classification='not_visible_retry_exhaustion', message_ids=sorted(copied), records=records)
    pilot.drain(fixture['name'] + '_' + phase)
    pilot.verify(fixture, fixture['name'] + '_' + phase)


def duplicate_old_events(pilot, repetition=1):
    from experiments.aws.workloads import partition_path, partition_values

    fixture = pilot.fixtures[f'check_duplicate_old_events_r{repetition}']
    values = partition_values(0)
    pilot.stop_worker()
    stopped = True
    try:
        pilot.configure()
        since = utc()
        key, _, _ = pilot.upload(fixture, values, 0)
        create = _capture_native(pilot, fixture, key, 'CREATE', 'original_create')
        pilot.start_worker()
        stopped = False
        _verified_phase(pilot, fixture, values, since, 'original_add', True, [create['message_id']])

        since = utc()
        ids = _replay(pilot, [create['body']] * 3, fixture['name'] + '_duplicate_three')
        _verified_phase(pilot, fixture, values, since, 'duplicate_three', True, ids)

        pilot.stop_worker()
        stopped = True
        since = utc()
        pilot.remove(fixture, key)
        delete = _capture_native(pilot, fixture, key, 'DELETE', 'original_delete')
        pilot.start_worker()
        stopped = False
        _verified_phase(pilot, fixture, values, since, 'native_delete', False, [delete['message_id']])

        since = utc()
        ids = _replay(pilot, [create['body']], fixture['name'] + '_old_create')
        records, copied = _await_ids(pilot, since, ids)
        if not set(ids) <= copied:
            raise AssertionError('old CREATE was acknowledged without the expected durable terminal outcome')
        _empty_terminal(pilot, fixture, key, values, since, 'old_create', records, copied, [create['body']])

        since = utc()
        pilot.upload(fixture, values, 0)
        _verified_phase(pilot, fixture, values, since, 'recreated', True)
        since = utc()
        ids = _replay(pilot, [delete['body']], fixture['name'] + '_old_delete')
        expected = [dict(key_sha256=hashlib.sha256(key.encode()).hexdigest(), operation='DELETE',
                         partition=partition_path(values).rstrip('/'))]
        _verified_phase(pilot, fixture, values, since, 'old_delete', True, ids, expected_changes=expected)
        table_id = pilot.outputs['GlueDatabaseName'] + '.' + fixture['table']['Name']
        if not any(item.get('action') == 'pmcp_result' and item.get('table_id') == table_id
                   and item.get('mutation') == 'DROP' and item.get('reason') == 'still_non_empty'
                   for item in _records(pilot, since)):
            raise AssertionError('old DELETE was not verified against the surviving object')
        return {'duplicate_replays': 3, 'old_create_outcome': 'not_visible_retry_exhaustion',
                'old_delete_outcome': 'still_non_empty', 'native_bodies_captured': 2,
                'replay_classification': 'exact_captured_native_s3_notifications', 'normal_worker_restored': True}
    finally:
        if stopped:
            pilot.stop_worker()
            pilot.configure()
            pilot.start_worker()


def rapid_changes(pilot, repetition=1):
    from experiments.aws.workloads import partition_values

    fixture = pilot.fixtures[f'check_rapid_changes_r{repetition}']
    pilot.stop_worker()
    stopped = True
    outcomes = []
    try:
        pilot.configure()
        values = partition_values(0)
        since, began = utc(), time.monotonic()
        key, _, created = pilot.upload(fixture, values, 0)
        pilot.remove(fixture, key)
        pilot.record('rapid_native_mutations', fixture=fixture['name'], case='create_delete',
                     first_completed_offset_s=created - began, pair_completed_offset_s=time.monotonic() - began)
        create = _capture_native(pilot, fixture, key, 'CREATE', 'rapid_create')
        delete = _capture_native(pilot, fixture, key, 'DELETE', 'rapid_delete')
        pilot.start_worker()
        stopped = False
        records, copied = _await_ids(pilot, since, [create['message_id'], delete['message_id']])
        if copied:
            _empty_terminal(pilot, fixture, key, values, since, 'create_delete', records, copied, [create['body'], delete['body']])
        else:
            _verified_phase(pilot, fixture, values, since, 'create_delete', False, [create['message_id'], delete['message_id']])
        outcomes.append({'case': 'create_delete', 'present': False,
                         'handling': 'not_visible_retry_exhaustion' if copied else 'ordinary_completion'})

        values = partition_values(1)
        since = utc()
        key, _, _ = pilot.upload(fixture, values, 1)
        _verified_phase(pilot, fixture, values, since, 'recreate_baseline', True)
        pilot.stop_worker()
        stopped = True
        since, began = utc(), time.monotonic()
        pilot.remove(fixture, key)
        removed = time.monotonic()
        pilot.upload(fixture, values, 1)
        pilot.record('rapid_native_mutations', fixture=fixture['name'], case='delete_recreate',
                     first_completed_offset_s=removed - began, pair_completed_offset_s=time.monotonic() - began)
        delete = _capture_native(pilot, fixture, key, 'DELETE', 'recreate_delete')
        create = _capture_native(pilot, fixture, key, 'CREATE', 'recreate_create')
        pilot.start_worker()
        stopped = False
        _verified_phase(pilot, fixture, values, since, 'delete_recreate', True, [delete['message_id'], create['message_id']])
        outcomes.append({'case': 'delete_recreate', 'present': True, 'handling': 'ordinary_completion'})
        return {'cases': outcomes, 'native_bodies_captured': 4, 'replayed_messages': 0,
                'worker_stopped_during_mutation_pairs': True, 'normal_worker_restored': True}
    finally:
        if stopped:
            pilot.stop_worker()
            pilot.configure()
            pilot.start_worker()


def retry_failures(pilot, count, repetition=1):
    from experiments.aws.workloads import partition_path, partition_values

    name = 'retryable_failures' if count == 2 else 'retry_exhaustion'
    fixture = pilot.fixtures[f'check_{name}_r{repetition}']
    values = partition_values(0)
    table_id = pilot.outputs['GlueDatabaseName'] + '.' + fixture['table']['Name']
    bodies = []
    pilot.stop_worker()
    try:
        pilot.configure({'operation': 'ADD', 'partition_path': partition_path(values).rstrip('/'),
                         'table_id': table_id, 'fail_catalog_calls': count})
        pilot.start_worker()
        since = utc()
        key, _, _ = pilot.upload(fixture, values, 0)
        if count == 2:
            _finish(pilot, fixture, since)
        else:
            deadline = min(pilot.deadline, time.monotonic() + pilot.timeout)
            while time.monotonic() < deadline:
                records = _records(pilot, since)
                copied = {item.get('message_id') for item in records if item.get('action') == 'message_dead_lettered'}
                acked = {item.get('message_id') for item in records if item.get('action') == 'message_acknowledged'}
                retries = [item for item in records if item.get('action') == 'pmcp_result'
                           and item.get('table_id') == table_id and item.get('status') == 'RETRY']
                if copied and copied <= acked and len(retries) == 6:
                    break
                time.sleep(2)
            else:
                raise TimeoutError('six retries followed by durable DLQ and acknowledgement were not observed')
            if pilot.presence(fixture, values) is not False:
                raise AssertionError('exhausted ADD unexpectedly registered the partition')
            bodies = _retain_expected_dlq(pilot, fixture, key, len(copied))
        injected = sum(item.get('action') == 'catalog_failure_injected' for item in _records(pilot, since))
        if injected != count:
            raise AssertionError(f'expected {count} injected failures, observed {injected}')
        pilot.record('retry_evidence', name=name, injected_failures=injected,
                     terminal_dlq_messages=len(bodies), records=_records(pilot, since))
    finally:
        pilot.stop_worker()
        pilot.configure()
        pilot.start_worker()
    if bodies:
        since = utc()
        _replay(pilot, bodies, name + '_normal_recovery')
        _finish(pilot, fixture, since)
    return {'injected_failures': count, 'expected_dlq_messages': len(bodies),
            'normal_worker_restored': True, 'replayed_bodies_are_captured_native_notifications': bool(bodies)}


def run_checks(pilot, names=None):
    results = []
    repetitions = getattr(getattr(pilot, 'args', None), 'repetitions', 1)
    repetitions = getattr(getattr(pilot, 'args', None), 'correctness_repetitions', None) or repetitions
    actions = {'short_burst': short_burst, 'long_burst': long_burst, 'two_tables': two_tables,
               'retryable_failures': lambda pilot, rep: retry_failures(pilot, 2, rep),
               'retry_exhaustion': lambda pilot, rep: retry_failures(pilot, 6, rep),
               'duplicate_old_events': lambda pilot, rep: duplicate_old_events(pilot, rep),
               'rapid_changes': lambda pilot, rep: rapid_changes(pilot, rep)}
    selected = _selected_names(names)
    for repetition in range(1, repetitions + 1):
        for name in selected:
            action = actions[name]
            result = {'name': name, 'repetition': repetition, 'attempted': False, 'status': 'incomplete'}
            try:
                if time.monotonic() >= pilot.deadline:
                    raise TimeoutError('pilot budget expired before check started')
                result['attempted'] = True
                pilot.drain(f'{name}_r{repetition}_before')
                result.update(action(pilot, repetition), status='passed')
            except Exception as error:
                result.update(status='incomplete' if isinstance(error, TimeoutError) else 'failed',
                              error=f'{type(error).__name__}: {error}')
            pilot.record('supporting_check_result', **result)
            results.append(result)
    return results
