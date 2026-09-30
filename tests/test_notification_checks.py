"""Exercise notification check orchestration against a stateful local provider."""

from datetime import datetime, timezone
import json
from types import SimpleNamespace
from urllib.parse import unquote_plus

import pytest

from experiments.aws import checks


class LocalPilot:
    def __init__(self, reverse=False):
        self.now, self.deadline, self.timeout = 0, 1000, 60
        self.args = SimpleNamespace(repetitions=1)
        self.running, self.reverse = True, reverse
        self.fixtures, self.catalog, self.logs, self.records, self.files = {}, set(), [], [], {}
        self.queues = {'event': [], 'dlq': []}
        self.sent, self.released, self.labels, self.counter = [], [], [], 0
        self.outputs = {'GlueDatabaseName': 'db', 'EventQueueUrl': 'event', 'DeadLetterQueueUrl': 'dlq'}
        self.clients = {'sqs': self}
        self.bundle = SimpleNamespace(write=self.write)

    def utc(self):
        return datetime.fromtimestamp(self.now, timezone.utc).isoformat()

    def emit(self, action, **fields):
        self.now += 0.001
        self.logs.append({'record': {'action': action, 'timestamp': self.utc(), **fields}})

    def record(self, event, **fields):
        self.records.append({'event': event, **fields})

    def write(self, name, data):
        assert name not in self.files, 'evidence name must be unique'
        self.files[name] = data

    def fixture(self, name, method):
        fixture = {'name': name, 'bucket': 'bucket', 'table': {'Name': name}, 'objects': {}}
        self.fixtures[name] = fixture
        return fixture

    def native(self, key, operation):
        self.now += 0.01
        body = json.dumps({'Records': [{'eventTime': self.utc(), 'eventName': operation,
                                       's3': {'bucket': {'name': 'bucket'}, 'object': {'key': key}}}]})
        self.enqueue('event', body)
        if self.running:
            self.process()

    def upload(self, fixture, values, record_id):
        key = fixture['name'] + '/' + '/'.join(values) + f'/{record_id}.parquet'
        fixture['objects'][key] = tuple(values)
        self.native(key, 'ObjectCreated:Put')
        return key, self.utc(), self.now

    def remove(self, fixture, key):
        del fixture['objects'][key]
        self.native(key, 'ObjectRemoved:Delete')

    def enqueue(self, queue, body):
        self.counter += 1
        message = {'MessageId': str(self.counter), 'ReceiptHandle': 'private-' + str(self.counter), 'Body': body, 'held': False}
        self.queues[queue].append(message)
        return message['MessageId']

    def send_message(self, QueueUrl, MessageBody):
        self.sent.append(MessageBody)
        message_id = self.enqueue(QueueUrl, MessageBody)
        if self.running:
            self.process()
        return {'MessageId': message_id}

    def receive_message(self, QueueUrl, **kwargs):
        messages = [message for message in self.queues[QueueUrl] if not message['held']][:kwargs['MaxNumberOfMessages']]
        for message in messages:
            message['held'] = True
        return {'Messages': messages}

    def change_message_visibility(self, QueueUrl, ReceiptHandle, VisibilityTimeout):
        self.released.append(VisibilityTimeout)
        for message in self.queues[QueueUrl]:
            if message['ReceiptHandle'] == ReceiptHandle:
                message['held'] = False

    def delete_message(self, QueueUrl, ReceiptHandle):
        self.queues[QueueUrl] = [message for message in self.queues[QueueUrl] if message['ReceiptHandle'] != ReceiptHandle]

    def stop_worker(self):
        self.running = False

    def require_worker_stopped(self):
        assert not self.running

    def configure(self, fault=None):
        assert not self.running and fault is None

    def start_worker(self):
        self.running = True
        self.process()

    def process(self):
        messages = [message for message in self.queues['event'] if not message['held']]
        if not messages:
            return
        if self.reverse:
            messages.reverse()
        groups = {}
        for message in messages:
            self.emit('message_received', message_id=message['MessageId'], receive_count='1')
            event = json.loads(message['Body'])['Records'][0]
            key = unquote_plus(event['s3']['object']['key'])
            parts = key.split('/')
            groups.setdefault((parts[0], tuple(parts[1:4])), []).append((message, event, key))
        for (name, values), group in groups.items():
            exists = any(value == values for value in self.fixtures[name]['objects'].values())
            before = (name, values) in self.catalog
            mutation = 'ADD' if group[-1][1]['eventName'].startswith('ObjectCreated:') else 'DROP'
            terminal = mutation == 'ADD' and not exists
            status = 'RETRY' if terminal else ('APPLIED' if exists != before else 'NOOP')
            reason = 'not_visible' if terminal else ('still_non_empty' if mutation == 'DROP' and exists else 'verified')
            for _ in range(6 if terminal else 1):
                self.emit('pmcp_result', table_id='db.' + name, mutation=mutation, status=status, reason=reason)
            if exists:
                self.catalog.add((name, values))
            else:
                self.catalog.discard((name, values))
            for message, _, _ in group:
                if terminal:
                    self.enqueue('dlq', message['Body'])
                    self.emit('message_dead_lettered', message_id=message['MessageId'])
                self.emit('message_acknowledged', message_id=message['MessageId'])
                self.queues['event'].remove(message)

    def telemetry(self):
        return self.logs

    def presence(self, fixture, values):
        return (fixture['name'], tuple(values)) in self.catalog

    def wait_presence(self, fixture, values, expected):
        assert self.presence(fixture, values) == expected

    def wait_processed(self, fixture, since, *, expected_changes=None):
        if expected_changes is not None:
            self.record('explicit_processing_expectation', expected_changes=expected_changes)
        assert any(item['record'].get('table_id') == 'db.' + fixture['name']
                   and item['record']['timestamp'] >= since and item['record'].get('status') in ('APPLIED', 'NOOP') for item in self.logs)

    def drain(self, label):
        assert label not in self.labels, 'drain evidence names must be unique'
        self.labels.append(label)
        assert not any(self.queues.values())

    def verify(self, fixture, label):
        expected = set(fixture['objects'].values())
        observed = {values for name, values in self.catalog if name == fixture['name']}
        assert observed == expected
        self.record('verified', label=label, rows=len(fixture['objects']))


def setup(monkeypatch, name, reverse=False):
    pilot = LocalPilot(reverse)
    monkeypatch.setattr(checks, 'utc', pilot.utc)
    monkeypatch.setattr(checks.time, 'monotonic', lambda: pilot.now)
    monkeypatch.setattr(checks.time, 'sleep', lambda delay: setattr(pilot, 'now', pilot.now + delay))
    checks.prepare_checks(pilot, names=[name])
    return pilot


def test_duplicates_and_old_events_replay_exact_capture_and_preserve_verified_state(monkeypatch):
    pilot = setup(monkeypatch, 'duplicate_old_events')
    results = checks.run_checks(pilot, names=['duplicate_old_events'])
    assert results[0]['status'] == 'passed', results
    captures = [value for value in pilot.files.values() if value.get('classification') == 'captured_native_s3_notification']
    create = next(item['body'] for item in captures if item['operation'] == 'CREATE')
    delete = next(item['body'] for item in captures if item['operation'] == 'DELETE')
    assert pilot.sent == [create, create, create, create, delete]
    assert len(pilot.fixtures['check_duplicate_old_events_r1']['objects']) == 1
    assert not any(pilot.queues.values()) and pilot.running
    assert all(value == 0 for value in pilot.released)
    assert 'private-' not in json.dumps(pilot.files)
    assert any('old_create' in name and name.endswith('_dlq.json') for name in pilot.files)
    expected = next(record['expected_changes'] for record in pilot.records if record['event'] == 'explicit_processing_expectation')
    assert len(expected) == 1 and expected[0]['operation'] == 'DELETE'


@pytest.mark.parametrize('reverse', [False, True])
def test_rapid_pairs_are_native_and_converge_for_either_delivery_order(monkeypatch, reverse):
    pilot = setup(monkeypatch, 'rapid_changes', reverse)
    results = checks.run_checks(pilot, names=['rapid_changes'])
    assert results[0]['status'] == 'passed', results
    assert len(pilot.fixtures['check_rapid_changes_r1']['objects']) == 1
    assert not pilot.sent, 'rapid check must use native mutations rather than replayed notifications'
    assert len([value for value in pilot.files.values() if value.get('classification') == 'captured_native_s3_notification']) == 4
    assert not any(pilot.queues.values()) and pilot.running
