from types import SimpleNamespace

from botocore.exceptions import ReadTimeoutError
import pytest

from experiments.aws import workloads


@pytest.mark.parametrize('timeouts', [1, 3])
@pytest.mark.parametrize('export', [False, True])
def test_telemetry_retries_same_page_and_rejects_persistent_failure(monkeypatch, timeouts, export):
    monkeypatch.setattr(workloads.time, 'sleep', lambda seconds: None)
    calls = []
    first = {'eventId': 'e1', 'logStreamName': 'worker', 'message': '{"action":"first"}'}
    invalid = {'eventId': 'e2', 'logStreamName': 'worker', 'message': 'not JSON'}
    last = {'eventId': 'e3', 'logStreamName': 'worker', 'message': '{"action":"last"}'}
    responses = iter([
        {'events': [first, invalid], 'nextToken': 'page2'},
        *[ReadTimeoutError(endpoint_url='https://logs.test') for _ in range(timeouts)],
        {'events': [last]},
    ])

    def read(**params):
        calls.append(params.get('nextToken'))
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return response

    pilot = workloads.Pilot.__new__(workloads.Pilot)
    pilot.start_ms, pilot.outputs = 0, {'LogGroupName': 'group', 'DeadLetterQueueUrl': 'dlq'}
    pilot.clients = {'logs': SimpleNamespace(filter_log_events=read),
                     'sqs': SimpleNamespace(get_queue_attributes=lambda **kw: {'Attributes': {'ApproximateNumberOfMessages': '0'}})}
    pilot.record = lambda *args, **kwargs: None
    artifacts = {}
    pilot.bundle = SimpleNamespace(write=lambda name, value: artifacts.__setitem__(name, value))
    collect = pilot.collect_runtime if export else pilot.telemetry
    if timeouts == 3:
        with pytest.raises(ReadTimeoutError):
            collect()
        assert calls == [None, 'page2', 'page2', 'page2']
    else:
        telemetry = collect()
        if export:
            assert artifacts['runtime-logs.json'] == [first, invalid, last]
            telemetry = artifacts['app-telemetry.json']
        assert [row['record']['action'] for row in telemetry] == ['first', 'last']
        assert [row['cloudwatch_event_id'] for row in telemetry] == ['e1', 'e3']
        assert calls == [None, 'page2', 'page2']


class ControlledThread:
    latest = None

    def __init__(self, target, **kwargs):
        self.target = target
        ControlledThread.latest = self

    def start(self):
        pass

    def join(self, **kwargs):
        pass

    def is_alive(self):
        return False


def test_lifecycle_waits_for_baseline_before_upload(monkeypatch):
    monkeypatch.setattr(workloads, 'Thread', ControlledThread)
    monkeypatch.setattr(workloads.time, 'sleep', lambda seconds: None)
    pilot = workloads.Pilot.__new__(workloads.Pilot)
    pilot.timeout, pilot.graph_rows = 0, []
    uploaded = []
    pilot.upload = lambda *args: uploaded.append(True) or ('key', '', 1)
    with pytest.raises(TimeoutError, match='baseline'):
        pilot.lifecycle_trial({'objects': {}}, 1)
    assert uploaded == []


@pytest.mark.parametrize('method', ['rtmsf', 'athena_msck'])
def test_visibility_survives_later_processing_failure(monkeypatch, method):
    monkeypatch.setattr(workloads, 'Thread', ControlledThread)
    monkeypatch.setattr(workloads, 'time', SimpleNamespace(monotonic=lambda: 2.0, sleep=lambda seconds: None))
    pilot = workloads.Pilot.__new__(workloads.Pilot)
    fixture = {'name': 'trial', 'bucket': 'bucket', 'table': {'Name': 'trial'}}
    pilot.fixtures, pilot.graph_rows, pilot.windows = {'trial': fixture}, [], []
    pilot.crawler, pilot.timeout = None, 1
    pilot.outputs = {'TaskRoleArn': 'worker-role'}
    pilot.verify = pilot.drain = pilot.record = lambda *args, **kwargs: None
    pilot.values = lambda index: ('new-partition',)
    pilot.upload = lambda *args: ('key', '2026-09-22T00:00:01+00:00', 1.0)
    pilot.presence = lambda *args: True
    pilot.wait_presence = lambda *args: ControlledThread.latest.target()

    def failure(*args):
        raise TimeoutError('processing failed after visibility')

    def failed_query(*args):
        pilot.last_query_start = {'started_at': '2026-09-22T00:00:01+00:00', 'started_monotonic_seconds': 1.0}
        ControlledThread.latest.target()
        failure()

    pilot.wait_processed, pilot.query = failure, failed_query
    trial = {'id': 'trial', 'method': method, 'graph': 2, 'x': 100, 'base': 100, 'repetition': 1}
    with pytest.raises(TimeoutError, match='processing failed'):
        pilot.comparison(trial)
    assert len(pilot.graph_rows) == 1
    assert pilot.graph_rows[0]['graph'] == 3
    assert pilot.graph_rows[0]['y_value'] == 1.0


def test_cloudwatch_defers_verification_reads_until_after_collection(monkeypatch):
    monkeypatch.setattr(workloads.time, 'sleep', lambda seconds: None)
    monkeypatch.setattr(workloads, 'wait_until', lambda *args: None, raising=False)
    pilot = workloads.Pilot.__new__(workloads.Pilot)
    fixture = {'name': 'trial', 'bucket': 'separate-bucket', 'table': {'Name': 'trial'}}
    pilot.fixtures, pilot.graph_rows, pilot.windows = {'trial': fixture}, [], []
    pilot.pending_verifications = []
    pilot.trial_buckets = object()
    pilot.crawler, pilot.timeout = None, 1
    pilot.outputs = {'TaskRoleArn': 'worker-role'}
    verified = []
    pilot.verify = lambda f, label: verified.append(label)
    pilot.drain = pilot.record = pilot.wait_processed = lambda *a, **k: None
    pilot.values = lambda index: ('existing-partition',)
    pilot.upload = lambda *args: ('key', '2026-09-22T00:00:01+00:00', 1.0)
    pilot.comparison(dict(id='trial', method='rtmsf', graph=4, x=100, base=1000, repetition=1))
    assert verified == ['trial_baseline']
    assert pilot.pending_verifications == ['trial']
    assert pilot.windows[0]['bucket'] == 'separate-bucket'
