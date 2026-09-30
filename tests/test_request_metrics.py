from datetime import datetime, timezone
import json

import pytest


def stamp(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def window(identifier='trial', bucket='trial-bucket'):
    return dict(id=identifier, bucket=bucket, filter_id='trial',
                started_at='2026-09-22T12:01:05+00:00',
                ended_at='2026-09-22T12:02:10+00:00',
                quiet_until='2026-09-22T12:03:00+00:00')


class CloudWatch:
    def __init__(self):
        self.values = {}
        self.requests = []

    def get_metric_statistics(self, **request):
        self.requests.append(request)
        bucket = next(d['Value'] for d in request['Dimensions'] if d['Name'] == 'BucketName')
        value = self.values.get((bucket, request['MetricName']), [])
        if isinstance(value, Exception):
            raise value
        return {'Datapoints': value, 'Label': request['MetricName']}


class Session:
    def __init__(self, client):
        self.cw = client

    def client(self, name, **kwargs):
        assert name == 'cloudwatch'
        return self.cw


def collector(tmp_path):
    from experiments.aws.request_metrics import RequestMetrics
    cw = CloudWatch()
    return RequestMetrics(Session(cw), 'us-west-2', tmp_path), cw


def test_counts_use_sum_whole_minutes_and_isolate_buckets(tmp_path):
    audit, cw = collector(tmp_path)
    cw.values['trial-bucket', 'ListRequests'] = [
        {'Timestamp': stamp('2026-09-22T12:02:00'), 'Sum': 7.0},
        {'Timestamp': stamp('2026-09-22T12:01:00'), 'Sum': 3.0},
    ]
    cw.values['other-bucket', 'ListRequests'] = [
        {'Timestamp': stamp('2026-09-22T12:01:00'), 'Sum': 100.0},
    ]
    counts = audit.collect([window(), window('other', 'other-bucket')])
    assert counts['trial']['s3_list_requests'] == 10
    assert counts['other']['s3_list_requests'] == 100
    for request in cw.requests:
        assert request['Namespace'] == 'AWS/S3'
        assert request['Statistics'] == ['Sum']
        assert request['Period'] == 60
        assert request['StartTime'] == stamp('2026-09-22T12:01:00')
        assert request['EndTime'] == stamp('2026-09-22T12:03:00')
        assert {'Name': 'FilterId', 'Value': 'trial'} in request['Dimensions']


def test_missing_is_unknown_but_recorded_zero_is_zero(tmp_path):
    audit, cw = collector(tmp_path)
    cw.values['trial-bucket', 'HeadRequests'] = [
        {'Timestamp': stamp('2026-09-22T12:01:00'), 'Sum': 0.0},
    ]
    result = audit.collect([window()])['trial']
    assert result == {'s3_head_requests': 0, 's3_get_requests': None, 's3_list_requests': None}
    coverage = json.loads((tmp_path / 'coverage.json').read_text())
    assert coverage['windows']['trial']['metric_reasons']['s3_get_requests'] == 'no_datapoints'
    assert coverage['completeness'] == 'not_guaranteed'


def test_later_collection_replaces_totals_and_preserves_earlier_snapshot(tmp_path):
    audit, cw = collector(tmp_path)
    cw.values['trial-bucket', 'ListRequests'] = [
        {'Timestamp': stamp('2026-09-22T12:01:00'), 'Sum': 1.0},
    ]
    assert audit.collect([window()])['trial']['s3_list_requests'] == 1
    cw.values['trial-bucket', 'ListRequests'][0]['Sum'] = 2.0
    assert audit.collect([window()])['trial']['s3_list_requests'] == 2
    snapshots = [json.loads(p.read_text()) for p in (tmp_path / 'collections').glob('*.json')]
    assert sorted(s['windows']['trial']['observed_counts']['s3_list_requests'] for s in snapshots) == [1, 2]


def test_metric_error_does_not_become_zero_or_hide_other_metrics(tmp_path):
    audit, cw = collector(tmp_path)
    cw.values['trial-bucket', 'GetRequests'] = TimeoutError('service unavailable')
    cw.values['trial-bucket', 'ListRequests'] = [
        {'Timestamp': stamp('2026-09-22T12:01:00'), 'Sum': 1.0},
    ]
    result = audit.collect([window()])['trial']
    assert result['s3_get_requests'] is None
    assert result['s3_list_requests'] == 1
    assert audit.coverage['errors'][0]['error'] == 'TimeoutError'


def test_includes_requests_assigned_to_a_later_quiet_minute(tmp_path):
    audit, cw = collector(tmp_path)
    cw.values['trial-bucket', 'ListRequests'] = [
        {'Timestamp': stamp('2026-09-22T12:01:00'), 'Sum': 73.0},
        {'Timestamp': stamp('2026-09-22T12:02:00'), 'Sum': 27.0},
    ]
    # The calls finish in one minute; CloudWatch labels 27 with the next minute.
    w = window() | {'ended_at': '2026-09-22T12:01:17+00:00'}
    assert audit.collect([w])['trial']['s3_list_requests'] == 100


def test_quiet_period_cannot_end_before_workload_or_inside_a_minute(tmp_path):
    audit, cw = collector(tmp_path)
    for end in ('2026-09-22T12:02:00+00:00', '2026-09-22T12:03:01+00:00'):
        with pytest.raises(ValueError):
            audit.collect([window() | {'quiet_until': end}])
    assert cw.requests == []


@pytest.mark.parametrize('change', [
    {'started_at': '2026-09-22T12:00:00'},
    {'ended_at': '2026-09-22T11:00:00+00:00'},
])
def test_invalid_window_rejected_before_aws_calls(tmp_path, change):
    audit, cw = collector(tmp_path)
    with pytest.raises(ValueError):
        audit.collect([window() | change])
    assert cw.requests == []


def test_probe_refuses_to_repeat_an_existing_request_record(tmp_path):
    from experiments.aws.check_request_metrics import issue_requests
    trial = dict(bucket='rtmsf-cw-check-n1-r1-test', count=1, repetition=1)
    record = tmp_path / f'{trial["bucket"]}.json'
    record.write_text('{"started_at":"earlier interrupted attempt"}')
    with pytest.raises(ValueError, match='already'):
        issue_requests(None, trial, tmp_path)
    assert 'earlier interrupted attempt' in record.read_text()


@pytest.mark.parametrize('bad_point', [
    {'Timestamp': stamp('2026-09-22T12:03:00'), 'Sum': 10.0},
    {'Timestamp': stamp('2026-09-22T12:01:00'), 'Sum': -1.0},
    {'Timestamp': stamp('2026-09-22T12:01:00'), 'Sum': 0.5},
])
def test_invalid_datapoint_is_unavailable(tmp_path, bad_point):
    audit, cw = collector(tmp_path)
    cw.values['trial-bucket', 'ListRequests'] = [bad_point]
    assert audit.collect([window()])['trial']['s3_list_requests'] is None


def test_cleanup_can_finish_a_partial_cleanup_without_repeating_requests():
    from botocore.exceptions import ClientError
    from experiments.aws.check_request_metrics import cleanup

    class MissingBucket:
        def delete_bucket_metrics_configuration(self, **request):
            assert request['Bucket'] == 'rtmsf-cw-check-remaining'
            raise ClientError({'Error': {'Code': 'NoSuchBucket'}}, 'DeleteBucketMetricsConfiguration')

    trials = [{'bucket': 'rtmsf-cw-check-done', 'cleaned_up': True},
              {'bucket': 'rtmsf-cw-check-remaining', 'cleanup_error': 'earlier failure'}]
    cleanup(MissingBucket(), trials)
    assert all(t['cleaned_up'] for t in trials)
    assert 'cleanup_error' not in trials[1]


def test_probe_retains_get_request_and_closes_body_when_download_fails(tmp_path):
    from experiments.aws.check_request_metrics import issue_requests

    class Body:
        closed = False

        def read(self):
            raise TimeoutError('interrupted download')

        def close(self):
            self.closed = True

    class S3:
        def head_object(self, **request):
            return {'ResponseMetadata': {'RequestId': 'head', 'HTTPStatusCode': 200}}

        def get_object(self, **request):
            return {'ResponseMetadata': {'RequestId': 'get', 'HTTPStatusCode': 200}, 'Body': body}

    body = Body()
    trial = dict(bucket='rtmsf-cw-check-n1-r1-test', count=1, repetition=1)
    issue_requests(S3(), trial, tmp_path)
    record = json.loads((tmp_path / f'{trial["bucket"]}.json').read_text())
    assert [r['request_id'] for r in record['requests']] == ['head', 'get']
    assert trial['requests_succeeded'] is False
    assert body.closed is True


def test_list_only_probe_issues_no_head_or_get_requests(tmp_path):
    from experiments.aws.check_request_metrics import issue_requests

    class S3:
        def list_objects_v2(self, **request):
            return {'IsTruncated': False, 'ResponseMetadata': {'RequestId': 'list', 'HTTPStatusCode': 200}}

    trial = dict(bucket='rtmsf-cw-check-list-n1-r1-test', count=1, repetition=1, list_only=True)
    issue_requests(S3(), trial, tmp_path)
    result = json.loads((tmp_path / f'{trial["bucket"]}.json').read_text())
    assert trial['requests_succeeded'] is True
    assert [r['operation'] for r in result['requests']] == ['list_objects_v2']


@pytest.mark.parametrize('total,put,expected', [(1, None, 0), (2, 1, 0), (2, None, None), (None, None, None), (1, 1, None)])
def test_sparse_zero_requires_fully_accounted_total(tmp_path, total, put, expected):
    audit, cw = collector(tmp_path)
    def points(count):
        return [] if count is None else [{'Timestamp': stamp('2026-09-22T12:01:00'), 'Sum': float(count)}]
    cw.values['trial-bucket', 'ListRequests'] = points(1)
    cw.values['trial-bucket', 'AllRequests'] = points(total)
    cw.values['trial-bucket', 'PutRequests'] = points(put)
    counts = audit.collect([window()])['trial']
    assert counts['s3_head_requests'] == counts['s3_get_requests'] == expected
    assert counts['s3_list_requests'] == 1
    if expected == 0:
        assert audit.coverage['windows']['trial']['metric_reasons']['s3_head_requests'] == 'zero_accounted_in_total'


def test_total_cannot_hide_failed_metric_read_or_absent_method_activity(tmp_path):
    audit, cw = collector(tmp_path)
    point = [{'Timestamp': stamp('2026-09-22T12:01:00'), 'Sum': 1.0}]
    cw.values['trial-bucket', 'AllRequests'] = point
    cw.values['trial-bucket', 'PutRequests'] = point
    assert all(v is None for v in audit.collect([window()])['trial'].values())
    cw.values['trial-bucket', 'ListRequests'] = point
    cw.values['trial-bucket', 'PutRequests'] = []
    cw.values['trial-bucket', 'HeadRequests'] = TimeoutError('metric read failed')
    assert audit.collect([window()])['trial']['s3_head_requests'] is None
