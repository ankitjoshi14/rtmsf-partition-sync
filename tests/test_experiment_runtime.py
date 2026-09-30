import pytest


def test_evidence_bundle_refuses_overwrite(tmp_path):
    from experiments.evidence import EvidenceBundle

    bundle = EvidenceBundle(tmp_path / "run", {"run_id": "run"})
    bundle.write("snapshots/initial.json", {"partitions": []})
    with pytest.raises(FileExistsError):
        bundle.write("snapshots/initial.json", {})
    with pytest.raises(FileExistsError):
        EvidenceBundle(tmp_path / "run", {})



def test_nonempty_dlq_invalidates_success():
    from experiments.aws.runtime import assert_empty_dlq

    assert_empty_dlq({"ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "0"})
    with pytest.raises(AssertionError, match="dead-letter"):
        assert_empty_dlq({"ApproximateNumberOfMessages": "1", "ApproximateNumberOfMessagesNotVisible": "0"})


def test_crash_evidence_requires_exit86_and_nonoverlapping_successor():
    from experiments.aws.runtime import assert_crash_trace

    crashed = {"taskArn": "old", "stoppedAt": 2, "containers": [{"name": "worker", "exitCode": 86}]}
    restarted = {"taskArn": "new", "startedAt": 3}
    assert_crash_trace([crashed], [restarted])
    with pytest.raises(AssertionError):
        assert_crash_trace([], [restarted])
    with pytest.raises(AssertionError):
        assert_crash_trace([crashed], [{"taskArn": "new", "startedAt": 1}])
    with pytest.raises(AssertionError):
        assert_crash_trace([crashed], [restarted, {"taskArn": "another", "startedAt": 4}])


def test_api_counting_deduplicates_log_records_but_preserves_real_attempts():
    from experiments.aws.runtime import runtime_operations

    record = {"cloudwatch_event_id": "same", "record": {"component": "aws_adapter", "action": "api_call", "operation": "S3.HeadObject", "attempts": 2, "timestamp": "2026-09-10T00:00:05+00:00"}}
    another = {"cloudwatch_event_id": "second", "record": {**record["record"], "attempts": 1}}
    operations, counts = runtime_operations([record, record, another])
    assert len(operations) == 2
    assert counts == {"S3.HeadObject": 3}


def test_stopped_worker_waits_for_new_configuration_before_scaling(monkeypatch):
    from types import SimpleNamespace
    from experiments.aws import runtime

    run = object.__new__(runtime.AwsRuntime)
    run.timeout, run.task_definition = 30, "new"
    stopped = {"pendingCount": 0, "runningCount": 0}
    states = [
        {**stopped, "taskDefinition": "old", "deployments": [{"taskDefinition": "old"}]},
        {**stopped, "taskDefinition": "new", "deployments": [{"taskDefinition": "new"}, {"taskDefinition": "old"}]},
        {**stopped, "taskDefinition": "new", "deployments": [{"taskDefinition": "new"}]},
    ]
    run.service = lambda: states.pop(0) if states else {**stopped, 'runningCount': 1}
    run.require_worker_stopped = lambda: None
    def scale(**request):
        assert not states, 'must not scale while the old deployment remains'
        assert request['desiredCount'] == 1
    run.clients = {'ecs': SimpleNamespace(update_service=scale,
        list_tasks=lambda **kw: {'taskArns': ['task']},
        describe_tasks=lambda **kw: {'tasks': [{'taskDefinitionArn': 'new'}]})}
    run.outputs = {'ClusterName': 'cluster', 'ServiceName': 'service'}
    run.lifecycle = []
    run.bundle = SimpleNamespace(write=lambda *args: None)
    monkeypatch.setattr(runtime.time, "sleep", lambda seconds: None)
    run.start_worker()
    assert not states and len(run.lifecycle) == 1


def test_query_verification_reads_beyond_one_hundred_pages():
    from types import SimpleNamespace
    from experiments.aws.runtime import AwsRuntime

    calls = []
    def page(**request):
        index = int(request.get('NextToken', 0))
        calls.append(index)
        return {'ResultSet': {'Rows': [{'Data': [{'VarCharValue': str(index)}]}]},
                **({'NextToken': str(index + 1)} if index < 100 else {})}
    runtime = AwsRuntime.__new__(AwsRuntime)
    runtime.clients = {'athena': SimpleNamespace(
        start_query_execution=lambda **kwargs: {'QueryExecutionId': 'query'},
        get_query_execution=lambda **kwargs: {'QueryExecution': {'Status': {'State': 'SUCCEEDED'}}},
        get_query_results=page)}
    runtime.outputs = {'GlueDatabaseName': 'db', 'AthenaWorkGroupName': 'wg', 'AthenaResultsBucketName': 'results'}
    runtime.timeout, runtime.run_id, runtime.queries = 600, 'test', []
    runtime.bundle = SimpleNamespace(write=lambda *args: None)
    rows, record = runtime.query('SELECT * FROM example', 'large-verification')
    assert calls == list(range(101)) and len(rows) == 101
    assert record['rows'] == rows
