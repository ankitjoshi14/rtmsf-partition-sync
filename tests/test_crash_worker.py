import importlib
import importlib.util
import json

import pytest

from rtmsf.models import PartitionIntent, PartitionRef
from tests.test_aws_adapters import Client, Telemetry, failure
from tests.test_pmcp import Catalog, Store
from tests.test_worker import Clock, Source, delivery, event, worker


def launcher():
    assert importlib.util.find_spec("experiments.aws.crash_worker") is not None, "experiment-only launcher is required"
    return importlib.import_module("experiments.aws.crash_worker")


def plan(operation="ADD", point="before_processing", **overrides):
    document = {
        "operation": operation, "partition_path": "p=a", "point": point,
        "marker_bucket": "data-bucket", "marker_key": "_experiment/crash/trial.json",
        **overrides,
    }
    return launcher().FaultPlan.from_environment({"RTMSF_EXPERIMENT_FAULT": json.dumps(document)})


class Markers:
    def __init__(self):
        self.calls = []
        self.documents = {}

    def put_object(self, **request):
        self.calls.append(request)
        assert request["IfNoneMatch"] == "*"
        key = request["Bucket"], request["Key"]
        if key in self.documents:
            raise failure("PreconditionFailed", 412)
        self.documents[key] = json.loads(request["Body"])
        return {}


def exit86(code):
    raise SystemExit(code)


def target(mutation="ADD", path="p=a"):
    partition = PartitionRef("logical-source", "db.table", path, (path.split("=", 1)[1],), f"root/{path}/")
    return PartitionIntent(partition, mutation, (f"root/{path}/file.parquet",))


@pytest.mark.parametrize("operation", ["ADD", "DROP"])
@pytest.mark.parametrize("point", ["before_processing", "after_catalog_change"])
def test_exact_boundary_crash_has_durable_evidence_and_replays_without_early_ack(monkeypatch, operation, point):
    module = launcher()
    monkeypatch.setattr(module.os, "_exit", exit86)
    clock = Clock()
    original = delivery("first", event(operation="CREATE" if operation == "ADD" else "DELETE"))
    source = Source(clock, [original])
    store = Store((("logical-source", "root/p=a/file.parquet"),) if operation == "ADD" else ())
    catalog = Catalog()
    intent = target(operation)
    if operation == "DROP":
        catalog.entries[intent.partition.key] = "existing"
    markers = Markers()
    telemetry = module.ExperimentTelemetry(Telemetry())
    telemetry.emit({"action": "message_received", "message_id": "stable-sqs-id"})
    faults = module.ExperimentFaults(plan(operation, point), markers, telemetry, task_id="task-old")
    first = worker(source, faults.wrap_pmcp(store, catalog))
    first.run_once()
    assert not markers.calls
    clock.advance(1000)
    with pytest.raises(SystemExit) as stopped:
        first.run_once()
    assert stopped.value.code == 86
    assert not source.acknowledged
    evidence, = markers.documents.values()
    assert evidence["operation"] == operation
    assert evidence["point"] == point
    assert evidence["partition_path"] == "p=a"
    assert evidence["source_id"] == "logical-source"
    assert evidence["table_id"] == "db.table"
    assert evidence["task_id"] == "task-old"
    assert evidence["pending_message_ids"] == ["stable-sqs-id"]
    expected_changed = point == "after_catalog_change"
    assert (intent.partition.key in catalog.entries) == (expected_changed if operation == "ADD" else not expected_changed)
    assert catalog.calls["read"] == (1 if expected_changed else 0)

    source.redeliver(original)
    restarted = module.ExperimentFaults(plan(operation, point), markers, telemetry, task_id="task-new")
    second = worker(source, restarted.wrap_pmcp(store, catalog))
    second.run_once()
    assert not source.acknowledged
    clock.advance(1000)
    second.run_once()
    assert source.acknowledged == ["replay"]
    assert (intent.partition.key in catalog.entries) == (operation == "ADD")
    assert len(markers.documents) == 1
    assert catalog.calls["create" if operation == "ADD" else "delete"] == 1


@pytest.mark.parametrize("operation", ["ADD", "DROP"])
def test_after_change_does_not_crash_for_noop_or_wrong_target(monkeypatch, operation):
    module = launcher()
    monkeypatch.setattr(module.os, "_exit", exit86)
    catalog = Catalog()
    store = Store((("logical-source", "root/p=a/file.parquet"), ("logical-source", "root/p=other/file.parquet")))
    if operation == "ADD":
        catalog.entries[target().partition.key] = "existing"
    markers = Markers()
    pmcp = module.ExperimentFaults(plan(operation, "after_catalog_change"), markers, module.ExperimentTelemetry(Telemetry())).wrap_pmcp(store, catalog)
    assert pmcp.apply(target()).status == ("NOOP" if operation == "ADD" else "APPLIED")
    assert pmcp.apply(target(operation)).status == "NOOP"
    assert pmcp.apply(target("ADD", "p=other")).status == "APPLIED"
    assert not markers.calls


def test_after_change_requires_successful_catalog_response(monkeypatch):
    module = launcher()
    monkeypatch.setattr(module.os, "_exit", exit86)
    catalog = Catalog()
    catalog.create_partition = lambda partition, location: False
    markers = Markers()
    store = Store((("logical-source", "root/p=a/file.parquet"),))
    pmcp = module.ExperimentFaults(plan(point="after_catalog_change"), markers, module.ExperimentTelemetry(Telemetry())).wrap_pmcp(store, catalog)
    assert pmcp.apply(target()).status == "NOOP"
    assert not markers.calls


@pytest.mark.parametrize("count,dead_lettered", [(2, False), (6, True)])
def test_catalog_retry_injection_uses_real_pmcp_retry_and_worker_terminal_handling(count, dead_lettered):
    module = launcher()
    fault = plan(point=None, marker_bucket=None, marker_key=None, fail_catalog_calls=count)
    clock = Clock()
    source = Source(clock, [delivery()])
    store = Store((("logical-source", "root/p=a/file.parquet"),))
    catalog = Catalog()
    sink = Telemetry()
    faults = module.ExperimentFaults(fault, None, module.ExperimentTelemetry(sink))
    subject = worker(source, faults.wrap_pmcp(store, catalog), settling_ms=0, jitter_ms=lambda maximum: 0)
    for _ in range(8):
        subject.run_once()
    assert source.acknowledged == ["one"]
    assert bool(source.dead_letters) == dead_lettered
    assert bool(catalog.entries) != dead_lettered
    assert sum(item.get("action") == "catalog_failure_injected" for item in sink.records) == count
    if dead_lettered:
        assert source.dead_letters[0][1] == "max_retries:external_retryable"


@pytest.mark.parametrize("overrides", [
    {"operation": "CREATE"}, {"partition_path": ""}, {"point": "buffered"},
    {"marker_key": "root/crash.json"}, {"marker_bucket": ""},
    {"fail_catalog_calls": -1}, {"fail_catalog_calls": 2}, {"point": None},
])
def test_invalid_fault_configuration_fails_before_starting_worker(overrides):
    with pytest.raises(ValueError):
        plan(**overrides)


def test_pending_message_evidence_tracks_successful_acknowledgement():
    telemetry = launcher().ExperimentTelemetry(Telemetry())
    telemetry.emit({"action": "message_received", "message_id": "one"})
    telemetry.emit({"action": "message_received", "message_id": "two"})
    telemetry.emit({"action": "message_acknowledged", "message_id": "one"})
    assert telemetry.pending_message_ids == {"two"}


@pytest.mark.parametrize("point", ["before_processing", "after_catalog_change"])
def test_marker_failure_does_not_terminate_or_acknowledge(monkeypatch, point):
    module = launcher()
    monkeypatch.setattr(module.os, "_exit", exit86)
    clock = Clock()
    source = Source(clock, [delivery()])
    markers = Client(put_object=[failure("AccessDenied", 403)])
    store = Store((("logical-source", "root/p=a/file.parquet"),))
    pmcp = module.ExperimentFaults(plan(point=point), markers, module.ExperimentTelemetry(Telemetry())).wrap_pmcp(store, Catalog())
    subject = worker(source, pmcp, settling_ms=0)
    with pytest.raises(RuntimeError, match="could not persist experiment crash marker"):
        subject.run_once()
    assert not source.acknowledged


@pytest.mark.parametrize("operation", ["ADD", "DROP"])
def test_reconciliation_in_opposite_catalog_direction_does_not_trigger_fault(monkeypatch, operation):
    module = launcher()
    monkeypatch.setattr(module.os, "_exit", exit86)
    catalog = Catalog()
    store = Store((("logical-source", "root/p=a/file.parquet"),) if operation == "DROP" else ())
    if operation == "ADD":
        catalog.entries[target().partition.key] = "stale"
    markers = Markers()
    pmcp = module.ExperimentFaults(plan(operation, "after_catalog_change"), markers, module.ExperimentTelemetry(Telemetry())).wrap_pmcp(store, catalog)
    assert pmcp.apply(target(operation)).status == ("RETRY" if operation == "ADD" else "APPLIED")
    assert not markers.calls


@pytest.mark.parametrize("location", ["s3://data-bucket/root/", "s3://data-bucket/_experiment/"])
def test_launcher_uses_normal_aws_composition_and_rejects_routed_markers(monkeypatch, location):
    from rtmsf import aws_app as app
    from tests.test_app import environment
    from tests.test_aws_adapters import table

    module = launcher()
    glue = Client(get_table=[table(location)])
    queue = Client()
    monkeypatch.setattr(app.boto3, "client", lambda name, **kwargs: {"glue": glue, "sqs": queue}.get(name, Client()))
    config = app.AppConfig.from_environment(environment())
    if "_experiment" in location:
        with pytest.raises(ValueError, match="outside"):
            module.build_experiment_worker(config, plan())
    else:
        subject = module.build_experiment_worker(config, plan())
        subject.run_once()
        assert len(glue.calls["get_table"]) == 1
        assert len(queue.calls["receive_message"]) == 1


def test_restart_marker_call_fits_budget_even_on_slowest_real_pmcp_path(monkeypatch):
    from rtmsf import aws_app as app
    from tests.test_app import environment
    from tests.test_aws_adapters import message, record, table

    module = launcher()
    clock = Clock()

    class SlowClient(Client):
        def __getattr__(self, name):
            operation = super().__getattr__(name)

            def call(**request):
                clock.advance(3000)
                return operation(**request)
            return call

    glue = SlowClient(get_table=[table()], get_partition=[failure("EntityNotFoundException")])
    s3 = SlowClient(
        put_object=[failure("PreconditionFailed", 412)],
        head_object=[failure("404", 404)] * 3,
        list_objects_v2=[{"KeyCount": 1}],
    )
    queue = Client(receive_message=[{"Messages": [message({"Records": [record(key=f"root/dt=t/{index}.parquet")]}, index) for index in range(3)]}])
    monkeypatch.setattr(app.boto3, "client", lambda name, **kwargs: {"glue": glue, "s3": s3, "sqs": queue}[name])
    monkeypatch.setattr("rtmsf.worker.time.monotonic_ns", lambda: clock() * 1_000_000)
    subject = module.build_experiment_worker(app.AppConfig.from_environment(environment()), plan(partition_path="dt=t"))
    subject.run_once()
    clock.advance(10000)
    subject.run_once()
    assert len(glue.calls["create_partition"]) == 1
    assert len(queue.calls["delete_message_batch"]) == 3
