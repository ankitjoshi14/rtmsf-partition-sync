"""Offline crash points over the real guarded native GCP composition."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime
import importlib
import importlib.util
import json
import os
import signal
from types import SimpleNamespace

import pytest
from pymetastore.hive_metastore import ttypes

from rtmsf.models import MutationResult, PartitionIntent, PartitionRef
from tests.test_gcp_functional import (
    BUCKET, NOTIFICATION, ROOT, RUN, TABLE, argv, no_network_escape, read_events, setup,
)
from tests.test_gcp_pubsub import message, no_unmocked_gapic_rpc


POINTS = ("before_add", "after_add", "before_drop", "after_drop")


def harness():
    assert importlib.util.find_spec("experiments.gcp.crash_worker") is not None, "GCP crash harness missing"
    return importlib.import_module("experiments.gcp.crash_worker")


def intent(point):
    timing, mutation = point.split("_")
    value = "recovery_" + mutation + "_" + timing
    path = "dt=" + value
    return PartitionIntent(PartitionRef("fixtures", TABLE, path, (value,), ROOT + path + "/"), mutation.upper())


class Killed(BaseException):
    pass


def proxy(point, tmp_path, result=MutationResult("APPLIED", "created")):
    fault = harness()
    target = intent(point)
    correlation = {"handle": "opaque-handle", "message_id": "native-message-id", "bucketId": BUCKET,
                   "objectId": target.partition.storage_prefix + "file.parquet", "objectGeneration": "17",
                   "eventType": "OBJECT_FINALIZE", "eventTime": "2026-10-19T20:25:00Z", "notificationConfig": NOTIFICATION,
                   "unknown": "AUTH-AND-RECEIPT-SENTINEL", "ackId": "synthetic-receipt"}
    source = SimpleNamespace(_correlations={"opaque-handle": correlation}, _pending={"opaque-handle": object()})
    worker = SimpleNamespace(_source=source, _handles_by_partition={target.partition.key: {"opaque-handle"}})
    calls = []
    delegate = SimpleNamespace(apply=lambda value: calls.append(value) or result)
    return fault._CrashPMCP(delegate, point, worker, tmp_path), target, calls, worker


@pytest.mark.parametrize("point", POINTS)
def test_only_exact_approved_intent_matches(point):
    fault = harness()
    target = intent(point)
    assert fault.matches(point, target)
    assert not fault.matches(point, None)
    assert not fault.matches("unknown", target)
    assert not fault.matches(point, replace(target, mutation="DROP" if target.mutation == "ADD" else "ADD"))
    for changes in ({"source_id": "other"}, {"table_id": "other.table"}, {"partition_path": "dt=other"},
                    {"values": ("other",)}, {"storage_prefix": ROOT + "dt=other/"},
                    {"storage_prefix": target.partition.storage_prefix[:-1]}):
        assert not fault.matches(point, replace(target, partition=replace(target.partition, **changes)))


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("changes", [{"source_id": "other"}, {"table_id": "other.table"},
    {"partition_path": "dt=other"}, {"values": ("other",)}, {"storage_prefix": ROOT + "dt=other/"},
    {"mutation": "opposite"}])
def test_unrelated_intent_delegates_without_crashing(monkeypatch, tmp_path, point, changes):
    adapter, target, calls, _ = proxy(point, tmp_path)
    monkeypatch.setattr(harness().os, "kill", lambda *_a: pytest.fail("unrelated intent crashed"))
    if "mutation" in changes:
        target = replace(target, mutation="DROP" if target.mutation == "ADD" else "ADD")
    else:
        target = replace(target, partition=replace(target.partition, **changes))
    assert adapter.apply(target) == MutationResult("APPLIED", "created")
    assert calls == [target]
    assert not (tmp_path / "fault-marker.json").exists()


@pytest.mark.parametrize("point", ("after_add", "after_drop"))
@pytest.mark.parametrize("result", [MutationResult("NOOP", "created"), MutationResult("RETRY", "deleted"),
    MutationResult("ERROR", "created"), MutationResult("APPLIED", "other"), MutationResult("APPLIED", "AUTH-SENTINEL")])
def test_after_point_requires_desired_actual_applied_result(monkeypatch, tmp_path, point, result):
    adapter, target, calls, _ = proxy(point, tmp_path, result)
    monkeypatch.setattr(harness().os, "kill", lambda *_a: pytest.fail("unchanged result crashed"))
    assert adapter.apply(target) is result
    assert calls == [target]
    assert not (tmp_path / "fault-marker.json").exists()


@pytest.mark.parametrize("point,reason", [("after_drop", "created"), ("after_add", "deleted")])
def test_opposite_catalog_mutation_does_not_trigger(monkeypatch, tmp_path, point, reason):
    result = MutationResult("APPLIED", reason)
    adapter, target, calls, _ = proxy(point, tmp_path, result)
    monkeypatch.setattr(harness().os, "kill", lambda *_a: pytest.fail("repair ADD must not crash after DROP"))
    assert adapter.apply(target) is result and calls == [target]


@pytest.mark.parametrize("point", POINTS)
def test_marker_is_safe_exclusive_and_fsynced_before_kill(monkeypatch, tmp_path, point):
    fault = harness()
    reason = "created" if point.endswith("add") else "deleted"
    adapter, target, calls, worker = proxy(point, tmp_path, MutationResult("APPLIED", reason))
    worker._source._correlations["other-handle"] = {"message_id": "unrelated"}
    synced = []
    original_fsync = fault.os.fsync

    def fsync(descriptor):
        synced.append(os.fstat(descriptor).st_ino)
        original_fsync(descriptor)

    def kill(pid, number):
        assert pid == os.getpid() and number == signal.SIGKILL
        marker = tmp_path / "fault-marker.json"
        assert synced == [marker.stat().st_ino, tmp_path.stat().st_ino]
        assert calls == ([] if point.startswith("before") else [target])
        raise Killed()

    monkeypatch.setattr(fault.os, "fsync", fsync)
    monkeypatch.setattr(fault.os, "kill", kill)
    with pytest.raises(Killed):
        adapter.apply(target)
    record = json.loads((tmp_path / "fault-marker.json").read_text())
    assert record["point"] == point and record["pid"] == os.getpid()
    assert datetime.fromisoformat(record["utc"]).utcoffset().total_seconds() == 0
    assert record["monotonic_seconds"] >= 0
    assert record["intent"] == {"source_id": "fixtures", "table_id": TABLE, "partition_path": target.partition.partition_path,
                                "values": list(target.partition.values), "storage_prefix": target.partition.storage_prefix,
                                "mutation": target.mutation}
    assert record.get("result") == (None if point.startswith("before") else {"status": "APPLIED", "reason": reason})
    event, = record["pending_events"]
    assert event["message_id"] == "native-message-id" and event["handle"] == "opaque-handle"
    assert event["objectId"] == target.partition.storage_prefix + "file.parquet"
    assert set(event) == {"handle", "message_id", *fault.run.NOTIFICATION_FIELDS}
    assert not any(value in json.dumps(record) for value in ("SENTINEL", "synthetic-receipt", "ackId", "unrelated"))
    with pytest.raises(FileExistsError):
        adapter.apply(target)


@pytest.mark.parametrize("missing", ("correlation", "pending", "partition_handles"))
def test_missing_pending_correlation_fails_without_kill(monkeypatch, tmp_path, missing):
    adapter, target, calls, worker = proxy("before_add", tmp_path)
    if missing == "correlation":
        worker._source._correlations.clear()
    elif missing == "pending":
        worker._source._pending.clear()
    else:
        worker._handles_by_partition.clear()
    monkeypatch.setattr(harness().os, "kill", lambda *_a: pytest.fail("uncorrelated crash"))
    with pytest.raises(RuntimeError, match="gcp_native_fault_missing_correlation"):
        adapter.apply(target)
    assert calls == [] and not (tmp_path / "fault-marker.json").exists()


@pytest.mark.parametrize("field,value", [("bucketId", "other"), ("objectId", ROOT + "dt=other/file"),
    ("notificationConfig", "other"), ("eventType", "unknown"), ("handle", "other"), ("message_id", "")])
def test_mismatched_correlation_fails_without_kill(monkeypatch, tmp_path, field, value):
    adapter, target, calls, worker = proxy("before_add", tmp_path)
    worker._source._correlations["opaque-handle"][field] = value
    monkeypatch.setattr(harness().os, "kill", lambda *_a: pytest.fail("mismatched correlation crashed"))
    with pytest.raises(RuntimeError, match="gcp_native_fault_missing_correlation"):
        adapter.apply(target)
    assert not calls and not (tmp_path / "fault-marker.json").exists()


def test_marker_fsync_failure_never_kills(monkeypatch, tmp_path):
    adapter, target, calls, _ = proxy("before_add", tmp_path)

    def fail(_descriptor):
        raise OSError("AUTH-AND-RECEIPT-SENTINEL")

    monkeypatch.setattr(harness().os, "fsync", fail)
    monkeypatch.setattr(harness().os, "kill", lambda *_a: pytest.fail("undurable marker crashed"))
    with pytest.raises(OSError):
        adapter.apply(target)
    assert calls == []


def test_unexpected_kill_return_never_returns_to_worker(monkeypatch, tmp_path):
    adapter, target, calls, _ = proxy("before_add", tmp_path)
    monkeypatch.setattr(harness().os, "kill", lambda *_a: None)
    with pytest.raises(RuntimeError, match="gcp_native_fault_kill_returned"):
        adapter.apply(target)
    assert not calls


@pytest.mark.parametrize("point", POINTS)
def test_real_worker_crashes_with_lock_and_pending_input_before_ack(monkeypatch, tmp_path, point):
    fault = harness()
    target = intent(point)
    key = target.partition.storage_prefix + "file.parquet"
    item = message("synthetic-receipt", key=key, kind="OBJECT_FINALIZE" if target.mutation == "ADD" else "OBJECT_DELETE")
    item.message.attributes["unknown"] = "AUTH-AND-RECEIPT-SENTINEL"
    app, state = setup(monkeypatch, tmp_path, [item])
    if target.mutation == "ADD":
        state.http.keys[key] = b"live"
    else:
        descriptor = deepcopy(state.server.table.sd)
        descriptor.location = f"gs://{BUCKET}/" + target.partition.storage_prefix[:-1]
        state.server.partitions[target.partition.values] = ttypes.Partition(values=list(target.partition.values),
            dbName=TABLE.split(".")[0], tableName=TABLE.split(".")[1], sd=descriptor,
            parameters={"gcp_probe_run_id": RUN}, writeId=None)
    lock = tmp_path / "worker.lock"
    monkeypatch.setattr(app, "LOCK_PATH", lock)
    builder = app.build_worker

    def kill(pid, number):
        with pytest.raises(RuntimeError, match="gcp_native_worker_already_running"):
            with app._host_lock(lock):
                pytest.fail("host lock released before crash")
        assert not state.closed and "acknowledge" not in state.boundary.actions()
        marker = json.loads((tmp_path / "output" / "fault-marker.json").read_text())
        assert marker["pending_events"][0]["message_id"] == "server-message-id"
        assert marker["pending_events"][0]["objectGeneration"] == "17"
        mutations = [call[1] for call in state.server.calls if call[1] in ("add_partition", "drop_partition")]
        assert mutations == ([] if point.startswith("before") else ["add_partition" if target.mutation == "ADD" else "drop_partition"])
        assert bool(state.server.partitions) == ((target.mutation == "DROP") if point.startswith("before") else (target.mutation == "ADD"))
        raise Killed()

    monkeypatch.setattr(fault.os, "kill", kill)
    with pytest.raises(Killed):
        fault.main(argv(tmp_path) + ["--fault-point", point])
    assert app.build_worker is builder
    assert not any(record["action"] == "message_acknowledged" for record in read_events(tmp_path / "output"))
    assert not any(value in (tmp_path / "output" / "fault-marker.json").read_text() for value in ("SENTINEL", "synthetic-receipt"))


def test_finite_return_without_fault_reports_nonzero_and_restores_builder(monkeypatch, tmp_path, capsys):
    fault = harness()
    app, state = setup(monkeypatch, tmp_path)
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    builder = app.build_worker
    assert fault.main(argv(tmp_path, "1") + ["--fault-point", "before_add"]) == 1
    assert app.build_worker is builder and set(state.closed) == {"storage", "subscriber", "publisher"}
    assert json.loads((tmp_path / "output" / "fault-report.json").read_text()) == {"status": "not_triggered", "point": "before_add"}
    assert '"status": "not_triggered"' in capsys.readouterr().out
    assert not (tmp_path / "output" / "fault-marker.json").exists()


def test_real_drop_repair_add_finishes_without_after_drop_fault(monkeypatch, tmp_path):
    fault = harness()
    target = intent("after_drop")
    key = target.partition.storage_prefix + "still-live.parquet"
    app, state = setup(monkeypatch, tmp_path, [message(key=key, kind="OBJECT_DELETE")])
    state.http.keys[key] = b"live"
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    monkeypatch.setattr(fault.os, "kill", lambda *_a: pytest.fail("DROP repair ADD crashed"))
    assert fault.main(argv(tmp_path) + ["--fault-point", "after_drop"]) == 1
    assert set(state.server.partitions) == {target.partition.values}
    assert [call[1] for call in state.server.calls if call[1] in ("add_partition", "drop_partition")] == ["add_partition"]
    assert "acknowledge" in state.boundary.actions()
    assert json.loads((tmp_path / "output" / "fault-report.json").read_text())["status"] == "not_triggered"
    assert not (tmp_path / "output" / "fault-marker.json").exists()


def test_missing_correlation_in_real_runner_fails_safely_and_keeps_pending(monkeypatch, tmp_path, capsys):
    fault = harness()
    target = intent("before_add")
    key = target.partition.storage_prefix + "file.parquet"
    app, state = setup(monkeypatch, tmp_path, [message(key=key)])
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    remember = app.EvidenceSource._remember

    def forget(source, deliveries):
        remember(source, deliveries)
        source._correlations.clear()

    monkeypatch.setattr(app.EvidenceSource, "_remember", forget)
    monkeypatch.setattr(fault.os, "kill", lambda *_a: pytest.fail("missing correlation crashed"))
    builder = app.build_worker
    assert fault.main(argv(tmp_path) + ["--fault-point", "before_add"]) == 1
    assert app.build_worker is builder
    report = json.loads((tmp_path / "output" / "worker-report.json").read_text())
    assert report["status"] == "failed" and report["pending_deliveries"] == 1
    assert "acknowledge" not in state.boundary.actions() and "publish" not in state.boundary.actions()
    assert not (tmp_path / "output" / "fault-marker.json").exists()
    assert capsys.readouterr().err == "gcp_native_worker_failed\n"


def test_startup_failure_restores_builder_and_suppresses_raw_exception(monkeypatch, tmp_path, capsys):
    fault = harness()
    app, state = setup(monkeypatch, tmp_path)
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    builder = app.build_worker

    def fail(**_kw):
        raise RuntimeError("AUTH-AND-RECEIPT-SENTINEL")

    monkeypatch.setattr(app, "StorageClient", fail)
    assert fault.main(argv(tmp_path) + ["--fault-point", "after_drop"]) == 1
    assert app.build_worker is builder
    captured = capsys.readouterr()
    assert captured.err == "gcp_native_worker_failed\n"
    assert "SENTINEL" not in captured.out + captured.err + (tmp_path / "output" / "worker-report.json").read_text()


@pytest.mark.parametrize("extra", [["--fault-point", "AUTH-SENTINEL"], ["--fault-point", "before_add", "--partition", "other"],
                                  ["--fault-point", "before_add", "--duration-seconds", "AUTH-SENTINEL"], []])
def test_invalid_cli_never_builds_or_exposes_arguments(monkeypatch, tmp_path, capsys, extra):
    fault = harness()
    builder = fault.run.build_worker
    monkeypatch.setattr(fault.run, "StorageClient", lambda **_kw: pytest.fail("invalid arguments reached cloud"))
    assert fault.main(argv(tmp_path) + extra) == 1
    assert fault.run.build_worker is builder
    captured = capsys.readouterr()
    assert captured.err == "gcp_native_worker_failed\n" and "SENTINEL" not in captured.out + captured.err
    assert not (tmp_path / "output").exists()
