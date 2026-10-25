"""Finite native GCP composition through offline generated provider boundaries."""

from copy import deepcopy
import hashlib
import importlib
import importlib.util
import json
from pathlib import Path
import signal
from types import SimpleNamespace
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
import requests
from google.auth.credentials import AnonymousCredentials
from google.cloud.storage import Client
from pymetastore.hive_metastore import ttypes

from rtmsf.adapters import gcp
from rtmsf.interfaces import FatalExternalError, RetryableExternalError
from tests.test_gcp_hms_catalog import Server
from tests.test_gcp_pubsub import (
    Boundary, BUCKET, NOTIFICATION, PROJECT, QUARANTINE, SUBSCRIPTION, TOPIC,
    message, no_unmocked_gapic_rpc,
)


RUN = "r20261019n1"
ROOT = "gcp-native/native-r20261019n1/data/"
TABLE = "gcp_native_r20261019n1.native_events"
OWNER = {"name": ROOT + "_OWNER.json", "generation": "1792445543821304", "size": 103,
         "sha256": "32e03b825a089d8d1d2992de3fe373b5db72c39bb8a809f6c3c1cfdd8c1190e6"}
OWNER_BYTES = json.dumps({"run_id": RUN, "root": ROOT, "scope": "owned native tests"}, sort_keys=True).encode()


def app_module():
    assert importlib.util.find_spec("experiments.gcp") is not None, "finite native GCP runner not implemented"
    return importlib.import_module("experiments.gcp.run")


def manifest():
    return {
        "project_id": PROJECT, "account": "joshi.ankit1492@gmail.com",
        "metastore_endpoint": "thrift://10.76.0.4:9083",
        "catalog_gate": {"status": "passed", "scope": "catalog-safety-only", "complete_steps": 34},
        "native_run": {"run_id": RUN, "table_id": TABLE, "source_id": "fixtures", "bucket": BUCKET,
                       "root_prefix": ROOT, "partition_columns": [["dt", "string"]],
                       "owner_key": "gcp_probe_run_id", "owner_value": RUN, "owner_object": deepcopy(OWNER)},
        "native_pipeline_setup": {
            "ingress_topic": TOPIC, "subscription": SUBSCRIPTION, "quarantine_topic": QUARANTINE,
            "quarantine_subscription": f"projects/{PROJECT}/subscriptions/rtmsf-native-quarantine-pull",
            "notification_config_attribute": NOTIFICATION, "notification_id": "3",
            "notification_prefix": "gcp-native/", "initial_ack_deadline_seconds": 180,
        },
    }


def resources(tmp_path, document=None):
    path = tmp_path / "resources.json"
    path.write_text(json.dumps(manifest() if document is None else document))
    return path


def read_events(root):
    return [json.loads(line) for line in (root / "worker-evidence.jsonl").read_text().splitlines()]


def test_exact_manifest_gate_and_owner_are_required(tmp_path):
    app = app_module()
    config = app.load_resources(resources(tmp_path))
    assert config["native_run"]["owner_object"] == OWNER
    assert hashlib.sha256(OWNER_BYTES).hexdigest() == OWNER["sha256"]
    assert app.LOCK_PATH == Path.home() / ".rtmsf-gcp-native.lock"


IDENTITIES = [(section, key) for section in ("native_run", "native_pipeline_setup")
              for key in manifest()[section]] + [(None, "project_id"), (None, "account"), (None, "metastore_endpoint")]


@pytest.mark.parametrize("section,key", IDENTITIES)
def test_manifest_identity_drift_is_rejected_without_echo(tmp_path, section, key):
    app = app_module()
    document = manifest()
    target = document if section is None else document[section]
    target[key] = "AUTH-AND-RECEIPT-SENTINEL"
    with pytest.raises(ValueError) as caught:
        app.load_resources(resources(tmp_path, document))
    assert "SENTINEL" not in str(caught.value)


@pytest.mark.parametrize("gate", [None, {}, {"status": "ACTIVE"}, {"verified": False}, {"passed": True}])
def test_provisioning_success_cannot_replace_catalog_gate(tmp_path, gate):
    document = manifest()
    document.update(catalog_gate=gate, live_catalog_verified=True, metastore_observed_state="ACTIVE")
    with pytest.raises(ValueError):
        app_module().load_resources(resources(tmp_path, document))


class StorageHTTP(requests.Session):
    is_mtls = False

    def __init__(self, clock):
        super().__init__()
        self.clock = clock
        self.calls = []
        self.keys = {OWNER["name"]: OWNER_BYTES}
        self.owner_metadata = {"name": OWNER["name"], "bucket": BUCKET,
                               "generation": OWNER["generation"], "size": str(OWNER["size"])}
        self.owner_body = OWNER_BYTES
        self.delay = 0

    def request(self, method, url, **kwargs):
        assert method == "GET", "worker must never write fixtures"
        assert urlsplit(url).hostname == "storage.googleapis.com"
        assert 0 < kwargs["timeout"] <= 15
        self.calls.append((url, kwargs))
        self.clock[0] += self.delay
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        key = unquote(parsed.path.split("/o/", 1)[1]) if "/o/" in parsed.path else None
        status = 200
        if query.get("alt") == ["media"]:
            assert key == OWNER["name"]
            assert query["ifGenerationMatch"] == [OWNER["generation"]]
            assert kwargs["headers"]["range"] == "bytes=0-102"
            body = self.owner_body
        elif key == OWNER["name"]:
            body = self.owner_metadata
        elif key is not None:
            status = 200 if key in self.keys else 404
            body = {"name": key, "bucket": BUCKET, "generation": "18", "size": "1"}
        elif parsed.path.endswith("/o"):
            prefix = query["prefix"][0]
            found = next((name for name in self.keys if name.startswith(prefix)), None)
            body = {} if found is None else {"items": [{"name": found, "bucket": BUCKET}]}
        else:
            body = {"name": BUCKET}
        response = requests.Response()
        response.status_code = status
        response._content = body if isinstance(body, bytes) else json.dumps(body).encode()
        response.headers["content-type"] = "application/octet-stream" if isinstance(body, bytes) else "application/json"
        response.request = requests.Request(method, url).prepare()
        response._content_consumed = True
        response.raw = SimpleNamespace(headers={}, close=lambda: None)
        return response


@pytest.fixture(autouse=True)
def no_network_escape(monkeypatch):
    monkeypatch.setattr(requests.Session, "request", lambda *_a, **_kw: pytest.fail("unmocked HTTP RPC"))
    monkeypatch.setattr(gcp, "_hms_connection", lambda *_a: pytest.fail("unmocked HMS connection"))


def setup(monkeypatch, tmp_path, messages=()):
    app = app_module()
    boundary = Boundary(messages)
    http = StorageHTTP(boundary.clock)
    client = Client(project=PROJECT, credentials=AnonymousCredentials(), _http=http)
    calls, closed = [], []
    server = Server(boundary.clock)
    server.database = ttypes.Database(name=TABLE.split(".")[0], parameters={"gcp_probe_run_id": RUN})
    server.table.dbName, server.table.tableName = TABLE.split(".")
    server.table.sd.location = f"gs://{BUCKET}/{ROOT[:-1]}"
    server.table.parameters["gcp_probe_run_id"] = RUN
    original_client = app.StorageClient

    def storage_client(**options):
        calls.append(options)
        assert original_client is Client and options == {"project": PROJECT}
        return client

    monkeypatch.setattr(app, "StorageClient", storage_client)
    monkeypatch.setattr(app, "create_pubsub_clients", lambda: (boundary.subscriber, boundary.publisher))
    monkeypatch.setattr(gcp, "_hms_connection", server.factory)
    monkeypatch.setattr(app.time, "monotonic", lambda: boundary.clock[0])
    monkeypatch.setattr(app.time, "monotonic_ns", lambda: int(boundary.clock[0] * 1_000_000_000))
    monkeypatch.setattr(app.time, "sleep", lambda duration: boundary.clock.__setitem__(0, boundary.clock[0] + duration))
    client.close = lambda: closed.append("storage")
    boundary.subscriber.transport.close = lambda: closed.append("subscriber")
    boundary.publisher.transport.close = lambda: closed.append("publisher")
    return app, SimpleNamespace(boundary=boundary, http=http, server=server, calls=calls, closed=closed,
                                 config=app.load_resources(resources(tmp_path)))


def test_real_composition_shares_whole_pmcp_budget_and_closes_clients(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path)
    with (tmp_path / "events.jsonl").open("x") as output:
        with app.build_worker(state.config, app.JsonEvidence(output)) as worker:
            protocol = worker._pmcp._pmcp
            assert isinstance(protocol, app.PartitionMutationCommitProtocol)
            assert isinstance(protocol._storage, gcp.GcsObjectStore)
            assert isinstance(protocol._catalog, gcp.HiveMetastorePartitionCatalog)
            assert protocol._storage._deadline is protocol._catalog._deadline is worker._pmcp._deadline
            assert worker._pmcp._deadline.seconds == 90 and worker._pmcp._deadline.rpc_seconds == 15
            assert worker._source._deadline is not worker._pmcp._deadline and worker._source._deadline.seconds == 30
            assert (worker._queue_lease_seconds, worker._pmcp_budget_ms, worker._source_budget_ms, worker._margin_ms) == (180, 90000, 30000, 1000)
            assert worker._poll_size <= 10 and worker._max_pending_deliveries == 20
            assert worker._buffer._settling_ms == 1000 and worker._buffer._maximum_ms == 3000
            assert len(state.http.calls) == 2  # owner-only baseline; no root-empty gate
            assert not state.closed
    assert set(state.closed) == {"storage", "subscriber", "publisher"}
    assert all(connection.closed for connection in state.server.connections)


@pytest.mark.parametrize("field,value", [("name", ROOT + "different"), ("bucket", "other-bucket"),
    ("generation", "1792445543821305"), ("size", "104"), ("body", b"wrong"), ("body", OWNER_BYTES + b"x")])
def test_owner_readback_is_exact_bounded_and_stops_before_pull(monkeypatch, tmp_path, field, value):
    app, state = setup(monkeypatch, tmp_path)
    if field == "body":
        state.http.owner_body = value
    else:
        state.http.owner_metadata[field] = value
    with (tmp_path / "events.jsonl").open("x") as output:
        with pytest.raises(ValueError, match="gcp_native_owner_mismatch"):
            with app.build_worker(state.config, app.JsonEvidence(output)):
                pytest.fail("unowned worker started")
    assert "pull" not in state.boundary.actions() and state.closed == ["storage"]


def test_native_add_drop_and_stale_generation_use_current_truth_with_safe_correlations(monkeypatch, tmp_path):
    key = ROOT + "dt=a/file.parquet"
    item = message(key=key)
    item.message.attributes["unknown"] = "AUTH-AND-RECEIPT-SENTINEL"
    app, state = setup(monkeypatch, tmp_path, [item])
    state.http.keys[key] = b"live"
    root = tmp_path / "output"
    root.mkdir()
    with (root / "worker-evidence.jsonl").open("x") as output:
        with app.build_worker(state.config, app.JsonEvidence(output)) as worker:
            worker.run_once()
            assert "acknowledge" not in state.boundary.actions()
            state.boundary.clock[0] += 1
            worker.run_once()
            assert set(state.server.partitions) == {("a",)}
            for index, kind in enumerate(("OBJECT_DELETE", "OBJECT_ARCHIVE")):
                state.boundary.messages = [message(f"synthetic-receipt-{index}", key=key, kind=kind)]
                worker.run_once()
                state.boundary.clock[0] += 1
                worker.run_once()
                assert set(state.server.partitions) == {("a",)}
            state.http.keys.pop(key)
            state.boundary.messages = [message("synthetic-receipt-last", key=key, kind="OBJECT_DELETE")]
            worker.run_once()
            state.boundary.clock[0] += 1
            worker.run_once()
            assert not state.server.partitions
    records = read_events(root)
    events = [record for record in records if record["action"] == "event_received"]
    acks = [record for record in records if record["action"] == "message_acknowledged"]
    pmcp = [record for record in records if record["action"] == "pmcp_result"]
    assert len(events) == len(acks) == 4
    assert events[0]["message_id"] == "server-message-id"
    assert events[0]["objectId"] == key and events[0]["objectGeneration"] == "17"
    assert events[0]["handle"] == acks[0]["handle"]
    assert records.index(events[0]) < records.index(pmcp[0]) < records.index(acks[0])
    assert [(r["mutation"], r["status"]) for r in pmcp] == [("ADD", "APPLIED"), ("DROP", "NOOP"), ("DROP", "NOOP"), ("DROP", "APPLIED")]
    assert all("utc" in record and "monotonic_seconds" in record for record in records)
    text = (root / "worker-evidence.jsonl").read_text()
    assert not any(value in text for value in ("AUTH-AND-RECEIPT-SENTINEL", "private-ack-id", "synthetic-receipt", "data_base64", "custom"))
    assert all(call[2][-1] is False for call in state.server.calls if call[1] == "drop_partition")


def test_elapsed_pmcp_budget_crosses_hms_and_storage_and_prevents_ack(monkeypatch, tmp_path):
    key = ROOT + "dt=a/file.parquet"
    app, state = setup(monkeypatch, tmp_path, [message(key=key)])
    state.http.keys[key] = b"live"
    with (tmp_path / "events.jsonl").open("x") as output:
        with app.build_worker(state.config, app.JsonEvidence(output)) as worker:
            worker.run_once()
            state.boundary.clock[0] += 1
            state.server.hooks["get_partition"] = lambda *_a: state.boundary.clock.__setitem__(0, state.boundary.clock[0] + 76)
            state.http.delay = 14
            with pytest.raises(RetryableExternalError, match="gcp_operation_deadline_exceeded"):
                worker.run_once()
            assert "acknowledge" not in state.boundary.actions()
            assert not state.server.partitions
            assert worker._source._pending


def test_malformed_and_terminal_quarantine_records_flush_before_successful_ack(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path, [message(data=b"AUTH-AND-RECEIPT-SENTINEL")])
    root = tmp_path / "output"
    root.mkdir()
    with (root / "worker-evidence.jsonl").open("x") as output:
        with app.build_worker(state.config, app.JsonEvidence(output)) as worker:
            worker.run_once()
            records = read_events(root)  # writes are visible before file close
            assert [record["action"] for record in records] == ["event_received", "lease_renewed", "quarantine_confirmed", "message_acknowledged"]
            assert records[0]["message_id"] == records[-1]["message_id"]
            assert records[-2]["category"] == "malformed"
            assert state.boundary.actions()[-2:] == ["publish", "acknowledge"]
    assert "AUTH-AND-RECEIPT-SENTINEL" not in (root / "worker-evidence.jsonl").read_text()


@pytest.mark.parametrize("operation", ["modify_ack_deadline", "publish", "acknowledge"])
def test_uncertain_source_action_never_logs_confirmation(monkeypatch, tmp_path, operation):
    from google.api_core.exceptions import DeadlineExceeded

    app, state = setup(monkeypatch, tmp_path, [message(data=b"malformed")])
    state.boundary.failures[operation] = DeadlineExceeded("AUTH-AND-RECEIPT-SENTINEL")
    root = tmp_path / "output"
    root.mkdir()
    with (root / "worker-evidence.jsonl").open("x") as output:
        with app.build_worker(state.config, app.JsonEvidence(output)) as worker:
            with pytest.raises(RetryableExternalError):
                worker.run_once()
            records = read_events(root)
            assert not any(record["action"] == "message_acknowledged" for record in records)
            if operation != "acknowledge":
                assert not any(record["action"] == "quarantine_confirmed" for record in records)
            assert len(worker._source._pending) == 1
    assert "SENTINEL" not in (root / "worker-evidence.jsonl").read_text()


def test_terminal_catalog_failure_logs_successful_quarantine_before_ack(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path, [message(key=ROOT + "dt=a/file")])
    root = tmp_path / "output"
    root.mkdir()

    def fail(*_args):
        raise RuntimeError("AUTH-AND-RECEIPT-SENTINEL")

    with (root / "worker-evidence.jsonl").open("x") as output:
        with app.build_worker(state.config, app.JsonEvidence(output)) as worker:
            worker.run_once()
            state.boundary.clock[0] += 1
            state.server.hooks["get_partition"] = fail
            worker.run_once()
    records = read_events(root)
    assert [(record["action"], record.get("category")) for record in records][-2:] == [
        ("quarantine_confirmed", "terminal"), ("message_acknowledged", None)]
    actions = state.boundary.actions()
    assert actions[actions.index("publish") + 1] == "acknowledge"
    assert "SENTINEL" not in (root / "worker-evidence.jsonl").read_text()


def test_restart_accepts_existing_owned_partition_and_current_objects(monkeypatch, tmp_path):
    key = ROOT + "dt=a/file"
    app, state = setup(monkeypatch, tmp_path, [message(key=key)])
    state.http.keys[key] = b"current"
    with (tmp_path / "first.jsonl").open("x") as output:
        with app.build_worker(state.config, app.JsonEvidence(output)) as first:
            first.run_once()
            state.boundary.clock[0] += 1
            first.run_once()
    state.boundary.messages = [message("restart-synthetic-receipt", key=key, kind="OBJECT_DELETE")]
    with (tmp_path / "restart.jsonl").open("x") as output:
        with app.build_worker(state.config, app.JsonEvidence(output)) as restarted:
            restarted.run_once()
            state.boundary.clock[0] += 1
            restarted.run_once()
            assert set(state.server.partitions) == {("a",)}
    assert state.http.keys == {OWNER["name"]: OWNER_BYTES, key: b"current"}
    assert not any(call[1] == "drop_partition" for call in state.server.calls)


def test_unowned_hms_table_stops_before_pubsub_creation(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path)
    state.server.table.parameters["gcp_probe_run_id"] = "unowned"
    monkeypatch.setattr(app, "create_pubsub_clients", lambda: pytest.fail("unowned table reached Pub/Sub"))
    with (tmp_path / "events.jsonl").open("x") as output:
        with pytest.raises(FatalExternalError, match="gcp_hms_unapproved_table"):
            with app.build_worker(state.config, app.JsonEvidence(output)):
                pytest.fail("unowned table reached worker")
    assert state.closed == ["storage"]


def argv(tmp_path, duration="2", output="output"):
    return ["--resources", str(resources(tmp_path)), "--output-dir", str(tmp_path / output), "--duration-seconds", duration]


@pytest.mark.parametrize("duration", ["0", "86401", "inf", "nan", "1.5", "AUTH-SENTINEL"])
def test_duration_bounds_suppress_argument_values(tmp_path, capsys, duration):
    app = app_module()
    assert app.main(argv(tmp_path, duration)) == 1
    assert "SENTINEL" not in capsys.readouterr().err
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("duration", ["1", "86400"])
@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_duration_boundary_is_accepted_and_signal_stop_is_cooperative(monkeypatch, tmp_path, duration, stop_signal):
    app, state = setup(monkeypatch, tmp_path)
    handlers = {}
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    monkeypatch.setattr(app.signal, "signal", lambda number, handler: handlers.setdefault(number, handler))
    original = app.RTMSFWorker.run_once

    def stop_after_call(worker):
        original(worker)
        handlers[stop_signal](stop_signal, None)

    monkeypatch.setattr(app.RTMSFWorker, "run_once", stop_after_call)
    assert app.main(argv(tmp_path, duration)) == 0
    report = json.loads((tmp_path / "output" / "worker-report.json").read_text())
    assert report["status"] == "stopped" and report["stop_reason"] == "signal"
    assert report["pending_deliveries"] == 0
    assert "passed" not in report


def test_main_deadline_sleeps_preserves_pending_and_lock_through_shutdown(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path, [message(key=ROOT + "dt=a/file")])
    lock = tmp_path / "worker.lock"
    monkeypatch.setattr(app, "LOCK_PATH", lock)
    monkeypatch.setattr(app.signal, "signal", lambda *_a: None)
    original_close = state.boundary.subscriber.transport.close

    def close():
        with pytest.raises(RuntimeError, match="gcp_native_worker_already_running"):
            with app._host_lock(lock):
                pytest.fail("lock released before clients closed")
        original_close()

    state.boundary.subscriber.transport.close = close
    assert app.main(argv(tmp_path, "1")) == 0
    report = json.loads((tmp_path / "output" / "worker-report.json").read_text())
    assert report["status"] == "stopped" and report["stop_reason"] == "duration"
    assert report["pending_deliveries"] == 1
    assert report["budgets"]["pmcp_seconds"] == 90
    assert report["configuration"] == state.config
    assert report["started_at"] <= report["finished_at"]
    assert state.boundary.clock[0] >= 1
    assert state.boundary.actions().count("pull") == 1
    assert "acknowledge" not in state.boundary.actions()
    assert state.http.keys == {OWNER["name"]: OWNER_BYTES}
    assert lock.exists()


def test_host_lock_excludes_different_outputs_without_replacing_inode(monkeypatch, tmp_path):
    app = app_module()
    lock = tmp_path / "worker.lock"
    monkeypatch.setattr(app, "LOCK_PATH", lock)
    with app._host_lock(lock):
        inode = lock.stat().st_ino
        for output in ("one", "two"):
            assert app.main(argv(tmp_path, output=output)) == 1
            assert not (tmp_path / output).exists()
    with app._host_lock(lock):
        assert lock.stat().st_ino == inode


@pytest.mark.parametrize("symlink", [True, False])
def test_existing_output_is_never_reused(monkeypatch, tmp_path, symlink):
    app = app_module()
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    target = tmp_path / "target"
    target.mkdir()
    (target / "sentinel").write_text("retained")
    output = tmp_path / "output"
    output.symlink_to(target, target_is_directory=True) if symlink else output.mkdir()
    monkeypatch.setattr(app, "StorageClient", lambda **_kw: pytest.fail("existing output reached ADC"))
    assert app.main(argv(tmp_path)) == 1
    assert (target / "sentinel").read_text() == "retained"


def test_startup_failure_has_safe_report_and_no_traceback(monkeypatch, tmp_path, capsys):
    app = app_module()
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    monkeypatch.setattr(app.signal, "signal", lambda *_a: None)

    def fail(**_kw):
        raise RuntimeError("AUTH-AND-RECEIPT-SENTINEL")

    monkeypatch.setattr(app, "StorageClient", fail)
    assert app.main(argv(tmp_path)) == 1
    captured = capsys.readouterr()
    assert captured.err == "gcp_native_worker_failed\n"
    report = json.loads((tmp_path / "output" / "worker-report.json").read_text())
    assert report["status"] == "failed" and report["error_category"] == "runtime"
    assert "SENTINEL" not in json.dumps(report) + captured.out + captured.err


def test_pull_uncertainty_exits_once_and_keeps_safe_pending_evidence(monkeypatch, tmp_path, capsys):
    from google.api_core.exceptions import DeadlineExceeded

    app, state = setup(monkeypatch, tmp_path, [message(key=ROOT + "dt=a/file")])
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    monkeypatch.setattr(app.signal, "signal", lambda *_a: None)
    state.boundary.failures["pull"] = lambda boundary: DeadlineExceeded("AUTH-SENTINEL") if boundary.actions().count("pull") == 2 else None
    assert app.main(argv(tmp_path)) == 1
    root = tmp_path / "output"
    report = json.loads((root / "worker-report.json").read_text())
    assert report["status"] == "failed" and report["error_category"] == "external_retryable"
    assert report["pending_deliveries"] == 1
    assert state.boundary.actions().count("pull") == 2
    records = read_events(root)
    assert [record["status"] for record in records if record["action"] == "pmcp_result"] == ["RETRY"]
    assert not any(record["action"] == "message_acknowledged" for record in records)
    assert set(state.closed) == {"storage", "subscriber", "publisher"}
    assert "SENTINEL" not in json.dumps(report) + capsys.readouterr().err


def test_single_event_is_mutated_and_acked_before_delayed_empty_pull_uncertainty(monkeypatch, tmp_path):
    from google.api_core.exceptions import DeadlineExceeded

    key = ROOT + "dt=a/file"
    app, state = setup(monkeypatch, tmp_path, [message(key=key)])
    state.http.keys[key] = b"current"
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    monkeypatch.setattr(app.signal, "signal", lambda *_a: None)
    root = tmp_path / "output"
    before_empty_pull = []

    def empty_pull(boundary):
        if boundary.actions().count("pull") == 2:
            before_empty_pull.append((set(state.server.partitions), read_events(root)))
            boundary.clock[0] += 15
            return DeadlineExceeded("AUTH-SENTINEL")
        return None

    state.boundary.failures["pull"] = empty_pull
    assert app.main(argv(tmp_path)) == 1
    partitions, records = before_empty_pull[0]
    assert partitions == {("a",)}
    event, = [record for record in records if record["action"] == "event_received"]
    result, = [record for record in records if record["action"] == "pmcp_result"]
    ack, = [record for record in records if record["action"] == "message_acknowledged"]
    assert event["handle"] == ack["handle"] and event["message_id"] == ack["message_id"]
    assert ack["objectId"] == key and ack["objectGeneration"] == "17"
    assert result["status"] == "APPLIED" and result["mutation"] == "ADD"
    assert records.index(event) < records.index(result) < records.index(ack)
    assert ack["monotonic_seconds"] >= 1
    assert state.boundary.actions() == ["get_subscription", "pull", "modify_ack_deadline", "acknowledge", "pull"]
    report = json.loads((root / "worker-report.json").read_text())
    assert report["status"] == "failed" and report["error_category"] == "external_retryable"
    assert report["pending_deliveries"] == 0
    assert "passed" not in report
    assert state.boundary.clock[0] >= 16


def test_shutdown_failure_cannot_leave_a_stopped_report(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path)
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    monkeypatch.setattr(app.signal, "signal", lambda *_a: None)

    def fail_close():
        raise RuntimeError("AUTH-SENTINEL")

    state.boundary.publisher.transport.close = fail_close
    assert app.main(argv(tmp_path, "1")) == 1
    report = json.loads((tmp_path / "output" / "worker-report.json").read_text())
    assert report["status"] == "failed"
    assert report["error_category"] == "runtime"
    assert "SENTINEL" not in json.dumps(report)
    assert set(state.closed) == {"storage", "subscriber"}


def test_lock_symlink_is_rejected_without_touching_target(tmp_path):
    app = app_module()
    target = tmp_path / "target"
    target.write_text("retained")
    lock = tmp_path / "worker.lock"
    lock.symlink_to(target)
    with pytest.raises(OSError):
        with app._host_lock(lock):
            pytest.fail("symlink lock accepted")
    assert target.read_text() == "retained"
