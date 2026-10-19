"""Offline safety and finite supervision of the approved native GCP soak."""

from copy import deepcopy
import base64
from email.parser import BytesParser
from email.policy import default
import hashlib
import importlib
import importlib.util
import json
import signal
import subprocess
from types import SimpleNamespace
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
import requests
from google.cloud.storage import Blob
from pymetastore.hive_metastore import ttypes

from tests.test_gcp_functional import BUCKET, OWNER, OWNER_BYTES, ROOT, RUN, resources, setup, StorageHTTP, no_network_escape
from tests.test_gcp_pubsub import no_unmocked_gapic_rpc


GATES = ("native_add_query", "lifecycle_drop_query", "recovery_before_add", "recovery_after_add",
         "recovery_before_drop", "recovery_after_drop", "markers", "special_names", "burst",
         "malformed_quarantine", "versions", "stale_duplicate_replay")
B_SENTINEL = {"name": ROOT + "dt=b/part-00000.parquet", "generation": "1792448993562638", "size": 438,
              "sha256": "e824f52db65a5f8e1cba3e36dc0943021c620dfeebff240d4b4f649c32ebb30f"}
FIXTURE_A = base64.b64decode(
    "UEFSMRUEFRYVFkwVAhUEEgAABwAAAHByb2JlLWEVABUSFRIsFQIVBBUGFQYcNgAoB3Byb2JlLWEYB3Byb2JlLWEAAAACAAAAAgEBAgAVAhksNQAYBnNjaGVtYRUCABUMJQIYBXZhbHVlJQBMHAAAABYCGRwZHCYAHBUMGSUEBhkYBXZhbHVlFQAWAhaSARaSASY6JggcNgAoB3Byb2JlLWEYB3Byb2JlLWEAGSwVBBUEFQIAFQAVBBUCADwWDhkGGSYAAgAAABaSARYCJggWkgEAGRwYDEFSUk9XOnNjaGVtYRigAS8vLy8vM0FBQUFBUUFBQUFBQUFLQUF3QUJnQUZBQWdBQ2dBQUFBQUJCQUFNQUFBQUNBQUlBQUFBQkFBSUFBQUFCQUFBQUFFQUFBQVVBQUFBRUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFBQUFBRUZFQUFBQUJ3QUFBQUVBQUFBQUFBQUFBVUFBQUIyWVd4MVpRQUFBQVFBQkFBRUFBQUEAGCBwYXJxdWV0LWNwcC1hcnJvdyB2ZXJzaW9uIDIxLjAuMBkcHAAAAGEBAABQQVIx")


def harness():
    assert importlib.util.find_spec("experiments.gcp.soak") is not None, "finite GCP soak missing"
    return importlib.import_module("experiments.gcp.soak")


def document(tmp_path, name, value):
    path = tmp_path / name
    path.write_text(json.dumps(value))
    return path


def gates():
    return {"run_id": RUN, "gates": {name: {"status": "passed", "evidence": "live/" + name + ".json"}
                                   for name in GATES}}


def baseline():
    return {"run_id": RUN, "objects": [deepcopy(OWNER), deepcopy(B_SENTINEL)],
            "catalog_partitions": ["dt=b"], "catalog_rows": [{"dt": "b", "value": "probe-b"}]}


@pytest.mark.parametrize("gate", GATES)
@pytest.mark.parametrize("status", (None, "pending", "ACTIVE", True))
def test_each_explicit_live_gate_is_required(tmp_path, gate, status):
    app = harness()
    value = gates()
    value["gates"][gate]["status"] = status
    with pytest.raises(ValueError, match="gcp_soak_correctness_gates_required"):
        app.load_gates(document(tmp_path, "gates.json", value))


def test_gate_contract_requires_evidence_and_current_object_configuration(tmp_path):
    app = harness()
    value = gates()
    assert app.load_gates(document(tmp_path, "gates.json", value)) == list(GATES)
    for change in ("missing", "provisioning", "evidence", "run"):
        changed = deepcopy(value)
        if change == "missing":
            del changed["gates"]["versions"]
        elif change == "provisioning":
            changed = {"run_id": RUN, "status": "passed", "provisioned": True}
        elif change == "evidence":
            changed["gates"]["versions"]["evidence"] = ""
        else:
            changed["run_id"] = "AUTH-SENTINEL"
        with pytest.raises(ValueError) as caught:
            app.load_gates(document(tmp_path, "gates.json", changed))
        assert "SENTINEL" not in str(caught.value)


def test_bounded_baseline_requires_immutable_owner_and_b_sentinel(tmp_path):
    app = harness()
    assert app.load_baseline(document(tmp_path, "baseline.json", baseline())) == baseline()["objects"]
    for objects in ([], [OWNER], [B_SENTINEL], [OWNER, B_SENTINEL, OWNER], baseline()["objects"] * 33):
        with pytest.raises(ValueError, match="gcp_soak_invalid_baseline"):
            app.load_baseline(document(tmp_path, "baseline.json", {"run_id": RUN, "objects": objects}))


@pytest.mark.parametrize("field,value", [("name", "outside/file"), ("name", ROOT + "dt=soak/file"),
    ("name", ROOT + "_SOAK_PULSE"), ("name", ROOT + "../file"), ("generation", "0"),
    ("generation", "01"), ("generation", 17), ("generation", True), ("size", True), ("size", -1),
    ("size", 1048577), ("sha256", "AUTH-SENTINEL"), ("unknown", "AUTH-SENTINEL")])
def test_unsafe_baseline_refused_without_echo(tmp_path, field, value):
    app = harness()
    changed = baseline()
    item = {"name": ROOT + "dt=other/file", "generation": "18", "size": 1, "sha256": "a" * 64}
    item[field] = value
    changed["objects"].append(item)
    with pytest.raises(ValueError) as caught:
        app.load_baseline(document(tmp_path, "baseline.json", changed))
    assert "SENTINEL" not in str(caught.value)


@pytest.mark.parametrize("item", (0, 1))
def test_known_baseline_identity_cannot_drift(tmp_path, item):
    changed = baseline()
    changed["objects"][item]["generation"] = "19"
    with pytest.raises(ValueError):
        harness().load_baseline(document(tmp_path, "baseline.json", changed))


class SoakHTTP(StorageHTTP):
    """Real pinned Storage SDK requests through the existing guarded HTTP fake."""

    def __init__(self, clock):
        super().__init__(clock)
        self.keys[B_SENTINEL["name"]] = FIXTURE_A.replace(b"probe-a", b"probe-b")
        self.generations = {OWNER["name"]: OWNER["generation"], B_SENTINEL["name"]: B_SENTINEL["generation"]}
        self.next_generation = 100
        self.mutations = []
        self.ambiguous = False
        self.bucket_properties = {"name": BUCKET, "versioning": {"enabled": False},
                                  "softDeletePolicy": {"retentionDurationSeconds": "0"}}

    def request(self, method, url, **kwargs):
        assert urlsplit(url).hostname == "storage.googleapis.com"
        assert 0 < kwargs["timeout"] <= 15
        parsed, query = urlsplit(url), parse_qs(urlsplit(url).query)
        key = unquote(parsed.path.split("/o/", 1)[1]) if "/o/" in parsed.path else None
        if method == "GET" and key is None and not parsed.path.endswith("/o"):
            response = super().request(method, url, **kwargs)
            response._content = json.dumps(self.bucket_properties).encode()
            return response
        if method == "GET" and (key == OWNER["name"] or key is None):
            return super().request(method, url, **kwargs)
        self.calls.append((url, kwargs))
        self.clock[0] += self.delay
        status, body = 200, {}
        if method == "POST":
            assert query["uploadType"] == ["multipart"]
            content_type = kwargs["headers"]["content-type"]
            parsed_body = BytesParser(policy=default).parsebytes(b"Content-Type: " + content_type + b"\r\n\r\n" + kwargs["data"])
            metadata, media = list(parsed_body.iter_parts())
            key = json.loads(metadata.get_payload(decode=True))["name"]
            body = media.get_payload(decode=True)
            expected = query["ifGenerationMatch"][0]
            assert key in {harness().SOAK_KEY, harness().PULSE_KEY}
            assert expected == self.generations.get(key, "0")
            self.keys[key] = body
            self.generations[key] = str(self.next_generation)
            self.next_generation += 1
            self.mutations.append((method, key, expected))
            body = {"name": key, "bucket": BUCKET, "generation": self.generations[key], "size": str(len(body))}
        elif method == "DELETE":
            assert key in {harness().SOAK_KEY, harness().PULSE_KEY}
            assert query["ifGenerationMatch"] == [self.generations[key]]
            self.mutations.append((method, key, self.generations[key]))
            del self.keys[key]
            del self.generations[key]
            status = 204
        else:
            assert method == "GET"
            if key not in self.keys:
                status = 404
            elif query.get("alt") == ["media"]:
                assert query["ifGenerationMatch"] == [self.generations[key]]
                if self.keys[key]:
                    assert kwargs["headers"]["range"] == f"bytes=0-{len(self.keys[key]) - 1}"
                else:
                    assert "range" not in kwargs["headers"]
                body = self.keys[key]
            else:
                body = {"name": key, "bucket": BUCKET, "generation": self.generations[key], "size": str(len(self.keys[key]))}
        if method in ("POST", "DELETE") and self.ambiguous:
            raise TimeoutError("AUTH-AND-RECEIPT-SENTINEL")
        response = requests.Response()
        response.status_code = status
        response._content = body if isinstance(body, bytes) else json.dumps(body).encode()
        response.headers["content-type"] = "application/octet-stream" if isinstance(body, bytes) else "application/json"
        response.request = requests.Request(method, url).prepare()
        response._content_consumed = True
        response.raw = SimpleNamespace(headers={}, close=lambda: None)
        return response


def storage_setup(monkeypatch, tmp_path):
    app = harness()
    native, state = setup(monkeypatch, tmp_path)
    http = SoakHTTP(state.boundary.clock)
    state.http = http
    client = native.StorageClient(project=native.PROJECT)
    client._http_internal = http
    monkeypatch.setattr(app, "StorageClient", lambda **options: client if options == {"project": native.PROJECT} else pytest.fail("ADC options drifted"))
    storage = app.GcsObjectStore(client, {"fixtures": BUCKET}, deadline=app.OperationDeadline())
    return app, storage, state


def test_fixture_is_exact_known_parquet_and_b_is_distinct(tmp_path):
    app = harness()
    path = tmp_path / "fixture.parquet"
    path.write_bytes(FIXTURE_A)
    assert app.load_fixture(path) == FIXTURE_A
    assert len(FIXTURE_A) == 438 and hashlib.sha256(FIXTURE_A).hexdigest() == app.FIXTURE_SHA
    assert hashlib.sha256(FIXTURE_A.replace(b"probe-a", b"probe-b")).hexdigest() == B_SENTINEL["sha256"]
    for body in (FIXTURE_A + b"x", FIXTURE_A.replace(b"probe-a", b"probe-b"), b"PAR1", b"x" * 100000):
        path.write_bytes(body)
        with pytest.raises(ValueError, match="gcp_soak_invalid_fixture"):
            app.load_fixture(path)


def test_independent_baseline_read_is_exact_pinned_and_bounded(monkeypatch, tmp_path):
    app, storage, state = storage_setup(monkeypatch, tmp_path)
    app.verify_baseline(storage, baseline()["objects"])
    assert len(state.http.calls) == 4 and not state.http.mutations
    assert storage._client._bucket_metadata_cache is None
    assert storage._deadline.seconds == 90 and storage._deadline.rpc_seconds == 15


@pytest.mark.parametrize("drift", ("generation", "bytes", "missing", "deadline"))
def test_baseline_drift_stops_without_mutation(monkeypatch, tmp_path, drift):
    app, storage, state = storage_setup(monkeypatch, tmp_path)
    before = deepcopy(state.http.keys)
    if drift == "generation":
        state.http.generations[B_SENTINEL["name"]] = "999"
    elif drift == "bytes":
        state.http.keys[B_SENTINEL["name"]] = b"x" * 438
    elif drift == "missing":
        del state.http.keys[B_SENTINEL["name"]]
    else:
        state.http.delay = 31
    preserved = deepcopy(state.http.keys)
    with pytest.raises(Exception):
        app.verify_baseline(storage, baseline()["objects"])
    assert state.http.keys == preserved and not state.http.mutations
    assert before[OWNER["name"]] == state.http.keys[OWNER["name"]]


def test_only_two_owned_keys_allow_conditioned_finite_no_retry_mutations(monkeypatch, tmp_path):
    app, storage, state = storage_setup(monkeypatch, tmp_path)
    calls = []
    for name in ("upload_from_string", "delete"):
        original = getattr(Blob, name)

        def wrapped(blob, *args, _original=original, **kwargs):
            assert kwargs["retry"] is None and 0 < kwargs["timeout"] <= 15
            calls.append(kwargs["if_generation_match"])
            return _original(blob, *args, **kwargs)

        monkeypatch.setattr(Blob, name, wrapped)
    owned = app.mutate(storage, app.SOAK_KEY, FIXTURE_A, None)
    assert owned == {"name": app.SOAK_KEY, "generation": "100", "size": 438, "sha256": app.FIXTURE_SHA}
    assert app.mutate(storage, app.SOAK_KEY, None, owned) is None
    pulse = app.mutate(storage, app.PULSE_KEY, b"pulse", None)
    second = app.mutate(storage, app.PULSE_KEY, b"pulse2", pulse)
    assert second["generation"] == "102" and calls == [0, 100, 0, 101]
    assert state.http.keys[OWNER["name"]] == OWNER_BYTES
    assert state.http.keys[B_SENTINEL["name"]] == FIXTURE_A.replace(b"probe-a", b"probe-b")
    for name in (OWNER["name"], B_SENTINEL["name"], ROOT + "dt=soak/other", "other"):
        with pytest.raises(ValueError):
            app.mutate(storage, name, FIXTURE_A, None)
    with pytest.raises(ValueError):
        app.mutate(storage, app.SOAK_KEY, None, None)


def test_owned_generation_drift_prevents_delete(monkeypatch, tmp_path):
    app, storage, state = storage_setup(monkeypatch, tmp_path)
    owned = app.mutate(storage, app.SOAK_KEY, FIXTURE_A, None)
    state.http.generations[app.SOAK_KEY] = "101"
    with pytest.raises(ValueError):
        app.mutate(storage, app.SOAK_KEY, None, owned)
    assert state.http.keys[app.SOAK_KEY] == FIXTURE_A
    assert len(state.http.mutations) == 1


def test_producer_ambiguity_retains_input_without_mutation_retry(monkeypatch, tmp_path):
    app, storage, state = storage_setup(monkeypatch, tmp_path)
    state.http.ambiguous = True
    with pytest.raises(Exception):
        app.mutate(storage, app.SOAK_KEY, FIXTURE_A, None)
    assert state.http.keys[app.SOAK_KEY] == FIXTURE_A
    assert state.http.mutations == [("POST", app.SOAK_KEY, "0")]


def event(action, key=None, generation="100", handle="handle", **changes):
    app = harness()
    return {"action": action, "handle": handle, "message_id": "native-message", "bucketId": BUCKET,
            "objectId": app.SOAK_KEY if key is None else key, "objectGeneration": generation,
            "eventType": "OBJECT_DELETE", "eventTime": "2026-10-19T22:00:00Z",
            "notificationConfig": app.run.APPROVED_PIPELINE["notification_config_attribute"], **changes}


def pmcp(**changes):
    return {"action": "pmcp_result", "partition": "dt=soak", "mutation": "DROP", "status": "APPLIED", **changes}


def tail_setup(tmp_path, records):
    path = tmp_path / "worker-evidence.jsonl"
    path.write_text("".join(json.dumps(item) + "\n" for item in records))
    report = {"final_delete_generation": "100", "final_delete_ack": None, "evidence_records": 0,
              "quarantine": {"malformed": 0, "terminal": 0}, "pending_observed": 0}
    return harness()._Tail(path, report), report, path


def test_exact_deletion_ack_requires_prior_receipt_and_successful_drop(tmp_path):
    tail, report, _ = tail_setup(tmp_path, [event("event_received"), pmcp(), event("message_acknowledged")])
    tail.read()
    assert report["final_delete_ack"] == {"object_key": harness().SOAK_KEY, "generation": "100",
                                          "event_type": "OBJECT_DELETE", "after_reconciliation": True}
    assert report["pending_observed"] == 0 and report["evidence_records"] == 3


@pytest.mark.parametrize("records", [
    [event("message_acknowledged")],
    [pmcp(), event("event_received"), event("message_acknowledged")],
    [event("event_received"), pmcp(status="RETRY"), event("message_acknowledged")],
    [event("event_received"), pmcp(mutation="ADD"), event("message_acknowledged")],
    [event("event_received"), pmcp(partition="dt=b"), event("message_acknowledged")],
    [event("event_received"), pmcp(), event("message_acknowledged", generation="101")],
    [event("event_received", eventType="OBJECT_ARCHIVE"), pmcp(), event("message_acknowledged", eventType="OBJECT_ARCHIVE")],
    [event("event_received", key=ROOT + "_SOAK_PULSE"), pmcp(), event("message_acknowledged", key=ROOT + "_SOAK_PULSE")],
    [event("event_received"), pmcp(), event("quarantine_confirmed", category="terminal"), event("message_acknowledged")],
])
def test_pulse_stale_or_unreconciled_ack_cannot_prove_final_delete(tmp_path, records):
    tail, report, _ = tail_setup(tmp_path, records)
    tail.read()
    assert report["final_delete_ack"] is None


def test_pending_handles_are_bounded_and_partial_reads_resume(tmp_path):
    tail, report, path = tail_setup(tmp_path, [event("event_received", handle="h" + str(i)) for i in range(20)])
    tail.read()
    assert len(tail.pending) == 20 and report["pending_observed"] == 20
    with path.open("a") as output:
        output.write(json.dumps(event("event_received", handle="h20"))[:50])
    tail.read()
    assert len(tail.pending) == 20
    with path.open("a") as output:
        output.write(json.dumps(event("event_received", handle="h20"))[50:] + "\n")
    with pytest.raises(RuntimeError, match="gcp_soak_pending_evidence_limit"):
        tail.read()
    assert len(tail.pending) == 20


def test_log_parser_has_bounded_read_and_line_size_without_echo(tmp_path):
    tail, report, path = tail_setup(tmp_path, [{"action": "unknown", "error": "AUTH-SENTINEL"}] * 4000)
    tail.read()
    assert tail.offset <= 65536 and report["evidence_records"] < 4000
    assert "SENTINEL" not in json.dumps(report)
    path.write_text(json.dumps({"error": "AUTH-SENTINEL" + "x" * 9000}))
    tail.offset = 0
    tail.partial = b""
    with pytest.raises(ValueError, match="gcp_soak_invalid_evidence") as caught:
        tail.read()
    assert "SENTINEL" not in str(caught.value)


def test_quarantine_counts_are_retained_separately_and_not_success(tmp_path):
    tail, report, _ = tail_setup(tmp_path, [event("quarantine_confirmed", category="malformed"),
                                         event("quarantine_confirmed", category="terminal")])
    tail.read()
    assert report["quarantine"] == {"malformed": 1, "terminal": 1} and report["final_delete_ack"] is None


def soak_argv(tmp_path, duration="12"):
    fixture = tmp_path / "fixture-a.parquet"
    fixture.write_bytes(FIXTURE_A)
    return ["--resources", str(resources(tmp_path)), "--gates", str(document(tmp_path, "gates.json", gates())),
            "--baseline", str(document(tmp_path, "baseline.json", baseline())), "--fixture-a", str(fixture),
            "--output-dir", str(tmp_path / "soak"), "--duration-seconds", duration]


def main_setup(monkeypatch, tmp_path, *, acknowledge=True):
    app, storage, state = storage_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "soak.lock")
    monkeypatch.setattr(app.run, "LOCK_PATH", tmp_path / "worker.lock")
    processes, order, handlers = [], [], {}
    state.control = lambda: None
    state.force_kill = False
    state.acknowledge = acknowledge
    state.quarantine = None

    class Process:
        def __init__(self, command, **options):
            assert all(process.poll() is not None for process in processes), "workers overlap"
            assert command[:3] == [app.sys.executable, "-m", "experiments.gcp.run"]
            assert set(options) == {"stdout", "stderr"}
            assert 1 <= int(command[command.index("--duration-seconds") + 1]) <= 86400
            self.root = app.Path(command[command.index("--output-dir") + 1])
            self.root.mkdir()
            (self.root / "worker-evidence.jsonl").write_text("")
            self.pid, self.returncode, self.waited = 1000 + len(processes), None, False
            order.append(("start", self.pid))
            processes.append(self)

        def poll(self):
            return self.returncode

        def exit(self, code=1, status="failed", category="external_retryable", reason=None):
            self.returncode = code
            value = {"status": status, "pending_deliveries": 0}
            if category is not None:
                value["error_category"] = category
            if reason is not None:
                value["stop_reason"] = reason
            (self.root / "worker-report.json").write_text(json.dumps(value))

        def wait(self, timeout):
            order.append(("wait", self.pid))
            if self.returncode is None:
                raise subprocess.TimeoutExpired("worker", timeout)
            self.waited = True
            return self.returncode

        def terminate(self):
            order.append(("terminate", self.pid))
            if not state.force_kill:
                self.exit(0, "stopped", None, "signal")

        def kill(self):
            order.append(("kill", self.pid))
            self.returncode = -signal.SIGKILL

    class Stop:
        def __init__(self):
            self.stopped = False

        def is_set(self):
            return self.stopped

        def set(self):
            self.stopped = True

        def wait(self, seconds):
            assert 0 <= seconds <= 1
            state.boundary.clock[0] += seconds
            state.control()
            return self.stopped

    def register(number, handler):
        previous = handlers.get(number)
        handlers[number] = handler
        return previous

    request = state.http.request

    def notify(method, url, **kwargs):
        result = request(method, url, **kwargs)
        if method in {"POST", "DELETE"} and processes:
            _, key, generation = state.http.mutations[-1]
            if method == "POST":
                generation = state.http.generations[key]
            kind = "OBJECT_FINALIZE" if method == "POST" else "OBJECT_DELETE"
            process = processes[-1]
            if process.poll() is None:
                records = [event("event_received", key=key, generation=generation, eventType=kind)]
                if key == app.SOAK_KEY:
                    if method == "POST":
                        sd = deepcopy(state.server.table.sd)
                        sd.location = f"gs://{BUCKET}/" + app.SOAK_PREFIX[:-1]
                        state.server.partitions[("soak",)] = ttypes.Partition(values=["soak"], dbName=app.run.APPROVED_RUN["table_id"].split(".")[0],
                            tableName="native_events", sd=sd, parameters={"gcp_probe_run_id": RUN}, writeId=None)
                    else:
                        state.server.partitions.pop(("soak",), None)
                    records.append(pmcp(mutation="ADD" if method == "POST" else "DROP"))
                if state.quarantine is not None:
                    records.append(event("quarantine_confirmed", key=key, generation=generation, eventType=kind, category=state.quarantine))
                if state.acknowledge or key == app.PULSE_KEY:
                    records.append(event("message_acknowledged", key=key, generation=generation, eventType=kind))
                with (process.root / "worker-evidence.jsonl").open("a") as output:
                    output.write("".join(json.dumps(record) + "\n" for record in records))
        return result

    monkeypatch.setattr(app.subprocess, "Popen", Process)
    monkeypatch.setattr(app, "Event", Stop)
    monkeypatch.setattr(app.signal, "signal", register)
    state.http.request = notify
    state.processes, state.order, state.handlers = processes, order, handlers
    return app, storage, state


def report(tmp_path):
    return json.loads((tmp_path / "soak" / "soak-report.json").read_text())


def test_finite_producer_final_delete_current_truth_and_baseline_are_checked(monkeypatch, tmp_path):
    app, storage, state = main_setup(monkeypatch, tmp_path)
    assert app.main(soak_argv(tmp_path)) == 0
    value = report(tmp_path)
    assert value["status"] == "completed" and value["duration_seconds"] == 12
    assert value["producer"] == {"creates": 2, "deletes": 2, "pulses": 1}
    assert value["final_storage_empty"] and value["final_catalog_absent"] and value["baseline_final"] == "passed"
    assert value["final_query"] == "pending_external_named_table_query"
    assert value["current_generation"] is None and value["pulse_generation"] is not None
    assert value["final_delete_ack"]["generation"] == value["final_delete_generation"]
    assert app.SOAK_KEY not in state.http.keys and app.PULSE_KEY in state.http.keys
    assert ("soak",) not in state.server.partitions
    assert len(state.processes) == 1 and state.processes[0].waited and value["child_pid"] is None
    assert state.order[-2:] == [("terminate", 1000), ("wait", 1000)]
    assert storage._client._bucket_metadata_cache is None and set(state.closed) == {"storage"}
    records = [json.loads(line) for line in (tmp_path / "soak" / "soak-evidence.jsonl").read_text().splitlines()]
    attempts = [record for record in records if record["action"] == "producer_attempt" and record["name"] == app.SOAK_KEY]
    assert [record["monotonic_seconds"] for record in attempts] == [0, 5, 10, 12]


def test_retryable_child_exit_waits_before_fresh_restart_and_keeps_failure(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path)

    def fail_once():
        if len(state.processes) == 1 and state.processes[0].poll() is None:
            state.processes[0].exit()

    state.control = fail_once
    assert app.main(soak_argv(tmp_path, "2")) == 0
    value = report(tmp_path)
    assert value["status"] == "completed_with_restarts" and value["child_restarts"] == 1 and value["failed_children"] == 1
    assert state.order[:3] == [("start", 1000), ("wait", 1000), ("start", 1001)]
    assert state.processes[0].waited and all(process.waited for process in state.processes)
    assert json.loads((state.processes[0].root / "worker-report.json").read_text())["status"] == "failed"
    stream = (tmp_path / "soak" / "soak-evidence.jsonl").read_text()
    assert "external_retryable_process_restart" in stream and '"cause":"unproven"' in stream


@pytest.mark.parametrize("category", ("external_fatal", "runtime", "configuration", "AUTH-SENTINEL", "missing"))
def test_unrecoverable_or_unreported_child_exit_stops_failed_without_replacement(monkeypatch, tmp_path, category, capsys):
    app, _, state = main_setup(monkeypatch, tmp_path)

    def fail():
        process = state.processes[-1]
        if process.poll() is None:
            process.exit(category=category)
            if category == "missing":
                (process.root / "worker-report.json").unlink()

    state.control = fail
    assert app.main(soak_argv(tmp_path, "12")) == 1
    assert report(tmp_path)["status"] == "failed" and len(state.processes) == 1
    assert state.processes[0].waited and app.SOAK_KEY in state.http.keys
    assert "SENTINEL" not in capsys.readouterr().out + json.dumps(report(tmp_path)) + (tmp_path / "soak" / "soak-evidence.jsonl").read_text()


@pytest.mark.parametrize("number", (signal.SIGTERM, signal.SIGINT))
def test_signal_stop_keeps_owned_input_and_confirms_child_exit(monkeypatch, tmp_path, number):
    app, _, state = main_setup(monkeypatch, tmp_path)
    state.control = lambda: state.handlers[number](number, None)
    assert app.main(soak_argv(tmp_path, "86400")) == 1
    value = report(tmp_path)
    assert value["status"] == "stopped" and value["stop_reason"] == "signal"
    assert state.http.keys[app.SOAK_KEY] == FIXTURE_A and value["producer"]["deletes"] == 0
    assert all(process.waited for process in state.processes) and value["child_pid"] is None


def test_child_shutdown_kills_only_owned_process_if_term_does_not_finish(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path)
    state.force_kill = True
    state.control = lambda: state.handlers[signal.SIGTERM](signal.SIGTERM, None)
    assert app.main(soak_argv(tmp_path, "20")) == 1
    assert state.order[-4:] == [("terminate", 1000), ("wait", 1000), ("kill", 1000), ("wait", 1000)]
    assert report(tmp_path)["forced_child_kills"] == 1 and state.processes[0].waited


def test_pulse_acks_cannot_mask_missing_final_delete_ack(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path, acknowledge=False)
    assert app.main(soak_argv(tmp_path, "1")) == 1
    value = report(tmp_path)
    assert value["status"] == "failed" and value["final_delete_ack"] is None
    assert value["producer"]["pulses"] == 60 and state.boundary.clock[0] <= 301
    assert app.PULSE_KEY in state.http.keys and all(process.waited for process in state.processes)


@pytest.mark.parametrize("guard", ("gates", "manifest", "baseline_generation", "baseline_bytes", "soak_prefix", "pulse", "catalog"))
def test_guards_stop_before_any_cloud_write_or_child_start(monkeypatch, tmp_path, guard):
    app, _, state = main_setup(monkeypatch, tmp_path)
    arguments = soak_argv(tmp_path)
    if guard == "gates":
        changed = gates()
        changed["gates"]["native_add_query"]["status"] = "pending"
        document(tmp_path, "gates.json", changed)
    elif guard == "manifest":
        document(tmp_path, "resources.json", {"project_id": "other"})
    elif guard == "baseline_generation":
        state.http.generations[B_SENTINEL["name"]] = "19"
    elif guard == "baseline_bytes":
        state.http.keys[B_SENTINEL["name"]] = b"x" * 438
    elif guard == "soak_prefix":
        state.http.keys[app.SOAK_PREFIX + "unknown"] = b"retained"
    elif guard == "pulse":
        state.http.keys[app.PULSE_KEY] = b"retained"
        state.http.generations[app.PULSE_KEY] = "20"
    else:
        state.server.partitions[("soak",)] = ttypes.Partition(values=["soak"])
    preserved = deepcopy(state.http.keys)
    assert app.main(arguments) == 1
    assert not state.http.mutations and not state.processes and state.http.keys == preserved


def test_main_ambiguous_mutation_reports_failure_and_retains_object(monkeypatch, tmp_path, capsys):
    app, _, state = main_setup(monkeypatch, tmp_path)
    state.http.ambiguous = True
    assert app.main(soak_argv(tmp_path, "20")) == 1
    value = report(tmp_path)
    assert value["status"] == "failed" and value["producer_uncertain"] is True
    assert value["mutation_pending"]["name"] == app.SOAK_KEY
    assert state.http.keys[app.SOAK_KEY] == FIXTURE_A and len(state.http.mutations) == 1
    assert all(process.waited for process in state.processes)
    assert "SENTINEL" not in json.dumps(value) + capsys.readouterr().err


@pytest.mark.parametrize("duration", ("0", "86401", "AUTH-SENTINEL", "1.5"))
def test_invalid_duration_cannot_reach_adc(monkeypatch, tmp_path, duration, capsys):
    app = harness()
    monkeypatch.setattr(app, "StorageClient", lambda **_kw: pytest.fail("invalid duration reached ADC"))
    assert app.main(soak_argv(tmp_path, duration)) == 1
    assert not (tmp_path / "soak").exists() and "SENTINEL" not in capsys.readouterr().err


def test_existing_output_and_supervisor_or_worker_lock_refuse_before_cloud(monkeypatch, tmp_path):
    app = harness()
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "supervisor.lock")
    monkeypatch.setattr(app.run, "LOCK_PATH", tmp_path / "worker.lock")
    monkeypatch.setattr(app, "StorageClient", lambda **_kw: pytest.fail("refusal reached ADC"))
    arguments = soak_argv(tmp_path)
    for lock in (app.LOCK_PATH, app.run.LOCK_PATH):
        with app.run._host_lock(lock):
            assert app.main(arguments) == 1
    (tmp_path / "soak").mkdir(exist_ok=True)
    sentinel = tmp_path / "soak" / "sentinel"
    sentinel.write_text("retained")
    assert app.main(arguments) == 1 and sentinel.read_text() == "retained"


@pytest.mark.parametrize("properties", [{"name": BUCKET, "versioning": {"enabled": True}, "softDeletePolicy": {"retentionDurationSeconds": "0"}},
    {"name": BUCKET, "versioning": {"enabled": False}, "softDeletePolicy": {"retentionDurationSeconds": "604800"}},
    {"name": BUCKET}, {"name": "other", "softDeletePolicy": {"retentionDurationSeconds": "0"}}])
def test_current_object_bucket_configuration_is_rechecked_before_writes(monkeypatch, tmp_path, properties):
    app, _, state = main_setup(monkeypatch, tmp_path)
    state.http.bucket_properties = properties
    assert app.main(soak_argv(tmp_path, "1")) == 1
    assert not state.http.mutations and not state.processes


def test_retryable_exit_during_final_checks_cannot_be_uninterrupted_completion(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path)
    verify = app.verify_baseline
    checked = []

    def finish(storage, objects):
        verify(storage, objects)
        checked.append(True)
        if len(checked) == 2:
            state.processes[-1].exit()

    monkeypatch.setattr(app, "verify_baseline", finish)
    assert app.main(soak_argv(tmp_path, "1")) == 0
    value = report(tmp_path)
    assert value["status"] == "completed_with_terminal_events" and value["failed_children"] == 1
    assert value["child_restarts"] == 0 and value["child_pid"] is None


def test_unrelated_quarantine_is_reported_without_masking_exact_final_ack(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path)
    added = []

    def quarantine():
        if not added:
            with (state.processes[-1].root / "worker-evidence.jsonl").open("a") as output:
                output.write(json.dumps({"action": "quarantine_confirmed", "category": "malformed"}) + "\n")
            added.append(True)

    state.control = quarantine
    assert app.main(soak_argv(tmp_path, "1")) == 0
    value = report(tmp_path)
    assert value["status"] == "completed_with_terminal_events"
    assert value["quarantine"] == {"malformed": 1, "terminal": 0} and value["final_delete_ack"] is not None


@pytest.mark.parametrize("body", ("AUTH-SENTINEL\n", "AUTH-SENTINEL" + "x" * 9000), ids=("invalid_json", "oversize_line"))
def test_corrupt_child_evidence_still_records_confirmed_process_exit(monkeypatch, tmp_path, body):
    app, _, state = main_setup(monkeypatch, tmp_path)

    def corrupt():
        state.processes[-1].exit()
        with (state.processes[-1].root / "worker-evidence.jsonl").open("a") as output:
            output.write(body)

    state.control = corrupt
    assert app.main(soak_argv(tmp_path, "3")) == 1
    assert report(tmp_path)["child_pid"] is None and all(process.waited for process in state.processes)


def test_baseline_generation_must_fit_uint64(tmp_path):
    changed = baseline()
    changed["objects"].append({"name": ROOT + "dt=other/file", "generation": "99999999999999999999", "size": 1, "sha256": "a" * 64})
    with pytest.raises(ValueError):
        harness().load_baseline(document(tmp_path, "baseline.json", changed))


def test_zero_exit_duration_rollover_restarts_without_counting_failed_child(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path)

    def rollover():
        if len(state.processes) == 1 and state.processes[0].poll() is None:
            state.processes[0].exit(0, "stopped", None, "duration")

    state.control = rollover
    assert app.main(soak_argv(tmp_path, "2")) == 0
    value = report(tmp_path)
    assert value["status"] == "completed_with_restarts" and value["failed_children"] == 0


def test_baseline_end_drift_prevents_completion_without_repairing_fixtures(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path)

    def drift():
        state.http.keys[B_SENTINEL["name"]] = b"x" * 438

    state.control = drift
    assert app.main(soak_argv(tmp_path, "1")) == 1
    value = report(tmp_path)
    assert value["status"] == "failed" and value["baseline_final"] != "passed"
    assert state.http.keys[B_SENTINEL["name"]] == b"x" * 438
    assert not any(name == B_SENTINEL["name"] for _, name, _ in state.http.mutations)


def test_signal_during_child_health_check_prevents_next_mutation(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path)
    check = app._Child.check

    def interrupted(child, until):
        check(child, until)
        state.handlers[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setattr(app._Child, "check", interrupted)
    assert app.main(soak_argv(tmp_path, "10")) == 1
    assert report(tmp_path)["status"] == "stopped" and not state.http.mutations


def test_settling_overrun_does_not_start_another_pulse(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path, acknowledge=False)
    check = app._Child.check

    def overrun(child, until):
        check(child, until)
        if child.report["phase"] == "settling":
            state.boundary.clock[0] += 301

    monkeypatch.setattr(app._Child, "check", overrun)
    assert app.main(soak_argv(tmp_path, "1")) == 1
    assert report(tmp_path)["producer"]["pulses"] == 0


@pytest.mark.parametrize("field,value", (("generation", True), ("generation", 100), ("size", True)))
def test_malformed_provider_metadata_prevents_owned_delete(monkeypatch, tmp_path, field, value):
    app, storage, state = storage_setup(monkeypatch, tmp_path)
    owned = app.mutate(storage, app.SOAK_KEY, FIXTURE_A, None)
    request = state.http.request

    def malformed(method, url, **kwargs):
        result = request(method, url, **kwargs)
        if method == "GET" and "/o/" in url and "alt=media" not in url:
            metadata = json.loads(result.content)
            metadata[field] = value
            result._content = json.dumps(metadata).encode()
        return result

    state.http.request = malformed
    with pytest.raises(ValueError):
        app.mutate(storage, app.SOAK_KEY, None, owned)
    assert state.http.keys[app.SOAK_KEY] == FIXTURE_A and len(state.http.mutations) == 1


def test_five_second_cadence_does_not_drift_by_each_sdk_call_time(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path)
    state.http.delay = 0.1
    assert app.main(soak_argv(tmp_path, "12")) == 0
    records = [json.loads(line) for line in (tmp_path / "soak" / "soak-evidence.jsonl").read_text().splitlines()]
    attempts = [record["monotonic_seconds"] for record in records if record["action"] == "producer_attempt" and record["name"] == app.SOAK_KEY]
    assert attempts[1] - attempts[0] == pytest.approx(5)
    assert attempts[2] - attempts[1] == pytest.approx(5)


def test_exit_evidence_write_failure_still_confirms_child_is_gone(monkeypatch, tmp_path):
    app, _, state = main_setup(monkeypatch, tmp_path)
    emit = app.run.JsonEvidence.emit

    def fail_exit(evidence, record):
        if record.get("action") == "child_exited":
            raise OSError("AUTH-SENTINEL")
        emit(evidence, record)

    monkeypatch.setattr(app.run.JsonEvidence, "emit", fail_exit)
    assert app.main(soak_argv(tmp_path, "1")) == 1
    assert report(tmp_path)["child_pid"] is None and all(process.waited for process in state.processes)


@pytest.mark.parametrize("suffix", ("dt=marker/_SUCCESS", "dt=marker/inner/_SUCCESS", "dt=marker_slash/"))
def test_zero_byte_and_terminal_slash_markers_are_preserved_in_baseline(monkeypatch, tmp_path, suffix):
    app, storage, state = storage_setup(monkeypatch, tmp_path)
    marker = {"name": ROOT + suffix, "generation": "21", "size": 0,
              "sha256": hashlib.sha256(b"").hexdigest()}
    changed = baseline()
    changed["objects"].append(marker)
    objects = app.load_baseline(document(tmp_path, "baseline.json", changed))
    state.http.keys[marker["name"]] = b""
    state.http.generations[marker["name"]] = "21"
    app.verify_baseline(storage, objects)
    app.verify_baseline(storage, objects)
    assert state.http.keys[marker["name"]] == b"" and state.http.generations[marker["name"]] == "21"
    assert not state.http.mutations


def test_baseline_sixty_four_unique_objects_fit_and_sixty_five_refuse(tmp_path):
    app = harness()
    changed = baseline()
    changed["objects"].extend({"name": ROOT + f"dt=markers/marker-{i}", "generation": str(i + 1),
                               "size": 0, "sha256": hashlib.sha256(b"").hexdigest()} for i in range(62))
    assert len(app.load_baseline(document(tmp_path, "baseline.json", changed))) == 64
    changed["objects"].append({"name": ROOT + "dt=markers/marker-last", "generation": "64", "size": 0,
                               "sha256": hashlib.sha256(b"").hexdigest()})
    with pytest.raises(ValueError):
        app.load_baseline(document(tmp_path, "baseline.json", changed))


@pytest.mark.parametrize("suffix", ("dt=marker//inner/", "dt=marker/./", "dt=marker/../", "dt=soak/", "dt=soak/inner/_SUCCESS", "_SOAK_PULSE"))
def test_marker_support_keeps_interior_and_owned_name_guards(tmp_path, suffix):
    changed = baseline()
    changed["objects"].append({"name": ROOT + suffix, "generation": "21", "size": 0,
                               "sha256": hashlib.sha256(b"").hexdigest()})
    with pytest.raises(ValueError):
        harness().load_baseline(document(tmp_path, "baseline.json", changed))


def test_empty_marker_hash_must_match_actual_empty_bytes(monkeypatch, tmp_path):
    app, storage, state = storage_setup(monkeypatch, tmp_path)
    marker = {"name": ROOT + "dt=marker/_SUCCESS", "generation": "21", "size": 0, "sha256": "a" * 64}
    state.http.keys[marker["name"]] = b""
    state.http.generations[marker["name"]] = "21"
    with pytest.raises(ValueError, match="gcp_soak_object_mismatch"):
        app.verify_baseline(storage, [marker])
    assert state.http.keys[marker["name"]] == b"" and not state.http.mutations


def test_sixty_four_baseline_names_fit_the_bounded_document_read(tmp_path):
    changed = baseline()
    changed["objects"].extend({"name": ROOT + f"dt=markers/{i:02d}/" + "x" * (1024 - len(ROOT) - len("dt=markers/00/")),
                               "generation": str(i + 1), "size": 0, "sha256": hashlib.sha256(b"").hexdigest()}
                              for i in range(62))
    path = document(tmp_path, "baseline.json", changed)
    assert 65536 < path.stat().st_size < 131072
    assert len(harness().load_baseline(path)) == 64
