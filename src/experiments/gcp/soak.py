"""Finite native GCP soak using ordinary VM ADC and one fresh worker at a time.

Gate JSON: {"run_id": RUN, "gates": {name: {"status": "passed", "evidence":
"path/to/live-evidence.json"}}} for every GATES name below. `versions` also
attests that bucket versioning is off and soft-delete retention is zero for
this current-object experiment; OBJECT_ARCHIVE cannot prove its final DELETE.
Provisioning or catalog-safety success cannot substitute for these live gates.

Baseline JSON: {"run_id": RUN, "objects": [{"name": ..., "generation": "...",
"size": ..., "sha256": "..."}], "catalog_partitions": ..., "catalog_rows": ...}.
The last two fields are informational. The <=64 objects must include the exact
immutable owner and b sentinel below; all are read back before any mutation.
Sizes may be 0..1MiB, including empty markers and exact terminal-slash names.
Only dt=soak/part-00000.parquet and the nonrouted _SOAK_PULSE may be mutated.
The pulse is retained, every child has its own evidence, and final query proof
must be supplied independently. Completion drains already observed own message
identities across child exits, with a 64-message limit and the finite settling
budget. No resource cleanup, global queue-empty, or exactly-once claim.
"""

import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
from threading import Event
import time

from google.cloud.storage import Client as StorageClient

from experiments.gcp import run
from rtmsf.adapters.gcp import GcsObjectStore, HiveMetastorePartitionCatalog, OperationDeadline, _unique_json_object
from rtmsf.models import PartitionRef


GATES = ("native_add_query", "lifecycle_drop_query", "recovery_before_add", "recovery_after_add",
         "recovery_before_drop", "recovery_after_drop", "markers", "special_names", "burst",
         "malformed_quarantine", "versions", "stale_duplicate_replay")
SOAK_PREFIX = run.ROOT + "dt=soak/"
SOAK_KEY = SOAK_PREFIX + "part-00000.parquet"
PULSE_KEY = run.ROOT + "_SOAK_PULSE"
LOCK_PATH = Path.home() / ".rtmsf-gcp-soak.lock"
FIXTURE_SHA = "aee59617040d80855ccce0df4354921299da7b1c4c8b090c703870cbb0b19eb3"
B_SENTINEL = {"name": run.ROOT + "dt=b/part-00000.parquet", "generation": "1792448993562638", "size": 438,
              "sha256": "e824f52db65a5f8e1cba3e36dc0943021c620dfeebff240d4b4f649c32ebb30f"}


def _json(path):
    with path.open("rb") as source:
        body = source.read(131073)
    if len(body) > 131072:
        raise ValueError("gcp_soak_document_too_large")
    return json.loads(body, object_pairs_hook=_unique_json_object)


def load_gates(path):
    try:
        value = _json(path)
        if value["run_id"] != run.RUN:
            raise ValueError
        for name in GATES:
            gate = value["gates"][name]
            if (gate["status"] != "passed" or not isinstance(gate["evidence"], str)
                    or not 1 <= len(gate["evidence"]) <= 512 or not gate["evidence"].strip()):
                raise ValueError
        return list(GATES)
    except (KeyError, TypeError, ValueError, UnicodeError):
        raise ValueError("gcp_soak_correctness_gates_required") from None


def _generation(value):
    return (isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{0,19}", value) is not None
            and int(value) <= 18446744073709551615)


def load_baseline(path):
    try:
        value = _json(path)
        objects = value["objects"]
        if value["run_id"] != run.RUN or not isinstance(objects, list) or not 2 <= len(objects) <= 64:
            raise ValueError
        names = set()
        for item in objects:
            if not isinstance(item, dict) or set(item) != {"name", "generation", "size", "sha256"}:
                raise ValueError
            name = item["name"]
            if (not isinstance(name, str) or not name.startswith(run.ROOT) or len(name.encode("utf-8")) > 1024
                    or any(segment in ("", ".", "..") for segment in name.removesuffix("/").split("/"))
                    or name.startswith(SOAK_PREFIX) or name == PULSE_KEY or name in names
                    or not _generation(item["generation"]) or type(item["size"]) is not int
                    or not 0 <= item["size"] <= 1048576 or not isinstance(item["sha256"], str)
                    or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None):
                raise ValueError
            names.add(name)
        if run.APPROVED_RUN["owner_object"] not in objects or B_SENTINEL not in objects:
            raise ValueError
        return objects
    except (KeyError, TypeError, ValueError, UnicodeError):
        raise ValueError("gcp_soak_invalid_baseline") from None


def load_fixture(path):
    with path.open("rb") as source:
        body = source.read(439)
    # The pinned bytes already prove the Parquet schema and the probe-a value.
    if len(body) != 438 or hashlib.sha256(body).hexdigest() != FIXTURE_SHA:
        raise ValueError("gcp_soak_invalid_fixture")
    return body


def _metadata(blob, name, size, sha256):
    generation = str(blob.generation)
    if (blob.name != name or not isinstance(blob._properties, dict)
            or blob._properties.get("name") != name or blob._properties.get("bucket") != run.BUCKET
            or blob._properties.get("generation") != generation or blob._properties.get("size") != str(size)
            or not _generation(generation) or blob.size != size):
        raise ValueError("gcp_soak_object_mismatch")
    return {"name": name, "generation": generation, "size": size, "sha256": sha256}


def _verify_blob(storage, expected):
    blob = storage._bucket("fixtures").blob(expected["name"])
    storage._call(blob.reload)
    if _metadata(blob, expected["name"], expected["size"], expected["sha256"]) != expected:
        raise ValueError("gcp_soak_object_mismatch")
    byte_range = {"start": 0, "end": expected["size"] - 1} if expected["size"] else {}
    body = storage._call(blob.download_as_bytes, if_generation_match=int(expected["generation"]),
                         checksum=None, **byte_range)
    if len(body) != expected["size"] or hashlib.sha256(body).hexdigest() != expected["sha256"]:
        raise ValueError("gcp_soak_object_mismatch")
    return blob


def verify_baseline(storage, objects):
    with storage._deadline.operation():
        for expected in objects:
            _verify_blob(storage, expected)


def mutate(storage, name, body, owned):
    if (name not in {SOAK_KEY, PULSE_KEY} or (body is None and owned is None)
            or (owned is not None and (owned.get("name") != name or not _generation(owned.get("generation"))))):
        raise ValueError("gcp_soak_unowned_mutation")
    with storage._deadline.operation():
        blob = storage._bucket("fixtures").blob(name) if owned is None else _verify_blob(storage, owned)
        generation = 0 if owned is None else int(owned["generation"])
        if body is None:
            storage._call(blob.delete, if_generation_match=generation)
            if storage.head_exists("fixtures", name):
                raise ValueError("gcp_soak_delete_readback_present")
            return None
        storage._call(blob.upload_from_string, body, if_generation_match=generation, checksum=None,
                      content_type="application/octet-stream")
        expected = _metadata(blob, name, len(body), hashlib.sha256(body).hexdigest())
        _verify_blob(storage, expected)
        return expected


class _Tail:
    """Incremental child evidence; retain at most the source's 20 pending handles."""

    def __init__(self, path, report, own_messages=None):
        self.path, self.report = path, report
        self.own_messages = set() if own_messages is None else own_messages
        self.offset, self.partial, self.pending = 0, b"", {}
        self.sequence, self.reconciliation = 0, None

    def read(self):
        if not self.path.exists():
            return
        with self.path.open("rb") as source:
            source.seek(self.offset)
            chunk = source.read(65536)
            self.offset = source.tell()
        lines = (self.partial + chunk).split(b"\n")
        self.partial = lines.pop()
        if len(self.partial) > 8192 or any(len(line) > 8192 for line in lines):
            raise ValueError("gcp_soak_invalid_evidence")
        for line in lines:
            try:
                record = json.loads(line, object_pairs_hook=_unique_json_object)
                if not isinstance(record, dict):
                    raise ValueError
            except (TypeError, ValueError, UnicodeError):
                raise ValueError("gcp_soak_invalid_evidence") from None
            self._record(record)
        self.report["pending_observed"] = len(self.pending)
        self.report["unacknowledged_own_messages"] = len(self.own_messages)

    def _identity(self, record):
        if (record.get("bucketId") != run.BUCKET or record.get("objectId") != SOAK_KEY
                or record.get("notificationConfig") != run.APPROVED_PIPELINE["notification_config_attribute"]
                or not _generation(record.get("objectGeneration"))
                or record.get("eventType") not in {"OBJECT_FINALIZE", "OBJECT_DELETE", "OBJECT_ARCHIVE"}
                or any(not isinstance(record.get(field), str) or not 1 <= len(record[field]) <= 256
                       for field in ("handle", "message_id", "eventTime"))):
            return None
        return tuple(record[field] for field in ("handle", "message_id", "objectGeneration", "eventType", "eventTime"))

    def _record(self, record):
        self.report["evidence_records"] += 1
        action = record.get("action")
        if action == "pmcp_result" and record.get("partition") == "dt=soak":
            self.sequence += 1
            self.reconciliation = (self.sequence, record.get("mutation"), record.get("status"))
        elif action == "quarantine_confirmed":
            category = record.get("category")
            if category not in {"malformed", "terminal"}:
                raise ValueError("gcp_soak_invalid_evidence")
            self.report["quarantine"][category] += 1
            pending = self.pending.get(record.get("handle"))
            if pending is not None:
                pending["quarantined"] = True
        elif action in {"event_received", "message_acknowledged"}:
            identity = self._identity(record)
            if identity is None:
                return
            handle, _, generation, kind, _ = identity
            if action == "event_received":
                message = identity[1:]
                if message not in self.own_messages and len(self.own_messages) >= 64:
                    raise RuntimeError("gcp_soak_own_message_limit")
                if handle not in self.pending:
                    if len(self.pending) >= 20:
                        raise RuntimeError("gcp_soak_pending_evidence_limit")
                    self.pending[handle] = {"identity": identity, "sequence": self.sequence, "quarantined": False}
                self.own_messages.add(message)
            else:
                pending = self.pending.pop(handle, None)
                if pending is not None and pending["identity"] == identity:
                    self.own_messages.discard(identity[1:])
                if (pending is not None and pending["identity"] == identity and not pending["quarantined"]
                        and kind == "OBJECT_DELETE" and generation == self.report["final_delete_generation"]
                        and self.reconciliation is not None and self.reconciliation[0] > pending["sequence"]
                        and self.reconciliation[1] == "DROP" and self.reconciliation[2] in {"APPLIED", "NOOP"}):
                    self.report["final_delete_ack"] = {"object_key": SOAK_KEY, "generation": generation, "event_type": kind,
                                                       "after_reconciliation": True}
        if action in {"pmcp_result", "event_received", "message_acknowledged", "quarantine_confirmed", "lease_renewed"}:
            self.report["latest_progress"] = action
            self.report["latest_progress_at"] = run.datetime.now(run.timezone.utc).isoformat()


def _checkpoint(root, report):
    report["updated_at"] = run.datetime.now(run.timezone.utc).isoformat()
    temporary = root / "soak-report.json.tmp"
    with temporary.open("w") as output:
        json.dump(report, output, indent=2)
        output.flush()
    os.replace(temporary, root / "soak-report.json")


class _Child:
    def __init__(self, args, report, evidence, stop_signal):
        self.args, self.report, self.evidence = args, report, evidence
        self.stop_signal = stop_signal
        self.own_messages = set()
        self.process, self.files, self.tail = None, [], None

    def start(self, until):
        if self.stop_signal.is_set():
            return
        if self.process is not None:
            raise RuntimeError("gcp_soak_child_overlap")
        with run._host_lock(run.LOCK_PATH):
            pass
        index = self.report["child_starts"] + 1
        self.root = self.args.output_dir / f"child-{index:06d}"
        try:
            self.files = []
            for stream in ("stdout", "stderr"):
                self.files.append((self.args.output_dir / f"child-{index:06d}.{stream}").open("x"))
            if self.stop_signal.is_set():
                for output in self.files:
                    output.close()
                self.files = []
                return
            self.process = subprocess.Popen([
                sys.executable, "-m", "experiments.gcp.run", "--resources", str(self.args.resources),
                "--output-dir", str(self.root), "--duration-seconds",
                str(min(86400, max(1, math.ceil(until - time.monotonic())))),
            ], stdout=self.files[0], stderr=self.files[1])
        except Exception:
            for output in self.files:
                output.close()
            self.files = []
            raise
        self.tail = _Tail(self.root / "worker-evidence.jsonl", self.report, self.own_messages)
        self.report.update(child_starts=index, child_pid=self.process.pid, pending_observed=0)
        if index > 1:
            self.report["child_restarts"] += 1
        self.evidence.emit({"action": "child_started", "index": index, "pid": self.process.pid})

    def _exit(self, code, expected=False, waited=False):
        if self.process.poll() is None:
            raise RuntimeError("gcp_soak_child_still_running")
        if not waited:
            self.process.wait(timeout=0)
        for output in self.files:
            output.close()
        self.files = []
        # Drain an exited finite file incrementally, with a separate local budget.
        until = time.monotonic() + 30
        evidence_error = False
        try:
            while self.tail.path.exists() and self.tail.offset < self.tail.path.stat().st_size:
                if time.monotonic() >= until:
                    raise RuntimeError("gcp_soak_evidence_drain_deadline")
                self.tail.read()
            if self.tail.partial:
                raise ValueError("gcp_soak_invalid_evidence")
        except Exception:
            evidence_error = True
        category, status, pending, reason = "missing_report", "unknown", None, None
        try:
            value = _json(self.root / "worker-report.json")
            status = value.get("status") if value.get("status") in {"failed", "stopped"} else "unknown"
            category = value.get("error_category")
            if category not in {"external_retryable", "external_fatal", "configuration", "runtime", None}:
                category = "unknown"
            pending = value.get("pending_deliveries")
            if type(pending) is not int or not 0 <= pending <= 20:
                raise ValueError
            reason = value.get("stop_reason") if value.get("stop_reason") in {"duration", "signal"} else None
        except (OSError, ValueError, TypeError, AttributeError):
            category, status, pending = "missing_report", "unknown", None
        clean_exit = (code == 0 and status == "stopped" and category is None
                      and (reason == "duration" or (expected and reason == "signal")))
        retryable_exit = code > 0 and status == "failed" and category == "external_retryable"
        record = {"action": "child_exited", "index": self.report["child_starts"], "return_code": code,
                  "status": status, "category": category, "pending_deliveries": pending, "expected_stop": expected,
                  "evidence_error": evidence_error}
        if retryable_exit:
            record.update(classification="external_retryable_process_restart", cause="unproven")
        self.report["child_exits"] += 1
        if not clean_exit:
            self.report["failed_children"] += 1
        self.report.update(latest_child_exit=record, child_pid=None)
        self.process, self.tail = None, None
        self.evidence.emit(record)
        if evidence_error:
            raise ValueError("gcp_soak_invalid_evidence")
        if not clean_exit and not retryable_exit:
            raise RuntimeError("gcp_soak_unrecoverable_child_exit")

    def check(self, until):
        if self.process is not None:
            self.tail.read()
            code = self.process.poll()
            if code is not None:
                self._exit(code)
        if self.process is None and not self.stop_signal.is_set() and time.monotonic() < until:
            self.start(until)

    def stop(self):
        if self.process is None:
            return
        code = self.process.poll()
        requested = code is None
        if requested:
            self.process.terminate()
            try:
                code = self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.report["forced_child_kills"] += 1
                code = self.process.wait(timeout=15)
        self._exit(code, expected=requested, waited=requested)


def _produce(storage, name, body, owned, args, report, evidence):
    record = {"action": "producer_attempt", "name": name, "mutation": "delete" if body is None else "upload",
              "if_generation_match": "0" if owned is None else owned["generation"]}
    report["mutation_pending"] = record
    evidence.emit(record)
    _checkpoint(args.output_dir, report)
    result = mutate(storage, name, body, owned)
    report["mutation_pending"] = None
    kind = "pulses" if name == PULSE_KEY else "deletes" if body is None else "creates"
    report["producer"][kind] += 1
    if name == SOAK_KEY:
        report.update(current_generation=None if result is None else result["generation"], current_object=result)
        if body is None:
            report.update(final_delete_generation=owned["generation"], final_delete_ack=None)
    else:
        report["pulse_generation"] = result["generation"]
    report["latest_progress"] = "producer_" + kind
    report["latest_progress_at"] = run.datetime.now(run.timezone.utc).isoformat()
    evidence.emit({"action": "producer_confirmed", "name": name, "generation": None if result is None else result["generation"],
                   "deleted_generation": owned["generation"] if body is None else None})
    _checkpoint(args.output_dir, report)
    return result


def _soak(args, config, objects, fixture, report, evidence, stop, child):
    deadline = OperationDeadline()
    client = StorageClient(project=run.PROJECT)
    try:
        storage = GcsObjectStore(client, {"fixtures": run.BUCKET}, deadline=deadline)
        native = config["native_run"]
        catalog = HiveMetastorePartitionCatalog(config["metastore_endpoint"], table_id=native["table_id"],
            source_id="fixtures", bucket=run.BUCKET, root_prefix=run.ROOT,
            partition_columns=native["partition_columns"], owner=run.RUN, deadline=deadline)
        partition = PartitionRef("fixtures", native["table_id"], "dt=soak", ("soak",), SOAK_PREFIX)
        verify_baseline(storage, objects)
        with deadline.operation():
            bucket = storage._bucket("fixtures")
            storage._call(bucket.reload)
            properties = bucket._properties
            if (properties.get("name") != run.BUCKET or bucket.versioning_enabled is not False
                    or properties.get("softDeletePolicy", {}).get("retentionDurationSeconds") != "0"):
                raise ValueError("gcp_soak_current_object_configuration_required")
        report["bucket_current_object_configuration"] = "passed"
        catalog.resolve_tables((native["table_id"],))
        if (storage.prefix_non_empty("fixtures", SOAK_PREFIX) or storage.head_exists("fixtures", PULSE_KEY)
                or catalog.partition_exists(partition)):
            raise ValueError("gcp_soak_initial_state_not_empty")
        report.update(baseline_initial="passed", phase="producer")
        _checkpoint(args.output_dir, report)
        if stop.is_set():
            return False
        start = time.monotonic()
        until = start + args.duration_seconds
        report["producer_started_at"] = run.datetime.now(run.timezone.utc).isoformat()
        child.start(until + 300)
        owned, tick = None, start
        while not stop.is_set() and time.monotonic() < until:
            child.check(until + 300)
            if not stop.is_set() and time.monotonic() >= tick and time.monotonic() < until:
                owned = _produce(storage, SOAK_KEY, fixture if owned is None else None, owned, args, report, evidence)
                tick += 5
            _checkpoint(args.output_dir, report)
            stop.wait(max(0, min(1, until - time.monotonic(), tick - time.monotonic())))
        if stop.is_set():
            return False
        report["producer_finished_at"] = run.datetime.now(run.timezone.utc).isoformat()
        if owned is not None:
            owned = _produce(storage, SOAK_KEY, None, owned, args, report, evidence)
        report["phase"] = "settling"
        until = time.monotonic() + 300
        pulse, tick = None, time.monotonic()
        while not stop.is_set() and time.monotonic() < until:
            child.check(until)
            if not stop.is_set() and time.monotonic() >= tick and time.monotonic() < until:
                pulse = _produce(storage, PULSE_KEY, b"soak-pulse", pulse, args, report, evidence)
                tick += 5
            if (not stop.is_set() and time.monotonic() < until and report["final_delete_ack"] is not None
                    and report["pending_observed"] == 0 and report["unacknowledged_own_messages"] == 0):
                report["final_storage_empty"] = not storage.prefix_non_empty("fixtures", SOAK_PREFIX)
                report["final_catalog_absent"] = not catalog.partition_exists(partition)
                if report["final_storage_empty"] and report["final_catalog_absent"]:
                    report["phase"] = "final_checks"
                    verify_baseline(storage, objects)
                    if not stop.is_set() and time.monotonic() < until:
                        report["baseline_final"] = "passed"
                        return True
            _checkpoint(args.output_dir, report)
            stop.wait(max(0, min(1, until - time.monotonic(), tick - time.monotonic())))
        if stop.is_set():
            return False
        raise RuntimeError("gcp_soak_settling_deadline")
    finally:
        # Keep the supervisor lock and all input/evidence through confirmed exit.
        try:
            child.stop()
        finally:
            client.close()


def main(argv=None):
    report, args, previous_handlers = None, None, {}
    try:
        parser = run._Parser(description=__doc__)
        for name in ("resources", "gates", "baseline", "fixture-a", "output-dir"):
            parser.add_argument("--" + name, type=Path, required=True)
        parser.add_argument("--duration-seconds", type=run._duration, required=True)
        args = parser.parse_args(argv)
        config = run.load_resources(args.resources)
        passed = load_gates(args.gates)
        objects = load_baseline(args.baseline)
        fixture = load_fixture(args.fixture_a)
        with run._host_lock(LOCK_PATH):
            with run._host_lock(run.LOCK_PATH):
                pass
            args.output_dir.mkdir(parents=True, exist_ok=False)
            report = {"status": "running", "run_id": run.RUN, "duration_seconds": args.duration_seconds,
                      "started_at": run.datetime.now(run.timezone.utc).isoformat(), "phase": "guards", "passed_gates": passed,
                      "producer": {"creates": 0, "deletes": 0, "pulses": 0}, "current_generation": None,
                      "current_object": None, "pulse_generation": None, "mutation_pending": None, "producer_uncertain": False,
                      "child_pid": None, "child_starts": 0, "child_exits": 0, "child_restarts": 0, "failed_children": 0,
                      "forced_child_kills": 0, "pending_observed": 0, "evidence_records": 0,
                      "unacknowledged_own_messages": 0,
                      "quarantine": {"malformed": 0, "terminal": 0}, "latest_progress": "guards",
                      "final_delete_generation": None, "final_delete_ack": None, "final_storage_empty": None,
                      "final_catalog_absent": None, "baseline_initial": "pending", "baseline_final": "pending",
                      "final_query": "pending_external_named_table_query",
                      "known_limits": ["GCS notifications have no delivery latency SLA; this is a finite observation.",
                          "Stale ADD input at final empty state may reach the existing retry/quarantine policy.",
                          "SDK authentication and in-flight calls may overrun phase budgets before elapsed checks reject them.",
                          "Retryable child failures have unproven source/PMCP cause; no uninterrupted or exactly-once claim."]}
            stop = Event()
            for number in (signal.SIGTERM, signal.SIGINT):
                previous_handlers[number] = signal.signal(number, lambda _number, _frame: stop.set())
            try:
                # Freeze only the already validated manifest fields for every fresh child.
                args.resources = args.output_dir / "approved-resources.json"
                with args.resources.open("x") as output:
                    json.dump({**config, "catalog_gate": {"status": "passed"}}, output)
                run.load_resources(args.resources)
                _checkpoint(args.output_dir, report)
                with (args.output_dir / "soak-evidence.jsonl").open("x") as output:
                    evidence = run.JsonEvidence(output)
                    child = _Child(args, report, evidence, stop)
                    complete = _soak(args, config, objects, fixture, report, evidence, stop, child)
                if not complete or stop.is_set():
                    report.update(status="stopped", stop_reason="signal")
                else:
                    if report["pending_observed"] or report["unacknowledged_own_messages"]:
                        raise RuntimeError("gcp_soak_own_input_not_drained")
                    report["status"] = ("completed_with_restarts" if report["child_restarts"]
                                        else "completed_with_terminal_events" if report["failed_children"]
                                        or any(report["quarantine"].values()) else "completed")
                report["phase"] = "finished"
            except Exception as error:
                report.update(status="failed", error_category=run._error_category(error),
                              producer_uncertain=report["mutation_pending"] is not None)
            finally:
                report["finished_at"] = run.datetime.now(run.timezone.utc).isoformat()
                _checkpoint(args.output_dir, report)
                for number, handler in previous_handlers.items():
                    signal.signal(number, handler)
        print(json.dumps({"status": report["status"]}), flush=True)
        return 0 if report["status"].startswith("completed") else 1
    except Exception:
        print("gcp_soak_failed", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
