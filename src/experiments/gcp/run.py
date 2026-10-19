"""Run one finite native GCP worker, preserving its input and cloud resources."""

import argparse
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
from threading import Event
import time

from google.cloud.storage import Client as StorageClient

from rtmsf.adapters.gcp import (
    GcsObjectStore, HiveMetastorePartitionCatalog, OperationDeadline,
    PubSubEventSource, _unique_json_object, create_pubsub_clients,
)
from rtmsf.interfaces import FatalExternalError, RetryableExternalError
from rtmsf.pmcp import PartitionMutationCommitProtocol
from rtmsf.routing import RoutingIndex
from rtmsf.worker import RTMSFWorker


PROJECT = "rtmsf-gcp-lab-20261018-e504"
BUCKET = PROJECT + "-fixtures"
RUN = "r20261019n1"
ROOT = "gcp-native/native-r20261019n1/data/"
LOCK_PATH = Path.home() / ".rtmsf-gcp-native.lock"
APPROVED_RUN = {
    "run_id": RUN, "table_id": "gcp_native_r20261019n1.native_events", "source_id": "fixtures",
    "bucket": BUCKET, "root_prefix": ROOT, "partition_columns": [["dt", "string"]],
    "owner_key": "gcp_probe_run_id", "owner_value": RUN,
    "owner_object": {"name": ROOT + "_OWNER.json", "generation": "1792445543821304", "size": 103,
                     "sha256": "32e03b825a089d8d1d2992de3fe373b5db72c39bb8a809f6c3c1cfdd8c1190e6"},
}
APPROVED_PIPELINE = {
    "ingress_topic": f"projects/{PROJECT}/topics/rtmsf-native-ingress",
    "subscription": f"projects/{PROJECT}/subscriptions/rtmsf-native-pull",
    "quarantine_topic": f"projects/{PROJECT}/topics/rtmsf-native-quarantine",
    "quarantine_subscription": f"projects/{PROJECT}/subscriptions/rtmsf-native-quarantine-pull",
    "notification_config_attribute": f"projects/_/buckets/{BUCKET}/notificationConfigs/3",
    "notification_id": "3", "notification_prefix": "gcp-native/", "initial_ack_deadline_seconds": 180,
}
BUDGETS = {"pmcp_seconds": 90, "rpc_seconds": 15, "source_seconds": 30,
           "lease_seconds": 180, "lease_safety_margin_ms": 1000,
           "settling_ms": 1000, "maximum_ms": 3000, "poll_size": 10,
           "max_pending_deliveries": 20, "poll_interval_seconds": 0.25}
NOTIFICATION_FIELDS = ("bucketId", "objectId", "objectGeneration", "eventType", "eventTime", "notificationConfig")


def load_resources(path):
    try:
        document = json.loads(path.read_text(), object_pairs_hook=_unique_json_object)
        if (document["project_id"] != PROJECT or document["account"] != "joshi.ankit1492@gmail.com"
                or document["metastore_endpoint"] != "thrift://10.76.0.4:9083"
                or document["catalog_gate"]["status"] != "passed"):
            raise ValueError
        selected = {}
        for section, approved in (("native_run", APPROVED_RUN), ("native_pipeline_setup", APPROVED_PIPELINE)):
            actual = {key: document[section][key] for key in approved}
            if actual != approved:
                raise ValueError
            selected[section] = actual
        return {"project_id": document["project_id"], "account": document["account"],
                "metastore_endpoint": document["metastore_endpoint"], **selected}
    except (KeyError, TypeError, ValueError, UnicodeError):
        raise ValueError("gcp_native_unapproved_configuration") from None


class JsonEvidence:
    def __init__(self, output):
        self._output = output

    def emit(self, record):
        # Core telemetry has no delivery identity; keep only its useful PMCP fields.
        if record.get("component") == "worker":
            if record.get("action") != "pmcp_result":
                return
            record = {key: record[key] for key in ("action", "partition", "status", "mutation", "observation_count")}
        record = {**record, "utc": datetime.now(timezone.utc).isoformat(), "monotonic_seconds": time.monotonic()}
        self._output.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._output.flush()


class EvidenceSource(PubSubEventSource):
    def __init__(self, *args, evidence, **kwargs):
        self._evidence, self._correlations = evidence, {}
        super().__init__(*args, **kwargs)

    def _remember(self, deliveries):
        pending = self._entries(deliveries)
        for delivery, entry in zip(deliveries, pending):
            if delivery.handle not in self._correlations:
                record = {"handle": delivery.handle, "message_id": entry.message.message_id}
                # A malformed notification's untrusted attributes are never echoed.
                if entry.events:
                    record.update({key: entry.message.attributes[key] for key in NOTIFICATION_FIELDS})
                self._correlations[delivery.handle] = record
                self._evidence.emit({"action": "event_received", **record})

    def receive(self, *args):
        deliveries = super().receive(*args)
        self._remember(deliveries)
        return deliveries

    def renew(self, deliveries, lease_seconds):
        # Initial renew runs inside receive before malformed handling or exposure.
        self._remember(deliveries)
        super().renew(deliveries, lease_seconds)
        for delivery in deliveries:
            self._evidence.emit({"action": "lease_renewed", **self._correlations[delivery.handle]})

    def acknowledge(self, deliveries):
        self._remember(deliveries)
        super().acknowledge(deliveries)
        for delivery in deliveries:
            record = self._correlations.pop(delivery.handle)
            self._evidence.emit({"action": "message_acknowledged", **record})

    def dead_letter(self, deliveries, reason):
        self._remember(deliveries)
        super().dead_letter(deliveries, reason)
        category = "malformed" if reason == "gcp_invalid_notification" else "terminal"
        for delivery in deliveries:
            self._evidence.emit({"action": "quarantine_confirmed", "category": category,
                                 **self._correlations[delivery.handle]})


class _BudgetedPMCP:
    def __init__(self, pmcp, deadline):
        self._pmcp, self._deadline = pmcp, deadline

    def apply(self, intent):
        # The outer check escapes PMCP's RETRY conversion before settlement.
        with self._deadline.operation():
            return self._pmcp.apply(intent)


def _verify_owner(storage, owner):
    with storage._deadline.operation():
        blob = storage._bucket("fixtures").blob(owner["name"])
        storage._call(blob.reload)
        if (blob.name != owner["name"] or blob._properties.get("name") != owner["name"]
                or blob._properties.get("bucket") != BUCKET or str(blob.generation) != owner["generation"]
                or blob.size != owner["size"]):
            raise ValueError("gcp_native_owner_mismatch")
        body = storage._call(blob.download_as_bytes, start=0, end=owner["size"] - 1,
                             if_generation_match=int(owner["generation"]), checksum=None)
        if len(body) != owner["size"] or hashlib.sha256(body).hexdigest() != owner["sha256"]:
            raise ValueError("gcp_native_owner_mismatch")


@contextmanager
def build_worker(config, evidence):
    native, pipeline = config["native_run"], config["native_pipeline_setup"]
    deadline = OperationDeadline(seconds=BUDGETS["pmcp_seconds"], rpc_seconds=BUDGETS["rpc_seconds"])
    with ExitStack() as stack:
        client = StorageClient(project=config["project_id"])
        stack.callback(client.close)
        storage = GcsObjectStore(client, {native["source_id"]: native["bucket"]}, deadline=deadline)
        catalog = HiveMetastorePartitionCatalog(
            config["metastore_endpoint"], table_id=native["table_id"], source_id=native["source_id"],
            bucket=native["bucket"], root_prefix=native["root_prefix"],
            partition_columns=native["partition_columns"], owner=native["owner_value"], deadline=deadline,
        )
        with deadline.operation():
            _verify_owner(storage, native["owner_object"])
            tables = catalog.resolve_tables((native["table_id"],))
        subscriber, publisher = create_pubsub_clients()
        stack.callback(subscriber.transport.close)
        stack.callback(publisher.transport.close)
        source = EvidenceSource(
            subscriber, pipeline["subscription"], publisher, pipeline["quarantine_topic"],
            {native["source_id"]: native["bucket"]},
            {native["source_id"]: pipeline["notification_config_attribute"]},
            source_topic=pipeline["ingress_topic"], evidence=evidence,
            deadline=OperationDeadline(seconds=BUDGETS["source_seconds"], rpc_seconds=BUDGETS["rpc_seconds"]),
            max_pending=BUDGETS["max_pending_deliveries"],
        )
        yield RTMSFWorker(
            source=source, pmcp=_BudgetedPMCP(PartitionMutationCommitProtocol(storage, catalog), deadline),
            telemetry=evidence, routing=RoutingIndex(tables),
            settling_ms=BUDGETS["settling_ms"], maximum_ms=BUDGETS["maximum_ms"],
            queue_lease_seconds=BUDGETS["lease_seconds"], pmcp_budget_ms=BUDGETS["pmcp_seconds"] * 1000,
            source_call_budget_ms=BUDGETS["source_seconds"] * 1000,
            lease_safety_margin_ms=BUDGETS["lease_safety_margin_ms"], poll_size=BUDGETS["poll_size"],
            max_pending_deliveries=BUDGETS["max_pending_deliveries"], poll_wait_seconds=0,
        )


@contextmanager
def _host_lock(path):
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("gcp_native_worker_already_running") from None
        yield
    finally:
        os.close(descriptor)


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError("gcp_native_invalid_arguments")


def _duration(value):
    seconds = int(value)
    if not 1 <= seconds <= 86400:
        raise ValueError("gcp_native_invalid_duration")
    return seconds


def _error_category(error):
    if isinstance(error, RetryableExternalError):
        return "external_retryable"
    if isinstance(error, FatalExternalError):
        return "external_fatal"
    return "configuration" if isinstance(error, (ValueError, TypeError, KeyError)) else "runtime"


def main(argv=None):
    try:
        parser = _Parser(description=__doc__)
        parser.add_argument("--resources", type=Path, required=True)
        parser.add_argument("--output-dir", type=Path, required=True)
        parser.add_argument("--duration-seconds", type=_duration, required=True)
        args = parser.parse_args(argv)
        with _host_lock(LOCK_PATH):
            args.output_dir.mkdir(parents=True, exist_ok=False)
            report = {"status": "failed", "started_at": datetime.now(timezone.utc).isoformat(),
                      "duration_seconds": args.duration_seconds, "budgets": BUDGETS, "pending_deliveries": 0}
            worker = None
            stop = Event()
            previous_handlers = {}
            try:
                for number in (signal.SIGTERM, signal.SIGINT):
                    previous_handlers[number] = signal.signal(number, lambda _number, _frame: stop.set())
                config = load_resources(args.resources)
                report["configuration"] = config
                with (args.output_dir / "worker-evidence.jsonl").open("x") as output:
                    with build_worker(config, JsonEvidence(output)) as worker:
                        until = time.monotonic() + args.duration_seconds
                        while not stop.is_set() and time.monotonic() < until:
                            worker.run_once()
                            if not stop.is_set():
                                remaining = until - time.monotonic()
                                if remaining > 0:
                                    time.sleep(min(BUDGETS["poll_interval_seconds"], remaining))
                        report.update(status="stopped", stop_reason="signal" if stop.is_set() else "duration")
            except Exception as error:
                report.update(status="failed", error_category=_error_category(error))
                report.pop("stop_reason", None)
                raise
            finally:
                if worker is not None:
                    report["pending_deliveries"] = len(worker._source._pending)
                report["finished_at"] = datetime.now(timezone.utc).isoformat()
                with (args.output_dir / "worker-report.json").open("x") as output:
                    json.dump(report, output, indent=2)
                for number, handler in previous_handlers.items():
                    signal.signal(number, handler)
        print(json.dumps({"status": report["status"]}), flush=True)
        return 0
    except Exception:
        print("gcp_native_worker_failed", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
