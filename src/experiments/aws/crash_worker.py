"""Experiment-only fault composition around the production AWS worker.

Run with ``python -m experiments.aws.crash_worker`` and RTMSF_EXPERIMENT_FAULT JSON.
Crash trials require operation, partition_path, point, marker_bucket, marker_key.
Retry trials instead omit point/marker fields and set fail_catalog_calls > 0.
"""

from dataclasses import dataclass
import json
import os
import signal
from threading import Event
from typing import Mapping
from urllib.request import urlopen
from urllib.parse import urlsplit

import boto3

from rtmsf.adapters.aws import JsonTelemetry, _invoke, _timestamp
from rtmsf.aws_app import AppConfig, PMCP_BUDGET_MS, SDK_CALL_BUDGET_MS, _sdk_config, build_worker
from rtmsf.models import PartitionIntent
from rtmsf.pmcp import PartitionMutationCommitProtocol
from rtmsf.interfaces import FatalExternalError, RetryableExternalError


@dataclass(frozen=True)
class FaultPlan:
    operation: str
    partition_path: str
    point: str | None = None
    marker_bucket: str | None = None
    marker_key: str | None = None
    table_id: str | None = None
    source_id: str | None = None
    fail_catalog_calls: int = 0

    def __post_init__(self):
        if self.operation not in {"ADD", "DROP"}:
            raise ValueError("fault operation must be ADD or DROP")
        if not isinstance(self.partition_path, str) or not self.partition_path or any(not part or "=" not in part for part in self.partition_path.split("/")):
            raise ValueError("fault partition_path must be an exact relative partition path")
        if type(self.fail_catalog_calls) is not int or self.fail_catalog_calls < 0:
            raise ValueError("fail_catalog_calls must be a nonnegative integer")
        for value in (self.table_id, self.source_id):
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError("optional fault source_id and table_id must be nonempty strings")
        if self.fail_catalog_calls:
            if any(value is not None for value in (self.point, self.marker_bucket, self.marker_key)):
                raise ValueError("catalog retry faults and crash faults are separate trials")
        elif self.point not in {"before_processing", "after_catalog_change"}:
            raise ValueError("fault point must be before_processing or after_catalog_change")
        elif not isinstance(self.marker_bucket, str) or not self.marker_bucket or not isinstance(self.marker_key, str) or not self.marker_key.startswith("_experiment/crash/") or not self.marker_key.endswith(".json"):
            raise ValueError("crash fault requires a marker bucket and _experiment/crash/*.json key")

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] = os.environ):
        try:
            document = json.loads(environ["RTMSF_EXPERIMENT_FAULT"])
            if not isinstance(document, dict):
                raise ValueError
            return cls(**document)
        except (KeyError, TypeError, ValueError):
            raise ValueError("invalid RTMSF_EXPERIMENT_FAULT configuration") from None

    def matches(self, intent: PartitionIntent) -> bool:
        partition = intent.partition
        return (
            intent.mutation == self.operation
            and partition.partition_path == self.partition_path
            and (self.table_id is None or partition.table_id == self.table_id)
            and (self.source_id is None or partition.source_id == self.source_id)
        )


class ExperimentTelemetry:
    """Retain only pending public SQS IDs for durable crash evidence."""

    def __init__(self, delegate):
        self._delegate = delegate
        self.pending_message_ids: set[str] = set()

    def emit(self, record):
        message_id = record.get("message_id")
        if message_id:
            if record.get("action") == "message_received":
                self.pending_message_ids.add(message_id)
            elif record.get("action") == "message_acknowledged":
                self.pending_message_ids.discard(message_id)
        self._delegate.emit(record)


class ExperimentFaults:
    def __init__(self, plan: FaultPlan, marker_client, telemetry: ExperimentTelemetry, *, task_id: str | None = None):
        self.plan = plan
        self._marker_client = marker_client
        self._telemetry = telemetry
        self._task_id = task_id
        self._marker_checked = False
        self._failures_remaining = plan.fail_catalog_calls
        self._intent: PartitionIntent | None = None

    def wrap_pmcp(self, storage, catalog):
        faults = self

        class Catalog:
            def resolve_tables(self, table_ids):
                return catalog.resolve_tables(table_ids)

            def partition_exists(self, partition):
                if faults._intent is not None and faults.plan.matches(faults._intent) and faults._failures_remaining:
                    faults._failures_remaining -= 1
                    faults._telemetry.emit({
                        "component": "experiment", "action": "catalog_failure_injected",
                        "phase": "experiment", "operation": faults.plan.operation,
                        "partition_path": partition.partition_path,
                        "failures_remaining": faults._failures_remaining,
                        "pending_message_ids": sorted(faults._telemetry.pending_message_ids),
                    })
                    raise RetryableExternalError("experiment_catalog_retry")
                return catalog.partition_exists(partition)

            def create_partition(self, partition, location):
                changed = catalog.create_partition(partition, location)
                if changed:
                    faults._after_catalog_change("ADD")
                return changed

            def delete_partition(self, partition):
                changed = catalog.delete_partition(partition)
                if changed:
                    faults._after_catalog_change("DROP")
                return changed

        real_pmcp = PartitionMutationCommitProtocol(storage, Catalog())

        class PMCP:
            def apply(self, intent):
                faults._intent = intent
                try:
                    if faults.plan.matches(intent) and faults.plan.point == "before_processing":
                        faults._crash()
                    return real_pmcp.apply(intent)
                finally:
                    faults._intent = None

        return PMCP()

    def _after_catalog_change(self, operation):
        if self._intent is not None and self.plan.matches(self._intent) and self.plan.point == "after_catalog_change" and operation == self.plan.operation:
            self._crash()

    def _crash(self):
        if self._marker_checked:
            return
        partition = self._intent.partition
        evidence = {
            "schema_version": 1, "timestamp": _timestamp(),
            "component": "experiment", "action": "process_crash", "phase": "experiment",
            "operation": self.plan.operation, "point": self.plan.point,
            "source_id": partition.source_id, "table_id": partition.table_id,
            "partition_path": partition.partition_path,
            "task_id": self._task_id, "exit_code": 86,
            # A delivery can contain multiple partitions. These are pending IDs,
            # not an assertion that each message contains the selected target.
            "pending_message_ids": sorted(self._telemetry.pending_message_ids),
        }
        try:
            response = _invoke(
                self._telemetry, "S3.PutObject", self._marker_client.put_object,
                phase="experiment", expected_errors=frozenset({"PreconditionFailed", "412"}),
                Bucket=self.plan.marker_bucket, Key=self.plan.marker_key,
                Body=json.dumps(evidence, sort_keys=True).encode("utf-8"),
                ContentType="application/json", IfNoneMatch="*",
            )
        except (FatalExternalError, RetryableExternalError):
            # Bypass PMCP's normal provider-error handling: a failed experiment
            # marker must terminate without turning this delivery into a DLQ ack.
            raise RuntimeError("could not persist experiment crash marker") from None
        self._marker_checked = True
        if response is not None:
            self._telemetry.emit(evidence)
            os._exit(86)


def build_experiment_worker(config: AppConfig, plan: FaultPlan, *, task_id: str | None = None):
    telemetry = ExperimentTelemetry(JsonTelemetry())
    marker_client = boto3.client("s3", config=_sdk_config()) if plan.point is not None else None
    faults = ExperimentFaults(plan, marker_client, telemetry, task_id=task_id)

    def compose(storage, catalog):
        tables = catalog.resolve_tables(config.table_ids)  # Cached by the real adapter.
        if plan.point is not None:
            if plan.marker_bucket not in config.sources.values():
                raise ValueError("experiment marker bucket must be a configured source")
            if any(config.sources[table.source_id] == plan.marker_bucket and plan.marker_key.startswith(table.root_prefix) for table in tables):
                raise ValueError("experiment marker must be outside all routed table roots")
        if plan.table_id is not None and plan.table_id not in config.table_ids:
            raise ValueError("fault table_id must be a configured table")
        if plan.source_id is not None and plan.source_id not in config.sources:
            raise ValueError("fault source_id must be a configured source")
        return faults.wrap_pmcp(storage, catalog)

    return build_worker(
        config, pmcp_factory=compose, telemetry=telemetry,
        pmcp_budget_ms=PMCP_BUDGET_MS + (SDK_CALL_BUDGET_MS if plan.point else 0),
    )


def _task_id(environ: Mapping[str, str]) -> str:
    if environ.get("RTMSF_EXPERIMENT_TASK_ID"):
        return environ["RTMSF_EXPERIMENT_TASK_ID"]
    metadata_uri = environ.get("ECS_CONTAINER_METADATA_URI_V4", "")
    parsed = urlsplit(metadata_uri)
    if parsed.scheme == "http" and parsed.hostname == "169.254.170.2":
        try:
            with urlopen(metadata_uri + "/task", timeout=2) as response:
                return json.load(response)["TaskARN"]
        except (OSError, KeyError, ValueError):
            pass
    return "hostname:" + os.uname().nodename


def main():
    config = AppConfig.from_environment()
    plan = FaultPlan.from_environment()
    stop = Event()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: stop.set())
    build_experiment_worker(config, plan, task_id=_task_id(os.environ)).run_forever(stop.is_set)


if __name__ == "__main__":
    main()
