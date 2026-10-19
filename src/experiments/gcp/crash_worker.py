"""SIGKILL one approved native GCP recovery intent, retaining pending input."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import sys
import time

from experiments.gcp import run
from rtmsf.models import PartitionRef


FAULTS = {
    "before_add": ("recovery_add_before", "ADD"),
    "after_add": ("recovery_add_after", "ADD"),
    "before_drop": ("recovery_drop_before", "DROP"),
    "after_drop": ("recovery_drop_after", "DROP"),
}


def matches(point, intent):
    if point not in FAULTS or intent is None:
        return False
    value, mutation = FAULTS[point]
    path = "dt=" + value
    return (intent.mutation == mutation and intent.partition == PartitionRef(
        "fixtures", run.APPROVED_RUN["table_id"], path, (value,), run.ROOT + path + "/"))


class _CrashPMCP:
    def __init__(self, delegate, point, worker, output_dir):
        if point not in FAULTS:
            raise ValueError("gcp_native_invalid_fault_point")
        self._delegate, self._point, self._worker, self._output_dir = delegate, point, worker, output_dir

    def apply(self, intent):
        matched = matches(self._point, intent)
        if matched and self._point.startswith("before"):
            self._crash(intent)
        result = self._delegate.apply(intent)
        if (matched and self._point.startswith("after") and result.status == "APPLIED"
                and result.reason == ("created" if intent.mutation == "ADD" else "deleted")):
            self._crash(intent, result)
        return result

    def _crash(self, intent, result=None):
        partition, source = intent.partition, self._worker._source
        pending = []
        fields = ("handle", "message_id", *run.NOTIFICATION_FIELDS)
        for handle in sorted(self._worker._handles_by_partition.get(partition.key, ())):
            correlation = source._correlations.get(handle)
            if (handle not in source._pending or correlation is None
                    or any(field not in correlation for field in fields)):
                continue
            event = {field: correlation[field] for field in fields}
            if (event["handle"] == handle and event["message_id"] and event["bucketId"] == run.BUCKET
                    and event["objectId"].startswith(partition.storage_prefix)
                    and event["eventType"] in {"OBJECT_FINALIZE", "OBJECT_DELETE", "OBJECT_ARCHIVE"}
                    and event["notificationConfig"] == run.APPROVED_PIPELINE["notification_config_attribute"]):
                pending.append(event)
        if not pending:
            raise RuntimeError("gcp_native_fault_missing_correlation")
        record = {"point": self._point, "pid": os.getpid(), "utc": datetime.now(timezone.utc).isoformat(),
                  "monotonic_seconds": time.monotonic(),
                  "intent": {"source_id": partition.source_id, "table_id": partition.table_id,
                             "partition_path": partition.partition_path, "values": partition.values,
                             "storage_prefix": partition.storage_prefix, "mutation": intent.mutation},
                  "pending_events": pending}
        if result is not None:
            record["result"] = {"status": result.status, "reason": result.reason}
        with (self._output_dir / "fault-marker.json").open("x") as output:
            json.dump(record, output, indent=2)
            output.flush()
            os.fsync(output.fileno())
        descriptor = os.open(self._output_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.kill(os.getpid(), signal.SIGKILL)
        raise RuntimeError("gcp_native_fault_kill_returned")


def main(argv=None):
    try:
        parser = run._Parser(description=__doc__)
        parser.add_argument("--resources", type=Path, required=True)
        parser.add_argument("--output-dir", type=Path, required=True)
        parser.add_argument("--duration-seconds", type=run._duration, required=True)
        parser.add_argument("--fault-point", choices=FAULTS, required=True)
        args = parser.parse_args(argv)
        builder = run.build_worker

        @contextmanager
        def build_worker(config, evidence):
            with builder(config, evidence) as worker:
                delegate = worker._pmcp
                worker._pmcp = _CrashPMCP(delegate, args.fault_point, worker, args.output_dir)
                try:
                    yield worker
                finally:
                    worker._pmcp = delegate

        run.build_worker = build_worker
        try:
            status = run.main(["--resources", str(args.resources), "--output-dir", str(args.output_dir),
                               "--duration-seconds", str(args.duration_seconds)])
        finally:
            run.build_worker = builder
        if status != 0:
            return status
        report = {"status": "not_triggered", "point": args.fault_point}
        with (args.output_dir / "fault-report.json").open("x") as output:
            json.dump(report, output, indent=2)
        print(json.dumps(report), flush=True)
        return 1
    except Exception:
        print("gcp_native_worker_failed", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
