"""Two-process live worker restart check; controller kills the buffered process."""

import argparse
import json
import os
from pathlib import Path
import platform
import time

import oci

from experiments.oci.run import MemoryCatalog, client_options, normalize_report, table_prefix
from rtmsf.adapters.oci import OciObjectStore, OciQueueEventSource
from rtmsf.models import TableDefinition
from rtmsf.pmcp import PartitionMutationCommitProtocol
from rtmsf.routing import RoutingIndex
from rtmsf.worker import RTMSFWorker


def verify_replay(expected_ids, observed_ids, records):
    assert expected_ids and expected_ids <= observed_ids, "same event must be redelivered"
    results = [r for r in records if r.get("action") == "pmcp_result"]
    assert len(results) == 1 and results[0]["status"] in {"APPLIED", "NOOP"} and results[0]["mutation"] == "ADD", "successful ADD required"
    buffered = sum(r.get("event_count", 0) for r in records if r.get("action") == "delivery_buffered")
    reconciled = acknowledged = 0
    for r in records:
        if r.get("action") == "pmcp_result":
            reconciled += r["observation_count"]
        elif r.get("action") == "delivery_acknowledged":
            assert not r["dead_lettered"], "unexpected quarantine"
            acknowledged += r["event_count"]
            assert acknowledged <= reconciled, "acknowledged before reconciliation"
    assert acknowledged == reconciled == buffered and buffered > 0, "all input must complete"
    return {"notifications": buffered, "reconciliations": len(results), "acknowledgements": acknowledged}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resources", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--prefix", type=table_prefix, required=True)
    parser.add_argument("--stage", choices=("buffer", "replay"), required=True)
    parser.add_argument("--config-file", default="~/.oci/config")
    parser.add_argument("--profile", default="RTMSF_OCI")
    parser.add_argument("--instance-principal", action="store_true")
    args = parser.parse_args()
    root = args.output_dir
    root.mkdir(parents=True, exist_ok=True)
    output = root / (args.stage + "-report.json")
    if output.exists():
        raise ValueError("refuse to overwrite restart evidence")
    resource = json.loads(args.resources.read_text())
    rs = resource["resources"]
    bucket, namespace = rs["bucket"], resource["namespace"]
    if bucket != "rtmsf-events-20261012" or resource["region"] != "us-sanjose-1":
        raise ValueError("refuse unrelated resources")
    previous = None
    if args.stage == "replay":
        previous = json.loads((root / "buffer-report.json").read_text())
        process_state = Path(f"/proc/{previous['pid']}/stat")
        if process_state.exists() and process_state.read_text().split()[2] != "Z":
            raise RuntimeError("previous worker must stop before replay")
        if previous["prefix"] != args.prefix or not previous["renewal_succeeded"]:
            raise ValueError("restart stages must match")
    config, options = client_options(args, resource["region"])
    objects = oci.object_storage.ObjectStorageClient(config, **options)
    queue = oci.queue.QueueClient(config, service_endpoint=rs["queue"]["endpoint"], **options)
    quarantine = oci.queue.QueueClient(config, service_endpoint=rs["quarantine"]["endpoint"], **options)
    records, raw = [], []
    started = time.monotonic()
    def emit(record):
        records.append(dict(normalize_report(record), elapsed_seconds=round(time.monotonic() - started, 3)))
    class Source(OciQueueEventSource):
        def receive(self, *values):
            deliveries = super().receive(*values)
            for d in deliveries:
                raw.append(json.loads(self._pending[d.handle].body))
            return deliveries
    source = Source(queue, rs["queue"]["id"], quarantine, rs["quarantine"]["id"], {"demo": (namespace, bucket)})
    storage, catalog = OciObjectStore(objects, {"demo": (namespace, bucket)}), MemoryCatalog()
    worker = RTMSFWorker(source=source, pmcp=PartitionMutationCommitProtocol(storage, catalog),
        telemetry=type("Telemetry", (), {"emit": staticmethod(emit)})(),
        routing=RoutingIndex([TableDefinition("demo.restart", "demo", args.prefix, ("dt",))]),
        settling_ms=1000, maximum_ms=3000, queue_lease_seconds=120,
        pmcp_budget_ms=20000, source_call_budget_ms=20000, poll_wait_seconds=0, max_pending_deliveries=20)
    key = args.prefix + "dt=a/f1"
    report = {"stage": args.stage, "prefix": args.prefix, "pid": os.getpid(), "hostname": platform.node(),
              "authentication": "instance principal" if args.instance_principal else "user session",
              "catalog": "in-memory test double", "passed": False}
    try:
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            worker.run_once()
            if args.stage == "buffer" and worker._pending:
                assert raw and all(e["data"]["resourceName"] == key for e in raw), "unexpected input"
                assert not any(r["action"] in {"pmcp_result", "delivery_acknowledged"} for r in records), "must stop before reconciliation"
                source.renew(tuple(p.delivery for p in worker._pending.values()), 120)
                report.update(renewal_succeeded=True, event_ids=sorted({e["eventID"] for e in raw}),
                              worker_records=records, events=raw, buffered_without_ack=True)
                output.write_text(json.dumps(report, indent=2) + "\n")
                print(json.dumps({"buffered": True, "pid": os.getpid()}), flush=True)
                time.sleep(180)
                raise RuntimeError("controller_did_not_stop_worker")
            if args.stage == "replay" and catalog.locations and not source._pending:
                summary = verify_replay(set(previous["event_ids"]), {e["eventID"] for e in raw}, records)
                assert storage.head_exists("demo", key), "authoritative object must exist"
                report.update(same_event_redelivered=True, previous_pid=previous["pid"], previous_worker_stopped=True,
                              object_verified=True, partition_added=True, replay_summary=summary)
                objects.delete_object(namespace, bucket, key)
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("restart_delivery_timeout")
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            worker.run_once()
            drops = [r for r in records if r["action"] == "pmcp_result" and r["mutation"] == "DROP"]
            if drops and not catalog.locations and not source._pending:
                assert all(r["status"] in {"APPLIED", "NOOP"} for r in drops)
                assert not storage.prefix_non_empty("demo", args.prefix)
                report.update(passed=True, cleanup_delete_reconciled=True)
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("restart_cleanup_timeout")
    except Exception as error:
        report["error_type"] = type(error).__name__
        raise
    finally:
        report.update(worker_records=records, events=raw, duration_seconds=round(time.monotonic() - started, 3))
        output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "report": str(output)}), flush=True)


if __name__ == "__main__":
    main()
