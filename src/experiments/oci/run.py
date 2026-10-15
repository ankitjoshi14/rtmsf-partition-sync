"""Finite live OCI object-event checks using the real RTMSF core and a test catalog."""

import argparse
import json
from pathlib import Path
import re
import time

import oci

from rtmsf.adapters.oci import OciObjectStore, OciQueueEventSource, enqueue_event
from rtmsf.pmcp import PartitionMutationCommitProtocol
from rtmsf.models import TableDefinition
from rtmsf.routing import RoutingIndex
from rtmsf.worker import RTMSFWorker


def normalize_report(record):
    return {key: value for key, value in record.items() if key not in {"receipt", "body"}}


def client_options(args, region):
    if args.instance_principal:
        config = {"region": region}
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    else:
        config = oci.config.from_file(str(Path(args.config_file).expanduser()), args.profile)
        signer = oci.auth.signers.SecurityTokenSigner(
            Path(config["security_token_file"]).read_text(),
            oci.signer.load_private_key_from_file(config["key_file"]))
    return config, dict(signer=signer, timeout=(3, 10), retry_strategy=oci.retry.NoneRetryStrategy())


def table_prefix(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*/", value):
        raise argparse.ArgumentTypeError("use one relative test-table directory ending in /")
    return value


class MemoryCatalog:
    """Test oracle only; no persistent catalog or query-engine visibility claim."""
    def __init__(self): self.locations = {}
    def partition_exists(self, p): return p.key in self.locations
    def create_partition(self, p, location):
        if p.key in self.locations: return False
        self.locations[p.key] = location
        return True
    def delete_partition(self, p): return self.locations.pop(p.key, None) is not None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resources", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--config-file", default="~/.oci/config")
    parser.add_argument("--profile", default="RTMSF_OCI")
    parser.add_argument("--instance-principal", action="store_true")
    parser.add_argument("--prefix", type=table_prefix, default="table-retry/")
    args = parser.parse_args()
    root = args.output_dir
    root.mkdir(parents=True, exist_ok=True)
    if (root / "live-report.json").exists():
        raise ValueError("refuse to overwrite previous lifecycle evidence")
    resource = json.loads(args.resources.read_text())
    resources = resource["resources"]
    config, options = client_options(args, resource["region"])
    client = oci.object_storage.ObjectStorageClient(config, **options)
    queue = oci.queue.QueueClient(config, service_endpoint=resources["queue"]["endpoint"], **options)
    quarantine = oci.queue.QueueClient(config, service_endpoint=resources["quarantine"]["endpoint"], **options)
    bucket, namespace = resources["bucket"], resource["namespace"]
    if bucket != "rtmsf-events-20261012" or resource["region"] != "us-sanjose-1":
        raise ValueError("refuse unrelated resources")
    prefix = args.prefix
    storage = OciObjectStore(client, {"demo": (namespace, bucket)})
    catalog = MemoryCatalog(); records = []; raw = []; checks = []
    report = {"provider": "OCI", "region": resource["region"], "catalog": "in-memory test double",
              "cloud_pipeline": "Object Storage -> Events -> Function -> Queue -> real RTMSF core",
              "prefix": prefix, "authentication": "instance principal" if args.instance_principal else "user session",
              "checks": checks, "passed": False}

    class Source(OciQueueEventSource):
        def receive(self, *args):
            deliveries = super().receive(*args)
            for d in deliveries:
                raw.append(json.loads(self._pending[d.handle].body))
            return deliveries

    source = Source(queue, resources["queue"]["id"], quarantine, resources["quarantine"]["id"],
                    {"demo": (namespace, bucket)})
    worker = RTMSFWorker(
        source=source, pmcp=PartitionMutationCommitProtocol(storage, catalog),
        telemetry=type("Telemetry", (), {"emit": staticmethod(records.append)})(),
        routing=RoutingIndex([TableDefinition("demo.table", "demo", prefix, ("dt",))]),
        settling_ms=1000, maximum_ms=3000, queue_lease_seconds=120,
        pmcp_budget_ms=20_000, source_call_budget_ms=20_000,
        poll_wait_seconds=0, max_pending_deliveries=20)

    def wait(case, expected, previous_acks):
        deadline = time.monotonic()+120
        while time.monotonic() < deadline:
            worker.run_once()
            acks = sum(x.get("action") == "delivery_acknowledged" for x in records)
            values = {key[2] for key in catalog.locations}
            if values == expected and acks > previous_acks and not source._pending:
                checks.append({"case": case, "status": "passed", "partitions": sorted(values),
                               "acknowledged_total": acks})
                return acks
            time.sleep(0.25)
        raise RuntimeError("delivery_timeout_" + case)

    def put(key, content):
        client.put_object(namespace, bucket, key, content, content_type="application/octet-stream")

    def delete(key):
        client.delete_object(namespace, bucket, key)

    try:
        # Source files are disposable; every mutation is scoped to this dedicated bucket.
        if storage.prefix_non_empty("demo", prefix): raise RuntimeError("test_prefix_not_fresh")
        acks=0
        put(prefix + "dt=a/f1", b"first")
        acks=wait("first_object_add", {"dt=a"}, acks)
        put(prefix + "dt=a/f2", b"second")
        acks=wait("additional_object_keeps_partition", {"dt=a"}, acks)
        put(prefix + "dt=a/f2", b"overwrite")
        acks=wait("overwrite_keeps_partition", {"dt=a"}, acks)
        put(prefix + "dt=b/f1", b"other")
        acks=wait("second_partition_add", {"dt=a", "dt=b"}, acks)
        delete(prefix + "dt=a/f1")
        acks=wait("partial_delete_keeps_partition", {"dt=a", "dt=b"}, acks)
        delete(prefix + "dt=a/f2")
        acks=wait("last_delete_removes_only_a", {"dt=b"}, acks)
        put(prefix + "dt=a/f1", b"recreated")
        acks=wait("recreate_adds_a", {"dt=a", "dt=b"}, acks)
        stale = next(x for x in raw if x.get("eventType", "").endswith("deleteobject")
                     and x["data"]["resourceName"] == prefix + "dt=a/f2")
        enqueue_event(queue, resources["queue"]["id"], json.dumps(stale))
        acks=wait("replayed_stale_delete_preserves_live_a", {"dt=a", "dt=b"}, acks)
        checks.append({"case": "exact_storage_readback", "status": "passed",
                       "a_exists": storage.head_exists("demo", prefix + "dt=a/f1"),
                       "b_exists": storage.head_exists("demo", prefix + "dt=b/f1")})
        if not checks[-1]["a_exists"] or not checks[-1]["b_exists"]:
            raise RuntimeError("storage_readback_failed")
        report["passed"] = True
    except Exception as error:
        report["error_type"] = type(error).__name__
        raise
    finally:
        report["observed_event_types"] = sorted({x.get("eventType", "") for x in raw})
        report["worker_records"] = [normalize_report(x) for x in records]
        (root / "live-report.json").write_text(json.dumps(report, indent=2))
        (root / "live-raw-events.json").write_text(json.dumps(raw, indent=2))
        print(json.dumps({"passed": report["passed"], "checks": len(checks),
                          "report": str(root / "live-report.json")}))


if __name__ == "__main__": main()
