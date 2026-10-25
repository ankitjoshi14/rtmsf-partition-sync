"""Read-only post-run witness; independent raw Hive and generation-pinned bytes."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import sys

from google.cloud.storage import Client
from pymetastore.hive_metastore import ThriftHiveMetastore, ttypes
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport
from experiments.gcp import run, soak
from rtmsf.adapters.gcp import GcsObjectStore


def expired(_number, _frame):
    raise TimeoutError("independent_check_deadline")


signal.signal(signal.SIGALRM, expired)
signal.alarm(300)
baseline = json.loads(Path(sys.argv[1]).read_text())
report_path = Path(sys.argv[2]) if sys.argv[2] != "-" else None
report = json.loads(report_path.read_text()) if report_path else None
result = {"scope": "read_only", "baseline_objects": [], "processes_exited": [], "locks_available": []}
for path in (run.LOCK_PATH, soak.LOCK_PATH):
    with path.open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result["locks_available"].append(path.name)
if report:
    assert report["status"].startswith("completed")
    assert report["pending_observed"] == report["unacknowledged_own_messages"] == 0
    assert report["child_pid"] is None and report["child_starts"] == report["child_exits"]
    records = [json.loads(line) for line in (report_path.parent / "soak-evidence.jsonl").read_text().splitlines()]
    for record in records:
        if record.get("action") == "child_started":
            pid = record["pid"]
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                result["processes_exited"].append(pid)
            else:
                raise AssertionError("child_process_remains")
with Client(project=run.PROJECT) as client:
    guarded = GcsObjectStore(client, {"fixtures": run.BUCKET})
    bucket = guarded._bucket("fixtures")
    bucket.reload(retry=None, timeout=15)
    assert bucket.versioning_enabled is False
    assert bucket._properties["softDeletePolicy"]["retentionDurationSeconds"] == "0"
    for expected in baseline["objects"] + [baseline["noncurrent_version_preserved"]]:
        blob = bucket.blob(expected["name"], generation=int(expected["generation"]))
        blob.reload(retry=None, timeout=15)
        data = blob.download_as_bytes(if_generation_match=int(expected["generation"]), retry=None, timeout=15)
        observed = {"name": blob.name, "generation": str(blob.generation), "size": blob.size,
                    "sha256": hashlib.sha256(data).hexdigest()}
        assert observed == expected and len(data) == expected["size"]
        result["baseline_objects"].append(observed)
    current = list(client.list_blobs(run.BUCKET, prefix=run.ROOT, max_results=65, page_size=65, retry=None, timeout=15))
    expected_names = {item["name"] for item in baseline["objects"]}
    assert {item.name for item in current} == expected_names | ({soak.PULSE_KEY} if report else set())
    if report:
        blob = bucket.blob(soak.PULSE_KEY)
        blob.reload(retry=None, timeout=15)
        assert str(blob.generation) == report["pulse_generation"] and blob.size == 10
        data = blob.download_as_bytes(if_generation_match=blob.generation, retry=None, timeout=15)
        assert data == b"soak-pulse"
        result["pulse"] = {"name": blob.name, "generation": str(blob.generation), "size": blob.size,
                           "sha256": hashlib.sha256(data).hexdigest()}
    result["current_names_exact"] = True
sock = TSocket.TSocket("10.76.0.4", 9083)
sock.setTimeout(15000)
transport = TTransport.TBufferedTransport(sock)
try:
    transport.open()
    hive = ThriftHiveMetastore.Client(TBinaryProtocol.TBinaryProtocol(transport))
    database, table_name = run.APPROVED_RUN["table_id"].split(".")
    table = hive.get_table(database, table_name)
    assert table.parameters["gcp_probe_run_id"] == run.RUN and table.tableType == "EXTERNAL_TABLE"
    assert table.sd.location == "gs://" + run.BUCKET + "/" + run.ROOT.rstrip("/")
    values = sorted(partition.values[0] for partition in hive.get_partitions(database, table_name, 65))
    expected_values = sorted({item["name"][len(run.ROOT):].split("/")[0][3:]
                              for item in baseline["objects"] if item["name"].startswith(run.ROOT + "dt=")})
    assert values == expected_values and "soak" not in values
    try:
        hive.get_partition(database, table_name, ["soak"])
    except ttypes.NoSuchObjectException:
        pass
    else:
        raise AssertionError("soak_partition_remains")
    result["catalog_values"] = values
finally:
    transport.close()
result["status"] = "passed"
print(json.dumps(result, ensure_ascii=False))
