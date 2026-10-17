"""Read-only proof witness; run in Azure separately from the worker."""
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from azure.servicebus import ServiceBusClient
from rtmsf.azure_app import AppConfig
from rtmsf.adapters.databricks import DatabricksSQLStatements, _rows, _json

logging.getLogger("azure").setLevel(logging.CRITICAL)
stage = sys.argv[1]
config = AppConfig.from_environment(json.loads(Path("/etc/rtmsf/proof-witness-config.json").read_text()))
table = ".".join("`" + part + "`" for part in config.table_id.split("."))
life_a = "worker_probe/dt=life/a.parquet"
life_b = "worker_probe/dt=life/b.parquet"
burst = {"worker_probe/dt=burst/f" + str(i) + ".parquet": ("burst", 100+i) for i in range(12)}
expected = {
    "baseline": ({}, []), "queued-first": ({life_a: ("life", 1)}, []),
    "first": ({life_a: ("life", 1)}, [("life", 1)]),
    "second": ({life_a: ("life", 1), life_b: ("life", 2)}, [("life", 1), ("life", 2)]),
    "overwrite": ({life_a: ("life", 11), life_b: ("life", 2)}, [("life", 2), ("life", 11)]),
    "partial-delete": ({life_a: ("life", 11)}, [("life", 11)]),
    "last-delete": ({}, []),
    "burst": (burst, [("burst", 100+i) for i in range(12)]),
    "replay": (burst, [("burst", 100+i) for i in range(12)]),
    "recovery": ({**burst, "worker_probe/dt=recovery/recover.parquet": ("recovery", 900)},
                 [("burst", 100+i) for i in range(12)] + [("recovery", 900)]),
    "recovery-queued": ({**burst, "worker_probe/dt=recovery/recover.parquet": ("recovery", 900)},
                        [("burst", 100+i) for i in range(12)]),
    "recovery-delete": (burst, [("burst", 100+i) for i in range(12)]),
    "recovery-delete-pending": (burst, [("burst", 100+i) for i in range(12)]),
    "cleanup": ({}, []),
}
assert stage in expected
observed = {"stage": stage, "utc": datetime.now(timezone.utc).isoformat()}

try:
    with ManagedIdentityCredential(retry_total=0, connection_timeout=5, read_timeout=5, logging_enable=False) as credential:
        with SecretClient(vault_url=config.key_vault_url, credential=credential, retry_total=0,
                          connection_timeout=5, read_timeout=5, logging_enable=False) as vault:
            secret = vault.get_secret("databricks-client-secret").value
        sql = DatabricksSQLStatements(config.databricks_host, config.databricks_warehouse_id,
                                     config.databricks_client_id, secret,
                                     journal_path=Path("/var/lib/rtmsf/live-witness-pending.json"),
                                     operation_seconds=110, max_polls=180, max_requests=200)
        with sql.operation():
            metadata = _json(_rows(sql.execute("DESCRIBE TABLE EXTENDED " + table + " AS JSON"))[0][0])
            assert metadata["type"] == "EXTERNAL" and metadata["provider"] == "parquet"
            assert metadata["location"] in (config.root_location, config.root_location[:-1])
            assert metadata["partition_columns"] == ["dt"]
            assert metadata["table_properties"]["partitionMetadataEnabled"] == "true"
            observed["owner"] = metadata["owner"]
            document = sql.execute("SHOW PARTITIONS " + table)
            manifest = document["manifest"]
            assert manifest["truncated"] is False and manifest["total_row_count"] <= 20
            assert manifest["schema"]["column_count"] == 1
            result = document.get("result", {})
            assert not any(result.get(k) is not None for k in ("next_chunk_index", "next_chunk_internal_link", "external_links"))
            partitions = result.get("data_array", [])
            assert len(partitions) == manifest["total_row_count"]
            observed["partitions"] = sorted(row[0] for row in partitions)
            # A unique literal gives this independent read a distinct SQL result-cache key.
            query = "SELECT to_json(named_struct('witness','" + str(uuid4()) + "','rows',sort_array(collect_list(named_struct('dt',dt,'id',id))))) FROM " + table
            rows = _json(_rows(sql.execute(query))[0][0])["rows"]
            observed["rows"] = rows
        with BlobServiceClient(account_url="https://rtmsfproof202610147405.blob.core.windows.net",
                               credential=credential, retry_total=0, connection_timeout=5,
                               read_timeout=5, logging_enable=False) as blobs:
            files = []
            for blob in blobs.get_container_client("proof").list_blobs(name_starts_with=config.root_prefix, timeout=10):
                assert blob.name.startswith(config.root_prefix) and len(files) < 200
                files.append({"path": blob.name, "bytes": blob.size})
            observed["storage"] = files
        with ServiceBusClient(fully_qualified_namespace=config.service_bus_namespace, credential=credential,
                              retry_total=0, try_timeout=10, logging_enable=False) as bus:
            with bus.get_queue_receiver(queue_name=config.service_bus_queue, prefetch_count=0, socket_timeout=5) as receiver:
                messages = []
                for message in receiver.peek_messages(max_message_count=3, timeout=10):
                    event = json.loads(b"".join(message.body))
                    messages.append({"message_id": str(message.message_id), "sequence_number": message.sequence_number,
                                     "delivery_count": message.delivery_count, "event_id": event.get("id"),
                                     "topic": event.get("topic"), "subject": event.get("subject"),
                                     "event_type": event.get("eventType"), "api": event.get("data", {}).get("api")})
                observed["queue_peek"] = messages
        observed["service"] = subprocess.run(["systemctl", "show", "rtmsf.service", "--property=MainPID,ActiveState,ExecMainStartTimestamp,ExecMainStatus"], capture_output=True, text=True, check=True).stdout.splitlines()
        telemetry = []
        for line in subprocess.run(["journalctl", "-u", "rtmsf.service", "-n", "200", "-o", "json", "--no-pager"], capture_output=True, text=True, check=True).stdout.splitlines():
            outer = json.loads(line)
            try:
                record = json.loads(outer.get("MESSAGE", ""))
            except ValueError:
                continue
            if record.get("action") == "pmcp_result":
                telemetry.append({"utc_microseconds": outer["__REALTIME_TIMESTAMP"], **record})
        observed["pmcp"] = telemetry[-8:]
        if stage not in {"queued-first", "recovery-queued", "recovery-delete-pending"}:
            assert not messages, "pending_queue_input"
        actual_files = {item["path"] for item in files if item["path"].endswith(".parquet")}
        actual_rows = sorted((row["dt"], row["id"]) for row in rows)
        expected_files, expected_rows = expected[stage]
        assert actual_files == set(expected_files), "physical_file_mismatch"
        assert actual_rows == sorted(expected_rows), "catalog_rows_mismatch"
        assert observed["partitions"] == sorted({"dt=" + dt for dt, _ in expected_rows}), "partition_mismatch"
        observed["result"] = "PASS"
except Exception as error:
    observed.update(result="FAIL", error_type=type(error).__name__)
print(json.dumps(observed), flush=True)
sys.exit(0 if observed["result"] == "PASS" else 1)
