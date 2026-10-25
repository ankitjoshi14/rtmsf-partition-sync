"""Disposable fixture commands. Run only in the approved Azure ACI."""
from datetime import datetime, timezone
from io import BytesIO
import hashlib
import json
import logging
import sys
from uuid import uuid4

from azure.core.exceptions import HttpResponseError
from azure.identity import ManagedIdentityCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.storage.blob import BlobServiceClient
import pyarrow as pa
import pyarrow.parquet as pq

ACCOUNT = "rtmsfproof202610147405"
ROOT = "worker_probe/"
TOPIC = "/subscriptions/fe1b4721-0c1f-4e16-8d94-d132be2faf38/resourceGroups/rtmsf-azure-proof-20261014/providers/Microsoft.Storage/storageAccounts/" + ACCOUNT
logging.getLogger("azure").setLevel(logging.CRITICAL)


def report(**values):
    print(json.dumps({"utc": datetime.now(timezone.utc).isoformat(), **values}), flush=True)


def path(partition, name):
    assert partition in {"life", "burst", "recovery"}
    assert name in {"a", "b", "recover"} or name in {"f" + str(i) for i in range(12)}
    return ROOT + "dt=" + partition + "/" + name + ".parquet"


def put(container, partition, name, value, *, overwrite=False):
    key = path(partition, name)
    output = BytesIO()
    pq.write_table(pa.table({"id": pa.array([value], type=pa.int64())}), output)
    payload = output.getvalue()
    result = container.get_blob_client(key).upload_blob(payload, overwrite=overwrite, timeout=10)
    report(action="overwrite" if overwrite else "create", path=key, id=value,
           bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest(), etag=result["etag"])


def listing(container):
    files = []
    for blob in container.list_blobs(name_starts_with=ROOT, timeout=10):
        assert blob.name.startswith(ROOT) and len(files) < 200
        files.append({"path": blob.name, "bytes": blob.size})
    report(action="list", files=files)
    return files


def denied(call):
    try:
        call()
    except HttpResponseError as error:
        assert error.status_code == 403, "expected_authorization_denial"
        return
    raise AssertionError("outside_prefix_access_not_denied")


def main():
    stage = sys.argv[1]
    assert stage in {"check", "boundary", "list", "first", "retrigger-first", "second", "overwrite", "partial-delete",
                     "last-delete", "burst", "replay", "recovery", "recovery-delete", "cleanup"}
    with ManagedIdentityCredential(retry_total=0, connection_timeout=5,
                                   read_timeout=5, logging_enable=False) as credential:
        with BlobServiceClient(account_url="https://" + ACCOUNT + ".blob.core.windows.net",
                               credential=credential, retry_total=0,
                               connection_timeout=5, read_timeout=5, logging_enable=False) as blobs:
            container = blobs.get_container_client("proof")
            if stage in {"check", "boundary"}:
                denied(lambda: list(container.list_blobs(name_starts_with="partition_probe/", timeout=10)))
                denied(lambda: container.get_blob_client("partition_probe/").get_blob_properties(timeout=10))
                report(action="prefix_boundary", outside_list="403", outside_head="403")
                files = listing(container)
                if stage == "check":
                    assert not any(f["path"].endswith(".parquet") for f in files), "fixture_not_empty"
            elif stage == "list":
                listing(container)
            elif stage == "first":
                put(container, "life", "a", 1)
            elif stage == "retrigger-first":
                put(container, "life", "a", 1, overwrite=True)
            elif stage == "second":
                put(container, "life", "b", 2)
            elif stage == "overwrite":
                put(container, "life", "a", 11, overwrite=True)
            elif stage in {"partial-delete", "last-delete"}:
                key = path("life", "b" if stage == "partial-delete" else "a")
                container.delete_blob(key, timeout=10)
                report(action="delete", path=key)
            elif stage == "burst":
                for number in range(12):
                    put(container, "burst", "f" + str(number), 100 + number)
            elif stage == "recovery":
                put(container, "recovery", "recover", 900)
            elif stage == "recovery-delete":
                key = path("recovery", "recover")
                container.delete_blob(key, timeout=10)
                report(action="delete", path=key)
            elif stage == "cleanup":
                for item in listing(container):
                    key = item["path"]
                    if key.endswith(".parquet"):
                        assert key in {path("life", "a"), path("life", "b"), path("recovery", "recover")} | {
                            path("burst", "f" + str(i)) for i in range(12)}
                        container.delete_blob(key, timeout=10)
                        report(action="delete", path=key)
            elif stage == "replay":
                # Synthetic messages test current truth; they do not prove Event Grid delivery.
                with ServiceBusClient(fully_qualified_namespace="rtmsfproofsb202610157405.servicebus.windows.net",
                                      credential=credential, retry_total=0, try_timeout=10,
                                      logging_enable=False) as bus:
                    with bus.get_queue_sender(queue_name="object-events") as sender:
                        for partition, operation in [("burst", "DELETE"), ("burst", "CREATE"),
                                                     ("life", "CREATE"), ("life", "DELETE")]:
                            key = path(partition, "f0" if partition == "burst" else "a")
                            event_id = "rtmsf-synthetic-" + str(uuid4())
                            event = {"id": event_id, "topic": TOPIC,
                                     "subject": "/blobServices/default/containers/proof/blobs/" + key,
                                     "eventType": "Microsoft.Storage.BlobCreated" if operation == "CREATE" else "Microsoft.Storage.BlobDeleted",
                                     "eventTime": "2026-10-15T00:00:00Z", "dataVersion": "", "metadataVersion": "1",
                                     "data": {"api": "PutBlob" if operation == "CREATE" else "DeleteBlob",
                                              "blobType": "BlockBlob", "url": "https://" + ACCOUNT + ".blob.core.windows.net/proof/" + key}}
                            for duplicate in range(2):
                                message_id = event_id + "-" + str(duplicate)
                                sender.send_messages(ServiceBusMessage(json.dumps(event), message_id=message_id), timeout=10)
                                report(action="synthetic_replay", event_id=event_id, message_id=message_id,
                                       operation=operation, path=key, duplicate=duplicate)
            report(stage=stage, result="PASS")


try:
    main()
except Exception as error:
    report(result="FAIL", error_type=type(error).__name__, status=getattr(error, "status_code", None))
    sys.exit(1)
