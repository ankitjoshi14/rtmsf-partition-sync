import io
import json
from datetime import datetime, timezone
from types import SimpleNamespace as N

import pytest

oci = pytest.importorskip("oci")
pytest.importorskip("fdk")

from rtmsf.adapters.oci import OciObjectStore, OciQueueEventSource, decode_event, enqueue_event
from rtmsf.oci_function import handler
from rtmsf.interfaces import FatalExternalError, RetryableExternalError
from rtmsf.models import TableDefinition
from rtmsf.pmcp import PartitionMutationCommitProtocol
from rtmsf.routing import RoutingIndex
from rtmsf.worker import RTMSFWorker


SOURCES = {"demo": ("ns", "bucket")}


def event(kind="createobject", key="table/dt=a/file+%20.parquet", namespace="ns"):
    return json.dumps({
        "source": "objectstorage",
        "eventType": "com.oraclecloud.objectstorage." + kind,
        "eventTime": "2026-10-12T10:00:00Z", "eventID": "test-event",
        "data": {"resourceName": key, "additionalDetails": {
            "namespace": namespace, "bucketName": "bucket"}},
    })


def response(data=None):
    return N(data=data)


def error(status, code):
    return oci.exceptions.ServiceError(status, code, {}, "not logged")


class Queue:
    def __init__(self, bodies=()):
        self.messages = [oci.queue.models.GetMessage(
            id=i, receipt=f"receipt-{i}", content=b) for i, b in enumerate(bodies)]
        self.actions = []
        self.fail_put = self.fail_delete = self.fail_update = False

    def get_messages(self, queue_id, **kw):
        self.actions.append(("receive", kw))
        messages, self.messages = self.messages[:kw["limit"]], self.messages[kw["limit"]:]
        return response(oci.queue.models.GetMessages(messages=messages))

    def put_messages(self, queue_id, details, **kw):
        if self.fail_put:
            raise error(503, "ServiceUnavailable")
        self.actions.append(("put", queue_id, details.messages[0].content))
        return response(oci.queue.models.PutMessages(messages=[oci.queue.models.PutMessage(id=9)]))

    def delete_messages(self, queue_id, details, **kw):
        self.actions.append(("delete", [e.receipt for e in details.entries]))
        errors = [oci.queue.models.DeleteMessagesResultEntry(error_code=500 if self.fail_delete else None)
                  for _ in details.entries]
        return response(oci.queue.models.DeleteMessagesResult(
            entries=errors, server_failures=int(self.fail_delete), client_failures=0))

    def update_messages(self, queue_id, details, **kw):
        self.actions.append(("renew", [e.receipt for e in details.entries]))
        errors = [oci.queue.models.UpdateMessagesResultEntry(error_code=500 if self.fail_update else None)
                  for _ in details.entries]
        return response(oci.queue.models.UpdateMessagesResult(
            entries=errors, server_failures=int(self.fail_update), client_failures=0))


@pytest.mark.parametrize("kind,operation", [("createobject", "CREATE"), ("updateobject", "CREATE"), ("deleteobject", "DELETE")])
def test_exact_oci_key_and_operation(kind, operation):
    item, = decode_event(event(kind), SOURCES)
    assert item.object_key == "table/dt=a/file+%20.parquet"
    assert item.source_id == "demo" and item.operation == operation
    assert item.source_event_time == datetime(2026, 10, 12, 10, tzinfo=timezone.utc)


def test_same_bucket_other_namespace_rejected():
    with pytest.raises(ValueError):
        decode_event(event(namespace="other"), SOURCES)


def test_live_oci_source_capitalization():
    body = json.loads(event()); body["source"] = "ObjectStorage"
    item, = decode_event(json.dumps(body), SOURCES)
    assert item.source_id == "demo"


@pytest.mark.parametrize("body", ["[]", "{}", "invalid", event("unknown")])
def test_malformed_copied_before_deletion(body):
    queue = Queue([body])
    source = OciQueueEventSource(queue, "main", queue, "quarantine", SOURCES)
    assert source.receive(1, 0, 120) == ()
    assert [a[0] for a in queue.actions] == ["receive", "put", "delete"]


def test_failed_quarantine_never_deletes_original():
    queue = Queue(["invalid"]); queue.fail_put = True
    source = OciQueueEventSource(queue, "main", queue, "quarantine", SOURCES)
    with pytest.raises(RetryableExternalError):
        source.receive(1, 0, 120)
    assert not any(a[0] == "delete" for a in queue.actions)


def test_receive_retains_receipt_and_renew_is_separate():
    queue = Queue([event()])
    source = OciQueueEventSource(queue, "main", queue, "quarantine", SOURCES)
    deliveries = source.receive(1, 0, 120)
    assert deliveries[0].handle != "receipt-0"
    assert not any(a[0] == "delete" for a in queue.actions)
    source.renew(deliveries, 120)
    source.acknowledge(deliveries)
    assert [a[0] for a in queue.actions] == ["receive", "renew", "delete"]


def test_failed_batch_delete_is_not_completed():
    queue = Queue([event()]); queue.fail_delete = True
    source = OciQueueEventSource(queue, "main", queue, "quarantine", SOURCES)
    deliveries = source.receive(1, 0, 120)
    with pytest.raises(RetryableExternalError):
        source.acknowledge(deliveries)
    queue.fail_delete = False
    source.acknowledge(deliveries)
    assert sum(a[0] == "delete" for a in queue.actions) == 2


def test_failed_renewal_stops_processing():
    queue = Queue([event()]); queue.fail_update = True
    source = OciQueueEventSource(queue, "main", queue, "quarantine", SOURCES)
    deliveries = source.receive(1, 0, 120)
    with pytest.raises(RetryableExternalError):
        source.renew(deliveries, 120)
    assert not any(a[0] == "delete" for a in queue.actions)


def test_bridge_returns_only_after_durable_queue_response():
    queue = Queue()
    enqueue_event(queue, "main", event())
    assert queue.actions[0][0] == "put"
    queue.fail_put = True
    with pytest.raises(RetryableExternalError):
        enqueue_event(queue, "main", event())


def test_function_success_and_queue_failure(monkeypatch):
    queue = Queue()
    monkeypatch.setenv("RTMSF_QUEUE_ENDPOINT", "https://queue.example.invalid")
    monkeypatch.setenv("RTMSF_QUEUE_ID", "main")
    monkeypatch.setattr(oci.auth.signers, "get_resource_principals_signer", lambda: object())
    monkeypatch.setattr(oci.queue, "QueueClient", lambda *a, **kw: queue)
    ctx = N(SetResponseHeaders=lambda *args: None)
    result = handler(ctx, io.BytesIO(event().encode()))
    assert result.status_code == 200
    queue.fail_put = True
    with pytest.raises(RetryableExternalError):
        handler(ctx, io.BytesIO(event().encode()))


def test_missing_bridge_confirmation_fails():
    queue = N(put_messages=lambda *a, **kw: response(oci.queue.models.PutMessages(messages=[])))
    with pytest.raises(RetryableExternalError):
        enqueue_event(queue, "main", event())


class Storage:
    def __init__(self):
        self.keys = set(); self.failure = None

    def head_object(self, namespace, bucket, key, **kw):
        if self.failure: raise self.failure
        if key not in self.keys: raise error(404, "ObjectNotFound")
        return response()

    def list_objects(self, namespace, bucket, **kw):
        if self.failure: raise self.failure
        return response(oci.object_storage.models.ListObjects(objects=[
            oci.object_storage.models.ObjectSummary(name=k) for k in sorted(self.keys)
            if k.startswith(kw["prefix"])][:1]))


def test_storage_checks_and_real_oci_location():
    client = Storage(); store = OciObjectStore(client, SOURCES)
    assert not store.head_exists("demo", "table/dt=a/f")
    client.keys.add("table/dt=a/f")
    assert store.head_exists("demo", "table/dt=a/f")
    assert store.prefix_non_empty("demo", "table/dt=a/")
    assert not store.prefix_non_empty("demo", "table/dt=b/")
    assert store.location_uri("demo", "table/dt=a/") == "oci://bucket@ns/table/dt=a/"


def test_empty_prefix_response_must_be_complete():
    for data in [oci.object_storage.models.ListObjects(objects=None),
                 oci.object_storage.models.ListObjects(objects=[], next_start_with="more")]:
        client = N(list_objects=lambda *a, **kw: response(data))
        with pytest.raises(RetryableExternalError):
            OciObjectStore(client, SOURCES).prefix_non_empty("demo", "table/")


def test_duplicate_receipt_response_is_rejected_without_ack():
    queue = Queue([event(), event()]); queue.messages[1].receipt = queue.messages[0].receipt
    source = OciQueueEventSource(queue, "main", queue, "quarantine", SOURCES)
    with pytest.raises(FatalExternalError): source.receive(2, 0, 120)
    assert not any(a[0] == "delete" for a in queue.actions)


@pytest.mark.parametrize("status,code,expected", [
    (403, "NotAuthorized", FatalExternalError),
    (404, "NotAuthorizedOrNotFound", FatalExternalError),
    (503, "ServiceUnavailable", RetryableExternalError)])
def test_storage_error_never_claims_absence(status, code, expected):
    client = Storage(); client.failure = error(status, code)
    store = OciObjectStore(client, SOURCES)
    with pytest.raises(expected): store.head_exists("demo", "key")
    with pytest.raises(expected): store.prefix_non_empty("demo", "table/")


def test_transport_failure_is_retryable():
    client = Storage(); client.failure = oci.exceptions.RequestException("transport failed")
    store = OciObjectStore(client, SOURCES)
    with pytest.raises(RetryableExternalError): store.head_exists("demo", "key")
    with pytest.raises(RetryableExternalError): store.prefix_non_empty("demo", "table/")


class CatalogDouble:
    def __init__(self): self.locations = {}
    def partition_exists(self, p): return p.key in self.locations
    def create_partition(self, p, location): self.locations[p.key] = location; return True
    def delete_partition(self, p): return self.locations.pop(p.key, None) is not None


def test_actual_worker_coalesces_before_ack_and_preserves_live_partition():
    queue = Queue([event(key="table/dt=a/f1"), event(key="table/dt=a/f2")])
    source = OciQueueEventSource(queue, "main", queue, "quarantine", SOURCES)
    storage = Storage(); storage.keys = {"table/dt=a/f1", "table/dt=a/f2"}
    catalog = CatalogDouble(); records=[]; now=[0]
    worker = RTMSFWorker(
        source=source, pmcp=PartitionMutationCommitProtocol(OciObjectStore(storage, SOURCES), catalog),
        telemetry=N(emit=records.append), routing=RoutingIndex([TableDefinition("demo.table", "demo", "table/", ("dt",))]),
        settling_ms=100, maximum_ms=500, queue_lease_seconds=120,
        pmcp_budget_ms=10_000, source_call_budget_ms=10_000,
        clock_ms=lambda: now[0], sleep_ms=lambda delay: now.__setitem__(0, now[0]+delay),
        poll_wait_seconds=0, max_pending_deliveries=20,
    )
    worker.run_once()
    assert not catalog.locations and not any(a[0] == "delete" for a in queue.actions)
    now[0]=100; worker.run_once()
    assert list(catalog.locations.values()) == ["oci://bucket@ns/table/dt=a/"]
    assert sum(r.get("action") == "pmcp_result" for r in records) == 1
    assert sum(a[0] == "delete" for a in queue.actions) == 2
    storage.keys.remove("table/dt=a/f1")
    queue.messages = [oci.queue.models.GetMessage(id=5, receipt="delete-partial", content=event("deleteobject", "table/dt=a/f1"))]
    worker.run_once(); now[0]=200; worker.run_once()
    assert len(catalog.locations) == 1
    storage.keys.clear()
    queue.messages = [oci.queue.models.GetMessage(id=6, receipt="delete-last", content=event("deleteobject", "table/dt=a/f2"))]
    worker.run_once(); now[0]=300; worker.run_once()
    assert not catalog.locations
