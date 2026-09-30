from collections import defaultdict
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from io import StringIO
import hashlib
import json

from botocore.exceptions import ClientError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError
import pytest

from rtmsf.adapters import aws
from rtmsf.interfaces import FatalExternalError, RetryableExternalError


SOURCES = {"warehouse": "data-bucket", "archive": "archive-bucket"}


class Client:
    def __init__(self, **responses):
        self.responses = {name: list(values) for name, values in responses.items()}
        self.calls = defaultdict(list)

    def __getattr__(self, name):
        def call(**request):
            self.calls[name].append(request)
            responses = self.responses.get(name, [])
            response = responses.pop(0) if responses else {}
            if isinstance(response, Exception):
                raise response
            return response
        return call


class Telemetry:
    def __init__(self):
        self.records = []

    def emit(self, record):
        self.records.append(dict(record))


def failure(code, status=400):
    return ClientError({"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, "Test")


def record(*, key="root/dt=2026-09-10/a.parquet", bucket="data-bucket", name="ObjectCreated:Put", event_time="2026-09-10T10:00:00Z"):
    return {"eventSource": "aws:s3", "eventName": name, "eventTime": event_time, "s3": {"bucket": {"name": bucket}, "object": {"key": key}}}


def message(body, index=0):
    return {"MessageId": f"provider-id-{index}", "ReceiptHandle": f"private-receipt-{index}", "Body": body if isinstance(body, str) else json.dumps(body)}


def source(client, telemetry=None, **kwargs):
    return aws.SqsEventSource(client, "event-queue", "dead-queue", sources=SOURCES, telemetry=telemetry, **kwargs)


def receive(client, telemetry=None):
    adapter = source(client, telemetry)
    return adapter, adapter.receive(10, 20, 180)


def test_payload_budget_pauses_receives_and_acknowledgement_releases_capacity(monkeypatch):
    body = json.dumps({"Records": [record()]})
    body += ' ' * (aws.SQS_MAX_MESSAGE_BYTES - len(body))
    client = Client(receive_message=[{"Messages": [message(body)]},
                                     {"Messages": [message({"Records": [record()]}, 1)]}])
    waits = []
    monkeypatch.setattr(aws.time, 'sleep', waits.append)
    adapter = source(client, max_buffer_bytes=aws.SQS_MAX_MESSAGE_BYTES)
    first = adapter.receive(10, 0, 180)
    assert client.calls['receive_message'][0]['MaxNumberOfMessages'] == 1
    assert adapter._pending_body_bytes == aws.SQS_MAX_MESSAGE_BYTES
    assert adapter.receive(10, 3, 180) == ()
    assert waits == [3] and len(client.calls['receive_message']) == 1
    assert not client.calls['delete_message_batch']
    adapter.acknowledge(first)
    assert adapter._pending_body_bytes == 0
    assert len(adapter.receive(10, 0, 180)) == 1


def test_failed_receive_releases_local_byte_accounting_without_acknowledging_input():
    client = Client(receive_message=[{'Messages': [message({'Records': [record()]}), message('invalid', 1)]}],
                    send_message_batch=[failure('ServiceUnavailable', 503)])
    adapter = source(client)
    with pytest.raises(RetryableExternalError):
        adapter.receive(10, 0, 180)
    assert adapter._pending_body_bytes == 0 and not adapter._messages
    assert not client.calls['delete_message_batch']


def test_receive_preserves_once_decoded_key_and_emits_only_canonical_fields():
    telemetry = Telemetry()
    client = Client(receive_message=[{"Messages": [message({"Records": [record(key="%2Froot//dt%3D2026-09-10/a%2Bb+%252F.parquet")]})]}])
    adapter, deliveries = receive(client, telemetry)
    delivery, = deliveries
    event, = delivery.events
    assert [field.name for field in fields(delivery)] == ["handle", "events"]
    assert delivery.handle != "private-receipt-0"
    assert event.source_id == "warehouse"
    assert event.object_key == "/root//dt=2026-09-10/a+b %2F.parquet"
    assert event.operation == "CREATE"
    assert event.ingested_at.utcoffset() == timedelta(0)
    assert event.source_event_time == datetime(2026, 9, 10, 10, tzinfo=timezone.utc)
    assert client.calls["receive_message"][0]["VisibilityTimeout"] == 180
    decoded, = [item for item in telemetry.records if item.get("action") == "message_decoded"]
    assert decoded["message_id"] == "provider-id-0"
    assert decoded["events"] == [{
        "source_id": "warehouse",
        "key_sha256": hashlib.sha256(event.object_key.encode()).hexdigest(),
        "operation": "CREATE",
    }]
    emitted = json.dumps(telemetry.records)
    assert event.object_key not in emitted
    assert "private-receipt" not in emitted


@pytest.mark.parametrize("event_time", [None, "not-a-time", "2026-09-10T10:00:00", 123, ""])
def test_optional_invalid_source_time_does_not_discard_valid_object_event(event_time):
    client = Client(receive_message=[{"Messages": [message({"Records": [record(event_time=event_time)]})]}])
    _, deliveries = receive(client)
    assert deliveries[0].events[0].source_event_time is None
    assert not client.calls["send_message_batch"]


def test_missing_source_time_and_delete_are_normalized():
    item = record(name="ObjectRemoved:Delete", bucket="archive-bucket")
    del item["eventTime"]
    client = Client(receive_message=[{"Messages": [message({"Records": [item]})]}])
    _, deliveries = receive(client)
    event = deliveries[0].events[0]
    assert (event.source_id, event.operation, event.source_event_time) == ("archive", "DELETE", None)


@pytest.mark.parametrize("body", ["broken-json", {}, {"Records": "bad"}, {"Records": [None]}, {"Records": [record(key="")]}, {"Records": [record(bucket="unconfigured-bucket")]}])
def test_malformed_messages_are_durably_dead_lettered_and_observable(body):
    telemetry = Telemetry()
    client = Client(receive_message=[{"Messages": [message(body)]}])
    _, deliveries = receive(client, telemetry)
    assert deliveries == ()
    sent = client.calls["send_message_batch"][0]
    assert sent["QueueUrl"] == "dead-queue"
    assert sent["Entries"][0]["MessageBody"] == message(body)["Body"]
    assert client.calls["delete_message_batch"]
    assert any(item.get("action") == "malformed_delivery" for item in telemetry.records)
    assert "private-receipt" not in json.dumps(telemetry.records)
    assert "original_body" not in json.dumps(telemetry.records)


def test_failed_malformed_dead_letter_never_acknowledges():
    client = Client(receive_message=[{"Messages": [message("invalid")]}], send_message_batch=[{"Failed": [{"Id": "0", "Code": "ServiceUnavailable", "SenderFault": False}]}])
    with pytest.raises(RetryableExternalError):
        receive(client)
    assert not client.calls["delete_message_batch"]


def test_ten_malformed_messages_fit_one_poll_and_two_terminal_batches():
    client = Client(receive_message=[{"Messages": [message("invalid", index) for index in range(10)]}])
    _, deliveries = receive(client)
    assert deliveries == ()
    assert {name: len(calls) for name, calls in client.calls.items()} == {"receive_message": 1, "send_message_batch": 1, "delete_message_batch": 1}


def test_full_size_dlq_body_is_preserved_without_json_wrapper_expansion():
    body = '"' * 1_048_576
    client = Client(receive_message=[{"Messages": [message(body)]}])
    _, deliveries = receive(client)
    assert deliveries == ()
    entry = client.calls["send_message_batch"][0]["Entries"][0]
    assert entry == {"Id": "0", "MessageBody": body}
    assert client.calls["delete_message_batch"]


def test_dead_letter_batches_split_at_utf8_byte_limit():
    body = "\U0001f600" * 100_000  # 400 KB per body, despite only 100,000 code points.
    client = Client(receive_message=[{"Messages": [message(body, index) for index in range(3)]}])
    receive(client)
    batches = client.calls["send_message_batch"]
    assert [len(batch["Entries"]) for batch in batches] == [2, 1]
    assert all(sum(len(entry["MessageBody"].encode("utf-8")) for entry in batch["Entries"]) <= 1_048_576 for batch in batches)
    assert all(entry["MessageBody"] == body for batch in batches for entry in batch["Entries"])


def test_deeply_nested_json_is_durably_dead_lettered():
    depth = 10_000
    body = "[" * depth + "0" + "]" * depth
    client = Client(receive_message=[{"Messages": [message(body)]}])
    _, deliveries = receive(client)
    assert deliveries == ()
    assert client.calls["send_message_batch"][0]["Entries"][0]["MessageBody"] == body
    assert client.calls["delete_message_batch"]


def test_partial_ack_retry_omits_already_deleted_receipts():
    telemetry = Telemetry()
    client = Client(receive_message=[{"Messages": [message({"Records": [record()]}, index) for index in range(2)]}], delete_message_batch=[{"Successful": [{"Id": "0"}], "Failed": [{"Id": "1", "Code": "InternalError", "SenderFault": False}]}, {}])
    adapter, deliveries = receive(client, telemetry)
    with pytest.raises(RetryableExternalError):
        adapter.acknowledge(deliveries)
    assert [item["message_id"] for item in telemetry.records if item.get("action") == "message_acknowledged"] == ["provider-id-0"]
    adapter.acknowledge(deliveries)
    assert client.calls["delete_message_batch"][1]["Entries"] == [{"Id": "0", "ReceiptHandle": "private-receipt-1"}]
    assert [item["message_id"] for item in telemetry.records if item.get("action") == "message_acknowledged"] == ["provider-id-0", "provider-id-1"]
    assert [item["message_id"] for item in telemetry.records if item.get("action") == "message_received"] == ["provider-id-0", "provider-id-1"]
    assert "private-receipt" not in json.dumps(telemetry.records)


def test_partial_dead_letter_retry_sends_only_undelivered_messages():
    client = Client(receive_message=[{"Messages": [message({"Records": [record()]}, index) for index in range(2)]}], send_message_batch=[{"Successful": [{"Id": "0"}], "Failed": [{"Id": "1", "Code": "InternalError", "SenderFault": False}]}, {}])
    adapter, deliveries = receive(client)
    with pytest.raises(RetryableExternalError):
        adapter.dead_letter(deliveries, "terminal")
    adapter.dead_letter(deliveries, "terminal")
    assert len(client.calls["send_message_batch"][1]["Entries"]) == 1
    assert not client.calls["delete_message_batch"]


def test_failed_api_attempts_include_request_id_without_provider_error_message():
    error = ClientError({"Error": {"Code": "InternalError", "Message": "private-receipt-secret"}, "ResponseMetadata": {"HTTPStatusCode": 500, "RequestId": "request-1", "RetryAttempts": 2}}, "Test")
    telemetry = Telemetry()
    with pytest.raises(RetryableExternalError, match="^InternalError$"):
        aws.S3ObjectStore(Client(head_object=[error]), sources=SOURCES, telemetry=telemetry).head_exists("warehouse", "a")
    record_value = telemetry.records[-1]
    assert record_value["request_id"] == "request-1"
    assert record_value["attempts"] == 3
    assert "private-receipt-secret" not in json.dumps(record_value)


@pytest.mark.parametrize("body", [{"Event": "s3:TestEvent"}, {"Records": [record(name="ObjectRestore:Completed")]}])
def test_test_and_unsupported_events_are_ignorable_deliveries(body):
    client = Client(receive_message=[{"Messages": [message(body)]}])
    _, deliveries = receive(client)
    assert len(deliveries) == 1
    assert deliveries[0].events == ()
    assert not client.calls["send_message_batch"]


def test_source_batches_more_than_ten_and_keeps_dlq_separate_from_ack():
    client = Client(receive_message=[{"Messages": [message({"Records": [record()]}, index) for index in range(start, min(start + 10, 23))]} for start in range(0, 23, 10)])
    adapter = source(client)
    deliveries = tuple(item for _ in range(3) for item in adapter.receive(10, 0, 180))
    adapter.renew(deliveries, 240)
    adapter.dead_letter(deliveries, "terminal_failure")
    assert not client.calls["delete_message_batch"]
    adapter.acknowledge(deliveries)
    for name in ("change_message_visibility_batch", "send_message_batch", "delete_message_batch"):
        assert [len(call["Entries"]) for call in client.calls[name]] == [10, 10, 3]
        assert all(len({entry["Id"] for entry in call["Entries"]}) == len(call["Entries"]) for call in client.calls[name])
    assert client.calls["change_message_visibility_batch"][0]["Entries"][0]["VisibilityTimeout"] == 240


@pytest.mark.parametrize("method,sdk_method,args", [("acknowledge", "delete_message_batch", ()), ("renew", "change_message_visibility_batch", (180,)), ("dead_letter", "send_message_batch", ("terminal",))])
def test_source_partial_failures_are_not_silent(method, sdk_method, args):
    telemetry = Telemetry()
    client = Client(receive_message=[{"Messages": [message({"Records": [record()]})]}], **{sdk_method: [{"Failed": [{"Id": "0", "Code": "InternalError", "SenderFault": False}]}]})
    adapter, deliveries = receive(client, telemetry)
    with pytest.raises(RetryableExternalError):
        getattr(adapter, method)(deliveries, *args)
    assert telemetry.records[-1]["status"] == "error"
    assert telemetry.records[-1]["failed_count"] == 1


@pytest.mark.parametrize("error", [failure("SlowDown", 503), ConnectTimeoutError(endpoint_url="https://aws.test"), ReadTimeoutError(endpoint_url="https://aws.test"), EndpointConnectionError(endpoint_url="https://aws.test")])
def test_network_failures_are_provider_neutral_retryable_errors(error):
    telemetry = Telemetry()
    store = aws.S3ObjectStore(Client(head_object=[error]), sources=SOURCES, telemetry=telemetry)
    with pytest.raises(RetryableExternalError):
        store.head_exists("warehouse", "root/private.parquet")
    api = telemetry.records[-1]
    assert api["status"] == "error"
    assert api["attempts"] == 1
    assert datetime.fromisoformat(api["timestamp"]).utcoffset() == timedelta(0)
    assert api["duration_ms"] >= 0
    assert "private.parquet" not in json.dumps(api)


def test_storage_maps_sources_and_preserves_keys_and_prefixes():
    client = Client(head_object=[failure("404", 404)], list_objects_v2=[{"KeyCount": 1}])
    store = aws.S3ObjectStore(client, sources=SOURCES)
    assert not store.head_exists("warehouse", "/a//b%20.parquet")
    assert store.prefix_non_empty("archive", "root/dt=x/")
    assert store.location_uri("warehouse", "root/dt=x/") == "s3://data-bucket/root/dt=x/"
    assert client.calls["head_object"] == [{"Bucket": "data-bucket", "Key": "/a//b%20.parquet"}]
    assert client.calls["list_objects_v2"] == [{"Bucket": "archive-bucket", "Prefix": "root/dt=x/", "MaxKeys": 1}]
    with pytest.raises(FatalExternalError):
        store.head_exists("unknown", "a")


def table(location="s3://data-bucket/root/", column="dt"):
    return {"Table": {"StorageDescriptor": {"Location": location, "Columns": [{"Name": "value", "Type": "int"}], "SerdeInfo": {"SerializationLibrary": "parquet-serde"}}, "PartitionKeys": [{"Name": column, "Type": "string"}]}}


def partition(table_id="db.one", source_id="warehouse", value="2026-09-10"):
    from rtmsf.models import PartitionRef
    return PartitionRef(source_id, table_id, f"dt={value}", (value,), f"root/dt={value}/")


def test_glue_resolves_tables_once_and_dispatches_partition_identity():
    client = Client(get_table=[table(), table("s3://archive-bucket/archive/", "site")])
    telemetry = Telemetry()
    catalog = aws.GluePartitionCatalog(client, sources=SOURCES, telemetry=telemetry)
    definitions = catalog.resolve_tables(("db.one", "db.two"))
    assert [(item.table_id, item.source_id, item.root_prefix, item.partition_columns) for item in definitions] == [("db.one", "warehouse", "root/", ("dt",)), ("db.two", "archive", "archive/", ("site",))]
    assert catalog.resolve_tables(("db.one", "db.two")) == definitions
    target = partition("db.two", "archive", "west")
    assert catalog.partition_exists(target)
    assert catalog.create_partition(target, "s3://archive-bucket/archive/site=west/")
    assert catalog.delete_partition(target)
    assert len(client.calls["get_table"]) == 2
    for name in ("get_partition", "create_partition", "delete_partition"):
        assert client.calls[name][0]["DatabaseName"] == "db"
        assert client.calls[name][0]["TableName"] == "two"
    created = client.calls["create_partition"][0]["PartitionInput"]
    assert created["Values"] == ["west"]
    assert created["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"] == "parquet-serde"
    assert created["StorageDescriptor"]["Location"] == "s3://archive-bucket/archive/site=west/"
    assert [item["phase"] for item in telemetry.records] == ["startup", "startup", "maintenance", "maintenance", "maintenance"]


def test_glue_partition_idempotence_and_descriptor_copy():
    original = table()
    client = Client(get_table=[original], get_partition=[failure("EntityNotFoundException")], create_partition=[failure("AlreadyExistsException")], delete_partition=[failure("EntityNotFoundException")])
    catalog = aws.GluePartitionCatalog(client, sources=SOURCES)
    catalog.resolve_tables(("db.one",))
    assert not catalog.partition_exists(partition())
    assert not catalog.create_partition(partition(), "s3://data-bucket/root/dt=x/")
    assert not catalog.delete_partition(partition())
    assert original["Table"]["StorageDescriptor"]["Location"] == "s3://data-bucket/root/"


@pytest.mark.parametrize("definition", [table("file:///local/"), table("s3://unknown/root/"), {"Table": {}}, table("s3://data-bucket/root/?query=bad")])
def test_invalid_existing_glue_tables_fail_at_startup(definition):
    catalog = aws.GluePartitionCatalog(Client(get_table=[definition]), sources=SOURCES)
    with pytest.raises(FatalExternalError):
        catalog.resolve_tables(("db.one",))


def test_unresolved_or_mismatched_table_is_never_mutated():
    client = Client(get_table=[table()])
    catalog = aws.GluePartitionCatalog(client, sources=SOURCES)
    catalog.resolve_tables(("db.one",))
    with pytest.raises(FatalExternalError):
        catalog.delete_partition(partition("db.unresolved"))
    with pytest.raises(FatalExternalError):
        catalog.create_partition(partition(source_id="archive"), "s3://archive-bucket/root/dt=x/")
    assert not client.calls["delete_partition"]
    assert not client.calls["create_partition"]


def test_json_telemetry_has_utc_time_and_flushes_without_mutating_record():
    output = StringIO()
    sink = aws.JsonTelemetry(output)
    record_value = {"component": "worker", "action": "buffered"}
    sink.emit(record_value)
    emitted = json.loads(output.getvalue())
    assert datetime.fromisoformat(emitted["timestamp"]).utcoffset() == timedelta(0)
    assert record_value == {"component": "worker", "action": "buffered"}
