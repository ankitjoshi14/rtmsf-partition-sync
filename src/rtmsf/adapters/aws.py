"""AWS translations for the provider-neutral RTMSF interfaces."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import sys
import time
from typing import Callable, IO, Mapping, Sequence
from urllib.parse import unquote_plus, urlsplit
from uuid import uuid4

from botocore.exceptions import ClientError, ConnectionError, HTTPClientError

from rtmsf.models import ObjectEvent, PartitionRef, TableDefinition
from rtmsf.interfaces import Delivery, EventSource, FatalExternalError, ObjectStore, PartitionCatalog, RetryableExternalError, TelemetrySink


RETRYABLE_CODES = {
    "InternalError", "InternalFailure", "InternalServiceException",
    "OperationTimeoutException", "RequestTimeout", "RequestTimeoutException",
    "ServiceUnavailable", "ServiceUnavailableException", "SlowDown",
    "Throttling", "ThrottlingException", "TooManyRequestsException",
    "RequestLimitExceeded", "ConditionalRequestConflict",
}
SQS_MAX_MESSAGE_BYTES = 1_048_576


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", "Unknown"))


def _raise_external(error: Exception) -> None:
    if isinstance(error, ClientError):
        code = _error_code(error)
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        retryable = code in RETRYABLE_CODES or code.startswith("Throttl") or int(status) >= 500
    else:
        code = type(error).__name__
        retryable = isinstance(error, (ConnectionError, HTTPClientError))
    if retryable:
        raise RetryableExternalError(code) from None
    raise FatalExternalError(code) from None


def _invoke(
    telemetry: TelemetrySink | None,
    operation: str,
    call: Callable[..., Mapping[str, object]],
    *,
    phase: str = "maintenance",
    expected_errors: frozenset[str] = frozenset(),
    **request: object,
) -> Mapping[str, object] | None:
    """Record SDK calls and failures without bodies, keys, or receipt handles."""
    started = time.monotonic()
    record = {
        "timestamp": _timestamp(),
        "component": "aws_adapter",
        "action": "api_call",
        "phase": phase,
        "category": f"rtmsf_{phase}",
        "operation": operation,
        "attempts": 1,
    }
    try:
        response = call(**request)
    except Exception as error:
        metadata = error.response.get("ResponseMetadata", {}) if isinstance(error, ClientError) else {}
        record.update(status="error", error_type=type(error).__name__)
        if isinstance(error, ClientError):
            record["error_code"] = _error_code(error)
        if isinstance(metadata, Mapping):
            record["attempts"] = int(metadata.get("RetryAttempts", 0)) + 1
            record["request_id"] = str(metadata.get("RequestId", ""))
        record["duration_ms"] = round((time.monotonic() - started) * 1000, 3)
        if telemetry is not None:
            telemetry.emit(record)
        if isinstance(error, ClientError) and _error_code(error) in expected_errors:
            return None
        _raise_external(error)
        raise AssertionError("unreachable")
    metadata = response.get("ResponseMetadata", {})
    if isinstance(metadata, Mapping):
        record["attempts"] = int(metadata.get("RetryAttempts", 0)) + 1
        record["request_id"] = str(metadata.get("RequestId", ""))
    failures = response.get("Failed", [])
    record.update(
        status="error" if failures else "ok",
        duration_ms=round((time.monotonic() - started) * 1000, 3),
    )
    if failures:
        record["failed_count"] = len(failures)
    if telemetry is not None:
        telemetry.emit(record)
    return response


def _sources(sources: Mapping[str, str]) -> dict[str, str]:
    if not sources or any(not isinstance(key, str) or not key or not isinstance(value, str) or not value for key, value in sources.items()):
        raise ValueError("sources must map nonempty logical IDs to bucket names")
    if len(set(sources.values())) != len(sources):
        raise ValueError("each source must identify a distinct bucket")
    return dict(sources)


def _source_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.utcoffset() is not None else None
    except (ValueError, OverflowError):
        return None


def _decode_s3_body(body: str, source_by_bucket: Mapping[str, str]) -> tuple[ObjectEvent, ...]:
    document = json.loads(body)
    if not isinstance(document, Mapping):
        raise ValueError("invalid source envelope")
    if document.get("Event") == "s3:TestEvent":
        return ()
    records = document.get("Records")
    if not isinstance(records, list) or not records:
        raise ValueError("missing source records")
    ingested_at = datetime.now(timezone.utc)
    events = []
    for record in records:
        if not isinstance(record, Mapping):
            raise ValueError("invalid source record")
        name = record.get("eventName")
        if not isinstance(name, str):
            raise ValueError("missing source event name")
        if record.get("eventSource", "aws:s3") != "aws:s3":
            continue
        if name.startswith("ObjectCreated:"):
            operation = "CREATE"
        elif name.startswith("ObjectRemoved:"):
            operation = "DELETE"
        else:
            continue
        s3 = record.get("s3", {})
        bucket = s3.get("bucket", {}) if isinstance(s3, Mapping) else {}
        item = s3.get("object", {}) if isinstance(s3, Mapping) else {}
        bucket_name = bucket.get("name") if isinstance(bucket, Mapping) else None
        encoded_key = item.get("key") if isinstance(item, Mapping) else None
        if not isinstance(bucket_name, str) or bucket_name not in source_by_bucket:
            raise ValueError("unconfigured event source")
        if not isinstance(encoded_key, str) or not encoded_key:
            raise ValueError("missing object key")
        events.append(ObjectEvent(
            source_id=source_by_bucket[bucket_name],
            object_key=unquote_plus(encoded_key, errors="strict"),
            operation=operation,
            ingested_at=ingested_at,
            source_event_time=_source_time(record.get("eventTime")),
        ))
    return tuple(events)


@dataclass
class _QueueMessage:
    receipt: str
    body: str
    message_id: str
    acknowledged: bool = False
    dead_lettered: bool = False


class SqsEventSource(EventSource):
    """Keep provider state private; expose only handles and canonical events."""

    def __init__(
        self,
        client: object,
        queue_url: str,
        dead_letter_url: str,
        sources: Mapping[str, str],
        telemetry: TelemetrySink | None = None,
        polling_client: object | None = None,
    ) -> None:
        self._client = client
        self._polling_client = polling_client if polling_client is not None else client
        self._queue_url = queue_url
        self._dead_letter_url = dead_letter_url
        self._source_by_bucket = {bucket: source for source, bucket in _sources(sources).items()}
        self._telemetry = telemetry
        self._messages: dict[str, _QueueMessage] = {}

    def receive(self, max_messages: int, wait_seconds: int, lease_seconds: int) -> Sequence[Delivery]:
        if max_messages < 1 or not 0 <= wait_seconds <= 20 or not 1 <= lease_seconds <= 43_200:
            raise ValueError("invalid receive limits")
        response = _invoke(
            self._telemetry, "SQS.ReceiveMessage", self._polling_client.receive_message,
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=min(10, max_messages),
            WaitTimeSeconds=wait_seconds,
            VisibilityTimeout=lease_seconds,
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
        )
        deliveries = []
        malformed = []
        admitted_handles = []
        try:
            for message in response.get("Messages", []):
                handle = uuid4().hex
                body = message.get("Body", "")
                self._messages[handle] = _QueueMessage(message["ReceiptHandle"], body, message.get("MessageId", ""))
                admitted_handles.append(handle)
                if self._telemetry is not None:
                    self._telemetry.emit({
                        "timestamp": _timestamp(), "component": "aws_adapter",
                        "action": "message_received", "phase": "maintenance",
                        "message_id": self._messages[handle].message_id,
                        "receive_count": message.get("Attributes", {}).get("ApproximateReceiveCount"),
                    })
                try:
                    events = _decode_s3_body(body, self._source_by_bucket)
                    decoded_events = [{
                        "source_id": event.source_id,
                        "key_sha256": hashlib.sha256(event.object_key.encode()).hexdigest(),
                        "operation": event.operation,
                    } for event in events]
                except (ValueError, TypeError, UnicodeError, RecursionError):
                    malformed.append(Delivery(handle, ()))
                else:
                    deliveries.append(Delivery(handle, events))
                    if self._telemetry is not None:
                        self._telemetry.emit({
                            "timestamp": _timestamp(), "component": "aws_adapter",
                            "action": "message_decoded", "phase": "maintenance",
                            "message_id": self._messages[handle].message_id,
                            "events": decoded_events,
                        })
            if malformed:
                if self._telemetry is not None:
                    self._telemetry.emit({"timestamp": _timestamp(), "component": "aws_adapter", "action": "malformed_delivery", "phase": "maintenance", "delivery_count": len(malformed), "reason": "malformed_source_event"})
                # A receive may need ten size-limited DLQ sends, followed by
                # one acknowledgement batch. Unsent input remains replayable.
                self.dead_letter(malformed, "malformed_source_event")
                self.acknowledge(malformed)
        except Exception:
            # Unreturned valid siblings remain in SQS for replay. Do not retain
            # their inaccessible handles after a malformed-message operation fails.
            for handle in admitted_handles:
                self._messages.pop(handle, None)
            raise
        return tuple(deliveries)

    def _entries(self, deliveries: Sequence[Delivery]) -> list[tuple[str, _QueueMessage]]:
        entries = []
        for delivery in deliveries:
            message = self._messages.get(delivery.handle)
            if message is None:
                raise FatalExternalError("unknown_delivery_handle")
            entries.append((delivery.handle, message))
        return entries

    def _batches(self, entries: Sequence[tuple[str, _QueueMessage]], operation: str, method: Callable, request_entry: Callable, *, terminal_state: str | None = None, queue_url: str | None = None) -> None:
        prepared = [(message, request_entry(message)) for _, message in entries]
        start = 0
        while start < len(prepared):
            batch = []
            batch_bytes = 0
            for message, request in prepared[start:start + 10]:
                size = len(request.get("MessageBody", "").encode("utf-8"))
                if size > SQS_MAX_MESSAGE_BYTES:
                    raise FatalExternalError("oversized_source_message")
                if batch and batch_bytes + size > SQS_MAX_MESSAGE_BYTES:
                    break
                batch.append((message, request))
                batch_bytes += size
            response = _invoke(
                self._telemetry, operation, method,
                QueueUrl=queue_url or self._queue_url,
                Entries=[{"Id": str(index), **request} for index, (_, request) in enumerate(batch)],
            )
            failed = response.get("Failed", [])
            failed_ids = {item.get("Id") for item in failed}
            if terminal_state is not None:
                for index, (message, _) in enumerate(batch):
                    if str(index) not in failed_ids:
                        setattr(message, terminal_state, True)
                        if self._telemetry is not None:
                            self._telemetry.emit({
                                "timestamp": _timestamp(), "component": "aws_adapter",
                                "action": f"message_{terminal_state}", "phase": "maintenance",
                                "message_id": message.message_id,
                            })
            if failed:
                if any(item.get("SenderFault") and item.get("Code") not in RETRYABLE_CODES for item in failed):
                    raise FatalExternalError("sqs_batch_sender_fault")
                raise RetryableExternalError("sqs_batch_partial_failure")
            start += len(batch)

    def acknowledge(self, deliveries: Sequence[Delivery]) -> None:
        entries = self._entries(deliveries)
        self._batches(
            [item for item in entries if not item[1].acknowledged],
            "SQS.DeleteMessageBatch", self._client.delete_message_batch,
            lambda message: {"ReceiptHandle": message.receipt},
            terminal_state="acknowledged",
        )
        for handle, _ in entries:
            self._messages.pop(handle, None)

    def renew(self, deliveries: Sequence[Delivery], lease_seconds: int) -> None:
        if not 1 <= lease_seconds <= 43_200:
            raise ValueError("invalid lease duration")
        self._batches(
            [item for item in self._entries(deliveries) if not item[1].acknowledged],
            "SQS.ChangeMessageVisibilityBatch", self._client.change_message_visibility_batch,
            lambda message: {"ReceiptHandle": message.receipt, "VisibilityTimeout": lease_seconds},
        )

    def dead_letter(self, deliveries: Sequence[Delivery], reason: str) -> None:
        entries = [item for item in self._entries(deliveries) if not item[1].dead_lettered]
        self._batches(
            entries,
            "SQS.SendMessageBatch", self._client.send_message_batch,
            lambda message: {"MessageBody": message.body},
            terminal_state="dead_lettered", queue_url=self._dead_letter_url,
        )
        if entries and self._telemetry is not None:
            self._telemetry.emit({"timestamp": _timestamp(), "component": "aws_adapter", "action": "dead_lettered", "phase": "maintenance", "delivery_count": len(entries), "reason": reason})


class S3ObjectStore(ObjectStore):
    def __init__(self, client: object, sources: Mapping[str, str], telemetry: TelemetrySink | None = None) -> None:
        self._client = client
        self._sources = _sources(sources)
        self._telemetry = telemetry

    def _bucket(self, source_id: str) -> str:
        try:
            return self._sources[source_id]
        except KeyError:
            raise FatalExternalError("unknown_source_id") from None

    def head_exists(self, source_id: str, key: str) -> bool:
        return _invoke(
            self._telemetry, "S3.HeadObject", self._client.head_object,
            expected_errors=frozenset({"404", "NoSuchKey", "NotFound"}),
            Bucket=self._bucket(source_id), Key=key,
        ) is not None

    def prefix_non_empty(self, source_id: str, prefix: str) -> bool:
        response = _invoke(
            self._telemetry, "S3.ListObjectsV2", self._client.list_objects_v2,
            Bucket=self._bucket(source_id), Prefix=prefix, MaxKeys=1,
        )
        return int(response.get("KeyCount", 0)) > 0

    def location_uri(self, source_id: str, prefix: str) -> str:
        return f"s3://{self._bucket(source_id)}/{prefix}"


@dataclass(frozen=True)
class _GlueTable:
    definition: TableDefinition
    descriptor: Mapping[str, object]


class GluePartitionCatalog(PartitionCatalog):
    """Resolve existing table descriptors once and cache them for mutations."""

    def __init__(self, client: object, sources: Mapping[str, str], telemetry: TelemetrySink | None = None) -> None:
        self._client = client
        self._source_by_bucket = {bucket: source for source, bucket in _sources(sources).items()}
        self._telemetry = telemetry
        self._tables: dict[str, _GlueTable] = {}

    def resolve_tables(self, table_ids: Sequence[str]) -> Sequence[TableDefinition]:
        for table_id in table_ids:
            if table_id in self._tables:
                continue
            if not isinstance(table_id, str) or table_id.count(".") != 1 or not all(table_id.split(".")):
                raise FatalExternalError("invalid_table_id")
            database, name = table_id.split(".")
            response = _invoke(self._telemetry, "Glue.GetTable", self._client.get_table, phase="startup", DatabaseName=database, Name=name)
            try:
                table = response["Table"]
                descriptor = deepcopy(table["StorageDescriptor"])
                location = urlsplit(descriptor["Location"])
                if location.scheme != "s3" or location.query or location.fragment:
                    raise ValueError("table location must be an S3 prefix")
                source_id = self._source_by_bucket[location.netloc]
                root = location.path[1:] if location.path.startswith("/") else location.path
                if root and not root.endswith("/"):
                    root += "/"
                columns = tuple(column["Name"] for column in table["PartitionKeys"])
                definition = TableDefinition(table_id, source_id, root, columns)
            except (KeyError, TypeError, ValueError):
                raise FatalExternalError("invalid_existing_table_definition") from None
            self._tables[table_id] = _GlueTable(definition, descriptor)
        return tuple(self._tables[table_id].definition for table_id in table_ids)

    def _table(self, partition: PartitionRef) -> _GlueTable:
        table = self._tables.get(partition.table_id)
        if table is None or table.definition.source_id != partition.source_id:
            raise FatalExternalError("unresolved_partition_table")
        return table

    def _identity(self, partition: PartitionRef) -> dict[str, str]:
        self._table(partition)
        database, name = partition.table_id.split(".")
        return {"DatabaseName": database, "TableName": name}

    def partition_exists(self, partition: PartitionRef) -> bool:
        return _invoke(
            self._telemetry, "Glue.GetPartition", self._client.get_partition,
            expected_errors=frozenset({"EntityNotFoundException"}),
            **self._identity(partition), PartitionValues=list(partition.values),
        ) is not None

    def create_partition(self, partition: PartitionRef, location: str) -> bool:
        descriptor = deepcopy(self._table(partition).descriptor)
        descriptor["Location"] = location
        return _invoke(
            self._telemetry, "Glue.CreatePartition", self._client.create_partition,
            expected_errors=frozenset({"AlreadyExistsException"}),
            **self._identity(partition),
            PartitionInput={"Values": list(partition.values), "StorageDescriptor": descriptor},
        ) is not None

    def delete_partition(self, partition: PartitionRef) -> bool:
        return _invoke(
            self._telemetry, "Glue.DeletePartition", self._client.delete_partition,
            expected_errors=frozenset({"EntityNotFoundException"}),
            **self._identity(partition), PartitionValues=list(partition.values),
        ) is not None


class JsonTelemetry(TelemetrySink):
    def __init__(self, stream: IO[str] = sys.stdout) -> None:
        self._stream = stream

    def emit(self, record: Mapping[str, object]) -> None:
        self._stream.write(json.dumps({"timestamp": _timestamp(), **record}, sort_keys=True, separators=(",", ":")) + "\n")
        self._stream.flush()
