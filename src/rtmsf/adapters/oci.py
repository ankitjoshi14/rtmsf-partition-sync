"""OCI ingress/storage boundary for the existing RTMSF worker; no catalog adapter."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from urllib.parse import quote
from uuid import uuid4

import oci
from circuitbreaker import CircuitBreakerError

from rtmsf.interfaces import Delivery, FatalExternalError, RetryableExternalError
from rtmsf.models import ObjectEvent


NO_RETRY = oci.retry.NoneRetryStrategy()
MAX_BODY_BYTES = 256 * 1024
MAX_PENDING = 20
OPERATIONS = {
    "com.oraclecloud.objectstorage.createobject": "CREATE",
    "com.oraclecloud.objectstorage.updateobject": "CREATE",
    "com.oraclecloud.objectstorage.deleteobject": "DELETE",
}


def _sources(sources):
    if not sources:
        raise ValueError("configure at least one logical source")
    result = {}
    for name, value in sources.items():
        if not isinstance(name, str) or not name or not isinstance(value, (tuple, list)) or len(value) != 2:
            raise ValueError("sources map logical IDs to (namespace, bucket)")
        if any(not isinstance(item, str) or not item for item in value):
            raise ValueError("namespace and bucket must be nonempty strings")
        result[name] = tuple(value)
    if len(set(result.values())) != len(result):
        raise ValueError("each source must identify a distinct namespace/bucket")
    return result


def _call(operation, *args, **kwargs):
    try:
        return operation(*args, retry_strategy=NO_RETRY, **kwargs)
    except oci.exceptions.ServiceError as error:
        kind = RetryableExternalError if error.status == 429 or error.status >= 500 else FatalExternalError
        raise kind(f"oci_status_{error.status}") from None
    except (oci.exceptions.RequestException, CircuitBreakerError):
        raise RetryableExternalError("oci_transport_failure") from None


def decode_event(body, sources):
    configured = _sources(sources)
    document = json.loads(body)
    if not isinstance(document, dict) or document.get("source") not in {"objectstorage", "ObjectStorage"}:
        raise ValueError("not an OCI Object Storage envelope")
    operation = OPERATIONS.get(document.get("eventType"))
    if operation is None:
        raise ValueError("unsupported event type")
    data = document.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("additionalDetails"), dict):
        raise ValueError("missing object details")
    details = data["additionalDetails"]
    source = next((name for name, target in configured.items()
                   if target == (details.get("namespace"), details.get("bucketName"))), None)
    key = data.get("resourceName")
    if source is None or not isinstance(key, str) or not key:
        raise ValueError("unconfigured source or missing exact object key")
    source_time = None
    value = document.get("eventTime")
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.utcoffset() is not None:
                source_time = parsed.astimezone(timezone.utc)
        except (ValueError, OverflowError):
            pass
    # OCI resourceName is an exact key; the S3 unquote_plus conversion is unsafe here.
    return (ObjectEvent(source, key, operation, datetime.now(timezone.utc), source_time),)


def enqueue_event(client, queue_id, body):
    if not isinstance(body, str) or not body or len(body.encode("utf-8")) > MAX_BODY_BYTES:
        raise FatalExternalError("invalid_or_oversized_queue_body")
    response = _call(client.put_messages, queue_id, oci.queue.models.PutMessagesDetails(
        messages=[oci.queue.models.PutMessagesDetailsEntry(content=body)]))
    items = response.data.messages
    if items is None or len(items) != 1 or items[0].id is None:
        raise RetryableExternalError("queue_handoff_unconfirmed")


@dataclass
class _Pending:
    receipt: str
    body: str
    quarantined: bool = False


class OciQueueEventSource:
    """Clients must target each queue's messagesEndpoint, with finite timeouts."""

    def __init__(self, client, queue_id, quarantine_client, quarantine_id, sources):
        if not queue_id or not quarantine_id or queue_id == quarantine_id:
            raise ValueError("use separate source and quarantine queues")
        self._client, self._queue_id = client, queue_id
        self._quarantine_client, self._quarantine_id = quarantine_client, quarantine_id
        self._sources = _sources(sources)
        self._pending = {}

    def receive(self, max_messages, wait_seconds, lease_seconds):
        if type(max_messages) is not int or max_messages < 1 or not 0 <= wait_seconds <= 30 or not 1 <= lease_seconds <= 43_200:
            raise ValueError("invalid receive bounds")
        capacity = min(max_messages, MAX_PENDING - len(self._pending))
        if capacity == 0:
            raise RuntimeError("worker pending-delivery limit must be at most 20")
        result = _call(self._client.get_messages, self._queue_id,
                       limit=capacity, timeout_in_seconds=wait_seconds,
                       visibility_in_seconds=lease_seconds)
        messages = result.data.messages
        if messages is None or len(messages) > capacity:
            raise FatalExternalError("queue_response_exceeded_capacity")
        receipts = [m.receipt for m in messages]
        if len(set(receipts)) != len(receipts) or set(receipts) & {p.receipt for p in self._pending.values()}:
            raise FatalExternalError("queue_reused_receipt")
        deliveries = []
        for message in messages:
            if not message.receipt or not isinstance(message.content, str) or len(message.content.encode("utf-8")) > MAX_BODY_BYTES:
                raise FatalExternalError("invalid_queue_message")
            handle = uuid4().hex
            self._pending[handle] = _Pending(message.receipt, message.content)
            try:
                events = decode_event(message.content, self._sources)
            except (ValueError, TypeError, UnicodeError, RecursionError):
                malformed = (Delivery(handle, ()),)
                self.dead_letter(malformed, "malformed_oci_event")
                self.acknowledge(malformed)
            else:
                deliveries.append(Delivery(handle, events))
        return tuple(deliveries)

    def _entries(self, deliveries):
        if len(deliveries) > MAX_PENDING or len({d.handle for d in deliveries}) != len(deliveries):
            raise ValueError("invalid delivery batch")
        return [self._pending[d.handle] for d in deliveries]

    @staticmethod
    def _verify_batch(response, expected):
        data = response.data
        entries = data.entries
        if entries is None or len(entries) != expected:
            raise RetryableExternalError("queue_batch_unconfirmed")
        if data.server_failures or data.client_failures or any(e.error_code for e in entries):
            raise RetryableExternalError("queue_batch_failed_restart_for_replay")

    def acknowledge(self, deliveries):
        pending = self._entries(deliveries)
        if not pending:
            return
        result = _call(self._client.delete_messages, self._queue_id,
                       oci.queue.models.DeleteMessagesDetails(entries=[
                           oci.queue.models.DeleteMessagesDetailsEntry(receipt=p.receipt) for p in pending]))
        self._verify_batch(result, len(pending))
        for delivery in deliveries:
            del self._pending[delivery.handle]

    def renew(self, deliveries, lease_seconds):
        if not 1 <= lease_seconds <= 43_200:
            raise ValueError("invalid lease")
        pending = self._entries(deliveries)
        if not pending:
            return
        result = _call(self._client.update_messages, self._queue_id,
                       oci.queue.models.UpdateMessagesDetails(entries=[
                           oci.queue.models.UpdateMessagesDetailsEntry(
                               receipt=p.receipt, visibility_in_seconds=lease_seconds) for p in pending]))
        self._verify_batch(result, len(pending))

    def dead_letter(self, deliveries, reason):
        # Preserve raw input; no receipt or credential material in quarantine content.
        for pending in self._entries(deliveries):
            if not pending.quarantined:
                enqueue_event(self._quarantine_client, self._quarantine_id, pending.body)
                pending.quarantined = True


class OciObjectStore:
    def __init__(self, client, sources):
        self._client, self._sources = client, _sources(sources)

    def head_exists(self, source_id, key):
        namespace, bucket = self._sources[source_id]
        try:
            self._client.head_object(namespace, bucket, key, retry_strategy=NO_RETRY)
        except oci.exceptions.ServiceError as error:
            # Ambiguous 404/authorization failures must never be treated as absence.
            if error.status == 404 and error.code == "ObjectNotFound":
                return False
            kind = RetryableExternalError if error.status == 429 or error.status >= 500 else FatalExternalError
            raise kind(f"oci_status_{error.status}") from None
        except (oci.exceptions.RequestException, CircuitBreakerError):
            raise RetryableExternalError("oci_transport_failure") from None
        return True

    def prefix_non_empty(self, source_id, prefix):
        namespace, bucket = self._sources[source_id]
        result = _call(self._client.list_objects, namespace, bucket, prefix=prefix, limit=1)
        if result.data.objects:
            return True
        if result.data.objects is None or result.data.next_start_with or result.data.prefixes:
            raise RetryableExternalError("incomplete_empty_prefix_response")
        return False

    def location_uri(self, source_id, prefix):
        namespace, bucket = self._sources[source_id]
        return f"oci://{bucket}@{namespace}/{quote(prefix, safe='/=')}"
