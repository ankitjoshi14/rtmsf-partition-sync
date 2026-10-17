"""Azure ingress and primary HNS storage truth for the existing RTMSF core."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import time
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID, uuid4

from rtmsf.interfaces import Delivery, EventSource, FatalExternalError, ObjectStore, RetryableExternalError
from rtmsf.models import ObjectEvent


MAX_BODY_BYTES = 256 * 1024
OPERATIONS = {
    "Microsoft.Storage.BlobCreated": {"PutBlob", "PutBlockList", "CopyBlob", "FlushWithClose"},
    "Microsoft.Storage.BlobDeleted": {"DeleteBlob", "DeleteFile"},
}


def _sources(sources):
    if not isinstance(sources, Mapping) or not sources:
        raise ValueError("configure at least one logical source")
    targets = set()
    for name, source in sources.items():
        if not isinstance(name, str) or not name or not isinstance(source, Mapping):
            raise ValueError("sources map logical IDs to account, container, and ARM topic")
        if set(source) != {"account", "container", "topic"} or any(not isinstance(value, str) for value in source.values()):
            raise ValueError("source requires account, container, and ARM topic strings")
        account, container, topic = source["account"], source["container"], source["topic"]
        if not re.fullmatch(r"[a-z0-9]{3,24}", account):
            raise ValueError("invalid storage account")
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", container) or "--" in container:
            raise ValueError("invalid storage container")
        if not re.fullmatch(r"/subscriptions/[^/]+/resourceGroups/[^/]+/providers/Microsoft\.Storage/storageAccounts/" + account, topic):
            raise ValueError("source topic must identify the configured storage account")
        target = account, container
        if target in targets:
            raise ValueError("each source must identify a distinct account/container")
        targets.add(target)
    return sources


def decode_event(body, sources):
    configured = _sources(sources)
    if not isinstance(body, str) or len(body.encode("utf-8")) > MAX_BODY_BYTES:
        raise ValueError("invalid or oversized event body")
    document = json.loads(body)
    if not isinstance(document, dict) or not isinstance(document.get("data"), dict):
        raise ValueError("require one Event Grid event object")
    if not isinstance(document.get("id"), str) or not document["id"]:
        raise ValueError("missing event ID")
    data = document["data"]
    url = data.get("url")
    if not isinstance(url, str) or url.startswith(" ") or any(ord(char) < 32 or ord(char) == 127 for char in url):
        raise ValueError("invalid object URL")
    parsed = urlsplit(url)
    host = re.fullmatch(r"([a-z0-9]{3,24})\.(?:blob|dfs)\.core\.windows\.net", parsed.netloc)
    if parsed.scheme != "https" or not host or parsed.query or parsed.fragment or not parsed.path.startswith("/"):
        raise ValueError("require a primary Azure Storage HTTPS URL")
    container, separator, encoded_key = parsed.path[1:].partition("/")
    if not separator or re.search(r"%(?![0-9a-fA-F]{2})", encoded_key):
        raise ValueError("missing key or invalid percent escape")
    key = unquote(encoded_key, encoding="utf-8", errors="strict")
    key.encode("utf-8", errors="strict")
    if not key or key.endswith("/") or any(part in {"", ".", ".."} for part in key.split("/")) or any(ord(char) < 32 or ord(char) == 127 for char in key):
        raise ValueError("require an exact file key without path normalization")
    source_id = next((name for name, source in configured.items() if
                      source["account"] == host[1] and source["container"] == container
                      and source["topic"] == document.get("topic")), None)
    if source_id is None:
        raise ValueError("unconfigured topic, account, or container")
    # shortcut: require raw subject equality until live special-name events verify escaping.
    if document.get("subject") != f"/blobServices/default/containers/{container}/blobs/{key}":
        raise ValueError("subject and exact URL key disagree")
    event_type, api = document.get("eventType"), data.get("api")
    if not isinstance(event_type, str) or not isinstance(api, str):
        raise ValueError("missing event type or storage API")
    if event_type == "Microsoft.Storage.BlobCreated" and api == "CreateFile":
        return ()
    if event_type not in OPERATIONS or api not in OPERATIONS[event_type] or data.get("blobType") != "BlockBlob":
        raise ValueError("unsupported or uncommitted file operation")
    source_time = None
    value = document.get("eventTime")
    if isinstance(value, str):
        try:
            parsed_time = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed_time.utcoffset() is not None:
                source_time = parsed_time.astimezone(timezone.utc)
        except (ValueError, OverflowError):
            pass
    operation = "CREATE" if event_type == "Microsoft.Storage.BlobCreated" else "DELETE"
    return (ObjectEvent(source_id, key, operation, datetime.now(timezone.utc), source_time),)


def _check_budget(started, seconds):
    elapsed = time.monotonic() - started
    if elapsed < 0 or elapsed > seconds:
        raise RetryableExternalError("azure_operation_budget_exceeded")


def _emit_call(telemetry, operation, started, error_type=None):
    if telemetry is not None:
        telemetry.emit({
            "timestamp": datetime.now(timezone.utc).isoformat(), "component": "azure_adapter",
            "action": "api_call", "phase": "maintenance", "category": "rtmsf_maintenance",
            "operation": operation, "attempts": 1, "status": "error" if error_type else "ok",
            "error_type": error_type, "duration_ms": round((time.monotonic() - started) * 1000, 3),
        })


@dataclass
class _PendingMessage:
    message: object
    lock_token: UUID
    terminal_reason: str | None = None
    settled: bool = False


class AzureServiceBusEventSource(EventSource):
    """Own one PEEK_LOCK receiver with no prefetch, retries, or uAMQP transport.

    Configure its public management timeout to 10 seconds and broker locks to
    300 seconds. The 80-second invocation budget detects overruns after return;
    public SDK timeouts cannot preempt every authentication/settlement phase.
    """

    def __init__(self, receiver, sources, telemetry=None):
        from azure.core.exceptions import ServiceRequestError, ServiceResponseError
        from azure.servicebus.amqp import AmqpMessageBodyType
        from azure.servicebus import exceptions

        self._receiver = receiver
        self._sources = {name: dict(source) for name, source in _sources(sources).items()}
        self._telemetry = telemetry
        self._data_type = AmqpMessageBodyType.DATA
        self._retryable = (
            exceptions.ServiceBusConnectionError, exceptions.ServiceBusCommunicationError,
            exceptions.ServiceBusServerBusyError, exceptions.OperationTimeoutError,
            exceptions.MessageLockLostError, exceptions.SessionLockLostError,
            ServiceRequestError, ServiceResponseError, ConnectionError, TimeoutError,
        )
        self._messages: dict[str, _PendingMessage] = {}

    def _call(self, operation, method, started, budget, *args, settled=None, **kwargs):
        _check_budget(started, budget)
        call_started = time.monotonic()
        try:
            result = method(*args, **kwargs)
        except Exception as error:
            retryable = isinstance(error, self._retryable)
            _emit_call(self._telemetry, operation, call_started, "retryable" if retryable else "fatal")
            kind = RetryableExternalError if retryable else FatalExternalError
            raise kind("azure_queue_transport_failure" if retryable else "azure_queue_configuration_or_state_failure") from None
        if settled is not None:
            if result is not None:
                raise FatalExternalError("azure_settlement_response_unconfirmed")
            # Broker confirmation must survive telemetry failure or an elapsed-budget overrun.
            settled.settled = True
        _emit_call(self._telemetry, operation, call_started)
        _check_budget(started, budget)
        return result

    @staticmethod
    def _lease(expiry, lease_seconds):
        if not isinstance(expiry, datetime) or expiry.utcoffset() != timedelta(0):
            raise RetryableExternalError("azure_broker_lease_unconfirmed")
        if (expiry - datetime.now(timezone.utc)).total_seconds() < lease_seconds:
            raise RetryableExternalError("azure_broker_lease_too_short")

    @staticmethod
    def _token(message):
        token = getattr(message, "lock_token", None)
        try:
            token = token if isinstance(token, UUID) else UUID(token) if isinstance(token, str) else None
        except ValueError:
            token = None
        if token is None or token.int == 0:
            raise FatalExternalError("azure_missing_or_invalid_lock_token")
        return token

    def _body(self, message):
        if message.body_type != self._data_type:
            raise ValueError("require AMQP DATA")
        body = message.body
        if isinstance(body, bytes):
            if len(body) > MAX_BODY_BYTES:
                raise ValueError("oversized event body")
            return body.decode("utf-8", errors="strict")
        fragments, size = [], 0
        for index, fragment in enumerate(body):
            if index >= 1024 or not isinstance(fragment, bytes):
                raise ValueError("invalid or excessive DATA fragments")
            size += len(fragment)
            if size > MAX_BODY_BYTES:
                raise ValueError("oversized event body")
            fragments.append(fragment)
        return b"".join(fragments).decode("utf-8", errors="strict")

    def receive(self, max_messages, wait_seconds, lease_seconds):
        started = time.monotonic()
        if type(max_messages) is not int or max_messages < 1 or type(wait_seconds) is not int or not 0 <= wait_seconds <= 20:
            raise ValueError("invalid receive limits")
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ValueError("invalid lease duration")
        if len(self._messages) >= 4:
            raise FatalExternalError("azure_pending_delivery_capacity_exceeded")
        messages = self._call(
            "ServiceBus.ReceiveMessages", self._receiver.receive_messages, started, 80 + wait_seconds,
            max_message_count=1, max_wait_time=wait_seconds or 0.01,
        )
        if not isinstance(messages, Sequence) or isinstance(messages, (str, bytes, bytearray)) or len(messages) > 1:
            raise FatalExternalError("azure_invalid_receive_response")
        if not messages:
            return ()
        message = messages[0]
        token = self._token(message)
        self._lease(getattr(message, "locked_until_utc", None), lease_seconds)
        if any(pending.message is message or pending.lock_token == token for pending in self._messages.values()):
            raise FatalExternalError("azure_reused_pending_message")
        reason = None
        try:
            events = decode_event(self._body(message), self._sources)
        except Exception:
            events, reason = (), "malformed_source_event"
        _check_budget(started, 80 + wait_seconds)
        self._lease(getattr(message, "locked_until_utc", None), lease_seconds)
        handle = uuid4().hex
        if handle in self._messages:
            raise FatalExternalError("azure_reused_delivery_handle")
        self._messages[handle] = _PendingMessage(message, token, reason)
        return (Delivery(handle, events),)

    def _entries(self, deliveries):
        if not isinstance(deliveries, Sequence) or len(deliveries) > 4:
            raise FatalExternalError("azure_invalid_delivery_batch")
        entries, seen = [], set()
        for delivery in deliveries:
            if not isinstance(delivery, Delivery) or not isinstance(delivery.handle, str):
                raise FatalExternalError("unknown_delivery_handle")
            if delivery.handle in seen:
                raise FatalExternalError("duplicate_delivery_handle")
            pending = self._messages.get(delivery.handle)
            if pending is None:
                raise FatalExternalError("unknown_delivery_handle")
            seen.add(delivery.handle)
            entries.append((delivery.handle, pending))
        return entries

    def acknowledge(self, deliveries):
        started = time.monotonic()
        entries = self._entries(deliveries)
        for _, pending in entries:
            if not pending.settled:
                if pending.terminal_reason is None:
                    self._call("ServiceBus.CompleteMessage", self._receiver.complete_message, started, 80,
                               pending.message, settled=pending)
                else:
                    self._call("ServiceBus.DeadLetterMessage", self._receiver.dead_letter_message, started, 80,
                               pending.message, settled=pending, reason=pending.terminal_reason)
        _check_budget(started, 80)
        for handle, _ in entries:
            del self._messages[handle]

    def dead_letter(self, deliveries, reason):
        started = time.monotonic()
        if not isinstance(reason, str) or not reason or len(reason.encode("utf-8")) > 256 or any(ord(char) < 32 or ord(char) == 127 for char in reason):
            raise ValueError("invalid dead-letter reason")
        for _, pending in self._entries(deliveries):
            if not pending.settled:
                self._call("ServiceBus.DeadLetterMessage", self._receiver.dead_letter_message, started, 80,
                           pending.message, settled=pending, reason=reason)
        _check_budget(started, 80)

    def renew(self, deliveries, lease_seconds):
        started = time.monotonic()
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise ValueError("invalid lease duration")
        for _, pending in self._entries(deliveries):
            if not pending.settled:
                expiry = self._call("ServiceBus.RenewMessageLock", self._receiver.renew_message_lock, started, 80,
                                    pending.message, timeout=10)
                self._lease(expiry, lease_seconds)
        _check_budget(started, 80)


def _exact_path(value, *, directory=False):
    if not isinstance(value, str) or not value or value.endswith("/") != directory:
        raise FatalExternalError("azure_invalid_exact_path")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError:
        raise FatalExternalError("azure_invalid_exact_path") from None
    parts = (value[:-1] if directory else value).split("/")
    if any(part in {"", ".", ".."} for part in parts) or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value):
        raise FatalExternalError("azure_invalid_exact_path")


def _eligible_file(key):
    # Hadoop/Spark markers and staging paths are not committed partition data.
    return all(not part.startswith((".", "_")) for part in key.split("/"))


class AzureObjectStore(ObjectStore):
    """Trusted primary clients need zero retries and finite transport timeouts.

    Storage timeout=5 is server-side; the 30-second elapsed budget checks the
    whole invocation. Negative truth requires complete bounded enumeration.
    """

    def __init__(self, blob_container_clients, filesystem_clients, sources, telemetry=None):
        from azure.core.exceptions import HttpResponseError, ServiceRequestError, ServiceResponseError

        try:
            self._sources = {name: dict(source) for name, source in _sources(sources).items()}
        except ValueError:
            raise FatalExternalError("azure_invalid_source_configuration") from None
        if any(not isinstance(clients, Mapping) or set(clients) != set(self._sources) or any(client is None for client in clients.values())
               for clients in (blob_container_clients, filesystem_clients)):
            raise FatalExternalError("azure_invalid_storage_client_configuration")
        self._blob_clients = dict(blob_container_clients)
        self._filesystem_clients = dict(filesystem_clients)
        self._telemetry = telemetry
        self._http_error = HttpResponseError
        self._transport_errors = (ServiceRequestError, ServiceResponseError, ConnectionError, TimeoutError)

    def _source(self, source_id):
        try:
            return self._sources[source_id]
        except (KeyError, TypeError):
            raise FatalExternalError("unknown_source_id") from None

    def _missing_or_raise(self, error, missing_code):
        if isinstance(error, self._http_error):
            status = error.status_code
            if status == 404 and getattr(error, "error_code", None) == missing_code:
                return False
            retryable = type(status) is int and (status in {408, 429} or 500 <= status < 600)
        else:
            retryable = isinstance(error, self._transport_errors)
        kind = RetryableExternalError if retryable else FatalExternalError
        raise kind("azure_storage_transient_failure" if retryable else "azure_storage_configuration_or_response_failure") from None

    def _head(self, source_id, key, started):
        _check_budget(started, 30)
        resource_type = [None]

        def resource_header(response):
            headers = getattr(getattr(response, "http_response", None), "headers", {})
            resource_type[0] = headers.get("x-ms-resource-type") if isinstance(headers, Mapping) else None

        call_started = time.monotonic()
        try:
            properties = self._blob_clients[source_id].get_blob_client(blob=key).get_blob_properties(
                timeout=5, raw_response_hook=resource_header,
            )
        except Exception as error:
            _emit_call(self._telemetry, "Blob.GetProperties", call_started, "external_failure")
            result = self._missing_or_raise(error, "BlobNotFound")
            _check_budget(started, 30)
            return result
        _emit_call(self._telemetry, "Blob.GetProperties", call_started)
        _check_budget(started, 30)
        if resource_type[0] == "directory":
            return False
        if resource_type[0] != "file":
            raise RetryableExternalError("azure_hns_resource_type_unconfirmed")
        try:
            copy_status = properties.copy.status
        except AttributeError:
            raise RetryableExternalError("azure_copy_status_unconfirmed") from None
        if copy_status in ("failed", "aborted"):
            raise FatalExternalError("azure_copy_failed")
        if copy_status not in (None, "success"):
            raise RetryableExternalError("azure_copy_status_unconfirmed")
        return True

    def head_exists(self, source_id, key):
        started = time.monotonic()
        self._source(source_id)
        _exact_path(key)
        _check_budget(started, 30)
        return self._head(source_id, key, started) if _eligible_file(key) else False

    def prefix_non_empty(self, source_id, prefix):
        started = time.monotonic()
        self._source(source_id)
        _exact_path(prefix, directory=True)
        candidates = 0
        _check_budget(started, 30)
        try:
            pages = self._filesystem_clients[source_id].get_paths(
                path=prefix[:-1], recursive=True, max_results=64, timeout=5,
            ).by_page()
        except Exception as error:
            _emit_call(self._telemetry, "DataLake.ListPaths", started, "external_failure")
            result = self._missing_or_raise(error, "PathNotFound")
            _check_budget(started, 30)
            return result
        _check_budget(started, 30)
        for _ in range(2):
            _check_budget(started, 30)
            call_started = time.monotonic()
            try:
                page = next(pages)
            except StopIteration:
                _check_budget(started, 30)
                if not hasattr(pages, "continuation_token") or pages.continuation_token is not None:
                    raise RetryableExternalError("azure_listing_exhaustion_unconfirmed")
                return False
            except Exception as error:
                _emit_call(self._telemetry, "DataLake.ListPaths", call_started, "external_failure")
                result = self._missing_or_raise(error, "PathNotFound")
                _check_budget(started, 30)
                return result
            _emit_call(self._telemetry, "DataLake.ListPaths", call_started)
            _check_budget(started, 30)
            entries = 0
            for entry in page:
                _check_budget(started, 30)
                entries += 1
                if entries > 64:
                    raise FatalExternalError("azure_listing_page_size_exceeded")
                name = getattr(entry, "name", None)
                _exact_path(name)
                if not name.startswith(prefix) or type(getattr(entry, "is_directory", None)) is not bool:
                    raise FatalExternalError("azure_invalid_listing_entry")
                if entry.is_directory or not _eligible_file(name):
                    continue
                if candidates >= 4:
                    raise RetryableExternalError("azure_listing_candidate_budget_exceeded")
                candidates += 1
                if self._head(source_id, name, started):
                    return True
            _check_budget(started, 30)
            if not hasattr(pages, "continuation_token"):
                raise RetryableExternalError("azure_listing_exhaustion_unconfirmed")
            if pages.continuation_token is None:
                return False
        raise RetryableExternalError("azure_listing_page_budget_exceeded")

    def location_uri(self, source_id, prefix):
        source = self._source(source_id)
        _exact_path(prefix, directory=True)
        return f"abfss://{source['container']}@{source['account']}.dfs.core.windows.net/{quote(prefix, safe='/=')}"
