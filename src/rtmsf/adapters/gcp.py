"""GCP native queue and current-live storage/catalog boundaries."""

from collections.abc import Mapping
from contextlib import contextmanager, suppress
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
import base64
import ipaddress
import json
import math
import os
import re
import socket
import time
from uuid import uuid4
from weakref import WeakKeyDictionary

from google.api_core import exceptions as api_errors
from google.auth import exceptions as auth_errors
from google.auth.transport.requests import AuthorizedSession
from google.pubsub_v1.services.publisher import PublisherClient
from google.pubsub_v1.services.publisher.transports.grpc import PublisherGrpcTransport
from google.pubsub_v1.services.subscriber import SubscriberClient
from google.pubsub_v1.services.subscriber.transports.grpc import SubscriberGrpcTransport
from google.pubsub_v1.types import pubsub
from requests.exceptions import RequestException

from pymetastore.hive_metastore import ThriftHiveMetastore, ttypes
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from rtmsf.interfaces import Delivery, FatalExternalError, PartitionCatalog, RetryableExternalError
from rtmsf.models import ObjectEvent, TableDefinition


STORAGE_ENDPOINT = "https://storage.googleapis.com"


def _canonical_endpoint(client):
    connection = getattr(client, "_connection", None)
    if getattr(connection, "API_BASE_URL", None) != STORAGE_ENDPOINT:
        return False
    return connection.get_api_base_url_for_mtls() == STORAGE_ENDPOINT


class OperationDeadline:
    """Share one elapsed budget across storage/catalog calls in an outer scope.

    SDK timeouts bound individual RPC waits; elapsed checks also reject overruns
    after authentication or slow response paths that those timeouts cannot stop.
    This context belongs to one synchronous writer.
    """

    def __init__(self, seconds=90, rpc_seconds=15, clock=None):
        for value in (seconds, rpc_seconds):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError("operation and RPC budgets must be positive finite seconds")
        if rpc_seconds > 15:
            raise ValueError("RPC budget must be at most 15 seconds")
        self.seconds, self.rpc_seconds = seconds, rpc_seconds
        self._clock = time.monotonic if clock is None else clock
        self._started = self._last = None
        self._failed = False

    @contextmanager
    def operation(self):
        outer = self._started is None
        if outer:
            self._started = self._last = self._clock()
            self._failed = False
        try:
            self.check()
            yield self
            self.check()
        finally:
            if outer:
                self._started = self._last = None

    def check(self):
        if self._started is None:
            raise RuntimeError("use deadline.operation() around external operations")
        now = self._clock()
        if (self._failed or type(now) not in (int, float) or not math.isfinite(now)
                or type(self._started) not in (int, float) or not math.isfinite(self._started)
                or now < self._last or now - self._started >= self.seconds):
            self._failed = True
            raise RetryableExternalError("gcp_operation_deadline_exceeded")
        self._last = now
        return self.seconds - (now - self._started)

    def rpc_timeout(self):
        return min(self.rpc_seconds, self.check())


def _sources(sources):
    if not isinstance(sources, Mapping) or not sources:
        raise ValueError("configure at least one logical source")
    result = {}
    for source_id, bucket in sources.items():
        if not isinstance(source_id, str) or not source_id or not isinstance(bucket, str):
            raise ValueError("sources map logical IDs to trusted bucket names")
        if (not 3 <= len(bucket) <= 222 or not re.fullmatch(r"[a-z0-9][a-z0-9._-]*[a-z0-9]", bucket)
                or any(len(part) > 63 or not part for part in bucket.split("."))
                or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", bucket)):
            raise ValueError("invalid bucket name")
        result[source_id] = bucket
    if len(set(result.values())) != len(result):
        raise ValueError("each source must identify a distinct bucket")
    return result


def _exact_text(value):
    if not isinstance(value, str) or not value:
        raise FatalExternalError("gcp_invalid_object_name")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError:
        raise FatalExternalError("gcp_invalid_object_name") from None
    return value


PUBSUB_RESPONSE_BYTES = 10_000_000
PUBSUB_ACK_BYTES = 512_000
# Expected initial deadline of the approved subscription; configuration drift fails.
PUBSUB_INITIAL_ACK_DEADLINE_SECONDS = 180
GCS_OPERATIONS = {"OBJECT_FINALIZE": "CREATE", "OBJECT_DELETE": "DELETE", "OBJECT_ARCHIVE": "DELETE"}


def _notification_configs(sources, notification_configs):
    if not isinstance(notification_configs, Mapping) or set(notification_configs) != set(sources):
        raise ValueError("gcp_invalid_notification_configuration")
    result = dict(notification_configs)
    for source, bucket in sources.items():
        value = result[source]
        if not isinstance(value, str) or not re.fullmatch(
                re.escape(f"projects/_/buckets/{bucket}/notificationConfigs/") + r"[1-9][0-9]*", value):
            raise ValueError("gcp_invalid_notification_configuration")
    return result


def _attributes(attributes):
    if not isinstance(attributes, Mapping) or len(attributes) > 100:
        raise ValueError("gcp_invalid_notification")
    for key, value in attributes.items():
        if (not isinstance(key, str) or not key or not isinstance(value, str)
                or len(key.encode("utf-8")) > 256 or len(value.encode("utf-8")) > 1024):
            raise ValueError("gcp_invalid_notification")


def _unique_json_object(pairs):
    document = dict(pairs)
    if len(document) != len(pairs):
        raise ValueError("gcp_invalid_notification")
    return document


def _invalid_json_constant(value):
    raise ValueError("gcp_invalid_notification")


def decode_event(data, attributes, sources, notification_configs):
    """Decode exact native GCS names; event generation never selects storage truth."""
    configured = _sources(sources)
    notifications = _notification_configs(configured, notification_configs)
    try:
        if not isinstance(data, bytes) or not data or len(data) > PUBSUB_RESPONSE_BYTES:
            raise ValueError
        _attributes(attributes)
        source = next((name for name, bucket in configured.items() if bucket == attributes.get("bucketId")), None)
        operation = GCS_OPERATIONS.get(attributes.get("eventType"))
        key = _exact_text(attributes.get("objectId"))
        generation, event_time = attributes.get("objectGeneration"), attributes.get("eventTime")
        if (source is None or operation is None or attributes.get("payloadFormat") != "JSON_API_V1"
                or attributes.get("notificationConfig") != notifications[source]
                or not isinstance(generation, str) or not re.fullmatch(r"[1-9][0-9]*", generation)
                or not isinstance(event_time, str) or not re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)", event_time)):
            raise ValueError
        source_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        document = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_json_object,
                              parse_constant=_invalid_json_constant)
        if (not isinstance(document, dict) or document.get("kind") != "storage#object"
                or document.get("bucket") != configured[source] or document.get("name") != key
                or document.get("generation") != generation):
            raise ValueError
        return (ObjectEvent(source, key, operation, datetime.now(timezone.utc), source_time),)
    except (ValueError, TypeError, UnicodeError, OverflowError, RecursionError, FatalExternalError):
        raise ValueError("gcp_invalid_notification") from None


_PUBSUB_CLIENT_PROVENANCE = WeakKeyDictionary()


def create_pubsub_clients(*, credentials=None):
    """Create the two pinned generated clients with SDK default secure gRPC transports.

    No endpoint, transport or channel injection is accepted. Provenance protects
    trusted Python callers from unsafe configuration, not deliberate SDK/registry
    monkeypatching. Omit credentials for the SDK's normal VM credential discovery.
    """
    if os.environ.get("PUBSUB_EMULATOR_HOST"):
        raise ValueError("gcp_pubsub_invalid_configuration")
    try:
        clients = (SubscriberClient(credentials=credentials), PublisherClient(credentials=credentials))
        for client in clients:
            transport = client.transport
            _PUBSUB_CLIENT_PROVENANCE[client] = (transport, transport.grpc_channel, transport._logged_channel)
        if not _pubsub_client(clients[0], SubscriberClient) or not _pubsub_client(clients[1], PublisherClient):
            raise ValueError
        return clients
    except Exception:
        raise ValueError("gcp_pubsub_invalid_configuration") from None


def _pubsub_client(client, expected):
    # High-level clients add background batching/lease threads and are not this boundary.
    if type(client) is not expected:
        return False
    provenance = _PUBSUB_CLIENT_PROVENANCE.get(client)
    transport = SubscriberGrpcTransport if expected is SubscriberClient else PublisherGrpcTransport
    return (provenance is not None and provenance[0] is client.transport
            and provenance[1] is client.transport.grpc_channel and provenance[2] is client.transport._logged_channel
            and not os.environ.get("PUBSUB_EMULATOR_HOST")
            and type(client.transport) is transport and not client.transport._ignore_credentials
            and client.transport._host in {"pubsub.googleapis.com:443", "pubsub.mtls.googleapis.com:443"})


def _pubsub_identity(value, collection):
    if not isinstance(value, str) or not re.fullmatch(
            rf"projects/(?:[a-z][a-z0-9-]{{4,28}}[a-z0-9]|[0-9]+)/{collection}/[A-Za-z][A-Za-z0-9._~+%-]{{2,254}}", value):
        raise ValueError("gcp_pubsub_invalid_configuration")
    return value


def _protobuf_size(message):
    return type(message).pb(message).ByteSize()


@dataclass(repr=False)
class _PubSubPending:
    receipt: str
    message: pubsub.PubsubMessage
    retained_bytes: int
    events: tuple
    lease_until: float
    quarantined: bool = False


class PubSubEventSource:
    """Clients from create_pubsub_clients; one source budget, stop/replay on uncertainty.

    Standard Pub/Sub ACK/renew success is not fencing or proof of ownership of
    expired receipts. Local evidence therefore expires from the RPC start.
    """

    def __init__(self, subscriber, subscription, publisher, quarantine_topic, sources,
                 notification_configs, *, source_topic, deadline=None, max_pending=20,
                 max_pending_bytes=20_000_000):
        try:
            if not _pubsub_client(subscriber, SubscriberClient) or not _pubsub_client(publisher, PublisherClient):
                raise ValueError
            self._subscription = _pubsub_identity(subscription, "subscriptions")
            self._source_topic = _pubsub_identity(source_topic, "topics")
            self._quarantine_topic = _pubsub_identity(quarantine_topic, "topics")
            if source_topic == quarantine_topic:
                raise ValueError
            self._sources = _sources(sources)
            self._notifications = _notification_configs(self._sources, notification_configs)
            if (type(max_pending) is not int or not 1 <= max_pending <= 20
                    or type(max_pending_bytes) is not int or not 1 <= max_pending_bytes <= 20_000_000
                    or deadline is not None and (not isinstance(deadline, OperationDeadline) or deadline.seconds > 30)):
                raise ValueError
        except (AttributeError, ValueError, TypeError):
            raise ValueError("gcp_pubsub_invalid_configuration") from None
        self._subscriber, self._publisher = subscriber, publisher
        # The pinned gRPC interceptor logs whole receipts/metadata at DEBUG.
        for client in (subscriber, publisher):
            client.transport._interceptor.intercept_unary_unary = (
                lambda continuation, details, request: continuation(details, request))
        self._deadline = OperationDeadline(seconds=30) if deadline is None else deadline
        self._max_pending, self._max_pending_bytes = max_pending, max_pending_bytes
        self._pending = {}
        self._last_clock = None
        with self._deadline.operation():
            actual = self._rpc(subscriber.get_subscription, pubsub.GetSubscriptionRequest(subscription=subscription))
            if (not isinstance(actual, pubsub.Subscription) or actual.name != subscription or actual.topic != source_topic
                    or actual.ack_deadline_seconds != PUBSUB_INITIAL_ACK_DEADLINE_SECONDS):
                raise FatalExternalError("gcp_pubsub_unapproved_subscription")
            self._subscription_lease = PUBSUB_INITIAL_ACK_DEADLINE_SECONDS

    def _check(self):
        self._deadline.check()
        now = self._deadline._last
        if self._last_clock is not None and now < self._last_clock:
            raise RetryableExternalError("gcp_pubsub_clock_reversed")
        self._last_clock = now
        if any(now >= item.lease_until for item in self._pending.values()):
            raise RetryableExternalError("gcp_pubsub_lease_unsafe")
        return now

    def _rpc(self, operation, request):
        self._check()
        if not _pubsub_client(self._subscriber, SubscriberClient) or not _pubsub_client(self._publisher, PublisherClient):
            raise FatalExternalError("gcp_pubsub_unapproved_endpoint")
        try:
            return operation(request=request, retry=None, timeout=self._deadline.rpc_timeout())
        except (api_errors.TooManyRequests, api_errors.ServerError, api_errors.DeadlineExceeded,
                auth_errors.TransportError, RequestException, OSError):
            raise RetryableExternalError("gcp_pubsub_transport_failure") from None
        except (auth_errors.GoogleAuthError, api_errors.GoogleAPICallError):
            raise FatalExternalError("gcp_pubsub_auth_or_configuration_failure") from None
        except (FatalExternalError, RetryableExternalError):
            raise
        except Exception:
            raise FatalExternalError("gcp_pubsub_rpc_failure") from None
        finally:
            self._check()

    @staticmethod
    def _lease(lease_seconds):
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 600:
            raise ValueError("gcp_pubsub_invalid_lease")

    def _owned(self, pending):
        now = self._check()
        if any(now >= item.lease_until for item in pending):
            raise RetryableExternalError("gcp_pubsub_lease_unsafe")
        return now

    def _entries(self, deliveries):
        if (not isinstance(deliveries, (tuple, list)) or len(deliveries) > self._max_pending
                or any(not isinstance(item, Delivery) or not isinstance(item.handle, str) for item in deliveries)):
            raise ValueError("gcp_invalid_delivery_batch")
        handles = [item.handle for item in deliveries]
        if len(set(handles)) != len(handles) or any(
                item.handle not in self._pending or item.events != self._pending[item.handle].events for item in deliveries):
            raise ValueError("gcp_invalid_delivery_batch")
        return [self._pending[handle] for handle in handles]

    def _ack_requests(self, pending, lease_seconds=None):
        kind = pubsub.AcknowledgeRequest if lease_seconds is None else pubsub.ModifyAckDeadlineRequest
        kwargs = {} if lease_seconds is None else {"ack_deadline_seconds": lease_seconds}
        chunks, current = [], []
        for item in pending:
            request = kind(subscription=self._subscription, ack_ids=[p.receipt for p in (*current, item)], **kwargs)
            if _protobuf_size(request) > PUBSUB_ACK_BYTES:
                if not current:
                    raise FatalExternalError("gcp_pubsub_ack_request_capacity_exceeded")
                chunks.append((current, kind(subscription=self._subscription, ack_ids=[p.receipt for p in current], **kwargs)))
                current = [item]
                request = kind(subscription=self._subscription, ack_ids=[item.receipt], **kwargs)
                if _protobuf_size(request) > PUBSUB_ACK_BYTES:
                    raise FatalExternalError("gcp_pubsub_ack_request_capacity_exceeded")
            else:
                current.append(item)
        if current:
            chunks.append((current, kind(subscription=self._subscription, ack_ids=[p.receipt for p in current], **kwargs)))
        return chunks

    def receive(self, max_messages, wait_seconds, lease_seconds):
        self._lease(lease_seconds)
        if (type(max_messages) is not int or not 1 <= max_messages <= 1000
                or type(wait_seconds) not in (int, float) or not math.isfinite(wait_seconds) or not 0 <= wait_seconds <= 30):
            raise ValueError("gcp_pubsub_invalid_receive_bounds")
        with self._deadline.operation():
            capacity = min(max_messages, self._max_pending - len(self._pending))
            if capacity <= 0:
                raise FatalExternalError("gcp_pubsub_pending_capacity_exceeded")
            started = self._check()
            response = self._rpc(self._subscriber.pull, pubsub.PullRequest(subscription=self._subscription, max_messages=capacity))
            if (not isinstance(response, pubsub.PullResponse) or _protobuf_size(response) > PUBSUB_RESPONSE_BYTES
                    or len(response.received_messages) > capacity):
                raise FatalExternalError("gcp_pubsub_invalid_pull_response")
            messages = response.received_messages
            receipts = [item.ack_id for item in messages]
            if len(set(receipts)) != len(receipts) or set(receipts) & {item.receipt for item in self._pending.values()}:
                raise FatalExternalError("gcp_pubsub_reused_ack_id")
            prepared = []
            for item in messages:
                try:
                    _exact_text(item.ack_id)
                    if not pubsub.ReceivedMessage.pb(item).HasField("message"):
                        raise ValueError
                    _exact_text(item.message.message_id)
                    if not pubsub.PubsubMessage.pb(item.message).HasField("publish_time"):
                        raise ValueError
                    item.message.publish_time.astimezone(timezone.utc)
                    _attributes(item.message.attributes)
                except (AttributeError, ValueError, TypeError, UnicodeError, FatalExternalError):
                    raise FatalExternalError("gcp_pubsub_invalid_message") from None
                prepared.append(_PubSubPending(item.ack_id, pubsub.PubsubMessage(item.message),
                                               _protobuf_size(item), (), started + self._subscription_lease))
            if sum(item.retained_bytes for item in (*self._pending.values(), *prepared)) > self._max_pending_bytes:
                raise FatalExternalError("gcp_pubsub_pending_capacity_exceeded")
            # Validate both possible receipt request sizes before admitting any input.
            self._ack_requests(prepared)
            self._ack_requests(prepared, lease_seconds)
            deliveries, malformed = [], []
            for item in prepared:
                handle = uuid4().hex
                if handle in self._pending:
                    raise FatalExternalError("gcp_pubsub_delivery_handle_collision")
                try:
                    item.events = decode_event(item.message.data, item.message.attributes, self._sources, self._notifications)
                except ValueError:
                    malformed.append(Delivery(handle, ()))
                else:
                    deliveries.append(Delivery(handle, item.events))
                self._pending[handle] = item
            all_deliveries = tuple(deliveries + malformed)
            self.renew(all_deliveries, lease_seconds)
            # Every original receipt must have survived the complete initial renewal.
            if prepared and self._check() >= started + self._subscription_lease:
                raise RetryableExternalError("gcp_pubsub_lease_unsafe")
            if malformed:
                self.dead_letter(malformed, "gcp_invalid_notification")
                self.acknowledge(malformed)
            self._owned(self._entries(deliveries))
            return tuple(deliveries)

    def renew(self, deliveries, lease_seconds):
        self._lease(lease_seconds)
        pending = self._entries(deliveries)
        with self._deadline.operation():
            for chunk, request in self._ack_requests(pending, lease_seconds):
                started = self._owned(pending)
                result = self._rpc(self._subscriber.modify_ack_deadline, request)
                self._owned(pending)
                if result is not None:
                    raise FatalExternalError("gcp_pubsub_invalid_renew_response")
                for item in chunk:
                    item.lease_until = started + lease_seconds
            self._owned(pending)

    def acknowledge(self, deliveries):
        pending = self._entries(deliveries)
        if any(not item.events and not item.quarantined for item in pending):
            raise FatalExternalError("gcp_pubsub_quarantine_unconfirmed")
        with self._deadline.operation():
            for chunk, request in self._ack_requests(pending):
                self._owned(pending)
                result = self._rpc(self._subscriber.acknowledge, request)
                self._owned(pending)
                if result is not None:
                    raise FatalExternalError("gcp_pubsub_invalid_ack_response")
                handles = [delivery.handle for delivery in deliveries if self._pending.get(delivery.handle) in chunk]
                for handle in handles:
                    del self._pending[handle]

    def dead_letter(self, deliveries, reason):
        pending = self._entries(deliveries)
        try:
            _exact_text(reason)
        except FatalExternalError:
            raise ValueError("gcp_pubsub_invalid_quarantine_reason") from None
        with self._deadline.operation():
            outbound = []
            for item in pending:
                if not item.quarantined:
                    envelope = {"data_base64": base64.b64encode(item.message.data).decode("ascii"),
                                "attributes": dict(item.message.attributes), "reason": reason,
                                "message_id": item.message.message_id,
                                "publish_time": item.message.publish_time.isoformat()}
                    data = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    outbound.append((item, pubsub.PubsubMessage(data=data)))
            if not outbound:
                self._owned(pending)
                return
            request = pubsub.PublishRequest(topic=self._quarantine_topic, messages=[message for _, message in outbound])
            if len(outbound) > 1000 or _protobuf_size(request) > PUBSUB_RESPONSE_BYTES:
                raise FatalExternalError("gcp_pubsub_publish_capacity_exceeded")
            self._owned(pending)
            result = self._rpc(self._publisher.publish, request)
            self._owned(pending)
            if (not isinstance(result, pubsub.PublishResponse) or len(result.message_ids) != len(outbound)
                    or len(set(result.message_ids)) != len(outbound)
                    or any(not value for value in result.message_ids)):
                raise RetryableExternalError("gcp_pubsub_publish_unconfirmed")
            for item, _ in outbound:
                item.quarantined = True


class GcsObjectStore:
    def __init__(self, client, sources, deadline=None, max_list_pages=4):
        self._client, self._sources = client, _sources(sources)
        if not _canonical_endpoint(client):
            raise ValueError("require the canonical GCS HTTPS endpoint")
        if type(max_list_pages) is not int or not 1 <= max_list_pages <= 4:
            raise ValueError("LIST page budget must be between 1 and 4")
        self._max_list_pages = max_list_pages
        self._deadline = OperationDeadline() if deadline is None else deadline
        # The pinned SDK cache starts background requests with default retries.
        client._bucket_metadata_cache = None
        # Auth transport otherwise replays 401 responses even with SDK retry=None.
        if isinstance(client._http, AuthorizedSession):
            client._http._max_refresh_attempts = 0

    def _bucket(self, source_id):
        if not isinstance(source_id, str) or source_id not in self._sources:
            raise FatalExternalError("gcp_unconfigured_source")
        if not _canonical_endpoint(self._client):
            raise FatalExternalError("gcp_untrusted_storage_endpoint")
        name = self._sources[source_id]
        bucket = self._client.bucket(name)
        if bucket.name != name:
            raise FatalExternalError("gcp_bucket_mismatch")
        return bucket

    def _call(self, operation, *args, allow_not_found=False, **kwargs):
        timeout = self._deadline.rpc_timeout()
        try:
            return operation(*args, retry=None, timeout=timeout, **kwargs)
        except api_errors.NotFound:
            if allow_not_found:
                raise
            raise FatalExternalError("gcp_status_404") from None
        except api_errors.GoogleAPICallError as error:
            status = error.code
            kind = RetryableExternalError if status is not None and (status in (408, 429) or status >= 500) else FatalExternalError
            raise kind(f"gcp_status_{int(status)}" if status is not None else "gcp_api_failure") from None
        except (ValueError, TypeError, AttributeError):
            raise FatalExternalError("gcp_invalid_response") from None
        except (RequestException, auth_errors.TransportError, TimeoutError, ConnectionError):
            raise RetryableExternalError("gcp_transport_failure") from None
        except auth_errors.GoogleAuthError:
            raise FatalExternalError("gcp_auth_failure") from None
        finally:
            self._deadline.check()

    def head_exists(self, source_id, key):
        with self._deadline.operation():
            bucket = self._bucket(source_id)
            key = _exact_text(key)
            # Reloading a reused Blob would select its previously populated generation.
            blob = bucket.blob(key)
            try:
                self._call(blob.reload, allow_not_found=True)
            except api_errors.NotFound:
                self._call(bucket.reload)
                if not isinstance(bucket._properties, dict) or bucket._properties.get("name") != self._sources[source_id]:
                    raise FatalExternalError("gcp_bucket_mismatch")
                return False
            if (not isinstance(blob._properties, dict) or blob._properties.get("name") != key
                    or blob._properties.get("bucket") != bucket.name):
                raise FatalExternalError("gcp_object_metadata_mismatch")
            return True

    def prefix_non_empty(self, source_id, prefix):
        with self._deadline.operation():
            bucket = self._bucket(source_id)
            prefix = self._directory(prefix)
            token, seen = None, set()
            for _ in range(self._max_list_pages):
                # max_results=1 can exhaust an iterator despite nextPageToken: reissue it.
                iterator = self._call(
                    self._client.list_blobs, bucket, prefix=prefix, max_results=1, page_size=1,
                    page_token=token, fields="nextPageToken,items(name,bucket)",
                )
                page = self._call_page(iterator)
                raw = page.raw_page
                if not isinstance(raw, dict) or raw.get("prefixes"):
                    raise FatalExternalError("gcp_invalid_list_page")
                items = raw.get("items", [])
                token = raw.get("nextPageToken")
                if "nextPageToken" in raw and (not isinstance(token, str) or not token or token in seen):
                    raise FatalExternalError("gcp_invalid_list_token")
                if not isinstance(items, list) or len(items) > 1:
                    raise FatalExternalError("gcp_invalid_list_page")
                for item in items:
                    if not isinstance(item, dict) or item.get("bucket") != bucket.name:
                        raise FatalExternalError("gcp_invalid_list_object")
                    if not _exact_text(item.get("name")).startswith(prefix):
                        raise FatalExternalError("gcp_out_of_prefix_object")
                if items:
                    return True
                if token is None:
                    return False
                seen.add(token)
            raise RetryableExternalError("gcp_list_page_budget_exceeded")

    def _call_page(self, iterator):
        # Refresh the timeout at the lazy request, after local iterator construction.
        def fetch(**kwargs):
            iterator.api_request = partial(iterator.api_request, **kwargs)
            return next(iterator.pages)

        try:
            return self._call(fetch)
        except StopIteration:
            raise FatalExternalError("gcp_missing_list_page") from None

    @staticmethod
    def _directory(prefix):
        prefix = _exact_text(prefix)
        if not prefix.endswith("/"):
            raise FatalExternalError("gcp_invalid_partition_directory")
        return prefix

    def location_uri(self, source_id, prefix):
        with self._deadline.operation():
            bucket = self._bucket(source_id)
            return _hadoop_location(bucket.name, prefix)


def _hadoop_location(bucket, prefix):
    prefix = GcsObjectStore._directory(prefix)[:-1]
    if any(segment in ("", ".", "..") for segment in prefix.split("/")):
        raise FatalExternalError("gcp_invalid_partition_directory")
    # Hive/Spark consume unescaped Hadoop Path text; percent escapes change the key.
    return f"gs://{bucket}/{prefix}"


HMS_OWNER_KEY = "gcp_probe_run_id"


class _HmsSocket(TSocket.TSocket):
    def open(self):
        if self.handle is not None:
            raise TTransport.TTransportException(
                type=TTransport.TTransportException.ALREADY_OPEN, message="gcp_hms_connection_already_open")
        # Thrift's resolver/retry open logs raw failures before the adapter can sanitize them.
        handle = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            handle.settimeout(self._timeout)
            handle.connect((self.host, self.port))
            self.handle = handle
        finally:
            if self.handle is None:
                with suppress(Exception):
                    handle.close()


def _hms_connection(host, port):
    sock = _HmsSocket(host, port, socket_family=socket.AF_INET)
    transport = TTransport.TBufferedTransport(sock)
    client = ThriftHiveMetastore.Client(TBinaryProtocol.TBinaryProtocol(transport))
    return sock, transport, client


class HiveMetastorePartitionCatalog(PartitionCatalog):
    """Metadata-only mutations of one approved existing run-owned external table.

    Pass the storage adapter's OperationDeadline and enter its outer operation()
    around PMCP. Connections are dedicated, bounded, and never retried or reused.
    """

    def __init__(self, endpoint, *, table_id, source_id, bucket, root_prefix,
                 partition_columns, owner, deadline=None, connection_factory=None):
        try:
            match = re.fullmatch(r"thrift://([0-9.]+):9083", _exact_text(endpoint))
            if match is None:
                raise ValueError
            host = match[1]
            address = ipaddress.IPv4Address(host)
            if not any(address in ipaddress.IPv4Network(network) for network in
                       ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")):
                raise ValueError
            identity = _exact_text(table_id).split(".")
            if len(identity) != 2 or not all(re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", part) for part in identity):
                raise ValueError
            _exact_text(source_id)
            _sources({source_id: bucket})
            _exact_text(owner)
            root_location = _hadoop_location(bucket, root_prefix)
            if not isinstance(partition_columns, (tuple, list)) or any(
                    not isinstance(column, (tuple, list)) for column in partition_columns):
                raise ValueError
            columns = tuple(tuple(column) for column in partition_columns)
            if not columns or any(len(column) != 2 for column in columns):
                raise ValueError
            for name, kind in columns:
                _exact_text(name)
                _exact_text(kind)
            definition = TableDefinition(table_id, source_id, root_prefix, tuple(name for name, _ in columns))
        except (ValueError, TypeError, FatalExternalError):
            raise ValueError("gcp_hms_invalid_configuration") from None
        if deadline is not None and not isinstance(deadline, OperationDeadline):
            raise ValueError("gcp_hms_invalid_deadline")
        if connection_factory is not None and not callable(connection_factory):
            raise ValueError("gcp_hms_invalid_connection_factory")
        self._host, self._identity = host, tuple(identity)
        self._definition, self._columns = definition, columns
        self._bucket, self._root_location, self._owner = bucket, root_location, owner
        self._deadline = OperationDeadline() if deadline is None else deadline
        self._connection_factory = _hms_connection if connection_factory is None else connection_factory
        self._descriptor = self._partition_keys = None

    def _rpc(self, sock, operation, *args, expected=()):
        try:
            sock.setTimeout(self._deadline.rpc_timeout() * 1000)
            return operation(*args)
        except expected:
            raise
        except TTransport.TTransportException as error:
            kind = RetryableExternalError if error.type in (
                TTransport.TTransportException.UNKNOWN, TTransport.TTransportException.NOT_OPEN,
                TTransport.TTransportException.TIMED_OUT, TTransport.TTransportException.END_OF_FILE,
            ) else FatalExternalError
            raise kind("gcp_hms_transport_failure") from None
        except OSError:
            raise RetryableExternalError("gcp_hms_transport_failure") from None
        except (FatalExternalError, RetryableExternalError):
            raise
        except Exception:
            raise FatalExternalError("gcp_hms_rpc_failure") from None
        finally:
            self._deadline.check()

    @contextmanager
    def _connection(self):
        self._deadline.check()
        try:
            sock, transport, client = self._connection_factory(self._host, 9083)
        except (TTransport.TTransportException, OSError):
            raise RetryableExternalError("gcp_hms_transport_failure") from None
        except Exception:
            raise FatalExternalError("gcp_hms_invalid_connection") from None
        try:
            self._rpc(sock, transport.open)
            yield sock, client
        finally:
            # A failed transport is discarded; close must not mask its original error.
            with suppress(Exception):
                transport.close()
            self._deadline.check()

    def _guard(self, sock, client):
        db, name = self._identity
        database = self._rpc(sock, client.get_database, db)
        table = self._rpc(sock, client.get_table, db, name)
        try:
            if (not isinstance(database, ttypes.Database) or database.name != db
                    or not isinstance(database.parameters, dict) or database.parameters.get(HMS_OWNER_KEY) != self._owner
                    or not isinstance(table, ttypes.Table) or (table.dbName, table.tableName) != self._identity
                    or table.tableType != "EXTERNAL_TABLE" or not isinstance(table.parameters, dict)
                    or table.parameters.get(HMS_OWNER_KEY) != self._owner
                    or table.parameters.get("EXTERNAL") != "TRUE"
                    or table.parameters.get("external.table.purge") != "false"
                    or not isinstance(table.sd, ttypes.StorageDescriptor) or table.sd.location != self._root_location
                    or not isinstance(table.partitionKeys, list)
                    or any(not isinstance(key, ttypes.FieldSchema) for key in table.partitionKeys)
                    or tuple((key.name, key.type) for key in table.partitionKeys) != self._columns):
                raise ValueError
            if self._descriptor is not None and (table.sd != self._descriptor or table.partitionKeys != self._partition_keys):
                raise ValueError
        except (AttributeError, TypeError, ValueError):
            raise FatalExternalError("gcp_hms_unapproved_table") from None
        self._deadline.check()
        return table

    def resolve_tables(self, table_ids):
        if not isinstance(table_ids, (list, tuple)) or tuple(table_ids) != (self._definition.table_id,):
            raise FatalExternalError("gcp_hms_unapproved_table")
        with self._deadline.operation():
            with self._connection() as (sock, client):
                table = self._guard(sock, client)
                if self._descriptor is None:
                    self._descriptor = deepcopy(table.sd)
                    self._partition_keys = deepcopy(table.partitionKeys)
        return (self._definition,)

    def _partition(self, partition):
        try:
            if (self._descriptor is None or partition.table_id != self._definition.table_id
                    or partition.source_id != self._definition.source_id or not isinstance(partition.values, tuple)
                    or len(partition.values) != len(self._columns)):
                raise ValueError
            for value in partition.values:
                if "/" in _exact_text(value):
                    raise ValueError
            path = "/".join(name + "=" + value for (name, _), value in zip(self._columns, partition.values))
            prefix = self._definition.root_prefix + path + "/"
            if partition.partition_path != path or partition.storage_prefix != prefix:
                raise ValueError
            return _hadoop_location(self._bucket, prefix)
        except (AttributeError, TypeError, ValueError, FatalExternalError):
            raise FatalExternalError("gcp_hms_invalid_partition_identity") from None

    def _get_partition(self, sock, client, partition, location):
        try:
            actual = self._rpc(sock, client.get_partition, *self._identity, list(partition.values),
                               expected=(ttypes.NoSuchObjectException,))
        except ttypes.NoSuchObjectException:
            # NoSuchObject can also mean table loss; absence requires trusted guards.
            self._guard(sock, client)
            return None
        self._guard(sock, client)
        descriptor = deepcopy(self._descriptor)
        descriptor.location = location
        if (not isinstance(actual, ttypes.Partition) or (actual.dbName, actual.tableName) != self._identity
                or actual.values != list(partition.values) or actual.sd != descriptor
                or not isinstance(actual.parameters, dict) or actual.parameters.get(HMS_OWNER_KEY) != self._owner):
            raise FatalExternalError("gcp_hms_conflicting_partition")
        self._deadline.check()
        return actual

    def _read_partition(self, partition, location):
        with self._connection() as (sock, client):
            self._guard(sock, client)
            return self._get_partition(sock, client, partition, location)

    def partition_exists(self, partition):
        with self._deadline.operation():
            location = self._partition(partition)
            return self._read_partition(partition, location) is not None

    def create_partition(self, partition, location):
        with self._deadline.operation():
            if location != self._partition(partition):
                raise FatalExternalError("gcp_hms_unapproved_partition_location")
            if self._read_partition(partition, location) is not None:
                return False
            descriptor = deepcopy(self._descriptor)
            descriptor.location = location
            # Hive 3.1.2 cannot decode the package's newer writeId wire field.
            outbound = ttypes.Partition(values=list(partition.values), dbName=self._identity[0],
                                        tableName=self._identity[1], sd=descriptor,
                                        parameters={HMS_OWNER_KEY: self._owner}, writeId=None)
            created = True
            with self._connection() as (sock, client):
                self._guard(sock, client)
                try:
                    self._rpc(sock, client.add_partition, outbound, expected=(ttypes.AlreadyExistsException,))
                except ttypes.AlreadyExistsException:
                    created = False
            if self._read_partition(partition, location) is None:
                raise RetryableExternalError("gcp_hms_add_readback_missing")
            return created

    def delete_partition(self, partition):
        with self._deadline.operation():
            location = self._partition(partition)
            with self._connection() as (sock, client):
                self._guard(sock, client)
                if self._get_partition(sock, client, partition, location) is None:
                    return False
                try:
                    deleted = self._rpc(sock, client.drop_partition, *self._identity, list(partition.values), False,
                                        expected=(ttypes.NoSuchObjectException,))
                except ttypes.NoSuchObjectException:
                    deleted = False
                if type(deleted) is not bool:
                    raise FatalExternalError("gcp_hms_invalid_drop_response")
            if self._read_partition(partition, location) is not None:
                raise RetryableExternalError("gcp_hms_drop_readback_present")
            return deleted
