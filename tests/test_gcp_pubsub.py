"""Native synchronous Pub/Sub boundary, using real pinned SDK protobufs."""

import base64
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest
from google.api_core import exceptions as api_errors
from google.auth.credentials import AnonymousCredentials
from google.pubsub_v1.services.publisher import PublisherClient
from google.pubsub_v1.services.publisher.transports.grpc import PublisherGrpcTransport
from google.pubsub_v1.services.subscriber import SubscriberClient
from google.pubsub_v1.services.subscriber.transports.grpc import SubscriberGrpcTransport
from google.pubsub_v1.types import pubsub

from rtmsf.adapters import gcp
from rtmsf.interfaces import Delivery, FatalExternalError, RetryableExternalError


PROJECT = "rtmsf-gcp-lab-20261018-e504"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/rtmsf-native-pull"
TOPIC = f"projects/{PROJECT}/topics/rtmsf-native-ingress"
QUARANTINE = f"projects/{PROJECT}/topics/rtmsf-native-quarantine"
BUCKET = PROJECT + "-fixtures"
SOURCES = {"fixtures": BUCKET}
NOTIFICATION = f"projects/_/buckets/{BUCKET}/notificationConfigs/3"
NOTIFICATIONS = {"fixtures": NOTIFICATION}


@pytest.fixture(autouse=True)
def no_unmocked_gapic_rpc(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("unmocked generated Pub/Sub RPC: tests must remain offline")
    for name in ("get_subscription", "pull", "modify_ack_deadline", "acknowledge"):
        monkeypatch.setattr(SubscriberClient, name, forbidden)
    monkeypatch.setattr(PublisherClient, "publish", forbidden)


def notification(key="gcp-native/dt=a/file+%2F space雪#?.parquet", kind="OBJECT_FINALIZE"):
    attributes = dict(eventType=kind, payloadFormat="JSON_API_V1", bucketId=BUCKET,
                      objectId=key, objectGeneration="17", eventTime="2026-10-19T20:25:00Z",
                      notificationConfig=NOTIFICATION, custom="unchanged")
    data = json.dumps(dict(kind="storage#object", bucket=BUCKET, name=key,
                           generation="17"), ensure_ascii=False).encode()
    return data, attributes


def message(ack="private-ack-id", *, key=None, kind="OBJECT_FINALIZE", data=None, attributes=None):
    body, attrs = notification(kind=kind) if key is None else notification(key, kind)
    return pubsub.ReceivedMessage(ack_id=ack, message=pubsub.PubsubMessage(
        data=body if data is None else data, attributes=attrs if attributes is None else attributes,
        message_id="server-message-id", publish_time=datetime(2026, 10, 19, tzinfo=timezone.utc)))


class Boundary:
    def __init__(self, messages=()):
        self.clock = [0.0]
        self.messages = list(messages)
        self.calls = []
        self.failures = {}
        self.durations = {}
        self.responses = {}
        self.subscriber, self.publisher = gcp.create_pubsub_clients(credentials=AnonymousCredentials())
        for name in ("get_subscription", "pull", "modify_ack_deadline", "acknowledge"):
            setattr(self.subscriber, name, self.operation(name))
        self.publisher.publish = self.operation("publish")

    def operation(self, name):
        def call(request, *, retry, timeout):
            assert retry is None and 0 < timeout <= 15
            self.calls.append((name, request, timeout))
            self.clock[0] += self.durations.get(name, 0)
            failure = self.failures.get(name)
            if callable(failure):
                failure = failure(self)
            if failure is not None:
                raise failure
            if name in self.responses:
                return self.responses[name]
            if name == "get_subscription":
                assert isinstance(request, pubsub.GetSubscriptionRequest)
                return pubsub.Subscription(name=SUBSCRIPTION, topic=TOPIC, ack_deadline_seconds=180)
            if name == "pull":
                assert isinstance(request, pubsub.PullRequest)
                assert "return_immediately" not in {field.name for field, _ in pubsub.PullRequest.pb(request).ListFields()}
                messages, self.messages = self.messages[:request.max_messages], self.messages[request.max_messages:]
                return pubsub.PullResponse(received_messages=messages)
            if name == "publish":
                assert isinstance(request, pubsub.PublishRequest)
                return pubsub.PublishResponse(message_ids=[f"confirmed-{i}" for i in range(len(request.messages))])
            assert isinstance(request, pubsub.ModifyAckDeadlineRequest if name == "modify_ack_deadline"
                              else pubsub.AcknowledgeRequest)
            return None
        return call

    def source(self, **kwargs):
        return gcp.PubSubEventSource(self.subscriber, SUBSCRIPTION, self.publisher, QUARANTINE,
                                    SOURCES, NOTIFICATIONS, source_topic=TOPIC,
                                    deadline=gcp.OperationDeadline(seconds=30, clock=lambda: self.clock[0]),
                                    **kwargs)

    def actions(self):
        return [name for name, _, _ in self.calls]


def receive(source, count=1, lease=180):
    return source.receive(count, 0, lease)


@pytest.mark.parametrize("kind,operation", [("OBJECT_FINALIZE", "CREATE"), ("OBJECT_DELETE", "DELETE"),
                                           ("OBJECT_ARCHIVE", "DELETE")])
@pytest.mark.parametrize("key", ["gcp-native/dt=a/file+%2F space雪#?.parquet", "gcp-native/dt=a/",
                                "gcp-native/dt=a/_SUCCESS", "gcp-native/dt=a//descendant/"])
def test_decoder_preserves_exact_keys_and_three_operations(kind, operation, key):
    data, attributes = notification(key, kind)
    event, = gcp.decode_event(data, attributes, SOURCES, NOTIFICATIONS)
    assert (event.source_id, event.object_key, event.operation) == ("fixtures", key, operation)
    assert event.source_event_time == datetime(2026, 10, 19, 20, 25, tzinfo=timezone.utc)
    assert event.ingested_at.utcoffset().total_seconds() == 0
    assert attributes["custom"] == "unchanged"


@pytest.mark.parametrize("attribute,value", [("eventType", "UNKNOWN"), ("payloadFormat", "NONE"),
    ("bucketId", "other-bucket"), ("objectId", ""), ("objectGeneration", "-1"),
    ("eventTime", "2026-10-19"), ("eventTime", "2026-10-19T20:25:00+01:00"),
    ("notificationConfig", NOTIFICATION + "0")])
def test_decoder_rejects_untrusted_attributes(attribute, value):
    data, attributes = notification(); attributes[attribute] = value
    with pytest.raises(ValueError, match="gcp_invalid_notification"):
        gcp.decode_event(data, attributes, SOURCES, NOTIFICATIONS)


@pytest.mark.parametrize("field,value", [("bucket", "other-bucket"), ("name", "other"),
                                       ("generation", "18"), ("kind", "other"), ("generation", 17)])
def test_decoder_rejects_payload_identity_mismatch(field, value):
    data, attributes = notification(); body = json.loads(data); body[field] = value
    with pytest.raises(ValueError):
        gcp.decode_event(json.dumps(body).encode(), attributes, SOURCES, NOTIFICATIONS)


@pytest.mark.parametrize("attribute", list(notification()[1])[:7])
def test_decoder_requires_notification_attributes(attribute):
    data, attributes = notification(); del attributes[attribute]
    with pytest.raises(ValueError):
        gcp.decode_event(data, attributes, SOURCES, NOTIFICATIONS)


def test_receipts_are_private_and_initial_renewal_precedes_exposure():
    boundary = Boundary([message(), message("second-private-ack")]); source = boundary.source()
    first, second = receive(source, 2)
    assert first.handle != second.handle and first.handle != "private-ack-id"
    assert "private-ack-id" not in repr((first, second, source._pending))
    assert boundary.actions() == ["get_subscription", "pull", "modify_ack_deadline"]
    request = boundary.calls[-1][1]
    assert tuple(request.ack_ids) == ("private-ack-id", "second-private-ack")
    assert request.ack_deadline_seconds == 180
    source.renew((first, second), 180)
    source.acknowledge((first, second))
    assert not source._pending
    with pytest.raises(ValueError, match="gcp_invalid_delivery_batch"):
        source.acknowledge((first,))


def test_random_handle_collision_fails_without_retry_or_exposure(monkeypatch):
    handles = iter(("same-random-handle", "same-random-handle", "another-random-handle"))
    monkeypatch.setattr(gcp, "uuid4", lambda: SimpleNamespace(hex=next(handles)))
    boundary = Boundary([message(), message("other-ack")]); source = boundary.source()
    with pytest.raises(FatalExternalError, match="gcp_pubsub_delivery_handle_collision"):
        receive(source, 2)
    assert boundary.actions() == ["get_subscription", "pull"]


def test_empty_pull_is_distinct_from_ambiguous_deadline():
    boundary = Boundary(); source = boundary.source()
    assert receive(source) == ()
    boundary.failures["pull"] = api_errors.DeadlineExceeded("private-ack-id secret")
    with pytest.raises(RetryableExternalError, match="^gcp_pubsub_transport_failure$") as error:
        receive(source)
    assert "private-ack-id" not in str(error.value)
    assert "acknowledge" not in boundary.actions()


@pytest.mark.parametrize("data", [b"not-json", b"[]", b"", b"\xff"])
def test_malformed_data_is_completely_quarantined_before_ack(data):
    boundary = Boundary([message(data=data)]); source = boundary.source()
    assert receive(source) == ()
    assert boundary.actions() == ["get_subscription", "pull", "modify_ack_deadline", "publish", "acknowledge"]
    envelope = json.loads(boundary.calls[-2][1].messages[0].data)
    assert base64.b64decode(envelope["data_base64"]) == data
    assert envelope["attributes"] == notification()[1]
    assert envelope["reason"] == "gcp_invalid_notification"
    assert envelope["message_id"] == "server-message-id"
    assert "private-ack-id" not in json.dumps(envelope)
    assert not source._pending


def test_quarantine_keeps_100_attributes_reason_collision_and_confirmed_flag():
    data, attributes = notification(); attributes["reason"] = "original-value"
    attributes.update({f"custom-{i}": "雪" for i in range(100 - len(attributes))})
    boundary = Boundary([message(data=data, attributes=attributes)]); source = boundary.source()
    deliveries = receive(source)
    source.dead_letter(deliveries, "catalog_failed")
    source.dead_letter(deliveries, "catalog_failed")
    assert boundary.actions().count("publish") == 1 and "acknowledge" not in boundary.actions()
    envelope = json.loads(boundary.calls[-1][1].messages[0].data)
    assert envelope["attributes"] == attributes and envelope["reason"] == "catalog_failed"
    assert not boundary.calls[-1][1].messages[0].attributes
    source.acknowledge(deliveries)


@pytest.mark.parametrize("response", [pubsub.PublishResponse(), pubsub.PublishResponse(message_ids=[""]),
                                    pubsub.PublishResponse(message_ids=["a", "b"]), object()])
def test_unconfirmed_publication_never_acknowledges(response):
    boundary = Boundary([message(data=b"bad")]); boundary.responses["publish"] = response
    with pytest.raises(RetryableExternalError, match="gcp_pubsub_publish_unconfirmed"):
        receive(boundary.source())
    assert "acknowledge" not in boundary.actions()


def test_duplicate_quarantine_publication_ids_do_not_confirm_distinct_inputs():
    boundary = Boundary([message(data=b"bad-one"), message("other-ack", data=b"bad-two")])
    boundary.responses["publish"] = pubsub.PublishResponse(message_ids=["same-id", "same-id"])
    source = boundary.source()
    with pytest.raises(RetryableExternalError, match="gcp_pubsub_publish_unconfirmed"):
        receive(source, 2)
    assert "acknowledge" not in boundary.actions()
    assert len(source._pending) == 2 and not any(item.quarantined for item in source._pending.values())


def test_ambiguous_publication_never_acknowledges():
    boundary = Boundary([message(data=b"bad")])
    boundary.failures["publish"] = api_errors.DeadlineExceeded("uncertain private-ack-id")
    with pytest.raises(RetryableExternalError):
        receive(boundary.source())
    assert "acknowledge" not in boundary.actions()


def test_quarantine_base64_expansion_over_actual_publish_limit_keeps_source_unacked():
    boundary = Boundary([message(data=b"x" * 7_600_000)]); source = boundary.source()
    with pytest.raises(FatalExternalError, match="gcp_pubsub_publish_capacity_exceeded"):
        receive(source)
    assert "publish" not in boundary.actions() and "acknowledge" not in boundary.actions()


@pytest.mark.parametrize("response", [pubsub.PullResponse(received_messages=[message(), message()]),
    pubsub.PullResponse(received_messages=[message("")]), pubsub.PullResponse(received_messages=[pubsub.ReceivedMessage(ack_id="ack")]),
    pubsub.PullResponse(received_messages=[message(data=b"x" * 10_000_000)]), object()])
def test_invalid_whole_pull_fails_before_any_renewal_or_ack(response):
    boundary = Boundary(); boundary.responses["pull"] = response; source = boundary.source()
    with pytest.raises(FatalExternalError):
        receive(source)
    assert boundary.actions() == ["get_subscription", "pull"]
    assert not source._pending


def test_already_pending_ack_id_fails_without_exposing_second_delivery():
    boundary = Boundary([message(), message()]); source = boundary.source(); receive(source)
    with pytest.raises(FatalExternalError, match="gcp_pubsub_reused_ack_id"):
        receive(source)
    assert boundary.actions().count("modify_ack_deadline") == 1


@pytest.mark.parametrize("max_pending,max_pending_bytes", [(1, 20_000_000), (20, 200)])
def test_whole_response_capacity_is_admitted_before_renewal(max_pending, max_pending_bytes):
    boundary = Boundary([message(), message("other")]); source = boundary.source(
        max_pending=max_pending, max_pending_bytes=max_pending_bytes)
    boundary.responses["pull"] = pubsub.PullResponse(received_messages=[message(), message("other")])
    with pytest.raises(FatalExternalError):
        receive(source, 2)
    assert boundary.actions() == ["get_subscription", "pull"]


@pytest.mark.parametrize("field,value", [("name", SUBSCRIPTION + "x"), ("topic", QUARANTINE),
                                       ("ack_deadline_seconds", 10), ("ack_deadline_seconds", 60),
                                       ("ack_deadline_seconds", 601)])
def test_actual_subscription_identity_and_original_deadline_are_verified(field, value):
    boundary = Boundary(); actual = pubsub.Subscription(name=SUBSCRIPTION, topic=TOPIC, ack_deadline_seconds=180)
    setattr(actual, field, value); boundary.responses["get_subscription"] = actual
    with pytest.raises(FatalExternalError, match="gcp_pubsub_unapproved_subscription"):
        boundary.source()
    assert boundary.actions() == ["get_subscription"]


def test_source_and_quarantine_topic_separation_is_checked():
    boundary = Boundary()
    with pytest.raises(ValueError):
        gcp.PubSubEventSource(boundary.subscriber, SUBSCRIPTION, boundary.publisher, TOPIC,
                             SOURCES, NOTIFICATIONS, source_topic=TOPIC)
    assert not boundary.calls


@pytest.mark.parametrize("endpoint", ["localhost:8085", "other.googleapis.com:443", "http://pubsub.googleapis.com"])
def test_custom_endpoints_are_rejected(endpoint):
    boundary = Boundary(); boundary.subscriber.transport._host = endpoint
    with pytest.raises(ValueError, match="gcp_pubsub_invalid_configuration"):
        boundary.source()


def test_emulator_is_rejected_even_with_canonical_injected_client(monkeypatch):
    boundary = Boundary(); monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")
    with pytest.raises(ValueError):
        boundary.source()


@pytest.mark.parametrize("count,wait,lease", [(0, 0, 180), (1001, 0, 180), (True, 0, 180),
                                           (1, -1, 180), (1, float("nan"), 180), (1, 0, 0), (1, 0, 601)])
def test_receive_bounds_fail_without_pull(count, wait, lease):
    boundary = Boundary(); source = boundary.source()
    with pytest.raises(ValueError):
        source.receive(count, wait, lease)
    assert boundary.actions() == ["get_subscription"]


def test_pull_and_renew_share_elapsed_budget_and_no_expired_receipt_is_exposed():
    boundary = Boundary([message()]); source = boundary.source()
    boundary.durations.update(pull=14, modify_ack_deadline=16)
    with pytest.raises(RetryableExternalError, match="gcp_operation_deadline_exceeded"):
        receive(source)
    assert boundary.calls[-1][2] <= 15 and "acknowledge" not in boundary.actions()


def test_original_receipt_expired_during_successful_renewal_is_rejected():
    boundary = Boundary([message()]); source = boundary.source()
    # A service returning OK to expired IDs is no ownership evidence.
    source._subscription_lease = 10
    boundary.durations.update(pull=5, modify_ack_deadline=6)
    with pytest.raises(RetryableExternalError, match="gcp_pubsub_lease_unsafe"):
        receive(source)
    assert "acknowledge" not in boundary.actions()


def test_lease_is_counted_from_renewal_start_and_expiry_is_not_repaired_by_ok():
    boundary = Boundary([message()]); source = boundary.source()
    boundary.durations["modify_ack_deadline"] = 4
    deliveries = receive(source, lease=5)
    boundary.clock[0] = 5
    with pytest.raises(RetryableExternalError, match="gcp_pubsub_lease_unsafe"):
        source.renew(deliveries, 180)
    assert boundary.actions().count("modify_ack_deadline") == 1


def test_clock_reversal_across_source_invocations_fails_before_network():
    boundary = Boundary([message()]); source = boundary.source(); boundary.clock[0] = 4
    deliveries = receive(source); boundary.clock[0] = 3
    with pytest.raises(RetryableExternalError, match="gcp_pubsub_clock_reversed"):
        source.acknowledge(deliveries)
    assert "acknowledge" not in boundary.actions()


@pytest.mark.parametrize("mode", ["duplicate", "events", "unknown", "wrong_type"])
def test_delivery_batches_validate_handles_and_original_events(mode):
    boundary = Boundary([message()]); source = boundary.source(); delivery, = receive(source)
    batch = {"duplicate": (delivery, delivery), "events": (Delivery(delivery.handle, ()),),
             "unknown": (Delivery("unknown", delivery.events),), "wrong_type": (object(),)}[mode]
    with pytest.raises(ValueError, match="gcp_invalid_delivery_batch"):
        source.acknowledge(batch)
    assert "acknowledge" not in boundary.actions()


def test_large_ack_ids_chunk_requests_by_actual_serialized_bytes():
    boundary = Boundary([message("x" * 260_000), message("y" * 260_000)]); source = boundary.source()
    deliveries = receive(source, 2)
    source.acknowledge(deliveries)
    assert boundary.actions().count("modify_ack_deadline") == 2
    assert boundary.actions().count("acknowledge") == 2
    for name, request, _ in boundary.calls:
        if name in {"modify_ack_deadline", "acknowledge"}:
            assert type(request).pb(request).ByteSize() <= 512_000


def test_unrepresentable_single_ack_id_fails_without_initial_renewal():
    boundary = Boundary([message("x" * 512_000)]); source = boundary.source()
    with pytest.raises(FatalExternalError, match="gcp_pubsub_ack_request_capacity_exceeded"):
        receive(source)
    assert "modify_ack_deadline" not in boundary.actions()


def test_final_initial_renewal_chunk_failure_exposes_nothing():
    boundary = Boundary([message("x" * 260_000), message("y" * 260_000)]); source = boundary.source()
    boundary.failures["modify_ack_deadline"] = lambda b: api_errors.ServiceUnavailable("private") if b.actions().count("modify_ack_deadline") == 2 else None
    with pytest.raises(RetryableExternalError):
        receive(source, 2)
    assert "acknowledge" not in boundary.actions()


def test_cumulative_quarantine_and_ack_chunks_share_receive_budget():
    boundary = Boundary([message(data=b"bad")]); source = boundary.source()
    boundary.durations.update(pull=10, modify_ack_deadline=10, publish=9, acknowledge=2)
    with pytest.raises(RetryableExternalError, match="gcp_operation_deadline_exceeded"):
        receive(source)
    assert boundary.calls[-1][0] == "acknowledge" and boundary.calls[-1][2] <= 1


@pytest.mark.parametrize("error,kind", [(api_errors.PermissionDenied("private"), FatalExternalError),
    (api_errors.Unauthenticated("private"), FatalExternalError), (api_errors.ServiceUnavailable("private"), RetryableExternalError),
    (OSError("private"), RetryableExternalError), (RuntimeError("private"), FatalExternalError)])
def test_rpc_errors_are_sanitized(error, kind):
    boundary = Boundary(); source = boundary.source(); boundary.failures["pull"] = error
    with pytest.raises(kind) as caught:
        receive(source)
    assert "private" not in str(caught.value)


def test_auth_transport_failure_remains_retryable_and_sanitized():
    from google.auth.exceptions import TransportError
    boundary = Boundary(); source = boundary.source()
    boundary.failures["pull"] = TransportError("private transport details")
    with pytest.raises(RetryableExternalError, match="gcp_pubsub_transport_failure"):
        receive(source)


def test_failed_malformed_publication_cannot_be_bypassed_with_an_ack():
    boundary = Boundary([message(data=b"bad")]); source = boundary.source()
    boundary.failures["publish"] = api_errors.DeadlineExceeded("uncertain publication")
    with pytest.raises(RetryableExternalError):
        receive(source)
    handle, = source._pending
    with pytest.raises(FatalExternalError, match="gcp_pubsub_quarantine_unconfirmed"):
        source.acknowledge((Delivery(handle, ()),))
    assert "acknowledge" not in boundary.actions()


@pytest.mark.parametrize("field,value", [("nanos", 1_000_000_001), ("seconds", 253402300800)])
def test_invalid_server_timestamp_is_sanitized_before_admission(field, value):
    item = message(); setattr(pubsub.PubsubMessage.pb(item.message).publish_time, field, value)
    boundary = Boundary([item]); source = boundary.source()
    with pytest.raises(FatalExternalError, match="gcp_pubsub_invalid_message"):
        receive(source)
    assert boundary.actions() == ["get_subscription", "pull"]


def test_source_cannot_share_the_90_second_storage_catalog_deadline():
    boundary = Boundary()
    with pytest.raises(ValueError, match="gcp_pubsub_invalid_configuration"):
        gcp.PubSubEventSource(boundary.subscriber, SUBSCRIPTION, boundary.publisher, QUARANTINE,
            SOURCES, NOTIFICATIONS, source_topic=TOPIC, deadline=gcp.OperationDeadline())
    assert not boundary.calls


@pytest.mark.parametrize("expired_before_pull", [True, False])
def test_existing_pending_receipt_must_remain_safe_during_later_receive(expired_before_pull):
    boundary = Boundary([message(), message("new-receipt")]); source = boundary.source()
    receive(source, lease=5)
    if expired_before_pull:
        boundary.clock[0] = 5
    else:
        boundary.durations.update(pull=3, modify_ack_deadline=3)
    before = boundary.actions().count("pull")
    with pytest.raises(RetryableExternalError, match="gcp_pubsub_lease_unsafe"):
        receive(source)
    assert "acknowledge" not in boundary.actions()
    if expired_before_pull:
        assert boundary.actions().count("pull") == before


def test_generated_wire_logging_is_disabled_per_client_without_global_logger_changes(caplog):
    import logging
    boundary = Boundary(); logger = logging.getLogger("google.pubsub_v1.services.subscriber.transports.grpc")
    original_level, original_handlers = logger.level, tuple(logger.handlers)
    boundary.source()
    assert (logger.level, tuple(logger.handlers)) == (original_level, original_handlers)
    # Invoke the same injected interceptor used by the generated gRPC stubs.
    future = SimpleNamespace(trailing_metadata=lambda: (), result=lambda: None)
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = boundary.subscriber.transport._interceptor.intercept_unary_unary(
            lambda details, request: future, SimpleNamespace(metadata=(), method="fake/ack"),
            pubsub.AcknowledgeRequest(ack_ids=["private-ack-id"]))
    assert result is future
    assert not caplog.records


def test_injected_insecure_channel_is_rejected_even_under_canonical_host():
    import grpc
    boundary = Boundary()
    boundary.subscriber = SubscriberClient(transport=SubscriberGrpcTransport(
        channel=grpc.insecure_channel("pubsub.googleapis.com:443")))
    boundary.subscriber.get_subscription = boundary.operation("get_subscription")
    with pytest.raises(ValueError, match="gcp_pubsub_invalid_configuration"):
        boundary.source()


@pytest.mark.parametrize("client_name,client_type,transport_type", [
    ("subscriber", SubscriberClient, SubscriberGrpcTransport),
    ("publisher", PublisherClient, PublisherGrpcTransport)])
@pytest.mark.parametrize("target", ["localhost:1", "pubsub.googleapis.com:443"])
def test_supplied_channel_callable_is_rejected_without_invoking_any_rpc(client_name, client_type, transport_type, target):
    import grpc
    boundary = Boundary()
    transport = transport_type(credentials=AnonymousCredentials(),
                               channel=lambda *args, **kwargs: grpc.insecure_channel(target))
    client = client_type(transport=transport)
    if client_name == "subscriber":
        client.get_subscription = boundary.operation("get_subscription")
    else:
        client.publish = boundary.operation("publish")
    setattr(boundary, client_name, client)
    with pytest.raises(ValueError, match="gcp_pubsub_invalid_configuration"):
        boundary.source()
    assert not boundary.calls


@pytest.mark.parametrize("client_name,client_type", [("subscriber", SubscriberClient), ("publisher", PublisherClient)])
def test_native_clients_constructed_outside_the_secure_helper_are_rejected(client_name, client_type):
    boundary = Boundary()
    client = client_type(credentials=AnonymousCredentials())
    if client_name == "subscriber":
        client.get_subscription = boundary.operation("get_subscription")
    else:
        client.publish = boundary.operation("publish")
    setattr(boundary, client_name, client)
    with pytest.raises(ValueError, match="gcp_pubsub_invalid_configuration"):
        boundary.source()
    assert not boundary.calls


def test_secure_helper_returns_native_clients_and_allows_offline_credentials():
    subscriber, publisher = gcp.create_pubsub_clients(credentials=AnonymousCredentials())
    boundary = Boundary(); boundary.subscriber, boundary.publisher = subscriber, publisher
    subscriber.get_subscription = boundary.operation("get_subscription")
    boundary.source()
    assert type(subscriber) is SubscriberClient and type(publisher) is PublisherClient
    assert boundary.actions() == ["get_subscription"]


def test_secure_helper_delegates_default_credentials_to_the_sdk(monkeypatch):
    import google.auth
    calls = []
    def default_credentials(*args, **kwargs):
        calls.append(True)
        return AnonymousCredentials(), PROJECT
    monkeypatch.setattr(google.auth, "default", default_credentials)
    subscriber, publisher = gcp.create_pubsub_clients()
    assert type(subscriber) is SubscriberClient and type(publisher) is PublisherClient and len(calls) == 2


def test_secure_helper_rejects_emulator_before_constructing_clients(monkeypatch):
    import google.auth
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "localhost:1")
    monkeypatch.setattr(google.auth, "default", lambda *args, **kwargs: pytest.fail("must reject before ADC"))
    with pytest.raises(ValueError, match="gcp_pubsub_invalid_configuration"):
        gcp.create_pubsub_clients()


@pytest.mark.parametrize("client_name", ["subscriber", "publisher"])
@pytest.mark.parametrize("replacement", ["transport", "channel", "logged_channel"])
@pytest.mark.parametrize("after_source", [False, True])
def test_secure_client_transport_and_channel_identity_cannot_change(client_name, replacement, after_source):
    import grpc
    boundary = Boundary()
    boundary.subscriber, boundary.publisher = gcp.create_pubsub_clients(credentials=AnonymousCredentials())
    boundary.subscriber.get_subscription = boundary.operation("get_subscription")
    boundary.subscriber.pull = boundary.operation("pull")
    source = boundary.source() if after_source else None
    client = getattr(boundary, client_name)
    if replacement == "transport":
        other = SubscriberClient(credentials=AnonymousCredentials()) if client_name == "subscriber" else PublisherClient(credentials=AnonymousCredentials())
        client._transport = other.transport
    elif replacement == "channel":
        client.transport._grpc_channel = grpc.insecure_channel("pubsub.googleapis.com:443")
    else:
        client.transport._logged_channel = grpc.insecure_channel("pubsub.googleapis.com:443")
    before = len(boundary.calls)
    with pytest.raises(FatalExternalError if after_source else ValueError):
        receive(source) if after_source else boundary.source()
    assert len(boundary.calls) == before


@pytest.mark.parametrize("data", [b'{"kind":"storage#object","kind":"bad"}', notification()[0][:-1] + b',"extra":NaN}',
    notification()[0][:-1] + b',"bucket":"' + BUCKET.encode() + b'"}'])
def test_ambiguous_or_nonstandard_json_is_terminal(data):
    _, attributes = notification()
    with pytest.raises(ValueError, match="gcp_invalid_notification"):
        gcp.decode_event(data, attributes, SOURCES, NOTIFICATIONS)


@pytest.mark.parametrize("attributes", [{"x": "v"} | {str(i): "v" for i in range(100)},
                                       {"x" * 257: "v"}, {"x": "雪" * 342}])
def test_attributes_use_utf8_byte_limits_before_admission(attributes):
    boundary = Boundary([message(attributes=attributes)]); source = boundary.source()
    with pytest.raises(FatalExternalError, match="gcp_pubsub_invalid_message"):
        receive(source)
    assert boundary.actions() == ["get_subscription", "pull"]


def test_pending_original_messages_are_snapshots_of_complete_data_and_attributes():
    item = message(); boundary = Boundary([item]); source = boundary.source(); delivery, = receive(source)
    item.message.data = b"changed"
    item.message.attributes["custom"] = "changed"
    source.dead_letter((delivery,), "terminal")
    envelope = json.loads(boundary.calls[-1][1].messages[0].data)
    assert base64.b64decode(envelope["data_base64"]) == notification()[0]
    assert envelope["attributes"]["custom"] == "unchanged"


@pytest.mark.parametrize("name", ["modify_ack_deadline", "acknowledge"])
def test_generated_void_rpcs_reject_nonvoid_protocol_response(name):
    boundary = Boundary([message()]); source = boundary.source(); deliveries = receive(source)
    boundary.responses[name] = object()
    with pytest.raises(FatalExternalError):
        (source.renew(deliveries, 180) if name == "modify_ack_deadline" else source.acknowledge(deliveries))
    assert source._pending


def test_final_chunk_uses_remaining_standalone_budget():
    boundary = Boundary([message("x" * 260_000), message("y" * 260_000)]); source = boundary.source()
    deliveries = receive(source, 2)
    boundary.durations["acknowledge"] = 16
    with pytest.raises(RetryableExternalError, match="gcp_operation_deadline_exceeded"):
        source.acknowledge(deliveries)
    assert [timeout for name, _, timeout in boundary.calls if name == "acknowledge"] == [15, 14]
    assert len(source._pending) == 1


def test_renewal_that_finishes_after_old_lease_cannot_prove_ownership():
    boundary = Boundary([message()]); source = boundary.source(); deliveries = receive(source, lease=5)
    boundary.clock[0] = 4
    boundary.durations["modify_ack_deadline"] = 2
    with pytest.raises(RetryableExternalError, match="gcp_pubsub_lease_unsafe"):
        source.renew(deliveries, 180)


class CurrentStorage:
    def __init__(self):
        self.keys = set()
        self.calls = []

    def head_exists(self, source, key):
        self.calls.append(("head", source, key))
        return key in self.keys

    def prefix_non_empty(self, source, prefix):
        self.calls.append(("list", source, prefix))
        return any(key.startswith(prefix) for key in self.keys)

    def location_uri(self, source, prefix):
        return f"gs://{BUCKET}/{prefix[:-1]}"


class Catalog:
    def __init__(self):
        self.locations = {}
        self.failure = None

    def partition_exists(self, partition):
        if self.failure:
            raise self.failure
        return partition.key in self.locations

    def create_partition(self, partition, location):
        self.locations[partition.key] = location
        return True

    def delete_partition(self, partition):
        return self.locations.pop(partition.key, None) is not None


def worker(boundary, source, storage, catalog, records):
    from rtmsf.models import TableDefinition
    from rtmsf.pmcp import PartitionMutationCommitProtocol
    from rtmsf.routing import RoutingIndex
    from rtmsf.worker import RTMSFWorker
    return RTMSFWorker(source=source, pmcp=PartitionMutationCommitProtocol(storage, catalog),
        telemetry=SimpleNamespace(emit=records.append),
        routing=RoutingIndex((TableDefinition("native.objects", "fixtures", "gcp-native/", ("dt",)),)),
        settling_ms=0, maximum_ms=1, queue_lease_seconds=180, pmcp_budget_ms=90_000,
        source_call_budget_ms=30_000, clock_ms=lambda: int(boundary.clock[0] * 1000),
        sleep_ms=lambda delay: boundary.clock.__setitem__(0, boundary.clock[0] + delay / 1000),
        poll_wait_seconds=0, max_pending_deliveries=20)


def test_existing_worker_lifecycle_duplicates_stale_generation_and_marker_truth():
    key = "gcp-native/dt=a/file+%2F space雪#?.parquet"
    boundary = Boundary([message(key=key), message("duplicate", key=key)])
    source = boundary.source(); storage = CurrentStorage(); storage.keys = {key}
    catalog = Catalog(); records = []; runtime = worker(boundary, source, storage, catalog, records)
    runtime.run_once()
    assert len(catalog.locations) == 1 and boundary.actions().count("acknowledge") == 2
    assert storage.calls == [("head", "fixtures", key)]
    # Late DELETE/ARCHIVE generation 17 sees a currently live replacement at the same name.
    for index, kind in enumerate(("OBJECT_DELETE", "OBJECT_ARCHIVE")):
        boundary.messages = [message(f"stale-{index}", key=key, kind=kind)]
        runtime.run_once()
        assert len(catalog.locations) == 1
    marker = "gcp-native/dt=a/"
    storage.keys = {marker}
    boundary.messages = [message("partial-delete", key=key, kind="OBJECT_DELETE")]
    runtime.run_once()
    assert len(catalog.locations) == 1
    storage.keys.clear()
    boundary.messages = [message("last-delete", key=marker, kind="OBJECT_DELETE")]
    runtime.run_once()
    assert not catalog.locations
    storage.keys = {marker}
    boundary.messages = [message("marker-create", key=marker)]
    runtime.run_once()
    assert len(catalog.locations) == 1
    assert "private-ack-id" not in repr(records)
    assert all(len(call) == 3 for call in storage.calls)  # no generation-selected HEAD/LIST


def test_existing_worker_terminal_catalog_failure_publishes_before_ack():
    boundary = Boundary([message()]); source = boundary.source(); storage = CurrentStorage()
    catalog = Catalog(); catalog.failure = FatalExternalError("private implementation detail")
    records = []; runtime = worker(boundary, source, storage, catalog, records)
    runtime.run_once()
    assert boundary.actions()[-2:] == ["publish", "acknowledge"]
    envelope = json.loads(boundary.calls[-2][1].messages[0].data)
    assert envelope["reason"] == "external_fatal" and "private-ack-id" not in repr(records)


def test_existing_worker_stops_catalog_work_when_initial_renewal_is_uncertain():
    boundary = Boundary([message()]); source = boundary.source(); storage = CurrentStorage()
    catalog = Catalog(); boundary.failures["modify_ack_deadline"] = api_errors.DeadlineExceeded("ambiguous")
    runtime = worker(boundary, source, storage, catalog, [])
    with pytest.raises(RetryableExternalError):
        runtime.run_once()
    assert not storage.calls and not catalog.locations and "acknowledge" not in boundary.actions()
