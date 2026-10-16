"""Service Bus ownership/settlement contract; execute these tests in Azure."""

from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.exceptions import (
    MessageAlreadySettled, MessageLockLostError, OperationTimeoutError,
    ServiceBusAuthenticationError, ServiceBusAuthorizationError, ServiceBusConnectionError,
)

from rtmsf.interfaces import Delivery, FatalExternalError, RetryableExternalError


SOURCES = {"proof": {
    "account": "proofaccount", "container": "proof",
    "topic": "/subscriptions/test-sub/resourceGroups/test-rg/providers/Microsoft.Storage/storageAccounts/proofaccount",
}}


def event_body(api="PutBlob"):
    return json.dumps({
        "id": "event", "topic": SOURCES["proof"]["topic"],
        "subject": "/blobServices/default/containers/proof/blobs/table/dt=a/file.parquet",
        "eventType": "Microsoft.Storage.BlobCreated",
        "data": {"api": api, "blobType": "BlockBlob", "url": "https://proofaccount.blob.core.windows.net/proof/table/dt=a/file.parquet"},
    }).encode()


def message(body=None, body_type=AmqpMessageBodyType.DATA):
    return SimpleNamespace(
        body=event_body() if body is None else body, body_type=body_type,
        lock_token=uuid4(), locked_until_utc=datetime.now(timezone.utc) + timedelta(seconds=300),
    )


class Receiver:
    def __init__(self, messages):
        self.messages = list(messages)
        self.calls = []
        self.failure = None

    def receive_messages(self, **kwargs):
        self.calls.append(("receive", kwargs))
        count = kwargs["max_message_count"]
        result, self.messages = self.messages[:count], self.messages[count:]
        return result

    def _settle(self, operation, item, kwargs):
        self.calls.append((operation, item, kwargs))
        if self.failure is not None:
            raise self.failure

    def complete_message(self, item):
        self._settle("complete", item, {})

    def dead_letter_message(self, item, **kwargs):
        self._settle("dead_letter", item, kwargs)

    def renew_message_lock(self, item, **kwargs):
        self._settle("renew", item, kwargs)
        return datetime.now(timezone.utc) + timedelta(seconds=300)


def source(receiver):
    from rtmsf.adapters.azure import AzureServiceBusEventSource
    return AzureServiceBusEventSource(receiver, SOURCES)


def receive(adapter):
    return adapter.receive(1, 0, 280)


def test_data_fragments_are_fully_decoded_and_same_message_is_completed():
    body = event_body(); item = message([body[:17], body[17:]])
    receiver = Receiver([item]); adapter = source(receiver)
    delivery, = receive(adapter)
    assert delivery.events[0].object_key == "table/dt=a/file.parquet"
    assert isinstance(delivery.handle, str) and delivery.handle
    assert receiver.calls[0][1]["max_message_count"] == 1
    assert 0 < receiver.calls[0][1]["max_wait_time"] <= 1
    adapter.acknowledge((delivery,))
    assert receiver.calls[-1] == ("complete", item, {})
    with pytest.raises((ValueError, FatalExternalError)):
        adapter.acknowledge((delivery,))


def test_native_dlq_is_settled_once_and_then_ack_is_a_local_forget():
    item = message(); receiver = Receiver([item]); adapter = source(receiver)
    deliveries = receive(adapter)
    adapter.dead_letter(deliveries, "external_fatal")
    adapter.dead_letter(deliveries, "external_fatal")
    adapter.renew(deliveries, 280)
    adapter.acknowledge(deliveries)
    assert [call[0] for call in receiver.calls] == ["receive", "dead_letter"]
    assert receiver.calls[-1][1] is item


@pytest.mark.parametrize("body,body_type", [
    (b"invalid JSON", AmqpMessageBodyType.DATA),
    (b"[]", AmqpMessageBodyType.DATA),
    (b"x" * (256 * 1024 + 1), AmqpMessageBodyType.DATA),
    (b"{}", AmqpMessageBodyType.VALUE),
    ([b""] * 1025, AmqpMessageBodyType.DATA),
])
def test_terminal_body_is_not_settled_in_receive_then_native_dlq_on_ack(body, body_type):
    item = message(body, body_type); receiver = Receiver([item]); adapter = source(receiver)
    delivery, = receive(adapter)
    assert delivery.events == ()
    assert [call[0] for call in receiver.calls] == ["receive"]
    adapter.acknowledge((delivery,))
    assert [call[0] for call in receiver.calls] == ["receive", "dead_letter"]
    assert receiver.calls[-1][1] is item


def test_initial_createfile_is_ignored_and_completed_without_dlq():
    receiver = Receiver([message(event_body("CreateFile"))]); adapter = source(receiver)
    deliveries = receive(adapter)
    assert deliveries[0].events == ()
    adapter.acknowledge(deliveries)
    assert [call[0] for call in receiver.calls] == ["receive", "complete"]


@pytest.mark.parametrize("terminal", [False, True])
def test_uncertain_settlement_keeps_original_message_for_retry(terminal):
    item = message(b"invalid" if terminal else None)
    receiver = Receiver([item]); adapter = source(receiver); deliveries = receive(adapter)
    receiver.failure = ServiceBusConnectionError(message="uncertain disposition")
    with pytest.raises(RetryableExternalError):
        adapter.acknowledge(deliveries)
    receiver.failure = None
    adapter.acknowledge(deliveries)
    action = "dead_letter" if terminal else "complete"
    assert [call[0] for call in receiver.calls] == ["receive", action, action]
    assert receiver.calls[-1][1] is item


def test_already_settled_exception_is_not_assumed_to_confirm_our_settlement():
    receiver = Receiver([message()]); adapter = source(receiver); deliveries = receive(adapter)
    receiver.failure = MessageAlreadySettled(action="complete")
    with pytest.raises(FatalExternalError):
        adapter.acknowledge(deliveries)


def test_confirmed_partial_settlement_is_not_repeated_after_later_failure():
    first_item, second_item = message(), message()
    receiver = Receiver([first_item, second_item]); adapter = source(receiver)
    first = receive(adapter); second = receive(adapter)
    adapter.acknowledge(first)
    receiver.failure = ServiceBusConnectionError(message="later receipt failed")
    with pytest.raises(RetryableExternalError):
        adapter.acknowledge(second)
    receiver.failure = None
    adapter.acknowledge(second)
    assert len([call for call in receiver.calls if call[0] == "complete" and call[1] is first_item]) == 1
    assert len([call for call in receiver.calls if call[0] == "complete" and call[1] is second_item]) == 2


def test_fragment_iteration_stops_before_unbounded_empty_fragments():
    consumed = []

    def fragments():
        for index in range(2000):
            consumed.append(index)
            yield b""

    receiver = Receiver([message(fragments())]); adapter = source(receiver)
    deliveries = receive(adapter)
    assert len(consumed) <= 1025
    adapter.acknowledge(deliveries)
    assert receiver.calls[-1][0] == "dead_letter"


@pytest.mark.parametrize("expiry", [None, datetime.now(timezone.utc), datetime.now(timezone.utc) + timedelta(seconds=60)])
def test_missing_expired_or_short_broker_lock_cannot_masquerade_as_worker_lease(expiry):
    item = message(); item.locked_until_utc = expiry
    receiver = Receiver([item]); adapter = source(receiver)
    with pytest.raises((FatalExternalError, RetryableExternalError)):
        receive(adapter)
    assert [call[0] for call in receiver.calls] == ["receive"]


def test_pending_capacity_and_unknown_handles_fail_before_sdk_settlement():
    receiver = Receiver([message() for _ in range(5)]); adapter = source(receiver)
    for _ in range(4):
        receive(adapter)
    with pytest.raises((ValueError, FatalExternalError)):
        receive(adapter)
    with pytest.raises((ValueError, FatalExternalError)):
        adapter.acknowledge((Delivery("unknown", ()),))
    assert [call[0] for call in receiver.calls] == ["receive"] * 4


def test_renew_uses_supported_timeout_and_rejects_an_unconfirmed_short_lock():
    item = message(); receiver = Receiver([item]); adapter = source(receiver); deliveries = receive(adapter)
    adapter.renew(deliveries, 280)
    assert receiver.calls[-1] == ("renew", item, {"timeout": 10})
    receiver.renew_message_lock = lambda *args, **kwargs: datetime.now(timezone.utc) + timedelta(seconds=60)
    with pytest.raises((FatalExternalError, RetryableExternalError)):
        adapter.renew(deliveries, 280)


@pytest.mark.parametrize("operation", ["acknowledge", "dead_letter", "renew"])
def test_duplicate_handles_are_rejected_before_any_sdk_call(operation):
    receiver = Receiver([message()]); adapter = source(receiver)
    delivery, = receive(adapter)
    args = (280,) if operation == "renew" else ("external_fatal",) if operation == "dead_letter" else ()
    with pytest.raises((ValueError, FatalExternalError)):
        getattr(adapter, operation)((delivery, delivery), *args)
    assert [call[0] for call in receiver.calls] == ["receive"]


def test_over_return_and_reused_lock_are_rejected_without_settlement():
    item = message(); receiver = Receiver([item]); adapter = source(receiver)
    receive(adapter)
    receiver.messages = [item]
    with pytest.raises((FatalExternalError, RetryableExternalError)):
        receive(adapter)
    receiver.receive_messages = lambda **kwargs: [message(), message()]
    with pytest.raises((FatalExternalError, RetryableExternalError)):
        receive(adapter)
    assert all(call[0] == "receive" for call in receiver.calls)


@pytest.mark.parametrize("lock_token", [None, "", "invalid-token"])
def test_invalid_lock_token_is_not_owned_or_settled(lock_token):
    item = message(); item.lock_token = lock_token
    receiver = Receiver([item]); adapter = source(receiver)
    with pytest.raises((FatalExternalError, RetryableExternalError)):
        receive(adapter)
    assert [call[0] for call in receiver.calls] == ["receive"]


@pytest.mark.parametrize("expiry", [
    datetime.now() + timedelta(seconds=300),
    (datetime.now(timezone.utc) + timedelta(seconds=300)).astimezone(timezone(timedelta(hours=1))),
])
def test_broker_expiry_requires_an_actual_utc_datetime(expiry):
    item = message(); item.locked_until_utc = expiry
    receiver = Receiver([item]); adapter = source(receiver)
    with pytest.raises((FatalExternalError, RetryableExternalError)):
        receive(adapter)
    assert [call[0] for call in receiver.calls] == ["receive"]


@pytest.mark.parametrize("failure,expected", [
    (MessageLockLostError(message="lock lost"), RetryableExternalError),
    (OperationTimeoutError(message="outcome unknown"), RetryableExternalError),
    (ServiceBusAuthenticationError(message="authentication failed"), FatalExternalError),
    (ServiceBusAuthorizationError(message="authorization failed"), FatalExternalError),
])
def test_queue_failure_classification_preserves_unconfirmed_message(failure, expected):
    receiver = Receiver([message()]); adapter = source(receiver); deliveries = receive(adapter)
    receiver.failure = failure
    with pytest.raises(expected):
        adapter.acknowledge(deliveries)
    receiver.failure = None
    adapter.acknowledge(deliveries)
    assert [call[0] for call in receiver.calls] == ["receive", "complete", "complete"]


def test_whole_settlement_budget_stops_before_later_message_and_keeps_confirmed_state(monkeypatch):
    from rtmsf.adapters import azure
    first_item, second_item = message(), message()
    receiver = Receiver([first_item, second_item]); adapter = source(receiver)
    clock = [0.0]; monkeypatch.setattr(azure.time, "monotonic", lambda: clock[0])
    first = receive(adapter); second = receive(adapter)
    original = receiver.dead_letter_message

    def slow_dead_letter(item, **kwargs):
        original(item, **kwargs)
        clock[0] += 81

    receiver.dead_letter_message = slow_dead_letter
    with pytest.raises(RetryableExternalError):
        adapter.dead_letter(first + second, "external_fatal")
    assert [call[1] for call in receiver.calls if call[0] == "dead_letter"] == [first_item]
    adapter.acknowledge(first)
    receiver.dead_letter_message = original
    adapter.dead_letter(second, "external_fatal")
    adapter.acknowledge(second)
    assert [call[0] for call in receiver.calls] == ["receive", "receive", "dead_letter", "dead_letter"]


def test_receive_budget_overrun_is_not_a_delivery_or_settlement(monkeypatch):
    from rtmsf.adapters import azure
    receiver = Receiver([message()]); adapter = source(receiver)
    clock = [0.0]; monkeypatch.setattr(azure.time, "monotonic", lambda: clock[0])
    original = receiver.receive_messages

    def slow_receive(**kwargs):
        result = original(**kwargs)
        clock[0] += 81
        return result

    receiver.receive_messages = slow_receive
    with pytest.raises(RetryableExternalError):
        receive(adapter)
    assert [call[0] for call in receiver.calls] == ["receive"]
