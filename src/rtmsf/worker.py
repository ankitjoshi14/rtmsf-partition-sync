"""Provider-neutral RTMSF worker orchestration."""

from dataclasses import dataclass
import math
import random
import time
from typing import Callable, Sequence

from rtmsf.interfaces import Delivery, EventSource, TelemetrySink
from rtmsf.routing import RoutingIndex
from rtmsf.woera import PartitionKey, WoeraBuffer


@dataclass
class _PendingDelivery:
    delivery: Delivery
    partition_keys: set[PartitionKey]
    lease_until_ms: int
    dead_letter_reason: str | None = None
    dead_lettered: bool = False


class RTMSFWorker:
    """Keep receipts owned until every intent and durable terminal action finishes.

    Provider budgets must include all SDK calls and batch chunks in one adapter
    invocation. A budget overrun or unsafe remaining lease terminates the run;
    the process must restart and replay its unacknowledged deliveries.
    """

    def __init__(
        self, *, source: EventSource, pmcp, telemetry: TelemetrySink,
        routing: RoutingIndex, settling_ms: int, maximum_ms: int,
        queue_lease_seconds: int, pmcp_budget_ms: int, source_call_budget_ms: int,
        clock_ms: Callable[[], int] | None = None,
        sleep_ms: Callable[[int], None] | None = None,
        retry_base_ms: int = 1000, retry_max_ms: int = 60_000,
        max_retries: int = 5, jitter_ms: Callable[[int], int] | None = None,
        poll_size: int = 10, poll_wait_seconds: int = 20,
        lease_safety_margin_ms: int = 1000, max_pending_deliveries: int = 100,
        max_pending_partitions: int = 1000, max_pending_events: int = 10_000,
    ) -> None:
        positive = (
            maximum_ms, queue_lease_seconds, pmcp_budget_ms, source_call_budget_ms,
            retry_base_ms, retry_max_ms, poll_size, max_pending_deliveries,
            max_pending_partitions, max_pending_events,
        )
        nonnegative = (settling_ms, max_retries, poll_wait_seconds, lease_safety_margin_ms)
        if any(type(value) is not int or value <= 0 for value in positive):
            raise ValueError("budgets, limits, and retry bounds must be positive integers")
        if any(type(value) is not int or value < 0 for value in nonnegative):
            raise ValueError("settling, retries, polling, and margin must be nonnegative integers")
        if maximum_ms < settling_ms or retry_max_ms < retry_base_ms:
            raise ValueError("maximum time bounds must cover their minimums")
        lease_ms = queue_lease_seconds * 1000
        reserve_ms = 2 * source_call_budget_ms + lease_safety_margin_ms
        if lease_ms <= max(pmcp_budget_ms, source_call_budget_ms) + reserve_ms:
            raise ValueError("lease must cover an operation and two renewals with margin")

        self._source, self._pmcp, self._telemetry = source, pmcp, telemetry
        self._routing = routing
        self._clock_ms = clock_ms or (lambda: time.monotonic_ns() // 1_000_000)
        self._sleep_ms = sleep_ms or (lambda delay: time.sleep(delay / 1000))
        self._retry_base_ms, self._retry_max_ms = retry_base_ms, retry_max_ms
        self._max_retries = max_retries
        self._jitter_ms = jitter_ms or (lambda maximum: random.randint(0, maximum))
        self._poll_size = poll_size
        self._poll_wait_seconds = min(
            poll_wait_seconds,
            (lease_ms - reserve_ms - source_call_budget_ms - 1) // 1000,
        )
        self._queue_lease_seconds = queue_lease_seconds
        self._lease_ms = lease_ms
        self._pmcp_budget_ms = pmcp_budget_ms
        self._source_budget_ms = source_call_budget_ms
        self._margin_ms = lease_safety_margin_ms
        self._max_pending_deliveries = max_pending_deliveries
        self._max_pending_partitions = max_pending_partitions
        self._max_pending_events = max_pending_events
        self._buffer = WoeraBuffer(settling_ms, maximum_ms)
        self._pending: dict[str, _PendingDelivery] = {}
        self._handles_by_partition: dict[PartitionKey, set[str]] = {}
        self._retry_counts: dict[PartitionKey, int] = {}

    def run_once(self) -> None:
        self._finish_deliveries()
        self._flush_ready()
        self._finish_deliveries()
        capacity = min(
            self._poll_size,
            self._max_pending_deliveries - len(self._pending),
            self._max_pending_partitions - len(self._handles_by_partition),
            self._max_pending_events - self._pending_event_count(),
        )
        if capacity:
            wait_seconds = self._next_poll_wait_seconds()
            self._protect_leases(wait_seconds * 1000 + self._source_budget_ms)
            wait_seconds = self._next_poll_wait_seconds()
            started = self._clock_ms()
            deliveries = self._source.receive(capacity, wait_seconds, self._queue_lease_seconds)
            self._check_budget(started, wait_seconds * 1000 + self._source_budget_ms)
            if len(deliveries) > capacity:
                raise RuntimeError("source exceeded requested delivery capacity")
            self._ingest(deliveries, started + self._lease_ms)
        else:
            self._wait_for_capacity()
        self._flush_ready()
        self._finish_deliveries()

    def run_forever(self, stop_requested: Callable[[], bool]) -> None:
        while not stop_requested():
            self.run_once()

    def _pending_event_count(self) -> int:
        return sum(len(pending.delivery.events) for pending in self._pending.values())

    def _ingest(self, deliveries: Sequence[Delivery], lease_until_ms: int) -> None:
        if self._pending_event_count() + sum(len(delivery.events) for delivery in deliveries) > self._max_pending_events:
            raise RuntimeError("received events exceed pending capacity")
        handles = [delivery.handle for delivery in deliveries]
        if len(set(handles)) != len(handles) or set(handles) & self._pending.keys():
            raise RuntimeError("source reused a pending delivery handle")

        prepared = []
        keys = set(self._handles_by_partition)
        for delivery in deliveries:
            routed = tuple(
                candidate for event in delivery.events
                if (candidate := self._routing.route(event)) is not None
            )
            keys.update(candidate.partition.key for candidate in routed)
            if len(keys) > self._max_pending_partitions:
                raise RuntimeError("received partitions exceed pending capacity")
            prepared.append((delivery, routed))

        now_ms = self._clock_ms()
        for delivery, routed in prepared:
            partition_keys = set()
            for candidate in routed:
                key = self._buffer.add(candidate, now_ms)
                partition_keys.add(key)
                self._handles_by_partition.setdefault(key, set()).add(delivery.handle)
            self._pending[delivery.handle] = _PendingDelivery(delivery, partition_keys, lease_until_ms)
            if partition_keys:
                self._telemetry.emit({
                    "component": "worker", "action": "delivery_buffered",
                    "event_count": len(routed), "partition_count": len(partition_keys),
                })

    def _flush_ready(self) -> None:
        now_ms = self._clock_ms()
        keys = self._buffer.ready_keys(now_ms)
        # Retain scheduling if a provider or telemetry call interrupts this batch.
        for key in keys:
            self._buffer.defer(key, now_ms)
        for key in keys:
            self._protect_leases(self._pmcp_budget_ms)
            intent = self._buffer.intent(key)
            started = self._clock_ms()
            result = self._pmcp.apply(intent)
            self._check_budget(started, self._pmcp_budget_ms)
            self._telemetry.emit({
                "component": "worker", "action": "pmcp_result",
                "source_id": intent.partition.source_id,
                "table_id": intent.partition.table_id,
                "partition": intent.partition.partition_path,
                "observation_count": intent.observation_count,
                "mutation": intent.mutation, "status": result.status, "reason": result.reason,
            })
            if result.status == "RETRY":
                self._retry(key, result.reason)
            elif result.status in {"APPLIED", "NOOP", "ERROR"}:
                self._complete_partition(key, result.reason if result.status == "ERROR" else None)
            else:
                raise ValueError(f"unsupported PMCP status: {result.status}")

    def _retry(self, key: PartitionKey, reason: str) -> None:
        attempt = self._retry_counts.get(key, 0) + 1
        if attempt > self._max_retries:
            self._complete_partition(key, f"max_retries:{reason}")
            return
        self._retry_counts[key] = attempt
        maximum_delay = min(self._retry_max_ms, self._retry_base_ms * 2 ** (attempt - 1))
        delay_ms = self._jitter_ms(maximum_delay)
        if type(delay_ms) is not int or not 0 <= delay_ms <= maximum_delay:
            raise ValueError("retry jitter must be within the configured delay")
        self._buffer.defer(key, self._clock_ms() + delay_ms)

    def _complete_partition(self, key: PartitionKey, reason: str | None) -> None:
        self._buffer.complete(key)
        self._retry_counts.pop(key, None)
        for handle in self._handles_by_partition.pop(key, ()):
            pending = self._pending[handle]
            pending.partition_keys.remove(key)
            if reason is not None and pending.dead_letter_reason is None:
                pending.dead_letter_reason = reason

    def _finish_deliveries(self) -> None:
        for handle, pending in tuple(self._pending.items()):
            if pending.partition_keys:
                continue
            deliveries = (pending.delivery,)
            if pending.dead_letter_reason is not None and not pending.dead_lettered:
                self._source_call(self._source.dead_letter, deliveries, pending.dead_letter_reason)
                pending.dead_lettered = True
            self._source_call(self._source.acknowledge, deliveries)
            del self._pending[handle]
            self._telemetry.emit({
                "component": "worker", "action": "delivery_acknowledged",
                "event_count": len(pending.delivery.events),
                "dead_lettered": pending.dead_lettered,
            })

    def _source_call(self, operation: Callable, *args) -> None:
        self._protect_leases(self._source_budget_ms)
        started = self._clock_ms()
        operation(*args)
        self._check_budget(started, self._source_budget_ms)

    def _check_budget(self, started_ms: int, budget_ms: int) -> None:
        elapsed_ms = self._clock_ms() - started_ms
        if elapsed_ms < 0 or elapsed_ms > budget_ms:
            raise RuntimeError("provider operation exceeded its monotonic time budget; restart for replay")

    def _protect_leases(self, operation_budget_ms: int) -> None:
        if not self._pending:
            return
        started = self._clock_ms()
        earliest = min(pending.lease_until_ms for pending in self._pending.values())
        renewal_reserve = self._source_budget_ms + self._margin_ms
        if earliest > started + operation_budget_ms + renewal_reserve:
            return
        if earliest <= started + renewal_reserve:
            raise RuntimeError("insufficient lease remaining for safe renewal; restart for replay")
        self._source.renew(
            tuple(pending.delivery for pending in self._pending.values()),
            self._queue_lease_seconds,
        )
        self._check_budget(started, self._source_budget_ms)
        for pending in self._pending.values():
            pending.lease_until_ms = started + self._lease_ms
        if started + self._lease_ms <= self._clock_ms() + operation_budget_ms + renewal_reserve:
            raise RuntimeError("renewed lease cannot cover the operation; restart for replay")

    def _next_poll_wait_seconds(self) -> int:
        due_ms = self._buffer.next_ready_ms()
        if due_ms is None:
            return self._poll_wait_seconds
        return min(self._poll_wait_seconds, math.ceil(max(0, due_ms - self._clock_ms()) / 1000))

    def _wait_for_capacity(self) -> None:
        due_ms = self._buffer.next_ready_ms()
        delay_ms = min(
            max(1000, self._poll_wait_seconds * 1000),
            self._lease_ms - 2 * self._source_budget_ms - self._margin_ms - 1,
        )
        if due_ms is not None:
            delay_ms = min(delay_ms, max(0, due_ms - self._clock_ms()))
        self._protect_leases(delay_ms)
        if due_ms is not None:
            delay_ms = min(delay_ms, max(0, due_ms - self._clock_ms()))
        self._sleep_ms(delay_ms)
