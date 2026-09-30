from datetime import datetime, timezone

import pytest

from rtmsf.models import MutationResult, ObjectEvent, TableDefinition
from rtmsf.interfaces import Delivery, RetryableExternalError
from rtmsf.routing import RoutingIndex

from rtmsf.worker import RTMSFWorker


class Clock:
    def __init__(self):
        self.now = 0

    def __call__(self):
        return self.now

    def advance(self, milliseconds):
        self.now += milliseconds


class Source:
    """An at-least-once source that detects expiration during every call."""

    def __init__(self, clock, deliveries=(), call_ms=0):
        self.clock = clock
        self.queue = list(deliveries)
        self.call_ms = call_ms
        self.leases = {}
        self.calls = []
        self.acknowledged = []
        self.dead_letters = []
        self.fail_ack = 0
        self.fail_dead_letter = 0

    def check_owned(self):
        assert all(self.clock() < deadline for deadline in self.leases.values())

    def elapse(self, milliseconds):
        self.check_owned()
        self.clock.advance(milliseconds)
        self.check_owned()

    def receive(self, max_messages, wait_seconds, lease_seconds):
        self.calls.append(("receive", max_messages, wait_seconds, lease_seconds))
        deliveries = self.queue[:max_messages]
        del self.queue[:max_messages]
        for delivery in deliveries:
            self.leases[delivery.handle] = self.clock() + lease_seconds * 1000
        self.elapse(self.call_ms + (0 if deliveries else wait_seconds * 1000))
        return deliveries

    def renew(self, deliveries, lease_seconds):
        handles = tuple(delivery.handle for delivery in deliveries)
        self.calls.append(("renew", handles, lease_seconds, self.clock()))
        self.elapse(self.call_ms)
        for handle in handles:
            self.leases[handle] = self.clock() + lease_seconds * 1000

    def acknowledge(self, deliveries):
        handles = tuple(delivery.handle for delivery in deliveries)
        self.calls.append(("acknowledge", handles, self.clock()))
        self.elapse(self.call_ms)
        if self.fail_ack:
            self.fail_ack -= 1
            raise RuntimeError("acknowledgement failed")
        self.acknowledged.extend(handles)
        for handle in handles:
            del self.leases[handle]

    def dead_letter(self, deliveries, reason):
        handles = tuple(delivery.handle for delivery in deliveries)
        self.calls.append(("dead_letter", handles, self.clock()))
        self.elapse(self.call_ms)
        if self.fail_dead_letter:
            self.fail_dead_letter -= 1
            raise RuntimeError("dead-letter persistence failed")
        self.dead_letters.append((handles, reason))

    def redeliver(self, delivery):
        self.clock.advance(20_000)
        self.leases.clear()
        self.queue.append(Delivery("replay", delivery.events))


class Telemetry:
    def __init__(self):
        self.records = []

    def emit(self, record):
        self.records.append(record)


class Pmcp:
    def __init__(self, source, results=(), call_ms=0):
        self.source = source
        self.results = list(results)
        self.call_ms = call_ms
        self.intents = []
        self.times = []

    def apply(self, intent):
        self.intents.append(intent)
        self.times.append(self.source.clock())
        self.source.elapse(self.call_ms)
        return self.results.pop(0) if self.results else MutationResult("APPLIED", "ok")


def event(partition="a", operation="CREATE", **overrides):
    values = {
        "source_id": "logical-source",
        "object_key": f"root/p={partition}/file.parquet",
        "operation": operation,
        "ingested_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return ObjectEvent(**values)


def delivery(handle="one", *events):
    return Delivery(handle, events or (event(),))


def worker(source, pmcp=None, telemetry=None, **overrides):
    options = {
        "source": source,
        "pmcp": pmcp or Pmcp(source),
        "telemetry": telemetry or Telemetry(),
        "routing": RoutingIndex((
            TableDefinition("db.table", "logical-source", "root/", ("p",)),
        )),
        "settling_ms": 1000,
        "maximum_ms": 5000,
        "queue_lease_seconds": 10,
        "pmcp_budget_ms": 1000,
        "source_call_budget_ms": 200,
        "lease_safety_margin_ms": 100,
        "poll_wait_seconds": 0,
        "clock_ms": source.clock,
        "sleep_ms": source.clock.advance,
        "jitter_ms": lambda maximum: maximum,
    }
    options.update(overrides)
    return RTMSFWorker(**options)


@pytest.mark.parametrize("option,value", [
    ("settling_ms", -1), ("maximum_ms", 999), ("queue_lease_seconds", 1),
    ("pmcp_budget_ms", 0), ("source_call_budget_ms", float("nan")),
    ("max_pending_deliveries", 0),
])
def test_invalid_time_budget_and_capacity_settings_fail_at_startup(option, value):
    with pytest.raises(ValueError):
        worker(Source(Clock()), **{option: value})


def test_events_wait_for_a_fresh_monotonic_quiet_period():
    clock = Clock()
    source = Source(clock, [delivery()])
    pmcp = Pmcp(source)
    subject = worker(source, pmcp)

    subject.run_once()
    clock.advance(999)
    subject.run_once()
    assert pmcp.intents == []
    assert source.acknowledged == []

    clock.advance(1)
    subject.run_once()
    assert pmcp.intents[0].mutation == "ADD"
    assert source.acknowledged == ["one"]


def test_default_clock_uses_monotonic_time(monkeypatch):
    clock = Clock()
    source = Source(clock, [delivery()])
    pmcp = Pmcp(source)
    monkeypatch.setattr("rtmsf.worker.time.monotonic_ns", lambda: clock() * 1_000_000)
    monkeypatch.setattr("rtmsf.worker.time.time", lambda: -10**20)
    subject = worker(source, pmcp, clock_ms=None)
    subject.run_once()
    clock.advance(1000)
    subject.run_once()
    assert len(pmcp.intents) == 1


def test_duplicate_deliveries_coalesce_and_acknowledge_together():
    clock = Clock()
    source = Source(clock, [delivery("one"), delivery("two")])
    pmcp = Pmcp(source)
    subject = worker(source, pmcp)
    subject.run_once()
    clock.advance(1000)
    subject.run_once()
    assert len(pmcp.intents) == 1
    assert pmcp.intents[0].observation_count == 2
    assert set(source.acknowledged) == {"one", "two"}


def test_delivery_is_not_acknowledged_until_every_partition_finishes():
    clock = Clock()
    source = Source(clock, [delivery("one", event("a"), event("b"))])
    pmcp = Pmcp(source, [
        MutationResult("APPLIED", "ok"), MutationResult("RETRY", "not_visible"),
    ])
    subject = worker(source, pmcp)
    subject.run_once()
    clock.advance(1000)
    subject.run_once()
    assert len(pmcp.intents) == 2
    assert source.acknowledged == []
    clock.advance(1000)
    subject.run_once()
    assert len(pmcp.intents) == 3
    assert source.acknowledged == ["one"]


def test_terminal_partition_does_not_acknowledge_other_unfinished_intents():
    clock = Clock()
    source = Source(clock, [delivery("one", event("a"), event("b"))])
    pmcp = Pmcp(source, [
        MutationResult("ERROR", "bad_partition"), MutationResult("RETRY", "later"),
    ])
    subject = worker(source, pmcp)
    subject.run_once()
    clock.advance(1000)
    subject.run_once()
    assert source.dead_letters == []
    assert source.acknowledged == []
    clock.advance(1000)
    subject.run_once()
    assert source.dead_letters == [(("one",), "bad_partition")]
    assert source.acknowledged == ["one"]


def test_durable_dead_letter_precedes_ack_and_failure_keeps_delivery_pending():
    clock = Clock()
    source = Source(clock, [delivery()])
    source.fail_dead_letter = 1
    pmcp = Pmcp(source, [MutationResult("ERROR", "fatal")])
    subject = worker(source, pmcp, settling_ms=0)
    with pytest.raises(RuntimeError, match="dead-letter persistence failed"):
        subject.run_once()
    assert source.acknowledged == []
    subject.run_once()
    assert len(pmcp.intents) == 1
    assert source.dead_letters == [(("one",), "fatal")]
    assert source.acknowledged == ["one"]
    assert [call[0] for call in source.calls].index("dead_letter") < [
        call[0] for call in source.calls
    ].index("acknowledge")


@pytest.mark.parametrize("terminal", [False, True])
def test_acknowledgement_failure_is_recoverable_without_reapplying_pmcp(terminal):
    source = Source(Clock(), [delivery()])
    source.fail_ack = 1
    result = MutationResult("ERROR", "fatal") if terminal else MutationResult("APPLIED", "ok")
    pmcp = Pmcp(source, [result])
    subject = worker(source, pmcp, settling_ms=0)
    with pytest.raises(RuntimeError, match="acknowledgement failed"):
        subject.run_once()
    source.clock.advance(9650)
    subject.run_once()
    assert len(pmcp.intents) == 1
    assert source.acknowledged == ["one"]
    assert len(source.dead_letters) == int(terminal)
    assert any(call[0] == "renew" for call in source.calls)


def test_ignored_events_are_acknowledged_and_ack_failure_is_recoverable():
    source = Source(Clock(), [delivery("one", event(object_key="elsewhere/file.parquet"))])
    source.fail_ack = 1
    pmcp = Pmcp(source)
    subject = worker(source, pmcp)
    with pytest.raises(RuntimeError, match="acknowledgement failed"):
        subject.run_once()
    subject.run_once()
    assert pmcp.intents == []
    assert source.acknowledged == ["one"]


def test_retry_deadline_starts_after_the_slow_pmcp_attempt():
    clock = Clock()
    source = Source(clock, [delivery()])
    pmcp = Pmcp(source, [MutationResult("RETRY", "later")], call_ms=800)
    subject = worker(source, pmcp, settling_ms=0, retry_base_ms=2000)
    subject.run_once()
    clock.advance(1999)
    subject.run_once()
    assert len(pmcp.intents) == 1
    clock.advance(1)
    subject.run_once()
    assert pmcp.times == [0, 2800]


def test_retry_exhaustion_is_durably_dead_lettered_then_acknowledged():
    source = Source(Clock(), [delivery()])
    pmcp = Pmcp(source, [MutationResult("RETRY", "later")])
    subject = worker(source, pmcp, settling_ms=0, max_retries=0)
    subject.run_once()
    assert source.dead_letters == [(("one",), "max_retries:later")]
    assert source.acknowledged == ["one"]


def test_slow_partition_batch_renews_all_owned_receipts_before_they_expire():
    clock = Clock()
    source = Source(clock, [delivery(str(i), event(str(i))) for i in range(3)], call_ms=1000)
    pmcp = Pmcp(source, call_ms=2500)
    subject = worker(source, pmcp, pmcp_budget_ms=3000, source_call_budget_ms=1000)
    subject.run_once()
    clock.advance(1000)
    subject.run_once()
    renewals = [call for call in source.calls if call[0] == "renew"]
    assert renewals
    assert set(renewals[0][1]) == {"0", "1", "2"}
    assert set(source.acknowledged) == {"0", "1", "2"}


def test_long_buffering_and_retry_keep_full_leases():
    clock = Clock()
    source = Source(clock, [delivery()], call_ms=100)
    pmcp = Pmcp(source, [MutationResult("RETRY", "later")])
    subject = worker(
        source, pmcp, settling_ms=20_000, maximum_ms=30_000,
        poll_wait_seconds=20, retry_base_ms=15_000,
    )
    for _ in range(20):
        subject.run_once()
        if source.acknowledged:
            break
    renewals = [call for call in source.calls if call[0] == "renew"]
    assert len(renewals) >= 3
    assert all(call[2] == 10 for call in renewals)
    assert len(pmcp.intents) == 2
    assert source.acknowledged == ["one"]


def test_worker_fails_closed_when_too_late_to_renew_safely():
    clock = Clock()
    source = Source(clock, [delivery()])
    pmcp = Pmcp(source)
    subject = worker(source, pmcp, source_call_budget_ms=1000)
    subject.run_once()
    clock.advance(9500)
    with pytest.raises(RuntimeError, match="lease"):
        subject.run_once()
    assert pmcp.intents == []
    assert source.acknowledged == []
    assert all(call[0] != "renew" for call in source.calls)


def test_provider_budget_overrun_stops_before_acknowledgement():
    source = Source(Clock(), [delivery()])
    pmcp = Pmcp(source, call_ms=1001)
    with pytest.raises(RuntimeError, match="budget"):
        worker(source, pmcp, settling_ms=0).run_once()
    assert source.acknowledged == []


def test_pending_delivery_limit_applies_backpressure_until_progress_is_possible():
    clock = Clock()
    source = Source(clock, [delivery("one"), delivery("two", event("b"))])
    subject = worker(source, max_pending_deliveries=1)
    subject.run_once()
    assert [call[1] for call in source.calls if call[0] == "receive"] == [1]
    subject.run_once()
    assert source.acknowledged == ["one"]
    assert len([call for call in source.calls if call[0] == "receive"]) == 1
    subject.run_once()
    assert len([call for call in source.calls if call[0] == "receive"]) == 2


@pytest.mark.parametrize("limits", [
    {"max_pending_events": 1}, {"max_pending_partitions": 1},
])
def test_oversized_delivery_fails_without_acknowledgement_or_partial_pmcp(limits):
    source = Source(Clock(), [delivery("one", event("a"), event("b"))])
    pmcp = Pmcp(source)
    with pytest.raises(RuntimeError, match="capacity"):
        worker(source, pmcp, settling_ms=0, **limits).run_once()
    assert source.acknowledged == []
    assert pmcp.intents == []


def test_worker_telemetry_uses_only_provider_neutral_event_and_partition_fields():
    source = Source(Clock(), [delivery()])
    telemetry = Telemetry()
    worker(source, telemetry=telemetry, settling_ms=0).run_once()
    records = telemetry.records
    assert {record["action"] for record in records} >= {
        "delivery_buffered", "pmcp_result", "delivery_acknowledged",
    }
    assert not any({"receipt", "raw_body", "message_id"} & record.keys() for record in records)


class Storage:
    def __init__(self, keys):
        self.keys = set(keys)

    def head_exists(self, source_id, key):
        return key in self.keys

    def prefix_non_empty(self, source_id, prefix):
        return any(key.startswith(prefix) for key in self.keys)

    def location_uri(self, source_id, prefix):
        return f"memory://{source_id}/{prefix}"


class Catalog:
    def __init__(self, partition, present):
        self.partitions = {partition.key} if present else set()
        self.mutations = []
        self.unknown_outcome = False

    def partition_exists(self, partition):
        return partition.key in self.partitions

    def _mutated(self, mutation, partition):
        self.mutations.append((mutation, partition.key))
        if self.unknown_outcome:
            self.unknown_outcome = False
            raise RetryableExternalError("outcome unknown after catalog mutation")
        return True

    def create_partition(self, partition, location):
        if partition.key in self.partitions:
            return False
        self.partitions.add(partition.key)
        return self._mutated("ADD", partition)

    def delete_partition(self, partition):
        if partition.key not in self.partitions:
            return False
        self.partitions.remove(partition.key)
        return self._mutated("DROP", partition)


@pytest.mark.parametrize("operation", ["CREATE", "DELETE"])
@pytest.mark.parametrize("crash_point", ["buffered", "unknown_outcome", "before_ack"])
def test_add_and_drop_replay_after_crashes_with_a_fresh_quiet_period(operation, crash_point):
    from rtmsf.pmcp import PartitionMutationCommitProtocol

    original = delivery("original", event(operation=operation))
    clock = Clock()
    source = Source(clock, [original])
    routing = RoutingIndex((TableDefinition("db.table", "logical-source", "root/", ("p",)),))
    partition = routing.route(original.events[0]).partition
    storage = Storage([original.events[0].object_key] if operation == "CREATE" else [])
    catalog = Catalog(partition, present=operation == "DELETE")
    pmcp = PartitionMutationCommitProtocol(storage, catalog)

    first = worker(source, pmcp)
    first.run_once()
    if crash_point != "buffered":
        clock.advance(1000)
        if crash_point == "unknown_outcome":
            catalog.unknown_outcome = True
            first.run_once()
        else:
            source.fail_ack = 1
            with pytest.raises(RuntimeError, match="acknowledgement failed"):
                first.run_once()

    assert source.acknowledged == []
    del first  # Lose in-memory state at the selected boundary, as a stopped process does.
    source.redeliver(original)
    replay_pmcp = Pmcp(source)

    class RecordingPmcp:
        def apply(self, intent):
            replay_pmcp.intents.append(intent)
            return pmcp.apply(intent)

    second = worker(source, RecordingPmcp())
    second.run_once()
    clock.advance(999)
    second.run_once()
    assert replay_pmcp.intents == []
    assert source.acknowledged == []
    clock.advance(1)
    second.run_once()
    assert (partition.key in catalog.partitions) is (operation == "CREATE")
    assert len(catalog.mutations) == 1
    assert source.acknowledged == ["replay"]
