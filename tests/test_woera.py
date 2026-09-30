from datetime import datetime, timezone

from rtmsf.models import ObjectEvent, PartitionEvent, PartitionRef


def observation(operation="CREATE", file="a.parquet", source_time=None):
    partition = PartitionRef("s", "orders", "dt=t", ("t",), "root/dt=t/")
    event = ObjectEvent("s", partition.storage_prefix + file, operation,
                        datetime.now(timezone.utc), source_time)
    return PartitionEvent(event, partition)


def buffer():
    from rtmsf.woera import WoeraBuffer
    return WoeraBuffer(settling_ms=6000, maximum_ms=31000, observed_key_limit=3)


def test_each_arrival_resets_q_but_never_past_maximum_residence():
    pending = buffer()
    key = pending.add(observation(), 1000)
    pending.add(observation(), 6000)
    assert pending.ready_keys(11999) == ()
    assert pending.ready_keys(12000) == (key,)
    pending.complete(key)
    key = pending.add(observation(), 20000)
    for now in range(25000, 51000, 5000):
        pending.add(observation(), now)
    assert pending.ready_keys(50999) == ()
    assert pending.ready_keys(51000) == (key,)


def test_burst_coalesces_observations_and_bounds_verified_keys():
    pending = buffer()
    for number in range(100):
        key = pending.add(observation(file=f"{number}.parquet"), 1000)
    assert pending.ready_keys(7000) == (key,)
    intent = pending.intent(key)
    assert intent.mutation == "ADD"
    assert intent.observation_count == 100
    assert len(intent.observed_create_keys) == 3
    pending.complete(key)
    assert pending.next_ready_ms() is None


def test_old_source_timestamps_do_not_control_eligibility_or_intent():
    pending = buffer()
    old = datetime(1970, 1, 1, tzinfo=timezone.utc)
    key = pending.add(observation("DELETE"), 1000)
    pending.add(observation(source_time=old), 2000)
    assert pending.next_ready_ms() == 8000
    assert pending.intent(key).mutation == "ADD"


def test_completed_window_is_not_a_permanent_deduplication_record():
    pending = buffer()
    for now in (0, 7000, 14000):
        key = pending.add(observation(), now)
        assert pending.ready_keys(now + 6000) == (key,)
        assert pending.intent(key).observation_count == 1
        pending.complete(key)


def test_retry_due_time_is_monotonic_and_has_no_extra_grace_timer():
    pending = buffer()
    key = pending.add(observation("DELETE"), 0)
    assert pending.ready_keys(6000) == (key,)
    pending.defer(key, 9000)
    assert pending.next_ready_ms() == 9000
    assert pending.ready_keys(8999) == ()
    assert pending.ready_keys(9000) == (key,)
    assert pending.intent(key).mutation == "DROP"


def test_new_arrival_cannot_bypass_a_scheduled_provider_retry():
    pending = buffer()
    key = pending.add(observation(), 0)
    pending.ready_keys(6000)
    pending.defer(key, 40000)
    pending.add(observation(), 20000)
    assert pending.next_ready_ms() == 40000
    assert pending.ready_keys(31000) == ()
