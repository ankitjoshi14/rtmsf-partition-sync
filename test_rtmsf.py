"""
test_rtmsf.py — Snapshot tests for RTMSF core domain + simulation.

Run: python3 -m pytest test_rtmsf.py -v
  or: python3 test_rtmsf.py
"""

import time
import pytest
from woera_buffer import WOERABuffer, CandidateIntent, EffectiveIntent
from pmcp import PMCP, Result
from mock_storage import MockStorage, StorageTransientException
from mock_catalog import (
    MockCatalog, CatalogTransientException, CatalogFatalException
)
from rtmsf_worker import RTMSFWorker, EventEnvelope, TABLE_ROOT, TENANT_ID, TABLE_FQN


# ------------------------------------------------------------------ helpers

def _make_candidate(pkey: str, kind: str = "ADD_CANDIDATE", proof: str = None) -> CandidateIntent:
    """Create a CandidateIntent for testing."""
    prefix = f"{TABLE_ROOT}{pkey}/"
    if proof is None and kind == "ADD_CANDIDATE":
        proof = f"{TABLE_ROOT}{pkey}/part-0000.parquet"
    return CandidateIntent(
        key=(TENANT_ID, TABLE_FQN, pkey),
        partition_key=pkey,
        partition_prefix=prefix,
        kind=kind,
        proof=proof,
    )


def _make_effective(pkey: str, kind: str = "ADD", proof_keys: list = None) -> EffectiveIntent:
    """Create an EffectiveIntent for testing."""
    prefix = f"{TABLE_ROOT}{pkey}/"
    if proof_keys is None:
        proof_keys = [f"{TABLE_ROOT}{pkey}/part-0000.parquet"] if kind == "ADD" else []
    return EffectiveIntent(
        key=(TENANT_ID, TABLE_FQN, pkey),
        partition_prefix=prefix,
        kind=kind,
        proof_keys=proof_keys,
    )


def _make_event(pkey: str, fname: str = "part-0000.parquet", op: str = "OBJECT_CREATED") -> EventEnvelope:
    """Create an EventEnvelope for testing."""
    return EventEnvelope(
        tenant_id=TENANT_ID,
        op=op,
        object_key=f"{TABLE_ROOT}{pkey}/{fname}",
        event_time=time.time(),
    )


# ================================================================== A. WOERA TESTS

class TestWOERACoalescing:
    """WOERA reconciliation: coalesces multiple events into 1 intent per partition."""

    def test_multiple_adds_coalesce_to_single_add(self):
        """3 ADD_CANDIDATEs for same partition → 1 ADD intent."""
        buf = WOERABuffer(W=0.1, G=0.0)
        key = (TENANT_ID, TABLE_FQN, "country=us/city=seattle")
        intents = [
            _make_candidate("country=us/city=seattle", proof=f"{TABLE_ROOT}country=us/city=seattle/part-000{i}.parquet")
            for i in range(3)
        ]
        result = buf.reconcile(intents, key)

        assert len(result) == 1
        assert result[0].kind == "ADD"
        assert len(result[0].proof_keys) <= 3

    def test_add_dominant_over_drop(self):
        """1 DROP + 2 ADD → 1 ADD intent (ADD-dominant rule)."""
        buf = WOERABuffer(W=0.1, G=0.0)
        key = (TENANT_ID, TABLE_FQN, "country=us/city=seattle")
        intents = [
            _make_candidate("country=us/city=seattle", kind="DROP_CANDIDATE", proof=None),
            _make_candidate("country=us/city=seattle", proof=f"{TABLE_ROOT}country=us/city=seattle/part-0001.parquet"),
            _make_candidate("country=us/city=seattle", proof=f"{TABLE_ROOT}country=us/city=seattle/part-0002.parquet"),
        ]
        result = buf.reconcile(intents, key)

        assert len(result) == 1
        assert result[0].kind == "ADD"

    def test_drop_only(self):
        """2 DROP_CANDIDATEs → 1 DROP intent."""
        buf = WOERABuffer(W=0.1, G=0.0)
        key = (TENANT_ID, TABLE_FQN, "country=us/city=seattle")
        intents = [
            _make_candidate("country=us/city=seattle", kind="DROP_CANDIDATE", proof=None),
            _make_candidate("country=us/city=seattle", kind="DROP_CANDIDATE", proof=None),
        ]
        result = buf.reconcile(intents, key)

        assert len(result) == 1
        assert result[0].kind == "DROP"
        assert result[0].proof_keys == []

    def test_empty_intents(self):
        """Empty list → empty result."""
        buf = WOERABuffer(W=0.1, G=0.0)
        result = buf.reconcile([], ("t", "tbl", "pkey"))
        assert result == []

    def test_duplicates_coalesce_naturally(self):
        """4 identical ADDs (at-least-once) → 1 ADD intent. No explicit dedup needed."""
        buf = WOERABuffer(W=0.1, G=0.0)
        key = (TENANT_ID, TABLE_FQN, "country=us/city=seattle")
        same_intent = _make_candidate("country=us/city=seattle")
        intents = [same_intent, same_intent, same_intent, same_intent]
        result = buf.reconcile(intents, key)

        assert len(result) == 1
        assert result[0].kind == "ADD"


# ================================================================== B. PMCP TESTS

class TestPMCPAdd:
    """PMCP apply_add: storage-verified partition creation."""

    def test_add_new_partition(self):
        """HEAD exists → APPLIED."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        pkey = "country=us/city=seattle"
        okey = f"{TABLE_ROOT}{pkey}/part-0000.parquet"
        storage.put(okey)

        eff = _make_effective(pkey)
        result = pmcp.apply(eff)

        assert result.status == "APPLIED"
        assert catalog.has_partition((TENANT_ID, TABLE_FQN, pkey))

    def test_add_existing_verified(self):
        """Partition in catalog + files exist → NOOP (already_exists_verified)."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog, enable_drop=True)

        pkey = "country=us/city=seattle"
        cat_key = (TENANT_ID, TABLE_FQN, pkey)
        okey = f"{TABLE_ROOT}{pkey}/part-0000.parquet"
        storage.put(okey)
        catalog.create_partition(cat_key, location=f"{TABLE_ROOT}{pkey}/")

        eff = _make_effective(pkey)
        result = pmcp.apply(eff)

        assert result.status == "NOOP"
        assert result.reason == "already_exists_verified"

    def test_add_existing_stale_cleared(self):
        """Partition in catalog + files gone → stale entry removed → RETRY."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog, enable_drop=True)

        pkey = "country=us/city=seattle"
        cat_key = (TENANT_ID, TABLE_FQN, pkey)
        catalog.create_partition(cat_key, location=f"{TABLE_ROOT}{pkey}/")
        # No files in storage — stale entry

        eff = _make_effective(pkey)
        result = pmcp.apply(eff)

        assert result.status == "RETRY"
        assert result.reason == "stale_catalog_entry_cleared"
        assert not catalog.has_partition(cat_key)  # stale entry removed

    def test_add_no_files_retry(self):
        """New partition, no files in storage → RETRY."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        eff = _make_effective("country=us/city=seattle")
        result = pmcp.apply(eff)

        assert result.status == "RETRY"


class TestPMCPDrop:
    """PMCP apply_drop: storage-verified partition removal."""

    def test_drop_empty(self):
        """Partition exists, storage empty → DROP APPLIED."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        pkey = "country=us/city=seattle"
        cat_key = (TENANT_ID, TABLE_FQN, pkey)
        catalog.create_partition(cat_key, location=f"{TABLE_ROOT}{pkey}/")

        eff = _make_effective(pkey, kind="DROP")
        result = pmcp.apply(eff)

        assert result.status == "APPLIED"
        assert not catalog.has_partition(cat_key)

    def test_drop_nonempty(self):
        """Partition exists, files present → NOOP."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        pkey = "country=us/city=seattle"
        cat_key = (TENANT_ID, TABLE_FQN, pkey)
        storage.put(f"{TABLE_ROOT}{pkey}/part-0000.parquet")
        catalog.create_partition(cat_key, location=f"{TABLE_ROOT}{pkey}/")

        eff = _make_effective(pkey, kind="DROP")
        result = pmcp.apply(eff)

        assert result.status == "NOOP"
        assert catalog.has_partition(cat_key)  # not removed

    def test_drop_disabled(self):
        """enable_drop=false → DROP intent returns NOOP."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog, enable_drop=False)

        pkey = "country=us/city=seattle"
        cat_key = (TENANT_ID, TABLE_FQN, pkey)
        catalog.create_partition(cat_key, location=f"{TABLE_ROOT}{pkey}/")

        eff = _make_effective(pkey, kind="DROP")
        result = pmcp.apply(eff)

        assert result.status == "NOOP"
        assert result.reason == "drop_disabled"
        assert catalog.has_partition(cat_key)  # still there

    def test_add_skip_verify_when_drop_disabled(self):
        """enable_drop=false + partition exists → NOOP without LIST verify."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog, enable_drop=False)

        pkey = "country=us/city=seattle"
        cat_key = (TENANT_ID, TABLE_FQN, pkey)
        catalog.create_partition(cat_key, location=f"{TABLE_ROOT}{pkey}/")
        # No files — but with enable_drop=false, PMCP shouldn't check

        eff = _make_effective(pkey, kind="ADD")
        result = pmcp.apply(eff)

        assert result.status == "NOOP"
        assert result.reason == "already_exists"
        assert storage.list_requests == 0  # no LIST call made


# ================================================================== B2. PMCP ERROR-PATH TESTS

class TestPMCPStorageTransient:
    """PMCP behavior when storage raises StorageTransientException."""

    def test_add_head_transient_returns_retry(self):
        """HEAD fails transiently on proof key → RETRY."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        pkey = "country=us/city=seattle"
        okey = f"{TABLE_ROOT}{pkey}/part-0000.parquet"
        storage.put(okey)
        storage.fail_next_head()

        eff = _make_effective(pkey)
        result = pmcp.apply(eff)

        assert result.status == "RETRY"
        assert result.reason == "storage_transient"
        assert pmcp.retries == 1

    def test_add_list_fallback_transient_returns_retry(self):
        """HEAD misses, LIST fallback fails transiently → RETRY."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        pkey = "country=us/city=seattle"
        # No file → HEAD returns False (no exception), then LIST fallback fires
        storage.fail_next_list()

        eff = _make_effective(pkey, proof_keys=[])  # no proofs → skip HEAD, go to LIST
        result = pmcp.apply(eff)

        assert result.status == "RETRY"
        assert result.reason == "storage_transient"

    def test_add_existing_verify_transient_returns_retry(self):
        """Existing partition, LIST verify fails transiently → RETRY."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog, enable_drop=True)

        pkey = "country=us/city=seattle"
        cat_key = (TENANT_ID, TABLE_FQN, pkey)
        catalog.create_partition(cat_key, location=f"{TABLE_ROOT}{pkey}/")
        storage.fail_next_list()

        eff = _make_effective(pkey)
        result = pmcp.apply(eff)

        assert result.status == "RETRY"
        assert result.reason == "storage_transient"
        assert catalog.has_partition(cat_key)  # partition untouched

    def test_drop_list_transient_returns_retry(self):
        """DROP emptiness check fails transiently → RETRY."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        pkey = "country=us/city=seattle"
        cat_key = (TENANT_ID, TABLE_FQN, pkey)
        catalog.create_partition(cat_key, location=f"{TABLE_ROOT}{pkey}/")
        storage.fail_next_list()

        eff = _make_effective(pkey, kind="DROP")
        result = pmcp.apply(eff)

        assert result.status == "RETRY"
        assert result.reason == "storage_transient"
        assert catalog.has_partition(cat_key)  # partition untouched


class TestPMCPCatalogExceptions:
    """PMCP behavior when catalog raises transient or fatal exceptions."""

    def test_create_transient_returns_retry(self):
        """Catalog transient on create → RETRY."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        pkey = "country=us/city=seattle"
        storage.put(f"{TABLE_ROOT}{pkey}/part-0000.parquet")
        catalog.fail_next_create(CatalogTransientException("timeout"))

        eff = _make_effective(pkey)
        result = pmcp.apply(eff)

        assert result.status == "RETRY"
        assert result.reason == "catalog_transient"
        assert pmcp.retries == 1

    def test_create_fatal_returns_error(self):
        """Catalog fatal on create → ERROR."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        pkey = "country=us/city=seattle"
        storage.put(f"{TABLE_ROOT}{pkey}/part-0000.parquet")
        catalog.fail_next_create(CatalogFatalException("permission denied"))

        eff = _make_effective(pkey)
        result = pmcp.apply(eff)

        assert result.status == "ERROR"
        assert result.reason == "catalog_fatal"
        assert pmcp.errors == 1

    def test_drop_transient_returns_retry(self):
        """Catalog transient on drop → RETRY."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        pkey = "country=us/city=seattle"
        cat_key = (TENANT_ID, TABLE_FQN, pkey)
        catalog.create_partition(cat_key, location=f"{TABLE_ROOT}{pkey}/")
        catalog.fail_next_drop(CatalogTransientException("timeout"))

        eff = _make_effective(pkey, kind="DROP")
        result = pmcp.apply(eff)

        assert result.status == "RETRY"
        assert result.reason == "catalog_transient"
        assert catalog.has_partition(cat_key)  # drop didn't happen

    def test_drop_fatal_returns_error(self):
        """Catalog fatal on drop → ERROR."""
        storage = MockStorage()
        catalog = MockCatalog()
        pmcp = PMCP(storage, catalog)

        pkey = "country=us/city=seattle"
        cat_key = (TENANT_ID, TABLE_FQN, pkey)
        catalog.create_partition(cat_key, location=f"{TABLE_ROOT}{pkey}/")
        catalog.fail_next_drop(CatalogFatalException("permission denied"))

        eff = _make_effective(pkey, kind="DROP")
        result = pmcp.apply(eff)

        assert result.status == "ERROR"
        assert result.reason == "catalog_fatal"
        assert pmcp.errors == 1


class TestWorkerRetryAndDLQ:
    """Worker retry queue and DLQ overflow behavior."""

    def test_dlq_after_max_retries(self):
        """Intent exceeding MAX_RETRIES moves to DLQ."""
        storage = MockStorage()
        catalog = MockCatalog()
        sim_clock = [0.0]
        worker = RTMSFWorker(storage, catalog, W=0.0, G=0.0,
                             clock_fn=lambda: sim_clock[0])
        worker.MAX_RETRIES = 3

        pkey = "country=us/city=seattle"
        events = [_make_event(pkey)]
        # process_batch will put the file then flush; delete it before flush
        # by using high not_ready_rate so HEAD/LIST can't see it
        storage.not_ready_rate = 1.0  # nothing visible
        worker.process_batch(events)

        # process_batch already flushed once → intent is in retry queue
        # Drain retries until DLQ (storage still not visible)
        for _ in range(10):
            sim_clock[0] += 120
            worker.drain_retries()

        assert len(worker.dlq) == 1
        assert worker.dlq[0].kind == "ADD"

    def test_retry_resolves_on_second_attempt(self):
        """Intent that fails once then succeeds does not go to DLQ."""
        storage = MockStorage()
        catalog = MockCatalog()
        sim_clock = [0.0]
        worker = RTMSFWorker(storage, catalog, W=0.0, G=0.0,
                             clock_fn=lambda: sim_clock[0])

        pkey = "country=us/city=seattle"
        # First attempt: file invisible → RETRY
        storage.not_ready_rate = 1.0
        events = [_make_event(pkey)]
        worker.process_batch(events)
        assert len(worker._retry_queue) == 1

        # Make file visible, advance clock, drain
        storage.not_ready_rate = 0.0
        storage._confirmed_visible.add(f"{TABLE_ROOT}{pkey}/part-0000.parquet")
        sim_clock[0] += 120
        resolved = worker.drain_retries()

        assert resolved == 1
        assert len(worker.dlq) == 0
        assert catalog.has_partition((TENANT_ID, TABLE_FQN, pkey))

    def test_error_goes_directly_to_dlq(self):
        """Catalog fatal during flush → intent goes straight to DLQ."""
        storage = MockStorage()
        catalog = MockCatalog()
        sim_clock = [0.0]
        worker = RTMSFWorker(storage, catalog, W=0.0, G=0.0,
                             clock_fn=lambda: sim_clock[0])

        pkey = "country=us/city=seattle"
        storage.put(f"{TABLE_ROOT}{pkey}/part-0000.parquet")
        catalog.fail_next_create(CatalogFatalException("fatal"))

        events = [_make_event(pkey)]
        worker.process_batch(events)

        assert len(worker.dlq) == 1
        assert len(worker._retry_queue) == 0


class TestWOERATmax:
    """T_max force-flush: keys are flushed after T_max regardless of activity."""

    def test_tmax_forces_flush(self):
        """Continuously extended key is force-flushed after T_max + G."""
        sim_clock = [0.0]
        W = 1.0
        G = 0.5
        # T_max defaults to 5*W = 5.0
        buf = WOERABuffer(W=W, G=G, clock_fn=lambda: sim_clock[0])

        key = (TENANT_ID, TABLE_FQN, "country=us/city=seattle")
        intent = _make_candidate("country=us/city=seattle")

        # Add intent at t=0
        buf.add(intent)

        # Keep extending: add new intents every 0.5s for 4 seconds
        for t in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]:
            sim_clock[0] = t
            buf.add(intent)

        # At t=4.0, T_max=5.0 hasn't elapsed yet, but flush_at is capped
        # at first_seen + T_max = 0 + 5.0 = 5.0
        # Ready when now >= flush_at + G = 5.0 + 0.5 = 5.5
        sim_clock[0] = 5.0
        assert len(buf.ready_keys()) == 0  # not yet

        sim_clock[0] = 5.6
        ready = buf.ready_keys()
        assert key in ready

    def test_default_tmax_is_5w(self):
        """Default T_max = 5*W per paper recommendation."""
        buf = WOERABuffer(W=2.0, G=0.0)
        assert buf.T_max == 10.0

    def test_explicit_tmax_overrides_default(self):
        """Explicit T_max overrides the 5*W default."""
        buf = WOERABuffer(W=2.0, G=0.0, T_max=3.0)
        assert buf.T_max == 3.0


# ================================================================== C. WORKER PIPELINE TESTS

class TestWorkerPipeline:
    """End-to-end: events → WOERA → PMCP → catalog."""

    def test_full_pipeline(self):
        """3 partitions × 2 files → 3 catalog entries."""
        storage = MockStorage()
        catalog = MockCatalog()
        worker = RTMSFWorker(storage, catalog, W=0.1, G=0.0, enable_drop=True)

        events = []
        for city in ["seattle", "portland", "denver"]:
            for i in range(2):
                events.append(_make_event(f"country=us/city={city}", f"part-{i:04d}.parquet"))

        worker.process_batch(events)
        time.sleep(0.2)
        worker.flush_and_commit()

        assert len(catalog._partitions) == 3
        assert catalog.has_partition((TENANT_ID, TABLE_FQN, "country=us/city=seattle"))
        assert catalog.has_partition((TENANT_ID, TABLE_FQN, "country=us/city=portland"))
        assert catalog.has_partition((TENANT_ID, TABLE_FQN, "country=us/city=denver"))

    def test_drop_pipeline(self):
        """Create partition, then delete files + send DELETE events → partition removed."""
        storage = MockStorage()
        catalog = MockCatalog()
        worker = RTMSFWorker(storage, catalog, W=0.1, G=0.0, enable_drop=True)

        # Create
        pkey = "country=us/city=seattle"
        events = [_make_event(pkey, f"part-{i:04d}.parquet") for i in range(2)]
        worker.process_batch(events)
        time.sleep(0.2)
        worker.flush_and_commit()
        assert catalog.has_partition((TENANT_ID, TABLE_FQN, pkey))

        # Delete files + send DELETE events
        for i in range(2):
            storage.delete(f"{TABLE_ROOT}{pkey}/part-{i:04d}.parquet")
        del_events = [_make_event(pkey, f"part-{i:04d}.parquet", op="OBJECT_DELETED") for i in range(2)]
        worker.process_batch(del_events)
        time.sleep(0.2)
        worker.flush_and_commit()
        assert not catalog.has_partition((TENANT_ID, TABLE_FQN, pkey))


# ================================================================== D. SIMULATION INTEGRATION TESTS

class TestSimulationIntegration:
    """Integration tests for the full 4-epoch simulation."""

    def _run_simulation(self):
        """Helper: run all 4 epochs with 2 workers via KafkaTopic."""
        from simulate import PARTITIONS, _generate_burst, EPOCH_SCENARIOS
        from mock_kafka import KafkaTopic, Normalizer

        storage = MockStorage()
        catalog = MockCatalog()
        topic = KafkaTopic(num_partitions=2)
        normalizer = Normalizer()
        wa = RTMSFWorker(storage, catalog, W=0.1, G=0.1, enable_drop=True)
        wb = RTMSFWorker(storage, catalog, W=0.1, G=0.1, enable_drop=True)

        for i in range(len(EPOCH_SCENARIOS)):
            burst = _generate_burst(EPOCH_SCENARIOS[i], 6, 0.01, 100 + i, storage, i)
            for ev in burst:
                key = normalizer.extract_key(ev.object_key)
                topic.produce(key=key, value=ev)

            batch_0 = topic.poll_partition(0, 50)
            batch_1 = topic.poll_partition(1, 50)
            if batch_0:
                wa.process_batch([m.value for m in batch_0])
            if batch_1:
                wb.process_batch([m.value for m in batch_1])
            time.sleep(0.3)
            wa.flush_and_commit()
            wb.flush_and_commit()
            for _ in range(10):
                if not wa._retry_queue and not wb._retry_queue:
                    break
                wa.drain_retries()
                wb.drain_retries()
                time.sleep(0.1)

        return catalog, PARTITIONS

    def test_deterministic_catalog(self):
        """4 epochs with 2 workers → final catalog = {boston, miami, bangalore}."""
        catalog, PARTITIONS = self._run_simulation()
        final = sorted([k[2] for k in catalog._partitions.keys()])
        expected = sorted([PARTITIONS[p] for p in ["P1", "P2", "P4"]])
        assert final == expected, f"Expected {expected}, got {final}"

    def test_two_worker_routing_deterministic(self):
        """Same key always routes to same partition across runs."""
        from mock_kafka import KafkaTopic

        keys = [
            "warehouse/sales/country=us/city=boston/",
            "warehouse/sales/country=us/city=miami/",
            "warehouse/sales/country=us/city=seattle/",
            "warehouse/sales/country=india/city=bangalore/",
            "warehouse/sales/country=india/city=mumbai/",
        ]

        # Run twice, verify same partition assignment
        first_run = {}
        for run in range(2):
            topic = KafkaTopic(num_partitions=2)
            for k in keys:
                msg = topic.produce(k, "test")
                if run == 0:
                    first_run[k] = msg.partition
                else:
                    assert first_run[k] == msg.partition, f"{k} routed differently"

    def test_reproducible_across_runs(self):
        """Two full simulation runs produce identical final catalogs."""
        results = []
        for _ in range(2):
            catalog, _ = self._run_simulation()
            results.append(sorted([k[2] for k in catalog._partitions.keys()]))
        assert results[0] == results[1], f"Run 1: {results[0]}, Run 2: {results[1]}"


# ================================================================== E. KAFKA MODEL TESTS

class TestKafkaPartition:
    """KafkaPartition: per-partition offset tracking and polling."""

    def test_per_partition_offsets(self):
        """Each partition has independent sequential offsets starting from 1."""
        from mock_kafka import KafkaTopic

        topic = KafkaTopic(num_partitions=2)

        # Produce 3 messages to partition-0, 2 to partition-1
        topic.partitions[0].append("key-a", "val-1")
        topic.partitions[0].append("key-a", "val-2")
        topic.partitions[0].append("key-a", "val-3")
        topic.partitions[1].append("key-b", "val-4")
        topic.partitions[1].append("key-b", "val-5")

        assert topic.partitions[0].offset == 3
        assert topic.partitions[1].offset == 2
        assert topic.partitions[0].messages[0].offset == 1
        assert topic.partitions[0].messages[2].offset == 3
        assert topic.partitions[1].messages[0].offset == 1

    def test_poll_returns_unread(self):
        """poll() returns only unread messages and advances position."""
        from mock_kafka import KafkaPartition

        p = KafkaPartition(0)
        p.append("k", "v1")
        p.append("k", "v2")
        p.append("k", "v3")

        batch1 = p.poll(2)
        assert len(batch1) == 2
        assert batch1[0].value == "v1"
        assert batch1[1].value == "v2"

        batch2 = p.poll(10)
        assert len(batch2) == 1
        assert batch2[0].value == "v3"

        batch3 = p.poll(10)
        assert len(batch3) == 0

    def test_lag(self):
        """lag() counts unread messages."""
        from mock_kafka import KafkaPartition

        p = KafkaPartition(0)
        p.append("k", "v1")
        p.append("k", "v2")
        assert p.lag() == 2

        p.poll(1)
        assert p.lag() == 1

        p.poll(1)
        assert p.lag() == 0

    def test_keyed_routing(self):
        """Same key always routes to same partition."""
        from mock_kafka import KafkaTopic

        topic = KafkaTopic(num_partitions=3)

        # Produce same key 10 times
        partitions_hit = set()
        for _ in range(10):
            msg = topic.produce("warehouse/sales/country=us/city=seattle/", "event")
            partitions_hit.add(msg.partition)

        assert len(partitions_hit) == 1  # always same partition

    def test_different_keys_can_route_differently(self):
        """Different keys may route to different partitions."""
        from mock_kafka import KafkaTopic

        # With enough partitions, different keys will hit different partitions
        topic = KafkaTopic(num_partitions=10)
        keys = [
            "warehouse/sales/country=us/city=seattle/",
            "warehouse/sales/country=us/city=denver/",
            "warehouse/sales/country=india/city=mumbai/",
            "warehouse/sales/country=india/city=bangalore/",
        ]
        partitions_hit = set()
        for k in keys:
            msg = topic.produce(k, "event")
            partitions_hit.add(msg.partition)

        # With 10 partitions and 4 different keys, at least 2 different partitions
        assert len(partitions_hit) >= 2


class TestNormalizer:
    """Normalizer: extract Kafka key from S3 object key."""

    def test_extract_key(self):
        """Strips s3://bucket/ and filename, returns partition prefix."""
        from mock_kafka import Normalizer

        key = Normalizer.extract_key(
            "warehouse/sales/country=us/city=seattle/part-0001.parquet"
        )
        assert key == "warehouse/sales/country=us/city=seattle/"

    def test_extract_key_plain_path(self):
        """Works with plain object key (no s3:// prefix)."""
        from mock_kafka import Normalizer

        key = Normalizer.extract_key(
            "warehouse/sales/country=us/city=seattle/part-0001.parquet"
        )
        assert key == "warehouse/sales/country=us/city=seattle/"

    def test_extract_partition_key(self):
        """Extracts just the Hive k=v portion."""
        from mock_kafka import Normalizer
        from rtmsf_worker import TABLE_ROOT

        pkey = Normalizer.extract_partition_key(
            f"{TABLE_ROOT}country=us/city=seattle/part-0001.parquet",
            TABLE_ROOT,
        )
        assert pkey == "country=us/city=seattle"


# ================================================================== RUN

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
