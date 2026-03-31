"""
sim_scenarios.py — Epoch definitions + event generation for RTMSF simulation.
"""

import time
from rtmsf_worker import EventEnvelope, TABLE_ROOT, TENANT_ID
from mock_storage import MockStorage
from event_generator import generate_events


# ------------------------------------------------------------------ fixed partition pool

PARTITIONS = {
    "P1": "country=us/city=boston",
    "P2": "country=us/city=miami",
    "P3": "country=us/city=seattle",
    "P4": "country=india/city=bangalore",
}


# ------------------------------------------------------------------ epoch scenarios

EPOCH_SCENARIOS = [
    {
        "title":              "Normal Flow",
        "tagline":            "baseline: event → WOERA coalesce → PMCP verify → catalog",
        "force_ooo":          False,
        "force_dup":          False,
        "force_repeat":       False,
        "not_ready_override": None,   # use --not-ready-rate CLI arg
        "what_happened": "A standard burst: files uploaded, notifications delivered in order, WOERA held them for the quiet window, PMCP confirmed each file via HEAD, and partitions were committed. This is the happy path.",
    },
    {
        "title":              "Transient Delete + Create (Compaction)",
        "tagline":            "DELETE and CREATE in same window — ADD-dominant rule absorbs the delete",
        "force_ooo":          True,
        "force_dup":          False,
        "force_repeat":       False,
        "not_ready_override": 0.0,
        "what_happened": "A compaction job deleted old partition files and wrote new ones. OBJECT_DELETED and OBJECT_CREATED notifications arrived in the same WOERA quiet window. ADD-dominant rule: any ADD evidence suppresses the DROP_CANDIDATE — the partition is kept, not removed. PMCP confirmed the new files and committed the ADD. This is the transient-delete pattern addressed in §3.4.",
    },
    {
        "title":              "Duplicate Resilience (At-Least-Once)",
        "tagline":            "duplicate events arrive — WOERA coalesces them naturally",
        "force_ooo":          False,
        "force_dup":          True,
        "force_repeat":       False,
        "not_ready_override": 0.0,
        "what_happened": "Kafka delivers at-least-once. Duplicate events arrived for the same partition. WOERA's ADD-dominant coalescing absorbed them naturally — multiple ADD_CANDIDATEs for the same partition key collapse into a single ADD EffectiveIntent. No explicit dedup step needed. PMCP's idempotent commit ensures the catalog mutation happens exactly once regardless of how many notifications fired.",
    },
    {
        "title":              "Partition Drop",
        "tagline":            "files deleted — PMCP verifies empty via LIST then removes from catalog",
        "force_ooo":          False,
        "force_dup":          False,
        "force_repeat":       False,
        "force_drop":         True,
        "not_ready_override": 0.0,
        "what_happened": "Files were deleted from object storage. A DROP intent was generated for each affected partition. WOERA buffered them through the quiet window. PMCP issued LIST on each partition prefix — confirmed empty — and removed the partition from the catalog. Catalog shrinks. This is the safe drop path: LIST proof prevents removing a partition while files still exist (e.g., from a concurrent upload).",
    },
]


# ------------------------------------------------------------------ event generation

def _make_event(pkey_str: str, filename: str, op: str = "OBJECT_CREATED",
                epoch: int = 0, seq: int = 0):
    """Create a deterministic EventEnvelope + object_key from fixed partition info."""
    okey = f"{TABLE_ROOT}{pkey_str}/{filename}"
    return EventEnvelope(
        tenant_id=TENANT_ID,
        op=op, object_key=okey,
        event_time=time.time() + seq * 0.001,
    ), okey


def _generate_burst(
    scenario: dict,
    burst_size: int,
    fraction: float,
    seed: int,
    storage: MockStorage,
    epoch_num: int,
) -> list:
    """
    Return a list of EventEnvelope objects for the current epoch using
    deterministic, fixed partitions from the PARTITIONS pool.

    Epoch plan:
      0 — Normal Flow:         ADD P1(2 files), P2(2 files), P3(2 files)
      1 — Compaction:          P1: 1×DELETE(old) + 2×CREATE(new)
      2 — Duplicate Resilience: P4: 2 unique files, each sent twice (coalesces in WOERA)
      3 — Partition Drop:      P3: DELETE all files → PMCP LIST-verifies empty → drop

    Final catalog: {P1, P2, P4} = {boston, miami, bangalore}
    """

    if epoch_num == 0:
        # ---- Normal Flow: ADD P1, P2, P3 (2 files each) ----
        events = []
        for label in ["P1", "P2", "P3"]:
            pkey = PARTITIONS[label]
            for i in range(2):
                ev, okey = _make_event(pkey, f"part-{i:04d}.parquet",
                                       epoch=epoch_num, seq=len(events))
                events.append(ev)
                storage.put(okey)
        return events

    elif epoch_num == 1:
        # ---- Compaction: P1 DELETE old file + CREATE new files ----
        pkey   = PARTITIONS["P1"]
        events = []
        # DELETE old file
        ev, okey = _make_event(pkey, "part-0000.parquet", op="OBJECT_DELETED",
                               epoch=epoch_num, seq=0)
        events.append(ev)
        storage.delete(okey)
        # CREATE new compacted files
        for i in range(1, 3):
            ev, okey = _make_event(pkey, f"part-{i:04d}.parquet",
                                   epoch=epoch_num, seq=i)
            events.append(ev)
            storage.put(okey)
        return events

    elif epoch_num == 2:
        # ---- Duplicate Resilience: P4 — 2 unique files, each sent twice ----
        pkey   = PARTITIONS["P4"]
        events = []
        for i in range(2):
            ev, okey = _make_event(pkey, f"part-{i:04d}.parquet",
                                   epoch=epoch_num, seq=i)
            events.append(ev)
            storage.put(okey)
            # Duplicate: same object_key, slightly different time — coalesces in WOERA
            dup = EventEnvelope(
                tenant_id=TENANT_ID,
                op="OBJECT_CREATED", object_key=ev.object_key,
                event_time=ev.event_time + 0.001,
            )
            events.append(dup)
        return events

    elif epoch_num == 3:
        # ---- Partition Drop: DELETE all P3 files from storage ----
        pkey   = PARTITIONS["P3"]
        prefix = f"{TABLE_ROOT}{pkey}/"
        # Remove all P3 files from storage so PMCP LIST confirms empty
        storage._objects = {
            o for o in storage._objects if not o.startswith(prefix)
        }
        # Generate DELETE events for the 2 original P3 files
        events = []
        for i in range(2):
            ev, _ = _make_event(pkey, f"part-{i:04d}.parquet",
                                op="OBJECT_DELETED", epoch=epoch_num, seq=i)
            events.append(ev)
        return events

    else:
        # Beyond planned epochs: fall back to normal generation
        evts, _, _ = generate_events(
            f=fraction,
            files_per_partition=3,
            duplicate_rate=0.20,
            seed=seed,
        )
        return evts[:burst_size]
