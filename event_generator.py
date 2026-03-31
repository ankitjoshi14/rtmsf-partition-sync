"""
event_generator.py — Synthetic event stream for the RTMSF benchmark.

Maps to: §3.8.2 workload parameters (c₁=365, c₂=50, c₃=24, P=438,000).

Generates OBJECT_CREATED events for ΔP = f × P randomly chosen partitions
in a three-level hierarchy (dt × region × hour), with configurable:
  - files_per_partition: simulates multi-file writes per partition
  - duplicate_rate:      simulates at-least-once re-delivery
  - out-of-order jitter: events are shuffled before return
"""

import random
import time
from typing import List, Tuple

from rtmsf_worker import EventEnvelope, TABLE_ROOT, TENANT_ID

# --------------------------------------------------------- hierarchy constants
# Match exactly §3.8.2: c₁=365, c₂=50, c₃=24 → P=438,000

C1 = 365    # distinct values at level 1 (dt)
C2 = 50     # distinct values at level 2 (region)
C3 = 24     # distinct values at level 3 (hour)
P  = C1 * C2 * C3   # 438,000 total leaf partitions

# N_list,scan = Σ(k=0..3) Π(i=1..k) c_i  (from §3.8 formula)
N_LIST_SCAN = 1 + C1 + (C1 * C2) + (C1 * C2 * C3)   # = 456,616


# --------------------------------------------------------- key builders

def partition_key(dt: int, region: int, hour: int) -> str:
    """Canonical Hive-style partition key string."""
    month = (dt // 30) + 1
    day   = (dt % 30)  + 1
    return f"dt=2024-{month:02d}-{day:02d}/region=R{region:03d}/hour={hour:02d}"


def object_key(pkey: str, file_idx: int) -> str:
    return f"{TABLE_ROOT}{pkey}/part-{file_idx:04d}.parquet"


def partition_prefix(pkey: str) -> str:
    return f"{TABLE_ROOT}{pkey}/"


# --------------------------------------------------------- generator

def generate_events(
    f: float,
    files_per_partition: int = 5,
    duplicate_rate: float = 0.10,
    seed: int = 42,
) -> Tuple[List[EventEnvelope], int, List[str]]:
    """
    Generate synthetic OBJECT_CREATED events.

    Args:
      f:                    change fraction (ΔP/P); range [0.001, 1.0]
      files_per_partition:  file events per changed partition (default 5)
      duplicate_rate:       fraction of events duplicated (at-least-once, default 10%)
      seed:                 random seed for reproducibility

    Returns:
      events:           shuffled list of EventEnvelope (out-of-order delivery)
      delta_p:          number of changed partitions (ΔP = round(f × P))
      partition_keys:   list of partition_key strings that changed
    """
    random.seed(seed)
    base_time = time.time()

    delta_p = max(1, round(f * P))

    # Select ΔP unique partitions from the full 3-level space
    selected: set = set()
    while len(selected) < delta_p:
        dt     = random.randint(0, C1 - 1)
        region = random.randint(0, C2 - 1)
        hour   = random.randint(0, C3 - 1)
        selected.add((dt, region, hour))

    events: List[EventEnvelope] = []
    partition_keys: List[str]   = []

    for (dt, region, hour) in selected:
        pkey = partition_key(dt, region, hour)
        partition_keys.append(pkey)

        for file_idx in range(files_per_partition):
            okey     = object_key(pkey, file_idx)
            evt_time = base_time + random.uniform(-10.0, 10.0)   # jitter

            events.append(EventEnvelope(
                tenant_id  = TENANT_ID,
                op         = "OBJECT_CREATED",
                object_key = okey,
                event_time = evt_time,
            ))

            # Simulate at-least-once duplicate delivery
            if random.random() < duplicate_rate:
                events.append(EventEnvelope(
                    tenant_id  = TENANT_ID,
                    op         = "OBJECT_CREATED",
                    object_key = okey,
                    event_time = evt_time + 0.001,  # same file → coalesces naturally in WOERA
                ))

    # Shuffle to simulate out-of-order delivery (§3.1 assumption 2)
    random.shuffle(events)

    return events, delta_p, partition_keys


def generate_delete_events(
    partition_keys: List[str],
    drop_fraction: float,
    files_per_partition: int = 5,
    duplicate_rate: float = 0.10,
    seed: int = 99,
) -> Tuple[List[EventEnvelope], int, List[str]]:
    """
    Generate synthetic OBJECT_DELETED events for a fraction of existing partitions.

    Args:
      partition_keys:       list of partition_key strings (from generate_events)
      drop_fraction:        fraction of partition_keys to delete (0.0–1.0)
      files_per_partition:  file events per dropped partition (must match creation)
      duplicate_rate:       fraction of events duplicated (at-least-once)
      seed:                 random seed for reproducibility

    Returns:
      events:           shuffled list of OBJECT_DELETED EventEnvelope
      delta_drop:       number of partitions targeted for deletion
      dropped_keys:     list of partition_key strings that were deleted
    """
    random.seed(seed)
    base_time = time.time()

    delta_drop = max(1, round(drop_fraction * len(partition_keys)))
    dropped_keys = random.sample(partition_keys, delta_drop)

    events: List[EventEnvelope] = []

    for pkey in dropped_keys:
        for file_idx in range(files_per_partition):
            okey = object_key(pkey, file_idx)
            evt_time = base_time + random.uniform(-10.0, 10.0)

            events.append(EventEnvelope(
                tenant_id  = TENANT_ID,
                op         = "OBJECT_DELETED",
                object_key = okey,
                event_time = evt_time,
            ))

            if random.random() < duplicate_rate:
                events.append(EventEnvelope(
                    tenant_id  = TENANT_ID,
                    op         = "OBJECT_DELETED",
                    object_key = okey,
                    event_time = evt_time + 0.001,
                ))

    random.shuffle(events)
    return events, delta_drop, dropped_keys
