"""
rtmsf_worker.py — RTMSF main worker loop.

Maps to: Algorithm 1 (rtmsf_worker), Algorithm 2 (flush_and_commit),
         Algorithm 3 (reduce_to_candidate_intent), §3.2 pipeline stages.

Single-threaded per §3.2: "The worker loop is single-threaded to simplify
WOERABuffer state management."
"""

import hashlib
import time
from dataclasses import dataclass, field
from typing import List, Optional, Callable

from mock_storage import MockStorage
from mock_catalog import MockCatalog
from woera_buffer import WOERABuffer, CandidateIntent
from pmcp import PMCP

# --------------------------------------------------------- routing constants

TABLE_ROOT   = "bucket://datalake/warehouse/sales/"
TENANT_ID    = "tenant-1"
TABLE_FQN    = "warehouse.sales"

DATA_EXTENSIONS  = {".parquet", ".orc", ".avro", ".csv"}
IGNORE_SUFFIXES  = {"_SUCCESS", ".crc", ".tmp"}


# ---------------------------------------------------------------- event type

@dataclass
class EventEnvelope:
    """Minimal cloud-agnostic event (§3.1 event envelope)."""
    tenant_id:  str
    table_fqn:  str
    op:         str          # "OBJECT_CREATED" | "OBJECT_DELETED"
    object_key: str
    event_time: float
    event_id:   str


# ---------------------------------------------------------------- worker

class RTMSFWorker:
    """
    Implements Algorithms 1 & 2 end-to-end.

    Metrics tracked for benchmark.py:
      events_received   — total raw events ingested
      events_filtered   — events discarded (non-data, wrong prefix, etc.)
      intents_buffered  — CandidateIntents added to WOERABuffer
      commits_applied   — successful PMCP catalog mutations
      latencies         — list of (event_first_seen → catalog commit) seconds
    """

    def __init__(
        self,
        storage: MockStorage,
        catalog: MockCatalog,
        W: float = 5.0,
        G: float = 1.0,
        clock_fn: Callable = None,
    ):
        self._clock   = clock_fn or time.time
        self.buffer   = WOERABuffer(W=W, G=G, clock_fn=self._clock)
        self.pmcp     = PMCP(storage, catalog)
        self.storage  = storage
        self.catalog  = catalog

        # Metrics
        self.events_received:  int = 0
        self.events_filtered:  int = 0
        self.intents_buffered: int = 0
        self.commits_applied:  int = 0
        self.latencies: List[float] = []

        # Per-partition first-event timestamp (for latency measurement)
        self._first_seen: dict = {}

    # --------------------------------------------------------- Algorithm 1

    def process_batch(self, events: List[EventEnvelope]) -> None:
        """
        Algorithm 1: RTMSF Worker Loop (one poll batch).

        For each event: normalize → validate → route → reduce → buffer.
        flush_and_commit() is called once after the full batch.
        """
        for envelope in events:
            self.events_received += 1

            # Validate op type
            if envelope.op not in ("OBJECT_CREATED", "OBJECT_DELETED"):
                self.events_filtered += 1
                continue

            # Filter non-data files (ignore list + extension check)
            if not self._is_data_file(envelope.object_key):
                self.events_filtered += 1
                continue

            # Reduce to CandidateIntent (Algorithm 3)
            intent = self._reduce_to_intent(envelope)
            if intent is None:
                self.events_filtered += 1
                continue

            # Record first-seen time for latency tracking
            pkey = intent.partition_key
            if pkey not in self._first_seen:
                self._first_seen[pkey] = self._clock()

            # Reflect object creation in mock storage for PMCP proofs
            if envelope.op == "OBJECT_CREATED":
                self.storage.put(envelope.object_key)
            else:
                self.storage.delete(envelope.object_key)

            self.buffer.add(intent)
            self.intents_buffered += 1

        # Flush once per batch (maximises WOERA coalescing)
        self.flush_and_commit()

    # --------------------------------------------------------- Algorithm 2

    def flush_and_commit(self) -> int:
        """
        Algorithm 2: flush_and_commit.

        For each flush-eligible key: pop → reconcile → PMCP.apply → audit.
        """
        committed = 0
        for key in self.buffer.ready_keys():
            intents  = self.buffer.pop(key)
            eff_list = self.buffer.reconcile(intents, key)
            for eff in eff_list:
                result = self.pmcp.apply(eff)
                if result.status == "APPLIED":
                    committed += 1
                    self.commits_applied += 1
                    # Record convergence latency
                    pkey = key[2]
                    if pkey in self._first_seen:
                        self.latencies.append(self._clock() - self._first_seen[pkey])
        return committed

    def force_flush_all(self) -> int:
        """
        Drain all buffered keys regardless of watermark.
        Used at experiment end to get final committed count.
        """
        committed = 0
        for key in self.buffer.force_flush_all():
            intents  = self.buffer.pop(key)
            eff_list = self.buffer.reconcile(intents, key)
            for eff in eff_list:
                result = self.pmcp.apply(eff)
                if result.status == "APPLIED":
                    committed += 1
                    self.commits_applied += 1
                    pkey = key[2]
                    if pkey in self._first_seen:
                        self.latencies.append(self._clock() - self._first_seen[pkey])
        return committed

    # --------------------------------------------------------- Algorithm 3

    def _reduce_to_intent(self, envelope: EventEnvelope) -> Optional[CandidateIntent]:
        """
        Algorithm 3: reduce_to_candidate_intent.

        Parses Hive-style k=v path segments into a canonical partition_key.
        Returns None for unroutable or non-partition paths.
        """
        key = envelope.object_key

        # Route: must be under the registered table root
        if not key.startswith(TABLE_ROOT):
            return None

        relative = key[len(TABLE_ROOT):]
        segments = relative.split("/")

        # Extract k=v path segments (exclude filename at end)
        kv_parts = [seg for seg in segments[:-1] if "=" in seg]
        if not kv_parts:
            return None

        partition_key    = "/".join(kv_parts)
        partition_prefix = f"{TABLE_ROOT}{partition_key}/"
        kind = "ADD_CANDIDATE" if envelope.op == "OBJECT_CREATED" else "DROP_CANDIDATE"

        # Stable dedup key: prefer provider event_id, else hash
        if envelope.event_id:
            dedup_key = envelope.event_id
        else:
            raw = f"{key}:{envelope.op}:{envelope.event_time}"
            dedup_key = hashlib.md5(raw.encode()).hexdigest()

        proof = envelope.object_key if kind == "ADD_CANDIDATE" else None

        return CandidateIntent(
            key              = (envelope.tenant_id, envelope.table_fqn, partition_key),
            partition_key    = partition_key,
            partition_prefix = partition_prefix,
            kind             = kind,
            event_time       = envelope.event_time,
            dedup_key        = dedup_key,
            proof            = proof,
        )

    # --------------------------------------------------------- helpers

    def _is_data_file(self, object_key: str) -> bool:
        filename = object_key.split("/")[-1]
        if any(filename.endswith(ig) for ig in IGNORE_SUFFIXES):
            return False
        return any(object_key.endswith(ext) for ext in DATA_EXTENSIONS)

    def reset_metrics(self) -> None:
        self.events_received  = 0
        self.events_filtered  = 0
        self.intents_buffered = 0
        self.commits_applied  = 0
        self.latencies        = []
        self._first_seen      = {}
