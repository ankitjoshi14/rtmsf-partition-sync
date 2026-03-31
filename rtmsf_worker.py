"""
rtmsf_worker.py — RTMSF main worker loop.

Maps to: Algorithm 1 (rtmsf_worker), Algorithm 2 (flush_and_commit),
         Algorithm 3 (reduce_to_intent), §3.2 pipeline stages.

Single-threaded per §3.2: "The worker loop is single-threaded to simplify
WOERABuffer state management."
"""

import random
import time
from dataclasses import dataclass, field
from typing import List, Optional, Callable

from mock_storage import MockStorage
from mock_catalog import MockCatalog
from woera_buffer import WOERABuffer, CandidateIntent
from pmcp import PMCP

# --------------------------------------------------------- routing constants

TABLE_ROOT   = "warehouse/sales/"
TENANT_ID    = "tenant-1"
TABLE_FQN    = "warehouse.sales"

DATA_EXTENSIONS  = {".parquet", ".orc", ".avro", ".csv"}
IGNORE_SUFFIXES  = {"_SUCCESS", ".crc", ".tmp"}


# ---------------------------------------------------------------- event type

@dataclass
class EventEnvelope:
    """Minimal cloud-agnostic event (§3.1 event envelope)."""
    tenant_id:  str
    op:         str          # "OBJECT_CREATED" | "OBJECT_DELETED"
    object_key: str
    event_time: float


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
        enable_drop: bool = True,
        clock_fn: Callable = None,
    ):
        self._clock   = clock_fn or time.time
        self.buffer   = WOERABuffer(W=W, G=G, clock_fn=self._clock)
        self.pmcp     = PMCP(storage, catalog, enable_drop=enable_drop)
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

        # Last flush results: pkey -> {"count": N, "kind": "ADD"|"DROP"}
        self.last_flushed: dict = {}

        # Retry queue: bounded retries with DLQ after MAX_RETRIES
        self.MAX_RETRIES:    int   = 5
        self.MAX_BACKOFF:    float = 60.0  # seconds (cap for exponential backoff)
        self._retry_counts:  dict  = {}    # key -> retry attempt count
        self._retry_queue:   list  = []    # list of (eff, attempt, retry_at)
        self.dlq:            list  = []    # dead-letter queue after MAX_RETRIES

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
        RETRY results are queued with bounded retries; after MAX_RETRIES, moved to DLQ.
        ERROR results go directly to DLQ.
        """
        committed = 0
        for key in self.buffer.ready_keys():
            intents  = self.buffer.pop(key)
            eff_list = self.buffer.reconcile(intents, key)
            pkey_str = key[2] if isinstance(key, tuple) else str(key)
            for eff in eff_list:
                self.last_flushed[pkey_str] = {"count": len(intents), "kind": eff.kind}
                result = self.pmcp.apply(eff)
                if result.status == "APPLIED":
                    committed += 1
                    self.commits_applied += 1
                    pkey = key[2]
                    if pkey in self._first_seen:
                        self.latencies.append(self._clock() - self._first_seen[pkey])
                    self._retry_counts.pop(key, None)
                elif result.status == "RETRY":
                    attempt = self._retry_counts.get(key, 0) + 1
                    if attempt <= self.MAX_RETRIES:
                        self._retry_counts[key] = attempt
                        backoff = random.uniform(0, min(2 ** attempt, self.MAX_BACKOFF))
                        self._retry_queue.append((eff, attempt, self._clock() + backoff))
                    else:
                        self.dlq.append(eff)
                        self._retry_counts.pop(key, None)
                elif result.status == "ERROR":
                    self.dlq.append(eff)
                    self._retry_counts.pop(key, None)
        return committed

    def drain_retries(self) -> int:
        """
        Re-apply RETRY intents whose backoff timer has elapsed.
        Uses exponential backoff with full jitter: each retry waits longer.
        Returns number of retries that resolved to APPLIED.
        """
        now = self._clock()
        resolved = 0
        still_pending = []
        for (eff, attempt, retry_at) in self._retry_queue:
            if now < retry_at:
                still_pending.append((eff, attempt, retry_at))
                continue
            key    = eff.key
            result = self.pmcp.apply(eff)
            if result.status == "APPLIED":
                resolved += 1
                self.commits_applied += 1
                pkey = key[2]
                if pkey in self._first_seen:
                    self.latencies.append(now - self._first_seen[pkey])
                self._retry_counts.pop(key, None)
            elif result.status == "RETRY":
                if attempt < self.MAX_RETRIES:
                    new_attempt = attempt + 1
                    self._retry_counts[key] = new_attempt
                    backoff = random.uniform(0, min(2 ** new_attempt, self.MAX_BACKOFF))
                    new_retry_at = now + backoff
                    still_pending.append((eff, new_attempt, new_retry_at))
                else:
                    self.dlq.append(eff)
                    self._retry_counts.pop(key, None)
            else:
                self.dlq.append(eff)
                self._retry_counts.pop(key, None)
        self._retry_queue = still_pending
        return resolved

    def force_flush_all(self) -> int:
        """
        Drain all buffered keys regardless of watermark.
        Used at experiment end to get final committed count.
        """
        committed = 0
        for key in self.buffer.force_flush_all():
            intents  = self.buffer.pop(key)
            eff_list = self.buffer.reconcile(intents, key)
            pkey_str = key[2] if isinstance(key, tuple) else str(key)
            for eff in eff_list:
                self.last_flushed[pkey_str] = {"count": len(intents), "kind": eff.kind}
                result = self.pmcp.apply(eff)
                if result.status == "APPLIED":
                    committed += 1
                    self.commits_applied += 1
                    pkey = key[2]
                    if pkey in self._first_seen:
                        self.latencies.append(self._clock() - self._first_seen[pkey])
                elif result.status == "RETRY":
                    attempt = self._retry_counts.get(key, 0) + 1
                    if attempt <= self.MAX_RETRIES:
                        self._retry_counts[key] = attempt
                        backoff = random.uniform(0, min(2 ** attempt, self.MAX_BACKOFF))
                        self._retry_queue.append((eff, attempt, self._clock() + backoff))
                    else:
                        self.dlq.append(eff)
        return committed

    # --------------------------------------------------------- Algorithm 3

    def _reduce_to_intent(self, envelope: EventEnvelope) -> Optional[CandidateIntent]:
        """
        Algorithm 3: reduce_to_intent.

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

        proof = envelope.object_key if kind == "ADD_CANDIDATE" else None

        # Table identity resolved via Routing Index (§3.2, Appendix A.1)
        # In this prototype, single-table: TABLE_FQN constant.
        return CandidateIntent(
            key              = (envelope.tenant_id, TABLE_FQN, partition_key),
            partition_key    = partition_key,
            partition_prefix = partition_prefix,
            kind             = kind,
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
        self._retry_counts    = {}
        self._retry_queue     = []
        self.dlq              = []
