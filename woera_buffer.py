"""
woera_buffer.py — WOERA: Weakly Ordered Event Reconciliation Algorithm.

Maps to: §3.4 (WOERA parameters, correctness properties, complexity),
         Algorithm 4 (reconcile), Appendix A.5 (WOERABuffer class).

Key design choices (mirroring the paper):
  - Per-key quiet window W: flush_at[k] = now() + W, extended on each new intent.
  - Per-worker grace interval G: wm_worker = now() - G.
  - Optional T_max: flush_at[k] capped at first_seen[k] + T_max so continuously
    active keys are force-flushed after at most T_max + G seconds (§3.4, Appendix B).
    When T_max is None, pure quiet-window semantics apply (Termination requires
    activity to eventually cease).
  - Min-heap + version counters for O(log A) flush scheduling (lazy updates).
  - ADD-dominant rule: ADD wins over DROP within the same window.

Deduplication is a natural byproduct of coalescing — WOERA emits one intent
per partition key regardless of how many events arrive.
"""

import heapq
import time
from dataclasses import dataclass, field
from typing import List, Optional, Callable


# ---------------------------------------------------------------- data types

@dataclass
class CandidateIntent:
    """Output of intent reduction (Algorithm 3). Input to WOERABuffer."""
    key: tuple                  # (tenant_id, table_fqn, partition_key_str)
    partition_key: str          # canonical "dt=.../region=.../hour=..." string
    partition_prefix: str       # full object-store prefix for this partition
    kind: str                   # "ADD_CANDIDATE" | "DROP_CANDIDATE"
    proof: Optional[str]        # object_key for ADD; None for DROP


@dataclass
class EffectiveIntent:
    """Output of WOERA reconciliation. Input to PMCP."""
    key: tuple                  # (tenant_id, table_fqn, partition_key_str)
    partition_prefix: str
    kind: str                   # "ADD" | "DROP"
    proof_keys: List[str]       # bounded (K=3); empty for DROP


# --------------------------------------------------------------- buffer class

class WOERABuffer:
    """
    Per-shard, in-memory buffer implementing WOERA.

    Complexity (from §3.4):
      add()         — O(log A)  where A = active key count
      ready_keys()  — O(log A)  per released key (amortized)
      pop()         — O(1)
      reconcile()   — O(b)      where b = intents buffered for the key
    """

    MAX_PROOF_KEYS = 3  # K in the paper

    def __init__(self, W: float, G: float, T_max: Optional[float] = None, clock_fn: Callable = None):
        """
        W:       quiet-window seconds — how long to wait after last intent before flushing.
        G:       grace-interval seconds — watermark lag (wm_worker = now() - G).
        T_max:   maximum buffering deadline (§3.4, Appendix B). Defaults to 5*W per
                 paper recommendation. flush_at[k] is capped at first_seen[k] + T_max;
                 the key becomes flush-eligible when now() >= first_seen[k] + T_max + G.
        clock_fn: injectable clock for testing/simulation (default: time.time).
        """
        self.W     = W
        self.G     = G
        self.T_max = T_max if T_max is not None else 5 * W
        self._clock = clock_fn or time.time

        self._events:     dict = {}    # key -> List[CandidateIntent]
        self._flush_at:   dict = {}    # key -> float
        self._first_seen: dict = {}    # key -> float  (time of first intent for T_max cap)
        self._version:    dict = {}    # key -> int    (lazy heap invalidation)
        self._heap:       list = []    # min-heap of (flush_at, key, version)

    # ------------------------------------------------------------------- add

    def add(self, intent: CandidateIntent) -> None:
        """
        Buffer one intent. Extends flush_at[k] forward by W from now.
        If T_max is set, caps flush_at[k] at first_seen[k] + T_max (Appendix A.5).
        Each call increments version[k] to invalidate stale heap entries.
        """
        k = intent.key
        self._events.setdefault(k, []).append(intent)
        if k not in self._first_seen:
            self._first_seen[k] = self._clock()
        self._flush_at[k] = self._clock() + self.W
        if self.T_max is not None:
            self._flush_at[k] = min(self._flush_at[k], self._first_seen[k] + self.T_max)
        self._version[k] = self._version.get(k, 0) + 1
        heapq.heappush(self._heap, (self._flush_at[k], k, self._version[k]))

    # ---------------------------------------------------------- flush eligibility

    def ready_keys(self) -> List[tuple]:
        """
        Return keys whose flush_at <= wm_worker (processing-time watermark).
        Skips stale heap entries using version counters (lazy deletion).
        """
        wm = self._clock() - self.G
        ready = []
        while self._heap and self._heap[0][0] <= wm:
            t, k, v = heapq.heappop(self._heap)
            if self._version.get(k, -1) != v:
                continue          # stale entry — key was updated after this push
            if k in self._events:
                ready.append(k)
        return ready

    def force_flush_all(self) -> List[tuple]:
        """
        Return all buffered keys regardless of watermark.
        Used in benchmarks to drain the buffer at experiment end.
        """
        return list(self._events.keys())

    # ------------------------------------------------------------------- pop

    def pop(self, key: tuple) -> List[CandidateIntent]:
        """Atomically remove and return all buffered intents for key."""
        intents = self._events.pop(key, [])
        self._flush_at.pop(key, None)
        self._first_seen.pop(key, None)
        self._version.pop(key, None)
        return intents

    # --------------------------------------------------------------- reconcile

    def reconcile(self, intents: List[CandidateIntent], key: tuple) -> List[EffectiveIntent]:
        """
        Algorithm 4: WOERA reconciliation.

        1. ADD-dominant rule: if any ADD_CANDIDATE present, emit single ADD.
        2. DROP-only: if only DROP_CANDIDATEs, emit single DROP.

        Deduplication is implicit: coalescing emits one EffectiveIntent per
        partition key regardless of how many candidate events arrived.
        """
        if not intents:
            return []

        has_add  = any(i.kind == "ADD_CANDIDATE"  for i in intents)
        has_drop = any(i.kind == "DROP_CANDIDATE" for i in intents)
        prefix   = intents[0].partition_prefix

        # ADD dominant
        if has_add:
            proofs = [i.proof for i in intents
                      if i.proof is not None][:self.MAX_PROOF_KEYS]
            return [EffectiveIntent(
                key=key,
                partition_prefix=prefix,
                kind="ADD",
                proof_keys=proofs,
            )]

        # DROP only
        if has_drop:
            return [EffectiveIntent(
                key=key,
                partition_prefix=prefix,
                kind="DROP",
                proof_keys=[],
            )]

        return []

    # ------------------------------------------------------------------ utils

    @property
    def active_key_count(self) -> int:
        return len(self._events)

    @property
    def buffer_snapshot(self) -> dict:
        """Read-only snapshot of buffer state for display/simulation use."""
        now = self._clock()
        result = {}
        for key, intents in self._events.items():
            flush_at   = self._flush_at.get(key, now)
            first_seen = self._first_seen.get(key, now)
            result[key] = {
                "count":    len(intents),
                "flush_in": max(0.0, flush_at - now),
                "age_s":    now - first_seen,
                "kind":     intents[-1].kind if intents else "UNKNOWN",
            }
        return result
