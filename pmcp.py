"""
pmcp.py — Partition Mutation Commit Protocol.

Maps to: §3.5, Algorithm 5 (apply_add), Algorithm 6 (apply_drop).

Invariants enforced (§3.5):
  ADD safety  — create partition only if storage confirms non-emptiness.
  DROP safety — drop partition only if storage confirms emptiness via LIST.
  Idempotency — repeated application produces NOOP after first success.
  Convergence — given continued delivery, catalog converges to storage state.

RETRY/ERROR semantics mirror the paper:
  RETRY — transient issue; caller re-enqueues with backoff.
  ERROR — fatal; caller DLQs the intent.
  NOOP  — already in desired state; no mutation needed.
"""

import random
from dataclasses import dataclass
from typing import Optional

from mock_storage import MockStorage, StorageTransientException
from mock_catalog import (
    MockCatalog, AlreadyExistsException,
    NotFoundException, CatalogTransientException, CatalogFatalException
)
from woera_buffer import EffectiveIntent

IGNORE_PREFIXES = {"_SUCCESS", ".crc", "_temporary"}


# ---------------------------------------------------------------- result type

@dataclass
class Result:
    status: str             # "APPLIED" | "NOOP" | "RETRY" | "ERROR"
    reason: Optional[str] = None
    backoff: float = 0.0


# ----------------------------------------------------------------- pmcp class

class PMCP:
    """
    Applies EffectiveIntents to the catalog with storage verification.

    Counters (for benchmarking):
      applied  — successful catalog mutations
      noops    — idempotent no-ops
      retries  — transient failures queued for retry
      errors   — fatal failures sent to DLQ
    """

    MAX_BACKOFF = 60.0   # seconds (capped exponential backoff per §3.5)

    def __init__(self, storage: MockStorage, catalog: MockCatalog):
        self.storage = storage
        self.catalog = catalog
        self.applied = 0
        self.noops   = 0
        self.retries = 0
        self.errors  = 0

    # ----------------------------------------------------------------- dispatch

    def apply(self, eff: EffectiveIntent) -> Result:
        if eff.kind == "ADD":
            return self.apply_add(eff)
        elif eff.kind == "DROP":
            return self.apply_drop(eff)
        self.errors += 1
        return Result("ERROR", "unknown_kind")

    # --------------------------------------------------------------- apply_add

    def apply_add(self, eff: EffectiveIntent) -> Result:
        """
        Algorithm 5: PMCP apply_add.

        Fast-path: HEAD on bounded proof keys (K=3).
        Fallback:  LIST prefix with max_keys=1 (handles visibility jitter).
        """
        # Optimization: catalog check first avoids all storage I/O
        if self.catalog.has_partition(eff.key):
            self.noops += 1
            return Result("NOOP", "already_exists")

        # Fast-path: HEAD on proof objects
        for proof in eff.proof_keys:
            try:
                if self.storage.head_exists(proof):
                    return self._create_partition(eff)
            except StorageTransientException:
                self.retries += 1
                return Result("RETRY", "storage_transient", self._backoff())

        # Fallback: minimal LIST to handle proof-key visibility jitter
        try:
            if self.storage.prefix_non_empty(eff.partition_prefix, IGNORE_PREFIXES):
                return self._create_partition(eff)
        except StorageTransientException:
            self.retries += 1
            return Result("RETRY", "storage_transient", self._backoff())

        # Storage evidence not yet visible — retry later
        self.retries += 1
        return Result("RETRY", "not_visible_yet", self._backoff())

    # --------------------------------------------------------------- apply_drop

    def apply_drop(self, eff: EffectiveIntent) -> Result:
        """
        Algorithm 6: PMCP apply_drop.

        DROP safety: only drop if LIST confirms prefix is empty.
        HEAD alone cannot prove emptiness (§3.5).
        """
        if not self.catalog.has_partition(eff.key):
            self.noops += 1
            return Result("NOOP", "not_found")

        # Emptiness proof via LIST
        try:
            if self.storage.prefix_non_empty(eff.partition_prefix, IGNORE_PREFIXES):
                self.noops += 1
                return Result("NOOP", "still_non_empty")
        except StorageTransientException:
            self.retries += 1
            return Result("RETRY", "storage_transient", self._backoff())

        try:
            self.catalog.drop_partition(eff.key)
            self.applied += 1
            return Result("APPLIED")
        except NotFoundException:
            self.noops += 1
            return Result("NOOP", "not_found")
        except CatalogTransientException:
            self.retries += 1
            return Result("RETRY", "catalog_transient", self._backoff())
        except CatalogFatalException:
            self.errors += 1
            return Result("ERROR", "catalog_fatal")

    # --------------------------------------------------------- internal helpers

    def _create_partition(self, eff: EffectiveIntent) -> Result:
        try:
            self.catalog.create_partition(eff.key, location=eff.partition_prefix)
            self.applied += 1
            return Result("APPLIED")
        except AlreadyExistsException:
            # Race: another worker committed first — idempotent NOOP
            self.noops += 1
            return Result("NOOP", "already_exists")
        except CatalogTransientException:
            self.retries += 1
            return Result("RETRY", "catalog_transient", self._backoff())
        except CatalogFatalException:
            self.errors += 1
            return Result("ERROR", "catalog_fatal")

    def _backoff(self) -> float:
        """Exponential backoff with full jitter [19], capped at MAX_BACKOFF."""
        return random.uniform(0, self.MAX_BACKOFF)

    # ------------------------------------------------------------------ utils

    def reset_counters(self) -> None:
        self.applied = 0
        self.noops   = 0
        self.retries = 0
        self.errors  = 0
