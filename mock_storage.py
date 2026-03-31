"""
mock_storage.py — Simulates an object store (e.g., S3 / OCI Object Storage).

Maps to: §3.1 assumptions 4-5, PMCP HEAD/LIST operations (Algorithms 5 & 6).
All operations are in-memory. HEAD and LIST request counts are tracked
so benchmark.py can compare scan-based vs event-driven request costs.

Optional visibility jitter: not_ready_rate simulates S3 eventual consistency
where a newly written file may not be visible to HEAD/LIST immediately.
"""

import random


class StorageTransientException(Exception):
    pass


class MockStorage:
    """
    In-memory object store with optional visibility jitter.

    Supports:
      put(key)                          — simulate object write
      delete(key)                       — simulate object delete
      head_exists(key)                  — HEAD request (O(1))
      prefix_non_empty(prefix)          — LIST max_keys=1 (O(n) scan over keys)

    Visibility jitter (not_ready_rate > 0):
      Recently put objects may be temporarily invisible to head_exists
      and prefix_non_empty, simulating S3 eventual consistency. Objects
      become visible after visibility_window seconds.

    Counters:
      head_requests, list_requests      — tracked for cost comparison
    """

    def __init__(self, not_ready_rate: float = 0.0):
        self._objects: set = set()
        self._confirmed_visible: set = set()  # objects confirmed visible (no more jitter)
        self.not_ready_rate: float = not_ready_rate
        self.head_requests: int = 0
        self.list_requests: int = 0
        self._fail_next_head: bool = False
        self._fail_next_list: bool = False

    # ------------------------------------------------------------------ writes

    def put(self, object_key: str) -> None:
        self._objects.add(object_key)

    def delete(self, object_key: str) -> None:
        self._objects.discard(object_key)

    # ------------------------------------------------------------------ reads

    def _is_visible(self, object_key: str) -> bool:
        """Check if an object is visible given current not_ready_rate.
        Once an object is confirmed visible, it stays visible permanently.
        Simulates S3 eventual consistency: write propagation delay."""
        if self.not_ready_rate <= 0:
            return True
        if object_key in self._confirmed_visible:
            return True
        if random.random() >= self.not_ready_rate:
            self._confirmed_visible.add(object_key)
            return True
        return False

    def fail_next_head(self):
        self._fail_next_head = True

    def fail_next_list(self):
        self._fail_next_list = True

    def head_exists(self, object_key: str) -> bool:
        """Simulate a HEAD request. Counts as one billed operation.
        Subject to visibility jitter (not_ready_rate): may return False
        for objects that exist, simulating S3 eventual consistency."""
        self.head_requests += 1
        if self._fail_next_head:
            self._fail_next_head = False
            raise StorageTransientException("injected HEAD failure")
        if object_key not in self._objects:
            return False
        return self._is_visible(object_key)

    def prefix_non_empty(self, prefix: str, ignore_prefixes=None) -> bool:
        """
        Simulate LIST prefix with max_keys=1.
        Returns True if at least one non-ignored object exists under prefix.
        Subject to same visibility jitter as HEAD to demonstrate PMCP RETRY.
        (In modern S3, LIST is strongly consistent, but other providers may not be.)
        """
        self.list_requests += 1
        if self._fail_next_list:
            self._fail_next_list = False
            raise StorageTransientException("injected LIST failure")
        ignore = ignore_prefixes or set()
        for key in self._objects:
            if key.startswith(prefix):
                fname = key.split("/")[-1]
                if not any(fname.startswith(ig) for ig in ignore):
                    if self._is_visible(key):
                        return True
        return False

    # ------------------------------------------------------------------ utils

    def reset_counters(self) -> None:
        self.head_requests = 0
        self.list_requests = 0

    @property
    def total_requests(self) -> int:
        return self.head_requests + self.list_requests

    @property
    def object_count(self) -> int:
        return len(self._objects)
