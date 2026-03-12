"""
mock_storage.py — Simulates an object store (e.g., S3 / OCI Object Storage).

Maps to: §3.1 assumptions 4-5, PMCP HEAD/LIST operations (Algorithms 5 & 6).
All operations are in-memory. HEAD and LIST request counts are tracked
so benchmark.py can compare scan-based vs event-driven request costs.
"""


class StorageTransientException(Exception):
    pass


class MockStorage:
    """
    In-memory object store.

    Supports:
      put(key)                          — simulate object write
      delete(key)                       — simulate object delete
      head_exists(key)                  — HEAD request (O(1))
      prefix_non_empty(prefix)          — LIST max_keys=1 (O(n) scan over keys)

    Counters:
      head_requests, list_requests      — tracked for cost comparison
    """

    def __init__(self):
        self._objects: set = set()
        self.head_requests: int = 0
        self.list_requests: int = 0

    # ------------------------------------------------------------------ writes

    def put(self, object_key: str) -> None:
        self._objects.add(object_key)

    def delete(self, object_key: str) -> None:
        self._objects.discard(object_key)

    # ------------------------------------------------------------------ reads

    def head_exists(self, object_key: str) -> bool:
        """Simulate a HEAD request. Counts as one billed operation."""
        self.head_requests += 1
        return object_key in self._objects

    def prefix_non_empty(self, prefix: str, ignore_prefixes=None) -> bool:
        """
        Simulate LIST prefix with max_keys=1.
        Returns True if at least one non-ignored object exists under prefix.
        Counts as one billed LIST operation.
        """
        self.list_requests += 1
        ignore = ignore_prefixes or set()
        for key in self._objects:
            if key.startswith(prefix):
                if not any(key.startswith(ig) for ig in ignore):
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
