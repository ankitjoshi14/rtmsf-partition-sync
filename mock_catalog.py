"""
mock_catalog.py — Simulates a Hive Metastore / partition catalog.

Maps to: §3.1 assumption 4, PMCP catalog operations (Algorithms 5 & 6).
All operations are in-memory. Mutation counts (creates, drops) are tracked
for benchmarking. Exception classes mirror real catalog error semantics.
"""


# ------------------------------------------------------------------ exceptions

class AlreadyExistsException(Exception):
    """Raised by create_partition when partition already registered."""
    pass


class NotFoundException(Exception):
    """Raised by drop_partition when partition not found."""
    pass


class CatalogTransientException(Exception):
    """Transient catalog error — caller should RETRY with backoff."""
    pass


class CatalogFatalException(Exception):
    """Fatal catalog error — caller should DLQ."""
    pass


# ------------------------------------------------------------------ catalog

class MockCatalog:
    """
    In-memory partition catalog.

    Supports:
      has_partition(key)                     — existence check (no I/O cost)
      create_partition(key, location)        — register partition
      drop_partition(key)                    — deregister partition

    Counters:
      creates, drops                         — tracked for benchmarking
    """

    def __init__(self):
        self._partitions: dict = {}   # key (tuple) -> location (str)
        self.creates: int = 0
        self.drops: int = 0

    # ------------------------------------------------------------------ reads

    def has_partition(self, key: tuple) -> bool:
        return key in self._partitions

    # ----------------------------------------------------------------- writes

    def create_partition(self, key: tuple, location: str) -> None:
        if key in self._partitions:
            raise AlreadyExistsException(f"Already exists: {key}")
        self._partitions[key] = location
        self.creates += 1

    def drop_partition(self, key: tuple) -> None:
        if key not in self._partitions:
            raise NotFoundException(f"Not found: {key}")
        del self._partitions[key]
        self.drops += 1

    # ------------------------------------------------------------------ utils

    def reset_counters(self) -> None:
        self.creates = 0
        self.drops = 0

    @property
    def partition_count(self) -> int:
        return len(self._partitions)
