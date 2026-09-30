"""Storage-verified, idempotent partition mutations behind provider-neutral interfaces."""

from rtmsf.models import MutationResult, PartitionIntent
from rtmsf.interfaces import FatalExternalError, ObjectStore, PartitionCatalog, RetryableExternalError

# Worst ADD path: catalog read, three observed HEADs, prefix LIST, catalog write.
MAX_CALLS_PER_INTENT = 6


class PartitionMutationCommitProtocol:
    def __init__(self, storage: ObjectStore, catalog: PartitionCatalog) -> None:
        self._storage = storage
        self._catalog = catalog

    def apply(self, intent: PartitionIntent) -> MutationResult:
        try:
            if intent.mutation == "ADD":
                return self._add(intent)
            return self._drop(intent)
        except RetryableExternalError:
            return MutationResult("RETRY", "external_retryable")
        except FatalExternalError:
            return MutationResult("ERROR", "external_fatal")

    def _add(self, intent: PartitionIntent) -> MutationResult:
        partition = intent.partition
        if self._catalog.partition_exists(partition):
            if self._storage.prefix_non_empty(partition.source_id, partition.storage_prefix):
                return MutationResult("NOOP", "already_exists_verified")
            self._catalog.delete_partition(partition)
            return MutationResult("RETRY", "stale_entry_removed")
        for key in intent.observed_create_keys[:3]:
            if key.startswith(partition.storage_prefix) and self._storage.head_exists(partition.source_id, key):
                return self._create(intent)
        if self._storage.prefix_non_empty(partition.source_id, partition.storage_prefix):
            return self._create(intent)
        return MutationResult("RETRY", "not_visible")

    def _drop(self, intent: PartitionIntent) -> MutationResult:
        partition = intent.partition
        if not self._catalog.partition_exists(partition):
            if self._storage.prefix_non_empty(partition.source_id, partition.storage_prefix):
                return self._create(intent)
            return MutationResult("NOOP", "not_found")
        if self._storage.prefix_non_empty(partition.source_id, partition.storage_prefix):
            return MutationResult("NOOP", "still_non_empty")
        if self._catalog.delete_partition(partition):
            return MutationResult("APPLIED", "deleted")
        return MutationResult("NOOP", "not_found")

    def _create(self, intent: PartitionIntent) -> MutationResult:
        partition = intent.partition
        location = self._storage.location_uri(partition.source_id, partition.storage_prefix)
        if self._catalog.create_partition(partition, location):
            return MutationResult("APPLIED", "created")
        return MutationResult("NOOP", "already_exists")
