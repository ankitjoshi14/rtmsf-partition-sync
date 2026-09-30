"""Interfaces for queues, object storage, catalogs and telemetry.

Delivery handles stay separate from algorithmic events."""

from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence

from rtmsf.models import ObjectEvent, PartitionRef, TableDefinition


class RetryableExternalError(RuntimeError):
    """The operation may succeed on a bounded retry."""


class FatalExternalError(RuntimeError):
    """The input requires durable terminal handling."""


@dataclass(frozen=True)
class Delivery:
    handle: str
    events: tuple[ObjectEvent, ...]


class EventSource(Protocol):
    def receive(self, max_messages: int, wait_seconds: int, lease_seconds: int) -> Sequence[Delivery]: ...
    def acknowledge(self, deliveries: Sequence[Delivery]) -> None: ...
    def renew(self, deliveries: Sequence[Delivery], lease_seconds: int) -> None: ...
    def dead_letter(self, deliveries: Sequence[Delivery], reason: str) -> None:
        """Durably copy terminal input; worker acknowledges only after success."""
        ...


class ObjectStore(Protocol):
    def head_exists(self, source_id: str, key: str) -> bool: ...
    def prefix_non_empty(self, source_id: str, prefix: str) -> bool: ...
    def location_uri(self, source_id: str, prefix: str) -> str: ...


class PartitionCatalog(Protocol):
    def resolve_tables(self, table_ids: Sequence[str]) -> Sequence[TableDefinition]: ...
    def partition_exists(self, partition: PartitionRef) -> bool: ...
    def create_partition(self, partition: PartitionRef, location: str) -> bool: ...
    def delete_partition(self, partition: PartitionRef) -> bool: ...


class TelemetrySink(Protocol):
    def emit(self, record: Mapping[str, object]) -> None: ...
