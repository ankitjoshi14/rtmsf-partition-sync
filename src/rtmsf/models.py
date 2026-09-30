"""Immutable, provider-neutral data models used by RTMSF."""

from dataclasses import dataclass
from datetime import datetime, timedelta


def _utc(value: datetime) -> bool:
    return isinstance(value, datetime) and value.utcoffset() == timedelta(0)


@dataclass(frozen=True)
class ObjectEvent:
    source_id: str
    object_key: str
    operation: str
    ingested_at: datetime
    source_event_time: datetime | None = None

    def __post_init__(self) -> None:
        if not self.source_id or not self.object_key:
            raise ValueError("source_id and exact object_key are required")
        if self.operation not in {"CREATE", "DELETE"}:
            raise ValueError("operation must be CREATE or DELETE")
        if not _utc(self.ingested_at):
            raise ValueError("ingested_at must be a timezone-aware UTC datetime")
        if self.source_event_time is not None and not _utc(self.source_event_time):
            raise ValueError("source_event_time must be UTC or None")


@dataclass(frozen=True)
class TableDefinition:
    """An existing table resolved by the catalog at process startup."""

    table_id: str
    source_id: str
    root_prefix: str
    partition_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.table_id or not self.source_id:
            raise ValueError("table_id and source_id are required")
        if self.root_prefix and not self.root_prefix.endswith("/"):
            raise ValueError("table root_prefix must end with '/' or be empty")
        if not self.partition_columns or len(set(self.partition_columns)) != len(self.partition_columns):
            raise ValueError("partition columns must be nonempty and unique")
        if any(not column or '/' in column or '=' in column for column in self.partition_columns):
            raise ValueError("invalid partition column name")


@dataclass(frozen=True)
class PartitionRef:
    source_id: str
    table_id: str
    partition_path: str
    values: tuple[str, ...]
    storage_prefix: str

    @property
    def key(self) -> tuple[str, str, str]:
        return self.source_id, self.table_id, self.partition_path


@dataclass(frozen=True)
class PartitionEvent:
    event: ObjectEvent
    partition: PartitionRef


@dataclass(frozen=True)
class PartitionIntent:
    partition: PartitionRef
    mutation: str
    observed_create_keys: tuple[str, ...] = ()
    observation_count: int = 1

    def __post_init__(self) -> None:
        if self.mutation not in {"ADD", "DROP"}:
            raise ValueError("mutation must be ADD or DROP")
        if self.observation_count < 1:
            raise ValueError("observation_count must be positive")


@dataclass(frozen=True)
class MutationResult:
    status: str
    reason: str

    def __post_init__(self) -> None:
        if self.status not in {"APPLIED", "NOOP", "RETRY", "ERROR"}:
            raise ValueError("unknown mutation status")
