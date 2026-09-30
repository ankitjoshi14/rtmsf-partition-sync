"""Validate configured table roots once and route exact object keys in memory."""

from dataclasses import dataclass
from typing import Sequence

from rtmsf.models import ObjectEvent, PartitionEvent, PartitionRef, TableDefinition


@dataclass(frozen=True, init=False)
class RoutingIndex:
    tables: tuple[TableDefinition, ...]

    def __init__(self, tables: Sequence[TableDefinition]) -> None:
        definitions = tuple(tables)
        if not definitions:
            raise ValueError("at least one configured table is required")
        if len({table.table_id for table in definitions}) != len(definitions):
            raise ValueError("duplicate configured table ID")
        for index, table in enumerate(definitions):
            for other in definitions[:index]:
                if table.source_id == other.source_id and (
                    table.root_prefix.startswith(other.root_prefix)
                    or other.root_prefix.startswith(table.root_prefix)
                ):
                    raise ValueError("ambiguous configured table roots")
        object.__setattr__(self, "tables", definitions)

    def route(self, event: ObjectEvent) -> PartitionEvent | None:
        for table in self.tables:
            if event.source_id != table.source_id or not event.object_key.startswith(table.root_prefix):
                continue
            parts = event.object_key[len(table.root_prefix):].split("/")
            if len(parts) != len(table.partition_columns) + 1:
                return None
            if not parts[-1].endswith(".parquet") or parts[-1].startswith(("_", ".")):
                return None
            values = []
            for segment, column in zip(parts[:-1], table.partition_columns):
                name, separator, value = segment.partition("=")
                if not separator or name != column or not value:
                    return None
                values.append(value)
            path = "/".join(parts[:-1])
            partition = PartitionRef(
                event.source_id, table.table_id, path, tuple(values),
                table.root_prefix + path + "/",
            )
            return PartitionEvent(event, partition)
        return None
