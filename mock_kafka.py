"""
mock_kafka.py — Kafka topic and partition simulation.

Models keyed partitioning: messages are routed to partitions by
hash(key) % num_partitions. Each partition maintains an independent
offset counter and ordered message log, matching real Kafka semantics.

Also includes a Normalizer that extracts the partition prefix from
an S3 object key for use as the Kafka message key (§3.2).
"""

import hashlib
from dataclasses import dataclass
from typing import Any, List


# ---------------------------------------------------------------- message type

@dataclass
class KafkaMessage:
    """A single message on a Kafka partition."""
    offset: int
    partition: int
    key: str
    value: Any  # EventEnvelope


# ---------------------------------------------------------------- partition

class KafkaPartition:
    """
    One partition of a Kafka topic.

    Maintains an ordered, append-only log with independent offset counter.
    Supports consumer-side polling with a read position.
    """

    def __init__(self, partition_id: int):
        self.partition_id = partition_id
        self.offset = 0
        self.messages: List[KafkaMessage] = []
        self._read_pos = 0

    def append(self, key: str, value: Any) -> KafkaMessage:
        """Append a message. Returns the created KafkaMessage."""
        self.offset += 1
        msg = KafkaMessage(
            offset=self.offset,
            partition=self.partition_id,
            key=key,
            value=value,
        )
        self.messages.append(msg)
        return msg

    def poll(self, max_records: int) -> List[KafkaMessage]:
        """Read up to max_records unread messages. Advances read position."""
        batch = self.messages[self._read_pos:self._read_pos + max_records]
        self._read_pos += len(batch)
        return batch

    def lag(self) -> int:
        """Number of unread messages."""
        return len(self.messages) - self._read_pos

    def reset_read_pos(self):
        """Reset consumer position to start (for epoch resets in simulation)."""
        self._read_pos = len(self.messages)


# ---------------------------------------------------------------- topic

class KafkaTopic:
    """
    A Kafka topic with keyed partitioning.

    produce() hashes the message key to determine the target partition.
    Each partition is consumed independently by a worker.
    """

    def __init__(self, num_partitions: int):
        self.partitions = [KafkaPartition(i) for i in range(num_partitions)]

    def produce(self, key: str, value: Any) -> KafkaMessage:
        """Route message to partition by hash(key) and append."""
        pid = int(hashlib.md5(key.encode()).hexdigest(), 16) % len(self.partitions)
        return self.partitions[pid].append(key, value)

    def poll_partition(self, partition_id: int, max_records: int) -> List[KafkaMessage]:
        """Poll unread messages from a specific partition."""
        return self.partitions[partition_id].poll(max_records)

    def total_lag(self) -> int:
        """Total unread messages across all partitions."""
        return sum(p.lag() for p in self.partitions)


# ---------------------------------------------------------------- normalizer

class Normalizer:
    """
    Extracts partition prefix from S3 object key for Kafka keying (§3.2).

    Input:  warehouse/sales/country=us/city=seattle/part-0001.parquet
    Output: warehouse/sales/country=us/city=seattle/

    The prefix is used as the Kafka message key. hash(prefix) determines
    the partition, ensuring all events for the same table partition
    route to the same Kafka partition → same worker.
    """

    @staticmethod
    def extract_key(object_key: str) -> str:
        """Return the partition prefix path (object key minus filename).

        Input:  warehouse/sales/country=us/city=seattle/part-0001.parquet
        Output: warehouse/sales/country=us/city=seattle/
        """
        segments = object_key.split("/")
        return "/".join(segments[:-1]) + "/"

    @staticmethod
    def extract_partition_key(object_key: str, table_root: str) -> str:
        """Extract just the Hive k=v partition key from object_key."""
        if not object_key.startswith(table_root):
            return ""
        relative = object_key[len(table_root):]
        segments = relative.split("/")
        kv_parts = [seg for seg in segments[:-1] if "=" in seg]
        return "/".join(kv_parts) if kv_parts else ""
