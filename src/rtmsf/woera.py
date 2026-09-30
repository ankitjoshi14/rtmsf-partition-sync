"""Coalesce partition activity using worker-local monotonic Q/T_max deadlines."""

from dataclasses import dataclass, field
import heapq
from itertools import count

from rtmsf.models import PartitionEvent, PartitionIntent, PartitionRef

PartitionKey = tuple[str, str, str]


@dataclass
class _PartitionActivity:
    partition: PartitionRef
    first_seen_ms: int
    eligible_at_ms: int
    version: int = 0
    observation_count: int = 0
    retry_not_before_ms: int = 0
    mutation: str = "DROP"
    observed_create_keys: list[str] = field(default_factory=list)


class WoeraBuffer:
    def __init__(self, settling_ms: int, maximum_ms: int, observed_key_limit: int = 3) -> None:
        if settling_ms < 0 or maximum_ms <= 0 or observed_key_limit <= 0:
            raise ValueError("invalid settling, maximum, or observed-key bound")
        self._settling_ms = settling_ms
        self._maximum_ms = maximum_ms
        self._observed_key_limit = observed_key_limit
        self._windows: dict[PartitionKey, _PartitionActivity] = {}
        self._heap: list[tuple[int, int, PartitionKey, int]] = []
        self._sequence = count()

    def add(self, event: PartitionEvent, now_ms: int) -> PartitionKey:
        key = event.partition.key
        current = self._windows.get(key)
        if current is None:
            current = _PartitionActivity(event.partition, now_ms, now_ms)
            self._windows[key] = current
        current.observation_count += 1
        current.eligible_at_ms = max(
            current.retry_not_before_ms,
            min(now_ms + self._settling_ms, current.first_seen_ms + self._maximum_ms),
        )
        if event.event.operation == "CREATE":
            current.mutation = "ADD"
            observed = event.event.object_key
            if observed not in current.observed_create_keys and len(current.observed_create_keys) < self._observed_key_limit:
                current.observed_create_keys.append(observed)
        self._schedule(key, current)
        return key

    def _schedule(self, key: PartitionKey, current: _PartitionActivity) -> None:
        current.version += 1
        heapq.heappush(self._heap, (current.eligible_at_ms, next(self._sequence), key, current.version))
        # Duplicates must not leave an unbounded history of obsolete deadlines.
        if len(self._heap) > max(64, 4 * len(self._windows)):
            self._heap = [
                entry for entry in self._heap
                if entry[2] in self._windows and self._windows[entry[2]].version == entry[3]
            ]
            heapq.heapify(self._heap)

    def next_ready_ms(self) -> int | None:
        while self._heap:
            due, _sequence, key, version = self._heap[0]
            current = self._windows.get(key)
            if current is not None and current.version == version:
                return due
            heapq.heappop(self._heap)
        return None

    def ready_keys(self, now_ms: int) -> tuple[PartitionKey, ...]:
        ready = []
        while (due := self.next_ready_ms()) is not None and due <= now_ms:
            _due, _sequence, key, _version = heapq.heappop(self._heap)
            ready.append(key)
        return tuple(ready)

    def intent(self, key: PartitionKey) -> PartitionIntent:
        current = self._windows[key]
        return PartitionIntent(
            current.partition, current.mutation,
            tuple(current.observed_create_keys), current.observation_count,
        )

    def defer(self, key: PartitionKey, due_ms: int) -> None:
        current = self._windows[key]
        current.retry_not_before_ms = due_ms
        current.eligible_at_ms = due_ms
        self._schedule(key, current)

    def complete(self, key: PartitionKey) -> None:
        self._windows.pop(key, None)
