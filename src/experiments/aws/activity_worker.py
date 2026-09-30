"""Experiment control: the real worker, with one pending intent per event."""
import signal
from threading import Event

from rtmsf.aws_app import AppConfig, build_worker
from rtmsf.woera import WoeraBuffer, _PartitionActivity


class ImmediateBuffer(WoeraBuffer):
    """Reuse deadline/retry handling without combining any received events."""

    def __init__(self):
        super().__init__(0, 31000)

    def add(self, event, now_ms):
        # Only the experiment's scheduling identity changes; PMCP sees the real
        # partition, location and key. Even events in one delivery stay separate.
        key = (*event.partition.key, next(self._sequence))
        current = _PartitionActivity(event.partition, now_ms, now_ms,
            observation_count=1, mutation='ADD' if event.event.operation == 'CREATE' else 'DROP',
            observed_create_keys=[event.event.object_key] if event.event.operation == 'CREATE' else [])
        self._windows[key] = current
        self._schedule(key, current)
        return key


def main():
    stop = Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    worker = build_worker(AppConfig.from_environment())
    worker._buffer = ImmediateBuffer()
    worker.run_forever(stop.is_set)


if __name__ == '__main__':
    main()
