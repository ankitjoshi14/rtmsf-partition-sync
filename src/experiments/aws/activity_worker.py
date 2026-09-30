"""Experiment control: the real worker, with one pending intent per event."""
import signal
import os
import resource
import sys
import time
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
    if os.environ.get('RTMSF_EXPERIMENT_HANDLER', 'immediate') == 'immediate':
        worker._buffer = ImmediateBuffer()
    sampled = 0
    try:
        while not stop.is_set():
            worker.run_once()
            if time.monotonic() - sampled >= 5:
                sampled = time.monotonic()
                report_memory(worker)
    finally:
        report_memory(worker, final=stop.is_set())


def report_memory(worker, *, final=False):
    # RSS is the process lifetime high-water mark; each buffer trial starts fresh.
    worker._telemetry.emit(dict(component='experiment', action='memory_sample', final=final,
        peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024),
        pending_messages=len(worker._pending),
        pending_body_bytes=worker._source._pending_body_bytes))


if __name__ == '__main__':
    main()
