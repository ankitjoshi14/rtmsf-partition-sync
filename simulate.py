#!/usr/bin/env python3
"""
simulate.py — RTMSF interactive 3-panel simulation.

Shows the real problems RTMSF solves through 4 scripted epochs (--step mode):
  1. Normal Flow          — baseline: event → WOERA coalesce → PMCP verify → catalog
  2. Compaction (DELETE+CREATE) — ADD-dominant absorbs transient delete in same window
  3. Duplicate Resilience  — duplicate events coalesce naturally via ADD-dominant rule
  4. Partition Drop       — files deleted; PMCP LIST-verifies empty then removes

Panels:
  EVENTS (combined)  — Object Store (top) + Kafka Topic (bottom)
  WOERA BUFFER       — quiet-window dedup + coalescing
  PMCP + CATALOG     — HEAD/LIST storage verification then catalog write

Modes:
  python3 simulate.py              # continuous, slow (default)
  python3 simulate.py --step       # scripted epoch mode: 5 curated scenarios
  python3 simulate.py --speed 1.0  # faster continuous
"""

import argparse
import collections
import random
import sys
import threading
import time

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.text import Text
except ImportError:
    print("rich is required:  python3 -m pip install rich")
    sys.exit(1)

from rtmsf_worker import RTMSFWorker, TABLE_ROOT, EventEnvelope, TENANT_ID
from mock_storage import MockStorage
from mock_catalog import MockCatalog
from mock_kafka import KafkaTopic, KafkaMessage, Normalizer

from sim_scenarios import PARTITIONS, EPOCH_SCENARIOS, _generate_burst
from sim_renderers import (
    render_header, build_events_panel, render_woera_buffer,
    render_pmcp_catalog, show_epoch_summary
)


# ------------------------------------------------------------------ shared state

MAX_LOG = 20


class SimState:
    def __init__(self):
        # Rolling logs per panel
        self.event_log:  collections.deque = collections.deque(maxlen=MAX_LOG)
        self.pmcp_log:   collections.deque = collections.deque(maxlen=MAX_LOG)
        self.buffer_snap: dict = {}
        self.buffer_snap_a: dict = {}
        self.buffer_snap_b: dict = {}
        self.flushed_keys_a: list = []   # partition keys flushed by Worker-0
        self.flushed_keys_b: list = []   # partition keys flushed by Worker-1
        self.consumed_a: int = 0
        self.consumed_b: int = 0

        # Global counters
        self.produced:   int = 0
        self.consumed:   int = 0
        self.retry_count: int = 0

        # Per-partition read position for Kafka display (reset between epochs)
        self._kafka_display_start: dict = {}

        # Per-partition tracking
        self.partition_counts:     dict = {}  # pkey → events produced
        self.topic_partition_seen: set  = set()

        # Retry queue: list of (retry_at: float, eff: EffectiveIntent)

        # Step-mode state
        self.epoch_num:            int   = 0
        self.epoch_done:           bool  = False
        self.epoch_fully_processed: bool = False
        self.epoch_committed_snap: int   = 0  # worker.commits_applied at epoch start
        self.epoch_events:         int   = 0
        self.epoch_retries:        int   = 0

        self.running: bool = True


# ------------------------------------------------------------------ helpers

def _ts() -> str:
    return time.strftime("%H:%M:%S")


def _filename(object_key: str) -> str:
    return object_key.split("/")[-1]


# ------------------------------------------------------------------ producer

def run_producer(
    topic: KafkaTopic,
    normalizer: Normalizer,
    state: SimState,
    step_gate: threading.Event,
    step_mode: bool,
    burst_size: int,
    burst_interval: float,
    fraction: float,
    speed: float,
    storage: MockStorage,
    default_not_ready_rate: float,
) -> None:
    """
    Simulates the object store + cloud notification service.
    In step mode, each epoch uses a scripted scenario from EPOCH_SCENARIOS.
    """
    seed = 100

    while state.running:
        if step_mode:
            step_gate.wait()
            step_gate.clear()
            if not state.running:
                return

            # Configure scenario for this epoch
            scenario = EPOCH_SCENARIOS[state.epoch_num % len(EPOCH_SCENARIOS)]
            override = scenario["not_ready_override"]
            storage.not_ready_rate = override if override is not None else default_not_ready_rate

            # Generate scenario-appropriate events
            burst = _generate_burst(scenario, burst_size, fraction, seed, storage, state.epoch_num)
        else:
            # Continuous mode: random events from the same partition pool
            random.seed(seed)
            burst = []
            pkeys = list(PARTITIONS.values())
            for _ in range(burst_size):
                pkey = random.choice(pkeys)
                fname = f"part-{random.randint(0, 999):04d}.parquet"
                okey = f"{TABLE_ROOT}{pkey}/{fname}"
                storage.put(okey)
                burst.append(EventEnvelope(
                    tenant_id=TENANT_ID,
                    op="OBJECT_CREATED",
                    object_key=okey,
                    event_time=time.time(),
                ))

        seed += 1

        for ev in burst:
            if not state.running:
                return

            pkey    = Normalizer.extract_partition_key(ev.object_key, TABLE_ROOT)
            fname   = _filename(ev.object_key)

            # Per-partition count (for "+N to same partition" label)
            prev_count = state.partition_counts.get(pkey, 0)
            state.partition_counts[pkey] = prev_count + 1

            # Event log: (ts, op, pkey, fname, prev_count)
            state.event_log.appendleft(
                (_ts(), ev.op, pkey, fname, prev_count)
            )
            state.produced      += 1
            state.epoch_events  += 1

            key = normalizer.extract_key(ev.object_key)
            topic.produce(key=key, value=ev)
            time.sleep(0.4 / speed)

        if not step_mode:
            time.sleep(burst_interval / speed)
        else:
            state.epoch_done = True
            # Producer pauses — gate re-opened by main after epoch summary


# ------------------------------------------------------------------ consumer

def run_consumer(
    topic: KafkaTopic,
    worker_a: RTMSFWorker,
    worker_b: RTMSFWorker,
    state: SimState,
    poll_interval: float,
    max_poll_records: int,
    speed: float,
) -> None:
    """
    Simulates the Kafka consumer poll loop.
    Each worker polls its own Kafka partition, processes events, and drains retries.
    """
    while state.running:
        # Poll each partition independently (each worker reads its own partition)
        batch_0 = topic.poll_partition(0, max_poll_records)
        batch_1 = topic.poll_partition(1, max_poll_records)

        if batch_0:
            events_0 = [msg.value for msg in batch_0]
            worker_a.process_batch(events_0)
            state.consumed_a += len(events_0)
            state.consumed += len(events_0)
        if batch_1:
            events_1 = [msg.value for msg in batch_1]
            worker_b.process_batch(events_1)
            state.consumed_b += len(events_1)
            state.consumed += len(events_1)

        if not batch_0 and not batch_1:
            worker_a.flush_and_commit()
            worker_b.flush_and_commit()

        # Drain retries (worker-owned, exponential backoff)
        worker_a.drain_retries()
        worker_b.drain_retries()

        prev_a = state.buffer_snap_a
        prev_b = state.buffer_snap_b
        state.buffer_snap_a = worker_a.buffer.buffer_snapshot
        state.buffer_snap_b = worker_b.buffer.buffer_snapshot
        # Track flushed keys: detect keys that disappeared from buffer (flushed to PMCP)
        for k, info in prev_a.items():
            if k not in state.buffer_snap_a:
                pkey = k[2] if isinstance(k, tuple) else str(k)
                if not any(f["pkey"] == pkey for f in state.flushed_keys_a):
                    flushed_info = worker_a.last_flushed.get(pkey, {})
                    state.flushed_keys_a.append({
                        "pkey": pkey,
                        "count": info["count"],
                        "kind": flushed_info.get("kind", "—"),
                    })
        for k, info in prev_b.items():
            if k not in state.buffer_snap_b:
                pkey = k[2] if isinstance(k, tuple) else str(k)
                if not any(f["pkey"] == pkey for f in state.flushed_keys_b):
                    flushed_info = worker_b.last_flushed.get(pkey, {})
                    state.flushed_keys_b.append({
                        "pkey": pkey,
                        "count": info["count"],
                        "kind": flushed_info.get("kind", "—"),
                    })

        # Signal main thread: consumer has fully processed this epoch's events.
        if (state.epoch_done
                and state.consumed > 0
                and state.consumed >= state.epoch_events
                and worker_a.buffer.active_key_count == 0
                and worker_b.buffer.active_key_count == 0
                and not worker_a._retry_queue
                and not worker_b._retry_queue):
            state.epoch_fully_processed = True

        time.sleep(poll_interval / speed)


# ------------------------------------------------------------------ main

def main() -> None:
    parser = argparse.ArgumentParser(
        description="RTMSF simulation — 3 panels, 4 scripted scenarios in --step mode"
    )
    parser.add_argument("--step",            action="store_true",
                        help="Scripted epoch mode: 4 curated correctness scenarios")
    parser.add_argument("--speed",           type=float, default=1.0,
                        help="Speed multiplier (default 0.3 = slow, 1.0 = real time)")
    parser.add_argument("--burst-size",      type=int,   default=4,
                        help="Events per burst (default 4)")
    parser.add_argument("--burst-interval",  type=float, default=5.0,
                        help="Seconds between bursts in continuous mode (default 5.0)")
    parser.add_argument("--poll-interval",   type=float, default=1.0,
                        help="Kafka consumer poll interval in seconds (default 1.0)")
    parser.add_argument("--quiet-window", "-W", type=float, default=3.0,
                        help="WOERA quiet window W in seconds (default 3.0)")
    parser.add_argument("--fraction",   "-f", type=float, default=0.002,
                        help="Fraction of total partitions per burst (default 0.002)")
    parser.add_argument("--max-poll-records", type=int,  default=50,
                        help="Max events per consumer poll (default 50)")
    parser.add_argument("--head-miss-rate",   type=float, default=0.25,
                        help="Probability HEAD misses → LIST fallback (default 0.25)")
    parser.add_argument("--not-ready-rate",   type=float, default=0.0,
                        help="Probability file not yet visible → PMCP RETRY (default 0.20)")
    args = parser.parse_args()

    state     = SimState()
    topic      = KafkaTopic(num_partitions=2)
    normalizer = Normalizer()
    step_gate             = threading.Event()
    step_gate.set()  # initially open so first epoch starts immediately

    # Mutable rate reference so scripted epochs can override per-epoch
    storage  = MockStorage(not_ready_rate=args.not_ready_rate)
    catalog  = MockCatalog()
    worker_a = RTMSFWorker(storage=storage, catalog=catalog, W=args.quiet_window, G=1.0, enable_drop=True)
    worker_b = RTMSFWorker(storage=storage, catalog=catalog, W=args.quiet_window, G=1.0, enable_drop=True)

    # ---- PMCP apply interceptor (logging only — no retry management) ----
    def _make_logged_apply(target_worker, worker_label):
        _orig_apply = target_worker.pmcp.apply

        def _logged_apply(eff):
            head_before = storage.head_requests
            list_before = storage.list_requests

            result = _orig_apply(eff)

            head_used = storage.head_requests > head_before
            list_used = storage.list_requests > list_before
            pkey = eff.key[2] if isinstance(eff.key, tuple) else str(eff.key)

            if result.status == "RETRY":
                state.retry_count += 1
                state.epoch_retries += 1

            state.pmcp_log.appendleft((_ts(), eff.kind, pkey, result.status, head_used, list_used, result.reason or "", worker_label))
            return result

        target_worker.pmcp.apply = _logged_apply

    _make_logged_apply(worker_a, "0")
    _make_logged_apply(worker_b, "1")

    # ---- 2-column Layout: Events | WOERA (top) + PMCP (bottom) ----
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=4),
        Layout(name="panels"),
        Layout(name="summary", size=4),
    )
    layout["panels"].split_row(
        Layout(name="events"),          # Object Store + Kafka combined
        Layout(name="right_stack"),     # WOERA + PMCP stacked vertically
    )
    layout["right_stack"].split_column(
        Layout(name="woera"),
        Layout(name="pmcp"),
    )

    console = Console()
    mode_label = "[bold yellow]STEP MODE[/bold yellow]" if args.step else "continuous"
    console.print(
        f"\n[bold]RTMSF Simulation[/bold]  {mode_label}  "
        f"[dim]W={args.quiet_window}s[/dim]\n"
        f"[dim]Ctrl+C to stop[/dim]\n"
    )

    # ---- Start threads ----
    # Initialize Kafka display start positions
    state._kafka_display_start = {p.partition_id: 0 for p in topic.partitions}

    prod_t = threading.Thread(
        target=run_producer,
        args=(topic, normalizer, state, step_gate, args.step,
              args.burst_size, args.burst_interval, args.fraction, args.speed,
              storage, args.not_ready_rate),
        daemon=True,
    )
    cons_t = threading.Thread(
        target=run_consumer,
        args=(topic, worker_a, worker_b, state, args.poll_interval,
              args.max_poll_records, args.speed),
        daemon=True,
    )
    prod_t.start()
    cons_t.start()

    # ---- Main render loop ----
    try:
        while state.running:
            with Live(layout, refresh_per_second=4, console=console) as live:
                # Empty summary placeholder while epoch runs
                layout["summary"].update(Panel("", border_style="dim", padding=(0, 2)))

                while state.running:
                    layout["header"].update(
                        render_header(args.quiet_window, args.step, state.epoch_num)
                    )
                    layout["events"].update(build_events_panel(state, topic, args.step))
                    layout["woera"].update(render_woera_buffer(state, worker_a, worker_b))
                    layout["pmcp"].update(render_pmcp_catalog(state, worker_a, worker_b, args.step))
                    time.sleep(0.25)

                    # In step mode: wait for consumer to signal epoch fully processed
                    if args.step and state.epoch_fully_processed:
                        # Final render with up-to-date state
                        state.buffer_snap_a = worker_a.buffer.buffer_snapshot
                        state.buffer_snap_b = worker_b.buffer.buffer_snapshot
                        layout["woera"].update(render_woera_buffer(state, worker_a, worker_b))
                        layout["pmcp"].update(render_pmcp_catalog(state, worker_a, worker_b, args.step))

                        # Show "What happened" in the summary panel
                        scenario = EPOCH_SCENARIOS[state.epoch_num % len(EPOCH_SCENARIOS)]
                        ep_display = state.epoch_num + 1
                        layout["summary"].update(Panel(
                            Text(scenario["what_happened"]),
                            title=f"[bold]Epoch {ep_display} — {scenario['title']}[/bold]",
                            border_style="dim",
                            padding=(0, 2),
                        ))
                        live.refresh()
                        break

            if not args.step or not state.running:
                break

            # Wait for Enter (summary is already visible in the layout)
            keep_going = show_epoch_summary(console, state, worker_a, worker_b, storage, catalog, args.quiet_window)
            if not keep_going:
                state.running = False
                break

            # Reset epoch-level stats for next epoch
            state.epoch_num             += 1
            state.epoch_done             = False
            state.epoch_fully_processed  = False
            state.epoch_events           = 0
            state.epoch_retries          = 0
            state.epoch_committed_snap   = worker_a.commits_applied + worker_b.commits_applied
            state._epoch_noops_snap      = worker_a.pmcp.noops + worker_b.pmcp.noops
            storage.not_ready_rate       = args.not_ready_rate  # reset to CLI default

            # Clear per-epoch display logs so Events panel shows only current epoch
            state.event_log.clear()
            state.pmcp_log.clear()
            state.produced              = 0
            state.consumed              = 0
            state.consumed_a            = 0
            state.consumed_b            = 0
            state.flushed_keys_a        = []
            state.flushed_keys_b        = []
            worker_a.last_flushed       = {}
            worker_b.last_flushed       = {}
            state.partition_counts      = {}
            state.topic_partition_seen  = set()
            state._kafka_display_start = {p.partition_id: len(p.messages) for p in topic.partitions}

            step_gate.set()  # release producer for next epoch

    except KeyboardInterrupt:
        state.running = False

    console.print("\n[dim]Simulation stopped.[/dim]")


if __name__ == "__main__":
    main()
