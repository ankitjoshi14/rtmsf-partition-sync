"""
benchmark.py — RTMSF prototype evaluation harness.

Runs three experiments that produce numbers for paper §3.11:

  Experiment 1 — Dedup ratio vs change fraction f
    Confirms Table 2 analytically: N_list,scan / N_list,event per workload.

  Experiment 2 — Convergence latency vs quiet-window W
    Measures time from first event → catalog commit at f=0.01.
    Uses a simulated clock so results don't depend on wall-clock sleeps.

  Experiment 3 — Worker throughput
    Events processed per second and partitions committed per second.

Usage:
    python benchmark.py

No external dependencies — standard library only.
"""

import statistics
import time
from typing import List

from mock_storage import MockStorage
from mock_catalog import MockCatalog
from rtmsf_worker import RTMSFWorker
from event_generator import (
    generate_events, C1, C2, C3, P, N_LIST_SCAN
)

S3_COST_PER_1K = 0.005   # USD per 1,000 LIST requests (AWS S3 [11])
ALPHA = 2                 # verification ops per changed partition (§3.8.2)


# ============================================================= helpers

def _separator(title: str) -> None:
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)


def _make_worker(W: float = 0.0, G: float = 0.0, clock_fn=None):
    storage = MockStorage()
    catalog = MockCatalog()
    worker  = RTMSFWorker(storage, catalog, W=W, G=G, clock_fn=clock_fn)
    return worker, storage, catalog


# ============================================================= Experiment 1

def experiment_1_dedup_ratio() -> List[dict]:
    """
    For each change fraction f, run RTMSF over synthetic events and record:
      - raw events ingested
      - partitions committed (should ≈ ΔP)
      - storage requests issued (N_list,event)
      - reduction factor vs N_list,scan
      - estimated daily cost at S3 pricing
    """
    _separator("Experiment 1: Request-Count Reduction vs Change Fraction f")
    print(f"  Hierarchy: dt×region×hour  |  c₁={C1}, c₂={C2}, c₃={C3}  |  P={P:,}")
    print(f"  N_list,scan = {N_LIST_SCAN:,}  |  α={ALPHA}  |  S3 pricing ${S3_COST_PER_1K}/1K requests\n")

    hdr = (f"{'f':>7}  {'ΔP':>7}  {'Raw Evts':>9}  {'WOERA Dedup':>12}  "
           f"{'Committed':>10}  {'N_list,evt':>11}  {'Reduction':>10}  {'Daily Cost':>11}")
    print(hdr)
    print("-" * (len(hdr) + 2))

    rows = []
    for f in [0.001, 0.01, 0.05, 0.1, 0.5, 1.0]:
        worker, storage, catalog = _make_worker()

        events, delta_p, _ = generate_events(
            f=f, files_per_partition=5, duplicate_rate=0.10
        )
        worker.process_batch(events)
        worker.force_flush_all()          # drain any remainder

        # Use pmcp.applied as canonical committed count (spans both flush steps)
        committed     = worker.pmcp.applied
        n_list_event  = storage.total_requests
        # WOERA dedup: how many file-level intents were coalesced → partition commits
        woera_dedup   = worker.intents_buffered - committed
        reduction     = N_LIST_SCAN / max(n_list_event, 1)
        daily_cost    = (n_list_event / 1000.0) * S3_COST_PER_1K

        print(
            f"{f:>7.3f}  {delta_p:>7,}  {worker.events_received:>9,}  {woera_dedup:>12,}  "
            f"{committed:>10,}  {n_list_event:>11,}  {reduction:>9.0f}×  ${daily_cost:>10.4f}"
        )
        rows.append(dict(
            f=f, delta_p=delta_p, raw_events=worker.events_received,
            committed=committed, n_list_event=n_list_event,
            reduction=reduction, daily_cost=daily_cost
        ))

    scan_cost = (N_LIST_SCAN / 1000.0) * S3_COST_PER_1K
    print(f"\n  Scan-based daily cost (fixed): ${scan_cost:.4f}  ({N_LIST_SCAN:,} requests)")
    return rows


# ============================================================= Experiment 2

def experiment_2_convergence_latency() -> None:
    """
    Vary quiet-window W from 1s to 120s; G=1s fixed; f=0.01.
    Use a simulated clock so the test runs instantly without real sleeps.
    Report median / P95 / P99 convergence latency.
    """
    _separator("Experiment 2: Convergence Latency vs Quiet-Window W  (f=0.01, G=1s)")
    print(f"  Latency = time from first event for a partition → catalog commit.\n")

    hdr = f"{'W (s)':>7}  {'G (s)':>6}  {'Committed':>10}  {'Median (s)':>11}  {'P95 (s)':>9}  {'P99 (s)':>9}"
    print(hdr)
    print("-" * len(hdr))

    events, delta_p, _ = generate_events(f=0.01, files_per_partition=5)

    for W in [1.0, 5.0, 10.0, 30.0, 60.0, 120.0]:
        G = 1.0

        # Simulated clock: starts at 0, advanced to W+G+ε after batch
        sim_clock = [0.0]

        def clock(sc=sim_clock):
            return sc[0]

        worker, storage, _ = _make_worker(W=W, G=G, clock_fn=clock)

        # Process all events at t=0
        worker.process_batch(events)

        # Advance clock past W+G so all keys become flush-eligible
        sim_clock[0] = W + G + 0.001
        worker.flush_and_commit()

        lats = worker.latencies
        if lats:
            med = statistics.median(lats)
            p95 = sorted(lats)[int(0.95 * len(lats))]
            p99 = sorted(lats)[int(0.99 * len(lats))]
            print(f"{W:>7.1f}  {G:>6.1f}  {len(lats):>10,}  {med:>11.3f}  {p95:>9.3f}  {p99:>9.3f}")
        else:
            print(f"{W:>7.1f}  {G:>6.1f}  {'0':>10}  {'—':>11}  {'—':>9}  {'—':>9}")

    print(f"\n  Note: latency ≈ W + G (plus negligible in-memory processing overhead).")
    print(f"  Operators tune W+G to balance freshness vs dedup efficiency (§3.10).")


# ============================================================= Experiment 3

def experiment_3_throughput() -> None:
    """
    Measure worker throughput (events/sec, partitions/sec) at different loads.
    Each run uses W=G=0 to measure pure processing speed without window delays.
    """
    _separator("Experiment 3: Worker Throughput vs Event Volume")
    print(f"  W=0, G=0 (no window delay); 5 files/partition; 10% duplicate rate.\n")

    hdr = (f"{'f':>7}  {'ΔP':>7}  {'Events':>10}  "
           f"{'Time (ms)':>10}  {'Events/s':>10}  {'Parts/s':>10}")
    print(hdr)
    print("-" * len(hdr))

    for f in [0.001, 0.01, 0.05, 0.1, 0.5, 1.0]:
        worker, _, _ = _make_worker()
        events, delta_p, _ = generate_events(f=f, files_per_partition=5)

        t0        = time.perf_counter()
        worker.process_batch(events)
        worker.force_flush_all()
        elapsed   = time.perf_counter() - t0

        committed  = worker.pmcp.applied
        elapsed_ms = elapsed * 1000.0
        ev_per_s   = len(events) / elapsed if elapsed > 0 else 0
        pt_per_s   = committed  / elapsed if elapsed > 0 else 0

        print(
            f"{f:>7.3f}  {delta_p:>7,}  {len(events):>10,}  "
            f"{elapsed_ms:>10.1f}  {ev_per_s:>10,.0f}  {pt_per_s:>10,.0f}"
        )


# ============================================================= main

def main():
    print("=" * 65)
    print("  RTMSF Reference Prototype — Benchmark Results")
    print("  Paper: 'A Real-Time, Event-Driven Architecture for")
    print("          Continuous Metadata Synchronization in Cloud Data Lakes'")
    print("=" * 65)
    print(f"\n  Partition space: P = {C1}×{C2}×{C3} = {P:,}")
    print(f"  N_list,scan    = 1+{C1}+{C1*C2:,}+{C1*C2*C3:,} = {N_LIST_SCAN:,}")
    print(f"  At f=0.01: N_list,event = {ALPHA}×{round(0.01*P):,} = {ALPHA*round(0.01*P):,}")
    print(f"  Expected reduction at f=0.01 ≈ {N_LIST_SCAN/(ALPHA*round(0.01*P)):.0f}×")

    experiment_1_dedup_ratio()
    experiment_2_convergence_latency()
    experiment_3_throughput()

    print("\n" + "=" * 65)
    print("  All experiments complete.")
    print("=" * 65)


if __name__ == "__main__":
    import sys, os

    # Always save a timestamped copy alongside the live output
    ts  = __import__("datetime").datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(os.path.dirname(__file__), f"benchmark_results_{ts}.txt")

    class Tee:
        def __init__(self, *files):
            self.files = files
        def write(self, data):
            for f in self.files:
                f.write(data)
        def flush(self):
            for f in self.files:
                f.flush()

    with open(out, "w") as fh:
        sys.stdout = Tee(sys.__stdout__, fh)
        main()
        sys.stdout = sys.__stdout__

    print(f"\n  Results saved → {out}")
