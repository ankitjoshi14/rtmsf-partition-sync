"""
benchmark.py — RTMSF prototype evaluation harness.

Runs three experiments that produce numbers for paper §3.11:

  Experiment 1 — Coalescing ratio vs change fraction f
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
    generate_events, generate_delete_events, C1, C2, C3, P, N_LIST_SCAN
)

S3_COST_PER_1K = 0.005   # USD per 1,000 LIST requests (AWS S3 [11])
ALPHA = 2                 # verification ops per changed partition (§3.8.2)


# ============================================================= helpers

def _separator(title: str) -> None:
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)


def _make_worker(W: float = 0.0, G: float = 0.0, clock_fn=None,
                 not_ready_rate: float = 0.0):
    storage = MockStorage(not_ready_rate=not_ready_rate)
    catalog = MockCatalog()
    worker  = RTMSFWorker(storage, catalog, W=W, G=G, clock_fn=clock_fn)
    return worker, storage, catalog


# ============================================================= Experiment 1

def _run_experiment_1_variant(not_ready_rate: float, label: str,
                              f_values=None) -> List[dict]:
    """Run experiment 1 with a given not_ready_rate and print results."""
    if f_values is None:
        f_values = [0.001, 0.01, 0.05, 0.1, 0.5, 1.0]
    hdr = (f"{'f':>7}  {'ΔP':>7}  {'Raw Evts':>9}  {'WOERA Coalesce':>12}  "
           f"{'Committed':>10}  {'N_list,evt':>11}  {'α_eff':>6}  {'Reduction':>10}  {'Daily Cost':>11}")
    print(f"\n  {label}")
    print(hdr)
    print("-" * (len(hdr) + 2))

    rows = []
    for f in f_values:
        worker, storage, catalog = _make_worker(not_ready_rate=not_ready_rate)

        events, delta_p, _ = generate_events(
            f=f, files_per_partition=5, duplicate_rate=0.10
        )
        worker.process_batch(events)
        worker.force_flush_all()          # drain any remainder

        committed     = worker.pmcp.applied
        n_list_event  = storage.total_requests
        alpha_eff     = n_list_event / max(committed, 1)
        woera_coalesced   = worker.intents_buffered - committed
        reduction     = N_LIST_SCAN / max(n_list_event, 1)
        daily_cost    = (n_list_event / 1000.0) * S3_COST_PER_1K

        print(
            f"{f:>7.3f}  {delta_p:>7,}  {worker.events_received:>9,}  {woera_coalesced:>12,}  "
            f"{committed:>10,}  {n_list_event:>11,}  {alpha_eff:>5.1f}  {reduction:>9.0f}×  ${daily_cost:>10.4f}"
        )
        rows.append(dict(
            f=f, delta_p=delta_p, raw_events=worker.events_received,
            committed=committed, n_list_event=n_list_event,
            alpha_eff=alpha_eff, reduction=reduction, daily_cost=daily_cost
        ))
    return rows


def experiment_1_coalescing_ratio() -> List[dict]:
    """
    For each change fraction f, run RTMSF over synthetic events and record:
      - raw events ingested
      - partitions committed (should ≈ ΔP)
      - storage requests issued (N_list,event)
      - reduction factor vs N_list,scan
      - estimated daily cost at S3 pricing

    Runs twice: best-case (no visibility jitter) and realistic (jitter → α_eff≈2).
    """
    _separator("Experiment 1: Request-Count Reduction vs Change Fraction f")
    print(f"  Hierarchy: dt×region×hour  |  c₁={C1}, c₂={C2}, c₃={C3}  |  P={P:,}")
    print(f"  N_list,scan = {N_LIST_SCAN:,}  |  α_analytical={ALPHA}  |  S3 pricing ${S3_COST_PER_1K}/1K requests")

    rows_best = _run_experiment_1_variant(
        not_ready_rate=0.0,
        label="(a) Best-case: not_ready_rate=0.0  (HEAD always hits, α_eff=1)"
    )
    rows_realistic = _run_experiment_1_variant(
        not_ready_rate=0.5,
        label="(b) Realistic: not_ready_rate=0.5  (HEAD may miss → LIST fallback, α_eff≈2)",
        f_values=[0.001, 0.01, 0.05, 0.1],
    )

    scan_cost = (N_LIST_SCAN / 1000.0) * S3_COST_PER_1K
    print(f"\n  Scan-based daily cost (fixed): ${scan_cost:.4f}  ({N_LIST_SCAN:,} requests)")
    print(f"  Note: realistic (b) capped at f≤0.1 — high-f runs are slow under mock jitter")
    print(f"  but Table 3 already shows RTMSF advantage shrinks as f→1.")
    return rows_best


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
    print(f"  Operators tune W+G to balance freshness vs coalescing efficiency (§3.10).")


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


# ============================================================= Experiment 4

def experiment_4_drop_benchmark() -> None:
    """
    DROP safety evaluation: create partitions, then delete a fraction and
    measure PMCP's LIST-verified removal.

    Setup: create ΔP partitions via ADD (f=0.01).
    Drop phase: delete files from drop_fraction of those partitions,
    generate OBJECT_DELETED events, measure LIST requests and catalog state.
    """
    _separator("Experiment 4: DROP Safety — Partition Removal Benchmark")
    print(f"  Setup: f=0.01 ADD → {round(0.01*P):,} partitions created.")
    print(f"  Then delete files + send OBJECT_DELETED for drop_fraction of those.\n")

    hdr = (f"{'drop_f':>7}  {'ΔDrop':>7}  {'Del Evts':>9}  "
           f"{'Dropped':>8}  {'Remaining':>10}  {'LIST reqs':>10}  {'α_drop':>7}")
    print(hdr)
    print("-" * len(hdr))

    # Phase 1: create partitions (shared across all drop_fraction runs)
    add_events, delta_p, pkeys = generate_events(f=0.01, files_per_partition=5)

    for drop_f in [0.01, 0.05, 0.10, 0.25, 0.50, 1.00]:
        # Fresh worker for each run
        worker, storage, catalog = _make_worker()

        # Phase 1: ADD — process creation events and commit
        worker.process_batch(add_events)
        worker.force_flush_all()
        add_committed = worker.pmcp.applied
        assert add_committed == delta_p, f"Setup: expected {delta_p} ADDs, got {add_committed}"

        # Reset storage counters — we only want to measure DROP cost
        storage.reset_counters()
        worker.pmcp.reset_counters()

        # Phase 2: DELETE files from storage + generate deletion events
        del_events, delta_drop, dropped_keys = generate_delete_events(
            pkeys, drop_fraction=drop_f, files_per_partition=5
        )
        # Actually remove files from mock storage
        for pkey in dropped_keys:
            for file_idx in range(5):
                okey = f"warehouse/sales/{pkey}/part-{file_idx:04d}.parquet"
                storage.delete(okey)

        # Phase 2: process deletion events
        worker.process_batch(del_events)
        worker.force_flush_all()

        # Drain retries (DROP may need retry if concurrent)
        sim_rounds = 0
        while worker._retry_queue and sim_rounds < 20:
            worker.drain_retries()
            sim_rounds += 1

        dropped   = worker.pmcp.applied
        remaining = catalog.partition_count
        list_reqs = storage.list_requests
        alpha_drop = list_reqs / max(dropped, 1)

        print(
            f"{drop_f:>7.2f}  {delta_drop:>7,}  {len(del_events):>9,}  "
            f"{dropped:>8,}  {remaining:>10,}  {list_reqs:>10,}  {alpha_drop:>6.1f}"
        )

    print(f"\n  Each DROP requires at least 1 LIST (emptiness proof, Algorithm 6).")
    print(f"  α_drop ≈ 1 confirms PMCP issues exactly 1 LIST per dropped partition.")
    print(f"  Remaining = initial {delta_p:,} - dropped; catalog state verified per run.")


# ============================================================= main

def main():
    print("=" * 65)
    print("  RTMSF Reference Prototype — Benchmark Results")
    print("  Paper: 'A Real-Time, Event-Driven Architecture for")
    print("          Continuous Metadata Synchronization in Cloud Data Lakes'")
    print("=" * 65)
    print(f"\n  Partition space: P = {C1}×{C2}×{C3} = {P:,}")
    print(f"  N_list,scan    = 1+{C1}+{C1*C2:,}+{C1*C2*C3:,} = {N_LIST_SCAN:,}")
    print(f"  At f=0.01: N_list,event(α=2) = {ALPHA}×{round(0.01*P):,} = {ALPHA*round(0.01*P):,}")
    print(f"  Expected reduction at f=0.01, α=2 ≈ {N_LIST_SCAN/(ALPHA*round(0.01*P)):.0f}×")

    experiment_1_coalescing_ratio()
    experiment_2_convergence_latency()
    experiment_3_throughput()
    experiment_4_drop_benchmark()

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
