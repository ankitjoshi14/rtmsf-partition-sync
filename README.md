# RTMSF — Real-Time Metadata Synchronization Framework

Reference prototype for the paper:

> **A Real-Time, Event-Driven Architecture for Continuous Metadata Synchronization in Cloud Data Lakes: A Cloud-Native Alternative to MSCK Repair**
> Ankit Joshi, Independent Researcher, USA

---

## What this is

RTMSF is a cloud-agnostic, event-driven control plane that converts at-least-once object-store notifications (S3, GCS, OCI, Azure Blob) into idempotent partition catalog mutations — without full directory scans.

It replaces scan-based repair (`MSCK REPAIR TABLE`) for Hive-style directory-partitioned tables. At 1% daily partition churn over a 438,000-partition hierarchy, the measured reduction is **104×** in storage request count ($2.28 → $0.022/day at S3 pricing).

Two correctness components:

- **WOERA** (Weakly Ordered Event Reconciliation Algorithm) — bounded event-time deduplication per `(tenant, table, partition)` key using a quiet window W, grace interval G, and optional maximum buffering deadline T_max.
- **PMCP** (Partition Mutation Commit Protocol) — storage-verified, idempotent ADD/DROP: ADD requires a HEAD existence proof; DROP requires a LIST emptiness proof.

---

## Repository structure

```
woera_buffer.py      — WOERABuffer: min-heap flush scheduler, reconcile (Algorithm 4)
pmcp.py              — PMCP: apply_add and apply_drop (Algorithms 5 & 6)
rtmsf_worker.py      — RTMSFWorker: worker loop, flush/commit, intent reduction (Algorithms 1–3)
event_generator.py   — Synthetic workload generator (§3.8.2 parameters)
mock_storage.py      — In-memory object store; counts HEAD and LIST requests
mock_catalog.py      — In-memory partition catalog; raises AlreadyExistsException / NotFoundException
benchmark.py         — Three-experiment benchmark harness (§3.11)
benchmark_results_20260311_154324.txt  — Canonical benchmark output
```

All modules use the Python standard library only — no external dependencies.

---

## Running the benchmark

```bash
python benchmark.py
```

Runs three experiments and writes a timestamped results file alongside the live output:

**Experiment 1 — Request-count reduction vs change fraction f**

Confirms O(ΔP) vs O(P) behavior across f = 0.001 to 1.0 over a
c₁=365 × c₂=50 × c₃=24 = 438,000-partition hierarchy.

**Experiment 2 — Convergence latency vs quiet-window W**

Uses a simulated clock. Confirms latency = W + G + ε (< 1 ms overhead)
across W = 1 s to 120 s at f = 0.01, G = 1 s.

**Experiment 3 — Worker throughput**

Single-threaded, in-memory, W = G = 0. Reports events/sec and partitions/sec
across f = 0.001 to 1.0.

Expected output (matches `benchmark_results_20260311_154324.txt`):

```
Experiment 1 (f=0.01):  4,380 committed,  4,380 requests,  104× reduction,  $0.022/day
Experiment 2 (W=5, G=1): median latency 6.001 s  (= W + G + ε)
Experiment 3 (f=0.01):  ~212,000 events/sec,  ~38,500 partitions/sec
```

**Requirements:** Python 3.8+, no pip installs needed.

---

## Key parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `W` | Quiet-window seconds; flush_at[k] = now() + W, extended on each new intent | 5.0 s |
| `G` | Grace interval; wm_worker = now() − G | 1.0 s |
| `T_max` | Optional max buffering deadline; flush_at[k] capped at first_seen[k] + T_max; key eligible when now() ≥ first_seen[k] + T_max + G | None |
| `f` | Change fraction ΔP/P | — |
| `α` | Verification ops per changed partition (analytical model) | 2 |

---

## Correspondence to paper sections

| Module | Paper section |
|--------|---------------|
| `woera_buffer.py` | §3.4, Algorithm 4, Appendix A.5 |
| `pmcp.py` | §3.5, Algorithms 5 & 6 |
| `rtmsf_worker.py` | §3.2–3.3, Algorithms 1–3 |
| `event_generator.py` | §3.8.2 workload parameters |
| `benchmark.py` | §3.11 (Experiments 1–3) |

---

## License

MIT License — see [LICENSE](LICENSE).
