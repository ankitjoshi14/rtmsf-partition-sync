# Paper evidence

Ankit Joshi · October 2, 2026

Start with [paper/README.md](paper/README.md). It maps the manuscript's five
figures and additional tables to their measured execution, inputs, settings and
limitations. The CSVs contain AWS observations; no simulated values are used.

`paper/` is the compact, offline reproduction input. The prepared release archive
adds scoped request/calibration records, worker telemetry, catalog/query snapshots,
exact per-run source archives, retained failures and checksums. Raw experiments
remain under local, Git-ignored `runs/`; they are not overwritten by the export.

Current results reach 1,000 starting partitions and 1,000 files in the affected
partition. Larger runs and multi-cloud validation are pending. The primary figures
have two comparison repetitions; the later activity comparisons have one and
the buffer comparison has two. These counts do not establish tail latency,
sustained capacity, total cost savings, or production adoption.
