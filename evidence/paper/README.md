# RTMSF paper results

Ankit Joshi · October 2, 2026 · RTMSF software 0.2.0

These are retained AWS measurements. [Experiment specification](../../docs/experiments.md)
defines the workloads and axes; [AWS runbook](../../docs/aws.md) gives the execution
commands. All measurements use one AWS worker, Q=10 s and T_max=31 s. Different
source versions are identified separately and their results are not pooled.

| Directory | Manuscript use | Measured conditions | Repetitions |
|---|---|---|---|
| `figures/` | Figures 1–5, recovery and supporting checks | 10/100/500/1,000 starting partitions; Figure 4: 1/100 existing files, 100 starting partitions | Two comparative; one correctness/fault |
| `graph-validation/` | Later marker and affected-file observations | 100/1,000 starting partitions; 1/100/1,000 existing files | One |
| `activity/` | Immediate control, continuous activity and load tables | Four burst schedules × two handlers; four membership-changing streams; four load conditions | One |
| `buffer/` | Buffer table and call accounting | 20 changes/s for 120 s; 10/1,000 targets × 100/1,000 message caps | Two |
| `failed-activity/` | Retained failure | First activity attempt: ten passed, one failed, five unattempted | Incomplete |

Execution IDs and exact measured source identities are in [provenance.json](provenance.json).
`graph-validation` retains the parent's `incomplete_or_failed` status. Its graph
phase passed; its first activity child failed on a producer connection error.
The successful activity follow-up used fresh fixtures. A fixture-seeding failure
occurred before measurements and is retained in the full release records.

## Regenerate offline

From the repository root, install the locked dependencies once:

```sh
uv sync --frozen --extra dev
uv run --frozen --extra dev pytest -q
```

Report generation makes no AWS calls:

```sh
uv run --frozen --extra experiment python -m experiments.figures \
  --csv evidence/paper/figures/graph_data.csv \
  --results evidence/paper/figures/report_inputs.json \
  --output-dir runs/regenerated-figures

uv run --frozen --extra experiment python -m experiments.activity_reports \
  --results evidence/paper/activity/report_inputs.json \
  --output-dir runs/regenerated-activity

uv run --frozen --extra experiment python -m experiments.activity_reports \
  --results evidence/paper/buffer/report_inputs.json \
  --output-dir runs/regenerated-buffer
```

Use `graph-validation` in the figure command or `failed-activity` in the activity
command to regenerate the additional sets. The commands write to the chosen output directory and replace generated reports
there on rerun; choose another path to retain both versions. Do not use an archived
evidence directory as output. Exact reproduction of the
historical report formatting uses each run's `source.tar.gz` in the full archive.
Current plotting retains the same scientific values, median/min–max rules and axes.

## What the observations establish

- RTMSF uses one HEAD for an ordinary new-partition ADD and one LIST for these
  registered-partition appends. The initialized event crawler also localizes
  new-partition work; its affected-file GET inspection grows in the tested configuration.
- Athena was faster than RTMSF at every primary response-time point. The starts
  are method-specific; RTMSF includes notification delivery and its grouping window.
  Internal Athena parallelism is not measured or asserted as the cause.
- Folder order changes Athena's observed request work substantially. This is an
  implementation/layout observation, not one universal LIST per partition.
- Grouping reduces repeated checks when activity revisits a partition. The
  immediate control has lower first visibility. Distributed activity offers less
  grouping opportunity, and the serial worker accumulates lag at 20/s.
- Larger message buffers improve grouping/catch-up. Pending notifications count
  toward the cap even when they share one partition. `buffer/call-accounting.json`
  retains the serial SDK-duration accounting behind the manuscript's explanation.

## Audit and release limits

Selected request counts require corroboration between CloudWatch and CloudTrail;
activity/buffer runs also compare worker API records. Missing values stay unknown.
Some absent CloudWatch component series are inferred as observed zero only through
the documented AllRequests accounting rule. Telemetry is best-effort; this is not
billing-grade or total-cloud-cost accounting.

The compact repository retains plotting/report inputs, not the complete AWS audit.
It omits embedded CloudTrail coverage and detailed request-coverage envelopes;
all scientific values and axes are unchanged. The full archive retains those records.
The full release archive adds scoped CloudTrail events and calibration evidence,
CloudWatch responses, decoded/ACK telemetry, operations, query rows and later
catalog snapshots. Credential identity fields, receipt handles and source IPs are
removed; the account number uses a consistent placeholder. Source archives and
scientific counts are preserved. Original artifact lists describe original private
bytes; the release's `SHA256SUMS` verifies exported bytes.

The primary figure run did not retain full catalog snapshots. Its assertions and
query rows remain available; later snapshots do not fill that historical gap.
Two-minute activity tests and small repetition counts do not establish tail latency,
long-run capacity, or a one-minute freshness SLA. The catalog-driven workflow in
the manuscript is motivating, not a deployed/evaluated consumer.
