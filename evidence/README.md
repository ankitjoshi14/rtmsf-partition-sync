# Experiment evidence

This folder contains selected AWS measurements and OCI/Azure/GCP functional proof records. It belongs in Git alongside the source code.
The AWS exporter copies measurement CSVs unchanged and generates reports from saved inputs.
Original execution records remain separate under `runs/` and are Git-ignored.
Start with [SUMMARY.md](SUMMARY.md) for AWS settings and outcomes; use the separate provider bundles below for functional evidence.
The separate [OCI functional proof](oci/README.md) preserves selected original
local/Compute reports, failures and deployed-source fingerprints; its catalog
was an in-memory test double.
The separate [Azure functional proof](azure/README.md) contains real catalog/storage/queue observations and process-recovery evidence; it is not part of the AWS performance measurements.
The separate [GCP functional proof](gcp/README.md) records twelve native gates, the completed 24-hour serialized lifecycle, preserved schema and independent final verification. The GCP guide includes its architecture and limits.

| Item | Purpose |
|---|---|
| `figures/graph_data.csv` | Measurement points for the five graphs. |
| `figures/graph_1` through `graph_5` | PNG previews and vector PDF exports. |
| `figures/checks_table.md` | Short/long bursts, routing, replay, markers, rapid changes, and retry-check results. |
| `figures/recovery_table.md` | Results for the four ADD/DROP crash cases. |
| `figures/report_inputs.json` | Individual recovery/check outcomes, settings, and graph outcomes used to generate tables and summaries. |
| `activity/` | Burst, grouping-comparison, continuous-traffic, and load CSVs with their summary. |
| `buffer/` | Buffer-limit comparison CSVs with memory and backlog results. |
| `activity/report_inputs.json`, `buffer/report_inputs.json` | Saved rows, timelines, and errors used to regenerate these reports. |
| [provenance.json](provenance.json) | Source run, code, image identity, and settings behind each selected phase. |
| [SHA256SUMS](SHA256SUMS) | File fingerprints that detect changes to exported files. |

Only selected phases appear. Checks and recovery use tables; their outcomes are not graph points in `graph_data.csv`.
The runner records those outcomes automatically. The report generators read `report_inputs.json`; no manual table entry is needed.
Missing measurements remain unknown. Failed and unattempted conditions remain recorded.
A workload pass does not establish measurement completeness or sustainable processing capacity.

## Redraw the graphs and tables

Run from the repository root with the experiment dependencies installed:

```sh
uv run --frozen --extra experiment python -m experiments.figures \
  --csv evidence/figures/graph_data.csv \
  --results evidence/figures/report_inputs.json \
  --output-dir runs/regenerated-figures
```

This command uses saved values and makes no cloud calls. It keeps evidence inputs unchanged and replaces reports in the output directory.
To regenerate the activity tables:

```sh
uv run --frozen --extra experiment python -m experiments.activity_reports \
  --results evidence/activity/report_inputs.json \
  --output-dir runs/regenerated-activity
```

For buffer tables, replace `evidence/activity/` with `evidence/buffer/` and choose a separate output directory.
The repository's `docs/experiments.md` defines measurements; provider guides describe cloud execution.
