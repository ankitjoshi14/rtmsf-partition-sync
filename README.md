# RTMSF

[![CI](https://github.com/ankitjoshi14/rtmsf-partition-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/ankitjoshi14/rtmsf-partition-sync/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](pyproject.toml)
[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Real-Time Metadata Synchronization Framework**, by **Ankit Joshi**.

RTMSF keeps partition membership in configured Hive-style tables aligned with
object storage. It groups upload/delete notifications by partition, checks
current storage, and adds or removes catalog entries as needed.

```text
Object event → routing → WOERA (group events) → PMCP (check storage and reconcile)
```

The Python core is provider-neutral. The included AWS implementation uses
S3, SQS, Glue, and one ECS/Fargate worker. Unfinished work remains replayable;
terminal failures are durably recorded before acknowledgement.
[System behavior](docs/system-behavior.md) explains guarantees, configuration,
and limits. Other cloud implementations and multi-worker operation are not validated.

## Start here

| What you need | Where to look |
|---|---|
| Understand the algorithm and its guarantees | [System behavior](docs/system-behavior.md) |
| Understand each graph, workload and measurement | [Experiment specification](docs/experiments.md) — authoritative design |
| Deploy, run experiments, regenerate graphs and clean up | [AWS runbook](docs/aws.md) |
| Inspect the paper results and limitations | [Evidence index](evidence/README.md) |

## Run locally

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone https://github.com/ankitjoshi14/rtmsf-partition-sync.git
cd rtmsf-partition-sync
uv sync --frozen --extra dev
uv run --frozen --extra dev pytest -q
```

Local tests use fakes and SDK stubs; no AWS account is needed. For the worker
alone, install `.[aws]` and run `python -m rtmsf.aws_app` with the configuration
in [system behavior](docs/system-behavior.md). Importing the core needs no AWS SDK.

## Repository responsibilities

```text
src/
  rtmsf/                  Core models, routing, WOERA, PMCP and worker
    adapters/aws.py       S3, SQS and Glue implementations of core interfaces
    aws_app.py            Start the normal AWS worker
  experiments/
    figures.py            Graph CSV, plots and outcome tables; works offline
    activity_reports.py   Activity comparison tables and CSVs; works offline
    evidence.py           Save run files and identify their source
    aws/                  AWS experiment runner and its helpers
      run.py              Settings, execution order, final reports and cleanup
      workloads.py        Fixtures, five-graph workloads and crash recovery
      checks.py           Supporting burst, routing and retry checks
      activity.py         Bounded handler comparison and continuous workloads
      activity_worker.py  Experiment-only per-event control using the real worker
      runtime.py          Worker control, query checks and observations
      crash_worker.py     Experiment-only fault injection around the real worker
      crawler.py          Glue event-based baseline
      trial_buckets.py    Isolate measurements in separate buckets
      request_metrics.py  CloudWatch request counts
      request_audit.py    CloudTrail corroboration and calibration
      check_request_metrics.py  Standalone known-request calibration
      deploy.py / template.yaml AWS fixture provisioning
tests/                    Local verification
docs/                     System behavior, experiment specification and runbook
evidence/                 Measured CSVs, report inputs, figures and provenance
runs/                     Generated local bundles (created on use, Git-ignored)
```

There are three responsibilities: **core system**, **experiment execution**, and
**report generation**. Another cloud would implement the core interfaces and add
a sibling to `src/experiments/aws/`, producing the same graph CSV format.
The AWS adapter declares its interfaces explicitly, for example
`class S3ObjectStore(ObjectStore)`; related AWS classes share one file.
`__init__.py` marks a directory as a Python package. These files contain no
worker or experiment logic. The manuscript and research notes are maintained
separately from this software repository.

## Generate results

The [specification](docs/experiments.md) defines five graphs, a recovery table,
and eight supporting checks. The [runbook](docs/aws.md) starts with a small AWS
validation, then explains configurable sizes and repetitions. Those runs require an AWS
account and incur charges. Larger planned sizes are not completed measurements.

```text
Specification → experiments.aws.run → runs/<execution>/
                                      ├── graph_data.csv → experiments.figures → graphs
                                      ├── report_inputs.json → outcome tables and summary
                                      └── logs, configuration, source and measurement records
```

Every execution saves its CSV and generates its reports. You can regenerate
reports offline from the CSV and recorded outcomes. Separate buckets isolate
measurements; they do not create separate figures. Missing measurements remain
unknown. The production image contains `rtmsf`; the experiment image additionally
contains the fault launcher.

Use the [retained evidence](evidence/README.md) to try graph generation;
the regeneration commands are in its README.
`evidence/` contains selected exports, not scripts or a history of exploratory
runs. [Provenance](evidence/provenance.json) identifies each measured source
version. Full audit records and source archives remain in the local run bundles.

## Contributing and citation

Report bugs or propose focused changes through
[GitHub issues](https://github.com/ankitjoshi14/rtmsf-partition-sync/issues).
Include a minimal reproduction and expected versus observed behavior; run the
local tests and describe any changes to system behavior or experiment claims.

Use [CITATION.cff](CITATION.cff) to cite the software, released under the
[MIT License](LICENSE).
