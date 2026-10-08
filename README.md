# RTMSF

[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](pyproject.toml)
[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Real-Time Metadata Synchronization Framework**, by **Ankit Joshi**.

Independent ingestion and cleanup jobs can change data files directly in object storage without updating a shared catalog.
RTMSF centralizes partition-membership maintenance for these existing tables.
It processes notifications and checks affected partitions without rescanning the complete table.

The first object triggers registration; intermediate changes preserve occupied entries; final deletion triggers removal after storage verification.
These updates follow the configured processing window. Membership does not establish batch readiness.

WOERA groups events by partition. PMCP verifies storage and updates catalog membership.
Tables and their schemas already exist; RTMSF does not discover tables or infer schemas.
The supplied router requires ordered `column=value` paths and correct existing partition locations and storage descriptors.

![RTMSF cloud-neutral architecture](docs/figures/architecture.svg)

The core uses provider-neutral interfaces. AWS supplies the current adapters and experiment runner.
Other cloud implementations and multi-worker operation are not validated.

## Choose your task

| Task | Guide |
|---|---|
| Understand the algorithm, guarantees, and limits | [System behavior](docs/system-behavior.md) |
| Run the AWS worker with existing tables | [AWS worker setup](docs/aws.md#run-the-normal-aws-worker) |
| Provision AWS resources and reproduce experiments | [AWS experiment setup](docs/aws.md#aws-experiment-setup) |
| Understand the five graphs, recovery table, and supporting checks | [Experiment specification](docs/experiments.md) |
| Inspect results or redraw saved graphs without cloud access | [Evidence guide](evidence/README.md) |

## Install and test locally

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone https://github.com/ankitjoshi14/rtmsf-partition-sync.git
cd rtmsf-partition-sync
uv sync --frozen --extra dev
uv run --frozen --extra dev pytest -q
```

Local tests use fakes and SDK stubs. They need no cloud account.

## Repository layout

```text
src/rtmsf/          Core algorithm, interfaces, and worker
  adapters/        Provider implementations; AWS is available
src/experiments/   Saved-result reporting and evidence export
  aws/             AWS provisioning, workloads, and measurement collection
tests/             Local verification
docs/              System behavior, experiment specification, and AWS guide
evidence/          Selected measured CSVs, graphs, tables, and report inputs
runs/              Original local execution records; generated and Git-ignored
```

Cloud-specific setup belongs in its provider guide and implementation folder.
Provider adapters and experiment runners share the report format.
The [experiment specification](docs/experiments.md) defines that format and the measurements.

Experiment runners save measurements and generate reports under `runs/`.
The offline exporter creates selected, verified bundles for sharing in `evidence/`.
That folder belongs in Git; raw runs remain separate. Exporting does not rerun an experiment.
Current evidence covers up to 10K starting partitions. Larger extensions remain unmeasured.

## Contributing and citation

Report bugs or propose focused changes through [GitHub issues](https://github.com/ankitjoshi14/rtmsf-partition-sync/issues).
Include a reproduction, expected behavior, and observed behavior. Run the local tests before submitting changes.
Use [CITATION.cff](CITATION.cff) to cite the software, released under the [MIT License](LICENSE).
