# Run the AWS experiments

This runbook covers the five-graph AWS experiments in
[the experiment specification](experiments.md). Its runner and reports are
implemented; accepted AWS results must come from the retained run evidence.

Run these commands from the repository root. Use your own AWS profile, account,
VPC and public subnets. The fixture tools are bounded to `us-west-2` and
`rtmsf-` stack names. The profile needs permission to provision the resources
in `src/experiments/aws/template.yaml` and operate the experiment.

The stack starts with zero workers. It creates disposable table fixtures and
fresh S3/SQS/Glue/Athena/ECR/ECS resources. The worker itself only maintains
configured existing tables. Public subnet egress on HTTPS lets Fargate reach AWS
APIs without creating a NAT gateway.

## 1. Install and provision

Replace the example account/network values before running:

```bash
uv sync --frozen --extra dev
export RTMSF_PROFILE=your-profile
export RTMSF_ACCOUNT=123456789012
export RTMSF_REGION=us-west-2
export RTMSF_STACK=rtmsf-paper-v1

uv run --frozen --extra dev python -m experiments.aws.deploy \
  --stack "$RTMSF_STACK" --expected-account "$RTMSF_ACCOUNT" \
  --profile "$RTMSF_PROFILE" --region "$RTMSF_REGION" \
  --experiment-id paper-v1 --database rtmsf_experiments \
  --vpc vpc-REPLACE --subnets subnet-REPLACE subnet-REPLACE
```

Deploy the stack once, then reuse it for sequential experiment executions.
Choose new stack, experiment and database names if creating a separate deployment.
`--experiment-id` is 3–20 lowercase letters/digits/hyphens, starting with a letter.
The named base buckets become `rtmsf-paper-v1-data-<account>-<region>` and
`rtmsf-paper-v1-crawler-<account>-<region>`; other stack resources inherit the
`rtmsf-paper-v1` stack prefix. These names apply to new deployments; existing AWS
resources retain their original names.
The command prints the stack outputs. Store its `EcrRepositoryUri` in
`RTMSF_ECR`; it is a registry/repository address, not a credential.

## 2. Build this source and record its identity

Use Docker with ARM64 build support. Rebuild the worker image when its runtime
code or dependency configuration changes. Host-only runner, report, test, or
documentation changes may reuse an image whose runtime and build-input hashes still match;
each execution separately archives and identifies its current host source.
The default Docker target is `production`, whose entry point is `rtmsf.aws_app`.
Build `--target experiment` for this pilot so the same image also contains
`experiments.aws.crash_worker`. The runner selects normal startup for measurement
trials and the experiment launcher only for deliberate fault trials.

For a crash case, the runner passes the target table, partition, ADD/DROP and
crash point as configuration. At that exact point, the launcher creates a
crash-once marker in S3 and exits abruptly. ECS starts a replacement with the
**same image and configuration**. The durable marker prevents a second injected
crash; the replacement processes the unacknowledged SQS message when it becomes
available again. The runner stays outside the worker and verifies the crash,
message replay/acknowledgement, and catalog state. Each case/repetition gets a
fresh marker. See [the four crash cases](experiments.md#table-1--crash-recovery).

```bash
export RTMSF_ECR=REPLACE_WITH_EcrRepositoryUri
export RTMSF_SOURCE_HASH=$(uv run --frozen --extra dev python -c \
  'from pathlib import Path; from experiments.evidence import source_snapshot; print(source_snapshot(Path("."))["sha256"])')
export RTMSF_TAG=pilot-source-$RTMSF_SOURCE_HASH
export RTMSF_BUILD_MANIFEST="runs/build-$RTMSF_SOURCE_HASH/image-build.json"

docker buildx build --platform linux/arm64 --provenance=false --load \
  --target experiment -t rtmsf:pilot .
aws ecr get-login-password --profile "$RTMSF_PROFILE" --region "$RTMSF_REGION" \
  | docker login --username AWS --password-stdin "${RTMSF_ECR%%/*}"
docker tag rtmsf:pilot "$RTMSF_ECR:$RTMSF_TAG"
docker push "$RTMSF_ECR:$RTMSF_TAG"

export RTMSF_DIGEST=$(aws ecr describe-images \
  --profile "$RTMSF_PROFILE" --region "$RTMSF_REGION" \
  --repository-name "${RTMSF_ECR#*/}" --image-ids imageTag="$RTMSF_TAG" \
  --query 'imageDetails[0].imageDigest' --output text)
export RTMSF_IMAGE="$RTMSF_ECR@$RTMSF_DIGEST"

uv run --frozen --extra dev python - <<'PY'
import hashlib, json, os, subprocess
from pathlib import Path
inspect_image = '''
import hashlib, importlib.metadata, json
from pathlib import Path
root = Path('/app')
files = sorted((root / 'src/rtmsf').rglob('*.py')) + [
    root / name for name in ('src/experiments/__init__.py',
                            'src/experiments/aws/__init__.py',
                            'src/experiments/aws/crash_worker.py',
                            'src/experiments/aws/activity_worker.py')]
print(json.dumps({
    'files': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
    'runtime_dependencies': {n: importlib.metadata.version(n) for n in ('boto3', 'botocore')},
}))
'''
runtime = subprocess.check_output([
    'docker', 'run', '--rm', '--entrypoint', 'python', 'rtmsf:pilot', '-c',
    inspect_image,
], text=True)
manifest = Path(os.environ['RTMSF_BUILD_MANIFEST'])
manifest.parent.mkdir(parents=True, exist_ok=True)
manifest.write_text(json.dumps({
    'source_hash': os.environ['RTMSF_SOURCE_HASH'],
    'image': os.environ['RTMSF_IMAGE'],
    'target': 'experiment',
    'build_files': {name: hashlib.sha256(Path(name).read_bytes()).hexdigest()
                    for name in ('Dockerfile', 'pyproject.toml', '.dockerignore')},
    **json.loads(runtime),
}, indent=2) + '\n')
PY
```

The immutable image digest identifies the runtime, including dependencies selected
by the Docker build. `uv.lock` pins the local test/harness environment. No AWS
credentials are baked into the image or build manifest.

## 3. Deploy and run a small validation

```bash
uv run --frozen --extra dev python -m experiments.aws.deploy \
  --stack "$RTMSF_STACK" --expected-account "$RTMSF_ACCOUNT" \
  --profile "$RTMSF_PROFILE" --region "$RTMSF_REGION" \
  --update-image --image "$RTMSF_IMAGE"

uv run --frozen --extra experiment python -m experiments.aws.run \
  --stack "$RTMSF_STACK" --expected-account "$RTMSF_ACCOUNT" \
  --profile "$RTMSF_PROFILE" --region "$RTMSF_REGION" \
  --sizes 100 1000 --files 1 100 1000 --graph4-partitions 1000 \
  --layout-sizes 100 1000 --repetitions 1 --timeout-seconds 600 --max-minutes 240 --activity quick \
  --request-source cloudwatch --audit-wait-seconds 600 \
  --build-manifest "$RTMSF_BUILD_MANIFEST" --output-dir runs
```

This validation selects all five graphs, fresh fixtures, Q=10 seconds, and one
repetition per condition. After the graph/correctness checks pass, the same command
runs the activity comparisons and load tests under `activity/` in the printed bundle.
An optional larger repeated run uses `--sizes 100 1000 5000 10000`,
`--layout-sizes 100 1000 5000 10000`, `--repetitions 5`,
`--correctness-repetitions 3`, `--activity full` and `--max-minutes 900`.
The 100K scaling extension is separate. Each run is independent; no CSV assembly is needed. A smaller Graph 5-only check uses
`--graph5-only --layout-sizes 100 --repetitions 1 --max-minutes 60`
with the same account, stack, image-manifest and collection options: four trials.
Each measured trial gets its own bucket, for example
`rtmsf-g2-glue-p1000-r1-<run>` or `rtmsf-g4-rtmsf-p1000-f100-r1-<run>`.
Wait 15 minutes after enabling CloudWatch, then calibrate CloudTrail calls; the final collection waits 10 minutes
with these buckets idle. Two complete quiet minutes separate setup reads from
each measurement: AWS assigned some setup reads to the second following minute
in the September 23 run. This guard addresses that observation, not a guaranteed
AWS delivery bound. Final verification queries run afterward. The runner removes
the new buckets and restores its temporary queue permissions and CloudTrail
selectors. `trial-buckets.json` records resource ownership and cleanup.
Each crawler trial also gets a separate queue, removed during cleanup, so late
notifications from another trial cannot interfere.
Graph 5 requires isolated CloudWatch buckets. `--request-source cloudtrail`
remains available only for selective earlier-graph modes, for historical checks.
After interruption, stop this run's worker/crawler and rerun the command with
`--cleanup-run <printed-run-directory>` to finish bucket/table cleanup and
restore permissions. This writes a separate receipt and preserves original evidence.
It targets the lifecycle trace, RTMSF/crawler/MSCK partition-size comparisons,
RTMSF/crawler file-count comparisons, RTMSF/Athena key-order comparisons,
and four ADD/DROP crash boundaries.
It also runs the eight supporting burst, routing, replay, rapid-change, and
retry checks. Any check that does not run stays explicitly **not attempted**.
Use `--repetitions 5 --correctness-repetitions 3` for the planned repeat run:
five per comparison condition, three per lifecycle/crash/supporting case.
Without the second option, both counts use `--repetitions` (default 1).
Choose the time budget explicitly. `--max-minutes` stops new trials from
starting; setup, active operations, shutdown and final collection can extend
elapsed time beyond it. Supported inputs are up to 100K main-layout partitions,
10K existing files and 100K Graph 5 partitions. Those limits do not establish
successful scale measurements. Choose sizes and repetitions as described below.

Read the printed directory's `SUMMARY.md`, `graph_data.csv`, `graph_1`–`graph_5`
PNG/PDF files, `recovery_table.md`, and `checks_table.md`. Failed, incomplete,
and unattempted conditions remain visible; an unstarted condition is not an
attempt. Retain `manifest.json`, `experiment.jsonl`, query/runtime evidence,
request windows and raw audit records, `report_inputs.json`, and
`artifact-hashes.json`. The build manifest verifies local runtime files against
the image contents and checks the deployed digest; the pilot retains it as
`image-build.json` and records actual task definitions.
The exact source used by an execution is retained in `source.tar.gz`.
The source archive includes the current experiment specification and tests.
Selected, verified exports belong in `evidence/`; the
runner does not publish or curate them automatically.
CSV times are rounded to three decimals; full timestamps remain in the experiment log.
Raw runs default to `runs/`, which is ignored by Git. One execution produces:

```text
runs/<execution>/
├── SUMMARY.md             # Start here: outcomes and missing measurements
├── graph_data.csv         # Points used for every graph
├── graph_1.png / .pdf      # Likewise graph_2 through graph_5
├── recovery_table.md      # Four crash cases
├── checks_table.md        # Supporting workloads
├── report_inputs.json     # Recreate the tables and captions offline
└── ...                    # Configuration, logs, raw observations, source and hashes
```

The command returns exit code 0 only when all selected work and required evidence
collection succeed; failed, incomplete or unattempted work returns 1 after saving
the bundle. `SUMMARY.md` and `report_inputs.json` record that status and errors.
The graph CSV has 12 columns, including `layout` after `method`, with separate
HEAD, GET and LIST rows. Graphs 2/4/5 sum these within each complete repetition
and layout. Graph 5 uses two panels with shared axes and the same method colors.
Legacy 11-column CSVs remain readable. Bucket names never determine the figures.
`--update-image` keeps the deployed CloudFormation template and its resource
names, changing only the worker image parameter. Template changes need a fresh
deployment through this utility. Rebuild after runtime source/dependency changes. A historical manifest without
`files`/`build_files`, or one with mismatched runtime/build-input hashes, cannot validate this checkout.
Keep each verified manifest with its evidence instead of overwriting old records.

### Repeat the manuscript's measured conditions

For the primary figures, replace the size options in the complete command above
with the following (omit `--activity quick` to keep the source sets separate):

```text
--sizes 10 100 500 1000 --layout-sizes 10 100 500 1000
--files 1 100 --graph4-partitions 100 --repetitions 2
--correctness-repetitions 1 --max-minutes 300
```

The graph-validation follow-up instead uses `--sizes 100 1000`,
`--layout-sizes 100 1000`, `--files 1 100 1000`, `--graph4-partitions 1000`,
and one repetition. These commands use current source and create fresh
measurements. Exact historical source is retained in the full release archive;
its digest is recorded in [provenance](../evidence/provenance.json).
Cloud timings need not match. Do not merge results across source versions.

### What a run and a rerun create

Every execution gets an ID such as `run_20260929_090000_a1b2`: UTC date/time plus
a random suffix. All its repetitions and graphs go into **one local folder**,
`runs/run_20260929_090000_a1b2/`. The folder is created automatically and contains
the files shown above. The runner records actual resource names in
`experiment.jsonl`; `trial-buckets.json` also maps measurement tables/buckets
and records their cleanup.
`runtime-api-counts.json` contains run-wide adapter totals for diagnostics;
graph request counts come from the separate per-trial measurement windows.

The stack and Glue database (`rtmsf_experiments` by default) are reused.
**Tables are fresh for every condition,
method and repetition.** For Graphs 2/3 at 100 partitions, repetition 1:

| Method | Example table in `rtmsf_experiments` |
|---|---|
| RTMSF | `rtmsf_scale_p100_r1_20260929_090000_a1b2` |
| Glue event crawler | `crawler_scale_p100_r1_20260929_090000_a1b2` |
| Athena MSCK | `msck_scale_p100_r1_20260929_090000_a1b2` |

Names follow **method → experiment → condition → repetition → run**.
`p100` means 100 starting partitions, `f100` means 100 existing files in the
changed partition, and `r1` means repetition 1. Graphs 2 and 3 share the `scale`
tables and trials. Other RTMSF table examples use the same run suffix:

| Purpose | Table name (`<run>` = `20260929_090000_a1b2`) |
|---|---|
| Graph 1 | `rtmsf_lifecycle_r1_<run>` |
| Graph 4 | `rtmsf_files_p100_f100_r1_<run>` |
| Graph 5 | `rtmsf_layout_day_first_p100_r1_<run>` (or `tenant_first`) |
| Recovery | `rtmsf_recovery_add_before_processing_r1_<run>` (one per crash case) |
| Supporting check | `rtmsf_check_short_burst_r1_<run>` (one per named check/fixture) |

Baseline tables use `crawler_` or `msck_` where applicable. The run suffix
separates reruns. Internal trial IDs in evidence records are unchanged; each
record includes its actual AWS table name.

For a small all-output run with `--sizes 100 --files 1 100
--graph4-partitions 100 --layout-sizes 100 --repetitions 1`, the runner creates
**24 new tables**: 11 comparison tables, 1 lifecycle table, 4 recovery tables,
and 8 supporting-check tables (the routing check needs two). The shared stack
also has one prototype table, created at deployment and copied for every method.
Other settings change the number of fresh tables; the main validation command
above creates 31.

Rerunning the same command creates a new execution ID, folder, table names,
measurement buckets and object prefixes. It runs fresh work; it does not append
to the previous CSV or resume an old run. Keep failed/incomplete bundles too.

At completion, the runner attempts to stop the worker and logging and remove
comparison tables/buckets and temporary crawler queues. Lifecycle, recovery and
supporting-check tables/data remain, along with shared stack resources and local
results. Inspect the recorded cleanup outcome; `--cleanup-run` retries measurement
resource cleanup without rerunning the experiment. Offline graph regeneration
uses existing CSV/outcomes and creates no AWS resources.

### Choose sizes and repetitions

Keep the common profile/account/stack/region, `--request-source cloudwatch`,
`--audit-wait-seconds 600`, `--build-manifest` and `--output-dir` arguments above.
Select all desired sizes in one command. For example, these options run Graphs
2/3 at four sizes, once per method:

```text
--comparisons-only --sizes 10 100 500 1000 --repetitions 1
```

That execution produces one CSV and its graphs in one result folder. The
plotting utility can redraw those reports offline from the saved records.

| Outputs | Size controls |
|---|---|
| Graphs 2/3 | `--comparisons-only --sizes N1 N2 ...` |
| Graph 4 | `--graph4-only --files F1 F2 ... --graph4-partitions N` |
| Graph 5 | `--graph5-only --layout-sizes N1 N2 ...` |

Omit the selective mode flag to run all five graphs, recovery and supporting
checks. `--repetitions` controls comparison repeats; `--correctness-repetitions`
separately controls lifecycle, recovery and supporting checks. Measured and optional larger matrices remain in [the specification](experiments.md).

Choose a time budget from preceding runs. `--max-minutes` stops new trials from
starting; setup, active work, collection and cleanup may finish later. At 100K,
three methods with five repetitions prepare 1.5 million baseline files. Large repeated matrices prepare many independent copies;
small-validation time budgets do not cover that work. Preserve failed and
unattempted results along with successful ones.

### Check CloudWatch request counters

To validate request instrumentation independently of the graph workloads:

```bash
uv run --frozen --extra experiment python -m experiments.aws.check_request_metrics \
  --profile "$RTMSF_PROFILE" --expected-account "$RTMSF_ACCOUNT" \
  --region "$RTMSF_REGION"
```

It creates six buckets named `rtmsf-cw-check-n<count>-r<repetition>-<run>`:
1, 10 and 100 requests per family, twice. Odd repetitions use HEAD object and
LIST V2; even repetitions use HEAD bucket and LIST V1. Multi-page cases exercise
pagination. Allow about 30 minutes for activation, requests and metric delivery.
Read `calibration.csv` in the printed evidence directory. It records expected
and observed counts, with blanks for missing data; raw request IDs, metric
snapshots, source and hashes accompany it. The command removes its temporary
buckets and metric configurations after collection. A pass verifies these
known batches, not guaranteed completeness of future CloudWatch measurements.
CloudWatch can label requests with a later minute. The bucket stays idle while
the collector includes complete minutes through `quiet_until`; the original
workload start/end remain recorded separately. Do not browse the trial bucket
or run verification reads during this interval.
Add `--list-only` to test zero HEAD/GET activity in fresh buckets. The live
control returned LIST=1 and AllRequests=1, with no HEAD/GET datapoints. Those
absent counters are inferred as observed zeros only when recorded operation
counts fully account for AllRequests, method activity exists, and no metric
read failed. Otherwise they remain unknown. Raw evidence distinguishes this
inference from an explicit zero datapoint.

After interruption, `--resume <check-directory> --cleanup-only` removes only
that check's remaining buckets without sending more test requests. Include the
same profile, account and region arguments. A cleanup receipt preserves the
original run record. The graph runner uses CloudWatch by default and retains
CloudTrail as supporting evidence.

Earlier CloudTrail HEAD/GET/LIST values are **observed lower bounds**, including recorded retries.
Calibration and a quiet collection period do not prove CloudTrail completeness.
Unavailable measurements remain blank CSV cells and gaps in figures, never
zero. A workload pass with missing request evidence becomes an incomplete
request result. Reconcile late records before making performance claims.

The required layout comparison is Graph 5, using the CSV's `layout` column.
`SUMMARY.md` records occupied prefixes; `checks_table.md` shows the short burst's
observed event, PMCP-attempt and applied-change counts.
If a budget prevents a check from running, use the selective commands below on
fresh fixtures and retain both the original outcome and the follow-up record.
Use `--checks-only duplicate_old_events rapid_changes` to rerun just those
checks on fresh fixtures. These options do not reuse earlier results.
Use `--recovery-only add_after_catalog_change` to rerun a selected crash case
with a fresh table and marker. The earlier result remains in its original run.
Use `--graph4-only` with selected `--files` values to rerun file-count comparisons
without repeating lifecycle, recovery, or the partition-count comparisons.
Use `--graph5-only` for the key-order comparison.

Regenerate all figures and tables without AWS calls, using the printed pilot
directory; the output below is a separate directory:

```bash
export RTMSF_RUN=runs/REPLACE_WITH_PRINTED_PILOT_ID
uv run --frozen --extra experiment python -m experiments.figures \
  --csv "$RTMSF_RUN/graph_data.csv" --results "$RTMSF_RUN/report_inputs.json" \
  --output-dir "${RTMSF_RUN}-regenerated"
```

## Bounded activity validation

The [activity protocol](experiments.md#bounded-activity-validation) compares the
complete worker with an experiment-only immediate handler and exercises ongoing
mixed traffic. It uses the same stack and experiment image. Build and deploy that
image above, then run:

```bash
uv run --frozen --extra experiment python -m experiments.aws.activity \
  --stack "$RTMSF_STACK" --expected-account "$RTMSF_ACCOUNT" \
  --profile "$RTMSF_PROFILE" --region "$RTMSF_REGION" \
  --build-manifest "$RTMSF_BUILD_MANIFEST" --repetitions 1 --max-minutes 65
```

Use `--conditions quick spaced distributed mixed` for just the comparison, or
select continuous conditions listed by `--help`. The successful manuscript
subset uses:

```text
--conditions quick spaced distributed mixed
continuous_1ps_10p continuous_1ps_20p continuous_3ps_10p continuous_3ps_20p
load_1ps_1000p load_5ps_1000p load_20ps_10p load_20ps_1000p
--repetitions 1 --max-minutes 150
```

The deadline stops new work;
measurement delivery and cleanup finish afterward. The printed `runs/<execution>`
contains `activity_data.csv`, `activity_timeline.csv`, `SUMMARY.md` and supporting
records. It does not replace the five-graph results. Regenerate its tables offline:

```bash
uv run --frozen --extra experiment python -m experiments.activity_reports \
  --results "$RTMSF_RUN/report_inputs.json" --output-dir "${RTMSF_RUN}-regenerated"
```

For the focused single-worker buffer comparison, use the same activity command
above with these additional options (allow up to 150 minutes for setup, eight
conditions, measurement delivery and cleanup):

```text
--conditions load_20ps_10p load_20ps_1000p --buffer-sizes 100 1000
--buffer-bytes 16777216 --queue-visibility-seconds 1000
--repetitions 2 --max-minutes 150
```

Each condition has a fresh worker process. Buffer settings and sampled memory
are included in the activity CSV/report; the five graph outputs do not change.

## Complete 10K execution

The next approved matrix is in [the specification](experiments.md#outputs-at-a-glance).
It uses 170 comparison trials, 48 activity conditions and eight buffer conditions.
The comparison fixtures alone seed 604,480 files. The 170 required two-minute
quiet periods take at least 5 hours 40 minutes; fixture setup, crawler runs,
recovery and collection add time. Allow an overnight run and possible daytime
completion. Time budgets below stop new trials, not all ongoing collection.

Use a clean checkout and the frozen dependencies. Confirm the AWS account,
ready stack, stopped worker, empty dead-letter queue and Docker access before
provisioning trial resources. Build and record the experiment image using section 2;
set `RTMSF_BUILD_MANIFEST` inside the new execution directory before building.
An AWS CLI identity check alone does not verify that Python can reach AWS or that
the session can access Docker. Check both:

```bash
aws sts get-caller-identity --profile "$RTMSF_PROFILE" --region "$RTMSF_REGION" \
  --query Account --output text
uv run --frozen --extra dev python -c \
  'import boto3, os; print(boto3.Session(profile_name=os.environ["RTMSF_PROFILE"], region_name=os.environ["RTMSF_REGION"]).client("sts").get_caller_identity()["Account"])'
docker info --format '{{.ServerVersion}}'
```

Each phase uses its existing runner, fresh fixtures and independent outcome/hash
records. Keep all three under one top-level execution directory. The activity
repetition count is three, independently of the five graph-comparison repeats:

```bash
export RTMSF_EXECUTION=runs/10k_20261003
mkdir "$RTMSF_EXECUTION"
export RTMSF_BUILD_MANIFEST="$RTMSF_EXECUTION/build/image-build.json"
```

After building/validating the image and updating the idle stack, run these commands
sequentially from the same checkout. No phase reads results from another phase:

```bash
uv run --frozen --extra experiment python -m experiments.aws.run \
  --stack "$RTMSF_STACK" --expected-account "$RTMSF_ACCOUNT" \
  --profile "$RTMSF_PROFILE" --region "$RTMSF_REGION" \
  --sizes 100 1000 5000 10000 --layout-sizes 100 1000 5000 10000 \
  --files 1 100 1000 --graph4-partitions 1000 \
  --repetitions 5 --correctness-repetitions 3 \
  --timeout-seconds 900 --max-minutes 1800 \
  --request-source cloudwatch --audit-wait-seconds 600 \
  --build-manifest "$RTMSF_BUILD_MANIFEST" \
  --output-dir "$RTMSF_EXECUTION/graphs"

uv run --frozen --extra experiment python -m experiments.aws.activity \
  --stack "$RTMSF_STACK" --expected-account "$RTMSF_ACCOUNT" \
  --profile "$RTMSF_PROFILE" --region "$RTMSF_REGION" \
  --build-manifest "$RTMSF_BUILD_MANIFEST" \
  --quick --repetitions 3 --burst-repetitions 3 --max-minutes 360 \
  --output-dir "$RTMSF_EXECUTION/activity"

uv run --frozen --extra experiment python -m experiments.aws.activity \
  --stack "$RTMSF_STACK" --expected-account "$RTMSF_ACCOUNT" \
  --profile "$RTMSF_PROFILE" --region "$RTMSF_REGION" \
  --build-manifest "$RTMSF_BUILD_MANIFEST" \
  --conditions load_20ps_10p load_20ps_1000p \
  --buffer-sizes 100 1000 --buffer-bytes 16777216 \
  --queue-visibility-seconds 1000 --repetitions 2 --max-minutes 240 \
  --output-dir "$RTMSF_EXECUTION/buffer"
```

Before starting the next phase, check that the previous command exited zero and
its worker/crawler shutdown and resource cleanup completed. If it failed, preserve
the bundle, resolve the recorded failure and stop before touching shared resources
again. The `--quick` activity flag selects the four representative load points;
it does not shorten their 120-second schedules or omit the four burst and four
continuous schedules. Buffer timing and memory settings stay distinct from the
normal-worker defaults.

Old local runs can be moved to a personal archive outside the checkout before a
new execution. Keep their bytes and failures; do not merge their CSVs into this
run or alter their source/hash records. A fork does not need those old runs.

## Shutdown and retained resources

The pilot attempts worker, crawler, and CloudTrail shutdown in `finally` and
records shutdown failures. Check those results and the actual task ARNs reaching
`STOPPED`. If the runner is forcibly interrupted, set the service desired
count to zero and wait for its tasks to stop before another run. Resolve its
`ClusterName`, `ServiceName`, and `TrailName` from the stack outputs; stop any
remaining trial crawler and trail logging too.

The runner removes its temporary measurement buckets, tables, and crawler queues.
The base stack, shared buckets/queues, supporting-check fixtures, ECR images, and
logs remain. Deleting the stack alone does not delete resources marked `Retain`
in the template. Remove those explicitly when you no longer need the data. The
worker has no idle compute cost after stopping; retained storage and logging can
still incur charges.
