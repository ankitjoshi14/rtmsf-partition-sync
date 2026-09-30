# Experiment specification

Ankit Joshi · existing protocol, review follow-ups updated September 30, 2026

**This is the authoritative design for five graphs, one recovery table, and
eight supporting checks.** The experiment runner collects the data specified
here; the plotting utility turns that data into these outputs. Change this
specification first if an experiment decision changes.

[System behavior](system-behavior.md) defines the guarantees and assumptions.
[The AWS runbook](aws.md) gives commands. [The evidence index](../evidence/README.md)
is the single place for completed-run status; planned sizes below are not results.

## Outputs at a glance

| Output | Question answered | X values | Y | Methods |
|---|---|---|---|---|
| Graph 1 | Does the catalog follow file creation and deletion? | Actual elapsed seconds | Expected and observed presence: absent/present | RTMSF |
| Graph 2 | Does a bigger table require more work for one new partition? | 100, 1K, 10K, 100K starting partitions | S3 HEAD + GET + LIST requests | RTMSF, Glue event crawler, Athena MSCK |
| Graph 3 | Once triggered, how long until the new partition is visible? | Same as Graph 2; same trials | Seconds from each method's trigger | Same as Graph 2 |
| Graph 4 | Does a fuller existing partition require more work? | 1, 10, 100, 1K, 10K existing files | S3 HEAD + GET + LIST requests | RTMSF, Glue event crawler |
| Graph 5 | Does folder order change the work? | 100, 1K, 10K starting partitions | S3 HEAD + GET + LIST, two layout panels | RTMSF, Athena MSCK |
| Table 1 | Does the system recover at each crash boundary? | Four named cases below | Observed state and verified/attempted counts | RTMSF |

Use **three fresh repetitions** for Graph 1, recovery, and supporting checks;
**five** per comparison condition. Graphs 2/3 share 60 trials, Graph 4 uses 50,
and Graph 5 uses 60: **170 measurement trials** in the full paper matrix.
Small validation runs use smaller counts and fewer repetitions, recorded explicitly.

## Shared setup

```text
orders/dt=2026-01-01/site=site-000/hour=00/part-000001.parquet
```

- `dt`, `site`, `hour` are string partition columns. Together they identify
  **one partition**; that folder may contain many files. Fill hours 00–23,
  then sites 000–099, then advance the date. This generates every main-table size.
- Each file is valid Snappy Parquet with one row: `record_id` and `amount`
  are BIGINT; `amount = 10 × record_id`. Corresponding method copies use
  identical file bytes. The familiar date/site/hour layout is easy to inspect;
  Graph 5 checks the effect of a different folder order.
- Every comparison condition/method/repetition gets a **fresh table and bucket**.
  The 1K trial does not grow the tested 100-partition table. Names include the
  method, condition, repetition, and unique execution identifier.
- Register all starting partitions. Complete baseline query checks, the crawler's
  initial scan, and setup-event draining before measurement. Each method gets
  its own change. Rotate method order across repetitions.
- One ARM64 ECS/Fargate worker: 0.25 vCPU, 512 MiB, `us-west-2`.
  Fixed Q=10 seconds, maximum buffering=31 seconds, SQS lease=180 seconds,
  and five retries after the initial attempt. Q is not a latency promise.
- Glue uses an existing catalog-table target in `CRAWL_EVENT_MODE`, preserving
  known schema and inheriting it for partitions. Record exact crawler settings
  and selected/effective Athena engine versions.

## Graph 1 — catalog correctness

Start with an empty RTMSF table. All four actions target the **same partition**.
Wait for notification processing and catalog/query checks after each action.

| Action | Files left / expected query rows | Expected catalog state |
|---|---:|---|
| Upload the first file | 1 | Present |
| Upload four more files | 5 | Present |
| Delete one file | 4 | Present |
| Delete the remaining four files | 0 | Absent |

**Collect:** derive expected presence from completed uploads/deletes. Independently
poll Glue `GetPartition` about once per second: found=1, confirmed absent=0,
API error=unknown. Save every poll, including unchanged states, from before the
first upload through final-stage completion. Verify partition values, location,
schema and exact query rows at each stage; query results do not supply the graph's
presence values.

**Plot:** two step lines against time since the first successful upload response;
Y labels are Absent/Present, CSV values are 0/1. Use actual action/poll times;
a negative time represents the pre-upload baseline. Attach each upload/delete
label once to its expected-presence row. Show unknown observations as gaps.

**Repeat:** three fresh cycles. Plot repetition 1, selected before running even
if it fails; retain all three traces and report their outcomes. Do not average
binary states. Additional file counts or a burst sweep are not part of this graph.

## Graph 2 — work as the table grows

For each N = 100, 1K, 10K, 100K:

1. Prepare N registered partitions with one file each, separately for each method.
2. Upload one file into the **next unused partition**, absent from storage and
   catalog before upload. At N=100, the last existing path ends in
   `site=site-004/hour=03/`; the new path ends in `site=site-004/hour=04/`.
3. Let the running RTMSF worker process its event. For Athena, submit
   `MSCK REPAIR TABLE` promptly after upload. For Glue, start its initialized
   crawler once the new notification is available. Add no scheduling delay.
4. Collect maintenance HEAD/GET/LIST requests using the protocol below. Continue
   counting after first visibility until processing finishes. Verify all N+1
   exact catalog entries and query rows outside the measurement window.
5. Repeat five times per method: 4 sizes × 3 methods × 5 = **60 trials**.

The normal RTMSF new-partition ADD checks its observed file with **one HEAD**,
with no LIST. This is an expectation to verify, not a value to insert into data.

## Graph 3 — time from trigger to visibility

Use Graph 2's trials. Independently poll Glue while the method runs; stop timing
at the first observed presence, not MSCK/crawler completion.

| Method | Time zero | Included time |
|---|---|---|
| RTMSF | Successful upload response | Notification delivery, receipt, settling, catalog processing |
| Athena MSCK | Immediately before `StartQueryExecution` | API latency/retries, queueing, processing until visibility |
| Glue event crawler | Immediately before `StartCrawler`, after notification is available | API latency, startup/queueing, processing until visibility |

Use monotonic-clock durations. Retain wall-clock upload/submission times, their
actual gap, query IDs and all poll times in the experiment record. One-second
polling gives an observed time, not the exact instant the catalog changed.

Label these triggers explicitly: this graph compares **method-specific response
times**, not common upload-to-visibility freshness or a production scheduling
interval. Events choose what Glue inspects; they do not automatically start a
crawl. AWS permits manual/API or scheduled recrawls after initialization
([documentation](https://docs.aws.amazon.com/glue/latest/dg/crawler-s3-event-notifications.html)).

## Graph 4 — work as the affected partition fills

- Start with 10K registered partitions. Give the first partition F = 1, 10, 100,
  1K or 10K files; all others have one. Use fresh copies for RTMSF and Glue.
- Append **one file to that already registered partition** after setup settles.
- Count maintenance HEAD/GET/LIST through processing completion. Then verify
  unchanged correct catalog membership and the additional query row.
- Plot requests against **existing files before the append**. There is no
  appearance timer because this partition is already visible.
- Repeat five times: 5 file counts × 2 methods × 5 = **50 trials**.

RTMSF normally needs one bounded LIST to establish nonemptiness. Glue's affected-
folder inspection may read files; measure its work rather than infer GET or LIST
counts from S3's maximum page size. This tests membership maintenance, not schema
discovery, and does not assert how every crawler configuration behaves.

## Graph 5 — sensitivity to folder order

- Use the same N tenant/day pairs and file bytes in both layouts:
  `day=2026-09-25/tenant=t000000/` and `tenant=t000000/day=2026-09-25/`.
- For N = 100, 1K, 10K, prepare fresh copies for each layout, RTMSF/Athena, and
  repetition. Register N starting partitions with one file each.
- Add one file for the **next unused tenant on the same day**. Verify absence
  before upload and all N+1 exact catalog entries and query rows afterward.
- Collect HEAD/GET/LIST as in Graph 2. Plot two panels with shared axes:
  X=starting partitions, Y=total requests. `layout` distinguishes the CSV rows.
- Rotate method/layout order; 3 sizes × 2 layouts × 2 methods × 5 = **60 trials**.

The main layout stays unchanged. This exposes traversal sensitivity instead of
choosing an expensive layout for the headline comparison. RTMSF's expected ADD
path uses one HEAD in either order. Athena measurements describe its implementation,
not a minimum request count required by upstream Hive, Spark, or every MSCK.

## Table 1 — crash recovery

One fresh trial is **empty → upload one file → present → delete it → empty**,
with one deliberate crash at the selected step. Run each case three times.

| Case / runner identifier | Crash point | Expected immediately after recovery |
|---|---|---|
| `add_before_processing` | After buffering, before PMCP processes ADD | Registered |
| `add_after_catalog_change` | After Glue registration succeeds, before message acknowledgement | Registered |
| `drop_before_processing` | After buffering, before PMCP processes DROP | Absent |
| `drop_after_catalog_change` | After Glue removal succeeds, before message acknowledgement | Absent |

**Automatic injection:** the separate `experiments.aws.crash_worker` launcher
runs the real worker, targeting one partition, operation and boundary. Its hook
saves a durable S3 marker outside table data (`_experiment/crash/<trial-id>.json`)
and calls `os._exit(86)`. ECS replaces the process; the marker prevents another
injected crash. SQS redelivers the unacknowledged message. The external runner
and observer survive. A timed external kill cannot reliably select these boundaries.
Normal `rtmsf.aws_app` startup does not read fault settings.

**Collect:** after recovery and before the next cycle step, independently observe
the catalog. Retain marker, task IDs/exit status, non-overlap evidence, same-message
redelivery/acknowledgement, and catalog/query responses. Complete the whole cycle,
checking one query row after ADD and zero after DROP. Cleanup cannot turn failure
into success.

**Generate:** each table row contains expected state, observed-state counts
(registered/absent/unknown), crash verified/attempted, replay+ACK verified/attempted,
and passed/attempted. Show failed, incomplete and unattempted counts alongside it.
A pass requires all of the evidence above. Missing evidence is incomplete.
The runner records individual outcomes in `report_inputs.json`; no hand-filled
table, cumulative mutation graph, or second graph CSV is needed.

## Eight supporting checks

Run three fresh repetitions per check. Generate `checks_table.md` from individual
recorded outcomes; include failed, incomplete and unattempted conditions.

| Check | Workload and verification |
|---|---|
| `short_burst` | Upload 20 distinct files to one partition back-to-back; verify 20 rows and record observed events, PMCP attempts and applied catalog changes. |
| `long_burst` | Upload 9 files at 0, 5, …, 40 seconds; record actual arrivals, progress at the buffering cap, and 9 final rows. |
| `two_tables` | Alternate 30 uploads: 5 files × 3 partitions × 2 tables (`orders`, `returns`, same schema); verify routing and rows. |
| `duplicate_old_events` | Replay a captured notification three times after settling; also replay old CREATE after deletion and old DELETE after recreation; check final state. |
| `marker_lifecycle` | Create `_SUCCESS` alone, add then delete a Parquet file, delete the marker last; verify registration followed by removal and retain each notification/ACK. |
| `rapid_changes` | Stop the worker; create/delete, then separately delete/recreate; restart and check final state and any retries/dead letters. |
| `retryable_failures` | Inject two retryable catalog-call failures, then permit real calls and verify success. |
| `retry_exhaustion` | Fail the initial attempt plus five retries; verify durable dead-letter storage before ACK, then successful replay with healthy calls. |

A stale CREATE whose data disappeared may exhaust retries and enter the DLQ while
the catalog is correctly empty. Preserve this [system boundary](system-behavior.md);
these checks do not establish error-free handling of every event ordering.

## How request counts are collected — Graphs 2, 4 and 5

1. Use a separate bucket per measured trial. Enable CloudWatch request metrics
   15 minutes before measurement; then issue known calibration calls. Complete
   setup reads/crawls and leave **two complete quiet minutes** before the change.
2. Record processing boundaries. Exclude seeding, uploader PUT/DELETE operations,
   calibration, observer/query checks, and log/results buckets. No unrelated client
   may read a measured bucket: CloudWatch cannot filter by requester.
3. Keep buckets idle during the final **10-minute delivery wait**. Freeze
   `quiet_until` before final verification queries or cleanup. Read `HeadRequests`,
   `GetRequests`, `ListRequests`: Sum, 60-second periods, BucketName + FilterId=trial.
4. Corroborate with CloudTrail S3 data events: HEAD=`HeadObject`/`HeadBucket`,
   GET=`GetObject`, LIST=`ListObjects`/`ListObjectsV2`. Match bucket, processing
   window and requester; deduplicate event IDs, retain request IDs and retries.
   Match calibration calls, including paginated LIST, by returned request IDs.
5. Retain raw responses, bounds, cutoff and coverage. Missing is **unknown, not
   zero**. An absent CloudWatch counter can be inferred as observed zero only
   when known operation counts account for `AllRequests`, activity exists, and
   metric reads succeeded. Record that inference separately.

[CloudWatch](https://docs.aws.amazon.com/AmazonS3/latest/userguide/cloudwatch-monitoring.html)
and [CloudTrail delivery](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/how-cloudtrail-works.html)
are best-effort; calibration and quiet periods do not prove exact completeness.
Reconcile late records before publication. These are observed selected S3 request
counts, not billing or total cloud cost. Retain Glue/SQS calls separately.

## Data files and plotting rules

Each execution writes `runs/<execution>/`: graph data, five PNG/PDF figures,
recovery/check tables, `SUMMARY.md`, `report_inputs.json`, configuration, logs,
source/image identity, dependency versions, inputs and artifact hashes.
The [runbook](aws.md) explains execution and offline regeneration.

`graph_data.csv` has exactly these columns, in order:

```text
graph,method,layout,measurement,x_axis_label,x_value,x_unit,y_axis_label,y_value,y_unit,repetition,action_label
```

- `graph` and `method` select the output/series. `layout` is blank for Graphs 1–4,
  `day_first`/`tenant_first` for Graph 5. `measurement` is expected/observed partition
  presence for Graph 1, `visibility_delay_s` for Graph 3, and separate
  `s3_head_requests`, `s3_get_requests`, `s3_list_requests` rows for request graphs.
- Axis labels explain values and units. `repetition` identifies a fresh repeat.
  `action_label` appears only on Graph 1 expected-presence action rows.
  Status, run IDs, resource names, timestamps and failures belong in supporting
  records, not this plotting CSV. CSV seconds use three decimals; logs keep precision.
- Sort by graph → method → layout → measurement → numeric X → repetition.
  Method order: RTMSF → Glue event crawler → Athena MSCK; request order: HEAD → GET
  → LIST. Filter those columns to inspect a series. Legacy 11-column input remains readable.
- Sum HEAD+GET+LIST **within each matching condition and repetition**, then plot
  medians with min–max whiskers. Missing any component makes that total unknown;
  keep blank values/gaps and report actual available/planned samples. Do not add
  duplicate total rows or silently replace failures with successful reruns.
- One solid color per method; no separate graph per request family or bucket.
  Graphs 2/4/5 use logarithmic X/Y; Graph 3 uses logarithmic X and linear seconds
  on Y. Preserve genuine zeros in the CSV/statistics and label them at the log-axis
  boundary. Graph 1 uses steps, repetition 1 only; retain all traces.
- PNG is for viewing; PDF is for the manuscript. Put repetition counts and
  limitations in captions/summary. Full settings and failures stay in the bundle.

Regenerate the five graphs from the [verified AWS sample](../evidence/README.md)
without making AWS calls. Its 100-partition measurements demonstrate the output
format; they do not complete the paper matrix above.

```sh
uv run --frozen --extra experiment python -m experiments.figures \
  --csv evidence/validation_20260928/graph_data.csv \
  --results evidence/validation_20260928/report_inputs.json \
  --output-dir runs/sample-graphs
```

## Settled scope and reproduction

- Preserve the five existing graphs and recovery table. The review follow-ups
  below propose a focused immediate-handler comparison and active-load evidence;
  they supersede the earlier decision to rely on burst checks alone.
- Keep Q=10 seconds and T_max=31 seconds. No settling-window or scheduler-interval
  sweep. Report actual delays; neither setting is an end-to-end latency guarantee
  or a claim of universally acceptable freshness.
- AWS supplies the measured implementation. Other cloud adapters/testing remain
  future work; core portability is a design claim, not multi-cloud evidence.
- Follow this specification and configure the desired sizes and repetitions for
  each execution. Its reports use its own CSV and recorded outcomes. Retain
  exact settings, source and failures alongside the results.
- Independent reruns should support similar conclusions, not identical seconds.
  Investigate disagreements. Keep setup work out of maintenance measurements,
  retain contrary results, and distinguish pilot validation from scale evidence.

## Review follow-ups — remaining work

The manuscript's prior-work omissions, crawler ADD/append scope, inferred-zero
explanation, and checksum/verification wording are corrected. Existing evidence
stays intact. The bounded activity pilot is implemented and has run; the
[evidence index](../evidence/README.md) records its outcomes and remaining limits.

| To-do | Purpose and completion evidence |
|---|---|
| Save full catalog checks | **Implemented and validated.** `Pilot.verify()` saves expected and observed values, locations and columns before assertions, including failed checks. No extra AWS requests. Historical missing snapshots remain missing. |
| Settle marker handling | **Implemented and AWS-validated in the lean run.** Routing covers all objects below a fully matched partition path, matching PMCP storage checks. Local regression and the AWS marker lifecycle check cover marker-only creation and marker-last deletion. |
| Compare with an immediate handler | **Bounded pilot complete; repetitions pending.** The same worker/PMCP processes separate per-event intents, preserving verification, ownership, retries and ACK rules. Quick, spaced, distributed and mixed bursts have recorded calls, delay and final correctness. This is a controlled comparison, not a new independent production implementation. |
| Exercise continuous updates | **Bounded pilot complete; stronger repetition/capacity evidence pending.** At stated rates, retain actual arrivals, processing during writes, approximate queue counts, visibility, retries/DLQ outcomes, observed drain and final correctness. The pilot demonstrates progress at its tested loads, not maximum sustainable throughput. |
| Repeat representative conditions | Apply the existing three correctness/fault repetitions and five comparison repetitions to a bounded representative matrix first. Retain failures and variation; these counts do not establish failure rates or tail latency. Larger historical tables are separate scaling evidence. |
| Finish the paper artifact | Regenerate reports from retained inputs, make offline reproduction self-contained, prepare source/evidence for public release, and revise the manuscript to match the new results. If the simple handler performs similarly, narrow the contribution claim. |

The bounded validation protocol below supplies the first workload schedules and
outputs. Final paper repetitions remain separate. Keep the comparator and load
generation in experiments; retain the five existing numbered graphs.

The window remains fixed at 10 seconds. We will measure behavior under different
traffic, without claiming that 10 seconds is optimal. This leaves the reviewer's
literal request for window sensitivity unperformed. Multi-cloud testing and new
Snowflake/projection performance baselines are outside this follow-up scope.

### Bounded activity validation

Historical pilot at checkpoint `f1af80f`: this run kept `src/rtmsf` unchanged and
retained the then-documented marker restriction. Catalog snapshots were added to the verifier. Add the per-event
launcher and activity runner under `src/experiments/aws`; extend only experiment
image/measurement plumbing. Check batching, retry/ACK ownership, snapshot retention,
and workload schedules locally before running AWS.

Each condition gets a fresh bucket/table with 100 registered one-file partitions,
the shared date/site/hour layout and identical payloads. Use one repetition for
this pilot. RTMSF uses Q=10/T_max=31; the immediate control uses the same worker,
PMCP and recovery rules, with a separate pending intent for every received event.
It cannot combine events from one SQS batch. Both run in the same Fargate image
and resource size. Rotate method order when repetitions are increased.

| Condition | Actions | Methods |
|---|---|---|
| Quick burst | 20 uploads into one new partition, back-to-back | RTMSF, immediate |
| Spaced burst | 20 uploads into one new partition at 0, 2, ..., 38 seconds | RTMSF, immediate |
| Distributed burst | One upload into each of 20 new partitions | RTMSF, immediate |
| Mixed burst | Alternate 10 uploads into one new partition with deletion of the sole file from each of 10 existing partitions | RTMSF, immediate |
| Continuous mixed traffic | 120 seconds at 1 or 3 operations/second, with 10 or 20 new upload-target partitions. Every fourth operation deletes a distinct original partition's sole file; other operations append across the upload targets. | RTMSF |

Record intended and actual operation times; late uploads must not masquerade as
the requested rate. Independently poll the first new partition and SQS visible/
in-flight counts about once per second. This visibility probe represents one
partition, not all-partition tail latency. Worker telemetry records progress
across all changed partitions. SQS counts are approximate and omit notifications
not delivered yet; they are not exact outstanding-work counts.

Prepare and verify all baselines before measurement. Reuse isolated CloudWatch
buckets and CloudTrail calibration, wait 15 minutes for activation, then leave two
complete quiet minutes after the last setup read. Once each workload ends, match
every changed file to received notifications and ACKs, then observe the queue
draining. Keep buckets idle until the final 10-minute delivery wait and frozen
metric cutoff. Save final full catalog checks and query rows afterward.

Fixture uploads may retry transient transport/service failures up to three times,
before measurement. If an ambiguous PUT already created the object, recovery must
read and match its exact bytes; retain the verification request ID and leave the
unknown original PUT request ID blank. Graph workloads retain one write attempt.
Activity producers allow three attempts for transient write failures, retaining
each retry and including its delay in the achieved operation rate. An uncertain
PUT repeats identical bytes at the same trial-owned key; DELETE repeats the same
owned key. No verification read is added during measurement. Duplicate S3 events
from accepted retries remain in worker evidence and request counts. The CSV keeps
`producer_retry_attempts` separate from worker `retry_attempts`; three failed
attempts still fail the condition. Earlier single-attempt failures remain recorded.
If the first upload retries, its original completion time is unknown: leave
first-partition visibility blank, retain timestamps/samples and explain the reason
in the report. Also leave it blank if visibility precedes upload confirmation;
do not clamp a negative interval to zero.

`activity_data.csv` contains condition, method, repetition, completed operations,
actual write duration, PMCP attempts, separate HEAD/GET/LIST counts, catalog API
attempts, first-new-partition visibility delay, processing attempts, completed event
observations and distinct processed partitions while writes continued, approximate
peak visible/in-flight queue counts, and observed drain
seconds. `activity_timeline.csv` retains the independent time samples.
`report_inputs.json` retains outcomes/errors and detailed measurements; `SUMMARY.md`
is the readable table. Missing values stay blank. Reconcile S3 counters with raw
CloudTrail and worker API records; failures and incomplete conditions stay visible.
Stop the worker, restore its previous task definition, and clean only owned trial
buckets/tables and temporary policies after retaining evidence.

### Next validation: marker coverage and active load

The approved follow-up fixes routing before collecting new evidence. Every
CREATE/DELETE below a fully matched configured partition path, including markers
and nested objects, must trigger reconciliation: PMCP already counts these objects.
Markers continue to count as nonempty storage; this does not validate file formats.
WOERA, PMCP, acknowledgement and retry policy remain unchanged. Add a regression
through the real router/worker and an AWS marker-last deletion check.

Keep the existing burst comparison and continuous mixed schedules. Add a separate
load family with 1,000 registered, one-file partitions and 10, 100 or 1,000 upload
targets. Run for 120 seconds at 1, 5 or 20 operations/second. Three of every four
operations append a unique file; the fourth deletes the oldest such appended file.
The original files remain, so this family isolates processing/backlog under mixed
activity; the existing mixed and lifecycle tests cover membership transitions.
Record actual achieved rates, scheduling delay, progress, queue counts, observed
unacknowledged work/age, terminal outcomes and drain. Correct final state alone is
insufficient. If 20/s does not overload the worker, report a tested lower bound on
capacity; do not claim that its maximum was found.
The AWS worker admits at most 100 pending notifications (`aws_app.py`); retain
this implementation limit when reporting backlog and throughput. Queue growth
at an offered rate does not establish sustainable capacity at that rate.

Quick validation: all five graphs at N=100/1,000 where N is the axis, file counts
1/100/1,000 for Graph 4, one correctness/recovery/comparison repetition, and one
activity repetition. The quick load subset is (1/s,1,000), (5/s,1,000),
(20/s,1,000), (20/s,10). Full evaluation extends N to 5,000/10,000 with five
comparison and three correctness/load repetitions. A 100K scaling extension is
separate and follows successful validation. Keep Q=10 and T_max=31 throughout.

Implementation order: regression then routing fix; marker check; load schedule and
report validation; one-command graph/activity execution; local tests and immutable
image verification; AWS quick run; audit outputs before longer runs. Preserve
failed attempts. Export source, instructions and selected evidence in one archive
for offline report regeneration; publishing is a separate action.
