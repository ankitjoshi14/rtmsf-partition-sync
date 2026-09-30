# Problem and related work

Ankit Joshi · problem-focused review; evidence updated September 30, 2026.

## The problem we are solving

Independent jobs write and delete files directly in object storage without
updating an existing Hive-style catalog. The table definition and partition
layout are already known. **Keep that catalog's partition membership aligned
with storage, without requiring those writers to adopt a shared table commit
protocol.**

| Storage change | Desired catalog state after reconciliation |
|---|---|
| First file appears in a new partition | Register the partition. |
| More files arrive in an existing partition | Keep it registered. |
| Some files are deleted, but the partition remains nonempty | Keep it registered. |
| The partition becomes empty | Remove its registration. |

**Why keep catalog maintenance independent of writers?** Ingestion, backfill,
transfer, and cleanup jobs can all change the same storage. Requiring every job
to maintain the catalog spreads membership logic, permissions, and failure
handling across those jobs. Changing writers is possible; covering every write
and deletion path creates an integration burden. This is the target deployment
rationale, not a claim of observed customer adoption or a distinction from Glue.

The engineering goals are practical freshness, low maintenance work as the
table grows, and recovery from transient failures. For event-based designs,
repeated or delayed notifications and worker crashes must be addressed.
Cloud portability is a separate design goal; only AWS is evaluated here.
Schema discovery, query indexing, and table transactions are outside this task.

The deployment must need explicit membership in an existing catalog, potentially
shared by multiple consumers. For suitable Athena-only queries, [partition
projection][projection] can avoid registration maintenance instead. Empty
registrations normally return no rows; removing them serves accurate enumeration
and metadata maintenance, rather than automatically correcting wrong query rows.

This table states the desired outcome, not an unconditional guarantee. RTMSF's
[contract] defines notification/availability assumptions, non-atomic storage
checks, single-worker scope, and terminal failures. Its empty-prefix check
counts any remaining object, including markers. Marker-only changes can be ignored
by routing and leave stale membership; the contract records that coverage gap.

## Measured benefit for this task

Graph 4 appended one file to an already registered partition. Both methods
passed the catalog and query checks: the partition correctly remained registered.
The completed September 30 local run (`run_20260930_004034_214f`, listed in
the [evidence index][evidence]) records these observed request counts in both
comparison repetitions:

| Existing files before the append | RTMSF GET requests | Glue event crawler GET requests |
|---|---:|---:|
| 1 | 0 | 2 |
| 100 | 0 | 101 |

RTMSF used one LIST and no HEAD requests at every size. The crawler also made
HEAD and LIST requests; its GET column above is not its total request count.

**Supported claim:** For this partition-membership task, RTMSF verified that the
affected partition remained nonempty without per-file GET inspection. Its
storage-check work stayed constant across the tested file counts, while crawler
GET requests increased with the affected partition's file count.

There are two completed trials per method/file count, with 100 starting partitions;
broader repeatability needs stronger evidence. These are observed storage requests, not exact billing or total-system
cost measurements. The result does not establish universal latency superiority
or constant work across retries and other workloads. The event crawler used 6
and 113 total HEAD/GET/LIST requests at the two file counts. In the new-partition
response-time comparison, Athena was faster than RTMSF at every tested size and
repetition; reduced storage work is not a universal latency advantage.

The measured crawler uses `UpdateBehavior=LOG`, `DeleteBehavior=LOG`, and inherited
partition schema. It is an ADD/append comparison, not a comparison between two
complete ADD/DROP implementations. Only RTMSF has lifecycle/removal evidence here.

## Why reduced inspection can preserve correct membership

- **Check storage, not just the event.** One surviving object establishes that a
  partition is nonempty; reading every file is unnecessary for that decision.
  A DELETE notification alone does not justify removing the partition. PMCP
  checks the current prefix before removal and keeps the catalog entry when
  that check finds a surviving object.
- **Coalescing reduces repeated work.** WOERA combines observations for a
  partition into a candidate intent. PMCP still checks storage when processing
  it; the combined event history does not replace that check.
- **Keep unfinished work replayable.** Deliveries remain unacknowledged until
  all derived work finishes or terminal input is durably recorded in the DLQ.
  A crash loses the in-memory buffer; retained notifications rebuild work in a
  fresh worker. Repeated catalog operations are handled idempotently.

**Supporting evidence:** The same completed execution passed its lifecycle trial
and all seven supporting checks, including duplicate/old-event and rapid-change
cases. Its short burst produced 20 observed events, one PMCP attempt,
and one catalog change. All four crash trials passed: ADD
and DROP, each before processing or after catalog mutation but before
acknowledgement, with one trial per case. The lifecycle and each supporting check
also ran once. Historical incomplete executions remain separately identified
in the [evidence index][evidence]; they are not substituted into these results.

This is a design argument supported by tested cases, not a proof for every
event ordering. The [contract] assumes retained notifications, provider
availability, and one active worker. Storage checks are not atomic with
external writers; replay is not exactly-once execution. A CREATE whose data
disappeared can exhaust retries and enter the DLQ even with a correctly empty
catalog.

## How prior work should be selected

Compare the **problem and deployment requirements first**. Scheduled scans and
cloud-specific services remain relevant even when they use a different method.
Event coalescing, bounded storage checks, and avoiding a checkpoint store are
RTMSF design choices, not filters for excluding other solutions.

| Work | Role in this paper | Relevant comparison |
|---|---|---|
| **[Glue event-based crawler][glue]** | Closest measured functional alternative | Updates an existing AWS catalog from changes to ordinary stored files. Its affected-folder inspection supports broader discovery; RTMSF specializes in partition membership. |
| **[Athena MSCK][athena]** | Measured repair baseline | Discovers and registers missing Hive-style partitions by scanning storage. It does not remove stale partitions. Compare its ADD behavior, work, and response time. |
| **[Snowflake external-table refresh][snowflake]** | Related ordinary-file notification system | SQS-triggered refresh adds, updates, and removes external-file metadata; path expressions can derive partitions. It maintains Snowflake's catalog rather than an existing shared Hive catalog. No bounded-check or performance equivalence is inferred. |
| **[Athena partition projection][projection]** | Deployment alternative that avoids registration | Derives partition values at query time and ignores registered partition metadata. Useful for suitable Athena queries, but does not maintain exact shared-catalog membership for other consumers. |
| **[Delta Lake][delta] and [Hudi][hudi]** | Architectural alternatives | Maintain metadata through their table commit protocols. Explain what adopting that architecture requires and the transactional guarantees it offers. Their metadata/index benchmarks do not measure RTMSF's external-catalog task. |
| **[TreeCat][treecat]** | Complementary catalog infrastructure | Improves catalog storage, queries, and transactions. RTMSF maintains entries in an existing catalog. TreeCat §7 explicitly leaves consistency between physical data and metadata as an integration challenge. |

**[MillWheel][millwheel] is a recovery-mechanism citation**, not a catalog
competitor. **[Big Metadata][bigmetadata] is peripheral background** and need
not appear in the focused comparison. Neither belongs beside Glue as an equal
functional alternative merely because it manages state or metadata.

A simple current-prefix reconcile handler with retained deliveries and retries
is also a relevant engineering comparator. Existing AWS-service comparisons do
not isolate WOERA's contribution over that design. A focused same-contract
comparison and active-load evidence would address those research gaps. Neither
is included in the finalized five-graph protocol or completed measurements.

## Short manuscript positioning

RTMSF addresses partition-membership maintenance for existing Hive-style tables
whose writers modify object storage without participating in a shared table
commit protocol. Glue event-based crawling and Athena MSCK are the measured
AWS alternatives, with their different scope and ADD/DROP behavior stated
explicitly. Delta Lake and Hudi represent a different architectural choice:
metadata maintained through format-managed commits. TreeCat addresses catalog
internals, while RTMSF addresses reconciliation with external storage.
Snowflake supplies precedent for notification-driven external-table refresh;
Athena projection can avoid explicit registration for suitable query workloads.
RTMSF's deployment distinction is maintaining membership in an existing catalog
without requiring a writer-format change. That distinction alone does not prove
the combined mechanism is a substantial new systems contribution.

This distinguishes the deployment problem and the systems' responsibilities.
It does not establish originality by excluding different architectures or by
finding no identical implementation. The [AWS evidence][evidence] and [contract]
state what RTMSF currently demonstrates.

<details>
<summary>Verified empirical sources and research scope</summary>

- **TreeCat, PVLDB 2025:** §6, Figs. 7–12; concurrency, metadata retrieval, and
  inserts, using CloudLab/HDFS. Its measured writes invoke its catalog protocol.
- **Delta Lake, PVLDB 2020:** §6, Figs. 4–7; partition-count query performance,
  data skipping, TPC-DS, and writes.
- **Hudi metadata-as-data, FORMATS 2026 workshop:** §5, Tables 2–3; secondary-index
  queries on S3 and record-index lookups on GCS. The supplied five-page PDF was
  read. Tables lack repetition/variability details; some other headline numbers
  cite earlier reports. It is a workshop paper, not a PVLDB article.
- **MillWheel, PVLDB 2013:** §8, Figs. 13–16; processing performance. Its full
  guarantees use durable state/checkpoints and fencing; external-effect
  idempotence belongs to applications (§4.1, §6).
- **Big Metadata, PVLDB 2021:** §6, Figs. 4–8; CMETA query/resource benefits,
  not isolated external-catalog synchronization measurements.

Paper results are author-reported, not independently reproduced here. Glue and
Athena are products measured in our trials, not additional research papers.
Do not place published benchmark numbers in our plots as matched measurements.
The search included primary PDFs, AWS documentation, and title/abstract screening
of [PVLDB volumes 13–19][volumes]. It is a bounded review, not an exhaustive
survey or proof that no earlier system addresses this problem.

</details>

[glue]: https://docs.aws.amazon.com/glue/latest/dg/crawler-s3-event-notifications.html
[athena]: https://docs.aws.amazon.com/athena/latest/ug/msck-repair-table.html
[snowflake]: https://docs.snowflake.com/en/user-guide/tables-external-s3
[projection]: https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html
[delta]: https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf
[hudi]: https://doi.org/10.1145/3802514.3809177
[treecat]: https://www.vldb.org/pvldb/vol18/p4323-oh.pdf
[millwheel]: https://www.vldb.org/pvldb/vol6/p1033-akidau.pdf
[bigmetadata]: https://www.vldb.org/pvldb/vol14/p3083-edara.pdf
[evidence]: ../evidence/README.md
[contract]: system-behavior.md
[volumes]: https://www.vldb.org/pvldb/volumes/
