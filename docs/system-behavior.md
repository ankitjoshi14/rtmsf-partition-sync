# System behavior

What RTMSF guarantees, what it assumes, and how the AWS worker is configured.
The [experiment specification](experiments.md) defines how we test this behavior.

The public flow is `ObjectEvent → PartitionEvent → PartitionIntent`.

- `ObjectEvent` contains a configured logical `source_id`, exact decoded
  `object_key`, `CREATE | DELETE`, UTC `ingested_at`, and optional UTC
  `source_event_time`. Source time is telemetry only. Missing or malformed
  provider timestamps do not prevent normalization.
- `PartitionEvent` pairs that immutable event with its resolved partition.
- `PartitionIntent` contains the partition, candidate `ADD | DROP`, a bounded
  tuple of observed CREATE keys, and an observation count.
- `MutationResult(status, reason)` is an immediate result: `APPLIED`, `NOOP`,
  `RETRY`, or `ERROR`. WOERA activity stays private and in memory.
- `Delivery(handle, events)` belongs to the event-source interface. The handle
  is opaque to the worker. Raw bodies and provider delivery metadata stay in the
  adapter; they do not determine partition ordering or mutation decisions.

## Routing and processing

An operator selects existing table IDs. The catalog adapter resolves their
locations and partition columns once at startup. An immutable routing index
maps source/key to one table and validates the configured layout. Duplicate IDs
and overlapping roots in the same source are rejected. Changing targets requires
configuration and a controlled restart. RTMSF does not create or discover tables.

Object keys and partition values are preserved exactly. All objects below the
configured partition path, including markers and nested files, trigger processing.
The AWS adapter decodes
S3 event keys once. No filesystem normalization, trimming, lowercasing, or second
URL decoding rewrites the resulting key.

`Q` is a processing-time settling interval reset by observed arrivals. `T_max`
bounds initial buffering from the first arrival. Both use the worker's local
monotonic clock. Source timestamps never drive deadlines. Retries have their own
backoff; new arrivals cannot bypass an already scheduled retry. After completion,
a later notification can open another window for the same partition.

WOERA uses ADD dominance within a window; the intent remains a candidate. PMCP
checks actual storage. A DELETE observation can repair an absent catalog entry
when surviving data proves that the partition should exist. Empty-partition
checks consider any object under the partition prefix, including marker files.

Deleting the last Parquet file while `_SUCCESS` remains keeps the registration;
deleting `_SUCCESS` then triggers removal. A marker alone can also cause registration,
because membership means any stored object, not queryable data. This does not inspect
schemas or validate file formats. Events outside a fully matched configured partition
path remain ignored; initial alignment and delivery of relevant events are required.

An ADD whose observed data is absent is retried with a bounded policy. If the
objects were created and then deleted before reconciliation, this can end in a
DLQ record even when the final catalog is already correctly empty. This policy
is observable; it must not be described as error-free handling of all churn.

## Ownership and recovery

The supplied AWS deployment has one active worker. It leaves input unacknowledged
until every derived intent finishes or terminal input has been durably copied to
the DLQ. Lease renewal covers pending work and bounded provider calls; a detected
budget overrun or unsafe lease causes process failure and replay. SDK timeout
settings are not a hard deadline for every OS/network failure.

A crash discards WOERA state. Durable unacknowledged messages replay into fresh
`Q` intervals. An uncertain or completed catalog mutation may be attempted again;
PMCP reconciles current state idempotently. No checkpoint database is used.

Deployment and experiment tools wait for old task ARNs to reach `STOPPED` before
replacement. Service running-count zero alone is insufficient. Full multi-worker
ADD/DROP would require an exclusive keyed event source and quiesce/replay; this
Standard-SQS deployment does not validate that configuration or zombie fencing.

Affected-partition convergence assumes notification admission and retention,
valid routing, and enough worker/provider availability for retries to progress.
It also requires an eligible notification after the relevant final storage-state
change. Initial catalog alignment is assumed or supplied separately; drift in
partitions with no eligible events is not repaired by the worker alone.
Permanent notification omission, queue expiry/purge, invalid routing and permanent
provider failure are outside that guarantee. Storage verification is not atomic
with arbitrary writers. An independent native repair can be an operational
backstop for omitted events.

## AWS configuration

| Variable | Meaning |
|---|---|
| `EVENT_QUEUE_URL`, `DEAD_LETTER_QUEUE_URL` | Durable input and terminal failure queues |
| `RTMSF_SOURCES` | JSON map of logical source IDs to S3 bucket names |
| `RTMSF_TABLE_IDS` | JSON list of configured `database.table` identifiers |
| `SETTLING_MS` | Q; current pilot uses 10000 (10 seconds) |
| `MAXIMUM_MS` | Maximum buffering residence; experiment uses 31000 |
| `QUEUE_VISIBILITY_SECONDS` | Initial/renewed delivery lease; default 180 |
| `MAX_PENDING_DELIVERIES` | Shared unfinished-message cap across all configured tables; default 100 |
| `MAX_BUFFER_BYTES` | Retained raw SQS body budget; default 16777216 (16 MiB), minimum 1 MiB |

The AWS adapter reserves room for maximum-size messages before each receive.
When byte capacity is unavailable it waits; input remains in SQS. This budget
excludes parsed events, table definitions, temporary response/decoding memory
and SDK state, so reserve additional process memory. Existing limits also bound
active partitions (1,000) and decoded events (10,000). Increasing the message
cap changes worst-case queue renewal time; startup rejects an insufficient
visibility timeout. The focused 100/1,000 comparison uses 1,000 seconds for both
settings, leaving Q=10 and T_max=31 unchanged. This longer lease can delay crash
replay; it is not a universal deployment recommendation or a freshness bound.

S3/Glue and queue mutation calls use explicit SDK timeout/retry settings. Runtime
budgets include the supported SQS batch shapes; startup configuration is checked
before polling. Provider integrations implement the four interfaces in `interfaces.py`:
`EventSource`, `ObjectStore`, `PartitionCatalog`, and `TelemetrySink`.

Crash tests use the separate `experiments.aws.crash_worker` launcher and its
`RTMSF_EXPERIMENT_FAULT` configuration. It records one durable marker outside
table data, then exits at the selected boundary: before PMCP processing or after
a successful catalog change, before acknowledgement. The marker prevents an
endless crash loop. Normal `rtmsf.aws_app` startup does not read these fault settings.
