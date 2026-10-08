# System behavior

RTMSF guarantees, assumptions, and provider interfaces.
The [experiment specification](experiments.md) defines tests for this behavior.
The [architecture diagram](figures/architecture.svg) shows event processing, storage checks, and catalog access.

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

The supplied router requires consecutive `column=value` segments below each table root, in catalog column order.
For example, columns `day, hour` map to `orders/day=2026-10-05/hour=09/`.
Existing partition locations and storage descriptors must already match the configured layout and table definition.
New entries inherit the table storage descriptor. RTMSF does not validate or repair existing partition metadata.
Custom partition locations outside this routing contract are unsupported.

Object keys and partition values are preserved exactly. All objects below the
configured partition path, including markers and nested files, trigger processing.
The AWS adapter decodes
S3 event keys once. No filesystem normalization, trimming, lowercasing, or second
URL decoding rewrites the resulting key. The supplied notification configuration
and decoder cover `ObjectCreated:*` and `ObjectRemoved:*`. Tested cleanup uses
application-issued deletes. Native S3 lifecycle expiration uses
[`LifecycleExpiration:*`](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html)
and is not covered; it requires additional event/adapter support or operational repair.

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

The operator assigns membership reconciliation to RTMSF and retains table/schema administration, initial alignment, batch readiness, and exceptional repair.
The operator or supervisor also manages worker replacement. The worker does not coordinate concurrent catalog-maintenance processes.
Coordinate external repair with writers and stop the worker before changing table definitions or repairing catalog metadata.

The supplied AWS deployment has one active worker. It leaves input unacknowledged
until every derived intent finishes or terminal input has been durably copied to
the DLQ. Lease renewal covers pending work and bounded provider calls; a detected
budget overrun or unsafe lease causes process failure and replay. SDK timeout
settings are not a hard deadline for every OS/network failure.

A crash discards WOERA state. Durable unacknowledged messages replay into fresh
`Q` intervals. An uncertain or completed catalog mutation may be attempted again;
PMCP reconciles current state idempotently. No checkpoint database is used.

The supplied AWS experiment deployment tools wait for old task ARNs to reach `STOPPED` before
replacement. Service running-count zero alone is insufficient. Full multi-worker
ADD/DROP would require an exclusive keyed event source and quiesce/replay; this
Standard-SQS deployment does not validate that configuration or zombie fencing.
Other launchers require their own single-worker replacement procedure; see the [normal AWS restart steps](aws.md#restart-and-exceptional-repair).

Affected-partition convergence assumes notification admission and retention,
valid routing, and enough worker/provider availability for retries to progress.
It also requires an eligible notification after the relevant final storage-state
change. Initial catalog alignment is assumed or supplied separately; drift in
partitions with no eligible events is not repaired by the worker alone.
Storage, routing, schema and other catalog writers must stabilize. A reconciliation
attempt must observe that stabilized storage/catalog state and complete its required
operations before retention or retry limits end automatic processing. Eventual
read consistency alone is insufficient: a stale nonempty LIST can finish DROP
with NOOP and ACK before reads converge. Such an adapter needs a later eligible
notification or an operational recheck; the core does not recheck after NOOP.
The evaluated S3 integration provides
[strong read-after-write consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)
for object PUT/DELETE operations. New events joining a retry window share its remaining
retry budget. The worker allows an initial attempt plus five retries by default;
the AWS experiment queue separately redrives after 20 receives. Normal deployments configure their own queue redrive policy.
Permanent notification omission, queue expiry/purge, invalid routing and permanent
provider failure are outside that guarantee. Storage verification is not atomic
with arbitrary writers. An independent native repair can be an operational
backstop for omitted events.

### Terminal input

Correlate retained input with worker telemetry and inspect stabilized storage and catalog membership for every affected partition.
The AWS failure queue preserves raw notification bodies; telemetry records the terminal reason separately.
One delivery can contain several partitions, so checking one partition is insufficient.

CREATE followed by DELETE can exhaust an ADD retry budget even when final absent membership is correct.
If every affected partition is correct and completed file activity explains the outcome, close the item under your operational policy.
Blind replay of the same empty ADD can repeat terminal handling.
Otherwise, resolve the failure and replay eligible retained input or repair membership.
Coordinate manual repair with writers and worker shutdown. RTMSF supplies no automatic failure classifier, DLQ reconciler, or general repair scanner.

## Provider integrations

Providers implement `EventSource`, `ObjectStore`, `PartitionCatalog`, and `TelemetrySink` in `src/rtmsf/interfaces.py`.
The core uses these interfaces for event delivery, storage checks, catalog changes, and telemetry.
[AWS configuration](aws.md#aws-worker-settings-and-bounds) describes the supplied worker's settings and adapter limits.
Other cloud implementations need their own setup guide and validation.
