# OCI functional validation evidence

These selected exports come from the original October 12 and October 14, 2026
OCI records. Both used real Object Storage → Events → Function → Queue and the
shared RTMSF core, with an **in-memory catalog test double**. The first worker
ran locally; the later worker ran on one OCI Compute VM. These are functional
checks, separate from AWS performance measurements.

Start with [compute-final.json](observations/compute-final.json), then the
[lifecycle](observations/compute-lifecycle.json),
[burst](observations/compute-bursts.json) and two restart reports
([buffer](observations/compute-restart-buffer.json),
[replay](observations/compute-restart-replay.json)). The [OCI guide](../../docs/oci.md)
describes the deployed architecture, settings and runner prerequisites.

## Outcomes and scope

The Compute run `run-20261015T055634` passed nine lifecycle checks and four
bursts. Its restart check killed the old process after buffering and renewal,
before PMCP/ACK. The replacement received the same event identity, added the
partition in a fresh in-memory catalog and acknowledged it; a subsequent delete
reconciled DROP. `buffer-report`'s `passed=false` records the unfinished buffer
stage, with `buffered_without_ack=true`, rather than a failed overall restart.
The final controller and replay reports record successful completion and the
old process stopping before replacement.

| Compute burst | Notifications | Reconciliations | ACKs |
|---|---:|---:|---:|
| Six creates in one partition | 6 | 1 | 6 |
| Six creates across two partitions | 6 | 2 | 6 |
| Three duplicate notifications | 3 | 1 | 3 |
| Twelve deletes across two partitions | 12 | 2 | 12 |

The [earlier local lifecycle](observations/local-lifecycle.json),
[local bursts](observations/local-bursts.json) and separate Queue process check
([stage 1](observations/queue-restart-stage1.json),
[stage 2](observations/queue-restart-stage2.json)) remain separately identified.
The [cleanup readback](observations/compute-cleanup-readback.json) records
terminated temporary Compute/boot volumes, removed temporary IAM and empty
primary/quarantine queues and DLQs. It is the Compute teardown observation;
the later deletion of the event pipeline remains in the private original archive.

Real catalog writes, query visibility, persistent catalog recovery, sustained
load, long polling, total API savings and billed cost remain unvalidated. ACK
and grouping counts do not establish performance equivalence with AWS. Restart
event identities and core worker records are retained; full native event
envelopes and controller/guest context remain outside this selected bundle.

## Preserved failures

| Artifact | Recorded result and limit |
|---|---|
| [local-first-failed.json](observations/local-first-failed.json) | Initial local proof did not pass and completed no lifecycle checks; this record does not explain the underlying timeout. |
| [compute-provisioning-failed.json](observations/compute-provisioning-failed.json) | Unsupported 0-VPU boot-volume setting; corrected to 10 VPUs/GB. |
| [compute-detachment-failed.json](observations/compute-detachment-failed.json) | Controller `TimeoutExpired` after lifecycle and burst stages; excluded from successful completion. |
| [compute-storage-failed.json](observations/compute-storage-failed.json), [storage-error.json](observations/storage-error.json) | Burst startup exited with `oci_status_404`; cause unresolved and not treated as object absence. |
| [compute-window-assertion-failed.json](observations/compute-window-assertion-failed.json), [window-assertion-error.json](observations/window-assertion-error.json) | Runner incorrectly required one reconciliation per partition; later code permits delivery across multiple windows and checks complete grouping/ACK instead. |

## Source and export provenance

[source-provenance.json](source-provenance.json) identifies the deployed
uncommitted `oci` source by its base commit, source archive SHA256 and all 65
member fingerprints. The archive's digest matches the successful Compute
report and source manifest; the selected [guest digest record](observations/guest-archive-digest.json)
provides the independent on-VM checksum observation.
Its core and OCI implementation files match repository commit
`de9789ea5130a8315163defdccd34dc8820743b1`. The retained original Function source
copies have separate fingerprints; the current `deploy/oci/` packaging was
relocated after validation and has not been redeployed.

[export-provenance.json](export-provenance.json) records the original path/hash
and exported hash for each selected report or terminal exception. Selection
omits resource IDs, native receipts, guest metadata, hostnames, process IDs,
configuration and full envelopes; outcomes, measurements, timestamps and public
event identities retain their original values. Original logs, archive, deployed
Function copies and private metadata remain outside Git. They were located in
the earlier local workspace identified in provenance; `runs/oci-validation/`
is the historical archive convention, not a directory supplied by this clone.

Check the shared file fingerprints from this directory with
`shasum -a 256 -c SHA256SUMS`. This makes no cloud calls and verifies the exported
representation; it does not repeat the cloud checks.
