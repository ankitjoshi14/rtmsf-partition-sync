# GCP functional proof and 24-hour lifecycle results

The reviewed native GCP experiment is complete. The final serialized run,
`gcp-soak-24h-7688fd5-20261020T040757Z`, used deployed commit
`7688fd5523b8e1ca1677ae9cc0d9373df516e77c`. The producer ran for
86,400.000308 seconds, from October 20 04:08:12.181975 UTC through October 21
04:08:12.182283 UTC. Independent final verification was recorded at October 21
04:18:41 UTC (October 20 Pacific).

[The GCP guide](../../docs/gcp.md) records the architecture, schema and operating
limits. [report_inputs.json](report_inputs.json) collects the saved outcomes.
[export-provenance.json](export-provenance.json) preserves original source and
exported hashes. [SHA256SUMS](SHA256SUMS) checks the shared bundle.

## Final outcome

| Check | Independently verified result |
|---|---|
| Finite live gates | All 12 passed: native ADD/query, lifecycle DROP/query, four crash boundaries, markers, special names, burst, malformed quarantine, versions and stale/duplicate replay. |
| Producer duration | A fresh finite 24 hours; elapsed time from the earlier failed run was excluded. |
| Serialized lifecycle | 7,477 creates / 7,477 deletes; minimum mutation interval 5.006158 seconds. |
| Event identity and ACK | All 14,954 unique own soak identities settled after matching receipt/handle and successful ADD/DROP reconciliation; zero unsettled identities. |
| Redelivery | 26 expired receipt handles were settled through exact-identity redelivery; the final pulse was separately received and acknowledged. |
| Ordering | ADD ACK before generation-conditioned DELETE; DELETE ACK before next creation. Reviewed guards require fresh storage/catalog truth before creation. |
| Processes and locks | All 244 actual child finishes/exits preceded replacements; supervisor/children absent and both host locks available. Independent parent wrapper exit 0. |
| Native fixtures | Exact 17 current generations/sizes and generation-pinned bytes/SHA256 preserved, plus noncurrent generation `1792451791947494`. |
| Final pulse | Generation `1792555692277915`, 10 bytes, content `soak-pulse`, retained. |
| Final soak truth | Soak object/prefix and catalog partition absent. |
| Independent Spark | Fresh named-table SHOW PARTITIONS and ordered SELECT both completed; exact 11 partitions / 12 rows, marker/special fixtures retained, no soak, exit 0. |
| Infrastructure at final verification | Metastore ACTIVE with the same private endpoint/UID; VM RUNNING with no expiry. |

The captured schema includes three owned external tables with `value STRING`
and partition key `dt STRING`: two empty catalog-probe tables and the native
table with eleven retained partitions. [schema.sql](schema.sql) records the
database/table definitions and exact partition locations from a fresh read-only
pre-shutdown observation.

The final source report remains `completed_with_restarts` and records
244 failed children, 243 restarts, zero forced kills and two read-only retries.
The terminal child returned `external_retryable`, exit 1, with
`expected_stop=true` and zero pending deliveries. Parent exit 0 does not
establish uninterrupted worker operation. Successful historical truth reads
are enforced by reviewed control flow but were not individually logged;
the final storage/catalog truth was independently measured.

## Retained failures and limitations

The earlier `79122ca` attempt failed early after 944 creates/deletes and
48 failed children / 47 restarts. Its drainage, fixtures, catalog absence,
actual exits and fresh 11-partition/12-row Spark proof were independently
verified. Those results do not establish 24-hour completion; its elapsed time
was excluded from the replacement attempt.

The earlier smoke exposed a directory-marker race when deletion overtook ADD.
The corrected serialized 60-second smoke passed, retaining its idle Pull
shutdown failure with zero pending input. Retryable underlying RPC causes and
the internal cause of the Unicode partition-value Thrift timeout remain
unproven. Unicode object filenames passed; the failed partition-value lookup
is excluded from that claim.

This is a single-active-worker serialized lab proof. It does not establish
arbitrary concurrent-writer correctness, exactly-once delivery, uninterrupted
runtime, sustainable throughput or actual billed cost. The original failures
and selected recovery proofs remain in `observations/` alongside successful
outcomes.

## Evidence selection

The selected source records include the gate evidence index and its
source proofs, the final independent correlations/read-only/query checks,
frozen/deployment/changed-seam checks, retry recovery proofs and the earlier
failed attempt. All 29 recorded retry recovery/exit histories are included,
along with pending-failure proofs and the eighth-burst metadata correction.
Exact fresh query SQL, rows, exit status and metadata are
included. JSONL records retain safe event/message identities and synthetic
handles; they contain no native acknowledgment IDs.

Local workspace paths and sharing references are normalized. Measurements,
statuses, timestamps and public event identities retain their recorded values.
`export-provenance.json` binds each selected export to its original hash;
`SHA256SUMS` fingerprints the exported representation.

Original full archives, worker stdout/stderr, the outside-directory wrapper
PID/start/finish/exit files, and the full managed metastore export remain in
the retained private preservation bundle outside Git. A successful metadata
export and file fingerprint check does not establish a tested restore.
