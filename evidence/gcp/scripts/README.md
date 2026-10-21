# Preserved GCP experiment helpers

These are exact copies of the helpers used for the recorded lab proof. They
retain proof-specific resource names and fixed runtime paths. Fingerprints in
`../export-provenance.json` bind each copy to its source; `../SHA256SUMS` covers
the shared files. They contain no credentials or native acknowledgment IDs.

| File | Scope and prerequisites |
|---|---|
| [gcp-query-witness.sh](gcp-query-witness.sh) | Live read-only witness on the private Linux VM. Requires the installed Spark 3.5.8, Java 11, GCS connector 3.1.13 and bounded systemd query scope. Accepts a private Thrift endpoint, warehouse, database, table and fresh output directory; runs exactly SHOW PARTITIONS and ordered SELECT in a new JVM. |
| [gcp-query-witness-test.sh](gcp-query-witness-test.sh) | Offline validation and fake-executable invocation check. Runs without Spark or cloud access. |
| [gcp-soak-evidence-check.py](gcp-soak-evidence-check.py) | Offline check of a full copied run directory containing `soak-report.json`, parent JSONL and all child reports/JSONL. Checks own identities, receive/reconciliation/ACK correlations, mutation ordering and five-second intervals. Selected observations alone are insufficient. |
| [gcp-soak-independent-check.py](gcp-soak-independent-check.py) | Live post-run read-only storage/catalog check on the original Linux VM. Requires GCP dependencies, VM identity, private Thrift access, saved baseline JSON and final report; checks process absence, host locks, pinned baseline bytes, retained version, pulse and final catalog absence. |

Run only the offline helper self-check from the repository root:

```sh
bash evidence/gcp/scripts/gcp-query-witness-test.sh
```

The full raw run and wrapper archive remains in the private preservation bundle.
The offline checker does not replace independent wrapper exits, resource health,
the exact 24-hour producer duration or fresh SQL proof. Historical successful
storage/catalog truth reads were enforced by reviewed code but not individually
logged.

Finite worker composition, fault injection and serialized producer/supervisor
code are versioned in [src/experiments/gcp/](../../../src/experiments/gcp/).
Logical reconstruction DDL is [schema.sql](../schema.sql). Live helpers require
the original approved resources and appropriate grants; do not rerun fixtures
or create replacement resources from this evidence directory.

## Inputs and approval prerequisites

The recorded producer used [fixture a](../fixtures/dt=a/part-00000.parquet):
438 bytes, SHA256 `aee59617040d80855ccce0df4354921299da7b1c4c8b090c703870cbb0b19eb3`,
one `value STRING` row `probe-a`. [Fixture b](../fixtures/dt=b/part-00000.parquet)
has the same schema and one `probe-b` row; SHA256
`e824f52db65a5f8e1cba3e36dc0943021c620dfeebff240d4b4f649c32ebb30f`.
These are exact original bytes, not regenerated Parquet with new writer metadata.
`experiments.gcp.soak.load_fixture` validates a's size and hash before cloud access.

| Input | Required content and source |
|---|---|
| `--resources` | Private deployment record accepted by `experiments.gcp.run.load_resources`: approved project/account, private Thrift endpoint, passed catalog gate, `native_run` and `native_pipeline_setup` matching the pinned constants. It is intentionally not a portable deployment config. |
| `--gates` | `run_id` and all twelve passed gates with evidence references; the recorded index is [gcp-live-gates-r20261019n1-20261019T2350.json](../observations/gcp-live-gates-r20261019n1-20261019T2350.json). Evidence references document prior checks; they do not rerun them. |
| `--baseline` | A fresh pre-run object inventory containing exactly `name`, `generation`, `size`, `sha256` for each protected object, including the pinned owner and b sentinel. It must exclude `dt=soak/` and `_SOAK_PULSE`. A final post-run inventory is not an interchangeable baseline. |
| `--fixture-a` | The preserved a file above. |
| `--output-dir` | Fresh private run directory; preserve parent/child records plus the independent wrapper PID/start/finish/exit records outside that directory. |

The finite worker requires the Linux host locks, reviewed dependencies and
private storage/Pub/Sub/Hive access described in [the GCP guide](../../../docs/gcp.md).
`run.py` pins resource identities, notification configuration and owner-object
generation; `soak.py` additionally pins b's generation and bytes. Recreating a
lab changes those identities. A new authorized experiment must review/update
those approvals, validate the new catalog/pipeline, record a new source
fingerprint and capture new gates/baseline. Retain the guards and the historical
inputs/results; editing a resource JSON alone cannot authorize a new lab.

## Metadata reconstruction reference

`schema.sql` records logical databases, tables and partition locations and now
includes each captured database's `gcp_probe_run_id`. It does not create data,
restore object generations, or clone every Hive storage-descriptor field.
SQL ADD PARTITION does not restore the owner parameters required by the adapter.

For an authorized restore, with the worker stopped, use the bounded Hive Thrift
client and [catalog-schema.json](../observations/catalog-schema.json) as follows:

1. Read back each database and table using `get_database` and `get_table`.
   Verify the saved owner property on both, external/non-purge properties,
   column schema, exact root and partition locations before any mutation.
2. For each saved partition, read it using `get_partition(db, table, values)`.
   Copy the restored table's full `sd`, changing only `location` to the saved
   partition location. Set `parameters['gcp_probe_run_id']` to the captured
   database/table owner value, preserving the other partition parameters.
   Set `writeId=None` for the Hive 3.1.2 wire contract, then call
   `alter_partition(db, table, partition)` on that exact partition.
3. Read back all partitions. Require full descriptor equality with the table
   descriptor except for location, exact values and matching ownership, as in
   `HiveMetastorePartitionCatalog._get_partition`. Then validate storage bytes,
   approved owner generation and fresh named-table Spark queries before a worker
   starts. Changed resource generations require a new approval/source record.

These are reconstruction instructions, not a tested restore. The full managed
metastore backup referenced by [metastore-backup.json](../metastore-backup.json)
remains private outside Git. Use it when historical metadata fidelity is required;
logical DDL and ownership repairs alone do not constitute a complete backup.
