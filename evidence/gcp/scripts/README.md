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
Captured reconstruction DDL is [schema.sql](../schema.sql). Live helpers require
the original approved resources and appropriate grants; do not rerun fixtures
or create replacement resources from this evidence directory.
