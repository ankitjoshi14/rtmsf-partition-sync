# Azure functional proof — October 16, 2026

The deployed path was ADLS Gen2 → Event Grid → Service Bus → one private Azure VM RTMSF service → Azure Databricks SQL / Unity Catalog. The disposable table was `rtmsf_azure_proof_20261014.rtmsf_proof.worker_probe`, with explicit Parquet `dt` partition metadata. Worker, fixture, SDK, test, and build execution all ran in Azure.

[report_inputs.json](report_inputs.json) collects the actual saved observations. [SHA256SUMS](SHA256SUMS) fingerprints this selected bundle. [The Azure guide](../../docs/azure.md) explains the implementation and supported event contract; [the diagram](../../docs/figures/azure-deployment.svg) uses official Microsoft Azure symbols.

Copied text exports have trailing whitespace removed; JSON payloads and reported values are unchanged.

| Check | Observed result |
|---|---|
| First/additional files | First partition ADD applied; subsequent ADD was a no-op. Rows changed from `1` to `1,2`. |
| Overwrite | Rows changed to `2,11` without a membership mutation. |
| Partial/last deletion | Occupied partition remained; deleting its final Parquet file removed the partition and all rows. |
| Twelve-file burst | Twelve rows `100..111`, one partition, three groups of four observations: one ADD and two no-ops. |
| Eight synthetic duplicate/reordered hints | Four occupied-burst hints reconciled safely. Four stale life-file hints exhausted `not_visible` retries and reached native DLQ. |
| Before-processing crash | PID `2800` killed before ADD; file present, recovery partition absent, no mutation journal. PID `2898` received and acknowledged the same event, delivery count `0 → 1`; independent witness found row `900` and no pending input. |
| Post-DROP crash | PID `3086` killed after successful exact DROP, before journal clear/ack. PID `3184` recovered the retained statement receipt and received/acknowledged the same event, delivery count `0 → 1`; independent witness confirmed catalog/storage agreement and no pending input. |
| Final fixture cleanup | Zero rows, partitions, Parquet files, and source messages. Directory markers remain. |
| Cloud suite/package checks | `619 passed in 6.05s`; wheel and sdist built. Source hashes and restricted-user VM installation were verified before either crash trial. |

The two crash trials used real storage events. Their fault markers bind the killed PID to public event/message IDs, and the post-DROP marker contains the original durable SQL receipt. Restart used the same systemd service and fixed host lock, with the old PID confirmed zero before changing fault mode. The temporary harness restricts its fault to the exact disposable recovery partition; the second fault additionally requires the journal hash of the exact DROP statement. The production core is unchanged.

Five DLQ messages remain as evidence: the first real event failed before the Databricks zero-chunk empty-result fix, and four explicitly synthetic stale-create hints were quarantined. The core gives CREATE precedence within a coalescing window, irrespective of source timestamps. These results do not establish successful completion of every replayed message.

The early cleanup witness recorded zero rows but three pending queue messages and correctly failed. The later cleanup witness passed after settlement. The earlier rows-only replay log is not used as completion evidence; the fresh `replay` witness checks the empty source queue. The original combined replay export was truncated by Portal and is retained only for its telemetry and complete DLQ record. Later witnesses were captured in Azure and exported through hash-checked gzip/base64 to avoid that limit.

The source regression for Databricks empty results and two review regressions for the temporary harness failed in Azure before their respective fixes. The first six harness checks were self-checks that passed; a driver filename containing `red` does not make them failing-regression evidence.

## Scope and shutdown

This is a bounded functional proof, with no claim of AWS-equivalent throughput, latency, request cost, sustainable load, or statistical performance. Live special-name subject escaping, broader ADLS write APIs, and multi-worker operation remain untested.

The worker is stopped/disabled with its normal `ExecStart` restored, ACI is terminated, and the VM is stopped/deallocated. Both temporary fixture grants are removed: [storage access](observations/azure-fixture-storage-grant-removed.txt) and [queue sending](observations/azure-fixture-queue-grant-removed.txt). After manual console navigation, [the Databricks warehouse reports Stopped with zero active clusters](observations/azure-warehouse-stopped.txt). Five-minute auto-stop remains configured. Automated console actions were blocked and the SQL-only credential could not manage warehouse state.

The approved Databricks ceiling is $10 and Azure uses existing trial credit. The cost view reports Databricks $0.24 and storage less than $0.01, with delayed billing. Final spend is unknown. Retained disks, Service Bus, Key Vault, and storage can incur ongoing charges after compute shutdown; no subscription upgrade was made.

## Saved sources

`scripts/` preserves the exact cloud fixture, independent witness, and capture helper. They contain proof-specific resource identifiers and run only against that approved disposable environment. They are not generic provisioning tools. The experiment-only crash harness is [src/experiments/azure/crash_worker.py](../../src/experiments/azure/crash_worker.py).

Do not rerun fixtures until the intended disposable prefix, table definition, scoped identity grants, queue lock/retention, single-worker ownership, and spending limits are verified. Store OAuth credentials in Key Vault and retrieve them in memory; no credential is included in this evidence bundle.
