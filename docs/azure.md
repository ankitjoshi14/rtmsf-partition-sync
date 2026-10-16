# Azure deployment architecture

The first Azure target is **ADLS Gen2 → Event Grid → Service Bus → one Azure VM worker → Databricks Unity Catalog**. It reuses the existing Python routing, WOERA, and PMCP core. The figure uses the same visual style as the [AWS](figures/aws-deployment.svg) and [OCI](figures/oci-deployment.svg) deployment diagrams, with [official Microsoft Azure service icons](figures/azure-icons.md).

![RTMSF deployment architecture on Azure](figures/azure-deployment.svg)

## Current validation boundary

The Azure catalog proof passed on October 15, 2026. An external Parquet table with `partitionMetadataEnabled=true` initially hid a path-written row from table queries. Registering its complete partition twice exposed exactly one row and one partition. Dropping the partition twice without PURGE removed catalog membership while a separate path query still read the physical row.

The Event Grid decoder now has 46 unit cases verified in Azure Container Instances, alongside 61 existing core cases. It validates the configured account, container, and full ARM topic; preserves exact URL keys; and accepts only supported committed file events. The tests cover percent decoding, literal plus, malformed URLs, invalid Unicode, timestamps, and operation filtering. Live Azure events must still verify special-name subject escaping.

One private Azure VM and Key Vault have also been deployed. The VM has no public IP, denies inbound traffic, and runs verification through Azure VM Run Command. It is deallocated between verification sessions; no worker service is installed yet.

The catalog proof establishes the selected catalog's partition behavior. Queue, storage, and catalog adapters, unattended SQL authentication, the event pipeline, a deployed cloud worker, and crash/replay behavior remain unfinished. The diagram describes the intended deployment; it does not claim a completed Azure port or establish Azure performance from AWS measurements.

## Services and responsibilities

| Azure component | Responsibility |
|---|---|
| ADLS Gen2 / Blob Storage | Store plain directory-partitioned Parquet files; provide authoritative primary-endpoint existence and listing checks. |
| Event Grid storage subscription | Deliver file notifications for the configured disposable table prefix. Retain failed ingress delivery in a separate Blob dead-letter container. |
| Service Bus queue | Retain pending input. Use PeekLock, renew every retained delivery, and complete only after all its reconciliation finishes. |
| Service Bus native DLQ | Retain terminal and expired input. Successful dead-lettering already settles the delivery; the adapter must not complete it again. |
| Azure Virtual Machine | Run one RTMSF systemd service, with no public IP or inbound ports. Stop and verify the old process before updating or replacing it. |
| Azure VM Run Command | Install and verify source and dependencies in Azure without a local CLI login or SSH connection. |
| Azure Key Vault | Store the user-entered Databricks OAuth credential; let only the approved VM identity retrieve the worker secret. |
| Databricks SQL warehouse / Unity Catalog | Resolve existing table definitions and execute exact SHOW/ADD/DROP partition operations for approved external Parquet tables. |
| Azure managed identity | Authorize worker queue receive/settlement, read-only test storage access, and retrieval of its approved OAuth secret. |
| Databricks-managed service principal | Provide unattended OAuth M2M SQL authentication with restricted resource permissions. It is separate from Azure storage/queue identity. |
| Databricks Access Connector | Authorize the SQL engine's governed storage reads. It does not authenticate the RTMSF worker to SQL. |
| VM journal / proof evidence | Record processing, settlement, and recovery evidence. Export the bounded proof logs before deallocating compute. |

The user selected a single VM on October 15, 2026, to match AWS EC2 ownership. Container Apps Single revision mode and maximum one replica do not guarantee one active process during replacement or platform prewarming, so they are insufficient for the unchanged core. The VM path avoids adding distributed worker ownership to this first port.

## First functional contract

Support completed Blob REST uploads/overwrites, ADLS `FlushWithClose`, and individual file deletions. Initial ADLS file creation is not a completed upload. Validate the account, container, configured prefix, and exact decoded object key before routing.

Directory rename, recursive directory deletion, SFTP mutations, and version/undelete restoration are outside this first contract. Transactional Delta/Iceberg commits require their own protocols. Do not infer an empty partition from a directory marker or from a listing page that still has a continuation token.

Validate external table type, Parquet provider, explicit partition metadata, approved storage location, and complete partition columns at startup. Quote identifiers and literals safely. Never use PURGE or a partial partition specification. The new disposable `worker_probe` table and its storage prefix remain separate from the completed catalog proof.

Bound pending messages, retained bytes, storage listing time, catalog statement time, and settlement/renewal calls. The configured queue lease must cover those bounds. A lost lease or uncertain settlement ends the run without prematurely acknowledging unfinished input; the live recovery proof must demonstrate convergence after replay.

## Execution and access checkpoints

Builds, dependency installation, worker execution, fixture generation, and live tests run in Azure. No local RTMSF worker starts. Prepare source and review deployment artifacts before applying new access grants or deploying resources.

The existing SQL warehouse is 2X-Small, with at most one cluster and five-minute auto-stop. Stop the worker, deallocate the VM, and stop the warehouse when each bounded proof session ends. VM disks and other retained resources can still incur charges while compute is deallocated. The Databricks spending limit is $10; Azure uses existing Free Trial credit. Do not upgrade to pay-as-you-go or treat delayed billing as zero cost.

Generate and enter any new OAuth credential through a user browser handoff into the reviewed cloud secret destination. Do not ask for credentials in chat, commit a secret, save a local `.env`, or emit a secret in logs. Exact Azure IAM grants and table ownership require review before application.

## Completion evidence

The first complete worker proof must demonstrate real committed events, first-file registration, additional-file no-op, overwrite, deletion with other files remaining, last-file removal, duplicate/reordered input, a same-partition burst, and restart/replay before and after catalog mutation and settlement. Use an independent catalog query and authoritative storage witness; duplicate SQL statements alone are insufficient.

Save source/dependency identity, queue/log evidence, scenario outcomes, warehouse timings, observed credit consumption, and final stopped compute state. Broader AWS-style performance experiments follow only after this functional Azure proof passes.

## Primary references

- [External partition discovery](https://learn.microsoft.com/en-us/azure/databricks/tables/external-partition-discovery)
- [SQL partition management](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-alter-table-manage-partition)
- [Storage event schema](https://learn.microsoft.com/en-us/azure/event-grid/event-schema-blob-storage)
- [Service Bus locks and settlement](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-transfers-locks-settlement)
- [SQL connector authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/python-sql-connector)
- [OAuth M2M and scoped secrets](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/oauth-m2m)
