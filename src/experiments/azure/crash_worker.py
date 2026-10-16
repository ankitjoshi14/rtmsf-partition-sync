"""Experiment-only crashes around the unchanged single-VM Azure worker."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import sys

from rtmsf import azure_app
from rtmsf.adapters.azure import AzureServiceBusEventSource
from rtmsf.adapters.databricks import DatabricksSQLStatements

TABLE = "rtmsf_azure_proof_20261014.rtmsf_proof.worker_probe"
DROP_SQL = "ALTER TABLE " + ".".join("`" + part + "`" for part in TABLE.split(".")) + " DROP IF EXISTS PARTITION (`dt` = 'recovery')"


def matches(point, intent):
    return (intent is not None and intent.partition.table_id == TABLE
            and intent.partition.source_id == "proof"
            and intent.partition.partition_path == "dt=recovery"
            and intent.mutation == ("ADD" if point == "before_processing" else "DROP"))


def install_fault(point):
    if point not in {"before_processing", "after_catalog_change"}:
        raise ValueError("invalid Azure proof fault")
    marker = Path("/var/lib/rtmsf/proof-fault-" + point + ".json")
    pending = {}
    active_intent = None
    last_state = None
    receive = AzureServiceBusEventSource.receive
    acknowledge = AzureServiceBusEventSource.acknowledge
    apply = azure_app._BudgetedPMCP.apply
    state = DatabricksSQLStatements._state
    clear = DatabricksSQLStatements._clear

    def emit(action, **values):
        print(json.dumps({"component": "experiment", "action": action,
                          "point": point, "pid": os.getpid(), **values}), flush=True)

    def crash():
        journal = azure_app.JOURNAL_PATH
        receipt = json.loads(journal.read_text()) if journal.exists() else None
        if point == "after_catalog_change":
            assert receipt and receipt["statement_id"] and last_state == "SUCCEEDED"
        else:
            assert receipt is None
        record = {"point": point, "pid": os.getpid(), "table": TABLE,
                  "utc": datetime.now(timezone.utc).isoformat(),
                  "pending_events": list(pending.values()), "journal": receipt}
        assert record["pending_events"]
        with marker.open("x") as output:
            json.dump(record, output)
            output.flush()
            os.fsync(output.fileno())
        descriptor = os.open(marker.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        emit("process_crash", **record)
        os.kill(os.getpid(), signal.SIGKILL)

    def receive_with_ids(source, *args):
        deliveries = receive(source, *args)
        for delivery in deliveries:
            entry = source._messages[delivery.handle]
            if entry.terminal_reason is not None:
                continue
            message = entry.message
            event = json.loads(source._body(message))
            pending[delivery.handle] = {"event_id": event["id"],
                "message_id": str(message.message_id), "sequence_number": message.sequence_number,
                "delivery_count": message.delivery_count, "subject": event["subject"]}
            emit("message_received", **pending[delivery.handle])
        return deliveries

    def acknowledge_with_ids(source, deliveries):
        records = [pending.get(delivery.handle) for delivery in deliveries]
        acknowledge(source, deliveries)
        for delivery, record in zip(deliveries, records):
            pending.pop(delivery.handle, None)
            if record is not None:
                emit("message_acknowledged", **record)

    def apply_with_fault(pmcp, intent):
        nonlocal active_intent
        active_intent = intent
        try:
            if point == "before_processing" and matches(point, intent) and not marker.exists():
                crash()
            return apply(pmcp, intent)
        finally:
            active_intent = None

    def state_with_evidence(sql, document, statement_id, mutation):
        nonlocal last_state
        result = state(sql, document, statement_id, mutation)
        if mutation:
            last_state = result
        return result

    def clear_with_fault(sql):
        if (point == "after_catalog_change" and matches(point, active_intent)
                and sql._journal == azure_app.JOURNAL_PATH and last_state == "SUCCEEDED"
                and not marker.exists()):
            receipt = json.loads(sql._journal.read_text())
            if receipt.get("statement_sha256") == hashlib.sha256(DROP_SQL.encode()).hexdigest():
                crash()
        return clear(sql)

    AzureServiceBusEventSource.receive = receive_with_ids
    AzureServiceBusEventSource.acknowledge = acknowledge_with_ids
    azure_app._BudgetedPMCP.apply = apply_with_fault
    DatabricksSQLStatements._state = state_with_evidence
    DatabricksSQLStatements._clear = clear_with_fault


def main():
    config = azure_app.AppConfig.from_environment()
    assert config.table_id == TABLE and config.root_prefix == "worker_probe/"
    install_fault(sys.argv[1])
    return azure_app.main()


if __name__ == "__main__":
    raise SystemExit(main())
