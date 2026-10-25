"""Check copied native soak evidence without importing the supervisor."""
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
report = json.loads((root / "soak-report.json").read_text())
assert report["status"].startswith("completed")
assert report["pending_observed"] == report["unacknowledged_own_messages"] == 0
assert report["baseline_initial"] == report["baseline_final"] == "passed"
assert report["final_storage_empty"] and report["final_catalog_absent"]
assert not report["producer_uncertain"] and report["mutation_pending"] is None
assert report["child_pid"] is None and report["child_starts"] == report["child_exits"]
assert report["producer"]["creates"] == report["producer"]["deletes"] > 0
key = "gcp-native/native-r20261019n1/data/dt=soak/part-00000.parquet"
outstanding, confirmed_creates, confirmed_deletes = set(), set(), set()
parent = [json.loads(line) for line in (root / "soak-evidence.jsonl").read_text().splitlines()]
for record in parent:
    if record.get("action") == "producer_confirmed" and record.get("name") == key:
        if record["generation"] is None:
            confirmed_deletes.add(record["deleted_generation"])
        else:
            confirmed_creates.add(record["generation"])
assert confirmed_creates == confirmed_deletes
assert len(confirmed_creates) == report["producer"]["creates"]
receipts = acknowledgments = terminal = 0
final_delete_ack = False
add_ack_times, drop_ack_times = {}, {}
for index in range(1, report["child_starts"] + 1):
    child_root = root / f"child-{index:06d}"
    child_report = json.loads((child_root / "worker-report.json").read_text())
    assert 0 <= child_report["pending_deliveries"] <= 20
    pending, reconciliation = {}, None
    last_time = -1
    for sequence, line in enumerate((child_root / "worker-evidence.jsonl").read_text().splitlines()):
        record = json.loads(line)
        assert record["monotonic_seconds"] >= last_time
        last_time = record["monotonic_seconds"]
        action = record["action"]
        if action == "pmcp_result" and record.get("partition") == "dt=soak":
            reconciliation = (sequence, record["mutation"], record["status"])
        elif action == "quarantine_confirmed" and record.get("handle") in pending:
            pending[record["handle"]][2] = True
            terminal += 1
        elif action in {"event_received", "message_acknowledged"} and record.get("objectId") == key:
            assert record["bucketId"] == "rtmsf-gcp-lab-20261018-e504-fixtures"
            assert record["notificationConfig"] == "projects/_/buckets/rtmsf-gcp-lab-20261018-e504-fixtures/notificationConfigs/3"
            identity = tuple(record[field] for field in ("message_id", "objectGeneration", "eventType", "eventTime"))
            handle = record["handle"]
            if action == "event_received":
                assert handle not in pending
                pending[handle] = [identity, sequence, False]
                outstanding.add(identity)
                receipts += 1
            else:
                observed, admitted, quarantined = pending.pop(handle)
                assert observed == identity
                assert quarantined or (reconciliation and reconciliation[0] > admitted
                                       and reconciliation[2] in {"APPLIED", "NOOP"})
                outstanding.discard(identity)
                acknowledgments += 1
                if not quarantined:
                    if identity[2] == "OBJECT_FINALIZE" and reconciliation[1] == "ADD":
                        add_ack_times.setdefault(identity[1], record["monotonic_seconds"])
                    if identity[2] == "OBJECT_DELETE" and reconciliation[1] == "DROP":
                        drop_ack_times.setdefault(identity[1], record["monotonic_seconds"])
                if identity[1] == report["final_delete_generation"] and identity[2] == "OBJECT_DELETE":
                    assert not quarantined and reconciliation[1] == "DROP"
                    final_delete_ack = True
assert not outstanding and final_delete_ack
previous_confirmation = previous_delete = None
for record in parent:
    if record.get("name") != key:
        continue
    if record["action"] == "producer_attempt":
        at = record["monotonic_seconds"]
        if previous_confirmation is not None:
            assert at - previous_confirmation >= 5
        if record["mutation"] == "delete":
            generation = record["if_generation_match"]
            assert generation in add_ack_times and add_ack_times[generation] < at
        elif previous_delete is not None:
            assert previous_delete in drop_ack_times and drop_ack_times[previous_delete] < at
    elif record["action"] == "producer_confirmed":
        previous_confirmation = record["monotonic_seconds"]
        if record["generation"] is None:
            previous_delete = record["deleted_generation"]
print(json.dumps({"status": "passed", "supervisor_status": report["status"],
                  "creates": len(confirmed_creates), "deletes": len(confirmed_deletes),
                  "own_receipts": receipts, "own_acknowledgments": acknowledgments,
                  "own_terminal_events": terminal, "own_unacknowledged": len(outstanding),
                  "restarts": report["child_restarts"], "failed_children": report["failed_children"],
                  "all_child_exits_recorded": True, "final_delete_correlated": final_delete_ack,
                  "serialized_ack_barriers_and_minimum_interval": True,
                  "baseline_report_preserved": True}))
