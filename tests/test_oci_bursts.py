import pytest

pytest.importorskip('oci')

from experiments.oci import bursts


def records(count=3):
    return ([{'action': 'delivery_buffered', 'event_count': 1} for _ in range(count)]
            + [{'action': 'pmcp_result', 'partition': 'dt=a', 'observation_count': count,
                'mutation': 'ADD', 'status': 'APPLIED'}]
            + [{'action': 'delivery_acknowledged', 'event_count': 1, 'dead_lettered': False}
               for _ in range(count)])


def test_burst_summary_requires_grouping_and_all_acknowledgements():
    report = bursts.summarize(records(), {'dt=a'}, 'ADD')
    assert report == {'notifications': 3, 'reconciliations': 1, 'acknowledgements': 3,
                      'observations_per_partition': {'dt=a': 3}}


def test_burst_summary_rejects_acknowledgement_before_reconciliation():
    items = records()
    early = items.pop()
    items.insert(0, early)
    with pytest.raises(AssertionError, match='acknowledged before reconciliation'):
        bursts.summarize(items, {'dt=a'}, 'ADD')


def test_burst_summary_rejects_inconsistent_observation_count():
    items = records()
    items.insert(4, {'action': 'pmcp_result', 'partition': 'dt=a', 'observation_count': 1,
                     'mutation': 'ADD', 'status': 'NOOP'})
    with pytest.raises(AssertionError, match='all notifications must be reconciled'):
        bursts.summarize(items, {'dt=a'}, 'ADD')


def test_burst_summary_rejects_missing_acknowledgement():
    with pytest.raises(AssertionError, match='all notifications'):
        bursts.summarize(records()[:-1], {'dt=a'}, 'ADD')


def test_external_rule_control_never_calls_events_api(monkeypatch, tmp_path):
    import json
    import sys
    from types import SimpleNamespace as NS
    from experiments.oci import bursts
    resources = tmp_path / "resources.json"
    resources.write_text(json.dumps({"namespace": "test", "region": "us-sanjose-1", "resources": {
        "bucket": "rtmsf-events-20261012", "queue": {"id": "primary", "endpoint": "primary"},
        "quarantine": {"id": "quarantine", "endpoint": "quarantine"}, "rule": {"id": "rule"}}}))
    stats = NS(queue=NS(visible_messages=0, in_flight_messages=0), dlq=NS(visible_messages=0, in_flight_messages=0))
    client = NS(get_stats=lambda *args: NS(data=stats),
                list_objects=lambda *args, **kwargs: NS(data=NS(objects=[], next_start_with=None, prefixes=[])),
                put_object=lambda *args, **kwargs: None)
    monkeypatch.setattr(bursts, "client_options", lambda *args: ({}, {}))
    monkeypatch.setattr(bursts.oci.object_storage, "ObjectStorageClient", lambda *args, **kwargs: client)
    monkeypatch.setattr(bursts.oci.queue, "QueueClient", lambda *args, **kwargs: client)
    monkeypatch.setattr(bursts.oci.events, "EventsClient", lambda *args, **kwargs: pytest.fail("controller owns rule"))
    class Worker:
        def __init__(self, **kwargs): pass
        def run_once(self): raise RuntimeError("stop_test_before_cloud_work")
    monkeypatch.setattr(bursts, "RTMSFWorker", Worker)
    monkeypatch.setattr(bursts.oci.util, "to_dict", lambda value: {"empty": True})
    monkeypatch.setattr(sys, "argv", ["bursts", "--resources", str(resources), "--output-dir", str(tmp_path / "result"), "--external-rule-control"])
    with pytest.raises(RuntimeError, match="stop_test_before_cloud_work"):
        bursts.main()
    report = json.loads((tmp_path / "result/burst-report.json").read_text())
    assert report["rule_control"] == "external controller"
    assert "rule_disabled" not in report
    assert "cleanup_error_type" not in report


def test_burst_summary_allows_late_notification_in_a_second_window():
    items = records(5) + records(1)
    report = bursts.summarize(items, {'dt=a'}, 'ADD')
    assert report == {'notifications': 6, 'reconciliations': 2, 'acknowledgements': 6,
                      'observations_per_partition': {'dt=a': 6}}
