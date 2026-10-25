"""Fault-harness checks; execute only in Azure before the live crash trials."""
import inspect
import hashlib
import json
import signal
from types import SimpleNamespace

import pytest

from experiments.azure import crash_worker as fault


@pytest.fixture
def hooks(monkeypatch):
    for owner, name in [(fault.AzureServiceBusEventSource, 'receive'),
                        (fault.AzureServiceBusEventSource, 'acknowledge'),
                        (fault.azure_app._BudgetedPMCP, 'apply'),
                        (fault.DatabricksSQLStatements, '_state'),
                        (fault.DatabricksSQLStatements, '_clear')]:
        monkeypatch.setattr(owner, name, inspect.getattr_static(owner, name))
    return monkeypatch


def intent(mutation='ADD', **changes):
    partition = SimpleNamespace(table_id=fault.TABLE, source_id='proof',
                                partition_path='dt=recovery')
    partition.__dict__.update(changes)
    return SimpleNamespace(partition=partition, mutation=mutation)


@pytest.mark.parametrize('point,mutation', [('before_processing', 'ADD'), ('after_catalog_change', 'DROP')])
def test_fault_matches_only_the_exact_approved_partition(point, mutation):
    assert fault.matches(point, intent(mutation))
    assert not fault.matches(point, intent('DROP' if mutation == 'ADD' else 'ADD'))
    for change in [{'table_id': 'other.table'}, {'source_id': 'other'}, {'partition_path': 'dt=life'}]:
        assert not fault.matches(point, intent(mutation, **change))
    assert not fault.matches(point, None)


def test_fault_rejects_unknown_point(hooks):
    with pytest.raises(ValueError):
        fault.install_fault('unknown')


def test_fault_preserves_sql_state_validation(hooks):
    fault.install_fault('after_catalog_change')
    sql = object.__new__(fault.DatabricksSQLStatements)
    document = {'statement_id': 'statement-1', 'status': {'state': 'SUCCEEDED'}}
    assert sql._state(document, 'statement-1', True) == 'SUCCEEDED'


@pytest.mark.parametrize('point,mutation', [('before_processing', 'ADD'), ('after_catalog_change', 'DROP')])
def test_crash_keeps_event_and_pending_receipt_before_acknowledgment(hooks, tmp_path, point, mutation):
    marker = tmp_path / 'fault.json'
    journal = tmp_path / 'pending.json'
    hooks.setattr(fault, 'Path', lambda _: marker)
    hooks.setattr(fault.azure_app, 'JOURNAL_PATH', journal)
    if point == 'after_catalog_change':
        statement = 'ALTER TABLE ' + '.'.join('`' + part + '`' for part in fault.TABLE.split('.')) + " DROP IF EXISTS PARTITION (`dt` = 'recovery')"
        journal.write_text(json.dumps({'statement_id': 'statement-1', 'statement_sha256': hashlib.sha256(statement.encode()).hexdigest()}))
    calls = []
    class Killed(Exception):
        pass
    def kill(pid, value):
        assert value == signal.SIGKILL
        assert marker.exists()
        raise Killed()
    hooks.setattr(fault.os, 'kill', kill)
    hooks.setattr(fault.DatabricksSQLStatements, '_clear', lambda _: calls.append('cleared'))
    sql = object.__new__(fault.DatabricksSQLStatements)
    sql._journal = journal
    def apply(_, value):
        calls.append('mutation')
        sql._state({'statement_id': 'statement-1', 'status': {'state': 'SUCCEEDED'}}, 'statement-1', True)
        sql._clear()
    hooks.setattr(fault.azure_app._BudgetedPMCP, 'apply', apply)
    hooks.setattr(fault.AzureServiceBusEventSource, 'receive', lambda *_: [SimpleNamespace(handle='handle')])
    source = SimpleNamespace(_messages={'handle': SimpleNamespace(terminal_reason=None, message=SimpleNamespace(
        message_id='message-1', sequence_number=1, delivery_count=1))},
        _body=lambda _: json.dumps({'id': 'event-1', 'subject': 'worker_probe/dt=recovery/recover.parquet'}))
    fault.install_fault(point)
    fault.AzureServiceBusEventSource.receive(source, 1, 1, 280)
    with pytest.raises(Killed):
        fault.azure_app._BudgetedPMCP.apply(None, intent(mutation))
    evidence = json.loads(marker.read_text())
    assert evidence['pending_events'][0]['event_id'] == 'event-1'
    assert calls == ([] if point == 'before_processing' else ['mutation'])
    assert journal.exists() == (point == 'after_catalog_change')


@pytest.mark.parametrize('body', [b'invalid JSON', b'[]', b'{}'])
def test_fault_preserves_native_dlq_for_malformed_deliveries(hooks, body):
    from tests.test_azure_queue import Receiver, message, receive, source
    item = message(body)
    receiver = Receiver([item])
    adapter = source(receiver)
    fault.install_fault('before_processing')
    deliveries = receive(adapter)
    assert not deliveries[0].events
    adapter.acknowledge(deliveries)
    assert [call[0] for call in receiver.calls] == ['receive', 'dead_letter']


def test_drop_intent_repair_add_does_not_trigger_post_drop_crash(hooks, tmp_path):
    marker = tmp_path / 'fault.json'
    journal = tmp_path / 'pending.json'
    journal.write_text(json.dumps({'statement_id': 'statement-1', 'statement_sha256': hashlib.sha256(b'ADD repair').hexdigest()}))
    hooks.setattr(fault, 'Path', lambda _: marker)
    hooks.setattr(fault.azure_app, 'JOURNAL_PATH', journal)
    sql = object.__new__(fault.DatabricksSQLStatements)
    sql._journal = journal
    cleared = []
    hooks.setattr(fault.DatabricksSQLStatements, '_clear', lambda _: cleared.append(True))
    def apply(_, value):
        sql._state({'statement_id': 'statement-1', 'status': {'state': 'SUCCEEDED'}}, 'statement-1', True)
        sql._clear()
    hooks.setattr(fault.azure_app._BudgetedPMCP, 'apply', apply)
    fault.install_fault('after_catalog_change')
    fault.azure_app._BudgetedPMCP.apply(None, intent('DROP'))
    assert cleared == [True] and not marker.exists()
