"""Azure worker composition contract; execute only on the approved Azure VM."""

from contextlib import contextmanager
from dataclasses import fields
from datetime import datetime, timedelta, timezone
import importlib
import importlib.util
import json
from pathlib import Path
import signal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from rtmsf.interfaces import FatalExternalError, RetryableExternalError
from rtmsf.models import MutationResult, PartitionIntent
from tests.test_databricks_catalog import HTTP, PARTITION, describe, inline


SOURCES = {"proof": {
    "account": "proofaccount", "container": "proof",
    "topic": "/subscriptions/proof-sub/resourceGroups/proof-rg/providers/Microsoft.Storage/storageAccounts/proofaccount",
}}


def app_module():
    assert importlib.util.find_spec("rtmsf.azure_app") is not None, "Azure worker composition not implemented"
    return importlib.import_module("rtmsf.azure_app")


def environment(**overrides):
    return {
        "RTMSF_SOURCES": json.dumps(SOURCES), "RTMSF_TABLE_ID": "proof.default.worker_probe",
        "RTMSF_ROOT_PREFIX": "worker_probe/",
        "AZURE_SERVICE_BUS_NAMESPACE": "proof-worker.servicebus.windows.net",
        "AZURE_SERVICE_BUS_QUEUE": "worker-events", "AZURE_KEY_VAULT_URL": "https://proof-vault.vault.azure.net",
        "DATABRICKS_HOST": "https://adb-1234567890123456.7.azuredatabricks.net",
        "DATABRICKS_WAREHOUSE_ID": "warehouse-1", "DATABRICKS_CLIENT_ID": "test-only-client", **overrides,
    }


class Telemetry:
    def __init__(self):
        self.records = []

    def emit(self, record):
        self.records.append(record)


def setup(monkeypatch, tmp_path, *, http=None, secret="test-only-secret", vault_delay=0, startup_error=None):
    app = app_module()
    http = http or HTTP([describe()])
    events, calls, products = [], {}, {}
    telemetry = Telemetry()

    class Resource:
        def __init__(self, name):
            self.name = name
            self.closed = False

        def __enter__(self):
            events.append("enter:" + self.name)
            return self

        def __exit__(self, *_args):
            self.close()

        def close(self):
            self.closed = True
            events.append("close:" + self.name)

    receiver = Resource("receiver")
    receiver.receive_messages = lambda **kwargs: events.append(("receive", kwargs)) or []
    receiver.complete_message = lambda *args, **kwargs: events.append("complete")
    receiver.dead_letter_message = lambda *args, **kwargs: events.append("dead_letter")
    receiver.renew_message_lock = lambda *args, **kwargs: datetime.now(timezone.utc) + timedelta(seconds=300)
    products["receiver"] = receiver
    products["container"] = object()
    products["filesystem"] = object()

    def factory(name):
        def construct(*args, **kwargs):
            events.append("construct:" + name)
            calls[name] = (args, kwargs)
            resource = Resource(name)
            products[name] = resource
            if name == "servicebus":
                def get_receiver(**options):
                    events.append("get_receiver")
                    calls["receiver"] = options
                    return receiver
                resource.get_queue_receiver = get_receiver
            elif name == "blob":
                def get_container(**options):
                    calls["container"] = options
                    return products["container"]
                resource.get_container_client = get_container
            elif name == "datalake":
                def get_filesystem(**options):
                    calls["filesystem"] = options
                    return products["filesystem"]
                resource.get_file_system_client = get_filesystem
            elif name == "vault":
                def get_secret(name):
                    events.append("get_secret")
                    calls["secret_name"] = name
                    http.clock.sleep(vault_delay)
                    if startup_error is not None:
                        raise startup_error
                    return SimpleNamespace(value=secret)
                resource.get_secret = get_secret
            return resource
        return construct

    for attribute, name in (
        ("ManagedIdentityCredential", "identity"), ("SecretClient", "vault"),
        ("BlobServiceClient", "blob"), ("DataLakeServiceClient", "datalake"),
        ("ServiceBusClient", "servicebus"),
    ):
        monkeypatch.setattr(app, attribute, factory(name))
    original_statements = app.DatabricksSQLStatements

    def statements(*args, **kwargs):
        events.append("construct:statements")
        calls["statements"] = (args, kwargs)
        result = original_statements(*args, **kwargs, request=http, clock=http.clock, sleep=http.clock.sleep)
        products["statements"] = result
        return result

    original_resolve = app.DatabricksPartitionCatalog.resolve_tables

    def resolve(catalog, table_ids):
        events.append("resolve:start")
        result = original_resolve(catalog, table_ids)
        events.append("resolve:done")
        return result

    monkeypatch.setattr(app, "DatabricksSQLStatements", statements)
    monkeypatch.setattr(app.DatabricksPartitionCatalog, "resolve_tables", resolve)
    monkeypatch.setattr(app.time, "monotonic", http.clock)
    monkeypatch.setattr(app, "JOURNAL_PATH", tmp_path / "pending.json")
    return app, SimpleNamespace(events=events, calls=calls, products=products, telemetry=telemetry, http=http)


def test_config_has_one_source_one_table_and_exact_derived_root():
    app = app_module()
    config = app.AppConfig.from_environment(environment(RTMSF_ROOT_PREFIX="worker probe/a+b/"))
    assert dict(config.sources) == SOURCES
    assert config.table_id == "proof.default.worker_probe"
    assert config.root_location == "abfss://proof@proofaccount.dfs.core.windows.net/worker%20probe/a%2Bb/"
    assert (config.settling_ms, config.maximum_ms) == (10000, 31000)
    assert not any("secret" in field.name for field in fields(config))


@pytest.mark.parametrize("overrides", [
    {"RTMSF_SOURCES": "broken"}, {"RTMSF_SOURCES": "[]"}, {"RTMSF_SOURCES": "{}"},
    {"RTMSF_SOURCES": json.dumps({**SOURCES, "other": {**SOURCES["proof"], "container": "other"}})},
    {"RTMSF_SOURCES": json.dumps({"proof": {**SOURCES["proof"], "account": "https://untrusted.invalid"}})},
    {"RTMSF_SOURCES": json.dumps({"proof": {**SOURCES["proof"], "topic": SOURCES["proof"]["topic"] + "/extra"}})},
    {"RTMSF_TABLE_ID": "default.worker_probe"}, {"RTMSF_TABLE_ID": "proof..worker_probe"},
    {"RTMSF_TABLE_ID": "proof.default.worker_probe.extra"}, {"RTMSF_TABLE_ID": "proof.default.worker\nprobe"},
    {"RTMSF_ROOT_PREFIX": "worker_probe"}, {"RTMSF_ROOT_PREFIX": "/worker_probe/"},
    {"RTMSF_ROOT_PREFIX": "worker_probe/../"}, {"RTMSF_ROOT_PREFIX": "worker_probe//"},
    {"RTMSF_ROOT_PREFIX": "_worker_probe/"}, {"RTMSF_ROOT_PREFIX": ".worker_probe/"},
    {"RTMSF_ROOT_PREFIX": "worker_probe/_data/"}, {"RTMSF_ROOT_PREFIX": "worker_probe/.data/"},
    {"RTMSF_ROOT_PREFIX": "worker_probe/\u202e/"},
    {"AZURE_SERVICE_BUS_NAMESPACE": "https://proof-worker.servicebus.windows.net"},
    {"AZURE_SERVICE_BUS_NAMESPACE": "proof-worker.servicebus.windows.net:443"},
    {"AZURE_SERVICE_BUS_NAMESPACE": "proof-worker.servicebus.windows.net/untrusted"},
    {"AZURE_SERVICE_BUS_NAMESPACE": "proof-worker.servicebus.windows.net.evil.invalid"},
    {"AZURE_SERVICE_BUS_QUEUE": "worker-events/$DeadLetterQueue"},
    {"AZURE_SERVICE_BUS_QUEUE": "https://untrusted.invalid"}, {"AZURE_SERVICE_BUS_QUEUE": "queue\nname"},
    {"AZURE_KEY_VAULT_URL": "http://proof-vault.vault.azure.net"},
    {"AZURE_KEY_VAULT_URL": "https://user@proof-vault.vault.azure.net"},
    {"AZURE_KEY_VAULT_URL": "https://proof-vault.vault.azure.net/"},
    {"AZURE_KEY_VAULT_URL": "https://proof-vault.vault.azure.net?secret=untrusted"},
    {"AZURE_KEY_VAULT_URL": "https://proof-vault.vault.azure.net.evil.invalid"},
    {"DATABRICKS_HOST": "https://adb-1234567890123456.7.azuredatabricks.net/"},
    {"DATABRICKS_HOST": "https://untrusted.invalid"}, {"DATABRICKS_CLIENT_ID": "client\nvalue"},
    {"DATABRICKS_WAREHOUSE_ID": ""},
])
def test_config_rejects_untrusted_values_before_authentication(overrides):
    with pytest.raises(ValueError):
        app_module().AppConfig.from_environment(environment(**overrides))


def test_missing_config_and_invalid_values_do_not_echo_input():
    with pytest.raises(ValueError) as failure:
        app_module().AppConfig.from_environment(environment(DATABRICKS_HOST="private-test-value"))
    assert "private-test-value" not in str(failure.value)
    with pytest.raises(ValueError):
        app_module().AppConfig.from_environment({})


def test_composition_bounds_clients_shares_identity_and_recovers_before_receiver(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path)
    with app.build_worker(app.AppConfig.from_environment(environment()), telemetry=state.telemetry) as worker:
        assert not any(resource.closed for resource in state.products.values() if hasattr(resource, "closed"))
        assert state.events.index("resolve:done") < state.events.index("get_receiver")
        assert state.events.index("resolve:done") < state.events.index("enter:receiver")
        assert state.calls["identity"] == ((), {
            "retry_total": 0, "connection_timeout": 5, "read_timeout": 5, "logging_enable": False,
        })
        identity = state.products["identity"]
        for name, endpoint in (
            ("blob", "https://proofaccount.blob.core.windows.net"),
            ("datalake", "https://proofaccount.dfs.core.windows.net"),
        ):
            assert state.calls[name] == ((), {
                "account_url": endpoint, "credential": identity, "retry_total": 0,
                "retry_to_secondary": False, "connection_timeout": 5, "read_timeout": 5, "logging_enable": False,
            })
        assert state.calls["vault"] == ((), {
            "vault_url": "https://proof-vault.vault.azure.net", "credential": identity,
            "retry_total": 0, "connection_timeout": 5, "read_timeout": 5, "logging_enable": False,
        })
        assert state.calls["secret_name"] == "databricks-client-secret"
        assert state.calls["servicebus"] == ((), {
            "fully_qualified_namespace": "proof-worker.servicebus.windows.net", "credential": identity,
            "retry_total": 0, "try_timeout": 10, "uamqp_transport": False, "logging_enable": False,
        })
        assert state.calls["receiver"] == {
            "queue_name": "worker-events", "receive_mode": app.ServiceBusReceiveMode.PEEK_LOCK,
            "prefetch_count": 0, "socket_timeout": 5,
        }
        assert state.calls["container"] == {"container": "proof"}
        assert state.calls["filesystem"] == {"file_system": "proof"}
        assert worker._source._receiver is state.products["receiver"]
        assert worker._source._telemetry is state.telemetry
        assert worker._poll_size == 1 and worker._max_pending_deliveries == 4
        assert worker._queue_lease_seconds == 280
        assert worker._pmcp_budget_ms == 110000 and worker._source_budget_ms == 80000 and worker._margin_ms == 1000
        assert state.calls["statements"][1]["operation_seconds"] == 110
        assert state.calls["statements"][1]["journal_path"] == tmp_path / "pending.json"
        worker.run_once()
        assert ("receive", {"max_message_count": 1, "max_wait_time": 20}) in state.events
    closed = [name for name, product in state.products.items() if hasattr(product, "closed")]
    assert all(state.products[name].closed for name in closed)
    assert state.events.index("close:receiver") < state.events.index("close:servicebus") < state.events.index("close:identity")


@pytest.mark.parametrize("secret", [None, "", "value\nwith-control", "value\u202ewith-format", "\ud800"])
def test_invalid_key_vault_value_stops_and_closes_without_queue_poll(monkeypatch, tmp_path, secret):
    app, state = setup(monkeypatch, tmp_path, secret=secret)
    with pytest.raises(ValueError, match="^invalid_databricks_secret$"):
        with app.build_worker(app.AppConfig.from_environment(environment()), telemetry=state.telemetry):
            pytest.fail("invalid secret reached worker")
    assert "get_receiver" not in state.events
    assert not any(isinstance(event, tuple) and event[0] == "receive" for event in state.events)
    assert state.products["identity"].closed and state.products["vault"].closed


def test_key_vault_elapsed_time_is_charged_before_metadata_sql(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path, vault_delay=111)
    with pytest.raises(RetryableExternalError, match="databricks_operation_budget_exceeded"):
        with app.build_worker(app.AppConfig.from_environment(environment()), telemetry=state.telemetry):
            pytest.fail("startup overrun reached worker")
    assert state.http.calls == []
    assert "get_receiver" not in state.events
    assert state.products["identity"].closed and state.products["vault"].closed


def test_unresolved_mutation_stops_before_receiver_and_outside_pmcp_categories(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path)
    (tmp_path / "pending.json").write_text("invalid pending mutation")
    with pytest.raises(RuntimeError) as failure:
        with app.build_worker(app.AppConfig.from_environment(environment()), telemetry=state.telemetry):
            pytest.fail("unresolved mutation reached worker")
    assert not isinstance(failure.value, (FatalExternalError, RetryableExternalError))
    assert state.http.calls == [] and "get_receiver" not in state.events
    assert state.products["identity"].closed and state.products["vault"].closed


def test_key_vault_failure_closes_clients_without_opening_receiver(monkeypatch, tmp_path):
    failure = RuntimeError("test-only-sensitive-startup-value")
    app, state = setup(monkeypatch, tmp_path, startup_error=failure)
    with pytest.raises(RuntimeError) as raised:
        with app.build_worker(app.AppConfig.from_environment(environment()), telemetry=state.telemetry):
            pytest.fail("failed startup reached worker")
    assert raised.value is failure
    assert "get_receiver" not in state.events
    assert state.products["identity"].closed and state.products["vault"].closed


def test_pmcp_storage_overrun_raises_before_any_delivery_settlement(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path, http=HTTP([describe(), inline([["dt=2026-10-15/region=west"]])]))
    monkeypatch.setattr("rtmsf.worker.time.monotonic_ns", lambda: int(state.http.clock.now * 1_000_000_000))
    with app.build_worker(app.AppConfig.from_environment(environment()), telemetry=state.telemetry) as worker:
        def slow_storage(*_args):
            state.http.clock.sleep(111)
            return True
        monkeypatch.setattr(app.AzureObjectStore, "prefix_non_empty", slow_storage)
        body = json.dumps({
            "id": "test-event", "topic": SOURCES["proof"]["topic"],
            "subject": "/blobServices/default/containers/proof/blobs/worker_probe/dt=2026-10-15/region=west/file.parquet",
            "eventType": "Microsoft.Storage.BlobDeleted",
            "data": {"api": "DeleteBlob", "blobType": "BlockBlob", "url": "https://proofaccount.blob.core.windows.net/proof/worker_probe/dt=2026-10-15/region=west/file.parquet"},
        })
        message = SimpleNamespace(body=body.encode(), body_type=worker._source._data_type,
                                  lock_token=uuid4(), locked_until_utc=datetime.now(timezone.utc) + timedelta(seconds=300))
        state.products["receiver"].receive_messages = lambda **_options: [message]
        worker.run_once()
        state.http.clock.sleep(31)
        with pytest.raises(RetryableExternalError, match="databricks_operation_budget_exceeded"):
            worker.run_once()
        assert "complete" not in state.events and "dead_letter" not in state.events


def test_pmcp_success_and_failed_body_preserve_existing_result_or_exception(monkeypatch, tmp_path):
    app, state = setup(monkeypatch, tmp_path)
    result = MutationResult("NOOP", "test_result")
    failure = RuntimeError("test-only-sensitive-body-value")

    class Protocol:
        def __init__(self, storage, catalog):
            assert isinstance(storage, app.AzureObjectStore)
            assert isinstance(catalog, app.DatabricksPartitionCatalog)

        def apply(self, _intent):
            assert state.products["statements"]._deadline is not None
            return result

    monkeypatch.setattr(app, "PartitionMutationCommitProtocol", Protocol)
    with app.build_worker(app.AppConfig.from_environment(environment()), telemetry=state.telemetry) as worker:
        assert worker._pmcp.apply(PartitionIntent(PARTITION, "DROP")) is result
        def failed_apply(_self, _intent):
            state.http.clock.sleep(111)
            raise failure
        monkeypatch.setattr(Protocol, "apply", failed_apply)
        with pytest.raises(RuntimeError) as raised:
            worker._pmcp.apply(PartitionIntent(PARTITION, "DROP"))
        assert raised.value is failure


def test_fixed_host_lock_rejects_duplicate_without_replacing_inode(tmp_path):
    app = app_module()
    assert app.LOCK_PATH == Path("/var/lib/rtmsf/worker.lock")
    assert app.JOURNAL_PATH == Path("/var/lib/rtmsf/databricks-pending.json")
    path = tmp_path / "worker.lock"
    with app._host_lock(path):
        inode = path.stat().st_ino
        with pytest.raises(RuntimeError, match="azure_worker_already_running"):
            with app._host_lock(path):
                pytest.fail("duplicate worker acquired singleton lock")
    assert path.exists() and path.stat().st_ino == inode
    with app._host_lock(path):
        assert path.stat().st_ino == inode


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_main_holds_lock_before_build_and_runs_with_signal_stop(monkeypatch, tmp_path, stop_signal):
    app = app_module()
    path = tmp_path / "worker.lock"
    handlers, events = {}, []
    monkeypatch.setattr(app, "LOCK_PATH", path)
    monkeypatch.setattr(app.signal, "signal", lambda number, handler: handlers.setdefault(number, handler))
    config = app.AppConfig.from_environment(environment())
    monkeypatch.setattr(app.AppConfig, "from_environment", lambda: config)

    @contextmanager
    def build(config):
        with pytest.raises(RuntimeError, match="azure_worker_already_running"):
            with app._host_lock(path):
                pytest.fail("authentication started before host lock")
        events.append("build")
        def run(stop):
            assert not stop()
            handlers[stop_signal](stop_signal, None)
            assert stop()
            events.append("run")
        try:
            yield SimpleNamespace(run_forever=run)
        finally:
            events.append("close")

    monkeypatch.setattr(app, "build_worker", build)
    assert app.main() == 0
    assert events == ["build", "run", "close"]
    assert path.exists()


def test_main_failure_is_fixed_redacted_output_and_nonzero(monkeypatch, tmp_path, capsys):
    app = app_module()
    monkeypatch.setattr(app, "LOCK_PATH", tmp_path / "worker.lock")
    monkeypatch.setattr(app.signal, "signal", lambda *_args: None)
    config = app.AppConfig.from_environment(environment())
    monkeypatch.setattr(app.AppConfig, "from_environment", lambda: config)

    @contextmanager
    def fail(_config):
        raise RuntimeError("test-only-sensitive-failure-value")
        yield

    monkeypatch.setattr(app, "build_worker", fail)
    assert app.main() != 0
    captured = capsys.readouterr()
    assert captured.out + captured.err == "azure_worker_failed\n"
    assert "test-only-sensitive" not in captured.out + captured.err
