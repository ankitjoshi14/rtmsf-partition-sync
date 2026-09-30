from dataclasses import fields

import pytest

from rtmsf import aws_app as app
from tests.test_aws_adapters import Client, Telemetry, message, source, table


def environment(**overrides):
    return {"EVENT_QUEUE_URL": "event-queue", "DEAD_LETTER_QUEUE_URL": "dead-queue", "RTMSF_SOURCES": '{"warehouse":"data-bucket"}', "RTMSF_TABLE_IDS": '["db.one"]', **overrides}


def test_environment_uses_finalized_contract_and_runtime_defaults():
    config = app.AppConfig.from_environment(environment())
    assert dict(config.sources) == {"warehouse": "data-bucket"}
    assert config.table_ids == ("db.one",)
    assert config.settling_ms == 10000
    assert config.maximum_ms == 31000
    assert config.queue_visibility_seconds == 180
    assert not any("experiment" in field.name for field in fields(config))
    assert not {"window_ms", "grace_ms", "table_prefix", "partition_columns", "data_bucket", "glue_database", "glue_table"}.intersection(field.name for field in fields(config))


@pytest.mark.parametrize("overrides", [{"RTMSF_SOURCES": "broken"}, {"RTMSF_SOURCES": "[]"}, {"RTMSF_SOURCES": "{}"}, {"RTMSF_SOURCES": '{"a":"same","b":"same"}'}, {"RTMSF_SOURCES": '{"a":12}'}, {"RTMSF_TABLE_IDS": '"db.one"'}, {"RTMSF_TABLE_IDS": "[]"}, {"RTMSF_TABLE_IDS": '["table"]'}, {"RTMSF_TABLE_IDS": '["db.one","db.one"]'}, {"SETTLING_MS": "0"}, {"MAXIMUM_MS": "5999"}, {"QUEUE_VISIBILITY_SECONDS": "1"}])
def test_invalid_config_fails_before_starting_aws_clients(overrides):
    with pytest.raises(ValueError):
        app.AppConfig.from_environment(environment(**overrides))


def test_missing_contract_config_is_reported_by_name():
    with pytest.raises(ValueError, match="RTMSF_SOURCES, RTMSF_TABLE_IDS"):
        app.AppConfig.from_environment({"EVENT_QUEUE_URL": "q", "DEAD_LETTER_QUEUE_URL": "d"})


def build(monkeypatch, config, *, glue=None, s3=None):
    clients = []
    glue = glue or Client(get_table=[table()])
    s3 = s3 or Client()

    def client(name, **kwargs):
        instance = {"s3": s3, "glue": glue}.get(name, Client())
        clients.append((name, kwargs, instance))
        return instance

    capture = {}
    monkeypatch.setattr(app.boto3, "client", client)
    monkeypatch.setattr(app, "RTMSFWorker", lambda **kwargs: capture.update(kwargs) or capture)
    monkeypatch.setattr(app, "JsonTelemetry", Telemetry)
    result = app.build_worker(config)
    return result, clients, glue, s3


def test_build_resolves_glue_routing_once_and_bounds_all_clients(monkeypatch):
    result, clients, glue, _ = build(monkeypatch, app.AppConfig.from_environment(environment()))
    assert len(glue.calls["get_table"]) == 1
    assert sorted(name for name, _, _ in clients) == ["glue", "s3", "sqs", "sqs"]
    for _, options, _ in clients:
        sdk = options["config"]
        assert sdk.connect_timeout == 1
        assert sdk.retries == {"mode": "standard", "total_max_attempts": 1}
        assert sdk.read_timeout in {2, 22}
    assert result["settling_ms"] == 10000
    assert result["maximum_ms"] == 31000
    assert result["queue_lease_seconds"] == 180
    assert result["pmcp_budget_ms"] == 18000
    assert result["source_call_budget_ms"] == 56000
    assert result["lease_safety_margin_ms"] == 5000
    assert result["max_pending_deliveries"] == 100
    assert "after_buffered" not in result
    assert result["routing"] is not None
    assert "table" not in result


def test_real_worker_composition_can_poll_without_contract_aliases(monkeypatch):
    glue = Client(get_table=[table()])
    queue = Client()
    monkeypatch.setattr(app.boto3, "client", lambda name, **kwargs: {"glue": glue, "sqs": queue}.get(name, Client()))
    worker = app.build_worker(app.AppConfig.from_environment(environment()))
    worker.run_once()
    assert queue.calls["receive_message"][0]["VisibilityTimeout"] == 180


def test_source_budget_covers_nonwaiting_poll_and_ten_full_size_poison_messages(monkeypatch):
    result, _, _, _ = build(monkeypatch, app.AppConfig.from_environment(environment()))
    elapsed_ms = 0

    class TimedClient(Client):
        def __getattr__(self, name):
            operation = super().__getattr__(name)

            def call(**request):
                nonlocal elapsed_ms
                elapsed_ms += 23_000 if name == "receive_message" else 3000
                return operation(**request)
            return call

    body = '"' * 1_048_576
    client = TimedClient(receive_message=[{"Messages": [message(body, index) for index in range(10)]}])
    assert source(client).receive(10, 0, 180) == ()
    assert client.calls["receive_message"][0]["WaitTimeSeconds"] == 0
    assert len(client.calls["send_message_batch"]) == 10
    assert len(client.calls["delete_message_batch"]) == 1
    assert elapsed_ms == 56_000
    assert result["source_call_budget_ms"] >= elapsed_ms
    assert result["queue_lease_seconds"] == 180
    assert 3 * result["source_call_budget_ms"] + result["lease_safety_margin_ms"] < 180_000


def test_normal_application_ignores_fault_settings(monkeypatch):
    config = app.AppConfig.from_environment(environment(
        RTMSF_EXPERIMENT_FAULT="malformed experiment configuration",
        RTMSF_EXPERIMENT_CRASH_MARKER_SOURCE_ID="unknown",
        RTMSF_EXPERIMENT_CRASH_MARKER_KEY="root/not-a-marker",
    ))
    result, _, _, s3 = build(monkeypatch, config)
    assert "after_buffered" not in result
    assert not s3.calls["put_object"]


def test_pmcp_factory_composes_with_real_adapters_and_shared_telemetry(monkeypatch):
    from rtmsf.adapters.aws import GluePartitionCatalog, S3ObjectStore
    from rtmsf.pmcp import PartitionMutationCommitProtocol

    monkeypatch.setattr(app.boto3, "client", lambda name, **kwargs: Client(get_table=[table()]))
    observed = []
    telemetry = Telemetry()

    def factory(storage, catalog):
        observed.append((storage, catalog))
        return PartitionMutationCommitProtocol(storage, catalog)

    worker = app.build_worker(app.AppConfig.from_environment(environment()), pmcp_factory=factory, telemetry=telemetry)
    worker.run_once()
    assert len(observed) == 1
    assert isinstance(observed[0][0], S3ObjectStore)
    assert isinstance(observed[0][1], GluePartitionCatalog)
    assert any(item.get("operation") == "SQS.ReceiveMessage" for item in telemetry.records)
