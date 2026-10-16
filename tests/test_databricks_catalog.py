"""Databricks boundary contract; execute only on the approved Azure VM."""

import importlib
import importlib.util
import json
import math
import os
from dataclasses import replace
from urllib.parse import parse_qs, quote

import pytest

from rtmsf.interfaces import FatalExternalError, RetryableExternalError
from rtmsf.models import PartitionIntent, PartitionRef, TableDefinition
from rtmsf.pmcp import PartitionMutationCommitProtocol


WORKSPACE = "https://adb-1234567890123456.7.azuredatabricks.net"
TABLE = "proof.default.worker_probe"
SOURCE = "proof"
ROOT_PREFIX = "worker_probe/"
ROOT_LOCATION = "abfss://proof@proofaccount.dfs.core.windows.net/worker_probe/"
PARTITION = PartitionRef(SOURCE, TABLE, "dt=2026-10-15/region=west", ("2026-10-15", "west"), ROOT_PREFIX + "dt=2026-10-15/region=west/")
LOCATION = ROOT_LOCATION + PARTITION.partition_path + "/"


def adapter_module():
    assert importlib.util.find_spec("rtmsf.adapters.databricks") is not None, "Databricks catalog adapter not implemented"
    return importlib.import_module("rtmsf.adapters.databricks")


def metadata():
    return {
        "catalog_name": "proof", "schema_name": "default", "table_name": "worker_probe",
        "type": "EXTERNAL", "provider": "parquet", "location": ROOT_LOCATION,
        "partition_columns": ["dt", "region"],
        "columns": [
            {"name": "value", "type": {"name": "int"}, "nullable": True},
            {"name": "dt", "type": {"name": "string"}, "nullable": True},
            {"name": "region", "type": {"name": "string", "collation": "UTF8_BINARY"}, "nullable": True},
        ],
        "table_properties": {"partitionMetadataEnabled": "true"},
    }


def inline(rows=(), name="partition", statement_id="statement-1"):
    rows = list(rows)
    return {
        "statement_id": statement_id, "status": {"state": "SUCCEEDED"},
        "manifest": {
            "format": "JSON_ARRAY", "schema": {"column_count": 1, "columns": [{"name": name, "type_name": "STRING", "position": 0}]},
            "total_chunk_count": 1, "total_row_count": len(rows), "truncated": False,
            "chunks": [{"chunk_index": 0, "row_offset": 0, "row_count": len(rows)}],
        },
        "result": {"chunk_index": 0, "row_offset": 0, "row_count": len(rows), "data_array": rows},
    }


def describe(document=None):
    return inline([[json.dumps(metadata() if document is None else document)]], "json_metadata")


def terminal(state="SUCCEEDED", statement_id="mutation-1"):
    return {"statement_id": statement_id, "status": {"state": state}}


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        assert seconds >= 0
        self.now += seconds


class HTTP:
    def __init__(self, responses=(), *, clock=None, auth_delay=0, auth_response=None):
        self.responses = list(responses)
        self.calls = []
        self.clock = clock or Clock()
        self.auth_delay = auth_delay
        self.auth_response = auth_response

    def __call__(self, method, url, **kwargs):
        call = {"method": method, "url": url, **kwargs}
        self.calls.append(call)
        if url.endswith("/oidc/v1/token"):
            self.clock.sleep(self.auth_delay)
            if self.auth_response is not None:
                return self.auth_response
            return 200, {}, json.dumps({"access_token": "test-only-token", "token_type": "Bearer", "expires_in": 3600}).encode()
        if url.endswith("/cancel"):
            return 200, {}, b""
        assert self.responses, "unexpected extra SQL request"
        response = self.responses.pop(0)
        if callable(response):
            response = response(call)
        if isinstance(response, BaseException):
            raise response
        if isinstance(response, tuple):
            return response
        return 200, {}, json.dumps(response).encode()

    @property
    def statements(self):
        return [json.loads(call["body"])["statement"] for call in self.calls if call["method"] == "POST" and call["url"].endswith("/api/2.0/sql/statements")]

    @property
    def mutations(self):
        return [statement for statement in self.statements if statement.startswith("ALTER TABLE")]


def sql_client(tmp_path, http, **options):
    module = adapter_module()
    options = {
        "journal_path": tmp_path / "pending.json", "request": http,
        "clock": http.clock, "sleep": http.clock.sleep, "operation_seconds": 30,
        "max_response_bytes": 65536, "max_requests": 20, "max_polls": 12,
        **options,
    }
    return module.DatabricksSQLStatements(WORKSPACE, "warehouse-1", "test-only-client", "test-only-secret", **options)


def catalog(tmp_path, http, *, table_id=TABLE, **sql_options):
    statements = sql_client(tmp_path, http, **sql_options)
    result = adapter_module().DatabricksPartitionCatalog(statements, table_id=table_id, source_id=SOURCE, root_prefix=ROOT_PREFIX, root_location=ROOT_LOCATION)
    return statements, result


def resolved(tmp_path, http, **options):
    statements, result = catalog(tmp_path, http, **options)
    result.resolve_tables((TABLE,))
    return statements, result


def assert_stop(error):
    assert isinstance(error, RuntimeError)
    assert not isinstance(error, (FatalExternalError, RetryableExternalError))


class Store:
    def __init__(self, *, nonempty=True, clock=None, delay=0):
        self.nonempty = nonempty
        self.clock = clock
        self.delay = delay

    def prefix_non_empty(self, source_id, prefix):
        if self.clock:
            self.clock.sleep(self.delay)
        return self.nonempty

    def head_exists(self, source_id, key):
        return self.prefix_non_empty(source_id, key)

    def location_uri(self, source_id, prefix):
        return ROOT_LOCATION + quote(prefix[len(ROOT_PREFIX):], safe="/=")


def test_resolves_exact_external_parquet_metadata_once(tmp_path):
    http = HTTP([describe()])
    _, result = catalog(tmp_path, http)
    expected = (TableDefinition(TABLE, SOURCE, ROOT_PREFIX, ("dt", "region")),)
    assert result.resolve_tables((TABLE,)) == expected
    assert result.resolve_tables((TABLE,)) == expected
    assert http.statements == ["DESCRIBE TABLE EXTENDED `proof`.`default`.`worker_probe` AS JSON"]


@pytest.mark.parametrize("change", [
    {"catalog_name": "other"}, {"type": "MANAGED"}, {"provider": "delta"},
    {"location": ROOT_LOCATION + "other/"}, {"table_properties": {}},
    {"table_properties": {"partitionMetadataEnabled": "false"}},
    {"partition_columns": []}, {"partition_columns": ["dt", "dt"]},
    {"partition_columns": ["missing"]},
    {"columns": [{"name": "dt", "type": {"name": "date"}}, {"name": "region", "type": {"name": "string"}}]},
    {"columns": [{"name": "dt", "type": "string"}, {"name": "region", "type": {"name": "string"}}]},
    {"columns": [{"name": "dt", "type": {"name": "string", "collation": "UTF8_LCASE"}}, {"name": "region", "type": {"name": "string"}}]},
    {"columns": [{"name": "dt", "type": {"name": "string"}}, {"name": "dt", "type": {"name": "string"}}, {"name": "region", "type": {"name": "string"}}]},
])
def test_unsupported_or_incomplete_metadata_stops_startup(tmp_path, change):
    document = metadata()
    document.update(change)
    _, result = catalog(tmp_path, HTTP([describe(document)]))
    with pytest.raises(FatalExternalError):
        result.resolve_tables((TABLE,))


def test_component_identifiers_and_literals_are_quoted_independently(tmp_path):
    table_id = "pr`oof.default.worker_probe"
    document = metadata()
    document["catalog_name"] = "pr`oof"
    value = "a\\'b"
    target = replace(PARTITION, table_id=table_id, partition_path=f"dt={value}/region=west", values=(value, "west"), storage_prefix=f"{ROOT_PREFIX}dt={value}/region=west/")
    location = ROOT_LOCATION + quote(target.partition_path + "/", safe="/=")
    http = HTTP([describe(document), inline(), terminal(), inline([[target.partition_path]]), terminal()])
    _, result = catalog(tmp_path, http, table_id=table_id)
    result.resolve_tables((table_id,))
    assert result.create_partition(target, location) is True
    assert result.delete_partition(target) is True
    quoted = value.replace("\\", "\\\\").replace("'", "\\'")
    spec = f"(`dt` = '{quoted}', `region` = 'west')"
    assert http.statements[0] == "DESCRIBE TABLE EXTENDED `pr``oof`.`default`.`worker_probe` AS JSON"
    assert http.mutations == [
        f"ALTER TABLE `pr``oof`.`default`.`worker_probe` ADD IF NOT EXISTS PARTITION {spec} LOCATION '{location}'",
        f"ALTER TABLE `pr``oof`.`default`.`worker_probe` DROP IF EXISTS PARTITION {spec}",
    ]
    assert all("PURGE" not in statement for statement in http.statements)


@pytest.mark.parametrize("target,location", [
    (replace(PARTITION, source_id="other"), LOCATION),
    (replace(PARTITION, table_id="proof.default.other"), LOCATION),
    (replace(PARTITION, values=("2026-10-15",)), LOCATION),
    (replace(PARTITION, values=("west", "2026-10-15")), LOCATION),
    (replace(PARTITION, partition_path="dt=2026-10-15/region=other"), LOCATION),
    (replace(PARTITION, storage_prefix="elsewhere/" + PARTITION.partition_path + "/"), LOCATION),
    (PARTITION, LOCATION + "other/"),
])
def test_partition_identity_full_values_prefix_and_location_are_trust_boundaries(tmp_path, target, location):
    http = HTTP([describe()])
    _, result = resolved(tmp_path, http)
    with pytest.raises(FatalExternalError):
        result.create_partition(target, location)
    assert http.mutations == []
    assert len(http.statements) == 1


@pytest.mark.parametrize("value", ["bad\x00value", "bad\nvalue", "bad\ud800value"])
def test_controls_and_invalid_utf8_cannot_enter_partition_sql(tmp_path, value):
    target = replace(PARTITION, values=(value, "west"), partition_path=f"dt={value}/region=west", storage_prefix=f"{ROOT_PREFIX}dt={value}/region=west/")
    http = HTTP([describe()])
    _, result = resolved(tmp_path, http)
    with pytest.raises(FatalExternalError):
        result.delete_partition(target)
    assert http.mutations == []


@pytest.mark.parametrize("chunk_count", [0, 1])
def test_only_complete_zero_rows_prove_absence_with_full_partition_spec(tmp_path, chunk_count):
    document = inline()
    if chunk_count == 0:
        document["manifest"]["total_chunk_count"] = 0
        document["manifest"]["chunks"] = []
        document["result"] = {}
    http = HTTP([describe(), document])
    _, result = resolved(tmp_path, http)
    assert result.partition_exists(PARTITION) is False
    assert http.statements[-1] == "SHOW PARTITIONS `proof`.`default`.`worker_probe` PARTITION (`dt` = '2026-10-15', `region` = 'west')"


@pytest.mark.parametrize("defect", ["truncated", "row_count", "column_count", "chunk_count", "next_chunk_index", "next_chunk_internal_link", "external", "closed", "missing_manifest"])
def test_incomplete_statement_read_never_becomes_absence(tmp_path, defect):
    document = inline()
    if defect == "truncated":
        document["manifest"]["truncated"] = True
    elif defect == "row_count":
        document["manifest"]["total_row_count"] = 1
    elif defect == "column_count":
        document["manifest"]["schema"]["column_count"] = 2
    elif defect == "chunk_count":
        document["manifest"]["total_chunk_count"] = 2
    elif defect == "next_chunk_index":
        document["result"]["next_chunk_index"] = 1
    elif defect == "next_chunk_internal_link":
        document["result"]["next_chunk_internal_link"] = "/api/2.0/sql/statements/statement-1/result/chunks/1"
    elif defect == "external":
        document["result"] = {"external_links": [{"external_link": "https://untrusted.invalid/result"}]}
    elif defect == "closed":
        document["status"]["state"] = "CLOSED"
    else:
        document.pop("manifest")
    http = HTTP([describe(), document])
    _, result = resolved(tmp_path, http)
    with pytest.raises((FatalExternalError, RetryableExternalError)):
        result.partition_exists(PARTITION)
    assert http.mutations == []
    assert all(call["url"].startswith(WORKSPACE + "/") for call in http.calls)


def test_complete_inline_partition_result_is_positive_evidence(tmp_path):
    http = HTTP([describe(), inline([[PARTITION.partition_path]])])
    _, result = resolved(tmp_path, http)
    assert result.partition_exists(PARTITION) is True


def test_single_writer_observed_mutation_boolean_and_idempotent_ddl(tmp_path):
    http = HTTP([describe(), inline(), terminal(), inline([[PARTITION.partition_path]]), inline([[PARTITION.partition_path]]), terminal(), inline()])
    _, result = resolved(tmp_path, http)
    assert result.create_partition(PARTITION, LOCATION) is True
    assert result.create_partition(PARTITION, LOCATION) is False
    assert result.delete_partition(PARTITION) is True
    assert result.delete_partition(PARTITION) is False
    assert len(http.mutations) == 2
    assert " ADD IF NOT EXISTS PARTITION " in http.mutations[0]
    assert " DROP IF EXISTS PARTITION " in http.mutations[1]
    assert not (tmp_path / "pending.json").exists()


@pytest.mark.parametrize("workspace", [
    WORKSPACE.replace("https:", "http:"), "https://databricks.attacker.invalid",
    WORKSPACE.replace("https://", "https://user@"), WORKSPACE + ":443",
    WORKSPACE + "/api", WORKSPACE + "?host=other",
])
def test_workspace_must_be_a_trusted_bare_https_host(tmp_path, workspace):
    http = HTTP()
    module = adapter_module()
    with pytest.raises((ValueError, FatalExternalError)):
        module.DatabricksSQLStatements(workspace, "warehouse-1", "test-only-client", "test-only-secret", journal_path=tmp_path / "pending.json", request=http)
    assert http.calls == []


@pytest.mark.parametrize("options", [
    {"operation_seconds": math.inf}, {"operation_seconds": 119},
    {"operation_seconds": 0}, {"max_response_bytes": 0}, {"max_requests": 0}, {"max_polls": 0},
])
def test_statement_limits_are_finite_positive_and_fit_below_lease_allowance(tmp_path, options):
    http = HTTP()
    with pytest.raises((ValueError, FatalExternalError)):
        sql_client(tmp_path, http, **options)
    assert http.calls == []


def test_oauth_sql_scope_token_cache_and_bounded_inline_request(tmp_path):
    http = HTTP([inline(), inline(), inline()])
    statements = sql_client(tmp_path, http)
    statements.execute("SELECT 1")
    statements.execute("SELECT 2")
    auth, = [call for call in http.calls if call["url"].endswith("/oidc/v1/token")]
    form = parse_qs(auth["body"].decode())
    assert form["grant_type"] == ["client_credentials"]
    assert form["scope"] == ["sql"]
    for call in http.calls:
        assert call["allow_redirects"] is False
        assert 0 < call["connect_timeout"] <= 30
        assert 0 < call["read_timeout"] <= 30
        assert call["connect_timeout"] + call["read_timeout"] <= 30
        assert call["max_response_bytes"] == 65536
    for call in http.calls[1:]:
        body = json.loads(call["body"])
        assert body["warehouse_id"] == "warehouse-1"
        assert body["disposition"] == "INLINE"
        assert body["format"] == "JSON_ARRAY"
        assert body["byte_limit"] <= 65536
    http.clock.sleep(3601)
    statements.execute("SELECT 3")
    assert len([call for call in http.calls if call["url"].endswith("/oidc/v1/token")]) == 2


def test_initial_authentication_consumes_the_operation_deadline(tmp_path):
    http = HTTP([inline()], auth_delay=11)
    statements = sql_client(tmp_path, http, operation_seconds=10)
    with pytest.raises(RetryableExternalError):
        statements.execute("SELECT 1")
    assert http.statements == []


@pytest.mark.parametrize("response", [
    (302, {"Location": "https://untrusted.invalid/statement"}, b"{}"),
    (200, {}, b"x" * 65537), (200, {}, b"\xff"),
])
def test_redirect_oversize_and_invalid_utf8_response_cannot_be_read_evidence(tmp_path, response):
    http = HTTP([response])
    with pytest.raises((FatalExternalError, RetryableExternalError)):
        sql_client(tmp_path, http).execute("SELECT 1")
    assert len(http.statements) == 1
    assert all(call["url"].startswith(WORKSPACE + "/") for call in http.calls)


def test_poll_and_total_request_limits_stop_a_nonterminal_read(tmp_path):
    http = HTTP([terminal("PENDING", "read-1")] * 10)
    statements = sql_client(tmp_path, http, max_polls=2, max_requests=5)
    with pytest.raises(RetryableExternalError):
        statements.execute("SELECT 1")
    assert len(http.calls) <= 5
    assert len([call for call in http.calls if call["method"] == "GET"]) <= 2
    assert http.clock.now <= 30


def test_one_operation_deadline_covers_pmcp_reads_storage_and_mutation(tmp_path):
    clock = Clock()
    def slow_read(call):
        clock.sleep(6)
        return inline()
    http = HTTP([describe(), slow_read], clock=clock)
    statements, result = resolved(tmp_path, http, operation_seconds=10)
    pmcp = PartitionMutationCommitProtocol(Store(clock=clock, delay=6), result)
    with statements.operation():
        outcome = pmcp.apply(PartitionIntent(PARTITION, "ADD"))
    assert outcome.status == "RETRY"
    assert http.mutations == []


def test_atomic_fsynced_marker_precedes_post_and_id_precedes_poll(tmp_path, monkeypatch):
    events = []
    real_fsync, real_replace = os.fsync, os.replace
    def fsync(fd):
        events.append("fsync")
        return real_fsync(fd)
    def replace_file(source, destination):
        events.append("replace")
        return real_replace(source, destination)
    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "replace", replace_file)
    marker = tmp_path / "pending.json"
    def submitted(call):
        assert marker.exists()
        assert "replace" in events and events.count("fsync") >= 2
        text = marker.read_text()
        assert "test-only-token" not in text and "test-only-secret" not in text
        assert json.loads(text).get("statement_id") is None
        return terminal("PENDING", "mutation-1")
    def polled(call):
        assert call["method"] == "GET"
        assert json.loads(marker.read_text())["statement_id"] == "mutation-1"
        return terminal()
    http = HTTP([submitted, polled])
    sql_client(tmp_path, http).execute("ALTER TABLE `proof`.`default`.`worker_probe` DROP IF EXISTS PARTITION (`dt` = 'x', `region` = 'west')", mutation=True)
    assert not marker.exists()


def test_missing_statement_id_is_not_retried_and_blocks_restart_and_pmcp(tmp_path):
    http = HTTP([describe(), inline(), inline(), TimeoutError("response lost after mutation POST")])
    statements, result = resolved(tmp_path, http)
    pmcp = PartitionMutationCommitProtocol(Store(), result)
    with pytest.raises(RuntimeError) as caught:
        pmcp.apply(PartitionIntent(PARTITION, "ADD"))
    assert_stop(caught.value)
    assert len(http.mutations) == 1
    assert (tmp_path / "pending.json").exists()
    restarted_http = HTTP()
    restarted = sql_client(tmp_path, restarted_http)
    with pytest.raises(RuntimeError) as caught:
        restarted.recover()
    assert_stop(caught.value)
    with pytest.raises(RuntimeError) as caught:
        restarted.execute("ALTER TABLE `proof`.`default`.`worker_probe` DROP IF EXISTS PARTITION (`dt` = 'x', `region` = 'west')", mutation=True)
    assert_stop(caught.value)
    assert restarted_http.calls == []


class SimulatedCrash(BaseException):
    pass


def pending_journal(tmp_path):
    http = HTTP([terminal("PENDING"), SimulatedCrash()])
    with pytest.raises(SimulatedCrash):
        sql_client(tmp_path, http).execute("ALTER TABLE `proof`.`default`.`worker_probe` DROP IF EXISTS PARTITION (`dt` = 'x', `region` = 'west')", mutation=True)
    assert json.loads((tmp_path / "pending.json").read_text())["statement_id"] == "mutation-1"


@pytest.mark.parametrize("state", ["SUCCEEDED", "CLOSED"])
def test_restart_known_id_confirms_terminal_before_new_statement(tmp_path, state):
    pending_journal(tmp_path)
    http = HTTP([terminal("RUNNING"), terminal(state), inline()])
    statements = sql_client(tmp_path, http)
    statements.recover()
    assert not (tmp_path / "pending.json").exists()
    statements.execute("SELECT 1")
    statement_calls = [call for call in http.calls if "/api/2.0/sql/statements" in call["url"]]
    assert statement_calls[0]["method"] == "GET"
    assert len([call for call in statement_calls if call["method"] == "GET"]) == 2
    assert statement_calls[-1]["method"] == "POST"
    assert http.mutations == []


def test_cancel_receipt_without_terminal_proof_retains_marker_and_stops(tmp_path):
    pending_journal(tmp_path)
    http = HTTP([terminal("RUNNING")] * 10)
    with pytest.raises(RuntimeError) as caught:
        sql_client(tmp_path, http, max_polls=2, max_requests=5).recover()
    assert_stop(caught.value)
    assert any(call["url"].endswith("/cancel") for call in http.calls)
    assert (tmp_path / "pending.json").exists()
    assert len(http.calls) <= 5
    assert http.mutations == []


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED", "CANCELED", "CLOSED"])
def test_known_terminal_cleanup_permits_storage_authoritative_replay(tmp_path, state):
    pending_journal(tmp_path)
    http = HTTP([terminal(state), describe(), inline(), inline(), terminal()])
    statements, result = catalog(tmp_path, http)
    statements.recover()
    result.resolve_tables((TABLE,))
    pmcp = PartitionMutationCommitProtocol(Store(), result)
    with statements.operation():
        assert pmcp.apply(PartitionIntent(PARTITION, "ADD")).status == "APPLIED"
    assert len(http.mutations) == 1
    assert " ADD IF NOT EXISTS " in http.mutations[0]
    assert not (tmp_path / "pending.json").exists()


def test_execute_without_recovery_cannot_overtake_an_unresolved_statement(tmp_path):
    pending_journal(tmp_path)
    http = HTTP()
    with pytest.raises(RuntimeError) as caught:
        sql_client(tmp_path, http).execute("ALTER TABLE `proof`.`default`.`worker_probe` DROP IF EXISTS PARTITION (`dt` = 'x', `region` = 'west')", mutation=True)
    assert_stop(caught.value)
    assert http.calls == []


def test_journal_write_failure_prevents_mutation_post(tmp_path, monkeypatch):
    def disk_failure(source, destination):
        raise OSError("disk unavailable")
    monkeypatch.setattr(os, "replace", disk_failure)
    http = HTTP([terminal()])
    with pytest.raises(RuntimeError) as caught:
        sql_client(tmp_path, http).execute("ALTER TABLE `proof`.`default`.`worker_probe` DROP IF EXISTS PARTITION (`dt` = 'x', `region` = 'west')", mutation=True)
    assert_stop(caught.value)
    assert http.mutations == []


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("phase", ["oauth", "sql"])
def test_revoked_auth_or_access_stops_pmcp_without_poison_dlq(tmp_path, status, phase):
    http = HTTP([describe(), (status, {}, b"{}")])
    _, result = resolved(tmp_path, http)
    if phase == "oauth":
        http.clock.sleep(3601)
        http.auth_response = (status, {}, b"{}")
    with pytest.raises(RuntimeError) as caught:
        PartitionMutationCommitProtocol(Store(), result).apply(PartitionIntent(PARTITION, "ADD"))
    assert_stop(caught.value)
    assert http.mutations == []


@pytest.mark.parametrize("mutation", [False, True])
def test_poll_statement_identity_cannot_change(tmp_path, mutation):
    http = HTTP([terminal("PENDING", "original-id"), terminal("SUCCEEDED", "other-id")])
    statements = sql_client(tmp_path, http)
    with pytest.raises(RuntimeError) as caught:
        statements.execute("SELECT 1", mutation=mutation)
    if mutation:
        assert_stop(caught.value)
        assert json.loads((tmp_path / "pending.json").read_text())["statement_id"] == "original-id"
    else:
        assert isinstance(caught.value, RetryableExternalError)
    assert http.calls[-1]["url"].endswith("/original-id")


def test_request_body_limit_precedes_auth_and_journal(tmp_path):
    http = HTTP()
    with pytest.raises(FatalExternalError):
        sql_client(tmp_path, http).execute("SELECT '" + "x" * 65536 + "'", mutation=True)
    assert http.calls == []
    assert not (tmp_path / "pending.json").exists()


def test_default_transport_keeps_socket_for_connection_close_response(monkeypatch):
    module = adapter_module()
    timeouts, closed = [], []
    class Peer:
        closed = False

        def settimeout(self, seconds):
            if self.closed:
                raise OSError("closed socket")
            timeouts.append(seconds)

    class Response:
        status = 200
        payload = b"complete"

        def __init__(self, peer):
            self.peer = peer

        def isclosed(self):
            return not self.payload

        def close(self):
            self.peer.closed = True

        def getheader(self, name):
            return "8"

        def getheaders(self):
            return [("Connection", "close")]

        def read1(self, limit):
            result, self.payload = self.payload[:limit], self.payload[limit:]
            if not self.payload:
                self.peer.closed = True
            return result

    class Connection:
        def __init__(self, host, **options):
            self.sock = Peer()

        def connect(self):
            pass

        def request(self, *args, **kwargs):
            pass

        def getresponse(self):
            response = Response(self.sock)
            self.sock = None
            return response

        def close(self):
            closed.append(True)

    monkeypatch.setattr(module.http.client, "HTTPSConnection", Connection)
    monkeypatch.setattr(module.time, "monotonic", lambda: 0)
    status, headers, body = module._https_request("GET", WORKSPACE + "/read", headers={}, body=None,
        connect_timeout=2, read_timeout=8, max_response_bytes=64, allow_redirects=False)
    assert (status, headers, body) == (200, {"Connection": "close"}, b"complete")
    assert timeouts and all(0 < timeout <= 8 for timeout in timeouts)
    assert closed == [True]
