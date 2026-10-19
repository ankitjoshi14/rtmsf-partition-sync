"""Offline Hive catalog boundary using the pinned generated Thrift structs."""
from copy import deepcopy

import pytest
from pymetastore.hive_metastore import ttypes

from rtmsf.interfaces import FatalExternalError, RetryableExternalError
from rtmsf.models import PartitionRef, TableDefinition


OWNER_KEY = "gcp_probe_run_id"
TABLE_ID = "gcp_probe_run.catalog_probe"
ROOT = "gcp-live-probe/run/"
LOCATION = "gs://proof-bucket/" + ROOT[:-1]


def table():
    return ttypes.Table(
        dbName="gcp_probe_run", tableName="catalog_probe", tableType="EXTERNAL_TABLE",
        parameters={OWNER_KEY: "run", "EXTERNAL": "TRUE", "external.table.purge": "false"},
        partitionKeys=[ttypes.FieldSchema("dt", "string", "partition")],
        sd=ttypes.StorageDescriptor(
            cols=[ttypes.FieldSchema("value", "string", "data")], location=LOCATION,
            inputFormat="parquet-input", outputFormat="parquet-output", compressed=False,
            numBuckets=-1, bucketCols=[], sortCols=[], parameters={},
            serdeInfo=ttypes.SerDeInfo(name="catalog_probe", serializationLib="parquet-serde", parameters={}),
        ), writeId=None,
    )


def ref(value="a"):
    path = "dt=" + value
    return PartitionRef("proof", TABLE_ID, path, (value,), ROOT + path + "/")


class Socket:
    def __init__(self):
        self.timeouts = []

    def setTimeout(self, ms):
        self.timeouts.append(ms)


class Transport:
    def __init__(self, server, sock, index):
        self.server, self.sock, self.index = server, sock, index
        self.closed = False

    def open(self):
        self.server.record(self, "open", ())

    def close(self):
        self.closed = True


class Server:
    """Stateful offline provider: failures may happen before or after mutations."""
    def __init__(self, clock=None):
        self.database = ttypes.Database(name="gcp_probe_run", parameters={OWNER_KEY: "run"})
        self.table = table()
        self.partitions = {}
        self.calls, self.connections = [], []
        self.hooks = {}
        self.clock = clock

    def record(self, transport, method, args):
        assert not transport.closed
        self.calls.append((transport.index, method, args, transport.sock.timeouts[-1]))
        hook = self.hooks.get(method)
        if hook:
            hook(*args)

    def factory(self, host, port):
        assert (host, port) == ("10.76.0.4", 9083)
        sock = Socket()
        transport = Transport(self, sock, len(self.connections))
        self.connections.append(transport)
        server = self

        class Client:
            def get_database(self, db):
                server.record(transport, "get_database", (db,))
                if server.database is None:
                    raise ttypes.NoSuchObjectException("private DB")
                return deepcopy(server.database)

            def get_table(self, db, name):
                server.record(transport, "get_table", (db, name))
                if server.table is None:
                    raise ttypes.NoSuchObjectException("private table")
                return deepcopy(server.table)

            def get_partition(self, db, name, values):
                server.record(transport, "get_partition", (db, name, values))
                if tuple(values) not in server.partitions:
                    raise ttypes.NoSuchObjectException("private partition")
                return deepcopy(server.partitions[tuple(values)])

            def add_partition(self, partition):
                server.record(transport, "add_partition", (partition,))
                key = tuple(partition.values)
                if key in server.partitions:
                    raise ttypes.AlreadyExistsException("private existing")
                server.partitions[key] = deepcopy(partition)
                return deepcopy(partition)

            def drop_partition(self, db, name, values, delete_data):
                server.record(transport, "drop_partition", (db, name, values, delete_data))
                if tuple(values) not in server.partitions:
                    raise ttypes.NoSuchObjectException("private missing")
                del server.partitions[tuple(values)]
                return True

        return sock, transport, Client()


def catalog(server=None, **kwargs):
    from rtmsf.adapters.gcp import HiveMetastorePartitionCatalog

    server = Server() if server is None else server
    options = dict(table_id=TABLE_ID, source_id="proof", bucket="proof-bucket",
                   root_prefix=ROOT, partition_columns=(("dt", "string"),), owner="run",
                   connection_factory=server.factory)
    options.update(kwargs)
    return HiveMetastorePartitionCatalog(options.pop("endpoint", "thrift://10.76.0.4:9083"), **options), server


def resolved(server=None, **kwargs):
    result, server = catalog(server, **kwargs)
    assert result.resolve_tables((TABLE_ID,)) == (TableDefinition(TABLE_ID, "proof", ROOT, ("dt",)),)
    return result, server


def mutations(server):
    return [call for call in server.calls if call[1] in ("add_partition", "drop_partition")]


def test_resolve_existing_owned_table_without_table_lifecycle_calls():
    result, server = resolved()
    assert [call[1] for call in server.calls] == ["open", "get_database", "get_table"]
    assert all(connection.closed for connection in server.connections)
    assert result.partition_exists(ref()) is False
    assert mutations(server) == []


@pytest.mark.parametrize("endpoint", [
    "10.76.0.4:9083", "thrift://localhost:9083", "thrift://8.8.8.8:9083",
    "thrift://127.0.0.1:9083", "thrift://169.254.1.1:9083", "thrift://[::1]:9083",
    "thrift://10.76.0.4:9084", "thrift://user:secret@10.76.0.4:9083",
    "thrift://10.76.0.4:9083/", "thrift://10.76.0.4:9083?q=secret",
    "thrift://10.76.0.4:9083#secret", "thrift://010.76.0.4:9083", None,
])
def test_endpoint_requires_exact_private_numeric_ipv4_thrift(endpoint):
    server = Server()
    with pytest.raises(ValueError) as caught:
        catalog(server, endpoint=endpoint)
    assert "secret" not in str(caught.value)
    assert server.connections == []


@pytest.mark.parametrize("option,value", [
    ("table_id", "a.b.c"), ("table_id", "a/b.table"), ("source_id", ""),
    ("bucket", "gs://proof-bucket"), ("owner", ""), ("owner", "x\ud800"),
    ("root_prefix", ""), ("root_prefix", "table"), ("root_prefix", "/table/"),
    ("root_prefix", "table//"), ("root_prefix", "table/../"),
    ("partition_columns", ()), ("partition_columns", (("dt", ""),)),
    ("partition_columns", (("dt", "string"), ("dt", "string"))),
    ("partition_columns", (("d/t", "string"),)),
])
def test_invalid_approved_configuration_never_connects(option, value):
    server = Server()
    with pytest.raises(ValueError):
        catalog(server, **{option: value})
    assert server.connections == []


def test_only_exact_approved_table_can_resolve():
    result, server = catalog()
    with pytest.raises(FatalExternalError):
        result.resolve_tables((TABLE_ID, "other.table"))
    assert server.connections == []


@pytest.mark.parametrize("target,attribute,value", [
    ("database", "name", "other"), ("database", "parameters", {OWNER_KEY: "other"}),
    ("table", "dbName", "other"), ("table", "tableName", "other"),
    ("table", "tableType", "MANAGED_TABLE"),
    ("table", "parameters", {OWNER_KEY: "run", "EXTERNAL": "TRUE"}),
    ("table", "parameters", {OWNER_KEY: "run", "EXTERNAL": "TRUE", "external.table.purge": "true"}),
    ("table", "parameters", {OWNER_KEY: "other", "EXTERNAL": "TRUE", "external.table.purge": "false"}),
    ("table", "parameters", {OWNER_KEY: "run", "EXTERNAL": "FALSE", "external.table.purge": "false"}),
    ("table", "partitionKeys", [ttypes.FieldSchema("other", "string")]),
    ("table", "partitionKeys", [ttypes.FieldSchema("dt", "int")]),
    ("descriptor", "location", LOCATION + "/"),
    ("descriptor", "location", "gs://other-bucket/" + ROOT[:-1]),
    ("descriptor", "inputFormat", "changed"),
    ("descriptor", "serdeInfo", ttypes.SerDeInfo(serializationLib="changed")),
])
def test_drift_after_resolution_refuses_lookup_and_mutation(target, attribute, value):
    result, server = resolved()
    object_ = server.table.sd if target == "descriptor" else getattr(server, target)
    setattr(object_, attribute, value)
    for action in (lambda: result.resolve_tables((TABLE_ID,)), lambda: result.partition_exists(ref()),
                   lambda: result.create_partition(ref(), LOCATION + "/dt=a"), lambda: result.delete_partition(ref())):
        with pytest.raises(FatalExternalError):
            action()
    assert mutations(server) == []


@pytest.mark.parametrize("missing", ["database", "table"])
def test_missing_database_or_table_is_configuration_failure_not_partition_absence(missing):
    result, server = resolved()
    setattr(server, missing, None)
    with pytest.raises(FatalExternalError) as caught:
        result.partition_exists(ref())
    assert "private" not in str(caught.value)
    assert not any(call[1] == "get_partition" for call in server.calls)


@pytest.mark.parametrize("field,value", [
    ("source_id", "other"), ("table_id", "other.table"), ("values", ()),
    ("values", ("a", "b")), ("values", ["a"]), ("values", ("",)),
    ("values", ("x\ud800",)), ("values", ("a/b",)),
    ("partition_path", "dt=b"), ("partition_path", "dt=%61"),
    ("storage_prefix", ROOT + "dt=a//"), ("storage_prefix", ROOT + "dt=a"),
    ("storage_prefix", "other/dt=a/"),
])
def test_untrusted_partition_identity_fails_before_connection(field, value):
    from dataclasses import replace
    result, server = resolved()
    count = len(server.connections)
    bad = replace(ref(), **{field: value})
    for action in (lambda: result.partition_exists(bad), lambda: result.create_partition(bad, LOCATION + "/dt=a"),
                   lambda: result.delete_partition(bad)):
        with pytest.raises(FatalExternalError):
            action()
    assert len(server.connections) == count


def test_unresolved_table_cannot_be_used():
    result, server = catalog()
    with pytest.raises(FatalExternalError):
        result.partition_exists(ref())
    assert server.connections == []


@pytest.mark.parametrize("value", ["a+% snow雪#?", "%20%2F%2E%2E", ".", "..", "a=b"])
def test_raw_partition_values_publish_exact_hadoop_locations(value):
    result, server = resolved()
    partition = ref(value)
    expected = LOCATION + "/dt=" + value
    assert result.create_partition(partition, expected) is True
    actual = server.partitions[(value,)]
    assert actual.sd.location == expected
    assert actual.values == [value]
    assert actual.writeId is None
    assert result.partition_exists(partition) is True
    assert result.create_partition(partition, expected) is False
    assert len(mutations(server)) == 1


def test_partition_column_order_controls_values_and_path():
    server = Server()
    server.table.partitionKeys = [ttypes.FieldSchema("region", "string"), ttypes.FieldSchema("dt", "string")]
    result, server = catalog(server, partition_columns=(("region", "string"), ("dt", "string")))
    result.resolve_tables((TABLE_ID,))
    good = PartitionRef("proof", TABLE_ID, "region=west/dt=a", ("west", "a"), ROOT + "region=west/dt=a/")
    assert result.create_partition(good, LOCATION + "/region=west/dt=a")
    assert server.partitions[("west", "a")].values == ["west", "a"]
    bad = PartitionRef("proof", TABLE_ID, "dt=a/region=west", ("a", "west"), ROOT + "dt=a/region=west/")
    with pytest.raises(FatalExternalError):
        result.partition_exists(bad)


def test_location_mismatch_fails_before_mutation():
    result, server = resolved()
    for location in (LOCATION + "/dt=%61", LOCATION + "/dt=a/", "gs://other-bucket/" + ROOT + "dt=a", None):
        with pytest.raises(FatalExternalError):
            result.create_partition(ref(), location)
    assert mutations(server) == []


def existing(server):
    sd = deepcopy(server.table.sd)
    sd.location = LOCATION + "/dt=a"
    server.partitions[("a",)] = ttypes.Partition(values=["a"], dbName="gcp_probe_run", tableName="catalog_probe",
                                               sd=sd, parameters={OWNER_KEY: "run"}, writeId=None)


@pytest.mark.parametrize("field,value", [
    ("dbName", "other"), ("tableName", "other"), ("values", ["b"]),
    ("parameters", {}), ("parameters", {OWNER_KEY: "other"}), ("sd", None),
    ("location", LOCATION + "/dt=b"), ("inputFormat", "changed"),
])
def test_conflicting_existing_partition_is_neither_noop_nor_droppable(field, value):
    result, server = resolved()
    existing(server)
    part = server.partitions[("a",)]
    setattr(part.sd if field in ("location", "inputFormat") else part, field, value)
    for action in (lambda: result.partition_exists(ref()), lambda: result.create_partition(ref(), LOCATION + "/dt=a"),
                   lambda: result.delete_partition(ref())):
        with pytest.raises(FatalExternalError):
            action()
    assert mutations(server) == []


def test_add_and_drop_have_fresh_guarded_readbacks_and_preserve_table_descriptor():
    result, server = resolved()
    baseline = deepcopy(server.table.sd)
    assert result.create_partition(ref(), LOCATION + "/dt=a") is True
    add = mutations(server)[0]
    assert add[2][0].sd == server.partitions[("a",)].sd
    assert server.table.sd == baseline
    assert [call[1] for call in server.calls[-3:]] == ["get_partition", "get_database", "get_table"]
    assert server.calls[-1][0] != add[0]
    assert result.delete_partition(ref()) is True
    drop = mutations(server)[1]
    assert drop[2] == ("gcp_probe_run", "catalog_probe", ["a"], False)
    assert server.calls[-1][0] != drop[0]
    assert result.delete_partition(ref()) is False
    assert len(mutations(server)) == 2
    assert all(connection.closed for connection in server.connections)


def test_already_exists_race_requires_fresh_validated_readback():
    result, server = resolved()
    def race(partition):
        server.partitions[tuple(partition.values)] = deepcopy(partition)
    server.hooks["add_partition"] = race
    assert result.create_partition(ref(), LOCATION + "/dt=a") is False
    assert server.calls[-1][0] != mutations(server)[0][0]


@pytest.mark.parametrize("conflict", [False, True])
def test_successful_add_cannot_return_true_without_valid_readback(conflict):
    result, server = resolved()
    lookups = [0]
    def readback(*args):
        lookups[0] += 1
        if lookups[0] == 2:
            if conflict:
                server.partitions[("a",)].sd.location = LOCATION + "/wrong"
            else:
                server.partitions.clear()
    server.hooks["get_partition"] = readback
    with pytest.raises(FatalExternalError if conflict else RetryableExternalError):
        result.create_partition(ref(), LOCATION + "/dt=a")
    assert len(mutations(server)) == 1


@pytest.mark.parametrize("outcome", [False, "missing"])
@pytest.mark.parametrize("present", [False, True])
def test_ambiguous_drop_result_requires_verified_absence(outcome, present):
    result, server = resolved()
    existing(server)
    original = server.factory
    def factory(host, port):
        sock, transport, client = original(host, port)
        def drop(*args):
            server.record(transport, "drop_partition", args)
            if not present:
                server.partitions.clear()
            if outcome == "missing":
                raise ttypes.NoSuchObjectException("private missing")
            return False
        client.drop_partition = drop
        return sock, transport, client
    result._connection_factory = factory
    if present:
        with pytest.raises(RetryableExternalError):
            result.delete_partition(ref())
    else:
        assert result.delete_partition(ref()) is False
    assert len(mutations(server)) == 1
    assert server.calls[-1][0] != mutations(server)[0][0]


@pytest.mark.parametrize("method", ["open", "get_database", "get_table", "get_partition", "add_partition", "drop_partition"])
@pytest.mark.parametrize("error,expected", [
    (TimeoutError("thrift://private:secret"), RetryableExternalError),
    (OSError("thrift://private:secret"), RetryableExternalError),
    (ttypes.TProtocolException(message="private:secret"), FatalExternalError),
    (ttypes.TApplicationException(message="private:secret"), FatalExternalError),
    (ttypes.InvalidObjectException("private:secret"), FatalExternalError),
])
def test_rpc_errors_are_sanitized_and_never_retried(method, error, expected):
    result, server = resolved()
    if method == "drop_partition":
        existing(server)
    count = len(server.calls)
    def fail(*args):
        raise error
    server.hooks[method] = fail
    with pytest.raises(expected) as caught:
        if method == "add_partition":
            result.create_partition(ref(), LOCATION + "/dt=a")
        elif method == "drop_partition":
            result.delete_partition(ref())
        else:
            result.partition_exists(ref())
    assert "private" not in str(caught.value) and "secret" not in str(caught.value)
    assert caught.value.__cause__ is None
    assert sum(call[1] == method for call in server.calls[count:]) == 1
    assert all(connection.closed for connection in server.connections)


def test_ambiguous_add_transport_failure_is_not_replayed_and_next_lookup_is_fresh():
    result, server = resolved()
    def fail_after_add(partition):
        server.partitions[tuple(partition.values)] = deepcopy(partition)
        raise TimeoutError("private after commit")
    server.hooks["add_partition"] = fail_after_add
    with pytest.raises(RetryableExternalError):
        result.create_partition(ref(), LOCATION + "/dt=a")
    broken = mutations(server)[0][0]
    server.hooks.clear()
    assert result.partition_exists(ref())
    assert server.calls[-1][0] != broken
    assert len(mutations(server)) == 1


def test_every_rpc_and_open_refreshes_remaining_socket_timeout():
    from rtmsf.adapters.gcp import OperationDeadline
    clock = [0.0]
    result, server = catalog(deadline=OperationDeadline(seconds=10, clock=lambda: clock[0]))
    for method in ("open", "get_database", "get_table", "get_partition", "add_partition"):
        server.hooks[method] = lambda *args: clock.__setitem__(0, clock[0] + 0.5)
    result.resolve_tables((TABLE_ID,))
    clock[0] = 0
    assert result.create_partition(ref(), LOCATION + "/dt=a")
    assert [call[3] for call in server.calls[3:]] == [10000 - 500 * index for index in range(16)]


def test_shared_storage_deadline_exhausted_before_catalog_mutation():
    from rtmsf.adapters.gcp import OperationDeadline
    from tests.test_gcp_adapters import adapter, metadata, PREFIX
    clock = [0.0]
    deadline = OperationDeadline(seconds=5, clock=lambda: clock[0])
    result, server = resolved(deadline=deadline)
    store, http, _ = adapter([(200, metadata(PREFIX + "file"), 3)], deadline=deadline)
    http.clock = clock
    server.hooks["get_partition"] = lambda *args: clock.__setitem__(0, 5)
    with pytest.raises(RetryableExternalError):
        with deadline.operation():
            assert store.head_exists("proof", PREFIX + "file")
            result.create_partition(ref(), LOCATION + "/dt=a")
    assert mutations(server) == []
    assert server.calls[-1][3] == 2000


def test_overrun_on_add_cannot_claim_applied_or_start_readback():
    from rtmsf.adapters.gcp import OperationDeadline
    clock = [0.0]
    result, server = resolved(deadline=OperationDeadline(seconds=5, clock=lambda: clock[0]))
    server.hooks["add_partition"] = lambda *args: clock.__setitem__(0, 5)
    with pytest.raises(RetryableExternalError):
        result.create_partition(ref(), LOCATION + "/dt=a")
    assert server.calls[-1][1] == "add_partition"


def test_outbound_partition_omits_write_id_on_generated_binary_wire():
    from thrift.Thrift import TType
    from thrift.protocol.TBinaryProtocol import TBinaryProtocol
    from thrift.transport.TTransport import TMemoryBuffer
    result, server = resolved()
    result.create_partition(ref(), LOCATION + "/dt=a")
    outbound = mutations(server)[0][2][0]
    buffer = TMemoryBuffer()
    outbound.write(TBinaryProtocol(buffer))
    protocol = TBinaryProtocol(TMemoryBuffer(buffer.getvalue()))
    protocol.readStructBegin()
    fields = []
    while True:
        _, kind, field = protocol.readFieldBegin()
        if kind == TType.STOP:
            break
        fields.append(field)
        protocol.skip(kind)
        protocol.readFieldEnd()
    assert 10 not in fields
    assert 6 in fields and 7 in fields


@pytest.mark.parametrize("kind,expected", [(0, RetryableExternalError), (1, RetryableExternalError),
    (3, RetryableExternalError), (4, RetryableExternalError), (2, FatalExternalError),
    (5, FatalExternalError), (6, FatalExternalError), (7, FatalExternalError)])
def test_generated_transport_errors_classify_invalid_frames_as_fatal(kind, expected):
    from thrift.transport.TTransport import TTransportException
    result, server = resolved()
    def fail(*args):
        raise TTransportException(kind, "private frame")
    server.hooks["get_partition"] = fail
    with pytest.raises(expected) as caught:
        result.partition_exists(ref())
    assert "private" not in str(caught.value)


@pytest.mark.parametrize("missing", ["database", "table"])
def test_missing_table_during_partition_rpc_is_never_absence(missing):
    result, server = resolved()
    server.hooks["get_partition"] = lambda *args: setattr(server, missing, None)
    with pytest.raises(FatalExternalError):
        result.partition_exists(ref())


def test_table_drift_during_positive_partition_lookup_refuses_truth():
    result, server = resolved()
    existing(server)
    server.hooks["get_partition"] = lambda *args: setattr(server.table.sd, "inputFormat", "changed")
    with pytest.raises(FatalExternalError):
        result.partition_exists(ref())


def test_already_exists_conflict_readback_refuses_noop():
    result, server = resolved()
    def race(partition):
        changed = deepcopy(partition)
        changed.parameters[OWNER_KEY] = "other"
        server.partitions[tuple(partition.values)] = changed
    server.hooks["add_partition"] = race
    with pytest.raises(FatalExternalError):
        result.create_partition(ref(), LOCATION + "/dt=a")


def test_true_drop_return_with_partition_present_is_failure():
    result, server = resolved()
    existing(server)
    server.hooks["get_partition"] = lambda *args: existing(server)
    with pytest.raises(RetryableExternalError):
        result.delete_partition(ref())


def test_default_factory_uses_generated_client_buffered_binary_and_real_socket_bounds(monkeypatch):
    import socket
    from rtmsf.adapters.gcp import _hms_connection
    from pymetastore.hive_metastore.ThriftHiveMetastore import Client
    from thrift.protocol.TBinaryProtocol import TBinaryProtocol
    from thrift.transport.TTransport import TBufferedTransport
    handles = []
    class Handle:
        def __init__(self, *args):
            self.bounds = []
            self.closed = False
            handles.append(self)
        def settimeout(self, seconds):
            self.bounds.append(seconds)
        def connect(self, address):
            assert address == ("10.76.0.4", 9083)
        def close(self):
            self.closed = True
    monkeypatch.setattr(socket, "socket", Handle)
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.76.0.4", 9083))])
    sock, transport, client = _hms_connection("10.76.0.4", 9083)
    assert isinstance(client, Client)
    assert isinstance(transport, TBufferedTransport)
    assert isinstance(client._iprot, TBinaryProtocol)
    sock.setTimeout(15000)
    transport.open()
    sock.setTimeout(2000)
    transport.close()
    assert handles[0].bounds == [15, 2]
    assert handles[0].closed


def test_successful_null_get_partition_response_is_not_absence():
    result, server = resolved()
    server.partitions[("a",)] = None
    with pytest.raises(FatalExternalError):
        result.partition_exists(ref())


@pytest.mark.parametrize("columns", [("dt",), (("dt",),), (("dt", "string", "extra"),), None])
def test_partition_column_configuration_requires_name_type_pairs(columns):
    with pytest.raises(ValueError):
        catalog(partition_columns=columns)


@pytest.mark.parametrize("missing", ["database", "table"])
def test_missing_table_during_repeated_drop_is_not_noop(missing):
    result, server = resolved()
    server.hooks["get_partition"] = lambda *args: setattr(server, missing, None)
    with pytest.raises(FatalExternalError):
        result.delete_partition(ref())
    assert mutations(server) == []


def test_changed_table_after_add_refuses_applied_readback():
    result, server = resolved()
    def change_table(*args):
        server.table.parameters["external.table.purge"] = "true"
    server.hooks["add_partition"] = change_table
    with pytest.raises(FatalExternalError):
        result.create_partition(ref(), LOCATION + "/dt=a")
    assert len(mutations(server)) == 1


def test_existing_conflict_appearing_during_drop_lookup_prevents_mutation():
    result, server = resolved()
    existing(server)
    server.hooks["get_partition"] = lambda *args: server.partitions[("a",)].parameters.clear()
    with pytest.raises(FatalExternalError):
        result.delete_partition(ref())
    assert mutations(server) == []
