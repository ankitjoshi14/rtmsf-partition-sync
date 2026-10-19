"""GCP current-live storage/catalog truth and one synchronous operation deadline."""

from collections.abc import Mapping
from contextlib import contextmanager, suppress
from copy import deepcopy
from functools import partial
import ipaddress
import math
import re
import socket
import time

from google.api_core import exceptions as api_errors
from google.auth import exceptions as auth_errors
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import RequestException

from pymetastore.hive_metastore import ThriftHiveMetastore, ttypes
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from rtmsf.interfaces import FatalExternalError, PartitionCatalog, RetryableExternalError
from rtmsf.models import TableDefinition


STORAGE_ENDPOINT = "https://storage.googleapis.com"


def _canonical_endpoint(client):
    connection = getattr(client, "_connection", None)
    if getattr(connection, "API_BASE_URL", None) != STORAGE_ENDPOINT:
        return False
    return connection.get_api_base_url_for_mtls() == STORAGE_ENDPOINT


class OperationDeadline:
    """Share one elapsed budget across storage/catalog calls in an outer scope.

    SDK timeouts bound individual RPC waits; elapsed checks also reject overruns
    after authentication or slow response paths that those timeouts cannot stop.
    This context belongs to one synchronous writer.
    """

    def __init__(self, seconds=90, rpc_seconds=15, clock=None):
        for value in (seconds, rpc_seconds):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError("operation and RPC budgets must be positive finite seconds")
        if rpc_seconds > 15:
            raise ValueError("RPC budget must be at most 15 seconds")
        self.seconds, self.rpc_seconds = seconds, rpc_seconds
        self._clock = time.monotonic if clock is None else clock
        self._started = self._last = None
        self._failed = False

    @contextmanager
    def operation(self):
        outer = self._started is None
        if outer:
            self._started = self._last = self._clock()
            self._failed = False
        try:
            self.check()
            yield self
            self.check()
        finally:
            if outer:
                self._started = self._last = None

    def check(self):
        if self._started is None:
            raise RuntimeError("use deadline.operation() around external operations")
        now = self._clock()
        if (self._failed or type(now) not in (int, float) or not math.isfinite(now)
                or type(self._started) not in (int, float) or not math.isfinite(self._started)
                or now < self._last or now - self._started >= self.seconds):
            self._failed = True
            raise RetryableExternalError("gcp_operation_deadline_exceeded")
        self._last = now
        return self.seconds - (now - self._started)

    def rpc_timeout(self):
        return min(self.rpc_seconds, self.check())


def _sources(sources):
    if not isinstance(sources, Mapping) or not sources:
        raise ValueError("configure at least one logical source")
    result = {}
    for source_id, bucket in sources.items():
        if not isinstance(source_id, str) or not source_id or not isinstance(bucket, str):
            raise ValueError("sources map logical IDs to trusted bucket names")
        if (not 3 <= len(bucket) <= 222 or not re.fullmatch(r"[a-z0-9][a-z0-9._-]*[a-z0-9]", bucket)
                or any(len(part) > 63 or not part for part in bucket.split("."))
                or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", bucket)):
            raise ValueError("invalid bucket name")
        result[source_id] = bucket
    if len(set(result.values())) != len(result):
        raise ValueError("each source must identify a distinct bucket")
    return result


def _exact_text(value):
    if not isinstance(value, str) or not value:
        raise FatalExternalError("gcp_invalid_object_name")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError:
        raise FatalExternalError("gcp_invalid_object_name") from None
    return value


class GcsObjectStore:
    def __init__(self, client, sources, deadline=None, max_list_pages=4):
        self._client, self._sources = client, _sources(sources)
        if not _canonical_endpoint(client):
            raise ValueError("require the canonical GCS HTTPS endpoint")
        if type(max_list_pages) is not int or not 1 <= max_list_pages <= 4:
            raise ValueError("LIST page budget must be between 1 and 4")
        self._max_list_pages = max_list_pages
        self._deadline = OperationDeadline() if deadline is None else deadline
        # The pinned SDK cache starts background requests with default retries.
        client._bucket_metadata_cache = None
        # Auth transport otherwise replays 401 responses even with SDK retry=None.
        if isinstance(client._http, AuthorizedSession):
            client._http._max_refresh_attempts = 0

    def _bucket(self, source_id):
        if not isinstance(source_id, str) or source_id not in self._sources:
            raise FatalExternalError("gcp_unconfigured_source")
        if not _canonical_endpoint(self._client):
            raise FatalExternalError("gcp_untrusted_storage_endpoint")
        name = self._sources[source_id]
        bucket = self._client.bucket(name)
        if bucket.name != name:
            raise FatalExternalError("gcp_bucket_mismatch")
        return bucket

    def _call(self, operation, *args, allow_not_found=False, **kwargs):
        timeout = self._deadline.rpc_timeout()
        try:
            return operation(*args, retry=None, timeout=timeout, **kwargs)
        except api_errors.NotFound:
            if allow_not_found:
                raise
            raise FatalExternalError("gcp_status_404") from None
        except api_errors.GoogleAPICallError as error:
            status = error.code
            kind = RetryableExternalError if status is not None and (status in (408, 429) or status >= 500) else FatalExternalError
            raise kind(f"gcp_status_{int(status)}" if status is not None else "gcp_api_failure") from None
        except (ValueError, TypeError, AttributeError):
            raise FatalExternalError("gcp_invalid_response") from None
        except (RequestException, auth_errors.TransportError, TimeoutError, ConnectionError):
            raise RetryableExternalError("gcp_transport_failure") from None
        except auth_errors.GoogleAuthError:
            raise FatalExternalError("gcp_auth_failure") from None
        finally:
            self._deadline.check()

    def head_exists(self, source_id, key):
        with self._deadline.operation():
            bucket = self._bucket(source_id)
            key = _exact_text(key)
            # Reloading a reused Blob would select its previously populated generation.
            blob = bucket.blob(key)
            try:
                self._call(blob.reload, allow_not_found=True)
            except api_errors.NotFound:
                self._call(bucket.reload)
                if not isinstance(bucket._properties, dict) or bucket._properties.get("name") != self._sources[source_id]:
                    raise FatalExternalError("gcp_bucket_mismatch")
                return False
            if (not isinstance(blob._properties, dict) or blob._properties.get("name") != key
                    or blob._properties.get("bucket") != bucket.name):
                raise FatalExternalError("gcp_object_metadata_mismatch")
            return True

    def prefix_non_empty(self, source_id, prefix):
        with self._deadline.operation():
            bucket = self._bucket(source_id)
            prefix = self._directory(prefix)
            token, seen = None, set()
            for _ in range(self._max_list_pages):
                # max_results=1 can exhaust an iterator despite nextPageToken: reissue it.
                iterator = self._call(
                    self._client.list_blobs, bucket, prefix=prefix, max_results=1, page_size=1,
                    page_token=token, fields="nextPageToken,items(name,bucket)",
                )
                page = self._call_page(iterator)
                raw = page.raw_page
                if not isinstance(raw, dict) or raw.get("prefixes"):
                    raise FatalExternalError("gcp_invalid_list_page")
                items = raw.get("items", [])
                token = raw.get("nextPageToken")
                if "nextPageToken" in raw and (not isinstance(token, str) or not token or token in seen):
                    raise FatalExternalError("gcp_invalid_list_token")
                if not isinstance(items, list) or len(items) > 1:
                    raise FatalExternalError("gcp_invalid_list_page")
                for item in items:
                    if not isinstance(item, dict) or item.get("bucket") != bucket.name:
                        raise FatalExternalError("gcp_invalid_list_object")
                    if not _exact_text(item.get("name")).startswith(prefix):
                        raise FatalExternalError("gcp_out_of_prefix_object")
                if items:
                    return True
                if token is None:
                    return False
                seen.add(token)
            raise RetryableExternalError("gcp_list_page_budget_exceeded")

    def _call_page(self, iterator):
        # Refresh the timeout at the lazy request, after local iterator construction.
        def fetch(**kwargs):
            iterator.api_request = partial(iterator.api_request, **kwargs)
            return next(iterator.pages)

        try:
            return self._call(fetch)
        except StopIteration:
            raise FatalExternalError("gcp_missing_list_page") from None

    @staticmethod
    def _directory(prefix):
        prefix = _exact_text(prefix)
        if not prefix.endswith("/"):
            raise FatalExternalError("gcp_invalid_partition_directory")
        return prefix

    def location_uri(self, source_id, prefix):
        with self._deadline.operation():
            bucket = self._bucket(source_id)
            return _hadoop_location(bucket.name, prefix)


def _hadoop_location(bucket, prefix):
    prefix = GcsObjectStore._directory(prefix)[:-1]
    if any(segment in ("", ".", "..") for segment in prefix.split("/")):
        raise FatalExternalError("gcp_invalid_partition_directory")
    # Hive/Spark consume unescaped Hadoop Path text; percent escapes change the key.
    return f"gs://{bucket}/{prefix}"


HMS_OWNER_KEY = "gcp_probe_run_id"


class _HmsSocket(TSocket.TSocket):
    def open(self):
        if self.handle is not None:
            raise TTransport.TTransportException(
                type=TTransport.TTransportException.ALREADY_OPEN, message="gcp_hms_connection_already_open")
        # Thrift's resolver/retry open logs raw failures before the adapter can sanitize them.
        handle = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            handle.settimeout(self._timeout)
            handle.connect((self.host, self.port))
            self.handle = handle
        finally:
            if self.handle is None:
                with suppress(Exception):
                    handle.close()


def _hms_connection(host, port):
    sock = _HmsSocket(host, port, socket_family=socket.AF_INET)
    transport = TTransport.TBufferedTransport(sock)
    client = ThriftHiveMetastore.Client(TBinaryProtocol.TBinaryProtocol(transport))
    return sock, transport, client


class HiveMetastorePartitionCatalog(PartitionCatalog):
    """Metadata-only mutations of one approved existing run-owned external table.

    Pass the storage adapter's OperationDeadline and enter its outer operation()
    around PMCP. Connections are dedicated, bounded, and never retried or reused.
    """

    def __init__(self, endpoint, *, table_id, source_id, bucket, root_prefix,
                 partition_columns, owner, deadline=None, connection_factory=None):
        try:
            match = re.fullmatch(r"thrift://([0-9.]+):9083", _exact_text(endpoint))
            if match is None:
                raise ValueError
            host = match[1]
            address = ipaddress.IPv4Address(host)
            if not any(address in ipaddress.IPv4Network(network) for network in
                       ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")):
                raise ValueError
            identity = _exact_text(table_id).split(".")
            if len(identity) != 2 or not all(re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", part) for part in identity):
                raise ValueError
            _exact_text(source_id)
            _sources({source_id: bucket})
            _exact_text(owner)
            root_location = _hadoop_location(bucket, root_prefix)
            if not isinstance(partition_columns, (tuple, list)) or any(
                    not isinstance(column, (tuple, list)) for column in partition_columns):
                raise ValueError
            columns = tuple(tuple(column) for column in partition_columns)
            if not columns or any(len(column) != 2 for column in columns):
                raise ValueError
            for name, kind in columns:
                _exact_text(name)
                _exact_text(kind)
            definition = TableDefinition(table_id, source_id, root_prefix, tuple(name for name, _ in columns))
        except (ValueError, TypeError, FatalExternalError):
            raise ValueError("gcp_hms_invalid_configuration") from None
        if deadline is not None and not isinstance(deadline, OperationDeadline):
            raise ValueError("gcp_hms_invalid_deadline")
        if connection_factory is not None and not callable(connection_factory):
            raise ValueError("gcp_hms_invalid_connection_factory")
        self._host, self._identity = host, tuple(identity)
        self._definition, self._columns = definition, columns
        self._bucket, self._root_location, self._owner = bucket, root_location, owner
        self._deadline = OperationDeadline() if deadline is None else deadline
        self._connection_factory = _hms_connection if connection_factory is None else connection_factory
        self._descriptor = self._partition_keys = None

    def _rpc(self, sock, operation, *args, expected=()):
        try:
            sock.setTimeout(self._deadline.rpc_timeout() * 1000)
            return operation(*args)
        except expected:
            raise
        except TTransport.TTransportException as error:
            kind = RetryableExternalError if error.type in (
                TTransport.TTransportException.UNKNOWN, TTransport.TTransportException.NOT_OPEN,
                TTransport.TTransportException.TIMED_OUT, TTransport.TTransportException.END_OF_FILE,
            ) else FatalExternalError
            raise kind("gcp_hms_transport_failure") from None
        except OSError:
            raise RetryableExternalError("gcp_hms_transport_failure") from None
        except (FatalExternalError, RetryableExternalError):
            raise
        except Exception:
            raise FatalExternalError("gcp_hms_rpc_failure") from None
        finally:
            self._deadline.check()

    @contextmanager
    def _connection(self):
        self._deadline.check()
        try:
            sock, transport, client = self._connection_factory(self._host, 9083)
        except (TTransport.TTransportException, OSError):
            raise RetryableExternalError("gcp_hms_transport_failure") from None
        except Exception:
            raise FatalExternalError("gcp_hms_invalid_connection") from None
        try:
            self._rpc(sock, transport.open)
            yield sock, client
        finally:
            # A failed transport is discarded; close must not mask its original error.
            with suppress(Exception):
                transport.close()
            self._deadline.check()

    def _guard(self, sock, client):
        db, name = self._identity
        database = self._rpc(sock, client.get_database, db)
        table = self._rpc(sock, client.get_table, db, name)
        try:
            if (not isinstance(database, ttypes.Database) or database.name != db
                    or not isinstance(database.parameters, dict) or database.parameters.get(HMS_OWNER_KEY) != self._owner
                    or not isinstance(table, ttypes.Table) or (table.dbName, table.tableName) != self._identity
                    or table.tableType != "EXTERNAL_TABLE" or not isinstance(table.parameters, dict)
                    or table.parameters.get(HMS_OWNER_KEY) != self._owner
                    or table.parameters.get("EXTERNAL") != "TRUE"
                    or table.parameters.get("external.table.purge") != "false"
                    or not isinstance(table.sd, ttypes.StorageDescriptor) or table.sd.location != self._root_location
                    or not isinstance(table.partitionKeys, list)
                    or any(not isinstance(key, ttypes.FieldSchema) for key in table.partitionKeys)
                    or tuple((key.name, key.type) for key in table.partitionKeys) != self._columns):
                raise ValueError
            if self._descriptor is not None and (table.sd != self._descriptor or table.partitionKeys != self._partition_keys):
                raise ValueError
        except (AttributeError, TypeError, ValueError):
            raise FatalExternalError("gcp_hms_unapproved_table") from None
        self._deadline.check()
        return table

    def resolve_tables(self, table_ids):
        if not isinstance(table_ids, (list, tuple)) or tuple(table_ids) != (self._definition.table_id,):
            raise FatalExternalError("gcp_hms_unapproved_table")
        with self._deadline.operation():
            with self._connection() as (sock, client):
                table = self._guard(sock, client)
                if self._descriptor is None:
                    self._descriptor = deepcopy(table.sd)
                    self._partition_keys = deepcopy(table.partitionKeys)
        return (self._definition,)

    def _partition(self, partition):
        try:
            if (self._descriptor is None or partition.table_id != self._definition.table_id
                    or partition.source_id != self._definition.source_id or not isinstance(partition.values, tuple)
                    or len(partition.values) != len(self._columns)):
                raise ValueError
            for value in partition.values:
                if "/" in _exact_text(value):
                    raise ValueError
            path = "/".join(name + "=" + value for (name, _), value in zip(self._columns, partition.values))
            prefix = self._definition.root_prefix + path + "/"
            if partition.partition_path != path or partition.storage_prefix != prefix:
                raise ValueError
            return _hadoop_location(self._bucket, prefix)
        except (AttributeError, TypeError, ValueError, FatalExternalError):
            raise FatalExternalError("gcp_hms_invalid_partition_identity") from None

    def _get_partition(self, sock, client, partition, location):
        try:
            actual = self._rpc(sock, client.get_partition, *self._identity, list(partition.values),
                               expected=(ttypes.NoSuchObjectException,))
        except ttypes.NoSuchObjectException:
            # NoSuchObject can also mean table loss; absence requires trusted guards.
            self._guard(sock, client)
            return None
        self._guard(sock, client)
        descriptor = deepcopy(self._descriptor)
        descriptor.location = location
        if (not isinstance(actual, ttypes.Partition) or (actual.dbName, actual.tableName) != self._identity
                or actual.values != list(partition.values) or actual.sd != descriptor
                or not isinstance(actual.parameters, dict) or actual.parameters.get(HMS_OWNER_KEY) != self._owner):
            raise FatalExternalError("gcp_hms_conflicting_partition")
        self._deadline.check()
        return actual

    def _read_partition(self, partition, location):
        with self._connection() as (sock, client):
            self._guard(sock, client)
            return self._get_partition(sock, client, partition, location)

    def partition_exists(self, partition):
        with self._deadline.operation():
            location = self._partition(partition)
            return self._read_partition(partition, location) is not None

    def create_partition(self, partition, location):
        with self._deadline.operation():
            if location != self._partition(partition):
                raise FatalExternalError("gcp_hms_unapproved_partition_location")
            if self._read_partition(partition, location) is not None:
                return False
            descriptor = deepcopy(self._descriptor)
            descriptor.location = location
            # Hive 3.1.2 cannot decode the package's newer writeId wire field.
            outbound = ttypes.Partition(values=list(partition.values), dbName=self._identity[0],
                                        tableName=self._identity[1], sd=descriptor,
                                        parameters={HMS_OWNER_KEY: self._owner}, writeId=None)
            created = True
            with self._connection() as (sock, client):
                self._guard(sock, client)
                try:
                    self._rpc(sock, client.add_partition, outbound, expected=(ttypes.AlreadyExistsException,))
                except ttypes.AlreadyExistsException:
                    created = False
            if self._read_partition(partition, location) is None:
                raise RetryableExternalError("gcp_hms_add_readback_missing")
            return created

    def delete_partition(self, partition):
        with self._deadline.operation():
            location = self._partition(partition)
            with self._connection() as (sock, client):
                self._guard(sock, client)
                if self._get_partition(sock, client, partition, location) is None:
                    return False
                try:
                    deleted = self._rpc(sock, client.drop_partition, *self._identity, list(partition.values), False,
                                        expected=(ttypes.NoSuchObjectException,))
                except ttypes.NoSuchObjectException:
                    deleted = False
                if type(deleted) is not bool:
                    raise FatalExternalError("gcp_hms_invalid_drop_response")
            if self._read_partition(partition, location) is not None:
                raise RetryableExternalError("gcp_hms_drop_readback_present")
            return deleted
