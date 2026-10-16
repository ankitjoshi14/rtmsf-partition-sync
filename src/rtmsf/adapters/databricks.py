"""One approved external Parquet table, with binary STRING partition identity.

Mutation booleans describe the preceding observation under a single writer.
The SQL engine does not promise an atomic changed result for IF [NOT] EXISTS.
HTTP phase limits are operational; stdlib header reads are not hard-preempted.
"""

import base64
from contextlib import contextmanager
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import ssl
import tempfile
import time
import unicodedata
from urllib.parse import quote, urlencode, urlsplit
import uuid

from rtmsf.interfaces import FatalExternalError, PartitionCatalog, RetryableExternalError
from rtmsf.models import PartitionRef, TableDefinition


class DatabricksWorkerStop(RuntimeError):
    """Stop before queue settlement or restart until recovery is confirmed."""


def _text(value):
    if not isinstance(value, str) or not value:
        raise ValueError("invalid_text")
    value.encode("utf-8", errors="strict")
    if any(unicodedata.category(char) in {"Cc", "Cf", "Cs"} for char in value):
        raise ValueError("invalid_text")
    return value


def _identifier(value):
    return "`" + _text(value).replace("`", "``") + "`"


def _literal(value):
    return "'" + _text(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate_json_key")
            result[key] = value
        return result
    def constant(value):
        raise ValueError("nonfinite_json_value")
    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)


def _https_request(method, url, *, headers, body, connect_timeout, read_timeout, max_response_bytes, allow_redirects):
    if allow_redirects:
        raise ValueError("redirects_not_supported")
    target = urlsplit(url)
    deadline = time.monotonic() + connect_timeout + read_timeout
    connection = http.client.HTTPSConnection(target.hostname, timeout=connect_timeout, context=ssl.create_default_context())
    response = None
    try:
        connection.connect()
        peer = connection.sock
        def remaining():
            seconds = deadline - time.monotonic()
            if seconds <= 0:
                raise TimeoutError("request_budget_exceeded")
            peer.settimeout(min(read_timeout, seconds))
        remaining()
        connection.request(method, target.path, body=body, headers=headers)
        remaining()
        response = connection.getresponse()
        length = response.getheader("Content-Length")
        if length is not None and (not length.isdecimal() or int(length) > max_response_bytes):
            raise ValueError("invalid_response_length")
        payload = bytearray()
        while not response.isclosed():
            remaining()
            chunk = response.read1(min(8192, max_response_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
            if len(payload) > max_response_bytes:
                raise ValueError("response_too_large")
        if length is not None and len(payload) != int(length):
            raise ValueError("incomplete_response")
        return response.status, dict(response.getheaders()), bytes(payload)
    finally:
        try:
            if response is not None:
                response.close()
        finally:
            connection.close()


class DatabricksSQLStatements:
    """Bounded workspace OAuth and SQL REST, with a durable mutation receipt."""

    def __init__(self, workspace_url, warehouse_id, client_id, client_secret, *, journal_path, request=None, clock=time.monotonic, sleep=time.sleep, operation_seconds=30, max_response_bytes=65536, max_requests=20, max_polls=12):
        try:
            workspace = urlsplit(_text(workspace_url))
            if (workspace.scheme != "https" or workspace.path or workspace.query or workspace.fragment
                or not re.fullmatch(r"adb-[0-9]+\.[0-9]+\.azuredatabricks\.net", workspace.netloc)):
                raise ValueError("invalid_workspace")
            if isinstance(operation_seconds, bool) or not math.isfinite(operation_seconds) or not 0 < operation_seconds < 119:
                raise ValueError("invalid_operation_limit")
            if any(type(limit) is not int or limit <= 0 for limit in (max_response_bytes, max_requests, max_polls)):
                raise ValueError("invalid_statement_limit")
            _text(warehouse_id)
            _text(client_id)
            _text(client_secret)
            self._journal = Path(journal_path)
        except (TypeError, ValueError, UnicodeError):
            raise ValueError("invalid_databricks_configuration") from None
        self._workspace = workspace_url
        self._warehouse = warehouse_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._request = request or _https_request
        self._clock, self._sleep = clock, sleep
        self._seconds = operation_seconds
        self._max_bytes, self._max_requests, self._max_polls = max_response_bytes, max_requests, max_polls
        self._target_hash = hashlib.sha256((workspace_url + "/" + warehouse_id).encode()).hexdigest()
        self._token, self._token_expires = None, 0
        self._deadline = None
        self._requests = self._polls = 0
        self._stopped = False

    def _stop(self, reason):
        self._stopped = True
        raise DatabricksWorkerStop(reason) from None

    @contextmanager
    def operation(self, *, deadline=None):
        try:
            if deadline is not None and (isinstance(deadline, bool) or not math.isfinite(deadline)):
                raise ValueError("invalid_deadline")
        except (TypeError, ValueError, OverflowError):
            raise ValueError("invalid_databricks_operation_deadline") from None
        outer = self._deadline is None
        bounded = self._clock() + self._seconds
        if deadline is not None:
            bounded = min(bounded, deadline)
        if outer:
            self._deadline = bounded
            self._requests = self._polls = 0
        else:
            self._deadline = min(self._deadline, bounded)
        try:
            self._remaining()
            yield
            self._remaining()
        finally:
            if outer:
                self._deadline = None

    def _remaining(self):
        remaining = self._deadline - self._clock()
        if remaining <= 0:
            raise RetryableExternalError("databricks_operation_budget_exceeded")
        return remaining

    def _http(self, method, path, headers, body=None):
        remaining = self._remaining()
        if self._requests >= self._max_requests:
            raise RetryableExternalError("databricks_request_limit_exceeded")
        self._requests += 1
        connect = min(5, remaining / 2)
        try:
            status, _, payload = self._request(method, self._workspace + path, headers=headers, body=body,
                connect_timeout=connect, read_timeout=remaining - connect,
                max_response_bytes=self._max_bytes, allow_redirects=False)
            if type(status) is not int or not isinstance(payload, bytes) or len(payload) > self._max_bytes:
                raise ValueError("invalid_http_response")
            if status in {401, 403}:
                self._stop("databricks_auth_or_access_denied")
            if 300 <= status < 400:
                raise RetryableExternalError("databricks_redirect_rejected")
            if not 200 <= status < 300:
                if status in {408, 429} or status >= 500:
                    raise RetryableExternalError("databricks_http_transient_failure")
                self._stop("databricks_http_configuration_failure")
            if method == "POST" and path.endswith("/cancel") and payload == b"":
                return {}
            result = _json(payload.decode("utf-8", errors="strict"))
            if not isinstance(result, dict):
                raise ValueError("invalid_json_response")
            return result
        except (OSError, http.client.HTTPException, TypeError, ValueError, UnicodeError):
            raise RetryableExternalError("databricks_transport_or_response_failure") from None

    def _access_token(self):
        if self._token is not None and self._clock() < self._token_expires:
            return self._token
        basic = base64.b64encode((self._client_id + ":" + self._client_secret).encode()).decode("ascii")
        document = self._http("POST", "/oidc/v1/token", {"Authorization": "Basic " + basic, "Content-Type": "application/x-www-form-urlencoded"},
            urlencode({"grant_type": "client_credentials", "scope": "sql"}).encode())
        try:
            token, expires = document["access_token"], document["expires_in"]
            if (not isinstance(token, str) or not token or not token.isascii()
                or any(not 33 <= ord(char) <= 126 for char in token)
                or document["token_type"].lower() != "bearer"
                or isinstance(expires, bool) or not math.isfinite(expires) or expires <= 0):
                raise ValueError("invalid_oauth_response")
        except (KeyError, TypeError, ValueError, AttributeError):
            self._stop("databricks_oauth_response_invalid")
        self._token = token
        self._token_expires = self._clock() + max(0, expires - 30)
        return token

    def _authorized(self, method, path, document=None):
        token = self._access_token()
        body = document
        if document is not None and not isinstance(document, bytes):
            body = json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode()
        return self._http(method, path, {"Authorization": "Bearer " + token, "Content-Type": "application/json"}, body)

    def _pending(self):
        try:
            if self._journal.is_symlink():
                raise ValueError("invalid_journal")
            if not self._journal.exists():
                return None
            if self._journal.stat().st_size > 8192:
                raise ValueError("invalid_journal")
            document = _json(self._journal.read_text(encoding="utf-8"))
            if (not isinstance(document, dict) or type(document.get("version")) is not int or document.get("version") != 1
                or document.get("target_sha256") != self._target_hash
                or not re.fullmatch(r"[0-9a-f]{32}", document.get("marker_id", ""))
                or not re.fullmatch(r"[0-9a-f]{64}", document.get("statement_sha256", ""))
                or document.get("statement_id") is not None and not self._valid_id(document["statement_id"])):
                raise ValueError("invalid_journal")
            return document
        except (OSError, TypeError, ValueError, UnicodeError):
            self._stop("databricks_journal_unreadable")

    def _sync_directory(self):
        fd = os.open(self._journal.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _save(self, marker):
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=self._journal.parent, prefix=".databricks-pending-", delete=False) as handle:
                temporary = handle.name
                handle.write(json.dumps(marker, sort_keys=True, separators=(",", ":")).encode())
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._journal)
            self._sync_directory()
        except OSError:
            self._stop("databricks_journal_write_unconfirmed")
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass

    def _clear(self):
        try:
            self._journal.unlink()
            self._sync_directory()
        except OSError:
            self._stop("databricks_journal_cleanup_unconfirmed")

    @staticmethod
    def _valid_id(statement_id):
        return isinstance(statement_id, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,256}", statement_id) is not None

    def _state(self, document, statement_id, mutation):
        if document.get("statement_id") != statement_id:
            if mutation:
                self._stop("databricks_statement_identity_unconfirmed")
            raise RetryableExternalError("databricks_statement_identity_unconfirmed")
        status = document.get("status")
        state = status.get("state") if isinstance(status, dict) else None
        if not isinstance(state, str) or state not in {"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}:
            if mutation:
                self._stop("databricks_mutation_state_unconfirmed")
            raise RetryableExternalError("databricks_read_state_unconfirmed")
        error = status.get("error")
        if isinstance(error, dict) and isinstance(error.get("error_code"), str) and error["error_code"] in {"PERMISSION_DENIED", "UNAUTHENTICATED", "UNAUTHORIZED"}:
            self._stop("databricks_auth_or_access_denied")
        return "SUCCEEDED" if mutation and state == "CLOSED" else state

    def _get(self, statement_id):
        if self._polls >= self._max_polls:
            raise RetryableExternalError("databricks_poll_limit_exceeded")
        self._polls += 1
        return self._authorized("GET", "/api/2.0/sql/statements/" + statement_id)

    def _cancel_confirm(self, statement_id):
        try:
            self._authorized("POST", "/api/2.0/sql/statements/" + statement_id + "/cancel", {})
            while True:
                document = self._get(statement_id)
                if self._state(document, statement_id, True) in {"SUCCEEDED", "FAILED", "CANCELED"}:
                    return document
                self._sleep(min(0.5, self._remaining()))
        except RetryableExternalError:
            self._stop("databricks_mutation_terminal_unconfirmed")

    def _await(self, document, statement_id, mutation):
        while self._state(document, statement_id, mutation) in {"PENDING", "RUNNING"}:
            if mutation and (self._polls >= self._max_polls - 1 or self._requests >= self._max_requests - 2):
                return self._cancel_confirm(statement_id)
            try:
                self._sleep(min(0.5, self._remaining()))
                document = self._get(statement_id)
            except RetryableExternalError:
                if mutation:
                    return self._cancel_confirm(statement_id)
                raise
        return document

    def recover(self):
        if self._stopped:
            self._stop("databricks_worker_stopped")
        marker = self._pending()
        if marker is None:
            return
        statement_id = marker.get("statement_id")
        if statement_id is None:
            self._stop("databricks_missing_statement_id_manual_reconciliation")
        with self.operation():
            try:
                document = self._get(statement_id)
                if self._state(document, statement_id, True) in {"PENDING", "RUNNING"}:
                    self._cancel_confirm(statement_id)
                self._clear()
            except RetryableExternalError:
                self._stop("databricks_mutation_terminal_unconfirmed")

    def execute(self, statement, *, mutation=False):
        if self._stopped or self._pending() is not None:
            self._stop("databricks_prior_mutation_unresolved")
        try:
            _text(statement)
            body = json.dumps({"warehouse_id": self._warehouse, "statement": statement, "disposition": "INLINE", "format": "JSON_ARRAY",
                "byte_limit": self._max_bytes, "wait_timeout": "0s", "on_wait_timeout": "CONTINUE"}, separators=(",", ":"), ensure_ascii=False).encode()
            if len(body) > self._max_bytes:
                raise ValueError("statement_payload_too_large")
        except (ValueError, UnicodeError):
            raise FatalExternalError("databricks_invalid_statement") from None
        with self.operation():
            self._access_token()
            self._remaining()
            marker = None
            if mutation:
                marker = {"version": 1, "marker_id": uuid.uuid4().hex, "target_sha256": self._target_hash,
                    "statement_sha256": hashlib.sha256(statement.encode()).hexdigest(), "statement_id": None}
                self._save(marker)
            try:
                document = self._authorized("POST", "/api/2.0/sql/statements", body)
                statement_id = document.get("statement_id")
                if not self._valid_id(statement_id):
                    if mutation:
                        self._stop("databricks_missing_statement_id_manual_reconciliation")
                    raise RetryableExternalError("databricks_read_identity_unconfirmed")
                if mutation:
                    marker["statement_id"] = statement_id
                    self._save(marker)
                document = self._await(document, statement_id, mutation)
            except RetryableExternalError:
                if mutation:
                    self._stop("databricks_mutation_receipt_unconfirmed")
                raise
            if mutation:
                self._clear()
            if self._state(document, statement_id, mutation) != "SUCCEEDED":
                raise RetryableExternalError("databricks_statement_not_succeeded")
            return document


def _rows(document):
    try:
        manifest, result = document["manifest"], document.get("result", {})
        schema = manifest["schema"]
        columns = schema["columns"]
        count = manifest["total_row_count"]
        if (document["status"]["state"] != "SUCCEEDED" or manifest["format"] != "JSON_ARRAY"
            or manifest.get("truncated") is not False or type(count) is not int or not 0 <= count <= 1
            or type(schema["column_count"]) is not int or schema["column_count"] != 1 or len(columns) != 1
            or type(columns[0]["position"]) is not int or columns[0]["position"] != 0 or columns[0]["type_name"] != "STRING"
            or not _text(columns[0]["name"]) or "external_links" in result
            or any(result.get(key) is not None for key in ("next_chunk_index", "next_chunk_internal_link"))):
            raise ValueError("incomplete_inline_result")
        chunks = manifest["chunks"]
        if type(manifest["total_chunk_count"]) is not int:
            raise ValueError("invalid_chunk_count")
        if manifest["total_chunk_count"] == 0 and count == 0 and chunks == [] and result == {}:
            return []
        if (manifest["total_chunk_count"] != 1 or len(chunks) != 1
            or any(type(chunks[0][key]) is not int or type(result[key]) is not int or chunks[0][key] != value or result[key] != value
                for key, value in (("chunk_index", 0), ("row_offset", 0), ("row_count", count)))):
            raise ValueError("incomplete_inline_result")
        rows = result["data_array"]
        if not isinstance(rows, list) or len(rows) != count:
            raise ValueError("incomplete_inline_result")
        for row in rows:
            if not isinstance(row, list) or len(row) != 1 or not isinstance(row[0], str):
                raise ValueError("incomplete_inline_result")
            row[0].encode("utf-8", errors="strict")
        return rows
    except (KeyError, TypeError, ValueError, UnicodeError):
        raise RetryableExternalError("databricks_inline_result_unconfirmed") from None


class DatabricksPartitionCatalog(PartitionCatalog):
    def __init__(self, statements, *, table_id, source_id, root_prefix, root_location):
        try:
            identity = _text(table_id).split(".")
            if len(identity) != 3:
                raise ValueError("invalid_table_id")
            self._quoted_table = ".".join(_identifier(part) for part in identity)
            _text(source_id)
            _text(root_prefix)
            if not root_prefix.endswith("/") or any(part in {"", ".", ".."} for part in root_prefix[:-1].split("/")):
                raise ValueError("invalid_root_prefix")
            location = urlsplit(_text(root_location))
            if (location.scheme != "abfss" or location.query or location.fragment
                or not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]@[a-z0-9]{3,24}\.dfs\.core\.windows\.net", location.netloc)
                or location.path != "/" + quote(root_prefix, safe="/=")):
                raise ValueError("invalid_root_location")
        except (TypeError, ValueError, UnicodeError):
            raise FatalExternalError("databricks_invalid_approved_table") from None
        self._statements = statements
        self._table_id, self._source_id = table_id, source_id
        self._root_prefix, self._root_location = root_prefix, root_location
        self._identity = tuple(identity)
        self._definition = None

    def resolve_tables(self, table_ids):
        if tuple(table_ids) != (self._table_id,):
            raise FatalExternalError("databricks_unapproved_table")
        with self._statements.operation():
            self._statements.recover()
            if self._definition is None:
                rows = _rows(self._statements.execute("DESCRIBE TABLE EXTENDED " + self._quoted_table + " AS JSON"))
                try:
                    if len(rows) != 1:
                        raise ValueError("missing_table_metadata")
                    metadata = _json(rows[0][0])
                    if (tuple(metadata[key] for key in ("catalog_name", "schema_name", "table_name")) != self._identity
                        or metadata["type"] != "EXTERNAL" or metadata["provider"] != "parquet"
                        or metadata["location"] not in (self._root_location, self._root_location[:-1])
                        or metadata["table_properties"]["partitionMetadataEnabled"] != "true"):
                        raise ValueError("unsupported_table_metadata")
                    partition_columns = metadata["partition_columns"]
                    if not isinstance(partition_columns, list) or not partition_columns or len(set(partition_columns)) != len(partition_columns):
                        raise ValueError("invalid_partition_columns")
                    columns = {}
                    if not isinstance(metadata["columns"], list) or not metadata["columns"]:
                        raise ValueError("missing_column_metadata")
                    for column in metadata["columns"]:
                        name = _text(column["name"])
                        if name in columns or not isinstance(column["type"], dict):
                            raise ValueError("invalid_column_metadata")
                        _text(column["type"]["name"])
                        columns[name] = column["type"]
                    for name in partition_columns:
                        kind = columns[_text(name)]
                        if kind["name"] != "string" or kind.get("collation", "UTF8_BINARY") != "UTF8_BINARY":
                            raise ValueError("nonbinary_string_partition")
                    self._definition = TableDefinition(self._table_id, self._source_id, self._root_prefix, tuple(partition_columns))
                except (KeyError, TypeError, ValueError, UnicodeError):
                    raise FatalExternalError("databricks_unsupported_table_metadata") from None
        return (self._definition,)

    def _partition(self, partition: PartitionRef):
        try:
            if (self._definition is None or partition.table_id != self._table_id or partition.source_id != self._source_id
                or not isinstance(partition.values, tuple) or len(partition.values) != len(self._definition.partition_columns)):
                raise ValueError("unresolved_partition")
            for value in partition.values:
                if "/" in _text(value):
                    raise ValueError("invalid_partition_value")
            path = "/".join(name + "=" + value for name, value in zip(self._definition.partition_columns, partition.values))
            if partition.partition_path != path or partition.storage_prefix != self._root_prefix + path + "/":
                raise ValueError("mismatched_partition_path")
            spec = "(" + ", ".join(_identifier(name) + " = " + _literal(value) for name, value in zip(self._definition.partition_columns, partition.values)) + ")"
            return spec, self._root_location + quote(path + "/", safe="/=")
        except (AttributeError, TypeError, ValueError, UnicodeError):
            raise FatalExternalError("databricks_invalid_partition_identity") from None

    def partition_exists(self, partition):
        spec, _ = self._partition(partition)
        return bool(_rows(self._statements.execute("SHOW PARTITIONS " + self._quoted_table + " PARTITION " + spec)))

    def create_partition(self, partition, location):
        spec, expected = self._partition(partition)
        if location != expected:
            raise FatalExternalError("databricks_unapproved_partition_location")
        with self._statements.operation():
            if self.partition_exists(partition):
                return False
            self._statements.execute("ALTER TABLE " + self._quoted_table + " ADD IF NOT EXISTS PARTITION " + spec + " LOCATION " + _literal(location), mutation=True)
        return True

    def delete_partition(self, partition):
        spec, _ = self._partition(partition)
        with self._statements.operation():
            if not self.partition_exists(partition):
                return False
            self._statements.execute("ALTER TABLE " + self._quoted_table + " DROP IF EXISTS PARTITION " + spec, mutation=True)
        return True
