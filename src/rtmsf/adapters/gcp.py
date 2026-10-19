"""GCP current-live storage truth and one synchronous operation deadline."""

from collections.abc import Mapping
from contextlib import contextmanager
from functools import partial
import math
import re
import time

from google.api_core import exceptions as api_errors
from google.auth import exceptions as auth_errors
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import RequestException

from rtmsf.interfaces import FatalExternalError, RetryableExternalError


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
            prefix = self._directory(prefix)[:-1]
            if any(segment in ("", ".", "..") for segment in prefix.split("/")):
                raise FatalExternalError("gcp_invalid_partition_directory")
            # Hive/Spark consume unescaped Hadoop Path text; percent escapes change the key.
            return f"gs://{bucket.name}/{prefix}"
