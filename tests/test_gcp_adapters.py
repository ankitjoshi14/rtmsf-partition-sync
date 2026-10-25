"""GCS current-live truth through the pinned SDK, with no cloud requests."""

import json
from urllib.parse import parse_qs, quote, urlsplit

import pytest
import requests
from google.auth.credentials import AnonymousCredentials, Credentials
from google.auth.exceptions import RefreshError, TransportError
from google.auth.transport.requests import AuthorizedSession
from google.cloud.storage import Client

from rtmsf.interfaces import FatalExternalError, RetryableExternalError


SOURCES = {"proof": "proof-bucket"}
PREFIX = "table/dt=a/"


class HTTP(requests.Session):
    is_mtls = False

    def __init__(self, replies, clock=None):
        super().__init__()
        self.replies = list(replies)
        self.calls = []
        self.clock = clock

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        reply = self.replies.pop(0)
        if self.clock is not None:
            self.clock[0] += reply[2]
        if isinstance(reply, Exception):
            raise reply
        status, body = reply[:2]
        result = requests.Response()
        result.status_code = status
        result._content = body if isinstance(body, bytes) else json.dumps(body).encode()
        result.headers["content-type"] = "application/json"
        result.request = requests.Request(method, url).prepare()
        return result


def adapter(replies, **kwargs):
    from rtmsf.adapters.gcp import GcsObjectStore

    http = HTTP(replies)
    client = Client(project="proof", credentials=AnonymousCredentials(), _http=http)
    return GcsObjectStore(client, SOURCES, **kwargs), http, client


def metadata(key, generation="1", bucket="proof-bucket"):
    return {"name": key, "bucket": bucket, "generation": generation, "size": "0"}


def item(key, bucket="proof-bucket"):
    return {"name": key, "bucket": bucket}


def query(call):
    return parse_qs(urlsplit(call[1]).query)


@pytest.mark.parametrize("key", [PREFIX + "file", PREFIX + "_SUCCESS", PREFIX + "_marker", PREFIX, PREFIX + "nested/"])
def test_every_live_object_including_zero_bytes_and_slash_objects_counts(key):
    store, http, _ = adapter([(200, metadata(key)), (200, {"items": [item(key)]})])
    assert store.head_exists("proof", key) is True
    assert store.prefix_non_empty("proof", PREFIX) is True
    assert len(http.calls) == 2


def test_head_reload_is_fresh_and_never_pinned_to_a_previous_generation():
    key = PREFIX + "file"
    store, http, _ = adapter([
        (200, metadata(key, "11")),
        (200, {"items": [dict(item(key), generation="11")]}),
        (404, {"error": {"message": "missing"}}),
        (200, {"name": "proof-bucket"}),
    ])
    assert store.head_exists("proof", key) is True
    assert store.prefix_non_empty("proof", PREFIX) is True
    assert store.head_exists("proof", key) is False
    assert all("generation" not in query(call) for call in http.calls)


def test_head_not_found_requires_fresh_successful_bucket_confirmation_each_time():
    store, http, _ = adapter([
        (404, {}), (200, {"name": "proof-bucket"}),
        (404, {}), (200, {"name": "proof-bucket"}),
    ])
    assert store.head_exists("proof", PREFIX + "missing") is False
    assert store.head_exists("proof", PREFIX + "missing") is False
    assert len(http.calls) == 4


@pytest.mark.parametrize("status,expected", [(404, FatalExternalError), (403, FatalExternalError), (429, RetryableExternalError), (503, RetryableExternalError)])
def test_missing_or_inaccessible_bucket_is_never_object_absence(status, expected):
    store, http, _ = adapter([(404, {}), (status, {"error": {"message": "private diagnostic"}})])
    with pytest.raises(expected) as caught:
        store.head_exists("proof", PREFIX + "missing")
    assert "private diagnostic" not in str(caught.value)
    assert len(http.calls) == 2


@pytest.mark.parametrize("body", [{}, {"name": "other-bucket"}, {"name": None}])
def test_bucket_confirmation_must_match_trusted_name(body):
    store, _, _ = adapter([(404, {}), (200, body)])
    with pytest.raises(FatalExternalError):
        store.head_exists("proof", PREFIX + "missing")


@pytest.mark.parametrize("body", [{}, [], metadata("other-key"), metadata(PREFIX + "file", bucket="other-bucket")])
def test_head_metadata_mismatch_cannot_be_positive_truth(body):
    store, _, _ = adapter([(200, body)])
    with pytest.raises(FatalExternalError):
        store.head_exists("proof", PREFIX + "file")


def test_empty_page_continuation_is_reissued_using_a_new_bounded_sdk_iterator():
    store, http, _ = adapter([
        (200, {"nextPageToken": "opaque+/%token"}),
        (200, {"items": [item(PREFIX + "_marker")]}),
    ])
    assert store.prefix_non_empty("proof", PREFIX) is True
    assert len(http.calls) == 2
    assert query(http.calls[0])["maxResults"] == ["1"]
    assert query(http.calls[1])["pageToken"] == ["opaque+/%token"]
    for call in http.calls:
        params = query(call)
        assert params["prefix"] == [PREFIX]
        assert params["fields"] == ["nextPageToken,items(name,bucket)"]
        assert not {"delimiter", "versions", "softDeleted", "includeFoldersAsPrefixes"} & params.keys()


def test_complete_empty_listing_is_negative_truth():
    store, http, _ = adapter([(200, {})])
    assert store.prefix_non_empty("proof", PREFIX) is False
    assert len(http.calls) == 1


def test_positive_full_page_does_not_hide_its_continuation_token():
    store, http, _ = adapter([(200, {"items": [item(PREFIX + "file")], "nextPageToken": "more"})])
    assert store.prefix_non_empty("proof", PREFIX) is True
    assert len(http.calls) == 1


def test_page_limit_fails_before_fetching_an_extra_page():
    store, http, _ = adapter([(200, {"nextPageToken": "a"}), (200, {"nextPageToken": "b"})], max_list_pages=2)
    with pytest.raises(RetryableExternalError):
        store.prefix_non_empty("proof", PREFIX)
    assert len(http.calls) == 2


@pytest.mark.parametrize("body", [
    {"nextPageToken": ""}, {"nextPageToken": 2}, {"nextPageToken": None},
    {"items": None}, {"items": {}}, {"items": [None]},
    {"items": [{}]}, {"items": [{"name": PREFIX + "file"}]},
    {"items": [item(2)]}, {"items": [item("table/dt=ab/file")]},
    {"items": [item(PREFIX + "file", "other-bucket")]},
    {"items": [item(PREFIX + "file"), item(PREFIX + "extra")]},
    {"prefixes": [PREFIX + "folder/"]},
])
def test_malformed_listing_is_never_truth(body):
    store, _, _ = adapter([(200, body)])
    with pytest.raises(FatalExternalError):
        store.prefix_non_empty("proof", PREFIX)


def test_repeated_continuation_token_fails_instead_of_looping():
    store, http, _ = adapter([(200, {"nextPageToken": "a"}), (200, {"nextPageToken": "a"})])
    with pytest.raises(FatalExternalError):
        store.prefix_non_empty("proof", PREFIX)
    assert len(http.calls) == 2


@pytest.mark.parametrize("operation", ["head", "list"])
@pytest.mark.parametrize("status,expected", [(400, FatalExternalError), (401, FatalExternalError), (403, FatalExternalError), (429, RetryableExternalError), (500, RetryableExternalError), (503, RetryableExternalError)])
def test_sdk_status_errors_have_no_implicit_retries_or_negative_truth(operation, status, expected):
    store, http, _ = adapter([(status, {"error": {"message": "private diagnostic"}})])
    with pytest.raises(expected) as caught:
        if operation == "head":
            store.head_exists("proof", PREFIX + "file")
        else:
            store.prefix_non_empty("proof", PREFIX)
    assert "private diagnostic" not in str(caught.value)
    assert len(http.calls) == 1


def test_list_not_found_is_never_empty_truth():
    store, http, _ = adapter([(404, {})])
    with pytest.raises(FatalExternalError):
        store.prefix_non_empty("proof", PREFIX)
    assert len(http.calls) == 1


@pytest.mark.parametrize("failure,expected", [
    (requests.Timeout("private diagnostic"), RetryableExternalError),
    (requests.ConnectionError("private diagnostic"), RetryableExternalError),
    (TransportError("private diagnostic"), RetryableExternalError),
    (RefreshError("private diagnostic"), FatalExternalError),
])
@pytest.mark.parametrize("operation", ["head", "list"])
def test_transport_and_auth_failures_are_sanitized(operation, failure, expected):
    store, http, _ = adapter([failure])
    with pytest.raises(expected) as caught:
        if operation == "head":
            store.head_exists("proof", PREFIX + "file")
        else:
            store.prefix_non_empty("proof", PREFIX)
    assert "private diagnostic" not in str(caught.value)
    assert len(http.calls) == 1


@pytest.mark.parametrize("operation", ["head", "list"])
def test_malformed_json_success_response_is_fatal_and_sanitized(operation):
    store, http, _ = adapter([(200, b"private malformed response {")])
    with pytest.raises(FatalExternalError) as caught:
        if operation == "head":
            store.head_exists("proof", PREFIX + "file")
        else:
            store.prefix_non_empty("proof", PREFIX)
    assert str(caught.value) == "gcp_invalid_response"
    assert len(http.calls) == 1


@pytest.mark.parametrize("prefix,location", [
    ("table/dt=a+% snow/雪/", "gs://proof-bucket/table/dt=a+% snow/雪"),
    ("table/dt=%20%2F%2E%2E/%2f%2e/", "gs://proof-bucket/table/dt=%20%2F%2E%2E/%2f%2e"),
    ("table/dt=a?b#c/", "gs://proof-bucket/table/dt=a?b#c"),
    ("table/dt=../.../dt=./", "gs://proof-bucket/table/dt=../.../dt=."),
])
def test_raw_keys_use_sdk_url_encoding_and_locations_use_unescaped_hadoop_path_text(prefix, location):
    key = prefix + "x+%/  file"
    store, http, _ = adapter([(200, metadata(key)), (200, {"items": [item(key)]})])
    assert store.head_exists("proof", key)
    assert store.prefix_non_empty("proof", prefix)
    assert urlsplit(http.calls[0][1]).path.endswith("/o/" + quote(key, safe=""))
    assert query(http.calls[1])["prefix"] == [prefix]
    assert store.location_uri("proof", prefix) == location


def test_location_removes_only_the_one_structural_directory_slash():
    store, http, _ = adapter([])
    assert store.location_uri("proof", PREFIX) == "gs://proof-bucket/table/dt=a"
    assert http.calls == []


@pytest.mark.parametrize("prefix", [
    "/", "//", "/table/dt=a/", "//table/dt=a/", "table//dt=a/",
    "table///dt=a/", "table/dt=a//", "./table/dt=a/", "../table/dt=a/",
    "table/./dt=a/", "table/../dt=a/", "table/dt=a/./", "table/dt=a/../",
])
def test_hadoop_aliased_locations_fail_but_native_gcs_reads_keep_exact_names(prefix):
    key = prefix + "file"
    store, http, _ = adapter([(200, metadata(key)), (200, {"items": [item(key)]})])
    with pytest.raises(FatalExternalError, match="^gcp_invalid_partition_directory$"):
        store.location_uri("proof", prefix)
    assert http.calls == []
    assert store.head_exists("proof", key)
    assert store.prefix_non_empty("proof", prefix)
    assert urlsplit(http.calls[0][1]).path.endswith("/o/" + quote(key, safe=""))
    assert query(http.calls[1])["prefix"] == [prefix]


@pytest.mark.parametrize("method,value", [("head_exists", "key"), ("prefix_non_empty", PREFIX), ("location_uri", PREFIX)])
def test_unknown_source_fails_before_any_storage_calls(method, value):
    store, http, _ = adapter([])
    with pytest.raises(FatalExternalError):
        getattr(store, method)("unknown", value)
    assert http.calls == []


@pytest.mark.parametrize("sources", [{}, {"proof": ""}, {"": "proof-bucket"}, {"proof": "gs://proof-bucket"}, {"proof": "proof-bucket", "other": "proof-bucket"}])
def test_invalid_sources_fail_at_configuration(sources):
    from rtmsf.adapters.gcp import GcsObjectStore

    client = Client(project="proof", credentials=AnonymousCredentials(), _http=HTTP([]))
    with pytest.raises(ValueError):
        GcsObjectStore(client, sources)


@pytest.mark.parametrize("endpoint", ["http://localhost:4443", "https://storage.example.invalid", "https://storage.googleapis.com/", None])
def test_untrusted_endpoint_is_rejected_at_configuration(endpoint):
    from rtmsf.adapters.gcp import GcsObjectStore

    client = Client(project="proof", credentials=AnonymousCredentials(), _http=HTTP([]))
    client._connection.API_BASE_URL = endpoint
    with pytest.raises(ValueError):
        GcsObjectStore(client, SOURCES)


@pytest.mark.parametrize("method,value", [("head_exists", ""), ("head_exists", "x\ud800"), ("prefix_non_empty", ""), ("prefix_non_empty", "table"), ("location_uri", "table")])
def test_invalid_input_is_rejected_without_normalization(method, value):
    store, http, _ = adapter([])
    with pytest.raises(FatalExternalError):
        getattr(store, method)("proof", value)
    assert http.calls == []


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True, "90"])
def test_invalid_deadline_budgets_fail_configuration(value):
    from rtmsf.adapters.gcp import OperationDeadline

    with pytest.raises(ValueError):
        OperationDeadline(seconds=value)
    with pytest.raises(ValueError):
        OperationDeadline(rpc_seconds=value)


@pytest.mark.parametrize("value", [0, -1, 1.5, True, "4", 100])
def test_invalid_page_bounds_fail_configuration(value):
    with pytest.raises(ValueError):
        adapter([], max_list_pages=value)


def test_shared_deadline_is_cumulative_across_storage_and_later_mutation():
    from rtmsf.adapters.gcp import OperationDeadline

    clock = [0.0]
    deadline = OperationDeadline(seconds=5, clock=lambda: clock[0])
    store, http, _ = adapter([(200, metadata(PREFIX + "file"), 3), (200, {}, 2)], deadline=deadline)
    http.clock = clock
    mutations = []
    with pytest.raises(RetryableExternalError):
        with deadline.operation():
            assert store.head_exists("proof", PREFIX + "file")
            store.prefix_non_empty("proof", PREFIX)
            deadline.check()
            mutations.append("must never happen")
    assert mutations == []
    assert [call[2]["timeout"] for call in http.calls] == [5, 2]


@pytest.mark.parametrize("replies,method", [
    ([(200, metadata(PREFIX + "file"), 5)], "head"),
    ([(200, {}, 5)], "list"),
    ([(200, {"items": [item(PREFIX + "_SUCCESS")]}, 5)], "list"),
    ([(404, {}, 3), (200, {"name": "proof-bucket"}, 2)], "head"),
])
def test_overrun_after_request_cannot_return_any_truth(replies, method):
    from rtmsf.adapters.gcp import OperationDeadline

    clock = [0.0]
    store, http, _ = adapter(replies, deadline=OperationDeadline(seconds=5, clock=lambda: clock[0]))
    http.clock = clock
    with pytest.raises(RetryableExternalError):
        if method == "head":
            store.head_exists("proof", PREFIX + "file")
        else:
            store.prefix_non_empty("proof", PREFIX)


def test_expired_outer_operation_does_not_start_a_storage_request():
    from rtmsf.adapters.gcp import OperationDeadline

    clock = [0.0]
    deadline = OperationDeadline(seconds=5, clock=lambda: clock[0])
    store, http, _ = adapter([], deadline=deadline)
    with pytest.raises(RetryableExternalError):
        with deadline.operation():
            clock[0] = 5
            store.head_exists("proof", PREFIX + "file")
    assert http.calls == []


def test_nested_scope_exception_does_not_reset_outer_deadline():
    from rtmsf.adapters.gcp import OperationDeadline

    clock = [0.0]
    deadline = OperationDeadline(seconds=5, clock=lambda: clock[0])
    with pytest.raises(RetryableExternalError):
        with deadline.operation():
            try:
                with deadline.operation():
                    clock[0] = 4
                    raise ValueError("test failure")
            except ValueError:
                pass
            assert deadline.rpc_timeout() == 1
            clock[0] = 5
            deadline.check()
    with deadline.operation():
        assert deadline.rpc_timeout() == 5


@pytest.mark.parametrize("bad_time", [1, float("nan"), float("inf")])
def test_reversed_or_nonfinite_clock_cannot_revive_deadline(bad_time):
    from rtmsf.adapters.gcp import OperationDeadline

    clock = [0.0]
    deadline = OperationDeadline(clock=lambda: clock[0])
    with pytest.raises(RetryableExternalError):
        with deadline.operation():
            clock[0] = 2
            deadline.check()
            clock[0] = bad_time
            deadline.check()


def test_default_rpc_timeout_is_finite_numeric_and_bounded_to_fifteen_seconds():
    store, http, _ = adapter([(200, metadata(PREFIX + "file")), (200, {})])
    assert store.head_exists("proof", PREFIX + "file")
    assert not store.prefix_non_empty("proof", PREFIX)
    assert all(type(call[2]["timeout"]) in (int, float) and 0 < call[2]["timeout"] <= 15 for call in http.calls)


def test_sdk_background_metadata_fetches_are_disabled_on_the_dedicated_client():
    from rtmsf.adapters.gcp import GcsObjectStore

    http = HTTP([(200, metadata(PREFIX + "file")), (200, {})])
    client = Client(project="proof", credentials=AnonymousCredentials(), _http=http)
    queued = []
    client._bucket_metadata_cache.get_or_queue_fetch = lambda name: queued.append(name)
    client._bucket_metadata_cache.check_and_evict = lambda name: queued.append(name)
    store = GcsObjectStore(client, SOURCES)
    assert store.head_exists("proof", PREFIX + "file")
    assert not store.prefix_non_empty("proof", PREFIX)
    assert queued == []
    assert len(http.calls) == 2


def test_mtls_endpoint_cannot_bypass_canonical_endpoint_guard():
    from rtmsf.adapters.gcp import GcsObjectStore

    http = HTTP([])
    http.is_mtls = True
    client = Client(project="proof", credentials=AnonymousCredentials(), _http=http)
    with pytest.raises(ValueError):
        GcsObjectStore(client, SOURCES)
    assert http.calls == []


def test_endpoint_mutation_is_rejected_before_storage_calls():
    store, http, client = adapter([])
    client._connection.API_BASE_URL = "https://storage.example.invalid"
    with pytest.raises(FatalExternalError):
        store.head_exists("proof", PREFIX + "file")
    assert http.calls == []


def test_lazy_page_timeout_is_refreshed_after_iterator_construction():
    from rtmsf.adapters.gcp import OperationDeadline

    clock = [0.0]
    deadline = OperationDeadline(seconds=5, clock=lambda: clock[0])
    store, http, client = adapter([(200, {})], deadline=deadline)
    original = client.list_blobs

    def delayed_iterator(*args, **kwargs):
        result = original(*args, **kwargs)
        clock[0] += 3
        return result

    client.list_blobs = delayed_iterator
    assert not store.prefix_non_empty("proof", PREFIX)
    assert http.calls[0][2]["timeout"] == 2


def test_expired_iterator_construction_does_not_fetch_a_page():
    from rtmsf.adapters.gcp import OperationDeadline

    clock = [0.0]
    store, http, client = adapter([], deadline=OperationDeadline(seconds=5, clock=lambda: clock[0]))
    original = client.list_blobs

    def expired_iterator(*args, **kwargs):
        result = original(*args, **kwargs)
        clock[0] += 5
        return result

    client.list_blobs = expired_iterator
    with pytest.raises(RetryableExternalError):
        store.prefix_non_empty("proof", PREFIX)
    assert http.calls == []


class RefreshingCredentials(Credentials):
    def __init__(self, ready):
        super().__init__()
        self.ready = ready
        self.refreshes = 0

    @property
    def valid(self):
        return self.ready

    def refresh(self, request):
        self.ready = True
        self.refreshes += 1

    def apply(self, headers, token=None):
        pass


@pytest.mark.parametrize("operation", ["head", "list"])
def test_standard_auth_session_does_not_replay_unauthorized_storage_request(monkeypatch, operation):
    from rtmsf.adapters.gcp import GcsObjectStore

    credentials = RefreshingCredentials(ready=True)
    session = AuthorizedSession(credentials)
    http = HTTP([(401, {})] * 3)
    monkeypatch.setattr(requests.Session, "request", lambda self, method, url, **kwargs: http.request(method, url, **kwargs))
    client = Client(project="proof", credentials=credentials, _http=session)
    store = GcsObjectStore(client, SOURCES)
    with pytest.raises(FatalExternalError) as caught:
        if operation == "head":
            store.head_exists("proof", PREFIX + "file")
        else:
            store.prefix_non_empty("proof", PREFIX)
    assert str(caught.value) == "gcp_status_401"
    assert len(http.calls) == 1
    assert credentials.refreshes == 0
    assert http.calls[0][2]["timeout"] == 15


def test_standard_auth_session_preserves_initial_before_request_refresh(monkeypatch):
    from rtmsf.adapters.gcp import GcsObjectStore

    credentials = RefreshingCredentials(ready=False)
    session = AuthorizedSession(credentials)
    http = HTTP([(200, metadata(PREFIX + "file"))])
    monkeypatch.setattr(requests.Session, "request", lambda self, method, url, **kwargs: http.request(method, url, **kwargs))
    client = Client(project="proof", credentials=credentials, _http=session)
    assert GcsObjectStore(client, SOURCES).head_exists("proof", PREFIX + "file")
    assert credentials.refreshes == 1
    assert len(http.calls) == 1
