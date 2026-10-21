"""HNS primary-storage truth contract; execute these tests in Azure."""

from types import SimpleNamespace

import pytest
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceRequestError, ServiceResponseError

from rtmsf.interfaces import FatalExternalError, RetryableExternalError


SOURCES = {"proof": {
    "account": "proofaccount", "container": "proof",
    "topic": "/subscriptions/test-sub/resourceGroups/test-rg/providers/Microsoft.Storage/storageAccounts/proofaccount",
}}
PREFIX = "table/dt=a/"


def response_error(status, code):
    error = ResourceNotFoundError("not found") if status == 404 else HttpResponseError("request failed")
    error.status_code, error.error_code = status, code
    return error


class Blobs:
    def __init__(self, objects):
        self.objects = objects
        self.calls = []

    def get_blob_client(self, blob):
        def properties(**kwargs):
            self.calls.append((blob, kwargs))
            value = self.objects[blob]
            if isinstance(value, Exception):
                raise value
            resource_type, copy_status = value
            headers = {} if resource_type is None else {"x-ms-resource-type": resource_type}
            kwargs["raw_response_hook"](SimpleNamespace(http_response=SimpleNamespace(headers=headers)))
            return SimpleNamespace(copy=SimpleNamespace(status=copy_status), size=0)
        return SimpleNamespace(get_blob_properties=properties)


class Pages:
    def __init__(self, pages):
        self.pages = list(pages)
        self.continuation_token = None
        self.fetched = 0

    def by_page(self):
        return self

    def __iter__(self):
        return self

    def __next__(self):
        if not self.pages:
            raise StopIteration
        self.fetched += 1
        page = self.pages.pop(0)
        self.continuation_token = "opaque-next-page" if self.pages else None
        if isinstance(page, Exception):
            raise page
        return iter(page)


class Filesystem:
    def __init__(self, pages):
        self.pages = Pages(pages)
        self.calls = []

    def get_paths(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages


def path(name, directory=False):
    return SimpleNamespace(name=name, is_directory=directory, content_length=0)


def store(blobs, filesystem):
    from rtmsf.adapters.azure import AzureObjectStore
    return AzureObjectStore({"proof": blobs}, {"proof": filesystem}, SOURCES)


def test_zero_byte_file_is_positive_but_directory_is_not_file_evidence():
    blobs = Blobs({PREFIX + "empty.parquet": ("file", None), PREFIX + "directory.parquet": ("directory", None)})
    adapter = store(blobs, Filesystem([]))
    assert adapter.head_exists("proof", PREFIX + "empty.parquet") is True
    assert adapter.head_exists("proof", PREFIX + "directory.parquet") is False


@pytest.mark.parametrize("status,code,expected", [
    (404, "BlobNotFound", False),
    (404, "ContainerNotFound", FatalExternalError),
    (403, "AuthorizationFailure", FatalExternalError),
    (429, "ServerBusy", RetryableExternalError),
    (503, "ServerBusy", RetryableExternalError),
])
def test_only_exact_blob_not_found_is_negative_truth(status, code, expected):
    adapter = store(Blobs({PREFIX + "file": response_error(status, code)}), Filesystem([]))
    if expected is False:
        assert adapter.head_exists("proof", PREFIX + "file") is False
    else:
        with pytest.raises(expected):
            adapter.head_exists("proof", PREFIX + "file")


@pytest.mark.parametrize("resource_type,copy_status,expected", [
    (None, None, RetryableExternalError),
    ("unexpected", None, RetryableExternalError),
    ("file", "pending", RetryableExternalError),
    ("file", "failed", FatalExternalError),
    ("file", "aborted", FatalExternalError),
])
def test_uncertain_or_failed_copy_never_becomes_empty_evidence(resource_type, copy_status, expected):
    adapter = store(Blobs({PREFIX + "file": (resource_type, copy_status)}), Filesystem([]))
    with pytest.raises(expected):
        adapter.head_exists("proof", PREFIX + "file")


def test_empty_page_with_continuation_does_not_mean_empty_partition():
    blobs = Blobs({PREFIX + "file.parquet": ("file", "success")})
    fs = Filesystem([[], [path(PREFIX + "file.parquet")]])
    assert store(blobs, fs).prefix_non_empty("proof", PREFIX) is True
    assert fs.pages.fetched == 2
    assert fs.calls[0]["path"] == PREFIX[:-1]
    assert fs.calls[0]["recursive"] is True
    assert 0 < fs.calls[0]["max_results"] <= 64


def test_complete_listing_of_directories_and_hidden_control_files_is_empty():
    fs = Filesystem([[path(PREFIX + "nested", True), path(PREFIX + "_SUCCESS"), path(PREFIX + ".temporary")], []])
    blobs = Blobs({})
    assert store(blobs, fs).prefix_non_empty("proof", PREFIX) is False
    assert fs.pages.fetched == 2
    assert blobs.calls == []


def test_stale_missing_candidate_is_skipped_but_later_live_file_counts():
    blobs = Blobs({PREFIX + "old": response_error(404, "BlobNotFound"), PREFIX + "new": ("file", None)})
    fs = Filesystem([[path(PREFIX + "old"), path(PREFIX + "new")]])
    assert store(blobs, fs).prefix_non_empty("proof", PREFIX) is True
    assert [call[0] for call in blobs.calls] == [PREFIX + "old", PREFIX + "new"]


def test_uncertain_candidate_prevents_negative_truth():
    blobs = Blobs({PREFIX + "file": ("file", "pending")})
    with pytest.raises(RetryableExternalError):
        store(blobs, Filesystem([[path(PREFIX + "file")]])).prefix_non_empty("proof", PREFIX)


def test_page_budget_stops_before_fetching_an_extra_page():
    fs = Filesystem([[], [], [path(PREFIX + "file")]])
    with pytest.raises(RetryableExternalError):
        store(Blobs({}), fs).prefix_non_empty("proof", PREFIX)
    assert fs.pages.fetched == 2


def test_candidate_budget_does_not_imply_empty_partition():
    keys = [PREFIX + str(index) for index in range(5)]
    blobs = Blobs({key: response_error(404, "BlobNotFound") for key in keys})
    with pytest.raises(RetryableExternalError):
        store(blobs, Filesystem([[path(key) for key in keys]])).prefix_non_empty("proof", PREFIX)
    assert len(blobs.calls) == 4


def test_out_of_prefix_listing_entry_is_a_fatal_contract_violation():
    with pytest.raises(FatalExternalError):
        store(Blobs({}), Filesystem([[path("table/dt=ab/file")]])).prefix_non_empty("proof", PREFIX)


@pytest.mark.parametrize("code,expected", [("PathNotFound", False), ("FilesystemNotFound", FatalExternalError)])
def test_listing_not_found_distinguishes_directory_from_filesystem(code, expected):
    adapter = store(Blobs({}), Filesystem([response_error(404, code)]))
    if expected is False:
        assert adapter.prefix_non_empty("proof", PREFIX) is False
    else:
        with pytest.raises(expected):
            adapter.prefix_non_empty("proof", PREFIX)


def test_location_uses_trusted_account_and_encodes_exact_prefix_once():
    blobs = Blobs({}); adapter = store(blobs, Filesystem([]))
    assert adapter.location_uri("proof", "table/dt=a+%/雪/") == "abfss://proof@proofaccount.dfs.core.windows.net/table/dt=a%2B%25/%E9%9B%AA/"
    with pytest.raises(FatalExternalError):
        adapter.location_uri("other", PREFIX)
    assert blobs.calls == []


@pytest.mark.parametrize("key", [
    "", "/table/file", "table//file", "table/./file", "table/../file", "table/file/",
    "table/\x00file", "table/\x7ffile", "table/\ud800file",
])
def test_invalid_exact_keys_are_rejected_before_storage_calls(key):
    blobs = Blobs({}); fs = Filesystem([]); adapter = store(blobs, fs)
    with pytest.raises(FatalExternalError):
        adapter.head_exists("proof", key)
    assert blobs.calls == [] and fs.calls == []


@pytest.mark.parametrize("prefix", ["", "table/dt=a", "/table/", "table//", "table/../", "table/\ud800/"])
def test_invalid_partition_directories_are_rejected_before_listing_or_location(prefix):
    blobs = Blobs({}); fs = Filesystem([]); adapter = store(blobs, fs)
    with pytest.raises(FatalExternalError):
        adapter.prefix_non_empty("proof", prefix)
    with pytest.raises(FatalExternalError):
        adapter.location_uri("proof", prefix)
    assert blobs.calls == [] and fs.calls == []


@pytest.mark.parametrize("key", [PREFIX + "_SUCCESS", PREFIX + ".temporary", PREFIX + "_temporary/file.parquet"])
def test_control_paths_cannot_count_as_observed_or_listed_file_evidence(key):
    blobs = Blobs({key: ("file", None)}); fs = Filesystem([[path(key)]])
    adapter = store(blobs, fs)
    assert adapter.head_exists("proof", key) is False
    assert adapter.prefix_non_empty("proof", PREFIX) is False
    assert blobs.calls == []


@pytest.mark.parametrize("value", ["_a", ".a"])
def test_hidden_prefix_in_partition_value_remains_file_evidence(value):
    prefix = f"table/dt={value}/"
    key = prefix + "file.parquet"
    adapter = store(Blobs({key: ("file", None)}), Filesystem([[path(key)]]))
    assert adapter.head_exists("proof", key) is True
    assert adapter.prefix_non_empty("proof", prefix) is True


@pytest.mark.parametrize("failure", [ServiceRequestError("connection failed"), ServiceResponseError("read failed")])
def test_transport_failure_is_never_negative_evidence(failure):
    adapter = store(Blobs({PREFIX + "file": failure}), Filesystem([failure]))
    with pytest.raises(RetryableExternalError):
        adapter.head_exists("proof", PREFIX + "file")
    with pytest.raises(RetryableExternalError):
        adapter.prefix_non_empty("proof", PREFIX)


def test_missing_listing_type_is_not_ignored_as_empty():
    item = path(PREFIX + "file"); item.is_directory = None
    with pytest.raises(FatalExternalError):
        store(Blobs({}), Filesystem([[item]])).prefix_non_empty("proof", PREFIX)


def test_listing_time_budget_overrun_cannot_return_negative_truth(monkeypatch):
    from rtmsf.adapters import azure
    fs = Filesystem([[]]); adapter = store(Blobs({}), fs); original = fs.get_paths
    clock = [0.0]; monkeypatch.setattr(azure.time, "monotonic", lambda: clock[0])

    def slow_listing(**kwargs):
        result = original(**kwargs)
        clock[0] += 31
        return result

    fs.get_paths = slow_listing
    with pytest.raises(RetryableExternalError):
        adapter.prefix_non_empty("proof", PREFIX)
    assert fs.pages.fetched == 0


def test_candidate_head_time_budget_overrun_cannot_return_positive_truth(monkeypatch):
    from rtmsf.adapters import azure
    key = PREFIX + "file"; blobs = Blobs({key: ("file", None)}); original = blobs.get_blob_client
    adapter = store(blobs, Filesystem([[path(key)]]))
    clock = [0.0]; monkeypatch.setattr(azure.time, "monotonic", lambda: clock[0])

    def slow_client(blob):
        client = original(blob); original_head = client.get_blob_properties

        def slow_head(**kwargs):
            result = original_head(**kwargs)
            clock[0] += 31
            return result

        client.get_blob_properties = slow_head
        return client

    blobs.get_blob_client = slow_client
    with pytest.raises(RetryableExternalError):
        adapter.prefix_non_empty("proof", PREFIX)


@pytest.mark.parametrize("operation", ["DataLake.ListPaths", "Blob.GetProperties"])
def test_prefix_telemetry_failure_propagates_for_stop_and_replay(operation):
    from rtmsf.adapters.azure import AzureObjectStore

    failure = OSError("telemetry unavailable")
    failed = []

    def emit(record):
        if record["operation"] == operation and not failed:
            failed.append(True)
            raise failure

    key = PREFIX + "file"
    telemetry = SimpleNamespace(emit=emit)
    adapter = AzureObjectStore(
        {"proof": Blobs({key: ("file", None)})},
        {"proof": Filesystem([[path(key)]])}, SOURCES, telemetry=telemetry,
    )
    with pytest.raises(OSError) as caught:
        adapter.prefix_non_empty("proof", PREFIX)
    assert caught.value is failure
    telemetry.emit({"operation": operation})
