import importlib.util
import json
from datetime import datetime, timezone

import pytest


def test_blob_url_decodes_once_and_preserves_literal_plus():
    assert importlib.util.find_spec("rtmsf.adapters.azure") is not None, "Azure adapter not implemented"
    from rtmsf.adapters.azure import decode_event

    key = "worker_probe/dt=2026-10-15/file+%20.parquet"
    body = json.dumps({
        "topic": "/subscriptions/test-sub/resourceGroups/test-rg/providers/Microsoft.Storage/storageAccounts/proofaccount",
        "subject": "/blobServices/default/containers/proof/blobs/" + key,
        "eventType": "Microsoft.Storage.BlobCreated",
        "eventTime": "2026-10-15T00:00:00Z",
        "id": "unit-test-event",
        "data": {
            "api": "PutBlob",
            "blobType": "BlockBlob",
            "contentLength": 1,
            "url": "https://proofaccount.blob.core.windows.net/proof/worker_probe/dt=2026-10-15/file%2B%2520.parquet",
        },
    })
    event, = decode_event(body, {"proof": {
        "account": "proofaccount", "container": "proof",
        "topic": "/subscriptions/test-sub/resourceGroups/test-rg/providers/Microsoft.Storage/storageAccounts/proofaccount",
    }})
    assert event.object_key == key
    assert event.source_id == "proof"
    assert event.operation == "CREATE"


def _event():
    return {
        "topic": "/subscriptions/test-sub/resourceGroups/test-rg/providers/Microsoft.Storage/storageAccounts/proofaccount",
        "subject": "/blobServices/default/containers/proof/blobs/table/dt=a/file.parquet",
        "eventType": "Microsoft.Storage.BlobCreated", "eventTime": "2026-10-15T00:00:00Z", "id": "test-event",
        "data": {"api": "PutBlob", "blobType": "BlockBlob", "url": "https://proofaccount.blob.core.windows.net/proof/table/dt=a/file.parquet"},
    }


def _decode(document):
    from rtmsf.adapters.azure import decode_event
    return decode_event(json.dumps(document), {"proof": {
        "account": "proofaccount", "container": "proof", "topic": _event()["topic"],
    }})


@pytest.mark.parametrize("api", ["PutBlob", "PutBlockList", "CopyBlob", "FlushWithClose"])
def test_fully_committed_create_apis(api):
    document = _event(); document["data"]["api"] = api
    item, = _decode(document)
    assert item.operation == "CREATE"


def test_initial_adls_create_does_not_register_content():
    document = _event(); document["data"]["api"] = "CreateFile"
    assert _decode(document) == ()


@pytest.mark.parametrize("api", ["DeleteBlob", "DeleteFile"])
def test_individual_file_deletion(api):
    document = _event(); document["eventType"] = "Microsoft.Storage.BlobDeleted"; document["data"]["api"] = api
    item, = _decode(document)
    assert item.operation == "DELETE"


@pytest.mark.parametrize("kind,api", [
    ("Microsoft.Storage.DirectoryDeleted", "DeleteDirectory"),
    ("Microsoft.Storage.BlobRenamed", "RenameFile"),
    ("Microsoft.Storage.AsyncOperationInitiated", "CopyBlob"),
    ("Microsoft.Storage.BlobCreated", "PutBlock"),
])
def test_unsupported_mutations_are_rejected(kind, api):
    document = _event(); document["eventType"] = kind; document["data"]["api"] = api
    with pytest.raises(ValueError):
        _decode(document)


@pytest.mark.parametrize("url", [
    " https://proofaccount.blob.core.windows.net/proof/table/dt=a/file.parquet",
    "http://proofaccount.blob.core.windows.net/proof/table/dt=a/file.parquet",
    "https://otheraccount.blob.core.windows.net/proof/table/dt=a/file.parquet",
    "https://proofaccount-secondary.blob.core.windows.net/proof/table/dt=a/file.parquet",
    "https://userinfo@proofaccount.blob.core.windows.net/proof/table/dt=a/file.parquet",
    "https://proofaccount.blob.core.windows.net:443/proof/table/dt=a/file.parquet",
    "https://proofaccount.blob.core.windows.net/proof/table/dt=a/file.parquet?unexpected=1",
    "https://proofaccount.blob.core.windows.net/proof/table/dt=a/file.parquet#fragment",
    "https://proofaccount.blob.core.windows.net/other/table/dt=a/file.parquet",
    "https://proofaccount.blob.core.windows.net/proof/table/dt=a/%GG.parquet",
    "https://proofaccount.blob.core.windows.net/proof/table/dt=a/%FF.parquet",
    "https://proofaccount.blob.core.windows.net/proof/table/dt=a/%00.parquet",
    "https://proofaccount.blob.core.windows.net/proof/table/dt=a/../file.parquet",
    "https://proofaccount.blob.core.windows.net/proof/table//file.parquet",
    "https://proofaccount.blob.core.windows.net/proof/table/dt=a/",
    "\thttps://proofaccount.blob.core.windows.net/proof/table/dt=a/file.parquet",
])
def test_untrusted_or_ambiguous_urls_are_rejected(url):
    document = _event(); document["data"]["url"] = url
    with pytest.raises(ValueError):
        _decode(document)


def test_other_subscription_topic_is_rejected_even_for_same_account_name():
    document = _event(); document["topic"] = document["topic"].replace("test-sub", "other-sub")
    with pytest.raises(ValueError):
        _decode(document)


def test_subject_does_not_override_the_exact_url_key():
    document = _event(); document["subject"] += ".other"
    with pytest.raises(ValueError):
        _decode(document)


def test_primary_dfs_url_and_unicode_key():
    document = _event()
    document["subject"] = "/blobServices/default/containers/proof/blobs/table/dt=a/雪.parquet"
    document["data"]["url"] = "https://proofaccount.dfs.core.windows.net/proof/table/dt=a/%E9%9B%AA.parquet"
    item, = _decode(document)
    assert item.object_key == "table/dt=a/雪.parquet"


@pytest.mark.parametrize("document", [[], {}, {"data": []}, {"data": {}, "id": 1}])
def test_one_complete_event_grid_object_is_required(document):
    with pytest.raises(ValueError):
        _decode(document)


@pytest.mark.parametrize("surrogate", ["\ud800", "\udfff"])
def test_json_escaped_surrogates_cannot_become_object_keys(surrogate):
    document = _event()
    key = "table/dt=a/" + surrogate + ".parquet"
    document["subject"] = "/blobServices/default/containers/proof/blobs/" + key
    document["data"]["url"] = "https://proofaccount.blob.core.windows.net/proof/" + key
    with pytest.raises(ValueError):
        _decode(document)


def test_literal_plus_in_url_remains_a_literal_plus():
    document = _event()
    key = "table/dt=a/file+name.parquet"
    document["subject"] = "/blobServices/default/containers/proof/blobs/" + key
    document["data"]["url"] = "https://proofaccount.blob.core.windows.net/proof/" + key
    item, = _decode(document)
    assert item.object_key == key


@pytest.mark.parametrize("value,expected", [
    ("2026-10-15T05:30:00+05:30", datetime(2026, 10, 15, tzinfo=timezone.utc)),
    ("2026-10-15T00:00:00Z", datetime(2026, 10, 15, tzinfo=timezone.utc)),
    (None, None),
    ("invalid", None),
    ("2026-10-15T00:00:00", None),
])
def test_optional_source_timestamp_is_utc_or_absent(value, expected):
    document = _event()
    if value is None:
        document.pop("eventTime")
    else:
        document["eventTime"] = value
    item, = _decode(document)
    assert item.source_event_time == expected
    assert item.ingested_at.utcoffset().total_seconds() == 0


@pytest.mark.parametrize("blob_type", [None, "AppendBlob", "PageBlob"])
def test_only_block_blob_file_events_are_supported(blob_type):
    document = _event()
    document["data"]["blobType"] = blob_type
    with pytest.raises(ValueError):
        _decode(document)
