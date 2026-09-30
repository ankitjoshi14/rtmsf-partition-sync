from collections import Counter

import pytest

from rtmsf.models import PartitionIntent, PartitionRef
from rtmsf.interfaces import FatalExternalError, RetryableExternalError


class Store:
    def __init__(self, keys=()):
        self.keys = set(keys)
        self.calls = Counter()

    def head_exists(self, source_id, key):
        self.calls["HEAD"] += 1
        return (source_id, key) in self.keys

    def prefix_non_empty(self, source_id, prefix):
        self.calls["LIST"] += 1
        return any(source == source_id and key.startswith(prefix) for source, key in self.keys)

    def location_uri(self, source_id, prefix):
        return f"memory://{source_id}/{prefix}"


class Catalog:
    def __init__(self):
        self.entries = {}
        self.calls = Counter()

    def partition_exists(self, partition):
        self.calls["read"] += 1
        return partition.key in self.entries

    def create_partition(self, partition, location):
        self.calls["create"] += 1
        if partition.key in self.entries:
            return False
        self.entries[partition.key] = location
        return True

    def delete_partition(self, partition):
        self.calls["delete"] += 1
        return self.entries.pop(partition.key, None) is not None


def fixture(keys=("root/dt=t/a.parquet",), mutation="ADD"):
    from rtmsf.pmcp import PartitionMutationCommitProtocol
    store = Store(("s", key) for key in keys)
    catalog = Catalog()
    partition = PartitionRef("s", "orders", "dt=t", ("t",), "root/dt=t/")
    intent = PartitionIntent(partition, mutation, ("root/dt=t/a.parquet",))
    return PartitionMutationCommitProtocol(store, catalog), store, catalog, intent


def test_add_verifies_storage_then_replay_does_not_duplicate_mutation():
    pmcp, store, catalog, intent = fixture()
    assert pmcp.apply(intent).status == "APPLIED"
    assert catalog.entries == {intent.partition.key: "memory://s/root/dt=t/"}
    assert pmcp.apply(intent).status == "NOOP"
    assert catalog.calls["create"] == 1
    assert store.calls["HEAD"] == 1


def test_vanished_observed_object_falls_back_to_partition_storage():
    pmcp, store, catalog, intent = fixture(keys=("root/dt=t/another.parquet",))
    assert pmcp.apply(intent).status == "APPLIED"
    assert store.calls == {"HEAD": 1, "LIST": 1}


def test_empty_add_retries_and_removes_an_existing_stale_entry():
    pmcp, store, catalog, intent = fixture(keys=())
    assert pmcp.apply(intent).status == "RETRY"
    assert not catalog.entries
    catalog.create_partition(intent.partition, "stale")
    assert pmcp.apply(intent).status == "RETRY"
    assert not catalog.entries


def test_partial_delete_keeps_partition_last_delete_removes_it_idempotently():
    pmcp, store, catalog, intent = fixture(mutation="DROP")
    catalog.create_partition(intent.partition, "memory://s/root/dt=t/")
    assert pmcp.apply(intent).reason == "still_non_empty"
    assert len(catalog.entries) == 1
    store.keys.clear()
    assert pmcp.apply(intent).status == "APPLIED"
    assert pmcp.apply(intent).status == "NOOP"
    assert catalog.calls["delete"] == 1


def test_delete_observation_repairs_missing_catalog_when_other_data_survives():
    pmcp, store, catalog, intent = fixture(keys=("root/dt=t/survivor.parquet",), mutation="DROP")
    assert pmcp.apply(intent).status == "APPLIED"
    assert catalog.entries == {intent.partition.key: "memory://s/root/dt=t/"}
    assert sum(store.calls.values()) + sum(catalog.calls.values()) == 3


@pytest.mark.parametrize("error,status", [(RetryableExternalError, "RETRY"), (FatalExternalError, "ERROR")])
def test_provider_failure_is_never_reported_as_success(error, status):
    pmcp, store, catalog, intent = fixture()
    def unavailable(partition):
        raise error("service unavailable")
    catalog.partition_exists = unavailable
    assert pmcp.apply(intent).status == status
    assert not catalog.entries


def test_verification_budget_is_bounded_and_cannot_use_another_prefix():
    pmcp, store, catalog, intent = fixture(keys=("elsewhere/a.parquet", "root/dt=t/unobserved.parquet"))
    intent = PartitionIntent(intent.partition, "ADD", tuple(f"root/dt=t/{i}.parquet" for i in range(20)))
    assert pmcp.apply(intent).status == "APPLIED"
    assert sum(store.calls.values()) + sum(catalog.calls.values()) <= 6
    pmcp, store, catalog, intent = fixture(keys=("elsewhere/a.parquet",))
    assert pmcp.apply(PartitionIntent(intent.partition, "ADD", ("elsewhere/a.parquet",))).status == "RETRY"
    assert store.calls["HEAD"] == 0
    assert not catalog.entries
