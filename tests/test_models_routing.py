from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

from rtmsf import models


def event(key="custom/dt=2026-09-10/site= New York /a+b%2F.parquet", source="objects"):
    return models.ObjectEvent(source, key, "CREATE", datetime.now(timezone.utc))


def index(*tables):
    from rtmsf.routing import RoutingIndex
    return RoutingIndex(tables or (models.TableDefinition("orders", "objects", "custom/", ("dt", "site")),))


def test_route_preserves_exact_decoded_key_and_value_without_second_decode():
    original = event()
    routed = index().route(original)
    assert routed.event is original
    assert routed.event.object_key == original.object_key
    assert routed.partition.values == ("2026-09-10", " New York ")
    assert routed.partition.storage_prefix == "custom/dt=2026-09-10/site= New York /"
    assert routed.partition.key == ("objects", "orders", "dt=2026-09-10/site= New York ")
    with pytest.raises(FrozenInstanceError):
        routed.event.operation = "DELETE"


def test_routes_custom_roots_and_multiple_sources_without_namespace_collision():
    router = index(
        models.TableDefinition("orders", "objects", "custom/", ("dt", "site")),
        models.TableDefinition("archive", "archive", "custom/", ("dt", "site")),
        models.TableDefinition("billing", "objects", "billing/", ("account",)),
    )
    assert router.route(event(source="archive")).partition.table_id == "archive"
    assert router.route(event("billing/account=customer-1/file.parquet")).partition.values == ("customer-1",)
    assert router.route(event(source="unconfigured")) is None


@pytest.mark.parametrize("bad_key", [
    "unknown/dt=2026/site=x/a.parquet",
    "custom/dt=/site=x/a.parquet",
    "custom/site=x/dt=2026/a.parquet",
    "custom/DT=2026/site=x/a.parquet",
    "custom/dt=2026/site=x",
    "custom/_temporary/dt=2026/site=x/a.parquet",
])
def test_unmatched_or_invalid_layouts_are_not_guessed(bad_key):
    assert index().route(event(bad_key)) is None


@pytest.mark.parametrize("second", [
    models.TableDefinition("nested", "objects", "custom/nested/", ("dt",)),
    models.TableDefinition("same-root", "objects", "custom/", ("dt",)),
    models.TableDefinition("orders", "objects", "different/", ("dt",)),
])
def test_rejects_ambiguous_or_duplicate_configured_tables(second):
    with pytest.raises(ValueError):
        index(models.TableDefinition("orders", "objects", "custom/", ("dt",)), second)


def test_prefix_boundary_and_bucket_root_routing():
    router = index(models.TableDefinition("root", "a", "", ("dt",)))
    assert router.route(event("dt=t/a.parquet", "a")).partition.storage_prefix == "dt=t/"
    assert index().route(event("custom-other/dt=t/site=s/a.parquet")) is None


def test_canonical_event_requires_operation_and_utc_ingress():
    with pytest.raises(ValueError, match="operation"):
        models.ObjectEvent("s", "key", "PUT", datetime.now(timezone.utc))
    with pytest.raises(ValueError, match="UTC"):
        models.ObjectEvent("s", "key", "CREATE", datetime.now())
