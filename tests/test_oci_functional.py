import pytest

pytest.importorskip("oci")

from experiments.oci.run import MemoryCatalog, normalize_report


def test_instance_authentication_never_reads_local_credentials(monkeypatch):
    from types import SimpleNamespace
    from experiments.oci import run
    signer = object()
    monkeypatch.setattr(run.oci.auth.signers, "InstancePrincipalsSecurityTokenSigner", lambda: signer)
    monkeypatch.setattr(run.oci.config, "from_file", lambda *args: pytest.fail("instance must not read user credentials"))
    config, options = run.client_options(SimpleNamespace(instance_principal=True), "us-sanjose-1")
    assert config == {"region": "us-sanjose-1"}
    assert options["signer"] is signer
    assert options["timeout"] == (3, 10)


@pytest.mark.parametrize("prefix", ["../", "/production/", "table/dt=a/", "", "table"])
def test_lifecycle_prefix_rejects_unscoped_paths(prefix):
    import argparse
    from experiments.oci import run
    with pytest.raises(argparse.ArgumentTypeError):
        run.table_prefix(prefix)


def test_lifecycle_prefix_accepts_fresh_test_root():
    from experiments.oci import run
    assert run.table_prefix("compute-life-20261014T120000/") == "compute-life-20261014T120000/"


def test_restart_requires_same_event_and_ack_after_reconciliation():
    from experiments.oci.restart import verify_replay
    records = [{"action": "delivery_buffered", "event_count": 1},
               {"action": "pmcp_result", "status": "APPLIED", "mutation": "ADD", "observation_count": 1},
               {"action": "delivery_acknowledged", "event_count": 1, "dead_lettered": False}]
    assert verify_replay({"original"}, {"original"}, records)["acknowledgements"] == 1
    with pytest.raises(AssertionError, match="same event"):
        verify_replay({"original"}, {"different"}, records)
    with pytest.raises(AssertionError, match="before reconciliation"):
        verify_replay({"original"}, {"original"}, [records[0], records[2], records[1]])
    with pytest.raises(AssertionError, match="all input"):
        verify_replay({"original"}, {"original"}, records[:2])


def test_report_never_includes_receipts():
    report = normalize_report({"case": "first-object", "receipt": "private-receipt", "body": "private-body", "status": "passed"})
    assert report == {"case": "first-object", "status": "passed"}


def test_catalog_locations_are_exact_and_idempotent():
    from rtmsf.models import PartitionRef
    p = PartitionRef("demo", "demo.table", "dt=a", ("a",), "table/dt=a/")
    c = MemoryCatalog()
    assert c.create_partition(p, "oci://bucket@ns/table/dt=a/")
    assert not c.create_partition(p, "oci://bucket@ns/table/dt=a/")
    assert c.partition_exists(p)
    assert c.delete_partition(p)
    assert not c.delete_partition(p)
