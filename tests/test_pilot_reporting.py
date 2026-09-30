import math
import sys
from types import SimpleNamespace

import boto3
import pytest

from experiments.aws import run, workloads
from experiments.figures import graph_row, render_figure


def test_graph5_rejects_shared_bucket_collection_before_aws(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['run', '--stack', 'rtmsf-pilot', '--profile', 'fixture',
        '--expected-account', '123456789012', '--graph5-only', '--request-source', 'cloudtrail'])
    monkeypatch.setattr(boto3, 'Session', lambda **kwargs: pytest.fail('must validate before AWS access'))
    with pytest.raises(SystemExit) as error:
        run.main()
    assert error.value.code == 2
    assert 'Graph 5 requires isolated CloudWatch buckets' in capsys.readouterr().err


def test_legacy_cloudtrail_cleanup_reaches_account_preflight(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['run', '--stack', 'rtmsf-pilot', '--profile', 'fixture',
        '--expected-account', '123456789012', '--request-source', 'cloudtrail', '--cleanup-run', 'old-run'])
    def preflight(**kwargs):
        raise RuntimeError('account preflight reached')
    monkeypatch.setattr(boto3, 'Session', preflight)
    with pytest.raises(RuntimeError, match='account preflight reached'):
        run.main()


def bare_pilot(deadline):
    pilot = object.__new__(run.Pilot)
    pilot.deadline, pilot.graph_results = deadline, []
    pilot.record = lambda *args, **kwargs: None
    pilot.crawler = None
    return pilot


def test_comparison_excludes_setup_reads_assigned_two_minutes_later(monkeypatch):
    # AWS labeled three reads completed at 07:32:56 in the 07:34 metric bin.
    # Starting the trial at 07:34 silently counted those setup reads as MSCK work.
    clock = [7 * 3600 + 32 * 60 + 56]
    late_setup_bin = 7 * 3600 + 34 * 60
    monkeypatch.setattr(run.time, "time", lambda: clock[0])
    monkeypatch.setattr(workloads, "wait_until", lambda until, reason: clock.__setitem__(0, until))
    pilot = bare_pilot(math.inf)
    pilot.fixtures = {"trial": {}}
    pilot.trial_buckets = object()
    pilot.verify = pilot.drain = lambda *args: None
    pilot.values = lambda index: ("p000100",)

    def upload(*args):
        assert clock[0] >= late_setup_bin + 60, "measurement includes late setup reads"
        raise InterruptedError("stop before real workload")

    pilot.upload = upload
    with pytest.raises(InterruptedError, match="stop before real workload"):
        pilot.comparison(dict(id="trial", method="rtmsf", graph=2, base=100))


def test_unstarted_and_interrupted_trials_are_retained_without_inflating_attempts():
    trial = dict(id="lifecycle_r1", graph=1, method="rtmsf", repetition=1)
    pilot = bare_pilot(-math.inf)
    pilot.attempt(trial, lambda: pytest.fail("A skipped action must not run"))
    assert pilot.graph_results[0]["attempted"] is False
    figure = render_figure(1, [], {"graph_results": pilot.graph_results})
    caption = " ".join(text.get_text() for text in figure.texts)
    assert "No observations recorded for repetition 1" in caption
    assert "Some trials failed, incomplete, or unattempted" in caption

    pilot = bare_pilot(math.inf)
    def interrupt():
        raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        pilot.attempt(trial, interrupt)
    assert pilot.graph_results[0]["attempted"] is True
    assert pilot.graph_results[0]["status"] == "incomplete"


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_attempt_cleans_up_failed_crawler_initialization_and_retains_errors(cleanup_fails):
    pilot = bare_pilot(math.inf)
    deleted = []
    def delete():
        deleted.append(True)
        if cleanup_fails:
            raise RuntimeError("delete failed")
    pilot.crawler = SimpleNamespace(delete=delete)
    def action():
        raise ValueError("initialization failed")
    trial = dict(id="g2_glue_event_crawler_1_r1", graph=2, method="glue_event_crawler", x=1, repetition=1)
    if cleanup_fails:
        with pytest.raises(RuntimeError, match="cleanup"):
            pilot.attempt(trial, action)
    else:
        pilot.attempt(trial, action)
        assert pilot.crawler is None
    assert deleted == [True]
    assert all(record["status"] == "failed" for record in pilot.graph_results)
    assert all("initialization failed" in record["error"] for record in pilot.graph_results)
    if cleanup_fails:
        assert all("delete failed" in record["error"] for record in pilot.graph_results)


@pytest.mark.parametrize("audit_mode", ["exception", "empty", "partial", "setup_failure", "recovery_only", "cloudwatch", "graph4_only", "graph5_only", "comparisons_only", "clean", "mixed", "budget_exhausted", "stale_build", "partial_build"])
def test_main_retains_blanks_and_never_passes_requests_without_audit(monkeypatch, tmp_path, audit_mode):
    captured, calls = {}, []
    outputs = {"TrailName": "trail", "EventQueueUrl": "queue", "CrawlerEventQueueUrl": "crawler_queue"}
    client = SimpleNamespace(
        get_caller_identity=lambda: {"Account": "123456789012"},
        describe_stacks=lambda **kwargs: {"Stacks": [{"StackStatus": "CREATE_COMPLETE", "Outputs": [
            {"OutputKey": key, "OutputValue": value} for key, value in outputs.items()]}]},
        start_logging=lambda **kwargs: calls.append("start_logging"),
        stop_logging=lambda **kwargs: calls.append("stop_logging"),
        purge_queue=lambda **kwargs: None,
    )
    session = SimpleNamespace(client=lambda *args, **kwargs: client)

    class FakePilot:
        attempt = run.Pilot.attempt

        def __init__(self, session, outputs, bundle, run_id, args):
            self.args = args
            self.graph_rows, self.graph_results, self.recovery_results, self.checks = [], [], [], []
            self.windows, self.lifecycle, self.fixtures = [], [], {}
            self.metadata = {"graph_results": self.graph_results}
            self.clients = {"sqs": client}
            self.deadline = 0 if audit_mode == "budget_exhausted" else math.inf
            self.crawler = None
            self.trial_buckets = None
            self.pending_verifications = []

        def fixture(self, name, *args, **kwargs):
            self.fixtures[name] = {}
            captured.setdefault("fixture_names", []).append(name)

        def recovery(self, case, fixture, repetition):
            captured.setdefault("recovery_cases", []).append(case)
            self.recovery_results.append(dict(case=case, repetition=repetition, status="passed"))

        def comparison(self, trial):
            if trial["graph"] == 2:
                self.graph_rows.append(graph_row(3, trial["method"], "visibility_delay_s", trial["x"], 1, trial["repetition"]))

        def collect_runtime(self):
            if audit_mode not in ("clean", "mixed", "budget_exhausted", "graph5_only"):
                raise RuntimeError("runtime logs unavailable")

        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    class FakeAudit:
        coverage = {}

        def __init__(self, *args):
            pass

        def calibrate(self):
            assert captured.get('activation_waited'), 'calibration ran before selector/metric activation'
            if audit_mode == "setup_failure":
                raise RuntimeError("calibration setup failed")

        def collect(self, *args, **kwargs):
            if audit_mode == "exception":
                raise RuntimeError("request audit unavailable")
            if audit_mode == "partial":
                return {"g2_rtmsf_1_r1": {"s3_head_requests": 1}}
            return {}

    class FakeBuckets:
        buckets = {'trial': 'rtmsf-g2-test'}
        state = {'trials': {'trial': {'metrics_enabled_at': '2026-09-22T00:00:00+00:00'}}}
        def __init__(self, *args):
            pass
        def provision(self, matrix):
            captured['matrix'] = matrix
        def enable_notifications(self, *args):
            pass
        def cleanup(self):
            captured['buckets_cleaned'] = True

    class FakeMetrics:
        coverage = {'source': 'cloudwatch'}
        def __init__(self, *args):
            pass
        def collect(self, windows):
            if audit_mode == 'exception':
                raise RuntimeError('request metrics unavailable')
            if audit_mode in ('empty', 'setup_failure'):
                return {}
            if audit_mode == 'partial':
                return {'g2_rtmsf_1_r1': {'s3_head_requests': 1}}
            return {t['id']: {metric: 1 for metric in run.METRICS} for t in captured['matrix']}

    monkeypatch.setattr(boto3, "Session", lambda **kwargs: session)
    monkeypatch.setattr(run, "Pilot", FakePilot)
    monkeypatch.setattr(run, "RequestAudit", FakeAudit)
    monkeypatch.setattr(run, "TrialBuckets", FakeBuckets)
    monkeypatch.setattr(run, "RequestMetrics", FakeMetrics)
    monkeypatch.setattr(run, "wait_until", lambda *args: captured.update(activation_waited=True))
    monkeypatch.setattr(run, "source_snapshot", lambda root: {"files": {}})
    monkeypatch.setattr(run, "prepare_checks", lambda pilot: None)
    monkeypatch.setattr(run, "run_checks", lambda pilot: [
        dict(name=name, repetition=repetition, status="passed")
        for repetition in range(1, pilot.args.correctness_repetitions + 1)
        for name in run.CHECK_NAMES
    ] if audit_mode in ("clean", "mixed") else [])
    monkeypatch.setattr(run.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(run, "write_reports", lambda root, rows, recovery, checks, metadata:
                        captured.update(rows=rows, recovery=recovery, checks=checks, metadata=metadata))
    argv = ["run", "--stack", "rtmsf-pilot",
                        "--expected-account", "123456789012", "--profile", "fixture", "--sizes", "1",
                        "--files", "1", "--graph4-partitions", "1", "--layout-sizes", "1", "--output-dir", str(tmp_path),
                        "--request-source", "cloudwatch"]
    if audit_mode in ("clean", "mixed"):
        argv += ["--max-minutes", "240"]
    if audit_mode == "mixed":
        argv += ["--repetitions", "5", "--correctness-repetitions", "3"]
    if audit_mode == "recovery_only":
        argv += ["--recovery-only", "add_after_catalog_change"]
    if audit_mode == "graph4_only":
        argv += ["--graph4-only"]
    if audit_mode == "graph5_only":
        argv += ["--graph5-only"]
    if audit_mode == "comparisons_only":
        argv += ["--comparisons-only"]
    if audit_mode in ('stale_build', 'partial_build'):
        import json
        build = {'verified_runtime_source_files': {}} if audit_mode == 'stale_build' else {'files': {}, 'image': 'repo@sha256:' + 'a' * 64}
        manifest = tmp_path / 'build.json'
        manifest.write_text(json.dumps(build))
        argv += ['--build-manifest', str(manifest)]
    monkeypatch.setattr(sys, "argv", argv)
    if audit_mode in ('stale_build', 'partial_build'):
        with pytest.raises(ValueError, match='build manifest needs|runtime files differ'):
            run.main()
        assert calls == [], 'invalid build must be rejected before AWS mutations'
        return
    status = run.main()
    assert status == (0 if audit_mode in ("clean", "mixed", "graph5_only") else 1)
    assert captured["metadata"]["run_status"] == ("passed" if status == 0 else "incomplete_or_failed")
    if audit_mode in ("clean", "mixed"):
        repeats = 3 if audit_mode == "mixed" else 1
        assert len(captured["matrix"]) == (45 if audit_mode == "mixed" else 9)
        assert len(captured["recovery"]) == 4 * repeats
        assert len(captured["checks"]) == 7 * repeats
        assert sum(name.startswith("lifecycle_") for name in captured["fixture_names"]) == repeats
        assert len([r for r in captured["rows"] if r["graph"] == 3]) == (15 if audit_mode == "mixed" else 3)
        return
    if audit_mode == "graph5_only":
        assert captured['metadata']['graphs'] == [5]
        assert captured['checks'] == captured['recovery'] == []
        assert len(captured['matrix']) == 4
        assert len(captured['rows']) == 12
        assert {row['layout'] for row in captured['rows']} == {'day_first', 'tenant_first'}
        assert all(row['graph'] == 5 and row['y_value'] == 1 for row in captured['rows'])
        return
    if audit_mode == "budget_exhausted":
        assert all(result["attempted"] is False for result in captured["recovery"])
        assert len(captured["recovery"]) == 4
        return
    if audit_mode in ("cloudwatch", "graph4_only", "comparisons_only"):
        assert captured['buckets_cleaned']
        assert captured['metadata']['request_coverage']['source'] == 'cloudwatch'
        assert all(r['y_value'] == 1 for r in captured['rows'] if r['graph'] in (2, 4))
        if audit_mode == "graph4_only":
            assert captured['metadata']['graphs'] == [4]
            assert captured['checks'] == []
            assert captured['fixture_names'] == ['g4_rtmsf_1_r1', 'g4_glue_event_crawler_1_r1']
            assert not captured.get('recovery_cases')
            assert len(captured['rows']) == 6
            assert all(row['graph'] == 4 for row in captured['rows'])
        if audit_mode == "comparisons_only":
            assert captured['metadata']['graphs'] == [2, 3]
            assert captured['checks'] == []
            assert len(captured['matrix']) == 3
            assert all(trial['graph'] == 2 for trial in captured['matrix'])
            assert not captured.get('recovery_cases')
        return
    if audit_mode == "recovery_only":
        assert captured["rows"] == captured["checks"] == []
        assert captured["metadata"]["graphs"] == []
        assert captured["fixture_names"] == ["add_after_catalog_change_r1"]
        assert captured["recovery_cases"] == ["add_after_catalog_change"]
        assert calls == ["start_logging", "stop_logging"]
        return
    requests = [row for row in captured["rows"] if row["graph"] in (2, 4)]
    assert len(requests) == 15
    assert sum(row["y_value"] is not None for row in requests) == (1 if audit_mode == "partial" else 0)
    results = captured["metadata"]["graph_results"]
    assert len(results) == 13
    assert all(result["status"] == "incomplete" for result in results if result["graph"] in (2, 4))
    assert all(check["attempted"] is False for check in captured["checks"])
    if audit_mode == "setup_failure":
        assert all(result["attempted"] is False for result in results)
    else:
        assert all(result["status"] == "passed" for result in results if result["graph"] == 3)
    assert calls == ["start_logging", "stop_logging"]
