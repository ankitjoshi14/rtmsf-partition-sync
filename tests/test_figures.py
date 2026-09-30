import csv
import math

import pytest


def test_layout_graph_keeps_conditions_and_missing_counts_separate(tmp_path):
    from experiments.figures import graph_row, render_figure, write_reports, main

    rows = [graph_row(5, method, metric, 100, value, layout=layout)
            for layout, counts in [('day_first', {'rtmsf': (1, 0, 0), 'athena_msck': (4, 0, 3)}),
                                   ('tenant_first', {'rtmsf': (1, None, 0), 'athena_msck': (104, 0, 103)})]
            for method, values in counts.items()
            for metric, value in zip(('s3_head_requests', 's3_get_requests', 's3_list_requests'), values)]
    fig = render_figure(5, rows)
    assert len(fig.axes) == 2
    for ax, expected in zip(fig.axes, (7, 207)):
        athena = next(line for line in ax.lines if line.get_label() == 'Athena MSCK')
        assert list(athena.get_ydata()) == [expected]
    rtmsf = next(line for line in fig.axes[1].lines if line.get_label() == 'RTMSF')
    assert math.isnan(rtmsf.get_ydata()[0])  # no borrowing the other layout's GET=0
    assert fig.axes[0].get_ylim() == fig.axes[1].get_ylim()
    with pytest.raises(ValueError, match='layout'):
        graph_row(5, 'rtmsf', 's3_head_requests', 100, 1)
    with pytest.raises(ValueError, match='method'):
        graph_row(5, 'glue_event_crawler', 's3_head_requests', 100, 1, layout='day_first')
    artifacts = write_reports(tmp_path/'first', rows, [], [], {'graphs': [5]})
    assert main(['--csv', str(artifacts['graph_data']), '--results', str(artifacts['report_inputs']),
                 '--output-dir', str(tmp_path/'second')]) == 0
    assert (tmp_path/'second/graph_data.csv').read_bytes() == artifacts['graph_data'].read_bytes()
    assert 'tenant_first' in artifacts['summary'].read_text()


def test_layout_graph_uses_common_x_conditions_when_a_layout_is_missing_data():
    from experiments.figures import graph_row, render_figure
    rows = [graph_row(5, 'rtmsf', metric, x, value, layout=layout)
            for layout, x in [('day_first', 100), ('tenant_first', 10000)]
            for metric, value in [('s3_head_requests', 1), ('s3_get_requests', 0), ('s3_list_requests', 0)]]
    fig = render_figure(5, rows)
    assert fig.axes[0].get_xlim() == fig.axes[1].get_xlim()
    assert list(fig.axes[0].lines[0].get_xdata()) == [100, 10000]
    assert math.isnan(fig.axes[0].lines[0].get_ydata()[1])


def test_graph_row_has_exact_schema_and_rejects_misplaced_action_labels():
    from experiments.figures import COLUMNS, graph_row

    row = graph_row(1, "rtmsf", "expected_partition_presence", 0, True,
                    action_label="Uploaded first file")
    assert tuple(row) == COLUMNS
    assert row["y_value"] == 1
    assert row["x_axis_label"] == "Time since first upload completed"
    assert row["y_unit"] == "0_or_1"
    with pytest.raises(ValueError, match="action_label"):
        graph_row(1, "rtmsf", "observed_partition_presence", 0, 1,
                  action_label="Uploaded first file")
    with pytest.raises(ValueError, match="finite"):
        graph_row(3, "rtmsf", "visibility_delay_s", 100, float("nan"))


def test_report_exposes_burst_work_and_fixture_shape_without_changing_graph_csv(tmp_path):
    from experiments.figures import COLUMNS, write_reports

    checks = [dict(name='short_burst', repetition=1, status='passed', observed_events=20,
                   pmcp_attempts=2, applied_catalog_changes=1)]
    layout = dict(partition_columns=['dt', 'site', 'hour'], starting_partitions=100,
                  occupied_prefixes=[1, 5, 100])
    artifacts = write_reports(tmp_path, [], [], checks,
                              {'graphs': [], 'fixture_layouts': {'rtmsf': layout, 'crawler': layout}})
    summary = artifacts['summary'].read_text()
    assert summary.count('| dt / site / hour | 100 | 1 / 5 / 100 |') == 1
    assert '20 | 2 | 1' in artifacts['checks_table'].read_text()
    assert tuple(artifacts['graph_data'].read_text().strip().split(',')) == COLUMNS


def test_report_csv_orders_series_numerically_and_preserves_unknowns(tmp_path):
    from experiments.figures import COLUMNS, graph_row, write_reports

    rows = [
        graph_row(2, "athena_msck", "s3_list_requests", 100, None),
        graph_row(2, "rtmsf", "s3_get_requests", 100, 0),
        graph_row(2, "rtmsf", "s3_head_requests", 1000, 5),
        graph_row(2, "rtmsf", "s3_head_requests", 100, "", 2),
        graph_row(2, "rtmsf", "s3_head_requests", 100, 1),
    ]
    artifacts = write_reports(tmp_path, rows, [], [], {})
    with artifacts["graph_data"].open(newline="") as stream:
        reader = csv.DictReader(stream)
        actual = list(reader)
        assert tuple(reader.fieldnames) == COLUMNS
    assert [(row["measurement"], row["x_value"], row["repetition"], row["y_value"])
            for row in actual] == [
        ("s3_head_requests", "100", "1", "1"),
        ("s3_head_requests", "100", "2", ""),
        ("s3_head_requests", "1000", "1", "5"),
        ("s3_get_requests", "100", "1", "0"),
        ("s3_list_requests", "100", "1", ""),
    ]
    assert "Trial outcomes unavailable" in artifacts["summary"].read_text()


def test_timeline_uses_repetition_one_and_preserves_unknown_gap():
    from experiments.figures import graph_row, render_figure

    rows = [graph_row(1, "rtmsf", "observed_partition_presence", x, y)
            for x, y in [(-1, 0), (0, None), (2, 1)]]
    rows.append(graph_row(1, "rtmsf", "observed_partition_presence", 8, 0, 2))
    metadata = {"graph_results": [
        {"graph": 1, "method": "rtmsf", "repetition": 1, "status": "failed"},
        {"graph": 1, "method": "rtmsf", "repetition": 2, "status": "passed"},
        {"graph": 1, "method": "rtmsf", "repetition": 3, "status": "incomplete"},
    ]}
    fig = render_figure(1, rows, metadata)
    line = next(line for line in fig.axes[0].lines if line.get_label() == "Observed")
    assert list(line.get_xdata()) == [-1, 0, 2]
    assert math.isnan(line.get_ydata()[1])
    assert line.get_drawstyle() == "steps-post"
    assert [tick.get_text() for tick in fig.axes[0].get_yticklabels()] == ["Absent", "Present"]
    caption = " ".join(text.get_text() for text in fig.texts)
    assert "trace 1 of 2" in caption  # only two repetitions have CSV observations
    assert "Some trials failed, incomplete, or unattempted" in caption


def test_duplicate_condition_cannot_inflate_available_sample_count():
    from experiments.figures import graph_row, render_figure

    row = graph_row(2, "rtmsf", "s3_head_requests", 100, 2)
    with pytest.raises(ValueError, match="Duplicate"):
        render_figure(2, [row, row])


def test_timeline_expected_state_remains_through_last_observation():
    from experiments.figures import graph_row, render_figure

    rows = [
        graph_row(1, "rtmsf", "expected_partition_presence", 0, 1),
        graph_row(1, "rtmsf", "expected_partition_presence", 5, 0),
        graph_row(1, "rtmsf", "observed_partition_presence", 8, 0),
    ]
    fig = render_figure(1, rows)
    line = next(line for line in fig.axes[0].lines if line.get_label() == "Expected")
    assert list(line.get_xdata()) == [0, 5, 8]
    assert list(line.get_ydata()) == [1, 0, 0]
    assert len(rows) == 3


@pytest.mark.parametrize('graph', [2, 4])
def test_request_log_lines_sum_within_repetition_and_keep_unknowns_separate_from_zero(graph):
    from experiments.figures import graph_row, render_figure

    rows = [graph_row(graph, 'rtmsf', metric, 100, value, repetition)
            for repetition, values in enumerate([(1, 9, 0), (9, 1, 0), (0, 0, 12), (2, 3, None)], 1)
            for metric, value in zip(('s3_head_requests', 's3_get_requests', 's3_list_requests'), values)]
    rows += [graph_row(graph, 'rtmsf', metric, 1000, 1) for metric in ('s3_head_requests', 's3_get_requests')]
    rows += [graph_row(graph, 'rtmsf', metric, 10000, 0) for metric in ('s3_head_requests', 's3_get_requests', 's3_list_requests')]
    rows += [graph_row(graph, 'rtmsf', metric, 100000, value, repetition)
             for repetition, values in enumerate([(0, 0, 0), (0, 0, 0), (9, 0, 0)], 1)
             for metric, value in zip(('s3_head_requests', 's3_get_requests', 's3_list_requests'), values)]
    fig = render_figure(graph, rows)
    assert len(fig.axes) == 1
    ax = fig.axes[0]
    line = next(line for line in ax.lines if line.get_label() == 'RTMSF')
    assert list(line.get_xdata()) == [100, 1000, 10000, 100000]
    assert line.get_ydata()[0] == 10  # median of totals, not sum of component medians
    assert math.isnan(line.get_ydata()[1])
    assert math.isnan(line.get_ydata()[2])  # zero is labeled, never shifted to a positive count
    assert math.isnan(line.get_ydata()[3])  # zeros participate in the median
    assert ax.get_xscale() == ax.get_yscale() == 'log'
    assert 'HEAD + GET + LIST' in ax.get_ylabel()
    assert 'RTMSF: unknown' in [text.get_text() for text in ax.texts]
    assert 'RTMSF: 0' in [text.get_text() for text in ax.texts]
    assert 'RTMSF: range 0–9; median 0' in [text.get_text() for text in ax.texts]
    ranges = [collection.get_segments() for collection in ax.collections
              if hasattr(collection, "get_segments")]
    assert any(list(segment[:, 1]) == [10, 12]
               for segments in ranges for segment in segments)
    assert '0–3 usable' in ' '.join(text.get_text() for text in fig.texts)


def test_method_lines_use_color_without_different_line_styles():
    from experiments.figures import METHODS, METHOD_LABELS, graph_row, render_figure

    rows = [graph_row(3, method, 'visibility_delay_s', 100, value)
            for method, value in zip(METHODS, (12, 18, 5))]
    fig = render_figure(3, rows)
    lines = [line for line in fig.axes[0].lines if line.get_label() in METHOD_LABELS]
    assert len(lines) == 3
    assert {line.get_linestyle() for line in lines} == {'-'}
    assert len({line.get_color() for line in lines}) == 3


@pytest.mark.parametrize('settings,planned', [({'repetitions': 5}, 5), ({'repetitions': 5, 'correctness_repetitions': 3}, 3)])
def test_recovery_and_checks_count_failed_incomplete_and_unattempted_cases(tmp_path, settings, planned):
    from experiments.figures import write_reports

    base = {"case": "add_before_processing", "expected_present": True}
    recovery = [
        {**base, "repetition": 1, "observed_present": True, "crash_verified": True,
         "replay_ack_verified": True, "status": "passed"},
        {**base, "repetition": 2, "observed_present": False, "crash_verified": True,
         "replay_ack_verified": False, "status": "failed", "error": "wrong state"},
        {**base, "repetition": 3, "observed_present": None, "crash_verified": False,
         "replay_ack_verified": False, "status": "incomplete", "error": "missing evidence"},
    ]
    checks = [{"name": "duplicate delivery", "repetition": i, "status": status}
              for i, status in enumerate(["passed", "failed", "incomplete"], 1)]
    artifacts = write_reports(tmp_path, [], recovery, checks, settings)
    table = artifacts["recovery_table"].read_text()
    assert "Registered: 1; absent: 1; unknown: 1 | 2/3 | 1/3 | 1/3" in table
    assert "1 failed; 1 incomplete" in table
    assert f"3/{planned} attempted" in table
    assert f"0/{planned} attempted" in table
    assert "Not attempted" in table
    assert "| duplicate delivery | 1/3 | 1 | 1 |" in artifacts["checks_table"].read_text()
    assert "missing evidence" in artifacts["summary"].read_text()


def test_reports_render_all_graphs_and_can_be_reproduced(tmp_path):
    from experiments.figures import graph_row, main, write_reports

    rows = [
        graph_row(1, 'rtmsf', 'expected_partition_presence', 0, 1),
        graph_row(1, 'rtmsf', 'observed_partition_presence', 1, 1),
        graph_row(3, 'rtmsf', 'visibility_delay_s', 100, 1),
    ]
    rows += [graph_row(graph, 'rtmsf', metric, 100, value, layout=layout)
             for graph, layout in [(2, ''), (4, ''), (5, 'day_first'), (5, 'tenant_first')]
             for metric, value in [('s3_head_requests', 1), ('s3_get_requests', 0), ('s3_list_requests', 0)]]
    artifacts = write_reports(tmp_path / "first", rows, [], [],
                              {'graphs': [1, 2, 3, 4, 5], 'data_kind': 'dummy'})
    for graph in range(1, 6):
        assert artifacts[f"graph_{graph}_png"].read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
        assert artifacts[f"graph_{graph}_pdf"].read_bytes().startswith(b"%PDF-")
        assert artifacts[f"graph_{graph}_png"].stat().st_size > 10000
    assert "DUMMY DATA" in artifacts["summary"].read_text()
    assert main(["--csv", str(artifacts["graph_data"]), "--results",
                 str(artifacts["report_inputs"]), "--output-dir", str(tmp_path / "second")]) == 0
    assert (tmp_path / "second/graph_data.csv").read_bytes() == artifacts["graph_data"].read_bytes()
    assert (tmp_path / "second/SUMMARY.md").read_bytes() == artifacts["summary"].read_bytes()
