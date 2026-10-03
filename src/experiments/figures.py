"""CSV, figures, and trial summaries for the AWS pilot; no AWS dependencies.

Regenerate from retained inputs with ``python -m experiments.figures --csv
graph_data.csv --results report_inputs.json --output-dir regenerated``.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter
from pathlib import Path
from statistics import median
from textwrap import fill


COLUMNS = (
    "graph", "method", "layout", "measurement", "x_axis_label", "x_value", "x_unit",
    "y_axis_label", "y_value", "y_unit", "repetition", "action_label",
)
METHODS = ("rtmsf", "glue_event_crawler", "athena_msck")
METHOD_LABELS = ("RTMSF", "Glue event-based crawler", "Athena MSCK")
COLORS = ("#0072B2", "#D55E00", "#009E73")
REQUESTS = ("s3_head_requests", "s3_get_requests", "s3_list_requests")
PRESENCE = ("expected_partition_presence", "observed_partition_presence")
LAYOUTS = {"day_first": "Day / tenant", "tenant_first": "Tenant / day"}
MEASUREMENTS = {1: PRESENCE, 2: REQUESTS, 3: ("visibility_delay_s",), 4: REQUESTS, 5: REQUESTS}
AXES = {
    1: ("Time since first upload completed", "seconds", "Partition presence", "0_or_1"),
    2: ("Existing registered partitions", "partitions", "S3 requests", "requests"),
    3: ("Existing registered partitions", "partitions", "Time from trigger to catalog visibility", "seconds"),
    4: ("Existing files in changed partition", "files", "S3 requests", "requests"),
    5: ("Existing registered partitions", "partitions", "S3 requests", "requests"),
}
RECOVERY_CASES = {
    "add_before_processing": ("ADD — before processing", True),
    "add_after_catalog_change": ("ADD — after registration, before acknowledgement", True),
    "drop_before_processing": ("DROP — before processing", False),
    "drop_after_catalog_change": ("DROP — after removal, before acknowledgement", False),
}


def _number(value, name):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite; use None or '' for an unknown observation")
    return int(number) if number.is_integer() else number


def graph_row(graph, method, measurement, x, y, repetition=1, action_label="", layout="") -> dict:
    """Make one canonical point. Only None/empty-string y means unavailable."""
    graph = _number(graph, "graph")
    if graph not in MEASUREMENTS or measurement not in MEASUREMENTS[graph]:
        raise ValueError(f"Unsupported graph/measurement: {graph}/{measurement}")
    if method not in METHODS or (graph == 1 and method != "rtmsf") or (graph == 5 and method == "glue_event_crawler"):
        raise ValueError(f"Unsupported method for graph {graph}: {method}")
    if (graph == 5 and layout not in LAYOUTS) or (graph != 5 and layout):
        raise ValueError("layout must identify day_first or tenant_first for Graph 5 only")
    if action_label and (graph != 1 or measurement != PRESENCE[0]):
        raise ValueError("action_label belongs only on expected-presence action rows")
    x, repetition = _number(x, "x_value"), _number(repetition, "repetition")
    if not isinstance(repetition, int) or repetition < 1:
        raise ValueError("repetition must be a positive integer")
    if graph != 1 and (not isinstance(x, int) or x <= 0):
        raise ValueError("Partition/file counts must be positive integers")
    y = None if y is None or y == "" else _number(y, "y_value")
    if y is not None:
        if graph == 1 and y not in (0, 1):
            raise ValueError("Partition presence must be 0, 1, or unknown")
        if graph != 1 and y < 0:
            raise ValueError("Request counts and visibility delays must be nonnegative")
        if graph in (2, 4, 5) and not isinstance(y, int):
            raise ValueError("Request counts must be integers")
    x_label, x_unit, y_label, y_unit = AXES[graph]
    if graph == 1:
        x = round(x, 3)
    if graph == 3 and y is not None:
        y = round(y, 3)
    return dict(zip(COLUMNS, (graph, method, layout, measurement, x_label, x, x_unit,
                              y_label, y, y_unit, repetition, action_label)))


def _ordered(rows):
    canonical = [graph_row(row["graph"], row["method"], row["measurement"],
                           row["x_value"], row["y_value"], row.get("repetition", 1),
                           row.get("action_label", ""), row.get("layout", "")) for row in rows]
    seen = set()
    for row in canonical:
        if row["graph"] == 1:
            continue
        key = tuple(row[field] for field in ("graph", "method", "layout", "measurement", "x_value", "repetition"))
        if key in seen:
            raise ValueError(f"Duplicate measurement for one condition/repetition: {key}")
        seen.add(key)
    return sorted(canonical, key=lambda row: (
        row["graph"], METHODS.index(row["method"]), row["layout"],
        MEASUREMENTS[row["graph"]].index(row["measurement"]),
        row["x_value"], row["repetition"],
    ))


def _config(metadata, key):
    return metadata.get(key, metadata.get("config", {}).get(key))


def _request_caption(metadata):
    if metadata.get('data_kind') == 'dummy':
        return 'Invented values for checking the figure template; not AWS measurements or performance predictions.'
    if metadata.get('request_coverage', {}).get('source') == 'cloudwatch' or _config(metadata, 'request_source') == 'cloudwatch':
        return ('Observed CloudWatch HEAD/GET/LIST counts; delivery completeness is not guaranteed. '
                'Absent counters become zero only when recorded operations account for AllRequests; otherwise they remain blank.')
    return 'Observed HEAD/GET/LIST lower bounds, including recorded retries; CloudTrail delivery completeness is not guaranteed.'


def _counts(records):
    counts = Counter(record.get("status", "incomplete") for record in records if record.get("attempted", True))
    unknown = set(counts) - {"passed", "failed", "incomplete"}
    if unknown:
        raise ValueError(f"Unsupported trial statuses: {sorted(unknown)}")
    return counts


def _outcome(records):
    counts = _counts(records)
    attempted = sum(counts.values())
    return (f"{counts['passed']}/{attempted} passed; {counts['failed']} failed; "
            f"{counts['incomplete']} incomplete; {len(records) - attempted} not attempted")


def _graph_outcome(graph, metadata):
    records = [record for record in metadata.get("graph_results", [])
               if int(record["graph"]) == graph]
    return _outcome(records) if records else "Trial outcomes unavailable; no pass inferred from measurements"


def _point_groups(rows):
    groups = {}
    for row in rows:
        key = (row["graph"], row["method"], row.get("layout", ""), row["measurement"], row["x_value"])
        groups.setdefault(key, []).append(row)
    return groups


def _request_totals(rows):
    """Sum complete, matching repetitions; never join measurements across trials."""
    trials = {}
    for row in rows:
        key = tuple(row.get(field, '') for field in ('graph', 'method', 'layout', 'x_value', 'repetition'))
        trials.setdefault(key, {})[row['measurement']] = row
    totals = []
    for metrics in trials.values():
        complete = all(metric in metrics and metrics[metric]['y_value'] is not None for metric in REQUESTS)
        value = sum(metrics[metric]['y_value'] for metric in REQUESTS) if complete else None
        totals.append(dict(next(iter(metrics.values())), measurement='s3_read_list_requests', y_value=value))
    return totals


def _availability(rows, methods, xs):
    if not rows:
        return ["No recorded measurements."]
    groups = _point_groups(rows)
    graph, measurement = rows[0]['graph'], rows[0]['measurement']
    counts = set()
    for method in methods:
        for x in xs:
            points = groups.get((graph, method, rows[0].get('layout', ''), measurement, x), [])
            counts.add((sum(row['y_value'] is not None for row in points), len(points)))
    if len(counts) == 1:
        available, recorded = next(iter(counts))
        if available == recorded:
            return [f'{available} repetitions per point.']
        return [f'{available}/{recorded} repetitions available per point; details in SUMMARY.md.']
    available = [count[0] for count in counts]
    return [f'Repetitions per point: {min(available)}–{max(available)} usable; counts vary, see SUMMARY.md.']


def render_figure(graph, rows, metadata=None):
    """Return a headless Matplotlib Figure, preserving gaps in the input data."""
    from matplotlib.figure import Figure
    from matplotlib.patches import Patch
    from matplotlib.ticker import FuncFormatter, LogLocator

    metadata = metadata or {}
    graph = int(graph)
    rows = [row for row in _ordered(rows) if row["graph"] == graph]
    fig = Figure(figsize=(12, 7), layout=None)
    titles = {1: "Does one partition appear and disappear correctly?",
              2: "Storage requests as the table grows",
              3: "Time from each method's trigger to catalog visibility",
              4: "Storage requests as the affected partition grows",
              5: "Does folder order change storage work?"}
    title = f"Graph {graph}. {titles[graph]}"
    if metadata.get("data_kind") == "dummy":
        title = "DUMMY DATA — invented example\n" + title
    fig.suptitle(title, fontsize=15, fontweight="bold", y=0.975)
    caption = []
    if graph == 1:
        ax = fig.subplots()
        selected = [row for row in rows if row["repetition"] == 1]
        for measurement, label, color, style in zip(PRESENCE, ("Expected", "Observed"),
                                                     (COLORS[1], COLORS[0]), ("--", "-")):
            points = [row for row in selected if row["measurement"] == measurement]
            # An expected state persists until the next successful workload action.
            # Extend it to the last observer timestamp without creating CSV samples.
            if measurement == PRESENCE[0] and points and selected:
                end = max(row["x_value"] for row in selected)
                if points[-1]["x_value"] < end:
                    points = points + [{**points[-1], "x_value": end}]
            ax.step([row["x_value"] for row in points],
                    [math.nan if row["y_value"] is None else row["y_value"] for row in points],
                    where="post", label=label, color=color, linestyle=style, linewidth=2,
                    marker=".", markersize=4, zorder=3 if measurement == PRESENCE[0] else 2)
        actions = [row for row in selected if row["action_label"]]
        for index, row in enumerate(actions):
            if row["y_value"] is not None:
                ax.annotate(fill(row["action_label"], 21), (row["x_value"], row["y_value"]),
                            xytext=(row["x_value"], 1.32 + 0.16 * (index % 2)),
                            ha="center", va="bottom", fontsize=9,
                            arrowprops={"arrowstyle": "-", "color": "#777777", "lw": 0.8})
        if not selected:
            ax.text(0.5, 0.5, "Repetition 1 has no recorded observations", ha="center", transform=ax.transAxes)
        ax.set(ylim=(-0.15, 1.85), xlabel=AXES[graph][0] + " (s)", ylabel=AXES[graph][2])
        ax.set_yticks([0, 1], ["Absent", "Present"])
        ax.legend(labels=['Expected from files', 'Observed in catalog'], loc="upper right", frameon=False)
        repetitions = len({row['repetition'] for row in rows})
        kind = 'synthetic ' if metadata.get('data_kind') == 'dummy' else ''
        caption.append(f'One partition throughout; trace 1 of {repetitions} {kind}repetitions shown. Delays show catalog catch-up.'
                       if selected else 'No observations recorded for repetition 1.')
    else:
        request_graph = graph in (2, 4, 5)
        if request_graph:
            rows = _request_totals(rows)
        all_rows = rows
        methods = [method for method in METHODS if any(row["method"] == method for row in rows)]
        layouts = tuple(LAYOUTS) if graph == 5 else ("",)
        axes = fig.subplots(1, len(layouts), sharex=True, sharey=True, squeeze=False)[0]
        positive_plot_values, availability = [], []
        for ax, layout in zip(axes, layouts):
            rows = [row for row in all_rows if row.get('layout', '') == layout]
            xs = sorted({row["x_value"] for row in all_rows})
            groups = _point_groups(rows)
            measurement = 's3_read_list_requests' if request_graph else 'visibility_delay_s'
            for index, method in enumerate(methods):
                color, label = COLORS[METHODS.index(method)], METHOD_LABELS[METHODS.index(method)]
                medians = []
                for x in xs:
                    values = [row['y_value'] for row in groups.get((graph, method, layout, measurement, x), [])
                              if row['y_value'] is not None]
                    middle = median(values) if values else math.nan
                    medians.append(math.nan if request_graph and middle == 0 else middle)
                    positive_plot_values.extend(value for value in values + [middle] if value > 0)
                    if values and min(values) < max(values) and (not request_graph or min(values) > 0):
                        ax.errorbar([x], [middle], yerr=[[middle - min(values)], [max(values) - middle]],
                                    fmt='none', color=color, capsize=2, elinewidth=0.8, capthick=0.8, zorder=3)
                    if request_graph:
                        if not values or min(values) == 0:
                            note = 'unknown' if not values else '0'
                            if values and max(values) > 0:
                                note = f'range 0–{max(values):,}; median {middle:g}'
                            ax.text(x, 0.025 + 0.05 * index, f'{label}: {note}', transform=ax.get_xaxis_transform(),
                                    ha='center', fontsize=8, color=color)
                        elif math.isfinite(middle):
                            ax.annotate(f'{middle:,.1f}'.removesuffix('.0'), (x, max(values)), xytext=(0, 7),
                                        textcoords='offset points', ha='center', fontsize=9, color=color)
                ax.plot(xs, medians, color=color, label=label, marker="o", markersize=4.5,
                        markerfacecolor=color, markeredgewidth=0, linewidth=1.7, zorder=4, linestyle='-')
            ax.set_xscale('log')
            if xs:
                ax.set_xticks(xs)
                ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f'{value:,.0f}'))
                ax.set_xlim(min(xs) / 1.3, max(xs) * 1.3)
            if request_graph:
                ax.set_yscale('log')
                ax.set_ylabel('Observed S3 read/list requests\n(HEAD + GET + LIST; log scale)')
                ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1,)))
                ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f'{value:,.0f}' if value >= 1 else f'{value:g}'))
                if positive_plot_values:
                    ax.set_ylim(min(positive_plot_values) / 1.5, max(positive_plot_values) * 1.7)
                else:
                    ax.set_ylim(0.5, 10)
            else:
                ax.set_ylabel('Observed visibility delay (s)')
                ax.set_ylim(bottom=0)
                if not any(row['y_value'] for row in rows):
                    ax.set_ylim(0, 1)
            ax.minorticks_off()
            if not xs:
                ax.text(0.5, 0.5, 'No recorded measurements', ha='center', transform=ax.transAxes)
            ax.set_xlabel(AXES[graph][0] + ' (log scale)', fontsize=10)
            if graph == 5:
                ax.set_title(LAYOUTS[layout], fontsize=12)
            availability.append((LAYOUTS[layout] + ': ' if graph == 5 else '') + _availability(rows, methods, xs)[0])
        if graph == 5:
            axes[1].set_ylabel('')
        if methods:
            handles = [Patch(facecolor=COLORS[METHODS.index(method)], label=METHOD_LABELS[METHODS.index(method)]) for method in methods]
            fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 0.90),
                       ncols=len(methods), frameon=False, fontsize=10)
        samples = ' '.join(availability)
        if metadata.get('data_kind') == 'dummy':
            samples = samples.replace('repetitions', 'synthetic repetitions')
        caption.append('Dot = median; whisker = min–max. ' + samples)
        if graph == 5:
            caption.append('Identical tenant/day pairs; swap folder order and add one new tenant partition.')
        if graph == 2:
            caption.append('One file creates one new partition.')
        if graph == 3:
            caption.append('Clock starts at upload completion (RTMSF), crawler start request (Glue), or query submission (Athena).')
        if graph == 4:
            size = _config(metadata, "graph4_partitions")
            fixed = f'Fixed table: {int(size):,} partitions.' if size is not None else 'Fixed table size not supplied.'
            caption.append('Append one file to an existing partition. ' + fixed)
    records = [record for record in metadata.get('graph_results', []) if int(record['graph']) == graph]
    if any(record.get('status') != 'passed' or not record.get('attempted', True) for record in records):
        caption.append('Some trials failed, incomplete, or unattempted; see SUMMARY.md.')
    for ax in fig.axes:
        ax.grid(axis="both", which="major", color="#dddddd", linewidth=0.7, alpha=0.7)
        ax.set_axisbelow(True)
        ax.spines[["top", "right"]].set_visible(False)
    caption = "\n".join(fill(line, 155) for line in caption)
    bottom = max(0.18, 0.11 + len(caption.splitlines()) * 0.021)
    fig.subplots_adjust(left=0.10, right=0.98, top=0.79 if graph != 1 else 0.83,
                        bottom=bottom, wspace=0.30)
    fig.text(0.075, 0.025, caption, fontsize=8, va="bottom", linespacing=1.4)
    return fig


def _cell(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


def _recovery_table(records, metadata):
    unknown_cases = {record["case"] for record in records} - set(RECOVERY_CASES)
    if unknown_cases:
        raise ValueError(f"Unsupported recovery cases: {sorted(unknown_cases)}")
    lines = ["## Table 1. Crash recovery", "",
             "| Crash case | Expected after recovery | Observed counts | Crash verified / attempted | Replayed + acknowledged / attempted | Passed / attempted |",
             "|---|---|---|---|---|---|"]
    planned = _config(metadata, "correctness_repetitions")
    if planned is None:
        planned = _config(metadata, "repetitions")
    notes = []
    for case, (label, expected) in RECOVERY_CASES.items():
        trials = [record for record in records if record["case"] == case and record.get("attempted", True)]
        count, statuses = len(trials), _counts(trials)
        states = Counter("registered" if record.get("observed_present") is True else
                         "absent" if record.get("observed_present") is False else "unknown" for record in trials)
        observed = (f"Registered: {states['registered']}; absent: {states['absent']}; unknown: {states['unknown']}"
                    if count else "Not attempted")
        crash = sum(record.get("crash_verified") is True for record in trials)
        replay = sum(record.get("replay_ack_verified") is True for record in trials)
        lines.append(f"| {label} | {'Registered' if expected else 'Absent'} | {observed} | {crash}/{count} | {replay}/{count} | {statuses['passed']}/{count} |")
        attempts = f"{count}/{planned} attempted" if planned is not None else f"{count} attempted (plan not supplied)"
        notes.append(f"- {label}: {attempts}; {statuses['failed']} failed; {statuses['incomplete']} incomplete.")
    return "\n".join(lines + [""] + notes + ["", "Observed states are measured after recovery, before the next cycle step or cleanup. Skipped trials are not attempts.", ""])


def _checks_table(checks):
    lines = ["## Supporting checks", "", "| Check | Passed / attempted | Failed | Incomplete | Not attempted |",
             "|---|---|---|---|---|"]
    for name in dict.fromkeys(record["name"] for record in checks):
        records = [record for record in checks if record["name"] == name]
        counts = _counts(records)
        attempted = sum(counts.values())
        lines.append(f"| {_cell(name)} | {counts['passed']}/{attempted} | {counts['failed']} | {counts['incomplete']} | {len(records) - attempted} |")
    if not checks:
        lines.extend(["", "No supporting checks recorded."])
    bursts = [record for record in checks if record.get('observed_events') is not None]
    if bursts:
        lines.extend(['', 'Short-burst work from delivered worker telemetry (including separate processing windows):', '',
                      '| Repetition | Observed file events | Observed PMCP attempts | Observed applied catalog changes |',
                      '|---|---|---|---|'])
        for record in bursts:
            lines.append(f"| {record.get('repetition', 1)} | {record['observed_events']} | {record['pmcp_attempts']} | {record['applied_catalog_changes']} |")
    return "\n".join(lines + [""])


def _summary(rows, recovery, checks, metadata, recovery_table, checks_table):
    lines = ["# AWS pilot report", ""]
    if 'run_status' in metadata:
        lines.extend([f"Run status: **{_cell(metadata['run_status'])}**.", ""])
        lines.extend(f"- {_cell(key)}: {_cell(value)}" for key, value in metadata.items() if key.endswith('_error'))
        lines.append("")
    if metadata.get("data_kind") == "dummy":
        lines.extend(["**DUMMY DATA — invented example values, not AWS results.**", ""])
        if metadata.get('synthetic_assumptions'):
            lines.extend(['Synthetic assumptions (including all five example repetitions):', ''])
            lines.extend(f'- {_cell(note)}' for note in metadata['synthetic_assumptions'])
            lines.append('')
    layouts = {(tuple(item['partition_columns']), item['starting_partitions'], tuple(item['occupied_prefixes']))
               for item in metadata.get('fixture_layouts', {}).values()}
    if layouts:
        lines.extend(['## Fixture layout', '',
                      '| Partition columns | Starting partitions | Occupied prefixes at each depth |', '|---|---|---|'])
        for columns, count, prefixes in sorted(layouts):
            lines.append(f"| {' / '.join(columns)} | {count} | {' / '.join(map(str, prefixes))} |")
        lines.extend(['', 'Partition count and prefix structure vary together; compare layouts in separate runs.', ''])
    if metadata.get('activity_status'):
        lines.extend(['', f"Activity validation: **{metadata['activity_status']}**. Tables, CSVs and supporting records are under [activity/](activity/).", ''])
    lines.extend(["## Graph outcomes", "", "| Graph | Trial outcomes | Figures |", "|---|---|---|"])
    for graph in metadata.get("graphs", MEASUREMENTS):
        lines.append(f"| {graph} | {_graph_outcome(graph, metadata)} | [PNG](graph_{graph}.png) · [PDF](graph_{graph}.pdf) |")
    lines.extend(["", "Graph 1 uses repetition 1, selected before running even if it fails. Binary traces are not averaged.",
                  "Graphs 2/4/5 show HEAD + GET + LIST totals on logarithmic axes, summed within each complete repetition before taking the median. Missing components make the total unknown.",
                  "Zero counts remain in the CSV and statistics; zero counts/ranges are labeled at the axis because logarithmic axes cannot display zero.",
                  "Graph 3 uses solid colored lines. Graphs 2–5 show one median point and thin min–max whiskers where repetitions differ. Individual values stay in the CSV; no p95 is estimated.",
                  "Graph 3 triggers: RTMSF successful upload response; crawler immediately before StartCrawler; MSCK immediately before StartQueryExecution. These are method-specific response times.",
                  "Blank values remain unknown. " + _request_caption(metadata) + " A trial pass does not establish exact accounting.",
                  "", recovery_table, checks_table, "## Measurement availability", "",
                  "Available / recorded counts exclude blanks from the available sample size. A 0/0 condition has no point records.", "",
                  "| Graph | Method | Layout | Measurement | X | Available / recorded |", "|---|---|---|---|---|---|"])
    totals = _request_totals([row for row in rows if row['graph'] in (2, 4, 5)])
    for (graph, method, layout, measurement, x), points in _point_groups(rows + totals).items():
        if graph == 1:
            continue
        available = sum(row["y_value"] is not None for row in points)
        lines.append(f"| {graph} | {method} | {layout} | {measurement} | {x} | {available}/{len(points)} |")
    failures = [record for record in metadata.get("graph_results", []) + recovery + checks
                if record.get("status") != "passed"]
    lines.extend(["", "## Failed, incomplete, or unattempted trials", ""])
    if not failures:
        lines.append("No failed or incomplete trial records were supplied. Missing records do not establish a pass.")
    for record in failures:
        name = record.get("case", record.get("name", f"Graph {record.get('graph')}: {record.get('method', '')}"))
        condition = f", X={record['x_value']}" if "x_value" in record else ""
        if record.get("layout"):
            condition += f", layout={record['layout']}"
        status = record.get('status', 'incomplete') if record.get('attempted', True) else 'not attempted'
        lines.append(f"- {_cell(name)}, repetition {record.get('repetition', '?')}{condition}: {status} — {_cell(record.get('error') or 'No diagnostic supplied')}.")
    lines.extend(["", "Settings, individual outcomes, and request-coverage details: [report_inputs.json](report_inputs.json).", "",
                  "Reproduce these reports without AWS calls from the repository root, substituting the path to the retained report directory:", "",
                  "```sh", "uv run --extra experiment python -m experiments.figures --csv <report-directory>/graph_data.csv --results <report-directory>/report_inputs.json --output-dir regenerated", "```", ""])
    return "\n".join(lines)


def write_reports(output_dir, rows, recovery_results, checks, metadata) -> dict[str, Path]:
    """Write canonical data, selected PNG/PDF figures, tables, and reproducible inputs.

    metadata.graph_results contains explicit per-attempt graph outcomes; values
    alone never establish a pass. metadata.graph4_partitions labels the fixed
    population; metadata.correctness_repetitions documents planned recovery attempts
    (older bundles use metadata.repetitions).
    """
    output_dir = Path(output_dir)
    rows = _ordered(rows)
    recovery_results, checks, metadata = list(recovery_results), list(checks), dict(metadata)
    recovery_table = _recovery_table(recovery_results, metadata)
    checks_table = _checks_table(checks)
    summary = _summary(rows, recovery_results, checks, metadata, recovery_table, checks_table)
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = {"graph_data": output_dir / "graph_data.csv",
                 "recovery_table": output_dir / "recovery_table.md",
                 "checks_table": output_dir / "checks_table.md",
                 "summary": output_dir / "SUMMARY.md",
                 "report_inputs": output_dir / "report_inputs.json"}
    with artifacts["graph_data"].open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    artifacts["recovery_table"].write_text(recovery_table, encoding="utf-8")
    artifacts["checks_table"].write_text(checks_table, encoding="utf-8")
    artifacts["summary"].write_text(summary, encoding="utf-8")
    artifacts["report_inputs"].write_text(json.dumps({"recovery_results": recovery_results,
        "checks": checks, "metadata": metadata}, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    for graph in metadata.get("graphs", MEASUREMENTS):
        fig = render_figure(graph, rows, metadata)
        for extension in ("png", "pdf"):
            path = output_dir / f"graph_{graph}.{extension}"
            image_metadata = {"Author": "Ankit Joshi", "CreationDate": None, "ModDate": None} if extension == "pdf" else {"Author": "Ankit Joshi"}
            fig.savefig(path, dpi=180, facecolor="white", metadata=image_metadata)
            artifacts[f"graph_{graph}_{extension}"] = path
        fig.clear()
    return artifacts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, type=Path, help="graph CSV (legacy 11-column files are accepted)")
    parser.add_argument("--results", type=Path, help="Retained report_inputs.json")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    with args.csv.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    inputs = json.loads(args.results.read_text(encoding="utf-8")) if args.results else {}
    metadata = inputs.get("metadata", {})
    if "dummy" in args.csv.name.lower():
        metadata["data_kind"] = "dummy"
    write_reports(args.output_dir, rows, inputs.get("recovery_results", []), inputs.get("checks", []), metadata)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
