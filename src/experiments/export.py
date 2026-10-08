"""Export verified saved runs into a fresh, reproducible report bundle; no AWS calls."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
import shutil
import tempfile

README = """# Experiment evidence

This folder contains selected reports from saved experiment runs. It belongs in Git alongside the source code.
The exporter copies measurement CSVs unchanged and generates reports from saved inputs.
Original execution records remain separate under `runs/` and are Git-ignored.
Start with [SUMMARY.md](SUMMARY.md) for settings and outcomes.

| Item | Purpose |
|---|---|
| `figures/graph_data.csv` | Measurement points for the five graphs. |
| `figures/graph_1` through `graph_5` | PNG previews and vector PDF exports. |
| `figures/checks_table.md` | Short/long bursts, routing, replay, markers, rapid changes, and retry-check results. |
| `figures/recovery_table.md` | Results for the four ADD/DROP crash cases. |
| `figures/report_inputs.json` | Individual recovery/check outcomes, settings, and graph outcomes used to generate tables and summaries. |
| `activity/` | Burst, grouping-comparison, continuous-traffic, and load CSVs with their summary. |
| `buffer/` | Buffer-limit comparison CSVs with memory and backlog results. |
| `activity/report_inputs.json`, `buffer/report_inputs.json` | Saved rows, timelines, and errors used to regenerate these reports. |
| [provenance.json](provenance.json) | Source run, code, image identity, and settings behind each selected phase. |
| [SHA256SUMS](SHA256SUMS) | File fingerprints that detect changes to exported files. |

Only selected phases appear. Checks and recovery use tables; their outcomes are not graph points in `graph_data.csv`.
The runner records those outcomes automatically. The report generators read `report_inputs.json`; no manual table entry is needed.
Missing measurements remain unknown. Failed and unattempted conditions remain recorded.
A workload pass does not establish measurement completeness or sustainable processing capacity.

## Redraw the graphs and tables

Run from the repository root with the experiment dependencies installed:

```sh
uv run --frozen --extra experiment python -m experiments.figures \\
  --csv evidence/figures/graph_data.csv \\
  --results evidence/figures/report_inputs.json \\
  --output-dir runs/regenerated-figures
```

This command uses saved values and makes no cloud calls. It keeps evidence inputs unchanged and replaces reports in the output directory.
To regenerate the activity tables:

```sh
uv run --frozen --extra experiment python -m experiments.activity_reports \\
  --results evidence/activity/report_inputs.json \\
  --output-dir runs/regenerated-activity
```

For buffer tables, replace `evidence/activity/` with `evidence/buffer/` and choose a separate output directory.
The repository's `docs/experiments.md` defines measurements; provider guides describe cloud execution.
"""
PRIVATE_FIELDS = {'profile', 'expected_account', 'build_manifest', 'output_dir',
                  'receipt_handle', 'ReceiptHandle', 'credentials', 'access_key',
                  'secret_key', 'session_token', 'password'}


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def public_record(value, account):
    if isinstance(value, dict):
        return {key: public_record(item, account) for key, item in value.items() if key not in PRIVATE_FIELDS}
    if isinstance(value, list):
        return [public_record(item, account) for item in value]
    if isinstance(value, str) and account:
        return value.replace(account, '000000000000')
    return value


def load_verified_run(directory):
    directory = Path(directory).resolve()
    hashes = json.loads((directory / 'artifact-hashes.json').read_text())
    for required in ('manifest.json', 'report_inputs.json'):
        if required not in hashes:
            raise ValueError(f'input hash manifest omits {required}')
    for name, expected in hashes.items():
        path = directory / name
        if not path.resolve().is_relative_to(directory) or not path.is_file():
            raise ValueError(f'invalid input path: {name}')
        if file_hash(path) != expected:
            raise ValueError(f'input hash mismatch: {name}')
    return (json.loads((directory / 'manifest.json').read_text()),
            json.loads((directory / 'report_inputs.json').read_text()), hashes)


def export_results(runs: dict[str, Path], output: Path) -> None:
    """Verify raw bundles, generate reports, preserve original CSVs, and hash the export."""
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError(f'choose a fresh export directory: {output}')
    if not runs or set(runs) - {'figures', 'activity', 'buffer'}:
        raise ValueError('select figures, activity, or buffer source bundles')
    sources = {name: Path(path).resolve() for name, path in sorted(runs.items())}
    if any(output.is_relative_to(path) or path.is_relative_to(output) for path in sources.values()):
        raise ValueError('export destination must stay separate from each source bundle')
    verified = {}
    for name, source in sources.items():
        manifest, inputs, hashes = load_verified_run(source)
        csv_names = ('graph_data.csv',) if name == 'figures' else ('activity_data.csv', 'activity_timeline.csv')
        if any(file not in hashes for file in csv_names):
            raise ValueError('input hash manifest omits a measurement CSV')
        verified[name] = (manifest, inputs, hashes, csv_names)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.evidence-export-', dir=output.parent) as temporary:
        stage = Path(temporary)
        provenance = {'schema_version': 1, 'runs': {}, 'report_generators': {
            str(Path(__file__).relative_to(Path(__file__).parents[2])): file_hash(Path(__file__)),
            'src/experiments/figures.py': file_hash(Path(__file__).with_name('figures.py')),
            'src/experiments/activity_reports.py': file_hash(Path(__file__).with_name('activity_reports.py'))},
            'selection': 'Scientific CSVs, report inputs, outcomes, compact graph request coverage; raw events and downloads stay in source bundles.',
            'account_placeholder': '000000000000'}
        summary = ['# Experiment results', '',
                   '| Output | Execution | Recorded outcomes | Report |', '|---|---|---|---|']
        notes = []
        settings = ['', '## Key settings', '', '| Phase | Setting | Value |', '|---|---|---|']
        for name, source in sources.items():
            manifest, inputs, hashes, csv_names = verified[name]
            account = manifest.get('config', {}).get('expected_account')
            if name == 'figures':
                trail = inputs['metadata'].get('supporting_cloudtrail_coverage', {})
                trail.pop('sources', None)
                for window in trail.get('windows', {}).values():
                    window.pop('events', None)
            inputs = public_record(inputs, account)
            destination = stage / name
            if name == 'figures':
                from experiments.figures import write_reports
                with (source / 'graph_data.csv').open(newline='') as stream:
                    rows = list(csv.DictReader(stream))
                write_reports(destination, rows, inputs['recovery_results'], inputs['checks'], inputs['metadata'])
                results = inputs['metadata'].get('graph_results', [])
                outcome_groups = [(f'Graph {graph}', [row for row in results if row['graph'] == graph])
                                  for graph in inputs['metadata'].get('graphs', [])]
                outcome_groups += [('Recovery', inputs['recovery_results']), ('Supporting checks', inputs['checks'])]
            else:
                from experiments.activity_reports import write_activity_reports
                write_activity_reports(destination, inputs['rows'], inputs['timeline'], inputs['errors'])
                outcome_groups = [(name, inputs['rows'])]
            # Keep the original bytes, including unknowns, precision, ordering, and line endings.
            for file in csv_names:
                with (source / file).open(newline='') as original, (destination / file).open(newline='') as generated:
                    if list(csv.reader(original)) != list(csv.reader(generated)):
                        raise ValueError(f'report inputs disagree with measurement CSV: {file}')
                shutil.copyfile(source / file, destination / file)
            config = public_record(manifest.get('config', {}), account)
            identity = {'execution': source.name, 'started_at': manifest.get('started_at'),
                        'source_sha256': manifest['source']['sha256'], 'config': config,
                        'verified_artifact_entries': len(hashes),
                        'original_input_hashes': {file: hashes[file] for file in ('manifest.json', 'report_inputs.json') + csv_names},
                        'original_artifact_manifest_sha256': file_hash(source / 'artifact-hashes.json')}
            if (source / 'image-build.json').exists():
                if 'image-build.json' not in hashes:
                    raise ValueError('input hash manifest omits image-build.json')
                build = json.loads((source / 'image-build.json').read_text())
                identity['image'] = public_record(build['image'], account)
            provenance['runs'][name] = identity
            for label, outcomes in outcome_groups:
                counts = Counter(record.get('status', 'unknown') if record.get('attempted', True)
                                 else 'not_attempted' for record in outcomes)
                status = '; '.join(f'{key}: {count}' for key, count in sorted(counts.items())) or 'No outcome records'
                summary.append(f'| {label} | {source.name} | {status} | [{name}/SUMMARY.md]({name}/SUMMARY.md) |')
            if name == 'figures':
                notes.append(f"Figure phase status: {inputs['metadata'].get('run_status', 'unknown')}.")
            else:
                notes.append(f"{name}: {len(inputs['errors'])} recorded errors.")
            for key in ('region', 'sizes', 'files', 'graph4_partitions', 'layout_sizes',
                        'conditions', 'repetitions', 'correctness_repetitions', 'burst_repetitions',
                        'buffer_sizes', 'buffer_bytes', 'queue_visibility_seconds',
                        'request_source', 'audit_wait_seconds'):
                value = config.get(key)
                if value is not None and not (name != 'figures' and key in ('graph4_partitions', 'correctness_repetitions')):
                    value = ', '.join(map(str, value)) if isinstance(value, list) else str(value)
                    settings.append(f"| {name} | {key} | {value.replace('|', r'\|').replace(chr(10), ' ')} |")
        summary += [''] + notes + ['', 'Graphs 2 and 3 share trials; their outcome records are separate.',
                    'Missing values remain unknown. Failed, incomplete, and unattempted records remain visible.']
        settings += ['', 'Full configuration, source identity, and image identity: [provenance.json](provenance.json).']
        (stage / 'README.md').write_text(README)
        (stage / 'SUMMARY.md').write_text('\n'.join(summary + settings) + '\n')
        (stage / 'provenance.json').write_text(json.dumps(provenance, indent=2, sort_keys=True) + '\n')
        (stage / 'SHA256SUMS').write_text(''.join(file_hash(file) + '  ' + str(file.relative_to(stage)) + '\n'
            for file in sorted(stage.rglob('*')) if file.is_file()))
        if output.exists():
            raise FileExistsError(f'export destination already exists: {output}')
        stage.rename(output)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('figures', 'activity', 'buffer'):
        parser.add_argument('--' + name, type=Path, help='saved raw phase directory')
    parser.add_argument('--output-dir', type=Path, required=True, help='fresh destination; existing directories are never replaced')
    args = parser.parse_args(argv)
    export_results({name: getattr(args, name) for name in ('figures', 'activity', 'buffer')
                    if getattr(args, name) is not None}, args.output_dir)
    print(f'Evidence export: {args.output_dir.resolve()}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
