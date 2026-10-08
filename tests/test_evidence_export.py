import hashlib
import json
from pathlib import Path

import pytest


def saved_run(root, kind):
    from experiments.activity_reports import write_activity_reports
    from experiments.figures import graph_row, write_reports

    root.mkdir()
    manifest = {'started_at': '2026-10-03T08:00:00+00:00',
                'source': {'sha256': 'a' * 64, 'files': {}},
                'config': {'profile': 'private-profile', 'expected_account': '123456789012',
                           'output_dir': str(root), 'repetitions': 1}}
    (root / 'manifest.json').write_text(json.dumps(manifest))
    if kind == 'figures':
        write_reports(root, [graph_row(3, 'rtmsf', 'visibility_delay_s', 100, None)], [], [],
                      {'graphs': [3], 'config': manifest['config'], 'run_status': 'incomplete_or_failed',
                       'graph_results': [{'graph': 3, 'method': 'rtmsf', 'repetition': 1,
                                          'status': 'failed', 'error': 'measurement unavailable'}]})
    else:
        write_activity_reports(root, [{'condition': 'quick', 'method': 'rtmsf', 'repetition': 1,
                                       'status': 'failed', 's3_list_requests': None}], [], ['measurement unavailable'])
    hashes = {file.name: hashlib.sha256(file.read_bytes()).hexdigest()
              for file in root.iterdir() if file.is_file()}
    (root / 'artifact-hashes.json').write_text(json.dumps(hashes))
    return root


def test_export_is_repeatable_and_preserves_failures_unknowns_and_raw_files(tmp_path):
    from experiments.export import export_results

    graphs = saved_run(tmp_path / 'graphs', 'figures')
    activity = saved_run(tmp_path / 'activity', 'activity')
    before = {str(file): file.read_bytes() for directory in (graphs, activity)
              for file in directory.iterdir()}
    first, second = tmp_path / 'first', tmp_path / 'second'
    export_results({'figures': graphs, 'activity': activity}, first)
    export_results({'figures': graphs, 'activity': activity}, second)
    for file in first.rglob('*'):
        if file.is_file():
            assert file.read_bytes() == (second / file.relative_to(first)).read_bytes()
    assert (first / 'figures/graph_data.csv').read_bytes() == (graphs / 'graph_data.csv').read_bytes()
    assert 'failed' in (first / 'SUMMARY.md').read_text()
    assert '| figures | repetitions | 1 |' in (first / 'SUMMARY.md').read_text()
    assert '```json' not in (first / 'SUMMARY.md').read_text()
    assert json.loads((first / 'provenance.json').read_text())['runs']['figures']['config']['repetitions'] == 1
    assert 'incomplete_or_failed' in (first / 'figures/SUMMARY.md').read_text()
    assert json.loads((first / 'activity/report_inputs.json').read_text())['rows'][0]['s3_list_requests'] is None
    assert 'private-profile' not in (first / 'provenance.json').read_text()
    assert '123456789012' not in (first / 'figures/report_inputs.json').read_text()
    assert before == {str(file): Path(file).read_bytes() for file in before}
    for line in (first / 'SHA256SUMS').read_text().splitlines():
        digest, name = line.split('  ', 1)
        assert hashlib.sha256((first / name).read_bytes()).hexdigest() == digest
    with pytest.raises(FileExistsError):
        export_results({'figures': graphs}, first)
    with pytest.raises(ValueError, match='source bundle'):
        export_results({'figures': graphs}, graphs / 'export')


def test_export_rejects_corrupt_or_escaping_input_before_creating_output(tmp_path):
    from experiments.export import export_results

    graphs = saved_run(tmp_path / 'graphs', 'figures')
    output = tmp_path / 'output'
    data = graphs / 'graph_data.csv'
    original = data.read_bytes()
    data.write_bytes(original + b'corrupt')
    with pytest.raises(ValueError, match='hash'):
        export_results({'figures': graphs}, output)
    assert not output.exists()
    data.write_bytes(original)
    hashes = json.loads((graphs / 'artifact-hashes.json').read_text())
    hashes['../outside'] = 'b' * 64
    (graphs / 'artifact-hashes.json').write_text(json.dumps(hashes))
    with pytest.raises(ValueError, match='path'):
        export_results({'figures': graphs}, output)
    assert not output.exists()


def test_export_rejects_report_inputs_that_disagree_with_measurement_csv(tmp_path):
    from experiments.export import export_results

    activity = saved_run(tmp_path / 'activity', 'activity')
    inputs = activity / 'report_inputs.json'
    data = json.loads(inputs.read_text())
    data['rows'][0]['completed_operations'] = 99
    inputs.write_text(json.dumps(data))
    hashes = activity / 'artifact-hashes.json'
    entries = json.loads(hashes.read_text())
    entries[inputs.name] = hashlib.sha256(inputs.read_bytes()).hexdigest()
    hashes.write_text(json.dumps(entries))
    with pytest.raises(ValueError, match='CSV'):
        export_results({'activity': activity}, tmp_path / 'output')
    assert not (tmp_path / 'output').exists()
