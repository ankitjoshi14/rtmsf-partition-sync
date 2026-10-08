"""Regenerate the activity tables and CSVs from retained report inputs."""
import csv
import json
from pathlib import Path

DATA_COLUMNS = ('condition', 'method', 'repetition', 'completed_operations', 'write_duration_s',
    'pmcp_attempts', 's3_head_requests', 's3_get_requests', 's3_list_requests', 'catalog_api_attempts',
    'first_partition_visibility_s', 'attempts_during_writes', 'processed_events_during_writes',
    'partitions_processed_during_writes', 'peak_queue_visible', 'peak_queue_inflight', 'drain_s',
    'achieved_operations_per_s', 'max_schedule_delay_s', 'unacknowledged_at_write_end',
    'oldest_unacknowledged_at_write_end_s', 'retry_attempts', 'producer_retry_attempts')
TIME_COLUMNS = ('condition', 'method', 'repetition', 'elapsed_s', 'completed_operations',
    'first_partition_present', 'observed_elapsed_s', 'queue_visible', 'queue_inflight',
    'acknowledged_operations', 'unacknowledged_operations', 'oldest_unacknowledged_s', 'error')
BUFFER_COLUMNS = ('max_pending_deliveries', 'max_buffer_bytes', 'queue_visibility_seconds',
                  'peak_rss_bytes', 'peak_pending_messages', 'peak_pending_body_bytes')


def write_activity_reports(root, rows, timeline, errors):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    payload = dict(kind='bounded_activity_validation', rows=rows, timeline=timeline, errors=errors)
    (root / 'report_inputs.json').write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n')
    buffered = any('max_pending_deliveries' in row for row in rows)
    for name, fields, records in (('activity_data.csv', DATA_COLUMNS + (BUFFER_COLUMNS if buffered else ()), rows),
                                 ('activity_timeline.csv', TIME_COLUMNS + (('max_pending_deliveries',) if buffered else ()), timeline)):
        with (root / name).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore', lineterminator='\n')
            writer.writeheader()
            writer.writerows({key: round(value, 3) if isinstance(value, float) else value
                              for key, value in record.items()} for record in records)
    lines = ['# Activity validation', '',
        'Bounded AWS validation; these repetitions do not establish tail latency or maximum sustainable capacity.', '',
        '| Condition | Method | Repeat | PMCP attempts | HEAD / GET / LIST | First partition visible (s) | Drain (s) | Outcome |',
        '|---|---|---:|---:|---|---:|---:|---|']
    def number(value):
        return 'unknown' if value is None else f'{value:.3f}' if isinstance(value, float) else str(value)
    def label(row):
        return row['condition'] + (f' (buffer {row["max_pending_deliveries"]})' if 'max_pending_deliveries' in row else '')
    for row in rows:
        requests = ' / '.join(number(row.get(k)) for k in ('s3_head_requests', 's3_get_requests', 's3_list_requests'))
        lines.append(f'| {label(row)} | {row["method"]} | {row["repetition"]} | {number(row.get("pmcp_attempts"))} | {requests} | {number(row.get("first_partition_visibility_s"))} | {number(row.get("drain_s"))} | {row["status"]} |')
    lines += ['', 'Visibility covers the first new partition, polled independently. Queue counts are approximate.',
              'Drain includes observation/telemetry waits and eight seconds of observed queue emptiness.',
              'S3 counts require agreement between CloudWatch, CloudTrail and worker API records.', '']
    lines += ['Producer retries are separate from worker retries; write timing and delivered duplicate notifications include their effects.', '']
    lines += [f'- {row["condition"]}/{row["method"]} repeat {row["repetition"]}: visibility unknown — {row["first_partition_visibility_note"]}.'
              for row in rows if row.get('first_partition_visibility_note') and not row['condition'].startswith('load_')]
    loads = [row for row in rows if row['condition'].startswith('load_')]
    if loads:
        lines += ['Load tests retain the seed file in each partition; visibility is not measured for these already registered targets.',
                  'Backlog matches completed producer operations to observed ACKs, including operations whose notifications have not arrived.',
                  'ACK timing uses worker UTC timestamps and assumes synchronized clocks; these are observed bounds, not exact queue internals.', '',
                  '| Load | Repeat | Achieved ops/s | Maximum start delay (s) | Unacknowledged at write end | Oldest age (s) | Drain (s) |',
                  '|---|---:|---:|---:|---:|---:|---:|']
        for row in loads:
            keys = ('achieved_operations_per_s', 'max_schedule_delay_s', 'unacknowledged_at_write_end',
                    'oldest_unacknowledged_at_write_end_s', 'drain_s')
            lines.append(f'| {label(row)} | {row["repetition"]} | ' + ' | '.join(number(row.get(k)) for k in keys) + ' |')
    lines += [f'- {row["condition"]}/{row["method"]}: {row["error"]}' for row in rows if row.get('error')]
    if buffered:
        lines += ['', 'Buffer comparison: fresh process per condition. RSS is the observed process high-water mark;',
                  'pending counts/bytes are sampled. Raw payload bytes exclude parsed objects and SDK/table memory.', '',
                  '| Condition | Buffer messages | Repeat | Peak RSS (MiB) | Sampled pending messages | Sampled payload (MiB) | Unacknowledged at write end | Drain (s) | Outcome |',
                  '|---|---:|---:|---:|---:|---:|---:|---:|---|']
        for row in rows:
            values = [row['condition'], row.get('max_pending_deliveries'), row['repetition'],
                      row['peak_rss_bytes'] / 1024 ** 2 if row.get('peak_rss_bytes') is not None else None,
                      row.get('peak_pending_messages'),
                      row['peak_pending_body_bytes'] / 1024 ** 2 if row.get('peak_pending_body_bytes') is not None else None,
                      row.get('unacknowledged_at_write_end'), row.get('drain_s'), row['status']]
            lines.append('| ' + ' | '.join(number(value) for value in values) + ' |')
    lines += [f'- {error}' for error in errors]
    (root / 'SUMMARY.md').write_text('\n'.join(lines) + '\n')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    data = json.loads(args.results.read_text())
    write_activity_reports(args.output_dir, data['rows'], data['timeline'], data['errors'])
