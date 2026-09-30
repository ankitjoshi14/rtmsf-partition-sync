"""Observed S3 request counts from CloudWatch; absence alone is not zero.

Each trial must own its bucket. Keep setup outside the starting minute and the
bucket idle after the workload through quiet_until (a complete UTC minute).
CloudWatch can assign a request to a later minute, so ending collection at the
last API response loses real counts. Delivery completeness is not guaranteed.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
import time
import uuid


METRICS = {'s3_head_requests': 'HeadRequests', 's3_get_requests': 'GetRequests',
           's3_list_requests': 'ListRequests'}
SUPPORTING_METRICS = {'all_requests': 'AllRequests', 'put_requests': 'PutRequests',
                      'post_requests': 'PostRequests', 'delete_requests': 'DeleteRequests',
                      'select_requests': 'SelectRequests'}


def wait_until(deadline, label):
    while (remaining := deadline - time.time()) > 0:
        print(f'{label}: {remaining / 60:.1f} minutes remaining', flush=True)
        time.sleep(min(30, remaining))


def _time(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError('request windows require explicit timezone offsets')
    return value.astimezone(timezone.utc)


class RequestMetrics:
    def __init__(self, session, region, evidence_dir):
        from botocore.config import Config
        self.cw = session.client('cloudwatch', region_name=region,
                                 config=Config(connect_timeout=3, read_timeout=15,
                                               retries={'total_max_attempts': 2}))
        self.root = Path(evidence_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.coverage = {}

    def collect(self, windows):
        """Read a fresh snapshot; repeated collections replace, never add totals."""
        bounds = {}
        for window in windows:
            start, end = _time(window['started_at']), _time(window['ended_at'])
            if end < start or window['id'] in bounds:
                raise ValueError('invalid or duplicate request window')
            if 'quiet_until' not in window:
                raise ValueError('record quiet_until: the bucket must stay idle during metric collection')
            quiet = _time(window['quiet_until'])
            if quiet < end or quiet <= start or quiet != quiet.replace(second=0, microsecond=0):
                raise ValueError('quiet_until must be a complete minute after the workload')
            bounds[window['id']] = (start.replace(second=0, microsecond=0), quiet)
        counts, details, errors = {}, {}, []
        for window in windows:
            start, end = bounds[window['id']]
            observed, reasons, raw = {}, {}, {}
            for group in (METRICS, SUPPORTING_METRICS):
                if group is SUPPORTING_METRICS and (
                        'no_datapoints' not in reasons.values()
                        or 'collection_error' in reasons.values()
                        or not any(v is not None and v > 0 for v in observed.values())):
                    break
                for metric, name in group.items():
                    value, reason = None, 'no_datapoints'
                    try:
                        response = self.cw.get_metric_statistics(
                            Namespace='AWS/S3', MetricName=name, Period=60, Statistics=['Sum'],
                            Dimensions=[{'Name': 'BucketName', 'Value': window['bucket']},
                                        {'Name': 'FilterId', 'Value': window.get('filter_id', 'trial')}],
                            StartTime=start, EndTime=end)
                        raw[metric] = response
                        points = response.get('Datapoints', [])
                        if points:
                            seen, total = set(), 0
                            for point in points:
                                timestamp, amount = _time(point['Timestamp']), float(point['Sum'])
                                if (timestamp in seen or not start <= timestamp < end
                                        or not math.isfinite(amount) or amount < 0 or not amount.is_integer()):
                                    raise ValueError('invalid request metric datapoint')
                                seen.add(timestamp)
                                total += int(amount)
                            value, reason = total, 'observed'
                    except Exception as error:
                        reason = 'collection_error'
                        errors.append(dict(window=window['id'], metric=metric, error=type(error).__name__))
                    observed[metric], reasons[metric] = value, reason
            # This accounts for the recorded total, not requests AWS failed to record.
            total = observed.get('all_requests')
            accounted = sum(v for k, v in observed.items() if k != 'all_requests' and v is not None)
            if total is not None and total == accounted and 'collection_error' not in reasons.values():
                for metric in METRICS:
                    if reasons[metric] == 'no_datapoints':
                        observed[metric], reasons[metric] = 0, 'zero_accounted_in_total'
            counts[window['id']] = {metric: observed[metric] for metric in METRICS}
            details[window['id']] = dict(window=window, observed_counts=counts[window['id']],
                                         supporting_counts={k: observed[k] for k in SUPPORTING_METRICS if k in observed},
                                         metric_reasons=reasons, responses=raw,
                                         metric_time_bounds={'started_at': start.isoformat(),
                                                             'ended_before': end.isoformat()})
        self.coverage = dict(source='cloudwatch', completeness='not_guaranteed',
                             count_semantics='observed_request_metrics',
                             zero_inference='Absent metric inferred zero only when positive method activity exists, '
                                            'all metric reads succeed, and observed operation counts sum to AllRequests.',
                             collection_cutoff=datetime.now(timezone.utc).isoformat(),
                             windows=details, errors=errors)
        payload = json.dumps(self.coverage, indent=2, sort_keys=True, default=str) + '\n'
        (self.root / 'collections').mkdir(exist_ok=True)
        (self.root / 'collections' / f'{uuid.uuid4().hex}.json').write_text(payload)
        (self.root / 'coverage.json').write_text(payload)
        return counts
