from copy import deepcopy
from datetime import datetime, timezone
import gzip
import io
import json

import pytest


OUTPUTS = {"DataBucketName": "rtmsf-data", "MsckBucketName": "msck-data", "CrawlerBucketName": "crawler-data", "AuditBucketName": "audit-logs", "TrailName": "request-trail"}
ROLE = "arn:aws:iam::123456789012:role/worker"


def event(identifier, operation="GetObject", bucket="rtmsf-data", when="2026-09-21T12:00:05Z", request_id=None):
    return {"eventID": identifier, "requestID": request_id or identifier, "eventTime": when,
            "eventSource": "s3.amazonaws.com", "awsRegion": "us-west-2", "eventName": operation,
            "requestParameters": {"bucketName": bucket, "key": "fixture/trial/part.parquet"},
            "userIdentity": {"arn": "arn:aws:sts::123456789012:assumed-role/worker/task", "sessionContext": {"sessionIssuer": {"arn": ROLE}}}}


def window(identifier="trial", bucket="rtmsf-data", **extra):
    return {"id": identifier, "bucket": bucket, "started_at": "2026-09-21T12:00:00+00:00", "ended_at": "2026-09-21T12:00:10+00:00", **extra}


class S3:
    def __init__(self):
        self.objects = {}
        self.requests = []
        self.listings = 0
        self.fail_list = False

    def response(self, operation, **params):
        request_id = f"request-{len(self.requests)}"
        self.requests.append({"operation": operation, "request_id": request_id, **params})
        return {"ResponseMetadata": {"RequestId": request_id}}

    def put_object(self, **params):
        self.objects[(params["Bucket"], params["Key"])] = params["Body"]
        return self.response("PutObject", **params)

    def head_bucket(self, **params):
        return self.response("HeadBucket", **params)

    def head_object(self, **params):
        return self.response("HeadObject", **params)

    def get_object(self, **params):
        return {**self.response("GetObject", **params), "Body": io.BytesIO(self.objects[(params["Bucket"], params["Key"])])}

    def get_paginator(self, operation):
        s3 = self

        class Paginator:
            def paginate(self, **params):
                if params["Bucket"] == OUTPUTS["AuditBucketName"]:
                    s3.listings += 1
                    if s3.fail_list:
                        raise RuntimeError("log list unavailable")
                keys = sorted(key for bucket, key in s3.objects if bucket == params["Bucket"] and key.startswith(params.get("Prefix", "")))
                size = params.get("PaginationConfig", {}).get("PageSize", len(keys) or 1)
                for i in range(0, len(keys), size):
                    yield {**s3.response("ListObjectsV2" if operation == "list_objects_v2" else "ListObjects", **params), "Contents": [{"Key": key} for key in keys[i:i + size]]}
        return Paginator()

    def deliver(self, records, name="part.json.gz"):
        payload = gzip.compress(json.dumps({"Records": records}).encode())
        self.objects[(OUTPUTS["AuditBucketName"], f"AWSLogs/123456789012/CloudTrail/us-west-2/2026/09/21/{name}")] = payload
        return payload


class Session:
    def __init__(self, s3):
        self.s3 = s3

    def client(self, service, **kwargs):
        assert service == "s3"
        return self.s3


def calibrated(tmp_path):
    from experiments.aws.request_audit import RequestAudit
    s3 = S3()
    audit = RequestAudit(Session(s3), OUTPUTS, "us-west-2", tmp_path)
    known = audit.calibrate()
    events = [event(f"cal-{i}", "ListObjects" if item["operation"] == "ListObjectsV2" else item["operation"], item["bucket"], when="2026-09-21T11:59:00Z", request_id=item["request_id"]) for i, item in enumerate(known)]
    return audit, s3, events


def test_calibration_exercises_every_bucket_and_pagination_outside_fixtures(tmp_path):
    audit, s3, records = calibrated(tmp_path)
    known = json.loads((tmp_path / "calibration.json").read_text())
    assert len(known) == 21  # HEAD object/bucket, GET, and two pages for each LIST API.
    assert len({item["request_id"] for item in known}) == len(known)
    for bucket in ("rtmsf-data", "msck-data", "crawler-data"):
        operations = [item["operation"] for item in known if item["bucket"] == bucket]
        assert operations.count("ListObjects") == operations.count("ListObjectsV2") == 2
        assert {"HeadObject", "HeadBucket", "GetObject"} <= set(operations)
    assert all(not key.startswith("fixture/") for bucket, key in s3.objects)


def test_collect_counts_retries_once_per_event_and_filters_window_bucket_identity(tmp_path):
    audit, s3, calibration = calibrated(tmp_path)
    first = event("get-first")
    retry = event("get-retry")
    other_identity = event("other-user")
    other_identity["userIdentity"] = {"arn": "arn:aws:iam::123456789012:user/other"}
    outside = [event("other-bucket", bucket="msck-data"), event("too-late", when="2026-09-21T12:00:11Z"), event("too-early", when="2026-09-21T11:59:59Z"), other_identity, event("write", "PutObject")]
    source = s3.deliver(calibration + [first, retry, event("head", "HeadObject"), event("list", "ListObjects"), event("list-v2", "ListObjectsV2")] + outside)
    s3.deliver([deepcopy(first)], "duplicate.json.gz")
    result = audit.collect([window(requester_arn=ROLE)], wait_seconds=0)
    assert result == {"trial": {"s3_head_requests": 1, "s3_get_requests": 2, "s3_list_requests": 2}}
    coverage = json.loads((tmp_path / "coverage.json").read_text())
    assert coverage["completeness"] == "not_guaranteed"
    assert coverage["calibration"]["rtmsf-data"]["covered"] is True
    assert coverage["calibration"]["rtmsf-data"]["event_names"]["ListObjectsV2"] == ["ListObjects"]
    assert coverage["windows"]["trial"]["observed_counts"] == result["trial"]
    assert len(coverage["windows"]["trial"]["events"]) == 5
    assert coverage["collection_cutoff"]
    assert source in [path.read_bytes() for path in (tmp_path / "cloudtrail").glob("*.json.gz")]
    raw = json.loads((tmp_path / "events.json").read_text())
    assert len([item for item in raw if item["eventID"] == "get-first"]) == 1


def test_missing_calibration_or_real_method_activity_is_unavailable_not_zero(tmp_path):
    audit, s3, calibration = calibrated(tmp_path)
    s3.deliver(calibration[1:] + [event("list", "ListObjects")])
    result = audit.collect([window(), window("quiet", "msck-data")], wait_seconds=0)
    unavailable = {"s3_head_requests": None, "s3_get_requests": None, "s3_list_requests": None}
    assert result == {"trial": {"s3_head_requests": None, "s3_get_requests": 0, "s3_list_requests": 1}, "quiet": unavailable}
    assert audit.coverage["windows"]["trial"]["observed_counts"]["s3_list_requests"] == 1
    assert audit.coverage["windows"]["trial"]["reason"] == "partial_calibration"
    assert audit.coverage["calibration"]["rtmsf-data"]["families"]["s3_head_requests"]["covered"] is False
    assert audit.coverage["calibration"]["rtmsf-data"]["families"]["s3_list_requests"]["covered"] is True
    assert audit.coverage["windows"]["quiet"]["reason"] == "no_method_events"


def test_log_failure_is_retained_as_unavailable_and_later_collection_reconciles(tmp_path):
    audit, s3, calibration = calibrated(tmp_path)
    s3.fail_list = True
    assert audit.collect([window()], wait_seconds=0)["trial"]["s3_get_requests"] is None
    assert audit.coverage["errors"]
    s3.fail_list = False
    s3.deliver(calibration + [event("get")])
    assert audit.collect([window()], wait_seconds=0)["trial"]["s3_get_requests"] == 1
    s3.deliver([event("late-delivery")], "later.json.gz")
    assert audit.collect([window()], wait_seconds=0)["trial"]["s3_get_requests"] == 2
    assert len(list((tmp_path / "collections").glob("*.json"))) == 3


def test_collection_waits_full_bound_even_when_calibration_and_events_arrive(monkeypatch, tmp_path):
    import experiments.aws.request_audit as module
    audit, s3, calibration = calibrated(tmp_path)
    s3.deliver(calibration + [event("get")])
    elapsed = [0]
    monkeypatch.setattr(module.time, "monotonic", lambda: elapsed[0])
    monkeypatch.setattr(module.time, "sleep", lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds))
    audit.collect([window()], wait_seconds=65)
    assert elapsed[0] == 65
    assert s3.listings >= 2
    assert audit.coverage["completeness"] == "not_guaranteed"


def test_invalid_windows_are_rejected_before_collecting(tmp_path):
    audit, s3, calibration = calibrated(tmp_path)
    with pytest.raises(ValueError, match="timezone"):
        audit.collect([window(started_at="2026-09-21T12:00:00")], wait_seconds=0)
    with pytest.raises(ValueError, match="bucket"):
        audit.collect([window(bucket="unrelated")], wait_seconds=0)
    with pytest.raises(ValueError, match="duplicate"):
        audit.collect([window(), window()], wait_seconds=0)
    assert s3.listings == 0


def test_calibration_rejects_request_id_with_wrong_operation(tmp_path):
    audit, s3, calibration = calibrated(tmp_path)
    calibration[0]["eventName"] = "HeadObject"  # ID belongs to HeadBucket.
    s3.deliver(calibration + [event("get")])
    counts = audit.collect([window()], wait_seconds=0)["trial"]
    assert counts["s3_head_requests"] is None
    assert counts["s3_get_requests"] == 1


def test_reconciliation_can_resume_from_retained_sources(tmp_path):
    from experiments.aws.request_audit import RequestAudit
    audit, s3, calibration = calibrated(tmp_path)
    s3.deliver(calibration + [event("get")])
    audit.collect([window()], wait_seconds=0)
    resumed = RequestAudit(Session(s3), OUTPUTS, "us-west-2", tmp_path)
    s3.deliver([event("later")], "later.json.gz")
    assert resumed.collect([window()], wait_seconds=0)["trial"]["s3_get_requests"] == 2
    assert resumed.coverage["count_semantics"] == "observed_lower_bound"
    assert len(resumed.coverage["sources"]) == 2


def test_window_accounts_for_cloudtrail_whole_second_timestamps(tmp_path):
    audit, s3, calibration = calibrated(tmp_path)
    s3.deliver(calibration + [event("first-second", when="2026-09-21T12:00:00Z"), event("last-second", when="2026-09-21T12:00:10Z"), event("outside", when="2026-09-21T12:00:11Z")])
    result = audit.collect([window(started_at="2026-09-21T12:00:00.500+00:00", ended_at="2026-09-21T12:00:10.500+00:00")], wait_seconds=0)
    assert result["trial"]["s3_get_requests"] == 2
    assert audit.coverage["windows"]["trial"]["event_time_bounds"] == {"started_at": "2026-09-21T12:00:00+00:00", "ended_before": "2026-09-21T12:00:11+00:00", "precision": "whole_seconds"}
