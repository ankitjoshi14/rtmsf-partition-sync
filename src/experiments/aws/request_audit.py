"""Collect observed S3 requests without treating CloudTrail delivery as complete.

Calibration runs immediately; collect after the trials to amortize delivery wait.
Raw gzip sources and every collection's cutoff/report are retained for later
reconciliation. Integer results are observed counts, never exact accounting.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
import time
import uuid


OPERATIONS = {
    "HeadObject": "s3_head_requests", "HeadBucket": "s3_head_requests",
    "GetObject": "s3_get_requests",
    "ListObjects": "s3_list_requests", "ListObjectsV2": "s3_list_requests",
}
BUCKET_OUTPUTS = ("DataBucketName", "MsckBucketName", "CrawlerBucketName")
METRICS = ("s3_head_requests", "s3_get_requests", "s3_list_requests")


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timestamp(value) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("request windows require explicit timezone offsets")
    return result.astimezone(timezone.utc)


def _bucket(event: dict) -> str | None:
    name = (event.get("requestParameters") or {}).get("bucketName")
    if name:
        return name
    for resource in event.get("resources", []):
        arn = resource.get("ARN", "")
        if ":s3:::" in arn:
            return arn.split(":s3:::", 1)[1].split("/", 1)[0]
    return None


def _requester_matches(event: dict, arn: str | None) -> bool:
    if arn is None:
        return True
    identity = event.get("userIdentity") or {}
    issuer = identity.get("sessionContext", {}).get("sessionIssuer", {})
    return arn in (identity.get("arn"), issuer.get("arn"))


class RequestAudit:
    def __init__(self, session, outputs: dict, region: str, evidence_dir):
        from botocore.config import Config

        self.s3 = session.client("s3", region_name=region, config=Config(connect_timeout=3, read_timeout=15, retries={"total_max_attempts": 2}))
        self.buckets = [outputs[name] for name in BUCKET_OUTPUTS]
        self.audit_bucket = outputs["AuditBucketName"]
        self.trail_name = outputs["TrailName"]
        self.region = region
        self.root = Path(evidence_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.calibration = self._read_json("calibration.json", [])
        self.coverage = self._read_json("coverage.json", {})
        self.events = {event["eventID"]: event for event in self._read_json("events.json", [])}
        self.sources = self.coverage.get("sources", {})

    def _read_json(self, name: str, default):
        path = self.root / name
        return json.loads(path.read_text()) if path.exists() else default

    def _write_json(self, name: str, value) -> None:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")

    def calibrate(self) -> list[dict]:
        """Issue HEAD/GET and two pages of each LIST API in every fixture bucket."""
        prefix = f"_request_audit/{uuid.uuid4().hex}/"

        def record(bucket, operation, response, started_at):
            self.calibration.append({
                "bucket": bucket, "operation": operation,
                "request_id": response["ResponseMetadata"]["RequestId"],
                "sdk_retry_attempts": response["ResponseMetadata"].get("RetryAttempts", 0),
                "started_at": started_at, "ended_at": _utc(), "prefix": prefix,
            })

        try:
            for bucket in self.buckets:
                for name in ("one", "two"):
                    self.s3.put_object(Bucket=bucket, Key=prefix + name, Body=b"request audit calibration\n")
                for operation, method, params in (
                    ("HeadBucket", self.s3.head_bucket, {"Bucket": bucket}),
                    ("HeadObject", self.s3.head_object, {"Bucket": bucket, "Key": prefix + "one"}),
                    ("GetObject", self.s3.get_object, {"Bucket": bucket, "Key": prefix + "one"}),
                ):
                    started_at = _utc()
                    response = method(**params)
                    if operation == "GetObject":
                        response["Body"].read()
                        response["Body"].close()
                    record(bucket, operation, response, started_at)
                for operation, api in (("ListObjects", "list_objects"), ("ListObjectsV2", "list_objects_v2")):
                    started_at = _utc()
                    for response in self.s3.get_paginator(api).paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1}):
                        record(bucket, operation, response, started_at)
                        started_at = _utc()
        finally:
            self._write_json("calibration.json", self.calibration)
        return self.calibration

    def _read_delivered(self, errors: list[dict]) -> None:
        try:
            pages = self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.audit_bucket, Prefix="AWSLogs/")
            for page in pages:
                for item in page.get("Contents", []):
                    key = item["Key"]
                    if key in self.sources or f"/CloudTrail/{self.region}/" not in key or not key.endswith(".json.gz"):
                        continue
                    try:
                        response = self.s3.get_object(Bucket=self.audit_bucket, Key=key)
                        try:
                            raw = response["Body"].read()
                        finally:
                            response["Body"].close()
                        local_name = f"cloudtrail/{hashlib.sha256(key.encode()).hexdigest()}.json.gz"
                        local_path = self.root / local_name
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        local_path.write_bytes(raw)
                        records = json.loads(gzip.decompress(raw))["Records"]
                        for event in records:
                            if "eventID" not in event:
                                raise ValueError("CloudTrail record has no eventID")
                            self.events.setdefault(event["eventID"], event)
                        self.sources[key] = {"path": local_name, "sha256": hashlib.sha256(raw).hexdigest(), "collected_at": _utc(), "record_count": len(records)}
                    except Exception as error:
                        errors.append({"operation": "read_log", "key": key, "error": type(error).__name__})
        except Exception as error:
            errors.append({"operation": "list_logs", "error": type(error).__name__})

    def _calibration_coverage(self) -> dict:
        requests = {}
        for event in self.events.values():
            if event.get("eventSource") == "s3.amazonaws.com" and event.get("awsRegion") == self.region:
                requests.setdefault((_bucket(event), event.get("requestID")), []).append(event)
        coverage = {}
        for bucket in self.buckets:
            known = [item for item in self.calibration if item["bucket"] == bucket]
            operations = Counter(item["operation"] for item in known)
            exercised = all(operations[name] >= (2 if name.startswith("List") else 1) for name in OPERATIONS)
            missing, names = [], {}
            for item in known:
                allowed_names = {"ListObjects", "ListObjectsV2"} if item["operation"].startswith("List") else {item["operation"]}
                matches = [event for event in requests.get((bucket, item["request_id"]), []) if event.get("eventName") in allowed_names]
                if not matches:
                    missing.append(item["request_id"])
                names.setdefault(item["operation"], set()).update(event["eventName"] for event in matches)
            families = {}
            for metric in METRICS:
                expected_operations = [name for name, family in OPERATIONS.items() if family == metric]
                family_known = [item for item in known if item["operation"] in expected_operations]
                family_missing = [item["request_id"] for item in family_known if item["request_id"] in missing]
                family_exercised = all(operations[name] >= (2 if name.startswith("List") else 1) for name in expected_operations)
                families[metric] = {"covered": bool(family_exercised and not family_missing), "expected_operations": expected_operations, "expected_requests": len(family_known), "matched_requests": len(family_known) - len(family_missing), "missing_request_ids": family_missing}
            coverage[bucket] = {"covered": bool(exercised and not missing), "expected_requests": len(known), "matched_requests": len(known) - len(missing), "missing_request_ids": missing, "event_names": {name: sorted(values) for name, values in names.items()}, "families": families}
        return coverage

    def collect(self, windows: list[dict], wait_seconds: float = 600) -> dict:
        """Return observed counts, or None when calibration/activity is unavailable.

        windows contain id, bucket, started_at, ended_at, and optionally
        requester_arn (a session ARN or its IAM role ARN). Each request family
        requires its own calibration: missing HeadBucket makes HEAD unavailable
        without suppressing independently calibrated GET/LIST. Bounds cover the
        complete UTC seconds containing the original start and end. The runner
        must keep setup and verification reads outside these widened bounds.
        Waiting never establishes completeness, even after a quiet interval.
        """
        if wait_seconds < 0:
            raise ValueError("wait_seconds must be nonnegative")
        bounds = {}
        for window in windows:
            if window["id"] in bounds:
                raise ValueError("duplicate request window id")
            if window["bucket"] not in self.buckets:
                raise ValueError("request window bucket is outside the audited fixtures")
            start, end = _timestamp(window["started_at"]), _timestamp(window["ended_at"])
            if end < start:
                raise ValueError("request window ends before it starts")
            bounds[window["id"]] = (start.replace(microsecond=0), end.replace(microsecond=0) + timedelta(seconds=1))
        started_at, deadline = _utc(), time.monotonic() + wait_seconds
        errors = []
        while True:
            self._read_delivered(errors)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(30, remaining))
        calibration = self._calibration_coverage()
        counts, details = {}, {}
        for window in windows:
            start, end = bounds[window["id"]]
            events = [event for event in self.events.values()
                      if event.get("eventSource") == "s3.amazonaws.com"
                      and event.get("awsRegion") == self.region
                      and event.get("eventName") in OPERATIONS
                      and _bucket(event) == window["bucket"]
                      and _requester_matches(event, window.get("requester_arn"))
                      and start <= _timestamp(event["eventTime"]) < end]
            observed = Counter(OPERATIONS[event["eventName"]] for event in events)
            reasons = {}
            for metric in METRICS:
                reason = "observed" if events else "no_method_events"
                if not calibration[window["bucket"]]["families"][metric]["covered"]:
                    reason = "calibration_incomplete"
                if errors:
                    reason = "collection_errors"
                reasons[metric] = reason
            counts[window["id"]] = {metric: observed[metric] if reasons[metric] == "observed" else None for metric in METRICS}
            reason = next(iter(set(reasons.values()))) if len(set(reasons.values())) == 1 else "partial_calibration"
            details[window["id"]] = {"window": window, "event_time_bounds": {"started_at": start.isoformat(), "ended_before": end.isoformat(), "precision": "whole_seconds"}, "reason": reason, "metric_reasons": reasons, "observed_counts": {metric: observed[metric] for metric in METRICS}, "events": events}
        self.coverage = {
            "audit_bucket": self.audit_bucket, "trail_name": self.trail_name,
            "collection_started_at": started_at, "collection_cutoff": _utc(),
            "wait_seconds": wait_seconds, "completeness": "not_guaranteed",
            "count_semantics": "observed_lower_bound",
            "limitations": ["CloudTrail delivery has no guaranteed completion time; calibration and quiet logs do not prove exact counts.", "Counts include recorded retries and deduplicate eventID. UTC windows widen to full seconds because CloudTrail eventTime lacks subsecond precision; the runner must separate setup and verification reads from these bounds."],
            "calibration": calibration, "windows": details, "errors": errors,
            "sources": self.sources, "unique_events": len(self.events),
        }
        self._write_json("events.json", list(self.events.values()))
        self._write_json("coverage.json", self.coverage)
        self._write_json(f"collections/{uuid.uuid4().hex}.json", self.coverage)
        return counts
