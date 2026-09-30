"""A fresh, bounded Glue event crawler for one existing pilot table at a time."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time
from urllib.parse import unquote_plus, urlsplit
import uuid


EVENT_MODE_LOG = "The crawl is running by consuming Amazon S3 events."
LIST_MODE_LOG = "Crawl is not running in S3 event mode"


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timestamp(value) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


class EventCrawler:
    def __init__(self, glue, sqs, logs, outputs: dict, evidence_dir):
        self.glue, self.sqs, self.logs = glue, sqs, logs
        self.outputs = outputs
        self.queue_url = outputs["CrawlerEventQueueUrl"]
        self.root = Path(evidence_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.name = None
        self.evidence = {}
        self._owned_prefixes = set()
        self._started_at = None
        self.started_monotonic = None
        self._previous_start = None

    def _save(self) -> None:
        if self.name:
            (self.root / f"{self.name}.json").write_text(json.dumps(self.evidence, indent=2, sort_keys=True, default=str) + "\n")

    def _attributes(self) -> dict:
        return self.sqs.get_queue_attributes(QueueUrl=self.queue_url, AttributeNames=["QueueArn", "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"])["Attributes"]

    def initialize(self, table: str, label: str, timeout_seconds: float = 900) -> str:
        """Create a crawler, finish its initial listing crawl, and drain seed events."""
        if self.name and not self.evidence.get("deleted_at"):
            raise ValueError("delete the previous crawler before initializing another")
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", label):
            raise ValueError("crawler label must be a simple identifier")
        location = self.glue.get_table(DatabaseName=self.outputs["GlueDatabaseName"], Name=table)["Table"]["StorageDescriptor"]["Location"]
        target = urlsplit(location)
        prefix = target.path.lstrip("/").rstrip("/") + "/"
        if target.scheme != "s3" or target.netloc != self.outputs["CrawlerBucketName"] or not prefix.startswith("fixture/") or prefix == "fixture/":
            raise ValueError("crawler target must be an isolated prefix in its owned fixture bucket")
        self._owned_prefixes.add(prefix)
        self.name = f"rtmsf-crawler-{label}-{uuid.uuid4().hex[:12]}"
        request = {
            "Name": self.name, "Role": self.outputs["CrawlerRoleArn"],
            "DatabaseName": self.outputs["GlueDatabaseName"],
            "Targets": {"CatalogTargets": [{"DatabaseName": self.outputs["GlueDatabaseName"], "Tables": [table], "EventQueueArn": self._attributes()["QueueArn"]}]},
            "RecrawlPolicy": {"RecrawlBehavior": "CRAWL_EVENT_MODE"},
            "SchemaChangePolicy": {"UpdateBehavior": "LOG", "DeleteBehavior": "LOG"},
            "Configuration": json.dumps({"Version": 1.0, "CrawlerOutput": {"Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}}}),
        }
        self.evidence = {"crawler_name": self.name, "table": table, "location": location, "create_request": request}
        try:
            self.glue.create_crawler(**request)
            self.start()
            self._completion(timeout_seconds, require_event_mode=False)
            self._drain_seed(timeout_seconds=min(timeout_seconds, 240))
            return self.name
        except Exception as error:
            self.evidence["initialization_error"] = type(error).__name__
            raise
        finally:
            self._save()

    def _owns_notification(self, body: str) -> bool:
        try:
            message = json.loads(body)
            if message.get("Event") == "s3:TestEvent":
                return message.get("Bucket") == self.outputs["CrawlerBucketName"]
            records = message.get("Records", [])
            return bool(records) and all(
                item.get("eventSource") == "aws:s3"
                and item["s3"]["bucket"]["name"] == self.outputs["CrawlerBucketName"]
                and any(unquote_plus(item["s3"]["object"]["key"]).startswith(prefix) for prefix in self._owned_prefixes)
                for item in records
            )
        except (ValueError, KeyError, TypeError):
            return False

    def _drain_seed(self, timeout_seconds: float) -> None:
        deadline, empty = time.monotonic() + timeout_seconds, 0
        record = self.evidence["seed_drain"] = {"started_at": _utc(), "deleted_messages": 0, "attributes": []}
        try:
            while time.monotonic() < deadline:
                attributes = self._attributes()
                record["attributes"].append({"at": _utc(), **attributes})
                active = sum(int(attributes.get(name, 0)) for name in ("ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"))
                if not active:
                    empty += 1
                    if empty >= 2:
                        record["ended_at"] = _utc()
                        return
                else:
                    empty = 0
                    response = self.sqs.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=10)
                    messages = response.get("Messages", [])
                    if not all(self._owns_notification(message["Body"]) for message in messages):
                        for message in messages:
                            self.sqs.change_message_visibility(QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"], VisibilityTimeout=0)
                        raise ValueError("refusing to delete an unowned crawler queue message")
                    if messages:
                        entries = [{"Id": str(index), "ReceiptHandle": message["ReceiptHandle"]} for index, message in enumerate(messages)]
                        response = self.sqs.delete_message_batch(QueueUrl=self.queue_url, Entries=entries)
                        successful = {item["Id"] for item in response.get("Successful", [])}
                        record["deleted_messages"] += len(successful)
                        if response.get("Failed") or successful != {item["Id"] for item in entries}:
                            raise RuntimeError("crawler seed message batch deletion was incomplete")
                        continue
                time.sleep(min(2, max(0, deadline - time.monotonic())))
            raise TimeoutError("crawler seed queue did not drain within its bound")
        finally:
            self._save()

    def _matching_created_events(self, message: dict, expected_key: str) -> list[dict]:
        matches = []
        try:
            for event in json.loads(message["Body"]).get("Records", []):
                bucket = event["s3"]["bucket"]["name"]
                key = unquote_plus(event["s3"]["object"]["key"])
                if (event.get("eventSource") == "aws:s3" and str(event.get("eventName", "")).startswith("ObjectCreated:")
                        and bucket == self.outputs["CrawlerBucketName"] and key == expected_key):
                    matches.append({"message_id": message.get("MessageId"), "bucket": bucket, "key": key,
                                    "event_name": event["eventName"], "event_time": event.get("eventTime"),
                                    "sequencer": event["s3"]["object"].get("sequencer"),
                                    "s3_request_id": (event.get("responseElements") or {}).get("x-amz-request-id")})
        except (ValueError, KeyError, TypeError, AttributeError):
            return []
        return matches

    def _observe_expected_notification(self, expected_key: str, observation: dict, remaining: float) -> bool:
        response = self.sqs.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10,
                                           WaitTimeSeconds=1 if remaining >= 1 else 0, VisibilityTimeout=10)
        messages = response.get("Messages", [])
        observation["expected_key"] = expected_key
        observation["received_message_ids"] = [message.get("MessageId") for message in messages]
        observation["matched_events"] = []
        try:
            for message in messages:
                observation["matched_events"].extend(self._matching_created_events(message, expected_key))
        finally:
            if messages:
                entries = [{"Id": str(index), "ReceiptHandle": message["ReceiptHandle"], "VisibilityTimeout": 0}
                           for index, message in enumerate(messages)]
                response = self.sqs.change_message_visibility_batch(QueueUrl=self.queue_url, Entries=entries)
                observation["release"] = response
                successful = {item["Id"] for item in response.get("Successful", [])}
                if response.get("Failed") or successful != {item["Id"] for item in entries}:
                    raise RuntimeError("crawler notification batch release was incomplete")
        return bool(observation["matched_events"])

    def notification_ready(self, timeout_seconds: float = 120, expected_key: str | None = None) -> dict:
        """Observe queue activity, optionally verifying a particular new object.

        With expected_key, receive bounded batches and release every message at
        visibility zero before returning. No messages are deleted. This proves
        arrival of the selected event, not absence of late seed notifications or
        subsequent consumption by the crawler. The default only reads attributes.
        """
        deadline = time.monotonic() + timeout_seconds
        observations = self.evidence["notification_wait"] = []
        try:
            while True:
                attributes = self._attributes()
                observation = {"at": _utc(), **attributes}
                observations.append(observation)
                if expected_key is not None:
                    if self._observe_expected_notification(expected_key, observation, deadline - time.monotonic()):
                        return attributes
                elif int(attributes.get("ApproximateNumberOfMessages", 0)) > 0:
                    return attributes
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("crawler notification did not arrive within its bound")
                time.sleep(min(2, remaining))
        except Exception as error:
            if expected_key is not None and observations:
                observations[-1]["error"] = type(error).__name__
            raise
        finally:
            self._save()

    def start(self) -> str:
        if not self.name:
            raise ValueError("initialize a crawler before starting")
        crawler = self.glue.get_crawler(Name=self.name)["Crawler"]
        if crawler["State"] != "READY":
            raise RuntimeError("crawler must be READY before starting")
        self._previous_start = crawler.get("LastCrawl", {}).get("StartTime")
        self._started_at = _utc()
        self.started_monotonic = time.monotonic()
        self.evidence.setdefault("starts", []).append(self._started_at)
        try:
            self.glue.start_crawler(Name=self.name)
            return self._started_at
        finally:
            self._save()

    def _log_events(self, last_crawl: dict) -> list[dict]:
        if not last_crawl.get("LogGroup") or not last_crawl.get("LogStream"):
            return []
        request = {"logGroupName": last_crawl["LogGroup"], "logStreamName": last_crawl["LogStream"], "startTime": int(_timestamp(last_crawl["StartTime"]) * 1000), "startFromHead": True}
        events = {}
        for _ in range(20):
            response = self.logs.get_log_events(**request)
            for event in response.get("events", []):
                events[(event["timestamp"], event["message"])] = event
            token = response.get("nextForwardToken")
            if not token or token == request.get("nextToken"):
                return list(events.values())
            request["nextToken"] = token
        raise RuntimeError("crawler log pagination exceeded its bound")

    def _completion(self, timeout_seconds: float, require_event_mode: bool) -> dict:
        if not self._started_at:
            raise ValueError("start the crawler before waiting for completion")
        deadline = time.monotonic() + timeout_seconds
        stage = "measured" if require_event_mode else "initial"
        result = self.evidence[stage] = {"started_at": self._started_at, "event_mode": False, "log_events": [], "observations": []}
        try:
            while True:
                crawler = self.glue.get_crawler(Name=self.name)["Crawler"]
                result["crawler"] = crawler
                result["observations"].append({"at": _utc(), "state": crawler["State"]})
                last = crawler.get("LastCrawl", {})
                current_run = (last.get("StartTime") is not None and last["StartTime"] != self._previous_start and _timestamp(last["StartTime"]) >= int(_timestamp(self._started_at)))
                if crawler["State"] == "READY" and current_run:
                    result.setdefault("completed_at", _utc())
                    if last.get("Status") != "SUCCEEDED":
                        raise RuntimeError(f"crawler ended with {last.get('Status')}")
                    try:
                        result["log_events"] = self._log_events(last)
                    except Exception as error:
                        result["log_error"] = type(error).__name__
                    messages = "\n".join(item["message"] for item in result["log_events"])
                    result["event_mode"] = EVENT_MODE_LOG in messages and LIST_MODE_LOG not in messages
                    if require_event_mode and LIST_MODE_LOG in messages:
                        raise RuntimeError("measured crawler fell back to listing mode")
                    if not require_event_mode or result["event_mode"]:
                        return result
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("crawler completion or event-mode evidence did not arrive within its bound")
                time.sleep(min(5, remaining))
        except Exception as error:
            result["error"] = type(error).__name__
            raise
        finally:
            self._save()

    def completion(self, timeout_seconds: float = 900) -> dict:
        return self._completion(timeout_seconds, require_event_mode=True)

    def delete(self, timeout_seconds: float = 300) -> None:
        """Stop and delete only this helper's freshly created crawler."""
        from botocore.exceptions import ClientError

        if not self.name or self.evidence.get("deleted_at"):
            return
        deadline = time.monotonic() + timeout_seconds
        try:
            crawler = self.glue.get_crawler(Name=self.name)["Crawler"]
            if crawler["State"] == "RUNNING":
                self.glue.stop_crawler(Name=self.name)
            while crawler["State"] != "READY":
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("crawler did not stop before deletion")
                time.sleep(min(5, remaining))
                crawler = self.glue.get_crawler(Name=self.name)["Crawler"]
            self.glue.delete_crawler(Name=self.name)
            self.evidence["deleted_at"] = _utc()
        except Exception as error:
            if isinstance(error, ClientError) and error.response.get("Error", {}).get("Code") == "EntityNotFoundException":
                self.evidence["already_absent"] = True
                self.evidence["deleted_at"] = _utc()
            else:
                self.evidence["cleanup_error"] = type(error).__name__
                raise
        finally:
            self._save()
