from datetime import datetime, timezone
import json

import pytest


OUTPUTS = {"GlueDatabaseName": "rtmsf_db", "CrawlerBucketName": "pilot-crawler", "CrawlerEventQueueUrl": "queue-url", "CrawlerRoleArn": "arn:aws:iam::123456789012:role/crawler"}
POSITIVE = "The crawl is running by consuming Amazon S3 events."
NEGATIVE = "Crawl is not running in S3 event mode"


def notification(key="fixture/trial/part.parquet"):
    return {"MessageId": "message-" + key, "ReceiptHandle": key, "Body": json.dumps({"Records": [{"eventSource": "aws:s3", "eventName": "ObjectCreated:Put", "s3": {"bucket": {"name": "pilot-crawler"}, "object": {"key": key}}}]})}


class Glue:
    def __init__(self):
        self.starts = 0
        self.last = {}
        self.state = "READY"
        self.deleted = []
        self.stale = False

    def get_table(self, **kwargs):
        return {"Table": {"StorageDescriptor": {"Location": "s3://pilot-crawler/fixture/trial/"}}}

    def create_crawler(self, **kwargs):
        self.created = kwargs

    def start_crawler(self, **kwargs):
        self.starts += 1
        if not self.stale:
            self.last = {"StartTime": datetime.now(timezone.utc), "Status": "SUCCEEDED", "LogGroup": "/aws-glue/crawlers", "LogStream": "crawler-stream"}

    def get_crawler(self, **kwargs):
        return {"Crawler": {"State": self.state, "LastCrawl": self.last}}

    def stop_crawler(self, **kwargs):
        self.state = "READY"

    def delete_crawler(self, **kwargs):
        self.deleted.append(kwargs["Name"])


class Sqs:
    def __init__(self):
        self.messages = [notification()]
        self.deleted = []
        self.receives = 0
        self.released = []
        self.batches = []

    def get_queue_attributes(self, **kwargs):
        return {"Attributes": {"QueueArn": "arn:aws:sqs:us-west-2:123456789012:pilot", "ApproximateNumberOfMessages": str(len(self.messages)), "ApproximateNumberOfMessagesNotVisible": "0", "ApproximateNumberOfMessagesDelayed": "0"}}

    def receive_message(self, **kwargs):
        self.receives += 1
        return {"Messages": self.messages[:10]}

    def delete_message(self, **kwargs):
        self.deleted.append(kwargs["ReceiptHandle"])
        self.messages = [item for item in self.messages if item["ReceiptHandle"] != kwargs["ReceiptHandle"]]

    def delete_message_batch(self, **kwargs):
        self.batches.append(kwargs["Entries"])
        for entry in kwargs["Entries"]:
            self.delete_message(ReceiptHandle=entry["ReceiptHandle"])
        return {"Successful": [{"Id": entry["Id"]} for entry in kwargs["Entries"]]}

    def change_message_visibility(self, **kwargs):
        self.released.append(kwargs)

    def change_message_visibility_batch(self, **kwargs):
        self.released.extend(kwargs["Entries"])
        return {"Successful": [{"Id": entry["Id"]} for entry in kwargs["Entries"]], "ResponseMetadata": {"RequestId": "release-request"}}


class Logs:
    def __init__(self, glue):
        self.glue = glue
        self.measured_message = POSITIVE

    def get_log_events(self, **kwargs):
        text = NEGATIVE if self.glue.starts == 1 else self.measured_message
        return {"events": [{"timestamp": kwargs["startTime"] + 1, "message": text}], "nextForwardToken": "end"}


@pytest.fixture
def setup(monkeypatch, tmp_path):
    from experiments.aws import crawler
    elapsed = [0]
    monkeypatch.setattr(crawler.time, "monotonic", lambda: elapsed[0])
    monkeypatch.setattr(crawler.time, "sleep", lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds))
    glue, sqs = Glue(), Sqs()
    logs = Logs(glue)
    helper = crawler.EventCrawler(glue, sqs, logs, OUTPUTS, tmp_path)
    return helper, glue, sqs, logs, tmp_path


def test_initialization_uses_catalog_event_target_and_drains_only_its_seed(setup):
    helper, glue, sqs, logs, root = setup
    name = helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    assert name == glue.created["Name"]
    assert glue.created["RecrawlPolicy"] == {"RecrawlBehavior": "CRAWL_EVENT_MODE"}
    assert glue.created["Targets"]["CatalogTargets"] == [{"DatabaseName": "rtmsf_db", "Tables": ["crawler_trial"], "EventQueueArn": "arn:aws:sqs:us-west-2:123456789012:pilot"}]
    assert glue.created["SchemaChangePolicy"] == {"UpdateBehavior": "LOG", "DeleteBehavior": "LOG"}
    assert sqs.deleted == ["fixture/trial/part.parquet"]
    assert helper.evidence["initial"]["event_mode"] is False
    assert list(root.glob("*.json"))


def test_notification_check_does_not_consume_and_measured_crawl_requires_positive_log(setup):
    helper, glue, sqs, logs, root = setup
    helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    sqs.messages = [notification("fixture/trial/new.parquet")]
    before = sqs.receives
    assert helper.notification_ready(timeout_seconds=20)["ApproximateNumberOfMessages"] == "1"
    assert sqs.receives == before
    started = helper.start()
    result = helper.completion(timeout_seconds=20)
    assert result["started_at"] == started
    assert result["event_mode"] is True
    assert result["log_events"][0]["message"] == POSITIVE
    helper.delete()
    assert glue.deleted == [helper.name]


def test_negative_event_mode_log_cannot_pass_as_event_mode(setup):
    helper, glue, sqs, logs, root = setup
    helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    logs.measured_message = NEGATIVE
    helper.start()
    with pytest.raises(RuntimeError, match="listing mode"):
        helper.completion(timeout_seconds=20)
    assert helper.evidence["measured"]["event_mode"] is False
    assert helper.evidence["measured"]["error"]


def test_unknown_seed_message_is_released_without_deletion(setup):
    helper, glue, sqs, logs, root = setup
    sqs.messages = [notification("fixture/someone_else/part.parquet")]
    with pytest.raises(ValueError, match="unowned"):
        helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    assert sqs.deleted == []
    assert sqs.released[0]["VisibilityTimeout"] == 0


def test_old_success_and_missing_event_mode_logs_time_out_with_evidence(setup):
    helper, glue, sqs, logs, root = setup
    helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    glue.stale = True
    helper.start()
    with pytest.raises(TimeoutError):
        helper.completion(timeout_seconds=5)
    assert helper.evidence["measured"]["error"] == "TimeoutError"
    assert helper.evidence["measured"]["crawler"]["LastCrawl"]


def test_seed_drain_batches_ten_owned_receipts_and_validates_whole_batch_first(setup):
    helper, glue, sqs, logs, root = setup
    sqs.messages = [notification(f"fixture/trial/part-{i}.parquet") for i in range(11)]
    helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    assert [len(batch) for batch in sqs.batches] == [10, 1]
    helper.delete()
    sqs.messages = [notification(), notification("fixture/another/part.parquet")]
    deleted = len(sqs.deleted)
    with pytest.raises(ValueError, match="unowned"):
        helper.initialize("crawler_trial", "next", timeout_seconds=20)
    assert len(sqs.deleted) == deleted
    assert len(sqs.released) == 2


def test_expected_notification_waits_for_exact_decoded_key_and_releases_every_message(setup):
    helper, glue, sqs, logs, root = setup
    helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    target = notification("fixture/trial/new+file.parquet")
    batches = [[notification()], [notification("fixture/unrelated/file.parquet"), target]]
    def receive(**kwargs):
        sqs.receives += 1
        return {"Messages": batches.pop(0)}
    sqs.receive_message = receive
    deleted, receives = len(sqs.deleted), sqs.receives
    helper.notification_ready(timeout_seconds=20, expected_key="fixture/trial/new file.parquet")
    assert sqs.receives - receives == 2
    assert len(sqs.deleted) == deleted
    assert len(sqs.released) == 3
    assert all(item["VisibilityTimeout"] == 0 for item in sqs.released)
    observation = helper.evidence["notification_wait"][-1]
    assert observation["matched_events"][0]["message_id"] == target["MessageId"]
    assert observation["matched_events"][0]["key"] == "fixture/trial/new file.parquet"
    assert observation["release"]["ResponseMetadata"]["RequestId"] == "release-request"
    assert glue.starts == 1


def test_expected_notification_release_failure_never_reports_ready(setup):
    helper, glue, sqs, logs, root = setup
    helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    sqs.messages = [notification("fixture/trial/new.parquet"), notification()]
    def partial_release(**kwargs):
        sqs.released.extend(kwargs["Entries"])
        return {"Successful": [{"Id": "0"}], "Failed": [{"Id": "1", "Code": "InternalError"}]}
    sqs.change_message_visibility_batch = partial_release
    deleted = len(sqs.deleted)
    with pytest.raises(RuntimeError, match="release"):
        helper.notification_ready(timeout_seconds=20, expected_key="fixture/trial/new.parquet")
    assert len(sqs.released) == 2
    assert len(sqs.deleted) == deleted
    assert helper.evidence["notification_wait"][-1]["error"] == "RuntimeError"
    assert glue.starts == 1


def test_expected_notification_rejects_other_bucket_and_delete_event(setup):
    helper, glue, sqs, logs, root = setup
    helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    key = "fixture/trial/new.parquet"
    wrong_bucket, deleted = notification(key), notification(key)
    body = json.loads(wrong_bucket["Body"])
    body["Records"][0]["s3"]["bucket"]["name"] = "other-bucket"
    wrong_bucket["Body"] = json.dumps(body)
    body = json.loads(deleted["Body"])
    body["Records"][0]["eventName"] = "ObjectRemoved:Delete"
    deleted["Body"] = json.dumps(body)
    sqs.messages = [wrong_bucket, deleted, {"MessageId": "invalid", "ReceiptHandle": "invalid", "Body": "invalid json"}]
    with pytest.raises(TimeoutError):
        helper.notification_ready(timeout_seconds=1, expected_key=key)
    assert all(item["VisibilityTimeout"] == 0 for item in sqs.released)
    assert not any(item["matched_events"] for item in helper.evidence["notification_wait"])


def test_cleanup_accepts_typed_missing_crawler_after_creation_failure(setup):
    from botocore.exceptions import ClientError
    helper, glue, sqs, logs, root = setup
    def fail_create(**kwargs):
        raise ClientError({"Error": {"Code": "AccessDeniedException"}}, "CreateCrawler")
    glue.create_crawler = fail_create
    with pytest.raises(ClientError):
        helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    def absent(**kwargs):
        raise ClientError({"Error": {"Code": "EntityNotFoundException"}}, "GetCrawler")
    glue.get_crawler = absent
    helper.delete()
    assert helper.evidence["already_absent"] is True
    assert helper.evidence["deleted_at"]
    assert helper.evidence["initialization_error"] == "ClientError"
    assert "cleanup_error" not in helper.evidence
    assert glue.deleted == []


def test_cleanup_does_not_hide_access_denied_as_absent(setup):
    from botocore.exceptions import ClientError
    helper, glue, sqs, logs, root = setup
    helper.initialize("crawler_trial", "trial", timeout_seconds=20)
    def denied(**kwargs):
        raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "EntityNotFoundException"}}, "GetCrawler")
    glue.get_crawler = denied
    with pytest.raises(ClientError):
        helper.delete()
    assert helper.evidence["cleanup_error"] == "ClientError"
    assert "deleted_at" not in helper.evidence
