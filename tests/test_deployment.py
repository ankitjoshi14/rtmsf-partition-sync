from pathlib import Path

import pytest
import yaml


def test_deployment_rejects_wrong_account_and_non_test_stack():
    from experiments.aws.deploy import validate_target

    validate_target("236680608737", "236680608737", "rtmsf-paper-v1", "us-west-2")
    with pytest.raises(ValueError, match="account"):
        validate_target("111111111111", "236680608737", "rtmsf-safe", "us-west-2")
    with pytest.raises(ValueError, match="rtmsf"):
        validate_target("236680608737", "236680608737", "production", "us-west-2")


def test_worker_replacement_requires_quiescence():
    from experiments.aws.deploy import require_stopped

    require_stopped({"desiredCount": 0, "runningCount": 0, "pendingCount": 0})
    for field in ("desiredCount", "runningCount", "pendingCount"):
        with pytest.raises(ValueError, match="stopped"):
            require_stopped({"desiredCount": 0, "runningCount": 0, "pendingCount": 0, field: 1})


def test_image_update_keeps_the_deployed_resource_template(monkeypatch):
    import sys
    from types import SimpleNamespace
    import boto3
    from experiments.aws import deploy

    updates = []
    outputs = {"ClusterName": "cluster", "ServiceName": "worker", "EcrRepositoryUri": "repository"}
    stack = {"StackStatus": "UPDATE_COMPLETE", "Parameters": [
        {"ParameterKey": "ExperimentId", "ParameterValue": "paper-v1"},
        {"ParameterKey": "WorkerImage", "ParameterValue": "old-image"}],
        "Outputs": [{"OutputKey": key, "OutputValue": value} for key, value in outputs.items()]}
    clients = {
        "sts": SimpleNamespace(get_caller_identity=lambda: {"Account": "123456789012"}),
        "cloudformation": SimpleNamespace(describe_stacks=lambda **kw: {"Stacks": [stack]},
                                           update_stack=lambda **kw: updates.append(kw)),
        "ecs": SimpleNamespace(describe_services=lambda **kw: {"services": [
            {"desiredCount": 0, "runningCount": 0, "pendingCount": 0}]}),
    }
    monkeypatch.setattr(boto3, "Session", lambda **kw: SimpleNamespace(client=lambda name, **kw: clients[name]))
    monkeypatch.setattr(deploy, "service_tasks", lambda *args: [])
    monkeypatch.setattr(sys, "argv", ["deploy", "--stack", "rtmsf-paper-v1", "--profile", "fixture",
                                    "--expected-account", "123456789012", "--update-image",
                                    "--image", "repository@sha256:" + "a" * 64])
    deploy.main()
    assert len(updates) == 1
    assert updates[0].get("UsePreviousTemplate") is True
    assert "TemplateBody" not in updates[0]
    assert updates[0]["Parameters"][0] == {"ParameterKey": "ExperimentId", "UsePreviousValue": True}


def test_stopping_task_blocks_replacement_even_when_service_counts_are_zero():
    from experiments.aws.deploy import require_stopped, require_tasks_stopped, service_tasks

    class Ecs:
        def list_tasks(self, **kwargs):
            return {"taskArns": ["old"] if kwargs["desiredStatus"] == "STOPPED" else []}

        def describe_tasks(self, **kwargs):
            return {"tasks": [{"taskArn": "old", "desiredStatus": "STOPPED", "lastStatus": "STOPPING"}]}

    require_stopped({"desiredCount": 0, "runningCount": 0, "pendingCount": 0})
    with pytest.raises(ValueError, match="STOPPED"):
        require_tasks_stopped(service_tasks(Ecs(), "cluster", "service"))
    require_tasks_stopped([{"taskArn": "old", "lastStatus": "STOPPED"}])


def test_template_is_single_worker_and_narrow_fixture_scope():
    class Loader(yaml.SafeLoader):
        pass

    Loader.add_multi_constructor("!", lambda loader, tag, node: loader.construct_scalar(node) if isinstance(node, yaml.ScalarNode) else loader.construct_sequence(node) if isinstance(node, yaml.SequenceNode) else loader.construct_mapping(node))
    document = yaml.load(Path("src/experiments/aws/template.yaml").read_text(), Loader=Loader)
    service = document["Resources"]["WorkerService"]["Properties"]
    assert service["DesiredCount"] == 0
    assert service["DeploymentConfiguration"] == {"MinimumHealthyPercent": 0, "MaximumPercent": 100}
    task = document["Resources"]["WorkerTask"]["Properties"]
    assert task["RuntimePlatform"]["CpuArchitecture"] == "ARM64"
    env = {item["Name"] for item in task["ContainerDefinitions"][0]["Environment"]}
    assert {"SETTLING_MS", "MAXIMUM_MS", "RTMSF_TABLE_IDS", "RTMSF_SOURCES"} <= env
    assert not ({"GRACE_MS", "WINDOW_MS", "W", "G"} & env)
    assert not any(resource["Type"] == "AWS::Glue::Crawler" for resource in document["Resources"].values())


def test_template_scopes_crawler_and_audit_to_separate_trial_buckets():
    class Loader(yaml.SafeLoader):
        pass

    Loader.add_multi_constructor("!", lambda loader, tag, node: loader.construct_scalar(node) if isinstance(node, yaml.ScalarNode) else loader.construct_sequence(node) if isinstance(node, yaml.SequenceNode) else loader.construct_mapping(node))
    document = yaml.load(Path("src/experiments/aws/template.yaml").read_text(), Loader=Loader)
    resources = document["Resources"]
    crawler = resources["CrawlerBucket"]
    assert crawler["DependsOn"] == "CrawlerNotificationPolicy"
    notifications = crawler["Properties"]["NotificationConfiguration"]["QueueConfigurations"]
    assert {item["Event"] for item in notifications} == {"s3:ObjectCreated:*", "s3:ObjectRemoved:*"}
    assert all(item["Filter"]["S3Key"]["Rules"] == [{"Name": "prefix", "Value": "fixture/"}] for item in notifications)
    role = resources["CrawlerRole"]["Properties"]
    assert role["AssumeRolePolicyDocument"]["Statement"][0]["Principal"] == {"Service": "glue.amazonaws.com"}
    statements = role["Policies"][0]["PolicyDocument"]["Statement"]
    sqs = next(item for item in statements if "sqs:ReceiveMessage" in item["Action"])
    assert sqs["Resource"] == "CrawlerEventQueue.Arn"
    assert "sqs:SetQueueAttributes" in sqs["Action"]
    trail = resources["RequestTrail"]["Properties"]
    assert trail["IsLogging"] is True
    assert trail["S3BucketName"] == "AuditBucket"
    assert trail["IsMultiRegionTrail"] is False
    selectors = trail["AdvancedEventSelectors"]
    assert len(selectors) == 1
    for selector in selectors:
        fields = {item["Field"]: item for item in selector["FieldSelectors"]}
        assert fields["eventCategory"]["Equals"] == ["Data"]
        assert fields["resources.type"]["Equals"] == ["AWS::S3::Object"]
        assert fields["readOnly"]["Equals"] == ["true"]
        assert "eventName" not in fields
        assert "resources.ARN" in fields
    object_arns = selectors[0]["FieldSelectors"][-1]["StartsWith"]
    assert object_arns == ["${DataBucket.Arn}/", "${MsckBucket.Arn}/", "${CrawlerBucket.Arn}/"]
    assert resources["RequestTrail"]["DependsOn"] == "AuditBucketPolicy"
    policy = resources["AuditBucketPolicy"]["Properties"]["PolicyDocument"]["Statement"]
    assert all(item["Principal"] == {"Service": "cloudtrail.amazonaws.com"} for item in policy)
    assert all("aws:SourceArn" in item["Condition"]["StringEquals"] for item in policy)
    assert {"CrawlerRoleArn", "CrawlerBucketName", "CrawlerEventQueueUrl", "AuditBucketName", "TrailName"} <= document["Outputs"].keys()
    env = {item["Name"]: item["Value"] for item in resources["WorkerTask"]["Properties"]["ContainerDefinitions"][0]["Environment"]}
    assert env["SETTLING_MS"] == "10000"
