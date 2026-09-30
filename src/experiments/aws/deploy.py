"""Create a new test stack, or update its image only while the worker is stopped."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import time


def validate_target(account: str, expected_account: str, stack: str, region: str) -> None:
    if account != expected_account or not re.fullmatch(r"\d{12}", expected_account):
        raise ValueError("AWS account does not match the explicit expected account")
    if not re.fullmatch(r"rtmsf-[a-z0-9-]{3,90}", stack):
        raise ValueError("stack must have an rtmsf- name")
    if region != "us-west-2":
        raise ValueError("this bounded experiment is restricted to us-west-2")


def require_stopped(service: dict) -> None:
    if any(service.get(field, 0) for field in ("desiredCount", "runningCount", "pendingCount")):
        raise ValueError("worker must be explicitly stopped before replacement")


def require_tasks_stopped(tasks: list[dict]) -> None:
    if any(task.get("lastStatus") != "STOPPED" for task in tasks):
        raise ValueError("every previous worker task must reach lastStatus=STOPPED before replacement")


def service_tasks(ecs, cluster: str, service: str, known_arns=()) -> list[dict]:
    arns = set(known_arns)
    # Desired STOPPED includes tasks whose process is still STOPPING.
    for desired in ("RUNNING", "STOPPED"):
        token = None
        for _ in range(10):
            request = {"cluster": cluster, "serviceName": service, "desiredStatus": desired, "maxResults": 100}
            if token:
                request["nextToken"] = token
            page = ecs.list_tasks(**request)
            arns.update(page["taskArns"])
            token = page.get("nextToken")
            if not token:
                break
        else:
            raise RuntimeError("bounded worker-task pagination exhausted")
    result = []
    ordered = sorted(arns)
    for offset in range(0, len(ordered), 100):
        response = ecs.describe_tasks(cluster=cluster, tasks=ordered[offset:offset + 100])
        if response.get("failures"):
            raise RuntimeError("cannot verify previous worker task state")
        result.extend(response["tasks"])
    if {task["taskArn"] for task in result} != arns:
        raise RuntimeError("worker task state response is incomplete")
    return result


def main() -> None:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", required=True)
    parser.add_argument("--expected-account", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--experiment-id", default="paper-v1")
    parser.add_argument("--database", default="rtmsf_experiments")
    parser.add_argument("--vpc")
    parser.add_argument("--subnets", nargs="+")
    parser.add_argument("--image", help="immutable ECR repository@sha256 digest; omit for stopped bootstrap")
    parser.add_argument("--update-image", action="store_true")
    args = parser.parse_args()
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    config = Config(connect_timeout=3, read_timeout=15, retries={"total_max_attempts": 2})
    sts = session.client("sts", config=config)
    validate_target(sts.get_caller_identity()["Account"], args.expected_account, args.stack, args.region)
    cfn = session.client("cloudformation", config=config)
    try:
        existing = cfn.describe_stacks(StackName=args.stack)["Stacks"][0]
    except ClientError as error:
        if "does not exist" not in str(error):
            raise
        existing = None
    if existing:
        if not args.update_image:
            raise ValueError("stack already exists; use --update-image after explicitly stopping its worker")
        outputs = {item["OutputKey"]: item["OutputValue"] for item in existing["Outputs"]}
        ecs = session.client("ecs", config=config)
        require_stopped(ecs.describe_services(cluster=outputs["ClusterName"], services=[outputs["ServiceName"]])["services"][0])
        require_tasks_stopped(service_tasks(ecs, outputs["ClusterName"], outputs["ServiceName"]))
        if not args.image or not args.image.startswith(outputs["EcrRepositoryUri"] + "@sha256:"):
            raise ValueError("image must use this stack's immutable ECR digest")
        parameters = [{"ParameterKey": item["ParameterKey"], "UsePreviousValue": True} for item in existing["Parameters"] if item["ParameterKey"] != "WorkerImage"]
        parameters.append({"ParameterKey": "WorkerImage", "ParameterValue": args.image})
        cfn.update_stack(StackName=args.stack, UsePreviousTemplate=True, Parameters=parameters, Capabilities=["CAPABILITY_IAM"])
    else:
        if args.update_image or not args.vpc or not args.subnets:
            raise ValueError("new stack needs --vpc and --subnets")
        parameters = {"ExperimentId": args.experiment_id, "DatabaseName": args.database, "VpcId": args.vpc, "SubnetIds": ",".join(args.subnets)}
        if args.image:
            parameters["WorkerImage"] = args.image
        cfn.create_stack(StackName=args.stack, TemplateBody=Path(__file__).with_name("template.yaml").read_text(), Parameters=[{"ParameterKey": key, "ParameterValue": value} for key, value in parameters.items()], Capabilities=["CAPABILITY_IAM"], Tags=[{"Key": "Purpose", "Value": "RTMSF bounded experiment"}])
    deadline = time.monotonic() + 1200
    while time.monotonic() < deadline:
        current = cfn.describe_stacks(StackName=args.stack)["Stacks"][0]
        status = current["StackStatus"]
        if status in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
            print(json.dumps({item["OutputKey"]: item["OutputValue"] for item in current["Outputs"]}, indent=2))
            return
        if "FAILED" in status or "ROLLBACK" in status:
            raise RuntimeError(f"stack failed: {status}; inspect retained CloudFormation events")
        time.sleep(5)
    raise TimeoutError("stack did not finish within 20 minutes")


if __name__ == "__main__":
    main()
