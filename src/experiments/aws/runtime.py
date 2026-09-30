"""Shared AWS worker control, queries and runtime observation for experiments."""

import json
import time
import uuid

from experiments.aws.deploy import require_stopped, require_tasks_stopped, service_tasks
from experiments.evidence import EvidenceBundle, utc


def runtime_operations(telemetry: list[dict]) -> tuple[list[dict], dict]:
    """Retain adapter calls and run-wide totals, separate from graph measurements."""
    operations, counts, seen = [], {}, set()
    for item in telemetry:
        event_id = item.get("cloudwatch_event_id")
        if event_id and event_id in seen:
            continue
        if event_id:
            seen.add(event_id)
        record = item["record"]
        if not isinstance(record, dict) or record.get("component") != "aws_adapter" or record.get("action") != "api_call":
            continue
        operation = {**record, "cloudwatch_event_id": event_id, "log_stream": item.get("log_stream")}
        operations.append(operation)
        counts[record["operation"]] = counts.get(record["operation"], 0) + int(record.get("attempts", 1))
    return operations, counts

def worker_counters(telemetry: list[dict]) -> dict:
    result = {"routed_observations": 0, "pmcp_attempts": 0, "terminal_partition_windows": 0, "successful_mutations": 0, "acknowledged_decoded_events": 0}
    seen = set()
    for item in telemetry:
        event_id, record = item.get("cloudwatch_event_id"), item["record"]
        if event_id and event_id in seen:
            continue
        if event_id:
            seen.add(event_id)
        if not isinstance(record, dict) or record.get("component") != "worker":
            continue
        if record.get("action") == "delivery_buffered":
            result["routed_observations"] += record["event_count"]
        elif record.get("action") == "pmcp_result":
            result["pmcp_attempts"] += 1
            result["terminal_partition_windows"] += record["status"] != "RETRY"
            result["successful_mutations"] += record["status"] == "APPLIED"
        elif record.get("action") == "delivery_acknowledged":
            result["acknowledged_decoded_events"] += record["event_count"]
    return {"scope": "entire run, including fixture startup and correctness/recovery; not steady-state-only", **result}


def assert_empty_dlq(attributes: dict) -> None:
    if any(int(value) != 0 for value in attributes.values()):
        raise AssertionError("dead-letter queue contains messages; this run is not a clean success")


def assert_crash_trace(stopped: list[dict], running: list[dict]) -> None:
    crashed = [task for task in stopped if any(item.get("name") == "worker" and item.get("exitCode") == 86 for item in task.get("containers", []))]
    if len(crashed) != 1 or len(running) != 1:
        raise AssertionError("replay evidence requires one exit86 task and one running successor")
    if crashed[0]["taskArn"] == running[0]["taskArn"] or crashed[0]["stoppedAt"] > running[0]["startedAt"]:
        raise AssertionError("crashed task must stop before its successor starts")


def parquet_payload(record_id: int) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    output = pa.BufferOutputStream()
    pq.write_table(pa.table({"record_id": pa.array([record_id], type=pa.int64()), "amount": pa.array([record_id * 10], type=pa.int64())}), output, compression="snappy")
    return output.getvalue().to_pybytes()


class AwsRuntime:
    """Own AWS clients and the operations shared by experiment workloads."""

    def __init__(self, session, outputs: dict, bundle: EvidenceBundle, run_id: str, timeout: int):
        from botocore.config import Config

        self.outputs, self.bundle = outputs, bundle
        self.run_id, self.timeout = run_id, timeout
        self.start_ms = int(time.time() * 1000)
        config = Config(connect_timeout=3, read_timeout=15, retries={"total_max_attempts": 1})
        self.clients = {name: session.client(name, config=config) for name in ("s3", "sqs", "glue", "athena", "ecs", "logs")}
        self.lifecycle = []
        self.queries = []
        self.task_definition = None

    def record(self, event, **fields):
        value = {'at': utc(), 'event': event, **fields}
        with (self.bundle.root / 'experiment.jsonl').open('a') as stream:
            stream.write(json.dumps(value, default=str) + '\n')
        return value

    def service(self) -> dict:
        response = self.clients["ecs"].describe_services(cluster=self.outputs["ClusterName"], services=[self.outputs["ServiceName"]])
        if response.get("failures") or len(response.get("services", [])) != 1:
            raise RuntimeError(f"cannot resolve experiment service: {response.get('failures')}")
        return response["services"][0]

    def wait_worker(self, running: bool, *, configured: bool = False) -> dict:
        deadline = time.monotonic() + self.timeout
        last = None
        while time.monotonic() < deadline:
            last = self.service()
            configuration_ready = not configured or (
                last["taskDefinition"] == self.task_definition and len(last["deployments"]) == 1
            )
            if configuration_ready and last["pendingCount"] == 0 and last["runningCount"] == int(running):
                return last
            time.sleep(2)
        raise TimeoutError(f"worker did not reach running={running}: {last}")

    def require_worker_stopped(self) -> None:
        require_stopped(self.service())
        require_tasks_stopped(service_tasks(self.clients["ecs"], self.outputs["ClusterName"], self.outputs["ServiceName"]))

    def start_worker(self) -> None:
        self.require_worker_stopped()
        self.clients["ecs"].update_service(cluster=self.outputs["ClusterName"], service=self.outputs["ServiceName"], desiredCount=1)
        self.wait_worker(True)
        listed = self.clients["ecs"].list_tasks(cluster=self.outputs["ClusterName"], serviceName=self.outputs["ServiceName"], desiredStatus="RUNNING")["taskArns"]
        if len(listed) != 1:
            raise AssertionError("experiment requires exactly one running worker")
        tasks = self.clients["ecs"].describe_tasks(cluster=self.outputs["ClusterName"], tasks=listed)["tasks"]
        if len(tasks) != 1 or tasks[0]["taskDefinitionArn"] != self.task_definition:
            raise AssertionError("running worker does not use the configured task definition")
        self.lifecycle.append({"event": "worker_started", "at": utc(), "tasks": tasks})
        self.bundle.write(f"runtime-start-{len(self.lifecycle)}.json", tasks)

    def stop_worker(self) -> None:
        known = {task["taskArn"] for task in service_tasks(self.clients["ecs"], self.outputs["ClusterName"], self.outputs["ServiceName"])}
        self.clients["ecs"].update_service(cluster=self.outputs["ClusterName"], service=self.outputs["ServiceName"], desiredCount=0)
        stopped = self.wait_worker(False)
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            tasks = service_tasks(self.clients["ecs"], self.outputs["ClusterName"], self.outputs["ServiceName"], known)
            known.update(task["taskArn"] for task in tasks)
            try:
                require_tasks_stopped(tasks)
            except ValueError:
                time.sleep(2)
                continue
            self.lifecycle.append({"event": "worker_stopped", "at": utc(), "service": stopped, "verified_stopped_tasks": tasks})
            return
        raise TimeoutError("previous worker process did not reach task STOPPED before shutdown deadline")

    def query(self, sql: str, label: str) -> tuple[list[list[str]], dict]:
        athena = self.clients["athena"]
        started = utc()
        started_mono = time.monotonic()
        self.last_query_start = {"started_at": started, "started_monotonic_seconds": started_mono}
        query_id = athena.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": self.outputs["GlueDatabaseName"]}, WorkGroup=self.outputs["AthenaWorkGroupName"], ResultConfiguration={"OutputLocation": f"s3://{self.outputs['AthenaResultsBucketName']}/athena/{self.run_id}/"}, ClientRequestToken=uuid.uuid4().hex)["QueryExecutionId"]
        execution = None
        try:
            deadline = time.monotonic() + self.timeout
            while time.monotonic() < deadline:
                execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
                state = execution["Status"]["State"]
                if state == "SUCCEEDED":
                    break
                if state in {"FAILED", "CANCELLED"}:
                    raise RuntimeError(f"Athena {label} {state}: {execution['Status'].get('StateChangeReason')}")
                time.sleep(1)
            else:
                athena.stop_query_execution(QueryExecutionId=query_id)
                raise TimeoutError(f"Athena {query_id} timed out")
            rows, token = [], None
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Athena {query_id} result pagination timed out")
                request = {"QueryExecutionId": query_id, "MaxResults": 1000}
                if token:
                    request["NextToken"] = token
                result = athena.get_query_results(**request)
                rows.extend([[field.get("VarCharValue", "") for field in row["Data"]] for row in result["ResultSet"]["Rows"]])
                token = result.get("NextToken")
                if not token:
                    break
            record = {"query_id": query_id, "sql": sql, "started_at": started, "started_monotonic_seconds": started_mono, "completed_at": utc(), "elapsed_seconds": time.monotonic() - started_mono, "execution": execution, "rows": rows}
            self.bundle.write(f"queries/{label}.json", record)
            self.queries.append(record)
            return rows, record
        except Exception as error:
            self.bundle.write(f"queries/{label}-failed.json", {"query_id": query_id, "sql": sql, "execution": execution, "error": str(error), "started_at": started, "failed_at": utc()})
            raise

    def drain(self, label: str) -> None:
        deadline, empty_since = time.monotonic() + self.timeout, None
        observations = []
        while time.monotonic() < deadline:
            response = self.clients["sqs"].get_queue_attributes(QueueUrl=self.outputs["EventQueueUrl"], AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"])["Attributes"]
            observations.append({"at": utc(), "attributes": response})
            now = time.monotonic()
            if all(int(value) == 0 for value in response.values()):
                empty_since = now if empty_since is None else empty_since
                if now - empty_since >= 8:
                    self.bundle.write(f"queue/{label}.json", {"observations": observations, "caveat": "SQS attributes are approximate; no proof of permanent notification completeness"})
                    return
            else:
                empty_since = None
            time.sleep(2)
        self.bundle.write(f"queue/{label}-timeout.json", observations)
        raise TimeoutError("queue did not remain observably empty for eight seconds")

    def _read_worker_logs(self) -> tuple[list[dict], list[dict]]:
        """Fetch one snapshot with raw events and parsed telemetry for both callers."""
        from botocore.exceptions import ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError

        logs, token = [], None
        for _ in range(100):
            request = {"logGroupName": self.outputs["LogGroupName"], "startTime": self.start_ms, "limit": 10000}
            if token:
                request["nextToken"] = token
            for attempt in range(3):
                try:
                    response = self.clients["logs"].filter_log_events(**request)
                    break
                except (ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError) as error:
                    if attempt == 2:
                        raise
                    self.record('telemetry_observation_retry', error=type(error).__name__, attempt=attempt + 1)
                    time.sleep(1)
            logs.extend(response.get("events", []))
            next_token = response.get("nextToken")
            if not next_token or next_token == token:
                break
            token = next_token
        else:
            raise RuntimeError("bounded log collection pagination exhausted")
        telemetry = []
        for entry in logs:
            try:
                parsed = json.loads(entry["message"])
            except (json.JSONDecodeError, TypeError):
                continue
            telemetry.append({"cloudwatch_event_id": entry.get("eventId"), "log_stream": entry.get("logStreamName"), "record": parsed})
        return logs, telemetry

    def telemetry(self) -> list[dict]:
        return self._read_worker_logs()[1]

    def collect_runtime(self) -> None:
        logs, telemetry = self._read_worker_logs()
        self.bundle.write("runtime-logs.json", logs)
        self.bundle.write("app-telemetry.json", telemetry)
        operations, counts = runtime_operations(telemetry)
        self.bundle.write("runtime-api-operations.json", operations)
        self.bundle.write("runtime-api-counts.json", counts)
        self.bundle.write("worker-counters.json", worker_counters(telemetry))
        dlq = self.clients["sqs"].get_queue_attributes(QueueUrl=self.outputs["DeadLetterQueueUrl"], AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        self.bundle.write("dead-letter-queue.json", dlq)
        assert_empty_dlq(dlq)
