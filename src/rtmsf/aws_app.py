"""Compose the finalized RTMSF worker with bounded AWS clients."""

from dataclasses import dataclass
import json
import os
import signal
from threading import Event
from typing import Callable, Mapping

import boto3
from botocore.config import Config

from rtmsf.adapters.aws import GluePartitionCatalog, JsonTelemetry, S3ObjectStore, SqsEventSource, _sources
from rtmsf.pmcp import MAX_CALLS_PER_INTENT, PartitionMutationCommitProtocol
from rtmsf.interfaces import ObjectStore, PartitionCatalog, TelemetrySink
from rtmsf.routing import RoutingIndex
from rtmsf.worker import RTMSFWorker


SDK_CONNECT_TIMEOUT_SECONDS = 1
SDK_READ_TIMEOUT_SECONDS = 2
SQS_POLL_READ_TIMEOUT_SECONDS = 22
MAX_PENDING_DELIVERIES = 100
SDK_CALL_BUDGET_MS = (SDK_CONNECT_TIMEOUT_SECONDS + SDK_READ_TIMEOUT_SECONDS) * 1000
# One catalog lookup, up to three HEADs, one prefix LIST, one mutation.
PMCP_BUDGET_MS = MAX_CALLS_PER_INTENT * SDK_CALL_BUDGET_MS
# Renew/ack cover at most 100 deliveries in ten batches; worker terminal calls
# handle one delivery. A receive can send ten full-size poison bodies and one
# ack batch. Its fixed 22-second read timeout also applies to zero-wait polls.
SOURCE_CALL_BUDGET_MS = max(
    ((MAX_PENDING_DELIVERIES + 9) // 10) * SDK_CALL_BUDGET_MS,
    (SDK_CONNECT_TIMEOUT_SECONDS + SQS_POLL_READ_TIMEOUT_SECONDS) * 1000 + 11 * SDK_CALL_BUDGET_MS,
)
LEASE_MARGIN_MS = 5000


@dataclass(frozen=True)
class AppConfig:
    event_queue_url: str
    dead_letter_queue_url: str
    sources: Mapping[str, str]
    table_ids: tuple[str, ...]
    settling_ms: int = 10000
    maximum_ms: int = 31000
    queue_visibility_seconds: int = 180

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] = os.environ) -> "AppConfig":
        required = ("EVENT_QUEUE_URL", "DEAD_LETTER_QUEUE_URL", "RTMSF_SOURCES", "RTMSF_TABLE_IDS")
        missing = [name for name in required if not environ.get(name)]
        if missing:
            raise ValueError("missing configuration: " + ", ".join(missing))
        try:
            sources = json.loads(environ["RTMSF_SOURCES"])
            table_ids = json.loads(environ["RTMSF_TABLE_IDS"])
        except (TypeError, ValueError):
            raise ValueError("RTMSF_SOURCES and RTMSF_TABLE_IDS must contain JSON") from None
        if not isinstance(sources, dict):
            raise ValueError("RTMSF_SOURCES must be a JSON object")
        sources = _sources(sources)
        if not isinstance(table_ids, list) or not table_ids or any(not isinstance(item, str) or item.count(".") != 1 or not all(item.split(".")) for item in table_ids):
            raise ValueError("RTMSF_TABLE_IDS must list database.table identifiers")
        if len(set(table_ids)) != len(table_ids):
            raise ValueError("RTMSF_TABLE_IDS must be unique")
        settling_ms = int(environ.get("SETTLING_MS", "10000"))
        maximum_ms = int(environ.get("MAXIMUM_MS", "31000"))
        lease_seconds = int(environ.get("QUEUE_VISIBILITY_SECONDS", "180"))
        if settling_ms <= 0 or maximum_ms < settling_ms:
            raise ValueError("require 0 < SETTLING_MS <= MAXIMUM_MS")
        lease_reserve_ms = max(PMCP_BUDGET_MS, SOURCE_CALL_BUDGET_MS) + 2 * SOURCE_CALL_BUDGET_MS + LEASE_MARGIN_MS
        if not lease_reserve_ms < lease_seconds * 1000 <= 43_200_000:
            raise ValueError("QUEUE_VISIBILITY_SECONDS cannot cover bounded operations and renewal")
        return cls(
            event_queue_url=environ["EVENT_QUEUE_URL"],
            dead_letter_queue_url=environ["DEAD_LETTER_QUEUE_URL"],
            sources=sources,
            table_ids=tuple(table_ids),
            settling_ms=settling_ms,
            maximum_ms=maximum_ms,
            queue_visibility_seconds=lease_seconds,
        )


def _sdk_config(read_timeout: int = SDK_READ_TIMEOUT_SECONDS) -> Config:
    # Disable SDK retries: the worker owns retries within its monotonic lease.
    return Config(connect_timeout=SDK_CONNECT_TIMEOUT_SECONDS, read_timeout=read_timeout, retries={"mode": "standard", "total_max_attempts": 1})


def build_worker(
    config: AppConfig, *,
    pmcp_factory: Callable[[ObjectStore, PartitionCatalog], object] = PartitionMutationCommitProtocol,
    telemetry: TelemetrySink | None = None,
    pmcp_budget_ms: int = PMCP_BUDGET_MS,
) -> RTMSFWorker:
    telemetry = telemetry if telemetry is not None else JsonTelemetry()
    source = SqsEventSource(
        boto3.client("sqs", config=_sdk_config()),
        config.event_queue_url,
        config.dead_letter_queue_url,
        sources=config.sources,
        telemetry=telemetry,
        polling_client=boto3.client("sqs", config=_sdk_config(SQS_POLL_READ_TIMEOUT_SECONDS)),
    )
    storage_client = boto3.client("s3", config=_sdk_config())
    storage = S3ObjectStore(storage_client, sources=config.sources, telemetry=telemetry)
    catalog = GluePartitionCatalog(boto3.client("glue", config=_sdk_config()), sources=config.sources, telemetry=telemetry)
    tables = catalog.resolve_tables(config.table_ids)
    return RTMSFWorker(
        source=source,
        pmcp=pmcp_factory(storage, catalog),
        telemetry=telemetry,
        routing=RoutingIndex(tables),
        settling_ms=config.settling_ms,
        maximum_ms=config.maximum_ms,
        queue_lease_seconds=config.queue_visibility_seconds,
        pmcp_budget_ms=pmcp_budget_ms,
        source_call_budget_ms=SOURCE_CALL_BUDGET_MS,
        max_pending_deliveries=MAX_PENDING_DELIVERIES,
        lease_safety_margin_ms=LEASE_MARGIN_MS,
    )


def main() -> None:
    stop = Event()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: stop.set())
    build_worker(AppConfig.from_environment()).run_forever(stop.is_set)


if __name__ == "__main__":
    main()
