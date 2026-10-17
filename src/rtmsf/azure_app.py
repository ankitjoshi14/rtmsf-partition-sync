"""Compose one approved Azure table with the unchanged RTMSF core.

SDK timeouts limit phases; elapsed operation checks detect overruns on return.
"""

from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import sys
from threading import Event
import time
from typing import Mapping
from urllib.parse import quote

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

from rtmsf.adapters.azure import AzureObjectStore, AzureServiceBusEventSource, _sources
from rtmsf.adapters.databricks import DatabricksPartitionCatalog, DatabricksSQLStatements, _json, _text
from rtmsf.pmcp import PartitionMutationCommitProtocol
from rtmsf.routing import RoutingIndex
from rtmsf.worker import RTMSFWorker


LOCK_PATH = Path("/var/lib/rtmsf/worker.lock")
JOURNAL_PATH = Path("/var/lib/rtmsf/databricks-pending.json")
PMCP_BUDGET_MS = 110_000
SOURCE_CALL_BUDGET_MS = 80_000
# Broker locks are 300 seconds; leave 20 seconds before the worker lease expires.
QUEUE_LEASE_SECONDS = 280
LEASE_MARGIN_MS = 1000


@dataclass(frozen=True)
class AppConfig:
    sources: Mapping[str, Mapping[str, str]]
    table_id: str
    root_prefix: str
    service_bus_namespace: str
    service_bus_queue: str
    key_vault_url: str
    databricks_host: str
    databricks_warehouse_id: str
    databricks_client_id: str
    settling_ms: int = 10000
    maximum_ms: int = 31000

    def __post_init__(self):
        try:
            sources = _sources(self.sources)
            if len(sources) != 1:
                raise ValueError("require_one_source")
            _text(next(iter(sources)))
            identity = _text(self.table_id).split(".")
            if len(identity) != 3:
                raise ValueError("invalid_table_id")
            for part in identity:
                _text(part)
            prefix = _text(self.root_prefix)
            if not prefix.endswith("/") or any(part in {"", ".", ".."} for part in prefix[:-1].split("/")):
                raise ValueError("invalid_root_prefix")
            patterns = (
                (self.service_bus_namespace, r"[a-z0-9][a-z0-9-]{4,48}[a-z0-9]\.servicebus\.windows\.net"),
                (self.service_bus_queue, r"[A-Za-z0-9][A-Za-z0-9_-]{0,259}"),
                (self.key_vault_url, r"https://[a-z][a-z0-9-]{1,22}[a-z0-9]\.vault\.azure\.net"),
                (self.databricks_host, r"https://adb-[0-9]+\.[0-9]+\.azuredatabricks\.net"),
            )
            if any(not re.fullmatch(pattern, _text(value)) for value, pattern in patterns):
                raise ValueError("invalid_trusted_endpoint")
            _text(self.databricks_warehouse_id)
            _text(self.databricks_client_id)
            if (type(self.settling_ms) is not int or type(self.maximum_ms) is not int
                or not 0 < self.settling_ms <= self.maximum_ms):
                raise ValueError("invalid_settling_window")
        except (TypeError, ValueError, UnicodeError):
            raise ValueError("invalid_azure_configuration") from None
        object.__setattr__(self, "sources", {name: dict(source) for name, source in sources.items()})

    @property
    def root_location(self):
        source = next(iter(self.sources.values()))
        return f"abfss://{source['container']}@{source['account']}.dfs.core.windows.net/{quote(self.root_prefix, safe='/=')}"

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] = os.environ):
        try:
            return cls(
                sources=_json(environ["RTMSF_SOURCES"]), table_id=environ["RTMSF_TABLE_ID"],
                root_prefix=environ["RTMSF_ROOT_PREFIX"],
                service_bus_namespace=environ["AZURE_SERVICE_BUS_NAMESPACE"],
                service_bus_queue=environ["AZURE_SERVICE_BUS_QUEUE"],
                key_vault_url=environ["AZURE_KEY_VAULT_URL"], databricks_host=environ["DATABRICKS_HOST"],
                databricks_warehouse_id=environ["DATABRICKS_WAREHOUSE_ID"],
                databricks_client_id=environ["DATABRICKS_CLIENT_ID"],
            )
        except (KeyError, TypeError, ValueError, UnicodeError):
            raise ValueError("invalid_azure_configuration") from None


class JsonTelemetry:
    def emit(self, record):
        print(json.dumps(dict(record), separators=(",", ":")), flush=True)


class _BudgetedPMCP:
    def __init__(self, pmcp, statements):
        self._pmcp, self._statements = pmcp, statements

    def apply(self, intent):
        # Storage elapsed time must also be checked before the core can settle.
        with self._statements.operation():
            return self._pmcp.apply(intent)


@contextmanager
def build_worker(config: AppConfig, *, telemetry=None):
    telemetry = telemetry if telemetry is not None else JsonTelemetry()
    startup_deadline = time.monotonic() + PMCP_BUDGET_MS / 1000
    with ExitStack() as stack:
        credential = stack.enter_context(ManagedIdentityCredential(
            retry_total=0, connection_timeout=5, read_timeout=5, logging_enable=False,
        ))
        vault = stack.enter_context(SecretClient(
            vault_url=config.key_vault_url, credential=credential, retry_total=0,
            connection_timeout=5, read_timeout=5, logging_enable=False,
        ))
        retrieved = vault.get_secret("databricks-client-secret")
        try:
            secret = _text(retrieved.value)
        except (AttributeError, TypeError, ValueError, UnicodeError):
            raise ValueError("invalid_databricks_secret") from None
        statements = DatabricksSQLStatements(
            config.databricks_host, config.databricks_warehouse_id, config.databricks_client_id, secret,
            journal_path=JOURNAL_PATH, operation_seconds=PMCP_BUDGET_MS / 1000,
        )
        source_id, source_config = next(iter(config.sources.items()))
        catalog = DatabricksPartitionCatalog(
            statements, table_id=config.table_id, source_id=source_id,
            root_prefix=config.root_prefix, root_location=config.root_location,
        )
        with statements.operation(deadline=startup_deadline):
            tables = catalog.resolve_tables((config.table_id,))
        blob = stack.enter_context(BlobServiceClient(
            account_url=f"https://{source_config['account']}.blob.core.windows.net", credential=credential,
            retry_total=0, retry_to_secondary=False, connection_timeout=5, read_timeout=5, logging_enable=False,
        ))
        datalake = stack.enter_context(DataLakeServiceClient(
            account_url=f"https://{source_config['account']}.dfs.core.windows.net", credential=credential,
            retry_total=0, retry_to_secondary=False, connection_timeout=5, read_timeout=5, logging_enable=False,
        ))
        storage = AzureObjectStore(
            {source_id: blob.get_container_client(container=source_config["container"])},
            {source_id: datalake.get_file_system_client(file_system=source_config["container"])},
            config.sources, telemetry,
        )
        servicebus = stack.enter_context(ServiceBusClient(
            fully_qualified_namespace=config.service_bus_namespace, credential=credential,
            retry_total=0, try_timeout=10, uamqp_transport=False, logging_enable=False,
        ))
        receiver = stack.enter_context(servicebus.get_queue_receiver(
            queue_name=config.service_bus_queue, receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
            prefetch_count=0, socket_timeout=5,
        ))
        yield RTMSFWorker(
            source=AzureServiceBusEventSource(receiver, config.sources, telemetry),
            pmcp=_BudgetedPMCP(PartitionMutationCommitProtocol(storage, catalog), statements),
            telemetry=telemetry, routing=RoutingIndex(tables),
            settling_ms=config.settling_ms, maximum_ms=config.maximum_ms,
            queue_lease_seconds=QUEUE_LEASE_SECONDS, pmcp_budget_ms=PMCP_BUDGET_MS,
            source_call_budget_ms=SOURCE_CALL_BUDGET_MS, lease_safety_margin_ms=LEASE_MARGIN_MS,
            poll_size=1, max_pending_deliveries=4,
        )


@contextmanager
def _host_lock(path):
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("azure_worker_already_running") from None
        yield
    finally:
        os.close(descriptor)


def main():
    try:
        stop = Event()
        signal.signal(signal.SIGTERM, lambda _signum, _frame: stop.set())
        signal.signal(signal.SIGINT, lambda _signum, _frame: stop.set())
        with _host_lock(LOCK_PATH):
            with build_worker(AppConfig.from_environment()) as worker:
                worker.run_forever(stop.is_set)
        return 0
    except Exception:
        print("azure_worker_failed", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
