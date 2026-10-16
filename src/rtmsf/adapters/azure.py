"""Decode committed Azure file notifications for the existing RTMSF core."""

from collections.abc import Mapping
from datetime import datetime, timezone
import json
import re
from urllib.parse import unquote, urlsplit

from rtmsf.models import ObjectEvent


MAX_BODY_BYTES = 256 * 1024
OPERATIONS = {
    "Microsoft.Storage.BlobCreated": {"PutBlob", "PutBlockList", "CopyBlob", "FlushWithClose"},
    "Microsoft.Storage.BlobDeleted": {"DeleteBlob", "DeleteFile"},
}


def _sources(sources):
    if not isinstance(sources, Mapping) or not sources:
        raise ValueError("configure at least one logical source")
    targets = set()
    for name, source in sources.items():
        if not isinstance(name, str) or not name or not isinstance(source, Mapping):
            raise ValueError("sources map logical IDs to account, container, and ARM topic")
        if set(source) != {"account", "container", "topic"} or any(not isinstance(value, str) for value in source.values()):
            raise ValueError("source requires account, container, and ARM topic strings")
        account, container, topic = source["account"], source["container"], source["topic"]
        if not re.fullmatch(r"[a-z0-9]{3,24}", account):
            raise ValueError("invalid storage account")
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", container) or "--" in container:
            raise ValueError("invalid storage container")
        if not re.fullmatch(r"/subscriptions/[^/]+/resourceGroups/[^/]+/providers/Microsoft\.Storage/storageAccounts/" + account, topic):
            raise ValueError("source topic must identify the configured storage account")
        target = account, container
        if target in targets:
            raise ValueError("each source must identify a distinct account/container")
        targets.add(target)
    return sources


def decode_event(body, sources):
    configured = _sources(sources)
    if not isinstance(body, str) or len(body.encode("utf-8")) > MAX_BODY_BYTES:
        raise ValueError("invalid or oversized event body")
    document = json.loads(body)
    if not isinstance(document, dict) or not isinstance(document.get("data"), dict):
        raise ValueError("require one Event Grid event object")
    if not isinstance(document.get("id"), str) or not document["id"]:
        raise ValueError("missing event ID")
    data = document["data"]
    url = data.get("url")
    if not isinstance(url, str) or url.startswith(" ") or any(ord(char) < 32 or ord(char) == 127 for char in url):
        raise ValueError("invalid object URL")
    parsed = urlsplit(url)
    host = re.fullmatch(r"([a-z0-9]{3,24})\.(?:blob|dfs)\.core\.windows\.net", parsed.netloc)
    if parsed.scheme != "https" or not host or parsed.query or parsed.fragment or not parsed.path.startswith("/"):
        raise ValueError("require a primary Azure Storage HTTPS URL")
    container, separator, encoded_key = parsed.path[1:].partition("/")
    if not separator or re.search(r"%(?![0-9a-fA-F]{2})", encoded_key):
        raise ValueError("missing key or invalid percent escape")
    key = unquote(encoded_key, encoding="utf-8", errors="strict")
    key.encode("utf-8", errors="strict")
    if not key or key.endswith("/") or any(part in {"", ".", ".."} for part in key.split("/")) or any(ord(char) < 32 or ord(char) == 127 for char in key):
        raise ValueError("require an exact file key without path normalization")
    source_id = next((name for name, source in configured.items() if
                      source["account"] == host[1] and source["container"] == container
                      and source["topic"] == document.get("topic")), None)
    if source_id is None:
        raise ValueError("unconfigured topic, account, or container")
    # shortcut: require raw subject equality until live special-name events verify escaping.
    if document.get("subject") != f"/blobServices/default/containers/{container}/blobs/{key}":
        raise ValueError("subject and exact URL key disagree")
    event_type, api = document.get("eventType"), data.get("api")
    if not isinstance(event_type, str) or not isinstance(api, str):
        raise ValueError("missing event type or storage API")
    if event_type == "Microsoft.Storage.BlobCreated" and api == "CreateFile":
        return ()
    if event_type not in OPERATIONS or api not in OPERATIONS[event_type] or data.get("blobType") != "BlockBlob":
        raise ValueError("unsupported or uncommitted file operation")
    source_time = None
    value = document.get("eventTime")
    if isinstance(value, str):
        try:
            parsed_time = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed_time.utcoffset() is not None:
                source_time = parsed_time.astimezone(timezone.utc)
        except (ValueError, OverflowError):
            pass
    operation = "CREATE" if event_type == "Microsoft.Storage.BlobCreated" else "DELETE"
    return (ObjectEvent(source_id, key, operation, datetime.now(timezone.utc), source_time),)
