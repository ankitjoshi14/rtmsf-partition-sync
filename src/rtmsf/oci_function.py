"""OCI Functions entry point: durably enqueue the event before returning success."""

import io
import os

import oci
from fdk import response

from rtmsf.adapters.oci import MAX_BODY_BYTES, enqueue_event


def handler(ctx, data: io.BytesIO = None):
    if data is None:
        raise ValueError("missing event body")
    payload = data.read(MAX_BODY_BYTES + 1)
    if len(payload) > MAX_BODY_BYTES:
        raise ValueError("event exceeds configured payload limit")
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.queue.QueueClient(
        {}, signer=signer, service_endpoint=os.environ["RTMSF_QUEUE_ENDPOINT"],
        timeout=(3, 10), retry_strategy=oci.retry.NoneRetryStrategy())
    enqueue_event(client, os.environ["RTMSF_QUEUE_ID"], payload.decode("utf-8"))
    return response.Response(ctx, response_data='{"enqueued":true}',
                             headers={"Content-Type": "application/json"})
