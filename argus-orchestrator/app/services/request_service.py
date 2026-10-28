import logging
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional
from pymongo import ReturnDocument
from pydantic import ValidationError

from app.core.config import settings
from app.platform.mongodb import rss_requests
from app.platform.redis import redis_client
from app.schemas.request import ConnectorRequest

logger = logging.getLogger(__name__)


_IDEMPOTENT_XADD_SCRIPT = """
local existing_id = redis.call("GET", KEYS[2])

if existing_id then
    local existing_message = redis.call(
        "XRANGE",
        KEYS[1],
        existing_id,
        existing_id,
        "COUNT",
        1
    )

    if #existing_message > 0 then
        return existing_id
    end
end

local message_id = redis.call(
    "XADD",
    KEYS[1],
    "*",
    unpack(ARGV)
)

redis.call("SET", KEYS[2], message_id)

return message_id
"""


async def _publish_request_idempotently(
    request_id: str,
    payload: dict,
) -> str:
    """
    Publish a request once per request_id.

    A retry returns the original Redis stream message ID,
    provided the deduplication record still exists.
    """
    if not request_id.strip():
        raise ValueError("request_id must not be empty")

    dedupe_key = f"argus:dispatch:request:{request_id}:stream_message_id"

    # Redis stream fields are passed as alternating field/value arguments.
    flat_fields = []
    for field, value in payload.items():
        flat_fields.extend((field, str(value)))

    message_id = await redis_client.eval(
        _IDEMPOTENT_XADD_SCRIPT,
        2,
        settings.connector_stream,
        dedupe_key,
        *flat_fields,
    )

    if not message_id:
        raise RuntimeError(
            f"Redis did not return a message ID for request {request_id}"
        )

    return str(message_id)

async def _get_existing_stream_message_id(request_id: str) -> Optional[str]:
        """Return the existing Redis message ID only if its stream entry exists."""
        dedupe_key = f"argus:dispatch:request:{request_id}:stream_message_id"

        message_id = await redis_client.get(dedupe_key)
        if not message_id:
            return None

        entries = await redis_client.xrange(
            settings.connector_stream,
            min=message_id,
            max=message_id,
            count=1,
        )

        if not entries:
            return None

        return str(message_id)


async def dispatch_request() -> Optional[ConnectorRequest]:
    """Claim a pending request, validate it, publish it to Redis,
    and record the dispatch details in MongoDB.
    """

    # 1. Claim one pending request atomically.
    now = datetime.now(timezone.utc)

    claimed_doc = await rss_requests.find_one_and_update(
        {"status": "pending"},
        {
            "$set": {
                "status": "processing",
                "claimed_at": now,
                "updated_at": now,
            }
        },
        return_document=ReturnDocument.AFTER,
    )

    if not claimed_doc:
        logger.debug("No pending RSS requests found.")
        return None

    request_id = claimed_doc.get("request_id") or str(claimed_doc["_id"])

    # 2. Validate the request.
    try:
        request_obj = ConnectorRequest(
            request_id=request_id,
            source=claimed_doc["source"],
            source_type=claimed_doc["source_type"],
            source_url=claimed_doc["url"],
            metadata=claimed_doc.get("metadata", {}),
        )
    except (ValidationError, KeyError) as exc:
        logger.exception("Request %s failed validation.", request_id)

        await rss_requests.update_one(
            {"_id": claimed_doc["_id"]},
            {
                "$set": {
                    "status": "failed",
                    "error_message": f"Validation error: {exc}",
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        raise

    # 3. Publish to Redis.
    redis_payload = request_obj.model_dump()
    redis_payload["metadata"] = json.dumps(redis_payload["metadata"])


    try:
        stream_message_id = await _publish_request_idempotently(
            request_id,
            redis_payload,
        )
    except Exception as exc:
        logger.exception(
            "Redis publication failed for request %s.",
            request_id,
        )
        await rss_requests.update_one(
            {"_id": claimed_doc["_id"]},
            {
                "$set": {
                    "status": "pending",
                    "error_message": f"Redis dispatch error: {exc}",
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        raise

    # This block must be outside the Redis except block.
    dispatch_time = datetime.now(timezone.utc)

    try:
        await rss_requests.update_one(
            {"_id": claimed_doc["_id"]},
            {
                "$set": {
                    "stream_message_id": stream_message_id,
                    "dispatched_at": dispatch_time,
                    "updated_at": datetime.now(timezone.utc),
                    "error_message": None,
                }
            },
        )
    except Exception:
        logger.exception(
            "Redis accepted request %s (message ID %s), "
            "but MongoDB audit update failed.",
            request_id,
            stream_message_id,
        )
        raise

    logger.info(
        "Dispatched request %s to %s (message ID: %s)",
        request_id,
        settings.connector_stream,
        stream_message_id,
    )

    return request_obj




async def recover_stale_processing_requests(
    stale_after_seconds: Optional[int] = None,
) -> int:
    """Recover one stale request by reconciling its Redis stream entry."""

    timeout_seconds = (
        stale_after_seconds
        if stale_after_seconds is not None
        else settings.request_recovery_timeout_seconds
    )
    if timeout_seconds <= 0:
        raise ValueError("Recovery timeout must be greater than zero.")

    lease_seconds = settings.request_recovery_lease_seconds
    if lease_seconds <= 0:
        raise ValueError("Recovery lease must be greater than zero.")

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=timeout_seconds)
    lease_cutoff = now - timedelta(seconds=lease_seconds)
    recovery_token = str(uuid.uuid4())

    stale_request = await rss_requests.find_one_and_update(
        {
            "status": "processing",
            "claimed_at": {"$lte": cutoff},
            "dispatched_at": None,
            "$or": [
                {"recovery_claimed_at": {"$exists": False}},
                {"recovery_claimed_at": None},
                {"recovery_claimed_at": {"$lte": lease_cutoff}},
            ],
        },
        {
            "$set": {
                "recovery_claimed_at": now,
                "recovery_token": recovery_token,
                "updated_at": now,
            }
        },
        return_document=ReturnDocument.AFTER,
    )

    if not stale_request:
        logger.debug("No eligible stale request found for recovery.")
        return 0

    request_id = stale_request.get("request_id")

    try:

        if not request_id:
            logger.error(
                "Stale MongoDB request %s has no request_id.",
                stale_request.get("_id"),
            )

            await rss_requests.update_one(
                {
                    "_id": stale_request["_id"],
                    "status": "processing",
                    "recovery_token": recovery_token,
                },
                {
                    "$unset": {
                        "recovery_claimed_at": "",
                        "recovery_token": "",
                    },
                    "$set": {
                        "updated_at": datetime.now(timezone.utc),
                    },
                },
            )
            return 0


        # Reuse the message if it is still present in the Redis stream.
        message_id = await _get_existing_stream_message_id(request_id)

        if not message_id:
            connector_request = ConnectorRequest(
                request_id=request_id,
                source=stale_request["source"],
                source_type=stale_request["source_type"],
                source_url=stale_request["url"],
                metadata=stale_request.get("metadata") or {},
            )

            payload = connector_request.model_dump()
            payload["metadata"] = json.dumps(payload["metadata"])

            # Idempotent publishing prevents duplicate stream entries
            # when the request's dedupe key and stream entry still exist.
            message_id = await _publish_request_idempotently(
                request_id,
                payload,
            )

        recovered_at = datetime.now(timezone.utc)

        update_result = await rss_requests.update_one(
            {
                "_id": stale_request["_id"],
                "status": "processing",
                "recovery_token": recovery_token,
                "dispatched_at": None,
            },
            {
                "$set": {
                    "stream_message_id": message_id,
                    "dispatched_at": recovered_at,
                    "updated_at": recovered_at,
                    "error_message": None,
                },
                "$unset": {
                    "recovery_claimed_at": "",
                    "recovery_token": "",
                },
            },
        )

        if update_result.modified_count == 0:
            logger.warning(
                "Redis reconciliation completed for request %s, "
                "but MongoDB was not updated under the recovery claim.",
                request_id,
            )
            return 0

        logger.info(
            "Recovered request %s with Redis message %s.",
            request_id,
            message_id,
        )
        return 1

    except Exception:
        logger.exception(
            "Failed to recover stale request %s.",
            request_id or stale_request.get("_id"),
        )

        try:
            await rss_requests.update_one(
                {
                    "_id": stale_request["_id"],
                    "status": "processing",
                    "recovery_token": recovery_token,
                },
                {
                    "$unset": {
                        "recovery_claimed_at": "",
                        "recovery_token": "",
                    },
                    "$set": {
                        "updated_at": datetime.now(timezone.utc),
                    },
                },
            )
        except Exception:
            logger.exception(
                "Failed to release recovery lease for request %s.",
                request_id or stale_request.get("_id"),
            )

        return 0


if __name__ == "__main__":
    import asyncio

    async def _run():
        result = await dispatch_request()
        if result:
            print(f"[SUCCESS] Dispatched: {result}")
        else:
            print("[INFO] No pending request found to dispatch.")

    asyncio.run(_run())
