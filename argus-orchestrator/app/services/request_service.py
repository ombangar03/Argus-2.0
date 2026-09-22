import logging 
import json
from datetime import datetime, timezone
from typing import Optional
from pymongo import ReturnDocument
from pydantic import ValidationError

from app.core.config import settings
from app.platform.mongodb import rss_requests
from app.platform.redis import redis_client
from app.schemas.request import ConnectorRequest

logger = logging.getLogger(__name__)

async def dispatch_request() -> Optional[ConnectorRequest]:
    """
    1. Atomically finds one pending request and marks it as 'processing'.
    2. Validates against ConnectorRequest schema.
    3. Publishes it to the Redis connector stream.
    4. Returns the dispatched ConnectorRequest or None if no pending work.
    """

    # 1. Atomically claim one pending request
    claimed_doc = await rss_requests.find_one_and_update(
        {
            "status": "pending"
        },
        {
            "$set": {
                "status": "processing",
                "claimed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
        },
        return_document=ReturnDocument.AFTER
    )

    if not claimed_doc:
        logger.debug("No pending RSS requests found. No work to dispatch.")
        return None

    request_id = claimed_doc.get("request_id") or str(claimed_doc["_id"])

    try:
        # 2. Build and validate the schema using Pydantic
        request_obj = ConnectorRequest(
            request_id=request_id,
            source=claimed_doc["source"],
            source_type=claimed_doc["source_type"],
            source_url=claimed_doc["url"],
            metadata=claimed_doc.get("metadata", {}),
        ) 

        # 3. Publish to Redis stream
        # this is where the job leaves orchestrator. queue:connector:rss

        dispatch_time = datetime.now(timezone.utc)

        redis_payload = request_obj.model_dump()

        redis_payload["metadata"] = json.dumps(
            redis_payload["metadata"]
        )

        stream_message_id = await redis_client.xadd(
            settings.connector_stream,
            redis_payload
        )

        logger.info(
            f"Dispatched request {request_id} to {settings.connector_stream} (msg_id: {stream_message_id})"
        )

        # 4. Save stream audit info back to MongoDB
        await rss_requests.update_one(
            {
                "_id": claimed_doc["_id"]
            },
            {
                "$set": {
                    "stream_message_id": stream_message_id,
                    "dispatched_at": dispatch_time,
                    "updated_at": datetime.now(timezone.utc)
                }
            }
        )

        return request_obj
    
    except ValidationError as exc:
        logger.error(
            f"Failed to validate claimed request {request_id}",
            extra={"errors": exc.errors()}
        )

        # Mark as failed so invalid document doesn't loop forever
        await rss_requests.update_one(
            {
                "_id": claimed_doc["_id"]
            },
            {
                "$set": {
                    "status": "failed",
                    "error_message": f"Validation error: {str(exc)}",
                    "updated_at": datetime.now(timezone.utc)
                }
            }
        )
        raise

    except Exception as exc:
        logger.exception(f"Failed to dispatch request {request_id} to Redis: {exc}")

        # Rollback status back to pending for transient errors (e.g. Redis disconnect)
        await rss_requests.update_one(
            {
                "_id": claimed_doc["_id"]
            },
            {
                "$set": {
                    "status": "pending",
                    "error_message": f"Dispatch error: {str(exc)}",
                    "updated_at": datetime.now(timezone.utc)
                }
            }
        )
        raise


if __name__ == "__main__":
    import asyncio

    async def _run():
        result = await dispatch_request()
        if result:
            print(f"[SUCCESS] Dispatched: {result}")
        else:
            print("[INFO] No pending request found to dispatch.")

    asyncio.run(_run())
