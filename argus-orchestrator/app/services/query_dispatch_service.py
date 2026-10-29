import logging 
import uuid
from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

from app.platform.mongodb import news_queries, rss_requests
from app.services.bing_service import build_bing_rss_url

logger = logging.getLogger(__name__)

async def dispatch_news_queries():
    """
    Reads enabled news queries and creates pending RSS requests.
    """

    cursor = news_queries.find(
        {
            "enabled": True
        }
    )

    created_count = 0
    skipped_count = 0

    async for query_doc in cursor:
        query = query_doc.get("query")
        source = query_doc.get("source")
        source_type = query_doc.get("source_type")

        if (
            not query
            or not isinstance(query, str)
            or not query.strip()
            or not source
            or not isinstance(source, str)
            or not source.strip()
            or not source_type
            or not isinstance(source_type, str)
            or not source_type.strip()
        ):
            logger.warning(
                "Skipping malformed news query %s: query=%r, source=%r, source_type=%r",
                query_doc.get("_id"),
                query,
                source,
                source_type,
            )
            skipped_count += 1
            continue

        query = query.strip()
        source = source.strip()
        source_type = source_type.strip()

        # normalize query for duplicate detection
        normalized_query = query.lower()

        active_query_key = f"{source}:{source_type}:{normalized_query}"

        existing_request= await rss_requests.find_one(
            {
                "active_query_key": active_query_key,
                "status": {
                    "$in": ["pending", "processing"]
                }
            }
        )

        if existing_request:
            logger.info(
                "Skipping query '%s'. An active request already exists",
                query,
            )

            skipped_count += 1
            continue

        # convert query into Bing RSS URL
        rss_url = build_bing_rss_url(query)

        now = datetime.now(timezone.utc)

        request_doc = {
            "request_id": f"req_{uuid.uuid4().hex[:12]}",

            "source": source,
            "source_type": source_type,
            "active_query_key": active_query_key,
            "url": rss_url,

            "metadata": {
                "query": query,
                "topic": query_doc.get("topic"),
            },

            "status": "pending",
            "priority": query_doc.get("priority", "NORMAL"),

            "created_at": now,
            "claimed_at": None,
            "dispatched_at": None,
            "completed_at": None,
            "updated_at": now,

            "stream_message_id": None,

            "items_count": 0,
            "new_items_count": 0,

            "error_message": None,
        }

        try:
            await rss_requests.insert_one(request_doc)

            created_count += 1

            logger.info(
                "Created new request %s for query '%s' (URL: %s)",
                request_doc["request_id"],
                query,
                rss_url,
            )
        except DuplicateKeyError:
            skipped_count += 1
            
            logger.info(
                "Skipping query '%s'. An active request already exists",
                query,
            )
    
    logger.info(
        "Query dispatch completed. Created=%s, Skipped=%s",
        created_count,
        skipped_count,
    )

    return {
        "created": created_count,
        "skipped": skipped_count,
    }

if __name__ == "__main__":
    import asyncio

    async def _run():
        result = await dispatch_news_queries()
        print(f"Dispatch result: {result}")

    asyncio.run(_run())