import logging
from datetime import datetime, timezone
from typing import Dict, Any, Union
from pymongo import UpdateOne
from pydantic import ValidationError

from app.platform.mongodb import rss_items, rss_requests
from app.schemas.result import CrawlResult, RSSItem

logger = logging.getLogger(__name__)


async def ensure_indexes():
    """
    Creates necessary MongoDB indexes for high-performance querying and deduplication.
    - rss_items: unique index on 'item_hash' (prevents duplicate articles).
    - rss_items: index on 'request_id' (fast lookups by crawl task)
    - rss_requests: index on 'request_id' (fast status updates).
    """
    # 1. Deduplication index on articles
    await rss_items.create_index("item_hash", unique=True)
    await rss_items.create_index("request_id")
    await rss_items.create_index("published_at")

    # 2. Fast lookup index on requests
    await rss_requests.create_index("request_id", unique=True, sparse=True)

    logger.info("MongoDB indexes verified successfully.")


async def save_rss_items(items: list[RSSItem]) -> dict:
    """
    Batch inserts or updates a list of RSSItem models into 'rss_items' collection.
    Uses bulk_write with upsert=True on 'item_hash' to guarantee zero duplicate articles.

    :return: Dictionary containing counts of processed, inserted and duplicate items.
    """
    if not items:
        return {
            "total": 0,
            "inserted": 0,
            "duplicates": 0
        }

    operations = [
        UpdateOne(
            {
                "item_hash": item.item_hash
            },
            {
                "$setOnInsert": item.model_dump()
            },
            upsert=True
        )
        for item in items
    ]

    try:
        # ordered=False allows MongoDB to write in parallel for maximum speed
        result = await rss_items.bulk_write(operations, ordered=False)
        inserted_count = result.upserted_count
        duplicate_count = len(items) - inserted_count

        logger.info(
            f"Bulk write finished: {inserted_count} new articles inserted, "
            f"{duplicate_count} existing duplicates skipped."
        )

        return {
            "total": len(items),
            "inserted": inserted_count,
            "duplicates": duplicate_count
        }

    except Exception as exc:
        logger.exception(f"Failed to bulk write articles into rss_items: {exc}")
        raise


async def process_crawl_result(
    result_payload: Union[CrawlResult, Dict[str, Any]]
) -> dict:
    """
    Core entrypoint for processing incoming crawl result:
    1. Validates payload using CrawlResult schemas.
    2. Upsert articles into 'rss_items' without duplicates.
    3. Atomically updates 'rss_requests' records to 'completed' or 'failed'.
    """
    # 1. Validate payload using CrawlResult schemas
    if isinstance(result_payload, dict):
        try:
            result = CrawlResult.model_validate(result_payload)
        except ValidationError as exc:
            logger.error(f"Malformed CrawlResult payload: {exc.errors()}")
            raise
    else:
        result = result_payload

    request_id = result.request_id
    now = datetime.now(timezone.utc)

    # 2. Handle SUCCESS outcome
    if result.status == "success":
        stats = await save_rss_items(result.items)

        # Transition request status from 'processing' to 'completed'
        update_result = await rss_requests.update_one(
            {
                "request_id": request_id,
            },
            {
                "$set": {
                    "status": "completed",
                    "items_count": len(result.items),
                    "new_items_count": stats["inserted"],
                    "completed_at": now,
                    "updated_at": now,
                    "error_message": None
                }
            }
        )

        logger.info(
            f"Request {request_id} marked as 'completed'. Matched: {update_result.matched_count}, Items Saved: {stats['inserted']}"
        )

        return {
            "request_id": request_id,
            "status": "completed",
            "item_saved": stats["total"],
            "items_inserted": stats["inserted"],
            "items_duplicates": stats["duplicates"]
        }

    # 3. Handle FAILED outcome
    else:
        error_msg = result.error_message or "Unknown connector crawl failure"

        await rss_requests.update_one(
            {
                "request_id": request_id
            },
            {
                "$set": {
                    "status": "failed",
                    "error_message": error_msg,
                    "completed_at": now,
                    "updated_at": now
                }
            }
        )

        logger.warning(
            f"Request {request_id} marked as 'failed'. Reason: {error_msg}"
        )

        return {
            "request_id": request_id,
            "status": "failed",
            "error_message": error_msg
        }


if __name__ == "__main__":
    import asyncio
    from app.core.logging import setup_logging

    async def _test():
        setup_logging()
        print("--- Testing result_service.py ---")

        # Ensure indexes exist
        await ensure_indexes()

        # Create a mock success result with 2 items
        mock_result = CrawlResult(
            request_id="req_test_demo",
            status="success",
            items_count=2,
            items=[
                RSSItem(
                    request_id="req_test_demo",
                    title="Copper Markets Hit Historic High",
                    link="https://example.com/copper-high-2026",
                    summary="Copper rallied 4% in Asian trading hours.",
                    author="Jane Doe"
                ),
                RSSItem(
                    request_id="req_test_demo",
                    title="Steel Tariffs Update",
                    link="https://example.com/steel-tariffs-2026",
                    summary="New tariff adjustments announced."
                )
            ]
        )

        # Seed mock request in rss_requests so we can see it complete
        await rss_requests.update_one(
            {"request_id": "req_test_demo"},
            {"$set": {"url": "https://example.com/feed", "status": "processing"}},
            upsert=True
        )

        # Test processing the result
        summary = await process_crawl_result(mock_result)
        print(f"[SUCCESS] Processed result: {summary}")

        # Test deduplication: re-run the exact same result!
        print("\n--- Testing Deduplication (Re-running same batch) ---")
        summary_dedup = await process_crawl_result(mock_result)
        print(f"[DEDUPLICATION TEST] Processed result: {summary_dedup}")
        print("Notice that items_inserted is 0 and items_duplicates is 2!")

    asyncio.run(_test())
