
import logging
from datetime import datetime, timezone
from typing import Any

from pymongo.errors import DuplicateKeyError

from app.platform.mongodb import rss_requests
from app.services.bing_service import build_bing_rss_url

logger = logging.getLogger(__name__)


async def create_scheduled_rss_request(
    unit_id: str,
    term: str,
    scheduled_for: float,
    priority: str,
    org_id: str = "",
    search_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create an idempotent RSS request for one scheduled occurrence.

    The request is stored in MongoDB. The existing RequestWorker
    will later publish it to the connector's Redis stream.
    """
    if not unit_id.strip():
        raise ValueError("unit_id must not be empty")

    if not term.strip():
        raise ValueError("term must not be empty")

    if not priority.strip():
        raise ValueError("priority must not be empty")

    # A stable timestamp representation gives retries the same request ID.
    scheduled_timestamp = float(scheduled_for)
    request_id = (
        f"scheduled:{unit_id}:{scheduled_timestamp:.6f}"
    )

    now = datetime.now(timezone.utc)
    rss_url = build_bing_rss_url(term)

    request_doc = {
        "request_id": request_id,
        "source": "bing",
        "source_type": "rss",
        "url": rss_url,
        "metadata": {
            "query": term,
            "topic": None,
            "unit_id": unit_id,
            "org_id": org_id,
            "search_options": search_options or {},
            "scheduled_for": scheduled_timestamp,
        },
        "status": "pending",
        "priority": priority.upper(),
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

        logger.info(
            "Created scheduled request %s for unit %s",
            request_id,
            unit_id,
        )

        return {
            "request_id": request_id,
            "created": True,
        }

    except DuplicateKeyError:
        # A retry may encounter a request created by an earlier attempt.
        existing = await rss_requests.find_one(
            {"request_id": request_id},
            {"_id": 1, "request_id": 1},
        )

        if existing is None:
            # The duplicate came from another constraint, not this request ID.
            raise

        logger.info(
            "Scheduled request %s already exists; treating as a retry",
            request_id,
        )

        return {
            "request_id": request_id,
            "created": False,
        }
