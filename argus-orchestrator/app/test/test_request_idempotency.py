import asyncio
import uuid
from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

from app.platform.mongodb import rss_requests


ACTIVE_QUERY_KEY = "test:idempotency:concurrency"


async def create_test_request(worker_name: str):
    request_doc = {
        "request_id": f"test_{uuid.uuid4().hex[:12]}",
        "source": "test",
        "source_type": "test",
        "active_query_key": ACTIVE_QUERY_KEY,
        "url": "https://www.moneycontrol.com/",
        "metadata": {
            "worker": worker_name,
        },
        "status": "pending",
        "priority": "NORMAL",
        "created_at": datetime.now(timezone.utc),
    }

    try:
        await rss_requests.insert_one(request_doc)

        print(
            f"{worker_name}: CREATED "
            f"{request_doc['request_id']}"
        )

        return "created"

    except DuplicateKeyError:
        print(
            f"{worker_name}: SKIPPED "
            f"(DuplicateKeyError)"
        )

        return "skipped"


async def main():
    print("Starting concurrent idempotency test...")

    results = await asyncio.gather(
        create_test_request("worker-A"),
        create_test_request("worker-B"),
    )

    print("\nResults:")
    print(results)

    active_count = await rss_requests.count_documents(
        {
            "active_query_key": ACTIVE_QUERY_KEY,
            "status": {
                "$in": ["pending", "processing"]
            },
        }
    )

    print(
        f"\nActive requests with same key: {active_count}"
    )


if __name__ == "__main__":
    asyncio.run(main())