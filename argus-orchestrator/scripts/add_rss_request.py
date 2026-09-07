import asyncio 
from datetime import datetime, timezone 
import uuid

from app.platform.mongodb import rss_requests 


async def seed_request():
    request_doc = {
        "request_id":f"req_{uuid.uuid4().hex[:12]}",
        "url": "https://agmetalminer.com/feed/",
        "platform": "rss",
        "status": "pending",
        "priority": "HIGH",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }

    result = await rss_requests.insert_one(request_doc)
    print(f"Seeded pending request with _id: {result.inserted_id}")

    print(f"Request ID: {request_doc['request_id']}")
    print(f"URL: {request_doc['url']}")


if __name__ == "__main__":
    asyncio.run(seed_request())