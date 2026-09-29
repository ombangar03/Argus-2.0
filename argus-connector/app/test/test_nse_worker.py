import asyncio
import json

from app.platform.redis import redis_client
from app.core.config import settings


async def main():

    request = {
        "request_id": "test_nse_worker_001",
        "source": "nse",
        "source_type": "nse_announcements",
        "source_url": (
            "https://nsearchives.nseindia.com/"
            "content/RSS/Online_announcements.xml"
        ),
        "metadata": {},
    }

    stream_id = await redis_client.xadd(
        settings.connector_stream,
        {
            "request_id": request["request_id"],
            "source": request["source"],
            "source_type": request["source_type"],
            "source_url": request["source_url"],
            "metadata": json.dumps(request["metadata"]),
        },
    )

    print("Request added to Redis.")
    print("Stream:", settings.connector_stream)
    print("Stream ID:", stream_id)

    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())