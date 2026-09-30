import asyncio
import json
import uuid

from app.platform.redis import redis_client
from app.core.config import settings


async def main():

    request = {
        "request_id": f"test_nse_circular_worker_{uuid.uuid4().hex[:8]}",
        "source": "nse",
        "source_type": "nse_circular",
        "source_url": "https://www.nseindia.com/api/circulars",
        "metadata": {
            "from_date": "23-09-2026",
            "to_date": "30-09-2026",
        },
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

    print("\n" + "=" * 70)
    print("NSE CIRCULAR REDIS REQUEST CREATED")
    print("=" * 70)

    print("Request ID :", request["request_id"])
    print("Stream     :", settings.connector_stream)
    print("Stream ID  :", stream_id)

    print("\nRequest:")
    print(json.dumps(request, indent=4))

    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())