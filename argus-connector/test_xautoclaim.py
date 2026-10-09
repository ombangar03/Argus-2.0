import asyncio

from app.platform.redis import redis_client


async def main():
    await redis_client.xadd(
        "queue:connector:rss",
        {
            "request_id": "xautoclaim-test-001",
            "source": "test",
            "source_type": "invalid_test_source",
            "source_url": "https://example.com",
            "metadata": "{}",
        },
    )

    print("Test message added.")

    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
