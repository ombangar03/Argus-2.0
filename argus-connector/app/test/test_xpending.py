import asyncio

from app.platform.redis import redis_client


async def main():
    result = await redis_client.xpending(
        "queue:connector:rss",
        "rss_connectors",
    )

    print(result)

    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())