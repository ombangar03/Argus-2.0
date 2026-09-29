import asyncio

from app.core.config import settings
from app.platform.redis import redis_client


async def main():

    print("Stream:", settings.connector_stream)
    print("Group:", settings.consumer_group)
    print("Consumer:", settings.consumer_name)
    print()

    try:
        stream_info = await redis_client.xinfo_stream(
            settings.connector_stream
        )

        print("STREAM EXISTS")
        print("Length:", stream_info["length"])

    except Exception as exc:
        print("STREAM CHECK ERROR:", exc)

    print()

    try:
        groups = await redis_client.xinfo_groups(
            settings.connector_stream
        )

        print("CONSUMER GROUPS:")

        for group in groups:
            print(group)

    except Exception as exc:
        print("GROUP CHECK ERROR:", exc)

    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())