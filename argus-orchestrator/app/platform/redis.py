import logging 

import redis.asyncio as redis

from app.core.config import settings

logger = logging.getLogger(__name__)

print("======================================")
print("Initializing Redis client....")
print("Redis URL:", settings.redis_url)

redis_client = redis.from_url(
    settings.redis_url,
    decode_responses = True
)

async def check_redis():

    print("checking redis connection.....")

    try:
        response = await redis_client.ping()
        print("Redis ping response:", response)

        if response:
            print("redis connection successful!")

            logger.info(
                "Redis connection successful"
            )

            return True

        print("Redis connection Failed")

    except Exception as exc:

        print("Redis connection Failed")
        print("Error:", exc)

        logger.exception(
            "Redis connection Failed"
        )

        return False