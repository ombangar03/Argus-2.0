import logging 
import redis.asyncio as redis 
from app.core.config import settings 

logger = logging.getLogger(__name__)

redis_client = redis.from_url(settings.redis_url, decode_responses=True)


async def check_redis() -> bool:
    """Ping Redis to confirm broker reachability"""
    try:
        response = await redis_client.ping()
        return response is True

    except Exception as exc:
        logger.exception("Redis connection failed")
        return False 