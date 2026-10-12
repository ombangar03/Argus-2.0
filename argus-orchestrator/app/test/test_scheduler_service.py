import asyncio 
import time
import uuid

from app.platform.redis import redis_client
from app.services.scheduler_service import (
    register_scheduled_unit,
    get_due_unit_id,
    _due_key,
    _unit_key,
)

async def main():
    unit_id = f"test-unit-{uuid.uuid4().hex}"
    platform = "rss"
    priority = "high"

    due_key = _due_key(platform, priority)
    unit_key = _unit_key(unit_id)

    try:
        # schedule the unit for 60sec in the future.try

        future_time = time.time() + 60

        await register_scheduled_unit(
            unit_id=unit_id,
            platform=platform,
            priority=priority,
            next_run_at=future_time,
            term="copper prices",
            cadence_seconds=300,
            org_id="test-org",
            search_options={"language": "en"},
        )

        #verify the metadata hash

        metadata = await redis_client.hgetall(unit_key)
        assert metadata["term"] == "copper prices"
        assert metadata["cadence_seconds"] == "300"

        # the unit should not be due before its schedule time
        early_results = await get_due_unit_id(
            platform,
            priority,
            now=future_time - 1,
        )

        assert unit_id not in early_results

        # it should be due at its scheduled time.
        due_results = await get_due_unit_id(
            platform,
            priority,
            now=future_time,
        )

        assert unit_id in due_results

        print("PASS: scheduled unit registered")
        print("PASS: metadata stored in Redis")
        print("PASS: future unit is not returned early")
        print("PASS: unit is returned when due")

    finally:
        # Clean up test data to avoid affecting other tests
        await redis_client.zrem(due_key, unit_id)
        await redis_client.delete(unit_key)

        await redis_client.srem(
            "schedule:active_keys",
            f"{platform}:{priority}",
        )
        
        await redis_client.aclose()
        
        print("Cleaned up test data")


if __name__ == "__main__":
    asyncio.run(main())