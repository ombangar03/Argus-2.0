
import asyncio
import time
import uuid

from app.platform.redis import redis_client
from app.services.scheduler_service import (
    register_scheduled_unit,
    dispatch_due_unit,
    _due_key,
    _unit_key,
)


async def main():
    # Unique platform keeps this test separate from real schedules.
    platform = f"test-{uuid.uuid4().hex}"
    priority = "high"
    unit_id = f"test-unit-{uuid.uuid4().hex}"

    due_key = _due_key(platform, priority)
    unit_key = _unit_key(unit_id)
    stream_name = f"queue:connector:{platform}:{priority}"

    try:
        now = time.time()

        await register_scheduled_unit(
            unit_id=unit_id,
            platform=platform,
            priority=priority,
            next_run_at=now - 1,
            term="copper prices",
            cadence_seconds=300,
            org_id="test-org",
            search_options={"language": "en"},
        )

        # Dispatch the due unit.
        result = await dispatch_due_unit(
            unit_id=unit_id,
            platform=platform,
            priority=priority,
            now=now,
        )

        assert result is not None
        print("PASS: due unit dispatched")

        # Confirm a message was published.
        messages = await redis_client.xrange(stream_name)
        assert len(messages) == 1

        message_id, payload = messages[0]
        assert payload["term"] == "copper prices"
        assert payload["unit_id"] == unit_id
        print("PASS: connector stream contains the job")

        # Confirm the next run is in the future.
        next_run = await redis_client.zscore(due_key, unit_id)
        assert next_run is not None
        assert next_run > now
        print("PASS: next run time was updated")

        # Dispatching again at the same time must not add another job.
        second_result = await dispatch_due_unit(
            unit_id=unit_id,
            platform=platform,
            priority=priority,
            now=now,
        )

        assert second_result is None

        messages_after_retry = await redis_client.xrange(stream_name)
        assert len(messages_after_retry) == 1
        print("PASS: repeated dispatch did not add another job")

    finally:
        # Clean up only this test's isolated Redis data.
        await redis_client.zrem(due_key, unit_id)
        await redis_client.delete(unit_key, stream_name)
        await redis_client.srem(
            "schedule:active_keys",
            f"{platform}:{priority}",
        )
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
