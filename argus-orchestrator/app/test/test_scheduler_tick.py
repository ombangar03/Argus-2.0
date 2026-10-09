
import asyncio
import time
import uuid

from app.platform.redis import redis_client
from app.services.scheduler_service import (
    register_scheduled_unit,
    dispatch_due_units_once,
    _due_key,
    _unit_key,
)


async def main():
    platform = f"test-{uuid.uuid4().hex}"
    priority = "high"

    due_key = _due_key(platform, priority)
    unit_id = f"test-unit-{uuid.uuid4().hex}"
    unit_key = _unit_key(unit_id)
    active_key = f"{platform}:{priority}"
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
        )

        # Run one dispatcher tick.
        count = await dispatch_due_units_once(
            now=now,
            platform_priority_keys={active_key},
        )

        assert count == 1
        print("PASS: dispatcher found and dispatched the due unit")

        # The message should be in the expected stream.
        messages = await redis_client.xrange(stream_name)
        assert len(messages) == 1
        assert messages[0][1]["unit_id"] == unit_id
        print("PASS: job published to the correct stream")

        # The unit should no longer be due at the same timestamp.
        remaining = await dispatch_due_units_once(
            now=now, 
            platform_priority_keys={active_key},
        )
        assert remaining == 0
        print("PASS: second tick did not dispatch the unit again")

    finally:
        await redis_client.zrem(due_key, unit_id)
        await redis_client.delete(unit_key, stream_name)
        await redis_client.srem(ACTIVE_KEYS_SET_FOR_TEST, active_key)
        await redis_client.aclose()


ACTIVE_KEYS_SET_FOR_TEST = "schedule:active_keys"


if __name__ == "__main__":
    asyncio.run(main())
