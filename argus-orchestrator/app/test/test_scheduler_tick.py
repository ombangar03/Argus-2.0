
import asyncio
import time
import uuid
from unittest.mock import AsyncMock, patch

from app.platform.redis import redis_client
from app.services import scheduler_service


ACTIVE_KEYS_SET = "schedule:active_keys"
MANAGED_UNITS_SET = "schedule:managed_unit_ids"


async def main():
    suffix = uuid.uuid4().hex

    test_cases = [
        {
            "platform": f"test-a-{suffix}",
            "priority": "high",
            "unit_id": f"test-unit-a-{suffix}",
            "term": "copper prices",
        },
        {
            "platform": f"test-b-{suffix}",
            "priority": "normal",
            "unit_id": f"test-unit-b-{suffix}",
            "term": "gold prices",
        },
    ]

    created_keys = []
    mock_create_request = AsyncMock()

    for case in test_cases:
        mock_create_request.side_effect = None
        mock_create_request.return_value = {
            "request_id": f"scheduled:{case['unit_id']}:test",
            "created": True,
        }

    # Return a deterministic request ID based on the unit being dispatched.
    async def create_request_side_effect(**kwargs):
        return {
            "request_id": (
                f"scheduled:{kwargs['unit_id']}:"
                f"{kwargs['scheduled_for']:.6f}"
            ),
            "created": True,
        }

    mock_create_request.side_effect = create_request_side_effect

    try:
        now = time.time()

        for case in test_cases:
            platform = case["platform"]
            priority = case["priority"]
            unit_id = case["unit_id"]

            await scheduler_service.register_scheduled_unit(
                unit_id=unit_id,
                platform=platform,
                priority=priority,
                next_run_at=now - 1,
                term=case["term"],
                cadence_seconds=300,
            )

            created_keys.append(
                (
                    scheduler_service._due_key(platform, priority),
                    unit_id,
                    scheduler_service._unit_key(unit_id),
                    f"{platform}:{priority}",
                )
            )

        # Patch the service where scheduler_service uses it.
        with patch.object(
            scheduler_service,
            "create_scheduled_rss_request",
            mock_create_request,
        ):
            count = await scheduler_service.dispatch_due_units_once(now=now)

            assert count == 2, (
                f"Expected 2 dispatched units, got {count}"
            )
            print("PASS: dispatcher processed both active combinations")

            assert mock_create_request.await_count == 2
            requested_units = {
                call.kwargs["unit_id"]
                for call in mock_create_request.await_args_list
            }
            expected_units = {
                case["unit_id"] for case in test_cases
            }
            assert requested_units == expected_units
            print("PASS: both due units created MongoDB requests")

            # Each unit should have a future Redis due score.
            for case in test_cases:
                due_key = scheduler_service._due_key(
                    case["platform"],
                    case["priority"],
                )
                next_run = await redis_client.zscore(
                    due_key,
                    case["unit_id"],
                )

                assert next_run is not None
                assert next_run > now
                print(
                    "PASS: next run updated for "
                    f"{case['platform']}:{case['priority']}"
                )

            # A second tick at the same timestamp must not create
            # additional requests because both units are now in the future.
            remaining = await scheduler_service.dispatch_due_units_once(
                now=now
            )

            assert remaining == 0, (
                f"Expected 0 repeated dispatches, got {remaining}"
            )
            assert mock_create_request.await_count == 2
            print("PASS: second tick did not redispatch either unit")

        # The scheduler must not publish directly to connector streams.
        for case in test_cases:
            stream_name = (
                f"queue:connector:{case['platform']}:{case['priority']}"
            )
            messages = await redis_client.xrange(stream_name)
            assert len(messages) == 0, (
                f"Scheduler unexpectedly published to {stream_name}"
            )

        print("PASS: scheduler did not publish to connector streams")

    finally:
        for due_key, unit_id, unit_key, active_key in created_keys:
            await redis_client.zrem(due_key, unit_id)
            await redis_client.delete(unit_key)

            await redis_client.srem(ACTIVE_KEYS_SET, active_key)
            await redis_client.srem(MANAGED_UNITS_SET, unit_id)

        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
