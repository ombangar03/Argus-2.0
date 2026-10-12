
import time
import unittest
import uuid
from unittest.mock import patch

from app.platform.redis import redis_client
from app.services import scheduler_service


class FakeCursor:
    def __init__(self, documents):
        self.documents = documents

    async def to_list(self, length=None):
        return self.documents


class FakeScheduledUnitsCollection:
    def __init__(self, documents):
        self.documents = documents

    def find(self, query):
        return FakeCursor(self.documents)


class TestSchedulerSyncDueTime(unittest.IsolatedAsyncioTestCase):

    async def test_sync_preserves_existing_redis_due_time(self):
        unit_id = f"test-preserve-due-{uuid.uuid4().hex}"
        platform = "rss"
        priority = "high"

        due_key = scheduler_service._due_key(platform, priority)
        unit_key = scheduler_service._unit_key(unit_id)

        active_key = f"{platform}:{priority}"
        original_due_time = time.time() + 3600
        runtime_due_time = original_due_time + 600

        try:
            # 1. Register the schedule for the first time.
            await scheduler_service.register_scheduled_unit(
                unit_id=unit_id,
                platform=platform,
                priority=priority,
                next_run_at=original_due_time,
                term="copper prices",
                cadence_seconds=300,
                org_id="test-org",
                search_options={"language": "en"},
                enabled=True,
                managed_by_mongodb=True,
            )

            # 2. Simulate runtime advancement by the scheduler.
            await redis_client.zadd(
                due_key,
                {unit_id: runtime_due_time},
            )

            score_before_sync = await redis_client.zscore(
                due_key,
                unit_id,
            )

            self.assertAlmostEqual(
                score_before_sync,
                runtime_due_time,
                places=3,
            )

            # 3. MongoDB still contains the original definition.
            documents = [
                {
                    "unit_id": unit_id,
                    "platform": platform,
                    "priority": priority,
                    "term": "copper prices",
                    "cadence_seconds": 300,
                    "next_run_at": original_due_time,
                    "org_id": "test-org",
                    "search_options": {"language": "en"},
                    "enabled": True,
                }
            ]

            fake_collection = FakeScheduledUnitsCollection(documents)

            with patch.object(
                scheduler_service,
                "scheduled_units",
                fake_collection,
            ):
                synchronized_count = (
                    await scheduler_service.sync_scheduled_units_from_mongodb()
                )

            # 4. Synchronization must not restore the stale MongoDB time.
            score_after_sync = await redis_client.zscore(
                due_key,
                unit_id,
            )

            self.assertEqual(synchronized_count, 1)
            self.assertAlmostEqual(
                score_after_sync,
                runtime_due_time,
                places=3,
            )

            print(
                "PASS: synchronization preserved the existing Redis due time"
            )

        finally:
            # Remove only the keys created by this test.
            async with redis_client.pipeline(transaction=True) as pipe:
                pipe.zrem(due_key, unit_id)
                pipe.delete(unit_key)
                pipe.srem(
                    scheduler_service.ACTIVE_KEYS_SET,
                    active_key,
                )
                pipe.srem(
                    scheduler_service.MANAGED_UNITS_SET,
                    unit_id,
                )
                await pipe.execute()


if __name__ == "__main__":
    unittest.main()
