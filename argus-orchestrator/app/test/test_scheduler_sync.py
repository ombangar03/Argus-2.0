
import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.services import scheduler_service as scheduler


class TestSchedulerMongoSync(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        # Mock MongoDB collection and its async cursor.
        self.mongo_cursor = MagicMock()
        self.mongo_cursor.to_list = AsyncMock(return_value=[])

        self.mongo_collection = MagicMock()
        self.mongo_collection.find.return_value = self.mongo_cursor

        # Mock Redis operations used by the synchronization function.
        self.redis = MagicMock()
        self.redis.smembers = AsyncMock(return_value=set())
        self.redis.hgetall = AsyncMock(return_value={})

        # Mock Redis pipeline context manager.
        self.pipeline = MagicMock()
        self.pipeline.execute = AsyncMock()

        self.pipeline_context = MagicMock()
        self.pipeline_context.__aenter__ = AsyncMock(
            return_value=self.pipeline
        )
        self.pipeline_context.__aexit__ = AsyncMock(return_value=False)

        self.redis.pipeline.return_value = self.pipeline_context

        self.mongo_patch = patch.object(
            scheduler, "scheduled_units", self.mongo_collection
        )
        self.redis_patch = patch.object(
            scheduler, "redis_client", self.redis
        )
        self.register_patch = patch.object(
            scheduler,
            "register_scheduled_unit",
            new_callable=AsyncMock,
        )

        self.mongo_patch.start()
        self.redis_patch.start()
        self.mock_register = self.register_patch.start()

        self.addCleanup(self.mongo_patch.stop)
        self.addCleanup(self.redis_patch.stop)
        self.addCleanup(self.register_patch.stop)

    def make_document(self, **overrides):
        document = {
            "unit_id": "test-schedule-001",
            "platform": "rss",
            "priority": "high",
            "term": "copper prices",
            "next_run_at": datetime.now(timezone.utc),
            "cadence_seconds": 300,
            "org_id": "test-org",
            "search_options": {"language": "en"},
            "enabled": True,
        }
        document.update(overrides)
        return document

    async def test_enabled_schedule_is_registered(self):
        document = self.make_document()
        self.mongo_cursor.to_list.return_value = [document]

        count = await scheduler.sync_scheduled_units_from_mongodb()

        self.assertEqual(count, 1)
        self.mock_register.assert_awaited_once_with(
            unit_id="test-schedule-001",
            platform="rss",
            priority="high",
            next_run_at=document["next_run_at"],
            term="copper prices",
            cadence_seconds=300,
            org_id="test-org",
            search_options={"language": "en"},
            enabled=True,
            managed_by_mongodb=True,
            preserve_due_time=True,
        )
        print("PASS: enabled schedule is registered")

    async def test_disabled_managed_schedule_is_removed(self):
        document = self.make_document(enabled=False)
        self.mongo_cursor.to_list.return_value = [document]
        self.redis.smembers.return_value = {"test-schedule-001"}
        self.redis.hgetall.return_value = {
            "platform": "rss",
            "priority": "high",
        }

        count = await scheduler.sync_scheduled_units_from_mongodb()

        self.assertEqual(count, 0)
        self.mock_register.assert_not_awaited()
        self.pipeline.zrem.assert_called_once_with(
            "schedule:due:rss:high",
            "test-schedule-001",
        )
        self.pipeline.delete.assert_called_once_with(
            "unit:test-schedule-001"
        )
        self.pipeline.srem.assert_called_once_with(
            scheduler.MANAGED_UNITS_SET,
            "test-schedule-001",
        )
        self.pipeline.execute.assert_awaited_once()
        print("PASS: disabled managed schedule is removed")

    async def test_invalid_schedule_preserves_existing_entry(self):
        document = self.make_document(cadence_seconds=0)
        self.mongo_cursor.to_list.return_value = [document]
        self.redis.smembers.return_value = {"test-schedule-001"}

        count = await scheduler.sync_scheduled_units_from_mongodb()

        self.assertEqual(count, 0)
        self.mock_register.assert_not_awaited()
        self.redis.hgetall.assert_not_awaited()
        self.pipeline.execute.assert_not_awaited()
        print("PASS: invalid schedule preserves existing entry")

    async def test_deleted_managed_schedule_is_removed(self):
        # MongoDB returns no documents, but Redis tracks an old managed unit.
        self.mongo_cursor.to_list.return_value = []
        self.redis.smembers.return_value = {"deleted-schedule-001"}
        self.redis.hgetall.return_value = {
            "platform": "rss",
            "priority": "normal",
        }

        count = await scheduler.sync_scheduled_units_from_mongodb()

        self.assertEqual(count, 0)
        self.pipeline.zrem.assert_called_once_with(
            "schedule:due:rss:normal",
            "deleted-schedule-001",
        )
        self.pipeline.delete.assert_called_once_with(
            "unit:deleted-schedule-001"
        )
        self.pipeline.srem.assert_called_once_with(
            scheduler.MANAGED_UNITS_SET,
            "deleted-schedule-001",
        )
        print("PASS: deleted managed schedule is removed")

    async def test_mongodb_read_failure_does_not_reconcile_redis(self):
        self.mongo_cursor.to_list.side_effect = RuntimeError(
            "Simulated MongoDB read failure"
        )

        with self.assertRaisesRegex(
            RuntimeError, "Simulated MongoDB read failure"
        ):
            await scheduler.sync_scheduled_units_from_mongodb()

        self.redis.smembers.assert_not_awaited()
        self.redis.hgetall.assert_not_awaited()
        self.pipeline.execute.assert_not_awaited()
        self.mock_register.assert_not_awaited()
        print("PASS: MongoDB read failure leaves Redis untouched")


if __name__ == "__main__":
    unittest.main()
