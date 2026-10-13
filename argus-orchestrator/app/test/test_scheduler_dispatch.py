import logging
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, patch

from app.services import scheduler_service


class TestSchedulerDispatch(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.redis = AsyncMock()
        self.create_request = AsyncMock(
            return_value={
                "request_id": "scheduled:unit-001:1900.000000",
                "created": True,
            }
        )

        self.redis_patch = patch.object(
            scheduler_service,
            "redis_client",
            self.redis,
        )
        self.request_patch = patch.object(
            scheduler_service,
            "create_scheduled_rss_request",
            self.create_request,
        )

        self.redis_patch.start()
        self.request_patch.start()

        self.addCleanup(self.redis_patch.stop)
        self.addCleanup(self.request_patch.stop)

        self.unit_id = "unit-001"
        self.platform = "rss"
        self.priority = "high"
        self.due_score = 1900.0
        self.now = 2000.0
        self.cadence_seconds = 300

        self.due_key = scheduler_service._due_key(
            self.platform,
            self.priority,
        )
        self.unit_key = scheduler_service._unit_key(self.unit_id)

        self.metadata = {
            "term": "copper prices",
            "platform": "rss",
            "priority": "high",
            "org_id": "test-org",
            "search_options": '{"language": "en"}',
            "cadence_seconds": str(self.cadence_seconds),
            "enabled": "true",
        }

        self.redis.zscore.return_value = self.due_score
        self.redis.hgetall.return_value = self.metadata
        self.redis.eval.return_value = 1

    async def test_due_unit_creates_request_with_correct_arguments(self):
        result = await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=self.now,
        )

        self.create_request.assert_awaited_once_with(
            unit_id="unit-001",
            term="copper prices",
            scheduled_for=1900.0,
            priority="high",
            org_id="test-org",
            search_options={"language": "en"},
        )

        self.assertIsNotNone(result)
        self.assertEqual(
            result["request_id"],
            "scheduled:unit-001:1900.000000",
        )
        self.assertEqual(result["next_run_at"], "2300.0")
        self.assertEqual(result["created"], "True")

        print("PASS: due unit creates a request with correct arguments")

    async def test_due_time_is_advanced_after_request_creation(self):
        call_order = []

        async def create_request_side_effect(**kwargs):
            call_order.append("request_created")
            return {
                "request_id": "scheduled:unit-001:1900.000000",
                "created": True,
            }

        async def eval_side_effect(*args):
            call_order.append("advance_due_time")
            return 1

        self.create_request.side_effect = create_request_side_effect
        self.redis.eval.side_effect = eval_side_effect

        result = await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=self.now,
        )

        self.assertIsNotNone(result)
        self.assertEqual(
            call_order,
            ["request_created", "advance_due_time"],
        )

        self.redis.eval.assert_awaited_once_with(
            scheduler_service._ADVANCE_DUE_TIME_SCRIPT,
            1,
            self.due_key,
            self.unit_id,
            self.due_score,
            self.now + self.cadence_seconds,
        )

        print("PASS: Redis due time advances after request creation")

    async def test_changed_due_score_returns_none_and_logs(self):
        self.redis.eval.return_value = 0

        with self.assertLogs(
            scheduler_service.logger,
            level=logging.INFO,
        ) as captured:
            result = await scheduler_service.dispatch_due_unit(
                unit_id=self.unit_id,
                platform=self.platform,
                priority=self.priority,
                now=self.now,
            )

        self.assertIsNone(result)
        self.create_request.assert_awaited_once()

        self.assertTrue(
            any(
                "already advanced" in message.lower()
                for message in captured.output
            ),
            f"Expected 'already advanced' in logs: {captured.output}",
        )

        print("PASS: changed due score returns None and logs the condition")

    async def test_future_due_unit_is_not_dispatched(self):
        self.redis.zscore.return_value = self.now + 100

        result = await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=self.now,
        )

        self.assertIsNone(result)
        self.create_request.assert_not_awaited()
        self.redis.hgetall.assert_not_awaited()
        self.redis.eval.assert_not_awaited()

        print("PASS: future unit does not create a request")

    async def test_missing_metadata_does_not_create_request(self):
        self.redis.hgetall.return_value = {}

        result = await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=self.now,
        )

        self.assertIsNone(result)
        self.create_request.assert_not_awaited()
        self.redis.eval.assert_not_awaited()

        print("PASS: missing metadata does not create a request")

    async def test_scheduler_does_not_publish_to_connector_stream(self):
        await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=self.now,
        )

        self.redis.xadd.assert_not_awaited()

        # The replacement Lua script must only advance the due score.
        self.assertNotIn(
            "XADD",
            scheduler_service._ADVANCE_DUE_TIME_SCRIPT.upper(),
        )

        # The script receives only the sorted-set key, not a stream key.
        eval_args = self.redis.eval.await_args.args
        self.assertEqual(eval_args[1], 1)
        self.assertEqual(eval_args[2], self.due_key)

        print("PASS: scheduler does not publish to a connector stream")


    async def _assert_daily_next_run(
        self,
        hour: int,
        minute: int,
        expected_day: int,
        expected_hour: int,
    ):
        """Verify the next daily run using Asia/Kolkata time."""
        ist = ZoneInfo("Asia/Kolkata")

        local_now = datetime(
            2026, 10, 13, hour, minute, tzinfo=ist
        )
        now_timestamp = local_now.timestamp()

        expected_local = datetime(
            2026, 10, expected_day,
            expected_hour, 0, tzinfo=ist
        )
        expected_timestamp = expected_local.timestamp()

        self.metadata.update({
            "schedule_type": "daily_times",
            "timezone_name": "Asia/Kolkata",
            "run_times": '["10:00", "19:00"]',
        })

        result = await scheduler_service.dispatch_due_unit(
            unit_id=self.unit_id,
            platform=self.platform,
            priority=self.priority,
            now=now_timestamp,
        )

        self.assertIsNotNone(result)
        self.assertAlmostEqual(
            float(result["next_run_at"]),
            expected_timestamp,
        )

    async def test_daily_schedule_before_first_run(self):
        await self._assert_daily_next_run(
            hour=9,
            minute=45,
            expected_day=13,
            expected_hour=10,
        )

    async def test_daily_schedule_between_run_times(self):
        await self._assert_daily_next_run(
            hour=10,
            minute=5,
            expected_day=13,
            expected_hour=19,
        )

    async def test_daily_schedule_after_last_run(self):
        await self._assert_daily_next_run(
            hour=19,
            minute=30,
            expected_day=14,
            expected_hour=10,
        )



if __name__ == "__main__":
    unittest.main()
