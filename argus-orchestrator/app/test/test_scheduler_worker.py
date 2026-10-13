
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import app.workers.scheduler_worker as scheduler_module
from app.workers.scheduler_worker import SchedulerWorker


class TestSchedulerWorker(unittest.IsolatedAsyncioTestCase):

    async def test_sync_runs_before_dispatch(self):
        worker = SchedulerWorker(tick_seconds=0.05)
        call_order = []

        async def fake_sync():
            call_order.append("sync")

        async def fake_dispatch():
            call_order.append("dispatch")

            # Stop after the first dispatch.
            await worker.stop()
            return 0

        with (
            patch.object(
                scheduler_module,
                "sync_scheduled_units_from_mongodb",
                new=AsyncMock(side_effect=fake_sync),
            ),
            patch.object(
                scheduler_module,
                "dispatch_due_units_once",
                new=AsyncMock(side_effect=fake_dispatch),
            ),
        ):
            await asyncio.wait_for(worker.run(), timeout=2)

        self.assertEqual(call_order, ["sync", "dispatch"])

    async def test_worker_runs_three_ticks_and_stops(self):
        worker = SchedulerWorker(tick_seconds=0.05)
        call_count = 0

        async def fake_sync():
            return None

        async def fake_dispatch():
            nonlocal call_count
            call_count += 1

            if call_count == 3:
                await worker.stop()

            return 0

        with (
            patch.object(
                scheduler_module,
                "sync_scheduled_units_from_mongodb",
                new=AsyncMock(side_effect=fake_sync),
            ),
            patch.object(
                scheduler_module,
                "dispatch_due_units_once",
                new=AsyncMock(side_effect=fake_dispatch),
            ),
        ):
            await asyncio.wait_for(worker.run(), timeout=2)

        self.assertEqual(call_count, 3)


if __name__ == "__main__":
    unittest.main()
