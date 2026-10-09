
import asyncio

import app.workers.scheduler_worker as scheduler_module
from app.workers.scheduler_worker import SchedulerWorker


async def main():
    worker = SchedulerWorker(tick_seconds=0.05)
    original_dispatch = scheduler_module.dispatch_due_units_once
    call_count = 0

    async def fake_dispatch():
        nonlocal call_count
        call_count += 1

        # Request shutdown after the third tick.
        if call_count == 3:
            await worker.stop()

        return 0

    try:
        # Replace the real dispatcher temporarily.
        scheduler_module.dispatch_due_units_once = fake_dispatch

        await asyncio.wait_for(worker.run(), timeout=2)

        assert call_count == 3
        print("PASS: scheduler worker executed three ticks")
        print("PASS: scheduler worker stopped gracefully")

    finally:
        # Restore the real dispatcher.
        scheduler_module.dispatch_due_units_once = original_dispatch


if __name__ == "__main__":
    asyncio.run(main())
