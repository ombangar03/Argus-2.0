
import asyncio
import uuid
from unittest.mock import AsyncMock, patch

from app.platform.redis import redis_client
from app.workers.result_worker import ResultWorker


async def main():
    # Use unique names so this test cannot interfere with real Argus streams.
    test_id = uuid.uuid4().hex
    stream_name = f"test:argus:recovery:{test_id}"
    group_name = f"test-recovery-group:{test_id}"
    seed_consumer = f"test-seed:{test_id}"

    worker = ResultWorker(
        block_ms=100,
        batch_size=10,
        claim_idle_ms=1000,
        recovery_interval=0.01,
    )

    # Redirect this worker instance to the temporary test stream.
    worker.stream_name = stream_name
    worker.group_name = group_name
    worker.consumer_name = f"test-worker:{test_id}"
    worker.recovery_consumer_name = f"test-recovery:{test_id}"

    expected_payload = {
        "request_id": f"recovery-test-{test_id}",
        "items": [],
    }

    try:
        await redis_client.ping()
        print("Redis connection: OK")

        # Create an isolated stream and consumer group.
        await redis_client.xgroup_create(
            name=stream_name,
            groupname=group_name,
            id="0",
            mkstream=True,
        )

        message_id = await redis_client.xadd(
            stream_name,
            {
                "request_id": expected_payload["request_id"],
                "items": "[]",
            },
        )
        print(f"Test message created: {message_id}")

        # Deliver the message to another consumer without acknowledging it.
        delivered = await redis_client.xreadgroup(
            groupname=group_name,
            consumername=seed_consumer,
            streams={stream_name: ">"},
            count=1,
        )

        if not delivered:
            raise AssertionError("The seed consumer received no message.")

        print("Message delivered but deliberately left pending.")

        # Wait until the message exceeds the worker's 1-second idle threshold.
        await asyncio.sleep(1.2)

        async def fake_process_result(payload):
            print(f"Worker processed reclaimed payload: {payload}")
            worker.stop()

            return {
                "request_id": payload["request_id"],
                "status": "completed",
                "items_inserted": 0,
            }

        worker._is_running = True
        worker._stop_event.clear()

        with patch(
            "app.workers.result_worker.process_crawl_result",
            new_callable=AsyncMock,
            side_effect=fake_process_result,
        ) as mock_process:

            # Run the actual recovery loop against real Redis.
            await worker._recover_pending_messages()

            mock_process.assert_awaited_once_with(expected_payload)

        # XACK should have removed the message from the pending list.
        pending = await redis_client.xpending(stream_name, group_name)

        if pending["pending"] != 0:
            raise AssertionError(
                f"Expected zero pending messages, got {pending['pending']}."
            )

        print("PASS: XAUTOCLAIM recovered the message.")
        print("PASS: Python worker processed the reclaimed message.")
        print("PASS: Message was acknowledged after successful processing.")
        print("PASS: Pending message count is now zero.")

    finally:
        worker.stop()

        # Delete only this test's temporary stream.
        await redis_client.delete(stream_name)
        print("Temporary test stream cleaned up.")


if __name__ == "__main__":
    asyncio.run(main())
