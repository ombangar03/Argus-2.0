import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app.workers.result_worker import ResultWorker


class TestResultWorker(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.worker = ResultWorker()

    @patch("app.workers.result_worker.redis_client")
    @patch("app.workers.result_worker.process_crawl_result", new_callable=AsyncMock)
    async def test_successful_processing_acknowledges_message(
        self, mock_process_result, mock_redis
    ):
        mock_process_result.return_value = {
            "request_id": "test-request-001",
            "status": "completed",
            "items_inserted": 3,
        }
        mock_redis.xack = AsyncMock()

        message_id = "1234567890-0"
        payload = {"request_id": "test-request-001", "items": []}

        await self.worker._process_message(message_id, payload)

        mock_process_result.assert_awaited_once_with(payload)
        mock_redis.xack.assert_awaited_once_with(
            self.worker.stream_name,
            self.worker.group_name,
            message_id,
        )

    @patch("app.workers.result_worker.redis_client")
    @patch("app.workers.result_worker.process_crawl_result", new_callable=AsyncMock)
    async def test_failed_processing_does_not_acknowledge_message(
        self, mock_process_result, mock_redis
    ):
        mock_process_result.side_effect = RuntimeError(
            "Simulated result-processing failure"
        )
        mock_redis.xack = AsyncMock()

        message_id = "1234567891-0"
        payload = {"request_id": "test-request-002", "items": []}

        with self.assertRaises(RuntimeError):
            await self.worker._process_message(message_id, payload)

        mock_process_result.assert_awaited_once_with(payload)
        mock_redis.xack.assert_not_awaited()

    @patch("app.workers.result_worker.redis_client")
    async def test_recovery_claims_and_processes_pending_message(
        self, mock_redis
    ):
        message_id = "1234567892-0"
        payload = {"request_id": "test-request-003", "items": []}

        mock_redis.xautoclaim = AsyncMock(
            return_value=(
                "0-0",
                [(message_id, payload)],
                [],
            )
        )

        self.worker._is_running = True
        self.worker._stop_event = asyncio.Event()


        with patch.object(
            self.worker, "_process_message", new_callable=AsyncMock
        ) as mock_process:
            # Stop the loop after the first recovery scan.
            async def stop_after_scan(*args, **kwargs):
                self.worker.stop()
                return (
                    "0-0",
                    [(message_id, payload)],
                    [],
                )

            mock_redis.xautoclaim.side_effect = stop_after_scan

            with patch(
                "app.workers.result_worker.asyncio.sleep",
                new_callable = AsyncMock,
            ) as mock_sleep:
                await self.worker._recover_pending_messages()

            mock_redis.xautoclaim.assert_awaited_once()
            mock_sleep.assert_awaited_once()
            mock_process.assert_awaited_once_with(message_id, payload)



if __name__ == "__main__":
    unittest.main()
