
import unittest
from unittest.mock import AsyncMock, patch

from app.workers.request_worker import RequestWorker


class TestRequestWorker(unittest.IsolatedAsyncioTestCase):

    @patch("app.workers.request_worker.check_redis", new_callable=AsyncMock)
    @patch("app.workers.request_worker.check_mongodb", new_callable=AsyncMock)
    async def test_worker_stops_when_health_check_fails(
        self, mock_check_mongodb, mock_check_redis
    ):
        mock_check_mongodb.return_value = False
        mock_check_redis.return_value = True

        worker = RequestWorker(poll_interval=0.01)

        await worker.start()

        mock_check_mongodb.assert_awaited_once()
        mock_check_redis.assert_awaited_once()


    @patch("app.workers.request_worker.dispatch_request", new_callable=AsyncMock)
    @patch(
        "app.workers.request_worker.recover_stale_processing_requests",
        new_callable=AsyncMock,
    )
    @patch("app.workers.request_worker.check_redis", new_callable=AsyncMock)
    @patch("app.workers.request_worker.check_mongodb", new_callable=AsyncMock)
    async def test_worker_attempts_recovery_and_dispatch(
        self,
        mock_check_mongodb,
        mock_check_redis,
        mock_recovery,
        mock_dispatch,
    ):
        mock_check_mongodb.return_value = True
        mock_check_redis.return_value = True
        mock_recovery.return_value = 0
        mock_dispatch.return_value = None

        worker = RequestWorker(poll_interval=0.01)

        async def stop_after_dispatch():
            worker.stop()
            return None

        mock_dispatch.side_effect = stop_after_dispatch

        await worker.start()

        mock_recovery.assert_awaited()
        mock_dispatch.assert_awaited()


    @patch("app.workers.request_worker.dispatch_request", new_callable=AsyncMock)
    @patch(
        "app.workers.request_worker.recover_stale_processing_requests",
        new_callable=AsyncMock,
    )
    @patch("app.workers.request_worker.check_redis", new_callable=AsyncMock)
    @patch("app.workers.request_worker.check_mongodb", new_callable=AsyncMock)
    async def test_dispatch_continues_when_recovery_fails(
        self,
        mock_check_mongodb,
        mock_check_redis,
        mock_recovery,
        mock_dispatch,
    ):
        mock_check_mongodb.return_value = True
        mock_check_redis.return_value = True
        mock_recovery.side_effect = RuntimeError("Recovery failed")
        mock_dispatch.return_value = None

        worker = RequestWorker(poll_interval=0.01)

        async def stop_after_dispatch():
            worker.stop()
            return None

        mock_dispatch.side_effect = stop_after_dispatch

        await worker.start()

        mock_recovery.assert_awaited()
        mock_dispatch.assert_awaited()


if __name__ == "__main__":
    unittest.main()
