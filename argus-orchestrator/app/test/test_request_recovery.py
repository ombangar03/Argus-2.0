
import unittest

from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch

from app.services import request_service


class TestRequestRecovery(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.mongo_patch = patch.object(
            request_service,
            "rss_requests",
        )
        self.mongo = self.mongo_patch.start()
        self.addCleanup(self.mongo_patch.stop)

        self.mongo.find_one_and_update = AsyncMock(
            return_value=None
        )
        self.mongo.update_one = AsyncMock()
        self.mongo.update_one.return_value.modified_count = 1

    async def test_no_eligible_request_returns_zero(self):
        """
        If MongoDB finds no eligible stale request,
        recovery must not publish anything.
        """
        recovered_count = (
            await request_service.recover_stale_processing_requests(
                stale_after_seconds=120
            )
        )

        self.assertEqual(recovered_count, 0)

        self.mongo.find_one_and_update.assert_awaited_once()

        query = self.mongo.find_one_and_update.await_args.args[0]
        update = self.mongo.find_one_and_update.await_args.args[1]

        self.assertEqual(query["status"], "processing")
        self.assertIn("$lte", query["claimed_at"])
        self.assertIsNone(query["dispatched_at"])

        self.assertIn("$set", update)
        self.assertIn("recovery_claimed_at", update["$set"])
        self.assertIn("recovery_token", update["$set"])



    async def test_stale_request_is_claimed_atomically(self):
        stale_request = {
            "_id": "mongo-id-123",
            "request_id": "scheduled:unit-001:1900.000000",
            "status": "processing",
            "source": "unit-001",
            "source_type": "rss",
            "url": "http://www.w3schools.com/xml/plant_catalog.xml",
            "metadata": {},
            "claimed_at": datetime.now(timezone.utc) - timedelta(seconds=300),
            "dispatched_at": None,
        }
        self.mongo.find_one_and_update.return_value = stale_request

        self.mongo.update_one = AsyncMock()
        self.mongo.update_one.return_value.modified_count = 1

        with patch.object(
            request_service,
            "_get_existing_stream_message_id",
            new_callable=AsyncMock,
            return_value="1234567890-0",
        ):

            recovered_count = (
                await request_service.recover_stale_processing_requests(
                    stale_after_seconds=120
                )
            )

        self.assertEqual(recovered_count, 1)
        self.mongo.find_one_and_update.assert_awaited_once()

        query, update = (
            self.mongo.find_one_and_update.await_args.args[:2]
        )

        self.assertEqual(query["status"], "processing")
        self.assertIn("$lte", query["claimed_at"])
        self.assertIsNone(query["dispatched_at"])

        fields = update["$set"]
        self.assertIn("recovery_claimed_at", fields)
        self.assertIn("recovery_token", fields)
        self.assertTrue(fields["recovery_token"])


    async def test_invalid_timeout_is_rejected(self):
        with self.assertRaises(ValueError):
            await request_service.recover_stale_processing_requests(
                stale_after_seconds=0
            )

        self.mongo.find_one_and_update.assert_not_awaited()



    async def test_active_lease_condition_is_in_query(self):
        """
        An active recovery lease must prevent another worker
        from claiming the same request.
        """
        self.mongo.find_one_and_update.return_value = None

        recovered_count = (
            await request_service.recover_stale_processing_requests(
                stale_after_seconds=120
            )
        )

        self.assertEqual(recovered_count, 0)

        query = self.mongo.find_one_and_update.await_args.args[0]
        lease_conditions = query["$or"]

        self.assertIn(
            {"recovery_claimed_at": {"$exists": False}},
            lease_conditions,
        )
        self.assertIn(
            {"recovery_claimed_at": None},
            lease_conditions,
        )
        self.assertTrue(
            any(
                isinstance(condition.get("recovery_claimed_at"), dict)
                and "$lte" in condition["recovery_claimed_at"]
                for condition in lease_conditions
            )
        )

    async def test_recovery_claim_does_not_publish_without_reconciliation(self):
        stale_request = {
            "_id": "mongo-id-123",
            "request_id": "scheduled:unit-001:1900.000000",
            "status": "processing",
            "source": "unit-001",
            "source_type": "rss",
            "url": "https://example.com/feed.xml",
            "metadata": {},
            "claimed_at": datetime.now(timezone.utc) - timedelta(seconds=300),
            "dispatched_at": None,
        }
        self.mongo.find_one_and_update.return_value = stale_request

        self.mongo.update_one = AsyncMock()
        self.mongo.update_one.return_value.modified_count = 1

        with patch.object(
            request_service,
            "_get_existing_stream_message_id",
            new_callable=AsyncMock,
            return_value="1234567890-0",
        ), patch.object(
            request_service,
            "_publish_request_idempotently",
            new_callable=AsyncMock,
        ) as publish_mock:
            recovered_count = (
                await request_service.recover_stale_processing_requests(
                    stale_after_seconds=120
                )
            )

        self.assertEqual(recovered_count, 1)
        publish_mock.assert_not_awaited()
        self.mongo.update_one.assert_awaited_once()


    @patch("app.services.request_service.redis_client")
    async def test_existing_stream_message_id_found(self, mock_redis):
        mock_redis.get = AsyncMock(return_value="1234567890-0")
        mock_redis.xrange = AsyncMock(
            return_value=[("1234567890-0", {"request_id": "req-1"})]
        )

        result = await request_service._get_existing_stream_message_id("req-1")

        self.assertEqual(result, "1234567890-0")
        mock_redis.get.assert_awaited_once_with(
            "argus:dispatch:request:req-1:stream_message_id"
        )
        mock_redis.xrange.assert_awaited_once()

    @patch("app.services.request_service.redis_client")
    async def test_existing_stream_message_id_missing_from_stream(self, mock_redis):
        mock_redis.get = AsyncMock(return_value="1234567890-0")
        mock_redis.xrange = AsyncMock(return_value=[])

        result = await request_service._get_existing_stream_message_id("req-1")

        self.assertIsNone(result)

    @patch("app.services.request_service.redis_client")
    async def test_existing_stream_message_id_without_dedupe_key(self, mock_redis):
        mock_redis.get = AsyncMock(return_value=None)

        result = await request_service._get_existing_stream_message_id("req-1")

        self.assertIsNone(result)
        mock_redis.xrange.assert_not_awaited()


    async def test_missing_request_id_releases_recovery_claim(self):
        stale_request = {
            "_id": "mongo-id-missing-request",
            "status": "processing",
            "claimed_at": datetime.now(timezone.utc)
            - timedelta(seconds=300),
            "dispatched_at": None,
        }
        self.mongo.find_one_and_update.return_value = stale_request

        recovered_count = (
            await request_service.recover_stale_processing_requests(
                stale_after_seconds=120
            )
        )

        self.assertEqual(recovered_count, 0)
        self.mongo.update_one.assert_awaited_once()

        update_filter = self.mongo.update_one.await_args.args[0]
        update_doc = self.mongo.update_one.await_args.args[1]

        self.assertEqual(
            update_filter["_id"],
            "mongo-id-missing-request",
        )
        self.assertIn("$unset", update_doc)
        self.assertIn("recovery_claimed_at", update_doc["$unset"])
        self.assertIn("recovery_token", update_doc["$unset"])


if __name__ == "__main__":
    unittest.main()
