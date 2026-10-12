
from app.schemas import result
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from app.services import request_service


class TestDispatchRequest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.request_id = "scheduled:unit-001:1900.000000"
        self.document_id = "mock-mongo-document-id"

        self.scheduled_document = {
            "_id": self.document_id,
            "request_id": self.request_id,
            "source": "bing",
            "source_type": "rss",
            "url": "https://example.com/rss?q=copper",
            "metadata": {
                "query": "copper prices",
                "topic": None,
                "unit_id": "unit-001",
                "org_id": "test-org",
                "search_options": {"language": "en"},
                "scheduled_for": 1900.0,
            },
            "status": "pending",
        }

        self.mock_mongodb = patch.object(
            request_service,
            "rss_requests",
        )
        self.mock_redis = patch.object(
            request_service,
            "redis_client",
        )

        self.mongo = self.mock_mongodb.start()
        self.redis = self.mock_redis.start()

        self.addCleanup(self.mock_mongodb.stop)
        self.addCleanup(self.mock_redis.stop)

        self.mongo.find_one_and_update = AsyncMock(
            return_value=self.scheduled_document.copy()
        )
        self.mongo.update_one = AsyncMock()
        self.redis.eval = AsyncMock(return_value="1234567890-0")

    async def test_scheduled_request_is_published_to_configured_stream(self):
        result = await request_service.dispatch_request()



        self.assertIsNotNone(result)
        self.assertEqual(result.request_id, self.request_id)
        self.assertEqual(result.source, "bing")
        self.assertEqual(result.source_type, "rss")
        self.assertEqual(
            result.source_url,
            self.scheduled_document["url"],
        )

        self.mongo.find_one_and_update.assert_awaited_once()

        claim_call = self.mongo.find_one_and_update.await_args

        self.assertEqual(
            claim_call.args[0],
            {"status": "pending"},
        )

        update_document = claim_call.args[1]

        self.assertEqual(
            update_document["$set"]["status"],
            "processing",
        )

        self.assertEqual(
            claim_call.kwargs["return_document"],
            request_service.ReturnDocument.AFTER,
        )


        self.redis.eval.assert_awaited_once()

        eval_args = self.redis.eval.await_args.args

        script = eval_args[0]
        num_keys = eval_args[1]
        stream_name = eval_args[2]
        dedupe_key = eval_args[3]
        field_args = eval_args[4:]

        self.assertEqual(num_keys, 2)
        self.assertEqual(stream_name, request_service.settings.connector_stream)
        self.assertEqual(stream_name, "queue:connector:rss")
        self.assertEqual(
            dedupe_key,
            f"argus:dispatch:request:{self.request_id}:stream_message_id",
        )

        self.assertIn("XADD", script)
        self.assertIn("GET", script)
        self.assertIn("SET", script)

        payload = dict(zip(field_args[::2], field_args[1::2]))

        self.assertEqual(payload["request_id"], self.request_id)
        self.assertEqual(payload["source"], "bing")
        self.assertEqual(payload["source_type"], "rss")
        self.assertEqual(
            payload["source_url"],
            self.scheduled_document["url"],
        )

        decoded_metadata = json.loads(payload["metadata"])


        self.mongo.update_one.assert_awaited_once()

        update_call = self.mongo.update_one.await_args
        self.assertEqual(
            update_call.args[0],
            {"_id": self.document_id},
        )

        audit_update = update_call.args[1]["$set"]
        self.assertEqual(
            audit_update["stream_message_id"],
            "1234567890-0",
        )
        self.assertIsInstance(
            audit_update["dispatched_at"],
            datetime,
        )
        self.assertIsInstance(
            audit_update["updated_at"],
            datetime,
        )

        # print(
        #     "PASS: scheduled request was published to the configured stream"
        # )
        # print("PASS: request metadata was serialized correctly")
        # print("PASS: MongoDB dispatch audit fields were updated")


    async def test_audit_failure_does_not_reset_request_to_pending(self):
        # Redis accepts the message, but MongoDB audit update fails.
        self.mongo.update_one.side_effect = RuntimeError(
            "MongoDB audit write failed"
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "MongoDB audit write failed",
        ):
            await request_service.dispatch_request()

        # The message must have been published exactly once.
        self.redis.eval.assert_awaited_once()

        # The failed audit update must not trigger a reset to pending.
        self.mongo.update_one.assert_awaited_once()
        update_call = self.mongo.update_one.await_args
        audit_update = update_call.args[1]["$set"]

        self.assertNotIn("status", audit_update)
        self.assertIn("stream_message_id", audit_update)

        # print(
        #     "PASS: audit failure does not reset the request to pending"
        # )


    async def test_no_pending_request_returns_none(self):
        self.mongo.find_one_and_update.return_value = None

        result = await request_service.dispatch_request()

        self.assertIsNone(result)
        self.redis.eval.assert_not_awaited()
        self.mongo.update_one.assert_not_awaited()

        # print("PASS: no pending request produces no Redis message")


    async def test_retry_uses_the_same_deduplication_key(self):
        payload = {
            "request_id": self.request_id,
            "source": "bing",
            "source_type": "rss",
            "source_url": self.scheduled_document["url"],
            "metadata": json.dumps(self.scheduled_document["metadata"]),
        }

        first_id = await request_service._publish_request_idempotently(
            self.request_id,
            payload,
        )
        second_id = await request_service._publish_request_idempotently(
            self.request_id,
            payload,
        )

        self.assertEqual(first_id, second_id)
        self.assertEqual(self.redis.eval.await_count, 2)

        first_call = self.redis.eval.await_args_list[0].args
        second_call = self.redis.eval.await_args_list[1].args

        self.assertEqual(first_call[3], second_call[3])
        self.assertEqual(first_call[0], second_call[0])

        print("PASS: retries use the same request ID and Lua script")


if __name__ == "__main__":
    unittest.main()
