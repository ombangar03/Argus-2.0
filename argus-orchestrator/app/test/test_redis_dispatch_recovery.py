
import unittest
import uuid

import redis.asyncio as redis

from app.services.request_service import _IDEMPOTENT_XADD_SCRIPT


class TestRedisDispatchRecovery(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.redis = redis.Redis(
            host="localhost",
            port=6380,
            decode_responses=True,
        )

        await self.redis.ping()

        # Unique names prevent collisions with other test runs.
        suffix = uuid.uuid4().hex
        self.stream_key = f"test:queue:connector:rss:{suffix}"
        self.dedupe_key = f"test:argus:dispatch:{suffix}"

        self.payload = {
            "request_id": f"test-request-{suffix}",
            "source": "test",
            "source_type": "rss",
            "source_url": "https://example.com/test-rss",
        }

    async def asyncTearDown(self):
        await self.redis.delete(self.stream_key, self.dedupe_key)
        await self.redis.aclose()

    async def publish(self):
        fields = []
        for key, value in self.payload.items():
            fields.extend((key, value))

        return await self.redis.eval(
            _IDEMPOTENT_XADD_SCRIPT,
            2,
            self.stream_key,
            self.dedupe_key,
            *fields,
        )

    async def test_deleted_stream_entry_is_republished_once(self):
        # 1. Publish the original message.
        original_id = await self.publish()

        self.assertTrue(
            await self.redis.xrange(
                self.stream_key,
                min=original_id,
                max=original_id,
            )
        )

        # 2. Delete the stream entry but retain its deduplication key.
        await self.redis.xdel(self.stream_key, original_id)

        self.assertEqual(
            await self.redis.get(self.dedupe_key),
            original_id,
        )
        self.assertEqual(await self.redis.xlen(self.stream_key), 0)

        # 3. Retry: a replacement message should be published.
        replacement_id = await self.publish()

        self.assertNotEqual(replacement_id, original_id)
        self.assertEqual(
            await self.redis.get(self.dedupe_key),
            replacement_id,
        )
        self.assertEqual(await self.redis.xlen(self.stream_key), 1)

        # 4. Retry again: reuse the replacement without duplication.
        retry_id = await self.publish()

        self.assertEqual(retry_id, replacement_id)
        self.assertEqual(await self.redis.xlen(self.stream_key), 1)


if __name__ == "__main__":
    unittest.main()
