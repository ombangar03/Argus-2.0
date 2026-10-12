
import unittest
from unittest.mock import AsyncMock, patch

from pymongo.errors import DuplicateKeyError

from app.services.scheduled_request_service import (
    create_scheduled_rss_request,
)


class TestScheduledRequestService(unittest.IsolatedAsyncioTestCase):

    async def test_creates_new_scheduled_request(self):
        with (
            patch(
                "app.services.scheduled_request_service.rss_requests"
            ) as mock_collection,
            patch(
                "app.services.scheduled_request_service.build_bing_rss_url",
                return_value="https://example.com/rss",
            ),
        ):
            mock_collection.insert_one = AsyncMock()

            result = await create_scheduled_rss_request(
                unit_id="unit-001",
                term="gold price",
                scheduled_for=1791800000.0,
                priority="normal",
                org_id="org-001",
                search_options={"region": "IN"},
            )

            self.assertTrue(result["created"])
            self.assertEqual(
                result["request_id"],
                "scheduled:unit-001:1791800000.000000",
            )

            request_doc = mock_collection.insert_one.await_args.args[0]

            self.assertEqual(request_doc["status"], "pending")
            self.assertEqual(request_doc["source"], "bing")
            self.assertEqual(request_doc["source_type"], "rss")
            self.assertEqual(request_doc["url"], "https://example.com/rss")
            self.assertEqual(request_doc["priority"], "NORMAL")
            self.assertEqual(
                request_doc["metadata"]["unit_id"],
                "unit-001",
            )
            self.assertNotIn("active_query_key", request_doc)

    async def test_existing_request_is_treated_as_retry(self):
        with patch(
            "app.services.scheduled_request_service.rss_requests"
        ) as mock_collection:
            mock_collection.insert_one = AsyncMock(
                side_effect=DuplicateKeyError("duplicate")
            )
            mock_collection.find_one = AsyncMock(
                return_value={"request_id": "scheduled:unit-001:1791800000.000000"}
            )

            result = await create_scheduled_rss_request(
                unit_id="unit-001",
                term="gold price",
                scheduled_for=1791800000.0,
                priority="NORMAL",
            )

            self.assertFalse(result["created"])
            mock_collection.insert_one.assert_awaited_once()
            mock_collection.find_one.assert_awaited_once()

    async def test_unmatched_duplicate_error_is_raised(self):
        with patch(
            "app.services.scheduled_request_service.rss_requests"
        ) as mock_collection:
            mock_collection.insert_one = AsyncMock(
                side_effect=DuplicateKeyError("duplicate")
            )
            mock_collection.find_one = AsyncMock(return_value=None)

            with self.assertRaises(DuplicateKeyError):
                await create_scheduled_rss_request(
                    unit_id="unit-001",
                    term="gold price",
                    scheduled_for=1791800000.0,
                    priority="NORMAL",
                )


if __name__ == "__main__":
    unittest.main()
