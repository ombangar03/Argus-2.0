
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

from app.schemas.result import CrawlResult, RSSItem
from app.services import result_service


class TestResultService(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.request_id = "test-request-001"

        self.item = RSSItem(
            request_id=self.request_id,
            source="bing",
            source_type="rss",
            title="Copper prices rise",
            link="https://example.com/copper-prices",
            summary="Copper prices increased today.",
        )

        self.success_result = CrawlResult(
            request_id=self.request_id,
            status="success",
            item_count=1,
            items=[self.item],
        )

    @patch("app.services.result_service.rss_requests")
    @patch("app.services.result_service.save_rss_items", new_callable=AsyncMock)
    async def test_successful_result_completes_request(
        self, mock_save_items, mock_requests
    ):
        mock_save_items.return_value = {
            "total": 1,
            "inserted": 1,
            "duplicates": 0,
        }
        mock_requests.update_one = AsyncMock(
            return_value=MagicMock(matched_count=1)
        )

        summary = await result_service.process_crawl_result(
            self.success_result
        )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["items_inserted"], 1)
        mock_save_items.assert_awaited_once_with([self.item])

        update_filter = mock_requests.update_one.await_args.args[0]
        update_data = mock_requests.update_one.await_args.args[1]

        self.assertEqual(update_filter["request_id"], self.request_id)
        self.assertEqual(update_data["$set"]["status"], "completed")

    @patch("app.services.result_service.rss_requests")
    async def test_failed_result_marks_request_failed(self, mock_requests):
        mock_requests.update_one = AsyncMock(
            return_value=MagicMock(matched_count=1)
        )

        failed_result = CrawlResult(
            request_id=self.request_id,
            status="failure",
            error_message="RSS feed could not be fetched",
        )

        summary = await result_service.process_crawl_result(failed_result)

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(
            summary["error_message"],
            "RSS feed could not be fetched",
        )

        update_data = mock_requests.update_one.await_args.args[1]
        self.assertEqual(update_data["$set"]["status"], "failed")

    @patch("app.services.result_service.rss_requests")
    @patch("app.services.result_service.save_rss_items", new_callable=AsyncMock)
    async def test_duplicate_result_does_not_insert_duplicate_articles(
        self, mock_save_items, mock_requests
    ):
        mock_save_items.return_value = {
            "total": 1,
            "inserted": 0,
            "duplicates": 1,
        }
        mock_requests.update_one = AsyncMock(
            return_value=MagicMock(matched_count=1)
        )

        summary = await result_service.process_crawl_result(
            self.success_result
        )

        self.assertEqual(summary["items_inserted"], 0)
        self.assertEqual(summary["items_duplicates"], 1)
        mock_save_items.assert_awaited_once_with([self.item])


    @patch("app.services.result_service.rss_requests")
    @patch("app.services.result_service.save_rss_items", new_callable=AsyncMock)
    async def test_unknown_request_is_rejected(
        self, mock_save_items, mock_requests
    ):
        mock_save_items.return_value = {
            "total": 1,
            "inserted": 1,
            "duplicates": 0,
        }
        mock_requests.update_one = AsyncMock(
            return_value=MagicMock(matched_count=0)
        )

        with self.assertRaisesRegex(ValueError, "not found"):
            await result_service.process_crawl_result(
                self.success_result
            )


    @patch("app.services.result_service.rss_requests")
    @patch(
        "app.services.result_service.save_rss_items",
        new_callable=AsyncMock,
    )
    async def test_status_update_failure_propagates(
        self, mock_save_items, mock_requests
    ):
        mock_save_items.return_value = {
            "total": 1,
            "inserted": 1,
            "duplicates": 0,
        }

        mock_requests.update_one = AsyncMock(
            side_effect=RuntimeError("MongoDB unavailable")
        )

        with self.assertRaisesRegex(
            RuntimeError, "MongoDB unavailable"
        ):
            await result_service.process_crawl_result(
                self.success_result
            )

        mock_save_items.assert_awaited_once_with([self.item])
        mock_requests.update_one.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
