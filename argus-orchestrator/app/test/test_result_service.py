
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
    @patch("app.services.result_service.filter_unseen_items", new_callable=AsyncMock)
    @patch("app.services.result_service.mark_items_seen", new_callable=AsyncMock)
    async def test_successful_result_completes_request(
        self,
        mock_mark_seen,
        mock_filter_items,
        mock_save_items,
        mock_requests,
    ):
        mock_filter_items.return_value = ([self.item], 0, 0)
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
        self.assertEqual(summary["items_duplicates"], 0)

        mock_filter_items.assert_awaited_once_with([self.item])
        mock_save_items.assert_awaited_once_with([self.item])
        mock_mark_seen.assert_awaited_once_with([self.item])

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
    @patch("app.services.result_service.filter_unseen_items", new_callable=AsyncMock)
    @patch("app.services.result_service.mark_items_seen", new_callable=AsyncMock)
    async def test_duplicate_result_does_not_insert_duplicate_articles(
        self,
        mock_mark_seen,
        mock_filter_items,
        mock_save_items,
        mock_requests,
    ):
        mock_filter_items.return_value = ([], 1, 0)
        mock_save_items.return_value = {
            "total": 0,
            "inserted": 0,
            "duplicates": 0,
        }
        mock_requests.update_one = AsyncMock(
            return_value=MagicMock(matched_count=1)
        )

        summary = await result_service.process_crawl_result(
            self.success_result
        )

        self.assertEqual(summary["items_inserted"], 0)
        self.assertEqual(summary["items_duplicates"], 1)
        mock_save_items.assert_awaited_once_with([])
        mock_mark_seen.assert_awaited_once_with([])

    @patch("app.services.result_service.rss_requests")
    @patch("app.services.result_service.save_rss_items", new_callable=AsyncMock)
    @patch("app.services.result_service.filter_unseen_items", new_callable=AsyncMock)
    @patch("app.services.result_service.mark_items_seen", new_callable=AsyncMock)
    async def test_unknown_request_is_rejected(
        self,
        mock_mark_seen,
        mock_filter_items,
        mock_save_items,
        mock_requests,
    ):
        mock_filter_items.return_value = ([self.item], 0, 0)
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

        mock_filter_items.assert_awaited_once_with([self.item])
        mock_save_items.assert_awaited_once_with([self.item])
        mock_mark_seen.assert_awaited_once_with([self.item])

    @patch("app.services.result_service.rss_requests")
    @patch("app.services.result_service.save_rss_items", new_callable=AsyncMock)
    @patch("app.services.result_service.filter_unseen_items", new_callable=AsyncMock)
    @patch("app.services.result_service.mark_items_seen", new_callable=AsyncMock)
    async def test_status_update_failure_propagates(
        self,
        mock_mark_seen,
        mock_filter_items,
        mock_save_items,
        mock_requests,
    ):
        mock_filter_items.return_value = ([self.item], 0, 0)
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

        mock_filter_items.assert_awaited_once_with([self.item])
        mock_save_items.assert_awaited_once_with([self.item])
        mock_mark_seen.assert_awaited_once_with([self.item])
        mock_requests.update_one.assert_awaited_once()

    @patch("app.services.result_service.redis_client")
    async def test_filter_unseen_items_removes_cached_articles(self, mock_redis):
        mock_redis.mget = AsyncMock(return_value=["1", None])

        second_item = RSSItem(
            request_id=self.request_id,
            source="bing",
            source_type="rss",
            title="Gold prices rise",
            link="https://example.com/gold-prices",
            summary="Gold prices increased today.",
        )

        unseen, redis_duplicates, batch_duplicates = (
            await result_service.filter_unseen_items([self.item, second_item])
        )

        self.assertEqual(unseen, [second_item])
        self.assertEqual(redis_duplicates, 1)
        self.assertEqual(batch_duplicates, 0)
        mock_redis.mget.assert_awaited_once()


    @patch("app.services.result_service.redis_client")
    async def test_filter_unseen_items_removes_duplicates_within_batch(self, mock_redis):
        mock_redis.mget = AsyncMock(return_value=[None])

        unseen, redis_duplicates, batch_duplicates = (
            await result_service.filter_unseen_items([self.item, self.item])
        )

        self.assertEqual(unseen, [self.item])
        self.assertEqual(redis_duplicates, 0)
        self.assertEqual(batch_duplicates, 1)
        mock_redis.mget.assert_awaited_once()


    @patch("app.services.result_service.redis_client")
    async def test_filter_unseen_items_falls_back_when_redis_fails(self, mock_redis):
        mock_redis.mget = AsyncMock(side_effect=ConnectionError("Redis unavailable"))

        unseen, redis_duplicates, batch_duplicates = (
            await result_service.filter_unseen_items([self.item])
        )

        self.assertEqual(unseen, [self.item])
        self.assertEqual(redis_duplicates, 0)
        self.assertEqual(batch_duplicates, 0)


    @patch("app.services.result_service.redis_client")
    async def test_mark_items_seen_sets_ttl(self, mock_redis):
        mock_pipeline = MagicMock()
        mock_pipeline.set = MagicMock()
        mock_pipeline.execute = AsyncMock()
        mock_pipeline.__aenter__ = AsyncMock(return_value=mock_pipeline)
        mock_pipeline.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pipeline = MagicMock(return_value=mock_pipeline)

        await result_service.mark_items_seen([self.item])

        mock_pipeline.set.assert_called_once_with(
            f"{result_service.ARTICLE_SEEN_PREFIX}{self.item.item_hash}",
            "1",
            ex=result_service.ARTICLE_SEEN_TTL_SECONDS,
        )
        mock_pipeline.execute.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
