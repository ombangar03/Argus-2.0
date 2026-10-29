
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from app.services import query_dispatch_service


class TestDispatchNewsQueries(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.news_queries_patch = patch.object(
            query_dispatch_service,
            "news_queries",
        )
        self.rss_requests_patch = patch.object(
            query_dispatch_service,
            "rss_requests",
        )
        self.build_url_patch = patch.object(
            query_dispatch_service,
            "build_bing_rss_url",
            return_value="https://example.com/feed",
        )

        self.news_queries = self.news_queries_patch.start()
        self.rss_requests = self.rss_requests_patch.start()
        self.build_url = self.build_url_patch.start()

        self.addCleanup(self.news_queries_patch.stop)
        self.addCleanup(self.rss_requests_patch.stop)
        self.addCleanup(self.build_url_patch.stop)

        self.rss_requests.find_one = AsyncMock(return_value=None)
        self.rss_requests.insert_one = AsyncMock()

    def set_queries(self, documents):
        async def iterate_documents():
            for document in documents:
                yield document

        self.news_queries.find.return_value = iterate_documents()

    async def test_valid_query_creates_request(self):
        self.set_queries([
            {
                "query": "gold prices",
                "source": "bing",
                "source_type": "rss",
                "topic": "commodities",
                "enabled": True,
            }
        ])

        result = await query_dispatch_service.dispatch_news_queries()

        self.assertEqual(result, {"created": 1, "skipped": 0})
        self.rss_requests.insert_one.assert_awaited_once()

        request_doc = self.rss_requests.insert_one.await_args.args[0]
        self.assertEqual(request_doc["source"], "bing")
        self.assertEqual(request_doc["source_type"], "rss")
        self.assertEqual(request_doc["status"], "pending")
        self.assertEqual(request_doc["metadata"]["query"], "gold prices")

    async def test_missing_source_is_skipped(self):
        self.set_queries([
            {
                "query": "gold prices",
                "source_type": "rss",
                "enabled": True,
            }
        ])

        result = await query_dispatch_service.dispatch_news_queries()

        self.assertEqual(result, {"created": 0, "skipped": 1})
        self.rss_requests.insert_one.assert_not_awaited()

    async def test_missing_source_type_is_skipped(self):
        self.set_queries([
            {
                "query": "gold prices",
                "source": "bing",
                "enabled": True,
            }
        ])

        result = await query_dispatch_service.dispatch_news_queries()

        self.assertEqual(result, {"created": 0, "skipped": 1})
        self.rss_requests.insert_one.assert_not_awaited()

    async def test_blank_source_is_skipped(self):
        self.set_queries([
            {
                "query": "gold prices",
                "source": "   ",
                "source_type": "rss",
                "enabled": True,
            }
        ])

        result = await query_dispatch_service.dispatch_news_queries()

        self.assertEqual(result, {"created": 0, "skipped": 1})
        self.rss_requests.insert_one.assert_not_awaited()

    async def test_blank_source_type_is_skipped(self):
        self.set_queries([
            {
                "query": "gold prices",
                "source": "bing",
                "source_type": " ",
                "enabled": True,
            }
        ])

        result = await query_dispatch_service.dispatch_news_queries()

        self.assertEqual(result, {"created": 0, "skipped": 1})
        self.rss_requests.insert_one.assert_not_awaited()

    async def test_missing_query_is_skipped(self):
        self.set_queries([
            {
                "source": "bing",
                "source_type": "rss",
                "enabled": True,
            }
        ])

        result = await query_dispatch_service.dispatch_news_queries()

        self.assertEqual(result, {"created": 0, "skipped": 1})
        self.rss_requests.insert_one.assert_not_awaited()

    async def test_blank_query_is_skipped(self):
        self.set_queries([
            {
                "query": "   ",
                "source": "bing",
                "source_type": "rss",
                "enabled": True,
            }
        ])

        result = await query_dispatch_service.dispatch_news_queries()

        self.assertEqual(result, {"created": 0, "skipped": 1})
        self.rss_requests.insert_one.assert_not_awaited()

    async def test_invalid_query_does_not_block_next_valid_query(self):
        self.set_queries([
            {
                "query": "invalid first query",
                "source_type": "rss",
                "enabled": True,
            },
            {
                "query": "silver prices",
                "source": "bing",
                "source_type": "rss",
                "enabled": True,
            },
        ])

        result = await query_dispatch_service.dispatch_news_queries()

        self.assertEqual(result, {"created": 1, "skipped": 1})
        self.rss_requests.insert_one.assert_awaited_once()

        request_doc = self.rss_requests.insert_one.await_args.args[0]
        self.assertEqual(request_doc["metadata"]["query"], "silver prices")

    async def test_existing_active_request_is_skipped(self):
        self.set_queries([
            {
                "query": "gold prices",
                "source": "bing",
                "source_type": "rss",
                "enabled": True,
            }
        ])
        self.rss_requests.find_one.return_value = {
            "request_id": "existing-request",
            "status": "pending",
        }

        result = await query_dispatch_service.dispatch_news_queries()

        self.assertEqual(result, {"created": 0, "skipped": 1})
        self.rss_requests.insert_one.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
git diff -- app/services/query_dispatch_service.py