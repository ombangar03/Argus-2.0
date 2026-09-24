from httpx import get
import logging
from datetime import datetime, timezone
from time import mktime
from typing import Optional
import feedparser
import httpx

from app.connectors.rss.model import RSSItem, CrawlResult

logger = logging.getLogger(__name__)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Argus/2.0"

def _parse_published_date(entry)-> Optional[datetime]:
    """Extracts and normalizes published tiemstamp from a feedparser entry."""
    time_struct = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)

    if time_struct:
        try:
            return datetime.fromtimestamp(mktime(time_struct), tz=timezone.utc)
        except Exception: 
            pass 
    
    return None


async def fetch_and_parse_rss(request_id: str, source: str, source_type: str, url: str) -> CrawlResult:
    """Downloads an RSS feed Asynchronously, parses articles, and returns a 
    CrawlResult model.
    """

    logger.info(f"Fetching RSS feed for request {request_id}: {url}")

    try:
        # fetch feed xml with httpx
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
            timeout=10
        ) as client:
            response = await client.get(url, timeout=10)
            response.raise_for_status()
            content = response.text

        feed = feedparser.parse(content)
        if feed.bozo and not feed.entries:
            # bozo flag is set when xml has syntax errors and no entries could be parsed
            error_detail =  str(getattr(feed, 
            "bozo_exception", 
            "Malformed XML")
        )
            
            logger.warning(f"Feed parser error for {url}: {error_detail}")
            return CrawlResult(
                request_id=request_id,
                status="failed",
                error_message=f"Invalid RSS feed forma: {error_detail}"
            )

        items = []
        for entry in feed.entries:
            logger.info(
                "RSS Entry | title=%s | link=%s | published=%s",
                getattr(entry, "title", None),
                getattr(entry, "link", None),
                getattr(entry, "published", None),
            )
            
            title = getattr(entry, "title", "").strip()
            link = getattr(entry, "link", "").strip()

            if not title or not link:
                continue

            summary = getattr(entry, "summary", None) or getattr(entry, "description", None)
            summary = summary.strip() if summary else None

            author = getattr(
                entry, "author", None) or getattr(entry, "author_detail", {}).get("name", None
            )
            published_at = _parse_published_date(entry)

            item = RSSItem(
                request_id=request_id,
                source=source,
                source_type=source_type,
                
                title=title,
                link=link,
                summary=summary,

                author=author,
                published_at=published_at
            )

            items.append(item)

        logger.info(f"Successfully prased {len(items)} items for reqeust {request_id}")

        return CrawlResult(
            request_id=request_id,
            status="success",
            item_count=len(items),
            items=items
        )

    except httpx.HTTPStatusError as exc:
        error_msg = f"HTTP error {exc.response.status_code}: {exc.response.reason_phrase}"
        logger.error(f"HTTP fetch failed for {url}: {error_msg}")
        return CrawlResult(
            request_id=request_id,
            status="failed",
            error_message=error_msg
        )

    except Exception as exc:
        error_msg = f"Unexpected error during feed processing: {str(exc)}"
        logger.exception(error_msg)
        return CrawlResult(
            request_id=request_id,
            status="failed",
            error_message=error_msg
        )


    
# if __name__ == "__main__":
#     import asyncio
#     from app.core.logging import setup_logging
#     async def _test():
#         setup_logging()
#         print("\n==========================================")
#         print("  TESTING PARSER ON LIVE FEED (REAL HTTP) ")
#         print("==========================================")
#         test_url = "https://agmetalminer.com/feed/"
#         print(f"Connecting to: {test_url}\n")
#         result = await fetch_and_parse_rss(request_id="req_live_test", url=test_url)
#         print(f"Result Status: {result.status}")
#         print(f"Total Items:   {result.items_count}")
#         if result.items:
#             print("\nPreviewing First 2 Live Articles:")
#             for i, item in enumerate(result.items[:2], 1):
#                 print(f"\n--- Article {i} ---")
#                 print(f"Title:        {item.title}")
#                 print(f"Link:         {item.url}")
#                 print(f"Item Hash:    {item.item_hash}")
#                 print(f"Published At: {item.published_at}")
#         else:
#             print(f"Error: {result.error_message}")
#         print("\n[SUCCESS] Parser live test completed!\n")
#     asyncio.run(_test())
        

        