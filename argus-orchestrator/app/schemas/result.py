import hashlib
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel, Field


class RSSItem(BaseModel):
    """
    Represents a single parsed article from an RSS feed.
    This is what gets saved into MongoDB's 'rss_items' collection.
    """

    request_id: str
    title: str
    link: str
    summary: Optional[str] = None
    author: Optional[str] = None
    published_at: Optional[datetime] = None
    item_hash: Optional[str] = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def compute_hash(self) -> str:
        """
        Generates a unique SHA-256 hash using the article's link.
        Used as a unique index in MongoDB to prevent inserting duplicate articles.
        """
        raw = self.link.strip().lower()
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def model_post_init(self, __context):
        """
        Automatically compute item_hash if not explicitly provided.
        """
        if not self.item_hash and self.link:
            self.item_hash = self.compute_hash()


class CrawlResult(BaseModel):
    """
    Represents the full result payload coming back from the connector via
    Redis Stream 'connector:rss:results'.
    """

    request_id: str
    status: str = "success"  # "success" or "failed"
    items_count: int = 0
    items: List[RSSItem] = []
    error_message: Optional[str] = None
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )



# if __name__ == "__main__":
#     print("=======================================")
#     print("Testing RSSItem & CrawlResult schemas...")
#     print("=======================================")

#     # 1. Test RSSItem creation with auto-hash
#     sample_item = RSSItem(
#         request_id="req_test_123",
#         title="Aluminium Prices Surge Amid Supply Disruption",
#         link="https://agmetalminer.com/2026/09/08/aluminium-prices-surge",
#         summary="Global aluminium spot prices rose by 3.2% this morning.",
#         author="John Doe"
#     )

#     print("[OK] Created RSSItem:")
#     print(f"  Title: {sample_item.title}")
#     print(f"  Link: {sample_item.link}")
#     print(f"  Auto-computed item_hash: {sample_item.item_hash}")
#     print(f"  Created At: {sample_item.created_at}")

#     # 2. Test CrawlResult wrapping items
#     result = CrawlResult(
#         request_id="req_test_123",
#         status="success",
#         items_count=1,
#         items=[sample_item]
#     )

#     print("\n[OK] Created CrawlResult:")
#     print(f"  Request ID: {result.request_id}")
#     print(f"  Status: {result.status}")
#     print(f"  Items Count: {len(result.items)}")
#     print(f"  First Article Title: {result.items[0].title}")
#     print("\nAll schema validations passed successfully!")