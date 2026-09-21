from pydantic._internal._signature import _HAS_DEFAULT_FACTORY
import hashlib
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel, Field


class RSSItems(BaseModel):
    """Represents a single parsed article from an Rss Feed."""
    request_id: str
    title: str
    link: str
    author: Optional[str] = None
    published_at: Optional[datetime] = None
    summary: Optional[str] = None
    item_hash: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


    def compute_hash(self) -> str:
        raw = self.link.strip().lower()
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()  #generates the SHA-256 fingerprint.


    def model_post_init(self, __conetext):
        if not self.item_hash and self.link:
            self.item_hash = self.compute_hash()


class CrawlResult(BaseModel):
    """Full result payload published to 'connector:rss:results'"""
    request_id: str
    status: str = "success"   #success or failed
    items_count: int = 0
    items: List[RSSItems] = []
    error_message: Optional[str] = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# if __name__ == "__main__":
#     print("\nTesting RSS items and Crawl Result--")
#     test_item = RSSItems(
#         request_id="req_test_1",
#         title="breaking tech news",
#         link="https://example.com/tech-news-2026",
#         summary="A new breakthrough was announced.",
#         author="Ravi bhale"
#     )

#     print("1. Created RSSitem:")
#     print(f"title: {test_item.title}")
#     print(f"link: {test_item.link}")
#     print(f"hash: {test_item.item_hash}")

#     test_result = CrawlResult(
#         request_id="req_test_1",
#         status="success",
#         items_count=1,
#         items=[test_item]
#     )
#     print("\n2. Created CrawlResult:")
#     print(f"   Request ID: {test_result.request_id}")
#     print(f"   Items Count: {test_result.items_count}")
#     print(f"   Status: {test_result.status}")
#     print("\n[SUCCESS] Model schema is working perfectly!\n")
    

