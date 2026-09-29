from app.schemas.connector import ConnectorRequest
from app.connectors.base import BaseConnector
from app.connectors.nse.models import NSEAnnouncement
from app.connectors.nse.rss import fetch_nse_rss
from app.connectors.rss.model import CrawlResult, RSSItem


class NSEConnector(BaseConnector):

    async def fetch(self, request: ConnectorRequest) -> CrawlResult:

        announcements = await fetch_nse_rss(
            request_id = request.request_id
        )

        items = [
            self._to_rss_item(announcement)
            for announcement in announcements
        ]

        return CrawlResult(
            request_id=request.request_id,
            status = "success",
            item_count = len(items),
            items = items,
        )

    @staticmethod
    def _to_rss_item(
        announcement: NSEAnnouncement,
    ) -> RSSItem:
        return RSSItem(
            request_id=announcement.request_id,
            source=announcement.source,
            source_type=announcement.source_type,
            title = announcement.title,
            link = announcement.link or "",
            description= announcement.description,
            subject= announcement.subject,
            published_at= announcement.published_at

        )
