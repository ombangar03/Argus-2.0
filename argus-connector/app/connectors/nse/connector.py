from app.schemas.connector import ConnectorRequest
from app.connectors.base import BaseConnector

from app.connectors.nse.models import NSEAnnouncement, NSECirculars
from app.connectors.nse.rss import fetch_nse_rss
from app.connectors.nse.circulars import fetch_nse_circulars

from app.connectors.rss.model import CrawlResult, RSSItem


class NSEConnector(BaseConnector):

    async def fetch(self, request: ConnectorRequest) -> CrawlResult:

        if request.source_type == "nse_announcements":
            return await self._fetch_announcements(request)
        
        if request.source_type == "nse_circular":
            return await self._fetch_circulars(request)

        raise ValueError(
            f"Unsupported NSE source_type: {request.source_type}"
            )

    async def _fetch_announcements(
        self,
        request: ConnectorRequest,
    ) -> CrawlResult:
        
        announcements = await fetch_nse_rss(
            request_id=request.request_id,
        )

        items = [
            self._announcement_to_rss_item(announcement)
            for announcement in announcements
        ]

        return CrawlResult(
            request_id = request.request_id,
            status="success",
            item_count = len(items),
            items=items,
        )

    #  circulars
    async def _fetch_circulars(
        self,
        request: ConnectorRequest,
    ) -> CrawlResult:

        from_date = request.metadata.get("from_date")
        to_date = request.metadata.get("to_date")

        if not from_date or not to_date:
            raise ValueError(
                "from_date and to_date are required for NSE circulars"
            )

        circulars = await fetch_nse_circulars(
            request_id=request.request_id,
            from_date=from_date,
            to_date=to_date,
        )

        items = [
            self._circular_to_rss_item(circular)
            for circular in circulars
        ]

        return CrawlResult(
            request_id = request.request_id,
            status="success",
            item_count = len(items),
            items=items,
        )
        

    @staticmethod
    def _announcement_to_rss_item(
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
    

    @staticmethod
    def _circular_to_rss_item(
        circular: NSECirculars,
    ) -> RSSItem:

        return RSSItem(
            request_id=circular.request_id,
            source=circular.source,
            source_type=circular.source_type,
            title=circular.subject or circular.display_number or "NSE Circular",
            link=circular.file_link or "",
            description=circular.subject,
            subject=circular.subject,
            published_at=circular.circular_date,
            metadata={
                "circular_number": circular.circular_number,
                "display_number": circular.display_number,
                "circular_display_date": circular.circular_display_date,
                "category": circular.category,
                "company": circular.company,
                "department": circular.department,
                "file_size": circular.file_size,
                "filename": circular.filename,
                "file_department": circular.file_department,
                "file_extension": circular.file_extension,
            },
        )