from app.connectors.base import BaseConnector
from app.connectors.rss.parser import fetch_and_parse_rss

class RSSConnector(BaseConnector):
    
    async def fetch(self, request):
        return await fetch_and_parse_rss(
            request_id=request.request_id,
            source=request.source,
            source_type=request.source_type,
            url=request.source_url,
        )