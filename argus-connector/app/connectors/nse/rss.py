import logging 
import httpx

from app.connectors.nse.models import NSEAnnouncement
from app.connectors.nse.parser import parse_nse_rss


logger = logging.getLogger(__name__)

NSE_RSS_URL = (
    "https://nsearchives.nseindia.com//content/RSS/Online_announcements.xml"
)

async def fetch_nse_rss(
    request_id: str,
) -> list[NSEAnnouncement]:

    headers =  {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/154.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "application/rss+xml, "
            "application/xml, "
            "text/xml, "
            "*/*"
        ),
    }

    async with httpx.AsyncClient(
        headers=headers,
        timeout=30.0,
        follow_redirects=True,
    )as client:

        response = await client.get(NSE_RSS_URL)
        print(response.status_code)
        print(response.url)

        response.raise_for_status()

        logger.info(
            "NSE RSS fetched successfully. Status: %s",
            response.status_code,
        )

        announcements = parse_nse_rss(
            xml_content=response.text,
            request_id=request_id,
        )

        logger.info(
            "NSE RSS parsed successfully. Announcements: %d",
            len(announcements),
        )

        return announcements