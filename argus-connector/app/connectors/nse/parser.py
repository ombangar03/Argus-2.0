from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
from xml.etree import ElementTree as ET 

from app.connectors.nse.models import NSEAnnouncement


def parse_subject(description: Optional[str]) -> Optional[str]:
    if not description:
        return None

    marker = "|SUBJECT:"

    if marker not in description:
        return None 

    subject = description.split(marker, 1)[1].strip()
    return subject or None


def parse_published_at(pub_date: Optional[str]) -> Optional[datetime]:

    if not pub_date:
        return None

    try:
        parsed_date = datetime.strptime(
            pub_date.strip(),
            "%d-%b-%Y %H:%M:%S"
        )

        return parsed_date.replace(tzinfo=timezone.utc)
    
    except ValueError:
        return None


def parse_nse_rss(
    xml_content: str,
    request_id: str,
) -> list[NSEAnnouncement]:

    root = ET.fromstring(xml_content)

    announcements = []

    for item in root.findall(".//item"):
        title = item.findtext("title")
        link = item.findtext("link")
        description = item.findtext("description")
        pub_date = item.findtext("pubDate")

        announcement = NSEAnnouncement(
            request_id = request_id,
            source="nse",
            source_type="nse_announcements",
            title = (title or "").strip() or None,
            link = (link or "").strip() or None,
            description = (description or "").strip() or None,
            subject = parse_subject(description),
            published_at=parse_published_at(pub_date),
        )

        announcements.append(announcement)

    return announcements

