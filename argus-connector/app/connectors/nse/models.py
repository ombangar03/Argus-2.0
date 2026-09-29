# convert raw nse announcements data into a predictable python structure. 
from datetime import datetime, timezone
from typing import Optional 

from pydantic import BaseModel, Field

class NSEAnnouncement(BaseModel):
    request_id: str

    source: str
    source_type: str

    title: str
    link: Optional[str] = None
    description: Optional[str] = None
    subject: Optional[str] = None

    published_at: Optional[datetime] = None

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

