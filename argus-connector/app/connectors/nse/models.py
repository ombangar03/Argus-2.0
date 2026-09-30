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


class NSECirculars(BaseModel):
    request_id: str

    source: str
    source_type: str

    circular_date: Optional[datetime] = None
    circular_display_date: Optional[str] = None

    category: Optional[str] = None
    company: Optional[str] = None
    department: Optional[str] = None

    display_number: Optional[str] = None
    circular_number: Optional[str] = None

    subject: Optional[str] = None

    file_link: Optional[str] = None
    file_size: Optional[str] = None
    filename: Optional[str] = None
    file_department: Optional[str] = None
    file_extension: Optional[str] = None

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
