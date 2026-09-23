from _typeshed import structseq
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class NewQuery(BaseModel):
    source: str = "bing"
    source_type: str = "rss"

    topic: str
    query: str

    enabled: bool = True
    priority: str = "HIGH"

    metadata: dict[str, Any] = Field(default_factory=dict)

    created_at: datetime =  Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    