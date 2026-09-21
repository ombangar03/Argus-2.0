from typing import Any, Optional
from pydantic import BaseModel


class ConnectorRequest(BaseModel):
    request_id: str
    platform: str
    source_url: Optional[str] = None
    source_type: Optional[str] = None
    meta_data: dict[str, Any] = {}