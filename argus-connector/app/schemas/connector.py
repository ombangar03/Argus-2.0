from pydantic import Field
from typing import Any, Optional
from pydantic import BaseModel


class ConnectorRequest(BaseModel):
    request_id: str
    source: str
    source_type: Optional[str] = None
    source_url: Optional[str] = None    
    metadata: dict[str, Any] = Field(default_factory=dict)