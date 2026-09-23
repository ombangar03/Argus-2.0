from typing import Any
from pydantic import BaseModel, Field

class ConnectorRequest(BaseModel):
     
    request_id: str
    source: str
    source_type: str
    source_url: str
    metadata: dict[str, Any] = Field(
        default_factory=dict
    )