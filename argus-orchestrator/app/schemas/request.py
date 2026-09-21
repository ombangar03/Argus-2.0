from pydantic import BaseModel, Field 

class ConnectorRequest(BaseModel):
    request_id: str
    url: str
    platform: str = Field(default="rss")