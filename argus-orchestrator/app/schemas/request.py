from pydantic import BaseModel, Field 

class RSSRequests(BaseModel):
    request_id: str
    url: str
    platform: str = Field(default="rss")