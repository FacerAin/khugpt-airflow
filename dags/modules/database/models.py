import uuid

from pydantic import BaseModel, Field


class PageDocument(BaseModel):
    id: str = Field(default_factory=uuid.uuid4, alias="_id")
    page_content: str
    page_url: str
    last_update: str
    metadata: dict = Field(default_factory=dict)
