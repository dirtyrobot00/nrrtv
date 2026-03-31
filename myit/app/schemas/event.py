"""
이벤트 Pydantic 스키마
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class EventResponse(BaseModel):
    id: int
    source: str
    source_id: Optional[str]
    title: str
    content: Optional[str]
    summary: Optional[str]
    url: Optional[str]
    tickers: Optional[str]
    themes: Optional[str]
    published_at: Optional[datetime]
    collected_at: datetime

    class Config:
        from_attributes = True


class EventListResponse(BaseModel):
    events: List[EventResponse]
    total: int


class EventRefreshRequest(BaseModel):
    """이벤트 새로고침 요청"""
    source: str = "all"  # dart / news / all
    tickers: Optional[List[str]] = None
