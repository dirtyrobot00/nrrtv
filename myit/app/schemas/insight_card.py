"""
인사이트 카드 Pydantic 스키마
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


# --- 요청 스키마 ---

class QAInput(BaseModel):
    """Q&A → 카드 변환 요청"""
    question: str
    answer: Optional[str] = None  # 없으면 LLM이 생성
    tickers: Optional[List[str]] = None  # 관련 티커
    themes: Optional[List[str]] = None   # 관련 테마


class CardUpdate(BaseModel):
    """카드 수정 (사용자 메모/가설 등)"""
    title: Optional[str] = None
    hypothesis: Optional[str] = None
    risk_rebuttal: Optional[str] = None
    status: Optional[str] = None


class CardTagInput(BaseModel):
    tag_type: str  # ticker / theme / custom
    tag_value: str


class CardEventFeedback(BaseModel):
    """이벤트에 대한 사용자 피드백"""
    event_id: int
    impact: str  # strengthen / weaken / neutral


# --- 응답 스키마 ---

class CardTagResponse(BaseModel):
    id: int
    tag_type: str
    tag_value: str
    is_auto: bool

    class Config:
        from_attributes = True


class CardVersionResponse(BaseModel):
    id: int
    version: int
    summary: Optional[str]
    change_reason: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class EventBriefResponse(BaseModel):
    id: int
    source: str
    title: str
    summary: Optional[str]
    published_at: Optional[datetime]
    impact: Optional[str] = None  # CardEvent에서 가져옴

    class Config:
        from_attributes = True


class InsightCardResponse(BaseModel):
    id: int
    title: str
    original_question: str
    summary: Optional[str]
    hypothesis: Optional[str]
    risk_rebuttal: Optional[str]
    followup_questions: Optional[str]
    data_cutoff: Optional[datetime]
    status: str
    current_version: int
    tags: List[CardTagResponse] = []
    recent_events: List[EventBriefResponse] = []
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class InsightCardListResponse(BaseModel):
    cards: List[InsightCardResponse]
    total: int
    page: int
    per_page: int
