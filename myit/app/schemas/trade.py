"""
매매 기록 Pydantic 스키마
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


# --- 요청 스키마 ---

class TradeCreate(BaseModel):
    ticker: str
    ticker_name: Optional[str] = None
    market: str = "KR"
    direction: str  # buy / sell
    quantity: float
    price: float
    fee: float = 0.0
    insight_card_id: Optional[int] = None
    entry_reason: Optional[str] = None
    setup_tag: Optional[str] = None
    traded_at: datetime


class TradeClose(BaseModel):
    """청산 시 회고 기록"""
    exit_reason: Optional[str] = None
    retrospective: Optional[str] = None


# --- 응답 스키마 ---

class TradeResponse(BaseModel):
    id: int
    ticker: str
    ticker_name: Optional[str]
    market: str
    direction: str
    quantity: float
    price: float
    total_amount: Optional[float]
    fee: float
    insight_card_id: Optional[int]
    entry_reason: Optional[str]
    exit_reason: Optional[str]
    retrospective: Optional[str]
    setup_tag: Optional[str]
    realized_pnl: Optional[float]
    return_pct: Optional[float]
    holding_days: Optional[int]
    source: str
    traded_at: datetime
    created_at: datetime

    class Config:
        from_attributes = True


class TradeListResponse(BaseModel):
    trades: List[TradeResponse]
    total: int


class PerformanceSummary(BaseModel):
    """성과 요약 — PRD 섹션 9: 의사결정 품질"""
    total_trades: int
    win_count: int
    loss_count: int
    win_rate: float
    total_pnl: float
    avg_return_pct: float
    avg_holding_days: float
    # 셋업별 승률
    setup_performance: List[dict] = []
    # 자주 손실 보는 상황 Top 3
    top_loss_patterns: List[str] = []
