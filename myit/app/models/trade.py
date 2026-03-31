"""
매매 기록 모델 — 투자 성과 연동
PRD 섹션 9: 투자 성과 연동/기록
"""
from datetime import datetime, timezone
from sqlalchemy import String, Text, DateTime, ForeignKey, Float, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
import enum

from app.core.database import Base


class TradeDirection(str, enum.Enum):
    BUY = "buy"
    SELL = "sell"


class Trade(Base):
    """
    매매 기록
    MVP: 수동 입력 + CSV 업로드
    V1: 브로커 읽기 전용 연동
    """
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)

    # 매매 정보
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True, comment="종목코드")
    ticker_name: Mapped[str] = mapped_column(String(200), nullable=True, comment="종목명")
    market: Mapped[str] = mapped_column(String(10), default="KR", comment="KR / US")
    direction: Mapped[str] = mapped_column(String(10), nullable=False, comment="buy / sell")
    quantity: Mapped[float] = mapped_column(Float, nullable=False, comment="수량")
    price: Mapped[float] = mapped_column(Float, nullable=False, comment="체결 단가")
    total_amount: Mapped[float] = mapped_column(Float, nullable=True, comment="총 금액")
    fee: Mapped[float] = mapped_column(Float, default=0.0, comment="수수료")

    # 인사이트 카드 연결 → "진입 근거" (PRD 루프 3)
    insight_card_id: Mapped[int] = mapped_column(
        ForeignKey("insight_cards.id"), nullable=True, comment="진입 근거 카드 ID"
    )

    # 매매 메모 / 회고
    entry_reason: Mapped[str] = mapped_column(Text, nullable=True, comment="진입 사유")
    exit_reason: Mapped[str] = mapped_column(Text, nullable=True, comment="청산 사유")
    retrospective: Mapped[str] = mapped_column(
        Text, nullable=True, comment="회고 노트 (가설이 맞았는가?)"
    )
    setup_tag: Mapped[str] = mapped_column(
        String(100), nullable=True, comment="전략/셋업 태그 (예: 실적발표놀이)"
    )

    # 성과 (청산 시 계산)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=True, comment="실현 손익")
    return_pct: Mapped[float] = mapped_column(Float, nullable=True, comment="수익률 (%)")
    holding_days: Mapped[int] = mapped_column(Integer, nullable=True, comment="보유기간 (일)")

    # 데이터 소스
    source: Mapped[str] = mapped_column(
        String(20), default="manual", comment="manual / csv / broker"
    )

    traded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="체결 시각"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    # 관계
    user = relationship("User", back_populates="trades")
    insight_card = relationship("InsightCard", back_populates="trades")
