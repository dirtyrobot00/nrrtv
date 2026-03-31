"""
이벤트 모델 — 공시/뉴스/텔레그램 등 외부 이벤트 저장
PRD 섹션 6-B: Living Insight 데이터 소스
"""
from datetime import datetime, timezone
from sqlalchemy import String, Text, DateTime, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
import enum

from app.core.database import Base


class EventSource(str, enum.Enum):
    """이벤트 소스 유형"""
    DART = "dart"          # 금감원 OPEN DART 공시
    NEWS = "news"          # 뉴스 RSS
    TELEGRAM = "telegram"  # 텔레그램 채널
    MANUAL = "manual"      # 사용자 수동 입력


class Event(Base):
    """
    이벤트 (Event Store)
    공시/뉴스/텔레그램/사용자 액션을 "이벤트"로 저장
    """
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # 이벤트 기본 정보
    source: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True, comment="dart/news/telegram/manual"
    )
    source_id: Mapped[str] = mapped_column(
        String(200), nullable=True, comment="원본 소스 고유ID (예: DART 접수번호)"
    )
    title: Mapped[str] = mapped_column(String(1000), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=True, comment="원문 내용")
    summary: Mapped[str] = mapped_column(Text, nullable=True, comment="LLM 요약")
    key_sentences: Mapped[str] = mapped_column(Text, nullable=True, comment="핵심 문장 추출 (JSON)")
    url: Mapped[str] = mapped_column(String(2000), nullable=True, comment="원문 링크")

    # 관련 종목/테마
    tickers: Mapped[str] = mapped_column(
        Text, nullable=True, comment="관련 티커 코드 리스트 (JSON)"
    )
    themes: Mapped[str] = mapped_column(
        Text, nullable=True, comment="관련 테마 리스트 (JSON)"
    )

    # 메타데이터
    metadata_json: Mapped[str] = mapped_column(
        Text, nullable=True, comment="소스별 추가 메타데이터 (JSON)"
    )
    published_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="원문 발행 시점"
    )
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    # 관계
    card_links = relationship("CardEvent", back_populates="event")
