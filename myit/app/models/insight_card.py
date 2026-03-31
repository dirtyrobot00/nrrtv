"""
인사이트 카드 모델 — PRD의 핵심 데이터 구조
Q&A → 카드 변환, 버전 관리, 태그, 이벤트 연결
"""
from datetime import datetime, timezone
from sqlalchemy import String, Text, DateTime, ForeignKey, Integer, Enum as SAEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship
import enum

from app.core.database import Base


class CardStatus(str, enum.Enum):
    """카드 상태"""
    DRAFT = "draft"          # 자동 생성 후 미확인
    ACTIVE = "active"        # 사용자 확인/저장
    ARCHIVED = "archived"    # 보관


class InsightCard(Base):
    """
    인사이트 카드 — LLM Q&A를 구조화한 지식 단위
    PRD 섹션 6-A: Insight Capture
    """
    __tablename__ = "insight_cards"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)

    # 카드 기본 필드 (PRD 정의)
    title: Mapped[str] = mapped_column(String(500), nullable=False, comment="LLM 자동 생성 제목")
    original_question: Mapped[str] = mapped_column(Text, nullable=False, comment="사용자 원 질문")
    summary: Mapped[str] = mapped_column(Text, nullable=True, comment="핵심 요약 (3~7줄)")
    full_answer: Mapped[str] = mapped_column(Text, nullable=True, comment="LLM 전체 답변")
    sources: Mapped[str] = mapped_column(Text, nullable=True, comment="근거 출처 링크 (JSON)")
    hypothesis: Mapped[str] = mapped_column(Text, nullable=True, comment="내 결론/가설 (사용자 메모)")
    risk_rebuttal: Mapped[str] = mapped_column(Text, nullable=True, comment="리스크/반박 (LLM 보조)")
    followup_questions: Mapped[str] = mapped_column(
        Text, nullable=True, comment="Follow-up 질문 3개 (JSON)"
    )

    # 메타데이터
    data_cutoff: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="데이터 기준 시점"
    )
    status: Mapped[str] = mapped_column(
        String(20), default=CardStatus.DRAFT.value, comment="draft/active/archived"
    )
    current_version: Mapped[int] = mapped_column(Integer, default=1)

    # 타임스탬프
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # 관계
    user = relationship("User", back_populates="insight_cards")
    versions = relationship("CardVersion", back_populates="card", cascade="all, delete-orphan")
    tags = relationship("CardTag", back_populates="card", cascade="all, delete-orphan")
    events = relationship("CardEvent", back_populates="card", cascade="all, delete-orphan")
    trades = relationship("Trade", back_populates="insight_card")


class CardVersion(Base):
    """
    카드 버전 — 같은 질문이라도 업데이트가 붙으면 v1/v2로 변화 추적
    PRD 섹션 6-C: Search & Recall - 버전 관리
    """
    __tablename__ = "card_versions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    card_id: Mapped[int] = mapped_column(ForeignKey("insight_cards.id"), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=True)
    full_answer: Mapped[str] = mapped_column(Text, nullable=True)
    change_reason: Mapped[str] = mapped_column(
        String(500), nullable=True, comment="버전 변경 사유 (예: 새 공시 반영)"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    card = relationship("InsightCard", back_populates="versions")


class CardTag(Base):
    """
    카드 태그 — 티커/테마 태그 (자동 추출 + 사용자 수정)
    """
    __tablename__ = "card_tags"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    card_id: Mapped[int] = mapped_column(ForeignKey("insight_cards.id"), nullable=False, index=True)
    tag_type: Mapped[str] = mapped_column(
        String(20), nullable=False, comment="ticker / theme / custom"
    )
    tag_value: Mapped[str] = mapped_column(String(200), nullable=False, comment="예: 005930, 반도체, AI")
    is_auto: Mapped[bool] = mapped_column(default=True, comment="자동 추출 여부")

    card = relationship("InsightCard", back_populates="tags")


class CardEvent(Base):
    """
    카드-이벤트 연결 — N:M 관계 + 사용자 피드백
    PRD 섹션 6-B: Living Insight
    """
    __tablename__ = "card_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    card_id: Mapped[int] = mapped_column(ForeignKey("insight_cards.id"), nullable=False, index=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id"), nullable=False, index=True)
    impact: Mapped[str] = mapped_column(
        String(20), nullable=True,
        comment="strengthen(강화) / weaken(약화) / neutral(무관) — 사용자 피드백"
    )
    linked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    card = relationship("InsightCard", back_populates="events")
    event = relationship("Event", back_populates="card_links")
