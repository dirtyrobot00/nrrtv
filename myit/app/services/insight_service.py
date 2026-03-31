"""
인사이트 서비스 — 카드 생성/관리/이벤트 매칭 비즈니스 로직
PRD 섹션 6-A, 6-B 핵심 로직
"""
import json
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.insight_card import InsightCard, CardVersion, CardTag, CardEvent, CardStatus
from app.models.event import Event
from app.services.llm_service import llm_service
from app.services.search_service import search_service


class InsightService:
    """인사이트 카드 비즈니스 로직"""

    async def create_from_qa(
        self,
        db: AsyncSession,
        user_id: int,
        question: str,
        answer: str = None,
        tickers: List[str] = None,
        themes: List[str] = None,
    ) -> InsightCard:
        """
        Q&A → 인사이트 카드 자동 생성
        1) LLM 답변 생성 (answer가 없는 경우)
        2) 카드 필드 추출 (제목/요약/태그/리스크/follow-up)
        3) DB 저장 + 벡터 인덱싱
        """
        # 1. LLM 답변 생성
        if not answer:
            answer = await llm_service.generate_answer(question)

        # 2. 카드 필드 추출
        fields = await llm_service.qa_to_card_fields(question, answer, tickers)

        # 3. 카드 생성
        card = InsightCard(
            user_id=user_id,
            title=fields.get("title", question[:100]),
            original_question=question,
            summary=fields.get("summary", ""),
            full_answer=answer,
            hypothesis="",
            risk_rebuttal=fields.get("risk_rebuttal", ""),
            followup_questions=json.dumps(
                fields.get("followup_questions", []), ensure_ascii=False
            ),
            data_cutoff=datetime.now(timezone.utc),
            status=CardStatus.DRAFT.value,
            current_version=1,
        )
        db.add(card)
        await db.flush()

        # 4. 태그 생성
        all_tickers = set(tickers or []) | set(fields.get("tickers", []))
        all_themes = set(themes or []) | set(fields.get("themes", []))

        for ticker in all_tickers:
            if ticker:
                db.add(CardTag(
                    card_id=card.id, tag_type="ticker", tag_value=ticker, is_auto=True
                ))
        for theme in all_themes:
            if theme:
                db.add(CardTag(
                    card_id=card.id, tag_type="theme", tag_value=theme, is_auto=True
                ))

        # 5. 첫 번째 버전 기록
        db.add(CardVersion(
            card_id=card.id,
            version=1,
            summary=card.summary,
            full_answer=card.full_answer,
            change_reason="최초 생성",
        ))

        await db.flush()

        # 6. 벡터 인덱싱
        tag_values = [t for t in list(all_tickers) + list(all_themes) if t]
        await search_service.index_card(
            card_id=card.id,
            question=question,
            summary=card.summary or "",
            hypothesis="",
            tags=tag_values,
        )

        return card

    async def get_cards(
        self,
        db: AsyncSession,
        user_id: int,
        page: int = 1,
        per_page: int = 20,
        ticker: str = None,
        theme: str = None,
        status: str = None,
    ) -> tuple[List[InsightCard], int]:
        """카드 목록 조회 (필터/페이지네이션)"""
        query = (
            select(InsightCard)
            .where(InsightCard.user_id == user_id)
            .options(selectinload(InsightCard.tags))
            .order_by(InsightCard.updated_at.desc())
        )

        if status:
            query = query.where(InsightCard.status == status)

        # 티커/테마 필터
        if ticker:
            query = query.join(CardTag).where(
                CardTag.tag_type == "ticker", CardTag.tag_value == ticker
            )
        if theme:
            query = query.join(CardTag).where(
                CardTag.tag_type == "theme", CardTag.tag_value == theme
            )

        # 전체 카운트
        count_query = select(func.count()).select_from(query.subquery())
        total = (await db.execute(count_query)).scalar() or 0

        # 페이지네이션
        query = query.offset((page - 1) * per_page).limit(per_page)
        result = await db.execute(query)
        cards = result.scalars().all()

        return list(cards), total

    async def get_card(self, db: AsyncSession, card_id: int, user_id: int) -> Optional[InsightCard]:
        """카드 상세 조회"""
        query = (
            select(InsightCard)
            .where(InsightCard.id == card_id, InsightCard.user_id == user_id)
            .options(
                selectinload(InsightCard.tags),
                selectinload(InsightCard.versions),
                selectinload(InsightCard.events),
            )
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def update_card(
        self, db: AsyncSession, card_id: int, user_id: int, **kwargs
    ) -> Optional[InsightCard]:
        """카드 수정 (가설/메모 등)"""
        card = await self.get_card(db, card_id, user_id)
        if not card:
            return None

        for key, value in kwargs.items():
            if value is not None and hasattr(card, key):
                setattr(card, key, value)

        # 벡터 인덱스 업데이트
        await search_service.index_card(
            card_id=card.id,
            question=card.original_question,
            summary=card.summary or "",
            hypothesis=card.hypothesis or "",
        )

        return card

    async def link_event_to_cards(self, db: AsyncSession, event: Event):
        """
        이벤트 → 관련 카드 자동 연결
        PRD 섹션 6-B: Living Insight 핵심 로직
        """
        if not event.tickers:
            return

        event_tickers = json.loads(event.tickers) if isinstance(event.tickers, str) else []

        for ticker in event_tickers:
            if not ticker:
                continue
            # 해당 티커 태그가 있는 활성 카드 찾기
            query = (
                select(InsightCard)
                .join(CardTag)
                .where(
                    CardTag.tag_type == "ticker",
                    CardTag.tag_value == ticker,
                    InsightCard.status != CardStatus.ARCHIVED.value,
                )
            )
            result = await db.execute(query)
            cards = result.scalars().all()

            for card in cards:
                # 중복 확인
                existing = await db.execute(
                    select(CardEvent).where(
                        CardEvent.card_id == card.id,
                        CardEvent.event_id == event.id,
                    )
                )
                if not existing.scalar_one_or_none():
                    db.add(CardEvent(card_id=card.id, event_id=event.id))


# 싱글턴
insight_service = InsightService()
