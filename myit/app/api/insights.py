"""
인사이트 카드 API — Q&A→카드 변환, CRUD, 이벤트 피드백
PRD 섹션 6-A: Insight Capture
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user_id
from app.schemas.insight_card import (
    QAInput, CardUpdate, CardEventFeedback,
    InsightCardResponse, InsightCardListResponse,
)
from app.services.insight_service import insight_service

router = APIRouter(prefix="/insights", tags=["인사이트 카드"])


@router.post("/from-qa", response_model=InsightCardResponse, status_code=201)
async def create_from_qa(
    data: QAInput,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """
    Q&A → 인사이트 카드 자동 변환 (1클릭 저장)
    - question만 있으면 LLM이 답변 생성 + 카드 변환
    - answer도 있으면 기존 답변을 카드로 변환
    """
    card = await insight_service.create_from_qa(
        db=db,
        user_id=user_id,
        question=data.question,
        answer=data.answer,
        tickers=data.tickers,
        themes=data.themes,
    )
    return InsightCardResponse.model_validate(card)


@router.get("/", response_model=InsightCardListResponse)
async def list_cards(
    page: int = 1,
    per_page: int = 20,
    ticker: Optional[str] = None,
    theme: Optional[str] = None,
    status: Optional[str] = None,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """인사이트 카드 목록 조회 (필터/페이지네이션)"""
    cards, total = await insight_service.get_cards(
        db=db, user_id=user_id, page=page, per_page=per_page,
        ticker=ticker, theme=theme, status=status,
    )
    return InsightCardListResponse(
        cards=[InsightCardResponse.model_validate(c) for c in cards],
        total=total,
        page=page,
        per_page=per_page,
    )


@router.get("/{card_id}", response_model=InsightCardResponse)
async def get_card(
    card_id: int,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """인사이트 카드 상세 조회 (버전/태그/이벤트 포함)"""
    card = await insight_service.get_card(db, card_id, user_id)
    if not card:
        raise HTTPException(status_code=404, detail="카드를 찾을 수 없습니다")
    return InsightCardResponse.model_validate(card)


@router.patch("/{card_id}", response_model=InsightCardResponse)
async def update_card(
    card_id: int,
    data: CardUpdate,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """카드 수정 (가설/메모/상태 변경)"""
    card = await insight_service.update_card(
        db, card_id, user_id,
        **data.model_dump(exclude_unset=True),
    )
    if not card:
        raise HTTPException(status_code=404, detail="카드를 찾을 수 없습니다")
    return InsightCardResponse.model_validate(card)


@router.post("/{card_id}/event-feedback")
async def submit_event_feedback(
    card_id: int,
    data: CardEventFeedback,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """
    이벤트에 대한 사용자 피드백
    '이 업데이트는 내 가설을 강화/약화/무관' (PRD Living Insight)
    """
    from sqlalchemy import select, update
    from app.models.insight_card import CardEvent

    result = await db.execute(
        select(CardEvent).where(
            CardEvent.card_id == card_id,
            CardEvent.event_id == data.event_id,
        )
    )
    card_event = result.scalar_one_or_none()
    if not card_event:
        raise HTTPException(status_code=404, detail="카드-이벤트 연결을 찾을 수 없습니다")

    card_event.impact = data.impact
    return {"status": "ok", "impact": data.impact}
