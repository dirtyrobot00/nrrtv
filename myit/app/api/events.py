"""
이벤트(공시/뉴스) API — 최신 이벤트 조회/새로고침
PRD 섹션 6-B: Living Insight
"""
import json
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user_id
from app.models.event import Event
from app.schemas.event import EventResponse, EventListResponse, EventRefreshRequest
from app.services.dart_service import dart_service
from app.services.news_service import news_service
from app.services.llm_service import llm_service
from app.services.insight_service import insight_service

router = APIRouter(prefix="/events", tags=["이벤트(공시/뉴스)"])


@router.get("/", response_model=EventListResponse)
async def list_events(
    source: Optional[str] = None,
    ticker: Optional[str] = None,
    limit: int = 30,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """최신 이벤트 목록 조회"""
    query = select(Event).order_by(Event.collected_at.desc()).limit(limit)
    if source:
        query = query.where(Event.source == source)
    if ticker:
        query = query.where(Event.tickers.contains(ticker))

    result = await db.execute(query)
    events = result.scalars().all()
    return EventListResponse(
        events=[EventResponse.model_validate(e) for e in events],
        total=len(events),
    )


@router.post("/refresh")
async def refresh_events(
    data: EventRefreshRequest,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """
    이벤트 새로고침 — DART 공시 + 뉴스 수집
    PRD: "Refresh 버튼 → 최신 이벤트 재검색/재요약"
    """
    new_events = []

    # 1. DART 공시 수집
    if data.source in ("all", "dart"):
        disclosures = await dart_service.fetch_recent_disclosures()
        for d in disclosures:
            # 중복 체크
            existing = await db.execute(
                select(Event).where(
                    Event.source == "dart",
                    Event.source_id == d["source_id"],
                )
            )
            if existing.scalar_one_or_none():
                continue

            event = Event(
                source=d["source"],
                source_id=d["source_id"],
                title=d["title"],
                content=d["content"],
                url=d["url"],
                tickers=d.get("tickers"),
                published_at=d.get("published_at"),
                metadata_json=d.get("metadata_json"),
            )

            # LLM 요약
            summary_data = await llm_service.summarize_event(event.title, event.content or "")
            event.summary = summary_data.get("summary", "")
            event.key_sentences = json.dumps(
                summary_data.get("key_sentences", []), ensure_ascii=False
            )

            db.add(event)
            await db.flush()

            # 관련 카드에 자동 연결
            await insight_service.link_event_to_cards(db, event)
            new_events.append(event)

    # 2. 뉴스 수집
    if data.source in ("all", "news"):
        news_items = await news_service.fetch_news(
            tickers=data.tickers, max_items=10
        )
        for n in news_items:
            existing = await db.execute(
                select(Event).where(
                    Event.source == "news",
                    Event.source_id == n["source_id"],
                )
            )
            if existing.scalar_one_or_none():
                continue

            event = Event(
                source=n["source"],
                source_id=n["source_id"],
                title=n["title"],
                content=n.get("content"),
                url=n.get("url"),
                tickers=n.get("tickers"),
                published_at=n.get("published_at"),
                metadata_json=n.get("metadata_json"),
            )
            db.add(event)
            await db.flush()

            await insight_service.link_event_to_cards(db, event)
            new_events.append(event)

    return {
        "status": "ok",
        "new_events_count": len(new_events),
        "message": f"{len(new_events)}개의 새 이벤트를 수집했습니다",
    }
