"""
홈 피드 API — PRD 섹션 7: 홈 화면 설계
"10초 안에 오늘 내가 볼 것이 정해져야 한다"
"""
from datetime import datetime, timezone
from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.database import get_db
from app.core.security import get_current_user_id
from app.models.user import User
from app.models.insight_card import InsightCard, CardEvent
from app.models.event import Event
from app.models.watchlist import WatchlistItem
from app.services.notification_service import notification_service

router = APIRouter(prefix="/home", tags=["홈"])


@router.get("/feed")
async def get_home_feed(
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """
    홈 피드 — My Pulse + Since your last visit + Today's Insight

    PRD 섹션 7 구현:
    - My Pulse: 워치리스트 상위 5개 변동/뉴스/공시
    - Since Your Last Visit: 마지막 접속 이후 업데이트
    - Today's 1 Insight: 과거 카드 1개 복습
    - Catalyst Radar: 공시/실적/이벤트
    """
    # 사용자 정보
    user_result = await db.execute(select(User).where(User.id == user_id))
    user = user_result.scalar_one_or_none()

    # 1. My Pulse — 워치리스트 종목 + 최근 이벤트
    watchlist_result = await db.execute(
        select(WatchlistItem)
        .join(WatchlistItem.watchlist)
        .where(WatchlistItem.watchlist.has(user_id=user_id))
        .order_by(WatchlistItem.sort_order)
        .limit(5)
    )
    watchlist_items = watchlist_result.scalars().all()

    my_pulse = []
    for item in watchlist_items:
        # 해당 종목의 최근 이벤트 1개
        event_result = await db.execute(
            select(Event)
            .where(Event.tickers.contains(item.ticker))
            .order_by(Event.collected_at.desc())
            .limit(1)
        )
        latest_event = event_result.scalar_one_or_none()

        my_pulse.append({
            "ticker": item.ticker,
            "ticker_name": item.ticker_name,
            "market": item.market,
            "latest_event": {
                "title": latest_event.title,
                "source": latest_event.source,
                "published_at": latest_event.published_at.isoformat() if latest_event and latest_event.published_at else None,
            } if latest_event else None,
        })

    # 2. Since Your Last Visit — 최근 이벤트 중 카드에 연결된 것
    recent_card_events = await db.execute(
        select(CardEvent)
        .join(InsightCard)
        .where(InsightCard.user_id == user_id)
        .order_by(CardEvent.linked_at.desc())
        .limit(5)
    )
    since_last_visit = []
    for ce in recent_card_events.scalars().all():
        since_last_visit.append({
            "card_id": ce.card_id,
            "event_id": ce.event_id,
            "linked_at": ce.linked_at.isoformat() if ce.linked_at else None,
            "impact": ce.impact,
        })

    # 3. Today's 1 Insight — 랜덤 과거 카드 1개 (스페이싱 복습)
    random_card_result = await db.execute(
        select(InsightCard)
        .where(InsightCard.user_id == user_id, InsightCard.status == "active")
        .order_by(func.random())
        .limit(1)
    )
    todays_insight = random_card_result.scalar_one_or_none()

    # 4. 알림
    notifications = await notification_service.get_unread(user_id, limit=5)

    return {
        "my_pulse": my_pulse,
        "since_last_visit": since_last_visit,
        "todays_insight": {
            "id": todays_insight.id,
            "title": todays_insight.title,
            "summary": todays_insight.summary,
            "original_question": todays_insight.original_question,
        } if todays_insight else None,
        "notifications": [
            {
                "id": n.id,
                "title": n.title,
                "body": n.body,
                "category": n.category,
                "created_at": n.created_at.isoformat(),
            } for n in notifications
        ],
        "disclaimer": "⚠️ myit는 정보 제공 목적이며, 투자 추천이 아닙니다.",
    }
