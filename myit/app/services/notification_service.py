"""
알림 서비스 — 알림 관리 (MVP: 인메모리, 추후 푸시/이메일 확장)
PRD 섹션 8: 리텐션 루프
"""
from datetime import datetime, timezone
from typing import List, Dict, Optional
from dataclasses import dataclass, field


@dataclass
class Notification:
    id: str
    user_id: int
    title: str
    body: str
    category: str  # event_update / morning_briefing / card_followup / trade_reminder
    data: Dict = field(default_factory=dict)
    is_read: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class NotificationService:
    """
    알림 서비스 (MVP: 인메모리)
    TODO v1: 푸시 알림 / 이메일 연동
    """

    def __init__(self):
        # MVP: 인메모리 저장 (프로덕션에서는 DB/Redis로 교체)
        self._store: Dict[int, List[Notification]] = {}
        self._counter = 0

    def _next_id(self) -> str:
        self._counter += 1
        return f"notif_{self._counter}"

    async def send(
        self,
        user_id: int,
        title: str,
        body: str,
        category: str = "general",
        data: Dict = None,
    ) -> Notification:
        """알림 발송"""
        notif = Notification(
            id=self._next_id(),
            user_id=user_id,
            title=title,
            body=body,
            category=category,
            data=data or {},
        )
        if user_id not in self._store:
            self._store[user_id] = []
        self._store[user_id].append(notif)
        return notif

    async def get_unread(self, user_id: int, limit: int = 20) -> List[Notification]:
        """읽지 않은 알림 조회"""
        notifications = self._store.get(user_id, [])
        unread = [n for n in notifications if not n.is_read]
        return sorted(unread, key=lambda n: n.created_at, reverse=True)[:limit]

    async def mark_read(self, user_id: int, notification_id: str):
        """알림 읽음 처리"""
        for notif in self._store.get(user_id, []):
            if notif.id == notification_id:
                notif.is_read = True
                break

    async def notify_event_update(
        self, user_id: int, card_title: str, event_title: str
    ):
        """이벤트 업데이트가 카드에 연결됐을 때 알림"""
        await self.send(
            user_id=user_id,
            title="📊 인사이트 업데이트",
            body=f"'{card_title}'에 새로운 이벤트가 연결되었습니다: {event_title}",
            category="event_update",
        )

    async def notify_morning_briefing(
        self, user_id: int, updates: List[Dict]
    ):
        """모닝 브리핑 알림 (PRD 루프 1)"""
        summary = f"오늘의 업데이트 {len(updates)}건"
        await self.send(
            user_id=user_id,
            title="☀️ 모닝 브리핑",
            body=summary,
            category="morning_briefing",
            data={"updates": updates},
        )

    async def notify_card_followup(self, user_id: int, card_title: str):
        """카드 후속 체크 알림 (PRD 루프 2)"""
        await self.send(
            user_id=user_id,
            title="🔄 Follow-up 체크",
            body=f"'{card_title}'에 대한 업데이트가 있는지 확인해보세요",
            category="card_followup",
        )


# 싱글턴
notification_service = NotificationService()
