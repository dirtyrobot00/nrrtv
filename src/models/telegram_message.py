"""Telegram message model."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class TelegramMessage(BaseModel):
    """Telegram message data model."""

    id: Optional[int] = None
    channel_id: int
    telegram_msg_id: int
    content: Optional[str] = None
    posted_at: Optional[datetime] = None
    views: Optional[int] = None
    has_media: bool = False
    raw_html: Optional[str] = None
    created_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return self.model_dump()
