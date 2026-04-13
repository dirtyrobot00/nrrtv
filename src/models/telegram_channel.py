"""Telegram channel model."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ChannelCategory(str, Enum):
    STOCK_REALTIME = "주식실황"
    STOCK_RESEARCH = "종목리서치"
    MACRO = "매크로"
    IPO = "IPO"
    OTHER = "기타"


class TelegramChannel(BaseModel):
    """Telegram channel data model."""

    id: Optional[int] = None
    username: str
    channel_name: Optional[str] = None
    channel_url: Optional[str] = None
    description: Optional[str] = None
    category: Optional[ChannelCategory] = None
    characteristics: Optional[str] = None
    subscriber_count: Optional[int] = None
    is_active: bool = True
    last_scraped_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        use_enum_values = True

    @property
    def url(self) -> str:
        return f"https://t.me/s/{self.username}"

    def to_dict(self) -> dict:
        return self.model_dump()
