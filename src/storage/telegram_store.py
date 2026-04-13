"""Telegram channel and message storage using SQLAlchemy ORM."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Index, Integer, String, Text,
    UniqueConstraint, create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

from src.utils.config import get_config
from src.utils.logger import LoggerMixin


class TelegramBase(DeclarativeBase):
    pass


class TelegramChannelORM(TelegramBase):
    __tablename__ = "telegram_channels"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), unique=True, nullable=False, index=True)
    channel_name = Column(String(500), nullable=True)
    channel_url = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    category = Column(String(100), nullable=True, index=True)
    characteristics = Column(Text, nullable=True)
    subscriber_count = Column(Integer, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    last_scraped_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    messages = relationship("TelegramMessageORM", back_populates="channel", cascade="all, delete-orphan")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "username": self.username,
            "channel_name": self.channel_name,
            "channel_url": self.channel_url,
            "description": self.description,
            "category": self.category,
            "characteristics": self.characteristics,
            "subscriber_count": self.subscriber_count,
            "is_active": self.is_active,
            "last_scraped_at": self.last_scraped_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class TelegramMessageORM(TelegramBase):
    __tablename__ = "telegram_messages"
    __table_args__ = (
        UniqueConstraint("channel_id", "telegram_msg_id", name="uq_channel_msg"),
        Index("idx_telegram_messages_posted_at", "posted_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(Integer, ForeignKey("telegram_channels.id", ondelete="CASCADE"), nullable=False, index=True)
    telegram_msg_id = Column(Integer, nullable=False)
    content = Column(Text, nullable=True)
    posted_at = Column(DateTime(timezone=True), nullable=True)
    views = Column(Integer, nullable=True)
    has_media = Column(Boolean, default=False, nullable=False)
    raw_html = Column(Text, nullable=True)
    linked_article_text = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    channel = relationship("TelegramChannelORM", back_populates="messages")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "channel_id": self.channel_id,
            "telegram_msg_id": self.telegram_msg_id,
            "content": self.content,
            "posted_at": self.posted_at,
            "views": self.views,
            "has_media": self.has_media,
            "linked_article_text": self.linked_article_text,
            "created_at": self.created_at,
        }


class TelegramStore(LoggerMixin):
    """CRUD operations for telegram_channels and telegram_messages."""

    def __init__(self, database_url: Optional[str] = None):
        if database_url is None:
            import os
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                config = get_config()
                database_url = config.get("database.url")

        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        TelegramBase.metadata.create_all(self.engine)
        self._migrate()

    def _migrate(self) -> None:
        """신규 컬럼을 기존 테이블에 추가한다 (이미 있으면 무시)."""
        from sqlalchemy import inspect, text
        inspector = inspect(self.engine)
        cols = {c["name"] for c in inspector.get_columns("telegram_messages")}
        with self.engine.connect() as conn:
            if "linked_article_text" not in cols:
                conn.execute(text("ALTER TABLE telegram_messages ADD COLUMN linked_article_text TEXT"))
                conn.commit()

    def get_session(self) -> Session:
        return self.SessionLocal()

    # ── Channel operations ───────────────────────────────────────────────

    def add_channel(
        self,
        username: str,
        channel_name: Optional[str] = None,
        category: Optional[str] = None,
        characteristics: Optional[str] = None,
        description: Optional[str] = None,
        subscriber_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Register a new channel. Raises ValueError if username already exists."""
        username = username.lstrip("@").strip()
        with self.get_session() as session:
            existing = session.query(TelegramChannelORM).filter_by(username=username).first()
            if existing:
                raise ValueError(f"Channel @{username} already registered (id={existing.id})")

            ch = TelegramChannelORM(
                username=username,
                channel_name=channel_name,
                channel_url=f"https://t.me/s/{username}",
                description=description,
                category=category,
                characteristics=characteristics,
                subscriber_count=subscriber_count,
            )
            session.add(ch)
            session.commit()
            session.refresh(ch)
            self.logger.info("channel_added", username=username, id=ch.id)
            return ch.to_dict()

    def get_channel(self, username: str) -> Optional[Dict[str, Any]]:
        username = username.lstrip("@").strip()
        with self.get_session() as session:
            ch = session.query(TelegramChannelORM).filter_by(username=username).first()
            return ch.to_dict() if ch else None

    def get_channel_by_id(self, channel_id: int) -> Optional[Dict[str, Any]]:
        with self.get_session() as session:
            ch = session.query(TelegramChannelORM).filter_by(id=channel_id).first()
            return ch.to_dict() if ch else None

    def list_channels(
        self,
        active_only: bool = True,
        category: Optional[str] = None,
        min_subscribers: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        with self.get_session() as session:
            q = session.query(TelegramChannelORM)
            if active_only:
                q = q.filter(TelegramChannelORM.is_active == True)
            if category:
                q = q.filter(TelegramChannelORM.category == category)
            if min_subscribers is not None:
                q = q.filter(TelegramChannelORM.subscriber_count >= min_subscribers)
            return [ch.to_dict() for ch in q.order_by(TelegramChannelORM.created_at.desc()).all()]

    def update_channel(self, username: str, **fields) -> bool:
        username = username.lstrip("@").strip()
        with self.get_session() as session:
            ch = session.query(TelegramChannelORM).filter_by(username=username).first()
            if not ch:
                return False
            for k, v in fields.items():
                if hasattr(ch, k):
                    setattr(ch, k, v)
            ch.updated_at = datetime.utcnow()
            session.commit()
            return True

    def deactivate_channel(self, username: str) -> bool:
        return self.update_channel(username, is_active=False)

    def mark_scraped(self, channel_id: int) -> None:
        with self.get_session() as session:
            ch = session.query(TelegramChannelORM).filter_by(id=channel_id).first()
            if ch:
                ch.last_scraped_at = datetime.utcnow()
                ch.updated_at = datetime.utcnow()
                session.commit()

    # ── Message operations ───────────────────────────────────────────────

    def save_messages(self, messages: List[Dict[str, Any]]) -> int:
        """Upsert-style: insert only new messages (skip duplicates). Returns count inserted."""
        if not messages:
            return 0

        inserted = 0
        with self.get_session() as session:
            for msg in messages:
                exists = (
                    session.query(TelegramMessageORM)
                    .filter_by(
                        channel_id=msg["channel_id"],
                        telegram_msg_id=msg["telegram_msg_id"],
                    )
                    .first()
                )
                if exists:
                    continue

                session.add(TelegramMessageORM(
                    channel_id=msg["channel_id"],
                    telegram_msg_id=msg["telegram_msg_id"],
                    content=msg.get("content"),
                    posted_at=msg.get("posted_at"),
                    views=msg.get("views"),
                    has_media=msg.get("has_media", False),
                    raw_html=msg.get("raw_html"),
                    linked_article_text=msg.get("linked_article_text"),
                ))
                inserted += 1

            session.commit()

        self.logger.info("messages_saved", inserted=inserted, total=len(messages))
        return inserted

    def get_latest_msg_id(self, channel_id: int) -> Optional[int]:
        """Return the highest telegram_msg_id stored for a channel (used for incremental scrape)."""
        with self.get_session() as session:
            row = (
                session.query(TelegramMessageORM.telegram_msg_id)
                .filter_by(channel_id=channel_id)
                .order_by(TelegramMessageORM.telegram_msg_id.desc())
                .first()
            )
            return row[0] if row else None

    def get_oldest_message(self, channel_id: int):
        """Return (posted_at, telegram_msg_id) of the oldest message for a channel. None if no messages."""
        with self.get_session() as session:
            row = (
                session.query(TelegramMessageORM.posted_at, TelegramMessageORM.telegram_msg_id)
                .filter_by(channel_id=channel_id)
                .order_by(TelegramMessageORM.telegram_msg_id.asc())
                .first()
            )
            return (row[0], row[1]) if row else (None, None)

    def list_messages(
        self,
        channel_id: int,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        with self.get_session() as session:
            rows = (
                session.query(TelegramMessageORM)
                .filter_by(channel_id=channel_id)
                .order_by(TelegramMessageORM.posted_at.desc())
                .limit(limit)
                .offset(offset)
                .all()
            )
            return [r.to_dict() for r in rows]

    def get_stats(self) -> Dict[str, Any]:
        with self.get_session() as session:
            return {
                "total_channels": session.query(TelegramChannelORM).count(),
                "active_channels": session.query(TelegramChannelORM).filter_by(is_active=True).count(),
                "total_messages": session.query(TelegramMessageORM).count(),
            }
