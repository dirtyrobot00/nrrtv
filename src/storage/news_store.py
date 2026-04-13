"""News article storage using SQLAlchemy ORM."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Column, DateTime, Index, Integer, String, Text,
    UniqueConstraint, create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from src.utils.logger import LoggerMixin


class NewsBase(DeclarativeBase):
    pass


class NewsArticleORM(NewsBase):
    __tablename__ = "news_articles"
    __table_args__ = (
        Index("idx_news_articles_published_at", "published_at"),
        Index("idx_news_articles_ticker", "ticker"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text, unique=True, nullable=False)
    title = Column(Text, nullable=False)
    content = Column(Text, nullable=True)
    summary = Column(Text, nullable=True)
    author = Column(Text, nullable=True)
    media = Column(String(255), nullable=True)
    ticker = Column(String(20), nullable=True)
    published_at = Column(DateTime(timezone=True), nullable=True)
    collected_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    raw_html = Column(Text, nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "url": self.url,
            "title": self.title,
            "content": self.content,
            "summary": self.summary,
            "author": self.author,
            "media": self.media,
            "ticker": self.ticker,
            "published_at": self.published_at,
            "collected_at": self.collected_at,
        }


class NewsStore(LoggerMixin):
    """CRUD operations for news_articles."""

    def __init__(self, database_url: Optional[str] = None):
        if database_url is None:
            import os
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                from src.utils.config import get_config
                config = get_config()
                database_url = config.get("database.url")

        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        NewsBase.metadata.create_all(self.engine)

    def get_session(self) -> Session:
        return self.SessionLocal()

    # ── Article operations ───────────────────────────────────────────────

    def save_articles(self, articles: List[Dict[str, Any]]) -> int:
        """URL 기준 중복 skip 후 삽입. 삽입 건수 반환."""
        if not articles:
            return 0

        inserted = 0
        with self.get_session() as session:
            for art in articles:
                if not art.get("url") or not art.get("title"):
                    continue
                exists = session.query(NewsArticleORM).filter_by(url=art["url"]).first()
                if exists:
                    continue
                session.add(NewsArticleORM(
                    url=art["url"],
                    title=art["title"],
                    content=art.get("content"),
                    summary=art.get("summary"),
                    author=art.get("author"),
                    media=art.get("media"),
                    ticker=art.get("ticker"),
                    published_at=art.get("published_at"),
                    raw_html=art.get("raw_html"),
                ))
                inserted += 1
            session.commit()

        self.logger.info("news_articles_saved", inserted=inserted, total=len(articles))
        return inserted

    def exists(self, url: str) -> bool:
        with self.get_session() as session:
            return session.query(NewsArticleORM.id).filter_by(url=url).first() is not None

    def get_latest_published_at(self, ticker: Optional[str] = None) -> Optional[datetime]:
        """가장 최근 published_at 반환 (realtime 기준점)."""
        with self.get_session() as session:
            q = session.query(NewsArticleORM.published_at)
            if ticker:
                q = q.filter(NewsArticleORM.ticker == ticker)
            row = q.order_by(NewsArticleORM.published_at.desc()).first()
            return row[0] if row else None

    def list_articles(
        self,
        limit: int = 100,
        offset: int = 0,
        ticker: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self.get_session() as session:
            q = session.query(NewsArticleORM)
            if ticker:
                q = q.filter(NewsArticleORM.ticker == ticker)
            rows = q.order_by(NewsArticleORM.published_at.desc()).limit(limit).offset(offset).all()
            return [r.to_dict() for r in rows]

    def get_stats(self) -> Dict[str, Any]:
        with self.get_session() as session:
            total = session.query(NewsArticleORM).count()
            latest = (
                session.query(NewsArticleORM.published_at)
                .order_by(NewsArticleORM.published_at.desc())
                .first()
            )
            oldest = (
                session.query(NewsArticleORM.published_at)
                .order_by(NewsArticleORM.published_at.asc())
                .first()
            )
            return {
                "total_articles": total,
                "latest_published_at": latest[0] if latest else None,
                "oldest_published_at": oldest[0] if oldest else None,
            }
