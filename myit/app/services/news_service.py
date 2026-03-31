"""
뉴스 수집 서비스 — RSS 기반
PRD 섹션 6-B: Living Insight 뉴스 소스
"""
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional
from email.utils import parsedate_to_datetime

import feedparser
import httpx

from app.core.config import settings


class NewsService:
    """뉴스 RSS 수집 서비스"""

    def __init__(self):
        self.rss_urls = [
            url.strip()
            for url in settings.NEWS_RSS_URLS.split(",")
            if url.strip()
        ]

    async def fetch_news(
        self,
        query: Optional[str] = None,
        tickers: Optional[List[str]] = None,
        max_items: int = 20,
    ) -> List[Dict]:
        """
        뉴스 RSS에서 최신 기사 수집
        query가 있으면 Google News 검색 RSS 사용
        """
        urls = list(self.rss_urls)

        if query:
            encoded_query = query.replace(" ", "+")
            urls.append(
                f"https://news.google.com/rss/search?q={encoded_query}&hl=ko&gl=KR&ceid=KR:ko"
            )

        if tickers:
            for ticker in tickers:
                urls.append(
                    f"https://news.google.com/rss/search?q={ticker}+주식&hl=ko&gl=KR&ceid=KR:ko"
                )

        all_items = []
        async with httpx.AsyncClient(timeout=15.0) as client:
            for url in urls:
                try:
                    response = await client.get(url)
                    feed = feedparser.parse(response.text)
                    for entry in feed.entries[:max_items]:
                        item = self._parse_entry(entry)
                        if item:
                            all_items.append(item)
                except Exception:
                    continue

        # 중복 제거 (제목 기준) + 최신순 정렬
        seen_titles = set()
        unique_items = []
        for item in all_items:
            if item["title"] not in seen_titles:
                seen_titles.add(item["title"])
                unique_items.append(item)

        unique_items.sort(
            key=lambda x: x.get("published_at") or "", reverse=True
        )
        return unique_items[:max_items]

    def _parse_entry(self, entry) -> Optional[Dict]:
        """RSS 엔트리 → 이벤트 형태로 변환"""
        try:
            published_at = None
            if hasattr(entry, "published"):
                try:
                    published_at = parsedate_to_datetime(entry.published).isoformat()
                except Exception:
                    published_at = entry.published

            return {
                "source": "news",
                "source_id": getattr(entry, "id", getattr(entry, "link", "")),
                "title": getattr(entry, "title", "제목 없음"),
                "content": getattr(entry, "summary", ""),
                "url": getattr(entry, "link", ""),
                "tickers": None,
                "published_at": published_at,
                "metadata_json": json.dumps({
                    "source_feed": getattr(entry, "source", {}).get("title", ""),
                }),
            }
        except Exception:
            return None


# 싱글턴
news_service = NewsService()
