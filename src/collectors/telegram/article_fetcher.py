"""기사 URL 추출 및 본문 크롤링.

텔레그램 메시지 HTML/텍스트에서 외부 링크를 추출하고,
기사 본문을 trafilatura로 파싱해 반환한다.

스킵 규칙:
- t.me, telegram.me 링크 (텔레그램 내부)
- 이미지/동영상 확장자 (jpg, png, mp4, ...)
- 유튜브, 트위터/X, 인스타그램 등 SNS
- 파일 다운로드 링크 (pdf, zip 등)
"""

import re
import time
from typing import List, Optional
from urllib.parse import urlparse

import httpx
import trafilatura

from src.utils.logger import LoggerMixin

# 스킵할 도메인 패턴
_SKIP_DOMAINS = re.compile(
    r"(t\.me|telegram\.me|youtube\.com|youtu\.be|twitter\.com|x\.com"
    r"|instagram\.com|facebook\.com|linkedin\.com|tiktok\.com"
    r"|reddit\.com|naver\.blog|blog\.naver\.com)",
    re.IGNORECASE,
)

# 스킵할 파일 확장자
_SKIP_EXTENSIONS = re.compile(
    r"\.(jpg|jpeg|png|gif|webp|svg|mp4|mov|avi|mp3|pdf|zip|rar|exe|dmg)$",
    re.IGNORECASE,
)

# HTML에서 href 추출 (raw_html용)
_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)

# 텍스트에서 URL 추출
_URL_RE = re.compile(r"https?://[^\s\)\]\>\"\']+")


def extract_urls(text: Optional[str], raw_html: Optional[str] = None) -> List[str]:
    """메시지 텍스트/HTML에서 외부 기사 URL만 추출한다."""
    candidates: List[str] = []

    if raw_html:
        candidates += _HREF_RE.findall(raw_html)
    if text:
        candidates += _URL_RE.findall(text)

    seen: set = set()
    result: List[str] = []
    for url in candidates:
        url = url.rstrip(".,;)")
        if url in seen:
            continue
        seen.add(url)

        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            continue
        if _SKIP_DOMAINS.search(parsed.netloc):
            continue
        if _SKIP_EXTENSIONS.search(parsed.path):
            continue

        result.append(url)

    return result


class ArticleFetcher(LoggerMixin):
    """외부 URL에서 기사 본문을 가져온다."""

    # 기사 1건당 최대 대기 (초)
    REQUEST_TIMEOUT = 15
    # 기사 요청 사이 최소 대기 (봇 탐지 회피)
    MIN_DELAY = 1.0

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    }

    def __init__(self):
        self._client = httpx.Client(
            headers=self.HEADERS,
            timeout=self.REQUEST_TIMEOUT,
            follow_redirects=True,
        )
        self._last_request: float = 0.0

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def fetch_article_text(self, url: str) -> Optional[str]:
        """URL에서 기사 본문 텍스트를 추출한다. 실패 시 None."""
        elapsed = time.time() - self._last_request
        if elapsed < self.MIN_DELAY:
            time.sleep(self.MIN_DELAY - elapsed)

        try:
            resp = self._client.get(url)
            self._last_request = time.time()

            if resp.status_code != 200:
                self.logger.debug("article_fetch_skip", url=url, status=resp.status_code)
                return None

            text = trafilatura.extract(
                resp.text,
                include_comments=False,
                include_tables=True,
                no_fallback=False,
            )
            if text:
                self.logger.debug("article_fetched", url=url, chars=len(text))
            else:
                self.logger.debug("article_empty", url=url)
            return text

        except Exception as e:
            self._last_request = time.time()
            self.logger.debug("article_fetch_error", url=url, error=str(e))
            return None

    def enrich_messages(self, messages: list) -> list:
        """메시지 리스트에 linked_article_text 필드를 채워 반환한다.

        각 메시지의 content + raw_html에서 URL을 추출하고,
        기사 본문을 가져와 '\\n\\n---\\n\\n' 구분자로 이어붙인다.
        """
        for msg in messages:
            urls = extract_urls(msg.get("content"), msg.get("raw_html"))
            if not urls:
                msg.setdefault("linked_article_text", None)
                continue

            parts: List[str] = []
            for url in urls:
                text = self.fetch_article_text(url)
                if text:
                    parts.append(f"[{url}]\n{text}")

            msg["linked_article_text"] = "\n\n---\n\n".join(parts) if parts else None

        return messages
