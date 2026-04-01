"""Telegram public channel scraper using t.me/s/ web preview.

스크래핑 주의사항:
- t.me/s/{username} : 공개 채널의 웹 미리보기 엔드포인트 (인증 불필요)
- 페이지당 약 20개 메시지 표시, ?before={msg_id} 로 이전 메시지 조회
- Rate limit: 요청 간 최소 5초 이상 랜덤 대기 (Telegram은 자동화 탐지 적극적)
- 429 응답 시 60초 이상 대기 후 재시도
- 봇 차단 방지: 실제 브라우저 UA 사용, Accept 헤더 포함
- 메시지 없는 페이지(비공개/삭제된 채널) 조기 종료
- 이미 수집된 msg_id 도달 시 조기 종료 (증분 수집)
"""

import random
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup

from src.utils.logger import LoggerMixin


class TelegramScrapeError(Exception):
    pass


class TelegramCollector(LoggerMixin):
    """Scrapes messages from public Telegram channels via t.me/s/."""

    BASE_URL = "https://t.me/s/{username}"
    # 요청 간 대기: 5~10초 랜덤 (탐지 회피)
    MIN_DELAY = 5.0
    MAX_DELAY = 10.0

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "DNT": "1",
    }

    def __init__(self, max_retries: int = 3, timeout: int = 30):
        self.max_retries = max_retries
        self.client = httpx.Client(
            headers=self.HEADERS,
            timeout=timeout,
            follow_redirects=True,
        )
        self._last_request_time: float = 0.0

    # ── Public API ───────────────────────────────────────────────────────

    def collect(
        self,
        channel_id: int,
        username: str,
        since_msg_id: Optional[int] = None,
        max_pages: int = 10,
    ) -> List[Dict[str, Any]]:
        """증분 수집: since_msg_id 이후 새 메시지만 가져온다.

        Args:
            channel_id: DB의 telegram_channels.id
            username: 채널 username (@없이)
            since_msg_id: 이 ID보다 큰 메시지만 수집. None이면 최신 1페이지만.
            max_pages: 최대 페이지 수.

        Returns:
            수집된 메시지 dict 리스트
        """
        username = username.lstrip("@").strip()
        all_messages: List[Dict[str, Any]] = []
        before_msg_id: Optional[int] = None
        pages_fetched = 0

        self.logger.info("collect_start", username=username, since_msg_id=since_msg_id, max_pages=max_pages)

        while pages_fetched < max_pages:
            html, status = self._fetch_page(username, before_msg_id)
            pages_fetched += 1

            if status == 404:
                self.logger.warning("channel_not_found", username=username)
                break
            if html is None:
                self.logger.error("fetch_failed", username=username, before=before_msg_id)
                break

            messages, oldest_msg_id = self._parse_messages(html, channel_id)

            if not messages:
                self.logger.info("no_messages_on_page", username=username, before=before_msg_id)
                break

            if since_msg_id is not None:
                new_messages = [m for m in messages if m["telegram_msg_id"] > since_msg_id]
                all_messages.extend(new_messages)
                if len(new_messages) < len(messages):
                    self.logger.info("reached_known_messages", username=username, since_msg_id=since_msg_id)
                    break
            else:
                all_messages.extend(messages)
                break  # since_msg_id 없으면 최신 1페이지만

            if oldest_msg_id is None:
                break
            before_msg_id = oldest_msg_id

        self.logger.info("collect_done", username=username, count=len(all_messages), pages=pages_fetched)
        return all_messages

    def backfill(
        self,
        channel_id: int,
        username: str,
        until_msg_id: Optional[int] = None,
        until_date: Optional[datetime] = None,
        max_pages: int = 50,
        save_callback=None,
    ) -> int:
        """과거 메시지 전체 수집 (백필).

        최신 → 과거 방향으로 페이지를 순회하며 모든 메시지를 가져온다.
        메시지 수가 많을 수 있으므로 페이지마다 save_callback을 호출해 즉시 저장한다.

        Args:
            channel_id: DB의 telegram_channels.id
            username: 채널 username (@없이)
            until_msg_id: 이 ID 이하에 도달하면 중단.
            until_date: 이 시점보다 오래된 메시지에 도달하면 중단 (timezone-aware 권장).
            max_pages: 최대 페이지 수 (1페이지 ≈ 20개 메시지).
            save_callback: save_callback(messages) 형태의 저장 함수.

        Returns:
            총 수집 메시지 수
        """
        username = username.lstrip("@").strip()
        total = 0
        before_msg_id: Optional[int] = None
        pages_fetched = 0

        # until_date를 timezone-aware로 통일
        if until_date is not None and until_date.tzinfo is None:
            until_date = until_date.replace(tzinfo=timezone.utc)

        self.logger.info(
            "backfill_start",
            username=username,
            until_msg_id=until_msg_id,
            until_date=str(until_date) if until_date else None,
            max_pages=max_pages,
        )

        while pages_fetched < max_pages:
            html, status = self._fetch_page(username, before_msg_id)
            pages_fetched += 1

            if status == 404:
                self.logger.warning("channel_not_found", username=username)
                break
            if html is None:
                self.logger.error("fetch_failed", username=username, before=before_msg_id)
                break

            messages, oldest_msg_id = self._parse_messages(html, channel_id)

            if not messages:
                self.logger.info("backfill_no_more_messages", username=username, pages=pages_fetched)
                break

            filtered, reached_end = self._apply_until_filters(messages, until_msg_id, until_date)

            if filtered:
                if save_callback:
                    save_callback(filtered)
                total += len(filtered)
                self.logger.info(
                    "backfill_page_done",
                    username=username,
                    page=pages_fetched,
                    page_count=len(filtered),
                    total=total,
                    oldest_msg_id=oldest_msg_id,
                )

            if reached_end:
                self.logger.info(
                    "backfill_reached_until",
                    username=username,
                    until_msg_id=until_msg_id,
                    until_date=str(until_date) if until_date else None,
                )
                break

            if oldest_msg_id is None:
                break
            before_msg_id = oldest_msg_id

        self.logger.info("backfill_done", username=username, total=total, pages=pages_fetched)
        return total

    @staticmethod
    def _apply_until_filters(
        messages: List[Dict[str, Any]],
        until_msg_id: Optional[int],
        until_date: Optional[datetime],
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """until_msg_id / until_date 조건으로 메시지를 필터링한다.

        Returns:
            (통과한 메시지 목록, 하한선에 도달했는지 여부)
        """
        filtered = list(messages)
        reached_end = False

        if until_msg_id is not None:
            before_count = len(filtered)
            filtered = [m for m in filtered if m["telegram_msg_id"] > until_msg_id]
            if len(filtered) < before_count:
                reached_end = True

        if until_date is not None:
            before_count = len(filtered)
            kept = []
            for m in filtered:
                posted = m.get("posted_at")
                if posted is None:
                    kept.append(m)
                    continue
                # posted_at을 timezone-aware로 통일
                if posted.tzinfo is None:
                    posted = posted.replace(tzinfo=timezone.utc)
                if posted >= until_date:
                    kept.append(m)
                else:
                    reached_end = True  # 이 메시지 이후론 모두 오래됨
            filtered = kept

        return filtered, reached_end

    def fetch_channel_meta(self, username: str) -> Dict[str, Any]:
        """채널 메타데이터(이름, 설명, 구독자 수)를 조회한다."""
        username = username.lstrip("@").strip()
        html, status = self._fetch_page(username, before_msg_id=None)
        if html is None or status != 200:
            raise TelegramScrapeError(f"Cannot fetch metadata for @{username} (status={status})")
        return self._parse_channel_meta(html, username)

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ── Internal helpers ─────────────────────────────────────────────────

    def _wait(self) -> None:
        """요청 간 랜덤 대기."""
        elapsed = time.time() - self._last_request_time
        delay = random.uniform(self.MIN_DELAY, self.MAX_DELAY)
        wait = max(0.0, delay - elapsed)
        if wait > 0:
            time.sleep(wait)
        self._last_request_time = time.time()

    def _fetch_page(
        self,
        username: str,
        before_msg_id: Optional[int],
    ) -> Tuple[Optional[str], int]:
        """t.me/s/{username}[?before=N] 를 가져온다. (html, status_code) 반환."""
        url = self.BASE_URL.format(username=username)
        params = {}
        if before_msg_id is not None:
            params["before"] = before_msg_id

        for attempt in range(self.max_retries):
            self._wait()
            try:
                resp = self.client.get(url, params=params)
                self.logger.debug("page_fetched", url=resp.url, status=resp.status_code)

                if resp.status_code == 200:
                    return resp.text, 200
                if resp.status_code == 404:
                    return None, 404
                if resp.status_code == 429:
                    # Telegram rate limit: 60초 이상 대기
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    self.logger.warning("rate_limited", retry_after=retry_after)
                    time.sleep(retry_after)
                    continue
                # 기타 에러
                self.logger.warning("unexpected_status", status=resp.status_code, attempt=attempt + 1)
                time.sleep(2 ** attempt)

            except (httpx.RequestError, httpx.TimeoutException) as e:
                self.logger.warning("request_error", error=str(e), attempt=attempt + 1)
                time.sleep(2 ** attempt)

        return None, 0

    def _parse_messages(
        self,
        html: str,
        channel_id: int,
    ) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        """HTML에서 메시지 목록과 가장 오래된 msg_id를 파싱한다."""
        soup = BeautifulSoup(html, "html.parser")
        wraps = soup.select(".tgme_widget_message_wrap")

        messages: List[Dict[str, Any]] = []
        oldest_msg_id: Optional[int] = None

        for wrap in wraps:
            msg_div = wrap.select_one(".tgme_widget_message")
            if msg_div is None:
                continue

            # data-post="channelname/12345"
            data_post = msg_div.get("data-post", "")
            msg_id = self._extract_msg_id(data_post)
            if msg_id is None:
                continue

            if oldest_msg_id is None or msg_id < oldest_msg_id:
                oldest_msg_id = msg_id

            content = self._extract_text(msg_div)
            posted_at = self._extract_datetime(msg_div)
            views = self._extract_views(msg_div)
            has_media = self._has_media(msg_div)
            raw_html = str(wrap)

            messages.append({
                "channel_id": channel_id,
                "telegram_msg_id": msg_id,
                "content": content,
                "posted_at": posted_at,
                "views": views,
                "has_media": has_media,
                "raw_html": raw_html,
            })

        return messages, oldest_msg_id

    def _parse_channel_meta(self, html: str, username: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")

        channel_name = None
        name_el = soup.select_one(".tgme_channel_info_header_title")
        if name_el:
            channel_name = name_el.get_text(strip=True)

        description = None
        desc_el = soup.select_one(".tgme_channel_info_description")
        if desc_el:
            description = desc_el.get_text(strip=True)

        subscriber_count = None
        for counter in soup.select(".tgme_channel_info_counter"):
            value_el = counter.select_one(".counter_value")
            type_el = counter.select_one(".counter_type")
            if value_el and type_el and "subscriber" in type_el.get_text().lower():
                raw = value_el.get_text(strip=True).replace(",", "")
                try:
                    if raw.endswith("K"):
                        subscriber_count = int(float(raw[:-1]) * 1_000)
                    elif raw.endswith("M"):
                        subscriber_count = int(float(raw[:-1]) * 1_000_000)
                    else:
                        subscriber_count = int(raw)
                except ValueError:
                    pass

        return {
            "username": username,
            "channel_name": channel_name,
            "description": description,
            "subscriber_count": subscriber_count,
            "channel_url": f"https://t.me/s/{username}",
        }

    # ── Parsers ──────────────────────────────────────────────────────────

    @staticmethod
    def _extract_msg_id(data_post: str) -> Optional[int]:
        m = re.search(r"/(\d+)$", data_post)
        return int(m.group(1)) if m else None

    @staticmethod
    def _extract_text(msg_div) -> Optional[str]:
        el = msg_div.select_one(".tgme_widget_message_text")
        if el is None:
            return None
        # <br> → 줄바꿈 변환
        for br in el.find_all("br"):
            br.replace_with("\n")
        return el.get_text(separator="", strip=False).strip() or None

    @staticmethod
    def _extract_datetime(msg_div) -> Optional[datetime]:
        time_el = msg_div.select_one("time.time")
        if time_el is None:
            return None
        dt_str = time_el.get("datetime", "")
        try:
            return datetime.fromisoformat(dt_str)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _extract_views(msg_div) -> Optional[int]:
        el = msg_div.select_one(".tgme_widget_message_views")
        if el is None:
            return None
        raw = el.get_text(strip=True).replace(",", "")
        # "1.2K" → 1200, "3.4M" → 3400000
        try:
            if raw.endswith("K"):
                return int(float(raw[:-1]) * 1000)
            if raw.endswith("M"):
                return int(float(raw[:-1]) * 1_000_000)
            return int(raw)
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _has_media(msg_div) -> bool:
        return bool(
            msg_div.select_one(".tgme_widget_message_photo")
            or msg_div.select_one(".tgme_widget_message_video")
            or msg_div.select_one(".tgme_widget_message_document")
        )
