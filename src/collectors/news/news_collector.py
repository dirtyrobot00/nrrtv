"""Naver Finance news collector module.

DB-first news collector that saves articles directly to the `news_articles`
table via NewsStore, without writing HTML files to disk.
"""

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup

from src.collectors.financial_report.base import BaseCollector, CollectorError
from src.models.document import Document, DocumentType
from src.storage.news_store import NewsStore
from src.utils.config import get_config


class NaverFinanceNewsCollector(BaseCollector):
    """Naver Finance 뉴스 수집기.

    뉴스 기사를 수집하여 news_articles 테이블에 직접 저장합니다.
    HTML 파일은 저장하지 않습니다.

    Usage:
        with NaverFinanceNewsCollector() as collector:
            # 최신 기사 수집 (첫 중복 URL에서 정지)
            articles = collector.collect_realtime(limit=50)

            # 날짜 범위 수집
            articles = collector.collect_backfill(
                from_date=datetime(2026, 1, 1),
                to_date=datetime(2026, 4, 7),
            )
    """

    def __init__(
        self,
        source_name: str = "naver_finance_news",
        rate_limit: float = 2.0,
        database_url: Optional[str] = None,
    ):
        config = get_config()
        self.source_config = config.get_source("news_sources", source_name)

        if not self.source_config:
            raise CollectorError(f"News source not found: {source_name}")

        if not self.source_config.get("enabled", False):
            raise CollectorError(f"News source is disabled: {source_name}")

        # BaseCollector 초기화 (httpx client, rate limiting 상속)
        super().__init__(
            source_name=source_name,
            output_dir=Path("data/raw/html"),  # 미사용이지만 BaseCollector 요구
            rate_limit=rate_limit or self.source_config.get("rate_limit", 2.0),
        )

        # DB 저장 전담 (DocumentStore는 사용하지 않음)
        self.news_store = NewsStore(database_url)

        self.base_url = self.source_config.get("base_url")
        self.selectors = self.source_config.get("selectors", {})
        self.params = self.source_config.get("params", {})

        self.logger.info(
            "news_collector_initialized",
            source=source_name,
            base_url=self.base_url,
        )

    # ── Abstract method 충족 ─────────────────────────────────────────────

    def collect(self, limit: Optional[int] = None) -> List[Dict]:
        """collect_realtime() 위임 (abstract 충족용)."""
        return self.collect_realtime(limit=limit or 50)

    # ── 공개 수집 메서드 ─────────────────────────────────────────────────

    def collect_realtime(
        self,
        limit: int = 50,
        ticker: Optional[str] = None,
    ) -> List[Dict]:
        """최신 뉴스 수집.

        Naver Finance 뉴스를 최신순으로 수집합니다.
        이미 DB에 있는 URL(중복)이 발견되면 즉시 중단합니다.

        Args:
            limit: 최대 수집 기사 수
            ticker: 종목코드 (예: 005930). 지정 시 해당 종목 뉴스만 수집.

        Returns:
            수집된 기사 dict 리스트
        """
        self.logger.info("realtime_started", limit=limit, ticker=ticker)

        collected: List[Dict] = []
        seen_urls: set = set()  # 현재 실행 중 수집된 URL 추적
        page = 1
        max_pages_safety = 20
        stop = False

        while not stop and page <= max_pages_safety:
            page_url = self._build_page_url(page, ticker)
            self.logger.info("scraping_page", page=page, url=page_url)

            try:
                response = self._make_request(page_url)
                articles_on_page = self._parse_article_list(response.text, page_url)
            except CollectorError as e:
                self.logger.error("page_fetch_failed", page=page, error=str(e))
                break

            if not articles_on_page:
                self.logger.info("no_articles_on_page", page=page)
                break

            for article_info in articles_on_page:
                url = article_info["url"]

                # 현재 실행 중 이미 수집한 URL → skip
                if url in seen_urls:
                    self.logger.debug("skipping_seen_in_run", url=url)
                    continue

                # 첫 중복 URL (DB) → realtime 정지점
                if self.news_store.exists(url):
                    self.logger.info("duplicate_found_stopping", url=url)
                    stop = True
                    break

                if len(collected) >= limit:
                    self.logger.info("limit_reached", collected=len(collected))
                    stop = True
                    break

                article_dict = self._download_article(article_info, ticker)
                if article_dict:
                    collected.append(article_dict)
                    seen_urls.add(url)
                    self.logger.info(
                        "article_collected",
                        title=article_info.get("title", "")[:50],
                    )

            if not stop:
                page += 1

        inserted = self.news_store.save_articles(collected)
        self.logger.info(
            "realtime_completed",
            total_collected=len(collected),
            inserted_to_db=inserted,
        )
        return collected

    def collect_backfill(
        self,
        from_date: datetime,
        to_date: Optional[datetime] = None,
        max_pages: int = 20,
        limit: Optional[int] = None,
        ticker: Optional[str] = None,
    ) -> List[Dict]:
        """과거 뉴스 수집 (백필).

        from_date ~ to_date 범위의 날짜를 역순(최신→과거)으로 순회하며,
        날짜별 URL (?date=YYYYMMDD&page=N)을 사용해 해당 날짜의 모든 기사를 수집합니다.
        이미 DB에 있는 URL은 건너뛰지만 중단하지 않습니다 (gap-fill).

        Args:
            from_date: 수집 시작 날짜 (이 날짜 이후 기사만 수집)
            to_date: 수집 종료 날짜 (default: 오늘)
            max_pages: 날짜당 최대 페이지 수
            limit: 최대 수집 기사 총합
            ticker: 종목코드

        Returns:
            수집된 기사 dict 리스트
        """
        if to_date is None:
            to_date = datetime.now()

        self.logger.info(
            "backfill_started",
            from_date=from_date.strftime("%Y-%m-%d"),
            to_date=to_date.strftime("%Y-%m-%d"),
            max_pages_per_day=max_pages,
            ticker=ticker,
        )

        collected: List[Dict] = []
        seen_urls: set = set()
        stop = False

        # to_date부터 from_date까지 하루씩 역순 순회
        current_date = to_date.replace(hour=0, minute=0, second=0, microsecond=0)
        from_date_only = from_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while not stop and current_date >= from_date_only:
            date_str = current_date.strftime("%Y%m%d")
            self.logger.info("backfill_date", date=date_str)

            for page in range(1, max_pages + 1):
                if stop:
                    break

                page_url = self._build_page_url_with_date(date_str, page, ticker)
                self.logger.info("scraping_page", date=date_str, page=page, url=page_url)

                try:
                    response = self._make_request(page_url)
                    articles_on_page = self._parse_article_list(response.text, page_url)
                except CollectorError as e:
                    self.logger.error("page_fetch_failed", date=date_str, page=page, error=str(e))
                    break

                if not articles_on_page:
                    self.logger.info("no_articles_on_page", date=date_str, page=page)
                    break

                for article_info in articles_on_page:
                    url = article_info["url"]

                    if url in seen_urls:
                        self.logger.debug("skipping_seen_in_run", url=url)
                        continue

                    # 중복 URL (DB) → skip (gap-fill: 중단 아님)
                    if self.news_store.exists(url):
                        self.logger.debug("skipping_duplicate", url=url)
                        seen_urls.add(url)
                        continue

                    if limit and len(collected) >= limit:
                        self.logger.info("limit_reached", collected=len(collected))
                        stop = True
                        break

                    article_dict = self._download_article(article_info, ticker)
                    if article_dict:
                        collected.append(article_dict)
                        seen_urls.add(url)
                        self.logger.info(
                            "article_collected",
                            title=article_info.get("title", "")[:50],
                            date=date_str,
                        )

            current_date -= timedelta(days=1)

        inserted = self.news_store.save_articles(collected)
        self.logger.info(
            "backfill_completed",
            total_collected=len(collected),
            inserted_to_db=inserted,
        )
        return collected

    # ── 내부 파싱/다운로드 메서드 ────────────────────────────────────────

    def _build_page_url_with_date(self, date_str: str, page: int, ticker: Optional[str] = None) -> str:
        """날짜 기반 페이지 URL 생성. date_str: 'YYYYMMDD'"""
        base = self.base_url
        params = [f"date={date_str}"]
        if ticker:
            base = "https://finance.naver.com/item/news_news.naver"
            params.append(f"code={ticker}")
        if page > 1:
            params.append(f"page={page}")
        return f"{base}?{'&'.join(params)}"

    def _build_page_url(self, page: int, ticker: Optional[str] = None) -> str:
        """페이지 URL 생성."""
        url = self.base_url
        pagination = self.source_config.get("pagination", {})

        params = []
        for key, value in self.params.items():
            params.append(f"{key}={value}")

        if ticker:
            # 종목별 뉴스는 별도 URL 사용
            url = "https://finance.naver.com/item/news_news.naver"
            params.append(f"code={ticker}")

        if pagination.get("enabled", False) and page > 1:
            page_param = pagination.get("page_param", "page")
            params.append(f"{page_param}={page}")

        if params:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{'&'.join(params)}"

        return url

    def _parse_article_list(self, html_content: str, page_url: str) -> List[Dict]:
        """HTML 목록 페이지에서 기사 정보 추출."""
        soup = BeautifulSoup(html_content, "html.parser")
        articles = []

        list_selector = self.selectors.get("article_list")
        if not list_selector:
            raise CollectorError("Missing selector: article_list")

        elements = soup.select(list_selector)
        self.logger.debug("article_elements_found", count=len(elements))

        for element in elements:
            try:
                info = self._extract_article_info(element, page_url)
                if info and info.get("url"):
                    articles.append(info)
            except Exception as e:
                self.logger.warning("failed_to_parse_element", error=str(e))
                continue

        return articles

    def _extract_article_info(self, element, base_url: str) -> Optional[Dict]:
        """HTML 요소에서 기사 기본 정보 추출."""
        title_selector = self.selectors.get("title")

        # 제목/링크 추출: 셀렉터로 찾되 텍스트 없으면(썸네일 등) 다음 a 태그 시도
        link_elem = None
        title = None
        if title_selector:
            for candidate in element.select(title_selector.split(",")[0].strip()):
                text = candidate.get_text(strip=True)
                if text:
                    link_elem = candidate
                    title = text
                    break
        if not link_elem:
            # Fallback: 텍스트가 있는 첫 번째 a 태그 (ul.right_list_1_2 li 대응)
            for a in element.select("a"):
                text = a.get_text(strip=True)
                if text:
                    link_elem = a
                    title = text
                    break

        if not title or not link_elem:
            return None

        article_url = link_elem.get("href")
        if not article_url:
            return None

        article_url = urljoin(base_url, article_url)

        date_selector = self.selectors.get("date")
        date_elem = element.select_one(date_selector) if date_selector else None
        date_str = date_elem.get_text(strip=True) if date_elem else None

        summary_selector = self.selectors.get("summary")
        summary_elem = element.select_one(summary_selector) if summary_selector else None
        summary = summary_elem.get_text(strip=True) if summary_elem else None

        return {
            "url": article_url,
            "title": title,
            "date_str": date_str,
            "summary": summary,
        }

    def _resolve_js_redirect(self, html: str) -> Optional[str]:
        """JS 리다이렉트 URL 추출. finance.naver.com → n.news.naver.com 처리."""
        m = re.search(r"top\.location\.href\s*=\s*['\"]([^'\"]+)['\"]", html)
        return m.group(1) if m else None

    def _download_article(
        self,
        article_info: Dict,
        ticker: Optional[str] = None,
    ) -> Optional[Dict]:
        """기사 페이지 다운로드 및 파싱. DB 저장용 dict 반환 (파일 저장 없음)."""
        url = article_info["url"]

        try:
            response = self._make_request(url)
        except CollectorError as e:
            self.logger.error("article_fetch_failed", url=url, error=str(e))
            return None

        html = response.text

        # JS 리다이렉트 감지 (finance.naver.com → n.news.naver.com)
        redirect_url = self._resolve_js_redirect(html)
        if redirect_url:
            self.logger.debug("js_redirect_followed", from_url=url, to_url=redirect_url)
            try:
                response = self._make_request(redirect_url)
                html = response.text
            except CollectorError as e:
                self.logger.error("redirect_fetch_failed", url=redirect_url, error=str(e))
                return None
        content, author, published_at = self._extract_article_content(html)
        media = self._extract_media_from_url(url)

        # ticker 우선순위: 인자 > URL에서 추출
        resolved_ticker = ticker or self._extract_ticker_from_url(url)

        return {
            "url": url,
            "title": article_info["title"],
            "content": content,
            "summary": article_info.get("summary"),
            "author": author,
            "media": media,
            "ticker": resolved_ticker,
            "published_at": published_at,
            "raw_html": html,
        }

    def _extract_article_content(self, html_content: str):
        """기사 페이지 HTML에서 본문, 저자, 발행일 추출.

        Returns:
            (content, author, published_at) 튜플
        """
        soup = BeautifulSoup(html_content, "html.parser")
        content = None
        author = None
        published_at = None

        # 본문 추출
        content_selectors = [
            "#newsct_article",       # Naver news (현재)
            "#articleBodyContents",  # Naver news (구)
            ".article_body",
            ".article_content",
            "#article-view-content-div",
        ]
        for selector in content_selectors:
            elem = soup.select_one(selector)
            if elem:
                for tag in elem.find_all(["script", "style"]):
                    tag.decompose()
                content = elem.get_text(strip=True)
                break

        # 저자 추출
        author_selectors = [
            ".media_end_head_journalist_name",
            ".byline",
            ".reporter",
            ".journalist_name",
            ".article_info .author",
        ]
        for selector in author_selectors:
            elem = soup.select_one(selector)
            if elem:
                author = elem.get_text(strip=True)
                break

        # 발행일 추출
        date_selectors = [
            ".media_end_head_info_datestamp_time[data-date-time]",
            ".media_end_head_info_datestamp_time",
            ".article_info .date",
            "span[class*='date']",
            "time[datetime]",
            "time",
        ]
        raw_date_str = None
        for selector in date_selectors:
            elem = soup.select_one(selector)
            if elem:
                # data-date-time 속성 우선
                raw_date_str = elem.get("data-date-time") or elem.get("datetime") or elem.get_text(strip=True)
                if raw_date_str:
                    break

        if raw_date_str:
            published_at = self._parse_korean_datetime(raw_date_str)

        return content, author, published_at

    # ── 날짜 파싱 ────────────────────────────────────────────────────────

    def _parse_korean_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """한국어/Naver 날짜 문자열을 datetime으로 변환.

        지원 포맷:
        - 2026.04.08 14:30
        - 2026.04.08 14:30:00
        - 2026-04-08T14:30:00 (ISO)
        - 2026년 04월 08일 14시 30분
        - 04월 08일 14:30 (당해 연도)
        - 30분 전 / 3시간 전 (상대 시간)
        """
        if not date_str:
            return None

        date_str = date_str.strip()

        # ISO 포맷: 2026-04-08T14:30:00 또는 2026-04-08 14:30:00 (공백 구분)
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?", date_str)
        if m:
            Y, Mo, D, H, Mi, S = m.groups()
            return datetime(int(Y), int(Mo), int(D), int(H), int(Mi), int(S or 0))

        # YYYY.MM.DD HH:MM[:SS]
        m = re.match(r"(\d{4})\.(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?", date_str)
        if m:
            Y, Mo, D, H, Mi, S = m.groups()
            return datetime(int(Y), int(Mo), int(D), int(H), int(Mi), int(S or 0))

        # YYYY.MM.DD (시간 없음)
        m = re.match(r"(\d{4})\.(\d{1,2})\.(\d{1,2})\s*$", date_str)
        if m:
            Y, Mo, D = m.groups()
            return datetime(int(Y), int(Mo), int(D))

        # YYYY년 MM월 DD일 HH시 MM분
        m = re.match(r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*(\d{1,2})시\s*(\d{1,2})분", date_str)
        if m:
            Y, Mo, D, H, Mi = m.groups()
            return datetime(int(Y), int(Mo), int(D), int(H), int(Mi))

        # MM월 DD일 HH:MM (당해 연도)
        m = re.match(r"(\d{1,2})월\s*(\d{1,2})일\s+(\d{1,2}):(\d{2})", date_str)
        if m:
            Mo, D, H, Mi = m.groups()
            now = datetime.now()
            return datetime(now.year, int(Mo), int(D), int(H), int(Mi))

        # X분 전
        m = re.match(r"(\d+)분\s*전", date_str)
        if m:
            return datetime.now() - timedelta(minutes=int(m.group(1)))

        # X시간 전
        m = re.match(r"(\d+)시간\s*전", date_str)
        if m:
            return datetime.now() - timedelta(hours=int(m.group(1)))

        self.logger.warning("unparseable_date_string", date_str=date_str)
        return None

    # ── URL 파싱 헬퍼 ────────────────────────────────────────────────────

    def _extract_ticker_from_url(self, url: str) -> Optional[str]:
        """URL 쿼리스트링에서 종목코드 추출 (code= 파라미터)."""
        try:
            qs = parse_qs(urlparse(url).query)
            return qs.get("code", [None])[0]
        except Exception:
            return None

    def _extract_media_from_url(self, url: str) -> Optional[str]:
        """URL에서 언론사 식별자 추출 (oid= 또는 office_id= 파라미터)."""
        try:
            qs = parse_qs(urlparse(url).query)
            # finance.naver.com: office_id= / n.news.naver.com 경로: /mnews/article/{oid}/...
            oid = qs.get("oid", [None])[0] or qs.get("office_id", [None])[0]
            if not oid:
                # n.news.naver.com/mnews/article/008/0005342195 형태
                m = re.search(r"/article/(\d+)/", url)
                oid = m.group(1) if m else None
            return f"naver_oid_{oid}" if oid else "naver_finance"
        except Exception:
            return "naver_finance"
