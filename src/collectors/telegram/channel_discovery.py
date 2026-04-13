"""텔레그램 금융 채널 자동 발견 에이전트.

전략:
  1. DB에 등록된 채널(+ 코드 내 시드)의 메시지 1페이지를 훑는다.
  2. 메시지 본문 및 링크에서 @mention / t.me/ 패턴을 추출한다.
  3. 아직 DB에 없는 후보를 대상으로 t.me/s/ 메타데이터를 조회한다.
  4. 한국 주식/금융 키워드 점수를 계산해 임계치 이상이면 DB에 추가한다.

안전 설계:
  - 요청 간 5~10초 랜덤 대기 (TelegramCollector 내장)
  - 한 번 실행당 최대 처리 채널 수를 제한 (max_new_per_run)
  - 과도한 후보 탐색 방지: max_candidates_per_run
  - 429 응답 시 60초 자동 대기 (TelegramCollector 내장)
"""

import re
from typing import Dict, List, Optional, Set

from bs4 import BeautifulSoup

from src.collectors.telegram.telegram_collector import TelegramCollector
from src.storage.telegram_store import TelegramStore
from src.utils.logger import LoggerMixin


# ── 한국 금융 관련 키워드 ─────────────────────────────────────────────────

FINANCE_KEYWORDS: List[str] = [
    # 한국어
    "주식", "증권", "투자", "주가", "코스피", "코스닥", "etf", "펀드",
    "리서치", "애널리스트", "종목", "매수", "매도", "포트폴리오",
    "실적", "배당", "공시", "ipo", "상장", "시황", "전망", "급등",
    "금리", "환율", "채권", "선물", "옵션", "파생",
    "재무", "경제", "기업분석", "차트", "기술적분석", "가치투자",
    "퀀트", "스윙", "단타", "스캘핑", "세력",
    # 영문
    "stock", "finance", "invest", "market", "trading", "equity",
    "macro", "fund", "portfolio",
    # 대형 종목 (채널명에 특정 주식 명칭이 들어가는 경우)
    "삼성", "sk", "현대", "lg", "카카오", "네이버", "셀트리온",
]

# 카테고리별 식별 키워드
CATEGORY_KEYWORDS: Dict[str, List[str]] = {
    "주식실황": ["실황", "실시간", "호가", "체결", "세력", "단타", "스캘핑", "데이트레이딩", "급등", "급락"],
    "종목리서치": ["리서치", "분석", "리포트", "report", "애널리스트", "기업분석", "종목분석", "가치투자", "quant", "퀀트"],
    "매크로": ["매크로", "macro", "금리", "환율", "경기", "경제", "fed", "연준", "시황", "글로벌"],
    "IPO": ["ipo", "공모", "상장", "청약", "공모주", "스팩"],
}

# 분명히 금융이 아닌 채널 필터 (username 패턴)
BLACKLIST_PATTERNS: List[str] = [
    r"^(bot|official|support|help|admin|news|crypto|nft)",
]


# ── 유틸 함수 ────────────────────────────────────────────────────────────

def extract_channel_mentions(text: str) -> Set[str]:
    """텍스트에서 @mention 및 t.me/ 링크를 추출해 username 집합으로 반환."""
    candidates: Set[str] = set()

    # @username 패턴 (최소 4자, 영숫자+밑줄)
    for m in re.finditer(r"@([A-Za-z][A-Za-z0-9_]{3,31})", text):
        candidates.add(m.group(1).lower())

    # t.me/username 패턴 (t.me/s/ 제외)
    for m in re.finditer(r"t\.me/(?!s/)([A-Za-z][A-Za-z0-9_]{3,31})(?:[/?#]|$|\s)", text):
        candidates.add(m.group(1).lower())

    return candidates


def score_relevance(channel_name: Optional[str], description: Optional[str]) -> int:
    """채널 이름/설명으로 한국 금융 관련성 점수(0~100)를 산출한다."""
    text = " ".join(filter(None, [channel_name, description])).lower()
    if not text:
        return 0
    score = sum(10 for kw in FINANCE_KEYWORDS if kw in text)
    return min(score, 100)


def guess_category(channel_name: Optional[str], description: Optional[str]) -> str:
    """채널 이름/설명으로 카테고리를 추측한다."""
    text = " ".join(filter(None, [channel_name, description])).lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return "기타"


def is_blacklisted(username: str) -> bool:
    """블랙리스트 패턴에 해당하는 username 여부."""
    u = username.lower()
    return any(re.match(p, u) for p in BLACKLIST_PATTERNS)


# ── 발견 에이전트 ────────────────────────────────────────────────────────

class ChannelDiscoveryAgent(LoggerMixin):
    """
    DB에 등록된 채널들을 순회하며 새로운 금융 채널을 발견해 DB에 추가한다.

    Args:
        store: TelegramStore 인스턴스
        max_new_per_run: 한 실행에서 최대 추가할 채널 수 (기본 10)
        max_candidates_per_run: 한 실행에서 최대 평가할 후보 수 (기본 50)
        min_relevance_score: DB에 추가하기 위한 최소 관련성 점수 (기본 20)
        seed_channels: 추가 시드 채널 username 목록
    """

    def __init__(
        self,
        store: TelegramStore,
        max_new_per_run: int = 10,
        max_candidates_per_run: int = 50,
        min_relevance_score: int = 20,
        seed_channels: Optional[List[str]] = None,
    ):
        self.store = store
        self.max_new_per_run = max_new_per_run
        self.max_candidates_per_run = max_candidates_per_run
        self.min_relevance_score = min_relevance_score
        self.seed_channels: List[str] = [
            s.lstrip("@").strip() for s in (seed_channels or [])
        ]
        self._collector = TelegramCollector()

    # ── Public ───────────────────────────────────────────────────────────

    def run(self, dry_run: bool = False) -> Dict:
        """채널 발견 사이클 1회 실행. 결과 요약 dict를 반환한다."""
        stats = {
            "discovered": 0,
            "skipped_existing": 0,
            "skipped_irrelevant": 0,
            "skipped_blacklist": 0,
            "errors": 0,
        }

        # DB의 활성 채널 + 시드 = 탐색 출발점
        known: Set[str] = {
            ch["username"].lower()
            for ch in self.store.list_channels(active_only=False)
        }
        sources = list(known) + [s for s in self.seed_channels if s.lower() not in known]

        self.logger.info("discovery_start", sources=len(sources), dry_run=dry_run)

        # ── Step 1: 언급된 채널 username 수집 ────────────────────────────
        candidates: Set[str] = set()
        for username in sources:
            new_mentions = self._scrape_mentions(username, stats)
            candidates |= new_mentions

        # 이미 알고 있는 채널 제거
        all_known_lower = known | {s.lower() for s in self.seed_channels}
        new_candidates = candidates - all_known_lower
        self.logger.info(
            "candidates_collected",
            total_mentions=len(candidates),
            new_candidates=len(new_candidates),
        )

        # ── Step 2: 후보 평가 및 DB 추가 ─────────────────────────────────
        evaluated = 0
        for username in sorted(new_candidates):
            if stats["discovered"] >= self.max_new_per_run:
                self.logger.info("max_new_reached", limit=self.max_new_per_run)
                break
            if evaluated >= self.max_candidates_per_run:
                self.logger.info("max_candidates_reached", limit=self.max_candidates_per_run)
                break

            evaluated += 1

            if is_blacklisted(username):
                stats["skipped_blacklist"] += 1
                self.logger.debug("blacklisted", username=username)
                continue

            if username in all_known_lower:
                stats["skipped_existing"] += 1
                continue

            # 메타데이터 조회
            try:
                meta = self._collector.fetch_channel_meta(username)
            except Exception as e:
                self.logger.warning("meta_fetch_failed", username=username, error=str(e))
                stats["errors"] += 1
                continue

            # 관련성 점수
            score = score_relevance(meta.get("channel_name"), meta.get("description"))
            if score < self.min_relevance_score:
                stats["skipped_irrelevant"] += 1
                self.logger.debug(
                    "irrelevant",
                    username=username,
                    score=score,
                    name=meta.get("channel_name"),
                )
                continue

            category = guess_category(meta.get("channel_name"), meta.get("description"))
            self.logger.info(
                "channel_discovered",
                username=username,
                name=meta.get("channel_name"),
                score=score,
                category=category,
                subscribers=meta.get("subscriber_count"),
                dry_run=dry_run,
            )

            if not dry_run:
                try:
                    self.store.add_channel(
                        username=username,
                        channel_name=meta.get("channel_name"),
                        category=category,
                        description=meta.get("description"),
                        subscriber_count=meta.get("subscriber_count"),
                    )
                    all_known_lower.add(username)
                    stats["discovered"] += 1
                except ValueError:
                    # 이미 존재 (중복)
                    stats["skipped_existing"] += 1
            else:
                stats["discovered"] += 1

        self.logger.info("discovery_done", **stats)
        return stats

    def close(self) -> None:
        self._collector.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ── Private ──────────────────────────────────────────────────────────

    def _scrape_mentions(self, username: str, stats: Dict) -> Set[str]:
        """채널 최신 1페이지를 긁어 언급된 username 집합을 반환한다."""
        # collect()로 메시지를 가져온 뒤 content + raw_html 파싱
        try:
            messages = self._collector.collect(
                channel_id=0,   # 임시 ID — 저장하지 않으므로 무관
                username=username,
                since_msg_id=None,
                max_pages=1,
            )
        except Exception as e:
            self.logger.warning("scrape_failed", username=username, error=str(e))
            stats["errors"] += 1
            return set()

        mentions: Set[str] = set()
        for msg in messages:
            # 텍스트 본문
            if msg.get("content"):
                mentions |= extract_channel_mentions(msg["content"])
            # raw HTML 내 href 링크
            if msg.get("raw_html"):
                soup = BeautifulSoup(msg["raw_html"], "html.parser")
                for a in soup.select("a[href]"):
                    href = a.get("href", "")
                    mentions |= extract_channel_mentions(href)

        self.logger.debug("mentions_scraped", source=username, count=len(mentions))
        return mentions
