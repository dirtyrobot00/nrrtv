"""채널 등록 + 메타데이터 조회 + 백필 one-shot 스크립트.

사용법:
    python script/add_and_backfill.py @KISemicon
    python script/add_and_backfill.py KISemicon
    python script/add_and_backfill.py https://t.me/KISemicon
    python script/add_and_backfill.py t.me/s/KISemicon

    # 옵션
    python script/add_and_backfill.py @KISemicon --since 2024-01-01
    python script/add_and_backfill.py @KISemicon --category 종목리서치 --pages 9999
    python script/add_and_backfill.py @KISemicon --skip-backfill   # 등록+메타만
"""

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from src.storage.telegram_store import TelegramStore, TelegramMessageORM
from src.collectors.telegram.telegram_collector import TelegramCollector

CATEGORIES = ["주식실황", "종목리서치", "매크로", "IPO", "기타"]
DEFAULT_SINCE = "2025-01-01"


def parse_username(raw: str) -> str:
    """@KISemicon / t.me/KISemicon / https://t.me/s/KISemicon 등을 username으로 정규화."""
    raw = raw.strip()
    # URL 형태
    m = re.search(r"t\.me/(?:s/)?([A-Za-z0-9_]+)", raw)
    if m:
        return m.group(1)
    # @mention 또는 plain
    return raw.lstrip("@").strip()


def get_store() -> TelegramStore:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("[ERROR] DATABASE_URL 환경변수가 없습니다.")
        sys.exit(1)
    return TelegramStore(database_url=db_url)


def main():
    parser = argparse.ArgumentParser(
        description="채널 등록 + 메타 조회 + 과거 메시지 백필 one-shot",
    )
    parser.add_argument("channel", help="채널 식별자: @username, t.me/username, URL 등")
    parser.add_argument("--since", default=DEFAULT_SINCE, metavar="YYYY-MM-DD",
                        help=f"백필 하한 날짜 (기본: {DEFAULT_SINCE})")
    parser.add_argument("--category", choices=CATEGORIES, default=None,
                        help=f"카테고리: {CATEGORIES}")
    parser.add_argument("--pages", type=int, default=9999,
                        help="백필 최대 페이지 수 (기본: 9999, 1페이지≈20개)")
    parser.add_argument("--skip-backfill", action="store_true",
                        help="등록·메타 조회만 하고 백필은 건너뜀")
    parser.add_argument("--fetch-articles", action="store_true",
                        help="메시지 내 하이퍼링크의 기사 본문도 수집")
    args = parser.parse_args()

    username = parse_username(args.channel)
    print(f"\n=== @{username} ===")

    # ── 1. 채널 등록 (이미 있으면 기존 채널 사용) ─────────────────────────
    store = get_store()
    ch = store.get_channel(username)
    if ch:
        print(f"[INFO] 이미 등록된 채널입니다 (id={ch['id']}). 기존 채널을 사용합니다.")
    else:
        ch = store.add_channel(username=username, category=args.category)
        print(f"[OK] 채널 등록 완료 (id={ch['id']})")

    # category가 아직 없고 인자로 넘어왔으면 업데이트
    if args.category and not ch.get("category"):
        store.update_channel(username, category=args.category)
        print(f"[OK] 카테고리 설정: {args.category}")

    # ── 2. 메타데이터 조회 ────────────────────────────────────────────────
    print(f"\n[STEP 2] t.me/s/{username} 메타데이터 조회 중...")
    with TelegramCollector() as col:
        try:
            meta = col.fetch_channel_meta(username)
        except Exception as e:
            print(f"[WARN] 메타데이터 조회 실패: {e}")
            meta = {}

    if meta:
        store.update_channel(
            username,
            channel_name=meta.get("channel_name"),
            description=meta.get("description"),
            subscriber_count=meta.get("subscriber_count"),
        )
        print(f"  채널명    : {meta.get('channel_name')}")
        print(f"  구독자 수 : {meta.get('subscriber_count')}")
        print(f"  설명      : {(meta.get('description') or '')[:80]}...")

    # ── 3. 백필 ──────────────────────────────────────────────────────────
    if args.skip_backfill:
        print("\n[INFO] --skip-backfill 옵션으로 백필을 건너뜁니다.")
        return

    # since 날짜 파싱
    until_date = None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            until_date = datetime.strptime(args.since, fmt).replace(tzinfo=timezone.utc)
            break
        except ValueError:
            continue
    if until_date is None:
        print(f"[ERROR] --since 형식이 잘못됐습니다. 예: 2025-01-01")
        sys.exit(1)

    # DB에서 이미 보유한 가장 오래된 msg_id 감지 → 그 아래만 백필
    ch = store.get_channel(username)
    until_msg_id = None
    with store.get_session() as session:
        row = (
            session.query(TelegramMessageORM.telegram_msg_id)
            .filter_by(channel_id=ch["id"])
            .order_by(TelegramMessageORM.telegram_msg_id.asc())
            .first()
        )
        if row:
            until_msg_id = row[0] - 1

    print(f"\n[STEP 3] 백필 시작")
    print(f"  since(하한) : {until_date.date()}")
    print(f"  until_msg_id: {until_msg_id} (DB 자동 감지)")
    print(f"  max_pages   : {args.pages}  (1페이지 ≈ 20개, 요청 간 5~10초)\n")

    total_saved = 0

    def save_and_report(messages):
        nonlocal total_saved
        saved = store.save_messages(messages)
        total_saved += saved
        oldest_date = min(
            (m["posted_at"] for m in messages if m.get("posted_at")), default=None
        )
        oldest_id = min(m["telegram_msg_id"] for m in messages)
        newest_id = max(m["telegram_msg_id"] for m in messages)
        print(f"  저장 {saved}개 (msg {oldest_id}~{newest_id}, 최오래된={oldest_date}) | 누계 {total_saved}개")

    with TelegramCollector(fetch_articles=args.fetch_articles) as col:
        col.backfill(
            channel_id=ch["id"],
            username=username,
            until_msg_id=until_msg_id,
            until_date=until_date,
            max_pages=args.pages,
            save_callback=save_and_report,
        )

    store.mark_scraped(ch["id"])
    print(f"\n[OK] 완료 — @{username} 총 {total_saved}개 저장")


if __name__ == "__main__":
    main()
