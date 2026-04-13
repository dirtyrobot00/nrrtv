"""Telegram 채널 수동 등록/관리 CLI.

사용법:
    # 채널 등록
    python script/manage_telegram_channels.py add <username> [옵션]

    # 채널 목록
    python script/manage_telegram_channels.py list [--category 주식실황] [--min-subs 2000]

    # 채널 정보 수정
    python script/manage_telegram_channels.py update <username> [옵션]

    # 채널 비활성화
    python script/manage_telegram_channels.py deactivate <username>

    # 채널 메타데이터 자동 조회 (t.me/s/ 에서 긁어옴)
    python script/manage_telegram_channels.py fetch-meta <username>

    # 메시지 수동 수집 (테스트용)
    python script/manage_telegram_channels.py scrape <username> [--pages 3]

    # 전체 통계
    python script/manage_telegram_channels.py stats
"""

import argparse
import os
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from src.storage.telegram_store import TelegramStore
from src.collectors.telegram.telegram_collector import TelegramCollector

CATEGORIES = ["주식실황", "종목리서치", "매크로", "IPO", "기타"]


def get_store() -> TelegramStore:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("[ERROR] DATABASE_URL 환경변수가 없습니다. .env 파일을 확인하세요.")
        sys.exit(1)
    return TelegramStore(database_url=db_url)


# ── 명령 핸들러 ──────────────────────────────────────────────────────────

def cmd_add(args):
    """채널 등록."""
    store = get_store()

    # category 검증
    if args.category and args.category not in CATEGORIES:
        print(f"[ERROR] category는 다음 중 하나여야 합니다: {CATEGORIES}")
        sys.exit(1)

    try:
        ch = store.add_channel(
            username=args.username,
            channel_name=args.name,
            category=args.category,
            characteristics=args.characteristics,
            description=args.description,
            subscriber_count=args.subscribers,
        )
        print(f"[OK] 채널 등록 완료")
        _print_channel(ch)
    except ValueError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


def cmd_list(args):
    """채널 목록 출력."""
    store = get_store()
    channels = store.list_channels(
        active_only=not args.all,
        category=args.category,
        min_subscribers=args.min_subs,
    )
    if not channels:
        print("등록된 채널이 없습니다.")
        return

    print(f"{'ID':>4}  {'Username':<25}  {'Category':<12}  {'Subs':>7}  {'Active':<6}  이름")
    print("-" * 80)
    for ch in channels:
        print(
            f"{ch['id']:>4}  "
            f"@{ch['username']:<24}  "
            f"{ch['category'] or '-':<12}  "
            f"{ch['subscriber_count'] or '-':>7}  "
            f"{'Y' if ch['is_active'] else 'N':<6}  "
            f"{ch['channel_name'] or ''}"
        )
    print(f"\n총 {len(channels)}개")


def cmd_update(args):
    """채널 정보 수정."""
    store = get_store()
    fields = {}
    if args.name is not None:
        fields["channel_name"] = args.name
    if args.category is not None:
        if args.category not in CATEGORIES:
            print(f"[ERROR] category는 다음 중 하나여야 합니다: {CATEGORIES}")
            sys.exit(1)
        fields["category"] = args.category
    if args.characteristics is not None:
        fields["characteristics"] = args.characteristics
    if args.description is not None:
        fields["description"] = args.description
    if args.subscribers is not None:
        fields["subscriber_count"] = args.subscribers

    if not fields:
        print("[WARN] 변경할 항목이 없습니다.")
        return

    ok = store.update_channel(args.username, **fields)
    if ok:
        print(f"[OK] @{args.username} 업데이트 완료: {list(fields.keys())}")
    else:
        print(f"[ERROR] @{args.username} 채널을 찾을 수 없습니다.")
        sys.exit(1)


def cmd_deactivate(args):
    """채널 비활성화."""
    store = get_store()
    ok = store.deactivate_channel(args.username)
    if ok:
        print(f"[OK] @{args.username} 비활성화됨")
    else:
        print(f"[ERROR] @{args.username} 채널을 찾을 수 없습니다.")
        sys.exit(1)


def cmd_fetch_meta(args):
    """t.me/s/ 에서 채널 메타데이터를 조회하고 DB를 업데이트한다."""
    store = get_store()
    username = args.username.lstrip("@").strip()

    print(f"t.me/s/{username} 에서 메타데이터 조회 중...")
    with TelegramCollector() as col:
        try:
            meta = col.fetch_channel_meta(username)
        except Exception as e:
            print(f"[ERROR] {e}")
            sys.exit(1)

    print(f"  채널명    : {meta.get('channel_name')}")
    print(f"  설명      : {meta.get('description')}")
    print(f"  구독자 수 : {meta.get('subscriber_count')}")

    ch = store.get_channel(username)
    if ch:
        store.update_channel(
            username,
            channel_name=meta.get("channel_name"),
            description=meta.get("description"),
            subscriber_count=meta.get("subscriber_count"),
        )
        print(f"[OK] DB 업데이트 완료 (id={ch['id']})")
    else:
        print("[INFO] DB에 없는 채널입니다. 등록하려면:")
        print(f"  python script/manage_telegram_channels.py add {username} --category 주식실황")


def cmd_scrape(args):
    """채널 메시지를 수동으로 수집한다 (테스트용)."""
    store = get_store()
    username = args.username.lstrip("@").strip()

    ch = store.get_channel(username)
    if ch is None:
        print(f"[ERROR] @{username} 가 DB에 없습니다. 먼저 add 명령으로 등록하세요.")
        sys.exit(1)

    since_msg_id = store.get_latest_msg_id(ch["id"])
    print(f"[INFO] @{username} (id={ch['id']}) 수집 시작 (since_msg_id={since_msg_id})")

    with TelegramCollector(fetch_articles=getattr(args, "fetch_articles", False)) as col:
        messages = col.collect(
            channel_id=ch["id"],
            username=username,
            since_msg_id=since_msg_id,
            max_pages=args.pages,
        )

    if not messages:
        print("[INFO] 새 메시지가 없습니다.")
        return

    saved = store.save_messages(messages)
    store.mark_scraped(ch["id"])
    print(f"[OK] {saved}개 저장 (수집 {len(messages)}개)")

    if args.verbose:
        for m in messages[:5]:
            print(f"\n  [{m['telegram_msg_id']}] {m['posted_at']}")
            print(f"  {(m['content'] or '(미디어)')[:120]}")


def cmd_backfill(args):
    """채널 과거 메시지 전체 백필."""
    from datetime import datetime, timezone
    store = get_store()
    username = args.username.lstrip("@").strip()

    ch = store.get_channel(username)
    if ch is None:
        print(f"[ERROR] @{username} 가 DB에 없습니다. 먼저 add 명령으로 등록하세요.")
        sys.exit(1)

    # until_msg_id: 이미 가진 가장 오래된 메시지 ID (있으면 그 아래는 스킵)
    until_msg_id = args.until_id
    if until_msg_id is None and not args.full and args.since_date is None:
        with store.get_session() as session:
            from src.storage.telegram_store import TelegramMessageORM
            row = (
                session.query(TelegramMessageORM.telegram_msg_id)
                .filter_by(channel_id=ch["id"])
                .order_by(TelegramMessageORM.telegram_msg_id.asc())
                .first()
            )
            if row:
                until_msg_id = row[0] - 1

    # --since-date 파싱: "YYYY-MM-DD" 또는 "YYYY-MM-DD HH:MM"
    until_date = None
    if args.since_date:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                until_date = datetime.strptime(args.since_date, fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue
        if until_date is None:
            print(f"[ERROR] --since-date 형식이 잘못됐습니다. 예: 2025-01-01 또는 '2025-01-01 09:00'")
            sys.exit(1)

    print(f"[INFO] @{username} (id={ch['id']}) 백필 시작")
    print(f"       until_msg_id={until_msg_id}")
    print(f"       since_date(하한)={until_date or '없음 (채널 시작까지)'}")
    print(f"       max_pages={args.pages}  (1페이지 ≈ 20개, 요청 간 5~10초)")

    total_saved = 0

    def save_and_report(messages):
        nonlocal total_saved
        saved = store.save_messages(messages)
        total_saved += saved
        oldest = min(m["telegram_msg_id"] for m in messages)
        newest = max(m["telegram_msg_id"] for m in messages)
        oldest_date = min(
            (m["posted_at"] for m in messages if m.get("posted_at")), default=None
        )
        print(f"  저장 {saved}개 (msg {oldest}~{newest}, 최오래된={oldest_date}) | 누계 {total_saved}개")

    with TelegramCollector(fetch_articles=getattr(args, "fetch_articles", False)) as col:
        col.backfill(
            channel_id=ch["id"],
            username=username,
            until_msg_id=until_msg_id,
            until_date=until_date,
            max_pages=args.pages,
            save_callback=save_and_report,
        )

    store.mark_scraped(ch["id"])
    print(f"\n[OK] 백필 완료 — 총 {total_saved}개 저장")


def cmd_backfill_missing(args):
    """백필이 부족한 채널을 자동으로 찾아서 백필한다."""
    from datetime import datetime, timezone
    store = get_store()

    # since_date 파싱
    since_date = None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            since_date = datetime.strptime(args.since_date, fmt).replace(tzinfo=timezone.utc)
            break
        except ValueError:
            continue
    if since_date is None:
        print(f"[ERROR] --since 형식이 잘못됐습니다. 예: 2025-01-01")
        sys.exit(1)

    channels = store.list_channels(active_only=not args.all, category=args.category)
    if not channels:
        print("등록된 채널이 없습니다.")
        return

    # 백필이 필요한 채널 탐색
    need_backfill = []
    print(f"{'ID':>4}  {'Username':<25}  {'가장 오래된 메시지':<22}  상태")
    print("-" * 75)
    for ch in channels:
        oldest_date, oldest_msg_id = store.get_oldest_message(ch["id"])
        if oldest_date is None:
            status = "메시지 없음 → 백필 필요"
            need_backfill.append((ch, None, None))
        else:
            # timezone-aware로 통일
            if oldest_date.tzinfo is None:
                oldest_date = oldest_date.replace(tzinfo=timezone.utc)
            if oldest_date > since_date:
                status = f"미완료 → {oldest_date.date()} 이전 누락"
                need_backfill.append((ch, oldest_date, oldest_msg_id))
            else:
                status = f"완료 ({oldest_date.date()})"
        print(
            f"{ch['id']:>4}  "
            f"@{ch['username']:<24}  "
            f"{str(oldest_date.date()) if oldest_date else '-':<22}  "
            f"{status}"
        )

    print(f"\n총 {len(channels)}개 채널 중 {len(need_backfill)}개 백필 필요\n")

    if not need_backfill:
        print(f"[OK] 모든 채널이 {since_date.date()} 이전까지 백필되어 있습니다.")
        return

    if args.dry_run:
        print("[INFO] --dry-run 모드: 실제 백필을 실행하지 않습니다.")
        return

    # 백필 실행
    for ch, oldest_date, oldest_msg_id in need_backfill:
        username = ch["username"]
        # until_msg_id: 이미 가진 가장 오래된 메시지 직전까지
        until_msg_id = (oldest_msg_id - 1) if oldest_msg_id else None

        print(f"\n{'='*60}")
        print(f"[BACKFILL] @{username} (id={ch['id']})")
        print(f"  since(하한) : {since_date.date()}")
        print(f"  until_msg_id: {until_msg_id}")
        print(f"  max_pages   : {args.pages}")
        print()

        total_saved = 0

        def save_and_report(messages):
            nonlocal total_saved
            saved = store.save_messages(messages)
            total_saved += saved
            oldest = min(m["telegram_msg_id"] for m in messages)
            newest = max(m["telegram_msg_id"] for m in messages)
            oldest_dt = min(
                (m["posted_at"] for m in messages if m.get("posted_at")), default=None
            )
            print(f"  저장 {saved}개 (msg {oldest}~{newest}, 최오래된={oldest_dt}) | 누계 {total_saved}개")

        with TelegramCollector(fetch_articles=getattr(args, "fetch_articles", False)) as col:
            col.backfill(
                channel_id=ch["id"],
                username=username,
                until_msg_id=until_msg_id,
                until_date=since_date,
                max_pages=args.pages,
                save_callback=save_and_report,
            )

        store.mark_scraped(ch["id"])
        print(f"[OK] @{username} 완료 — {total_saved}개 저장")


def cmd_stats(args):
    """전체 통계."""
    store = get_store()
    s = store.get_stats()
    print(f"채널 총수  : {s['total_channels']}")
    print(f"활성 채널  : {s['active_channels']}")
    print(f"메시지 총수: {s['total_messages']}")


def _print_channel(ch: dict):
    print(f"  ID          : {ch['id']}")
    print(f"  Username    : @{ch['username']}")
    print(f"  Channel URL : {ch['channel_url']}")
    print(f"  이름        : {ch['channel_name'] or '-'}")
    print(f"  Category    : {ch['category'] or '-'}")
    print(f"  특징        : {ch['characteristics'] or '-'}")
    print(f"  설명        : {ch['description'] or '-'}")
    print(f"  구독자 수   : {ch['subscriber_count'] or '-'}")
    print(f"  활성        : {ch['is_active']}")


# ── argparse 설정 ────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="manage_telegram_channels",
        description="Telegram 채널 수동 등록/관리 CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # add
    p_add = sub.add_parser("add", help="채널 등록")
    p_add.add_argument("username", help="채널 username (@ 없이)")
    p_add.add_argument("--name", help="채널 표시명")
    p_add.add_argument("--category", help=f"카테고리: {CATEGORIES}")
    p_add.add_argument("--characteristics", help="채널 성격/특징 설명 (자유 텍스트)")
    p_add.add_argument("--description", help="채널 소개글")
    p_add.add_argument("--subscribers", type=int, help="구독자 수")
    p_add.set_defaults(func=cmd_add)

    # list
    p_list = sub.add_parser("list", help="채널 목록")
    p_list.add_argument("--all", action="store_true", help="비활성 채널 포함")
    p_list.add_argument("--category", help="카테고리 필터")
    p_list.add_argument("--min-subs", type=int, dest="min_subs", help="최소 구독자 수")
    p_list.set_defaults(func=cmd_list)

    # update
    p_upd = sub.add_parser("update", help="채널 정보 수정")
    p_upd.add_argument("username", help="채널 username")
    p_upd.add_argument("--name", help="새 표시명")
    p_upd.add_argument("--category", help="새 카테고리")
    p_upd.add_argument("--characteristics", help="새 특징 설명")
    p_upd.add_argument("--description", help="새 소개글")
    p_upd.add_argument("--subscribers", type=int, help="새 구독자 수")
    p_upd.set_defaults(func=cmd_update)

    # deactivate
    p_deact = sub.add_parser("deactivate", help="채널 비활성화")
    p_deact.add_argument("username", help="채널 username")
    p_deact.set_defaults(func=cmd_deactivate)

    # fetch-meta
    p_meta = sub.add_parser("fetch-meta", help="t.me/s/ 에서 채널 메타데이터 자동 조회")
    p_meta.add_argument("username", help="채널 username")
    p_meta.set_defaults(func=cmd_fetch_meta)

    # scrape
    p_scrape = sub.add_parser("scrape", help="메시지 수동 수집")
    p_scrape.add_argument("username", help="채널 username")
    p_scrape.add_argument("--pages", type=int, default=3, help="최대 페이지 수 (기본 3)")
    p_scrape.add_argument("--verbose", "-v", action="store_true", help="메시지 미리보기 출력")
    p_scrape.add_argument("--fetch-articles", action="store_true", dest="fetch_articles",
                          help="메시지 내 링크의 기사 본문도 수집")
    p_scrape.set_defaults(func=cmd_scrape)

    # backfill
    p_bf = sub.add_parser("backfill", help="과거 메시지 전체 백필")
    p_bf.add_argument("username", help="채널 username")
    p_bf.add_argument("--pages", type=int, default=50, help="최대 페이지 수 (기본 50, 1페이지≈20개)")
    p_bf.add_argument("--until-id", type=int, dest="until_id", default=None,
                      help="이 msg_id 이하에서 중단 (기본: DB 최솟값 자동 감지)")
    p_bf.add_argument("--full", action="store_true",
                      help="DB 기존 데이터 무시하고 처음부터 전체 수집")
    p_bf.add_argument("--since-date", dest="since_date", default=None,
                      metavar="YYYY-MM-DD",
                      help="이 날짜보다 오래된 메시지에서 중단. 예: 2025-01-01 또는 '2025-01-01 09:00'")
    p_bf.add_argument("--fetch-articles", action="store_true", dest="fetch_articles",
                      help="메시지 내 링크의 기사 본문도 수집")
    p_bf.set_defaults(func=cmd_backfill)

    # backfill-missing
    p_bfm = sub.add_parser("backfill-missing", help="백필이 부족한 채널 자동 감지 후 백필")
    p_bfm.add_argument("--since", default="2025-01-01", dest="since_date", metavar="YYYY-MM-DD",
                       help="백필 하한 날짜 (기본: 2025-01-01)")
    p_bfm.add_argument("--pages", type=int, default=9999,
                       help="채널당 최대 페이지 수 (기본: 9999, 1페이지≈20개)")
    p_bfm.add_argument("--all", action="store_true", help="비활성 채널도 포함")
    p_bfm.add_argument("--category", help="특정 카테고리만 처리")
    p_bfm.add_argument("--dry-run", action="store_true", dest="dry_run",
                       help="실제 백필 없이 필요 채널 목록만 출력")
    p_bfm.add_argument("--fetch-articles", action="store_true", dest="fetch_articles",
                       help="메시지 내 링크의 기사 본문도 수집")
    p_bfm.set_defaults(func=cmd_backfill_missing)

    # stats
    p_stats = sub.add_parser("stats", help="전체 통계")
    p_stats.set_defaults(func=cmd_stats)

    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
