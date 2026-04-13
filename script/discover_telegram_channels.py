"""텔레그램 금융 채널 자동 발견 스크립트.

사용법:
    python script/discover_telegram_channels.py [옵션]

옵션:
    --dry-run           DB에 쓰지 않고 탐색 결과만 출력
    --max-new N         이번 실행에서 추가할 최대 채널 수 (기본 100)
    --max-candidates N  평가할 최대 후보 수 (기본 50)
    --min-score N       관련성 최소 점수 0~100 (기본 20)
    --seed CHANNEL      탐색 출발 채널 추가 (여러 번 사용 가능)

예시:
    # 드라이런 — DB는 건드리지 않고 어떤 채널이 발견될지만 확인
    python script/discover_telegram_channels.py --dry-run

    # 시드 채널 지정해서 실행
    python script/discover_telegram_channels.py --seed koreastocknews --seed koreainvest

    # 최대 5개만 추가, 점수 30 이상만 수락
    python script/discover_telegram_channels.py --max-new 5 --min-score 30

cron 설정 (1시간마다 자동 실행):
    # crontab -e 로 아래 줄 추가 (경로는 실제 환경에 맞게 수정)
    0 * * * * cd /Users/koscom/Desktop/dev/NRRTV && python script/discover_telegram_channels.py >> logs/discovery.log 2>&1
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from src.collectors.telegram.channel_discovery import ChannelDiscoveryAgent
from src.storage.telegram_store import TelegramStore
from src.utils.logger import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(
        description="텔레그램 금융 채널 자동 발견",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="DB에 쓰지 않고 탐색 결과만 출력",
    )
    parser.add_argument(
        "--max-new", type=int, default=100, metavar="N",
        help="이번 실행에서 추가할 최대 채널 수 (기본 100)",
    )
    parser.add_argument(
        "--max-candidates", type=int, default=50, metavar="N",
        help="평가할 최대 후보 수 (기본 50)",
    )
    parser.add_argument(
        "--min-score", type=int, default=20, metavar="N",
        help="관련성 최소 점수 0~100 (기본 20)",
    )
    parser.add_argument(
        "--seed", action="append", default=[], metavar="CHANNEL",
        help="탐색 출발 채널 username (여러 번 사용 가능)",
    )
    parser.add_argument(
        "--backfill-since", default="2025-01-01", metavar="YYYY-MM-DD",
        dest="backfill_since",
        help="신규 채널 backfill 하한 날짜 (기본 2025-01-01)",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="로그 레벨 (기본 INFO)",
    )
    args = parser.parse_args()

    # 로깅 설정
    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(exist_ok=True)
    setup_logging(
        log_level=args.log_level,
        log_to_file=True,
        log_file_path=logs_dir / "discovery.log",
        log_format="text",
    )

    # DB 연결
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("[ERROR] DATABASE_URL 환경변수가 없습니다. .env 파일을 확인하세요.")
        sys.exit(1)

    store = TelegramStore(database_url=db_url)

    # 실행 정보 출력
    print(f"\n{'='*60}")
    print(f"  텔레그램 채널 발견 에이전트")
    print(f"  시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  dry_run        : {args.dry_run}")
    print(f"  max_new        : {args.max_new}")
    print(f"  max_candidates : {args.max_candidates}")
    print(f"  min_score      : {args.min_score}")
    print(f"  backfill_since : {args.backfill_since}")
    if args.seed:
        print(f"  시드 채널    : {', '.join(args.seed)}")

    current_stats = store.get_stats()
    print(f"  현재 DB      : 채널 {current_stats['total_channels']}개 (활성 {current_stats['active_channels']}개)")
    print(f"{'='*60}\n")

    if not args.dry_run and current_stats["active_channels"] == 0 and not args.seed:
        print("[WARN] DB에 등록된 채널이 없고 --seed 도 없습니다.")
        print("       먼저 시드 채널을 등록하거나 --seed 옵션으로 지정하세요.")
        print()
        print("  예시:")
        print("    python script/manage_telegram_channels.py add koreastocknews --category 주식실황")
        print("    python script/discover_telegram_channels.py --seed koreastocknews")
        print()

    # 에이전트 실행
    with ChannelDiscoveryAgent(
        store=store,
        max_new_per_run=args.max_new,
        max_candidates_per_run=args.max_candidates,
        min_relevance_score=args.min_score,
        seed_channels=args.seed,
    ) as agent:
        result = agent.run(dry_run=args.dry_run)

    # 결과 출력
    print(f"\n{'='*60}")
    print(f"  완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'─'*60}")
    print(f"  새로 {'발견(dry-run)' if args.dry_run else '추가'}  : {result['discovered']}개")
    print(f"  이미 등록됨       : {result['skipped_existing']}개")
    print(f"  관련성 낮아 스킵  : {result['skipped_irrelevant']}개")
    print(f"  블랙리스트 스킵   : {result['skipped_blacklist']}개")
    print(f"  오류              : {result['errors']}개")
    if not args.dry_run:
        after_stats = store.get_stats()
        print(f"{'─'*60}")
        print(f"  DB 현황: 채널 {after_stats['total_channels']}개 (활성 {after_stats['active_channels']}개)")
    print(f"{'='*60}\n")

    # 새로 추가된 채널이 있으면 backfill-missing 이어서 실행
    if not args.dry_run and result["discovered"] > 0:
        print(f"[INFO] {result['discovered']}개 신규 채널 발견 → backfill-missing 실행\n")
        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "script" / "manage_telegram_channels.py"),
            "backfill-missing",
            "--since", args.backfill_since,
        ]
        subprocess.run(cmd, check=False)


if __name__ == "__main__":
    main()
