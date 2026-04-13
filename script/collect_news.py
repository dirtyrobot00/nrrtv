#!/usr/bin/env python3
"""Naver Finance 뉴스 수집 스크립트.

realtime: 최신 뉴스를 주기적으로 계속 수집 (데몬/폴링)
backfill: 날짜 범위 지정 수집

Usage:
    # realtime - 5분(300초) 간격 폴링 데몬 (기본)
    python3 script/collect_news.py --mode realtime

    # realtime - 간격 지정 (60초)
    python3 script/collect_news.py --mode realtime --interval 60

    # realtime - 종목별 폴링
    python3 script/collect_news.py --mode realtime --ticker 005930 --interval 120

    # backfill - 날짜 범위 지정
    python3 script/collect_news.py --mode backfill --from-date 2026-01-01 --to-date 2026-04-07

    # backfill - 최대 페이지 지정
    python3 script/collect_news.py --mode backfill --from-date 2026-01-01 --max-pages 100

    # dry-run - 수집 계획만 출력 (실제 수집 없음)
    python3 script/collect_news.py --mode backfill --from-date 2026-01-01 --dry-run
"""

import argparse
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가 (외부 실행 지원)
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.collectors.news import NaverFinanceNewsCollector
from src.storage.news_store import NewsStore
from src.utils.logger import setup_logging, get_logger


def print_dry_run_plan(args, logger):
    """dry-run 시 수집 계획 출력."""
    logger.info("=" * 60)
    logger.info("DRY-RUN MODE - 실제 수집을 수행하지 않습니다")
    logger.info("=" * 60)
    logger.info(f"mode       : {args.mode}")
    logger.info(f"ticker     : {args.ticker or '(없음 - 전체 뉴스)'}")
    logger.info(f"limit      : {args.limit or '(제한 없음)'}")

    if args.mode == "realtime":
        logger.info(f"max_pages  : {args.max_pages or 20} (safety cap)")
        logger.info("stop 조건  : 첫 번째 중복 URL 발견 시")
    else:
        logger.info(f"from_date  : {args.from_date}")
        logger.info(f"to_date    : {args.to_date or datetime.now().strftime('%Y-%m-%d')}")
        logger.info(f"max_pages  : {args.max_pages or 50}")
        logger.info("stop 조건  : 기사 날짜 < from_date 도달 시")

    # DB 현황 조회
    try:
        store = NewsStore()
        stats = store.get_stats()
        logger.info("-" * 40)
        logger.info("현재 DB 현황:")
        logger.info(f"  총 기사 수     : {stats['total_articles']}")
        logger.info(f"  최신 발행일    : {stats['latest_published_at']}")
        logger.info(f"  가장 오래된일  : {stats['oldest_published_at']}")
    except Exception as e:
        logger.warning(f"DB 현황 조회 실패: {e}")

    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Naver Finance 뉴스 수집기",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--mode",
        choices=["realtime", "backfill"],
        default="realtime",
        help="수집 모드 (default: realtime)",
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="종목코드 (예: 005930). 미지정 시 전체 뉴스 수집.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="최대 수집 기사 수",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="최대 페이지 수 (realtime: 20, backfill: 50)",
    )
    parser.add_argument(
        "--from-date",
        type=str,
        default=None,
        help="backfill 시작 날짜 (YYYY-MM-DD). backfill 모드에서 필수.",
    )
    parser.add_argument(
        "--to-date",
        type=str,
        default=None,
        help="backfill 종료 날짜 (YYYY-MM-DD). default: 오늘.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="수집 계획만 출력. 실제 HTTP 요청 및 DB 저장 없음.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="realtime 폴링 간격 (초, default: 300). realtime 모드 전용.",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING"],
        default="INFO",
        help="로그 레벨 (default: INFO)",
    )

    args = parser.parse_args()

    setup_logging(log_level=args.log_level, log_to_file=True, log_format="text")
    logger = get_logger(__name__)

    # dry-run
    if args.dry_run:
        print_dry_run_plan(args, logger)
        return

    # backfill 모드 필수 파라미터 검증
    if args.mode == "backfill" and not args.from_date:
        logger.error("backfill 모드에서는 --from-date 가 필수입니다.")
        parser.print_help()
        sys.exit(1)

    if args.mode == "realtime":
        # 폴링 데몬: Ctrl+C 또는 SIGTERM 까지 반복
        stop_flag = {"stop": False}

        def _handle_signal(signum, frame):
            logger.info("종료 신호 수신. 다음 루프 후 종료합니다...")
            stop_flag["stop"] = True

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        logger.info("=" * 60)
        logger.info(f"realtime 폴링 데몬 시작 (interval={args.interval}초)")
        logger.info("종료: Ctrl+C 또는 SIGTERM")
        logger.info("=" * 60)

        cycle = 0
        while not stop_flag["stop"]:
            cycle += 1
            logger.info(f"[cycle={cycle}] 수집 시작")
            try:
                with NaverFinanceNewsCollector(rate_limit=2.0) as collector:
                    results = collector.collect_realtime(
                        limit=args.limit or 50,
                        ticker=args.ticker,
                    )
                logger.info(f"[cycle={cycle}] 수집 완료: {len(results)}건 신규")
            except Exception as e:
                logger.error(f"[cycle={cycle}] 수집 오류: {e}", exc_info=True)

            if stop_flag["stop"]:
                break

            logger.info(f"[cycle={cycle}] {args.interval}초 대기 중... (Ctrl+C로 종료)")
            # 1초씩 sleep하여 SIGINT에 빠르게 반응
            for _ in range(args.interval):
                if stop_flag["stop"]:
                    break
                time.sleep(1)

        logger.info("realtime 폴링 데몬 종료.")

    else:
        # backfill: 1회 실행
        try:
            with NaverFinanceNewsCollector(rate_limit=2.0) as collector:
                from_date = datetime.strptime(args.from_date, "%Y-%m-%d")
                to_date = (
                    datetime.strptime(args.to_date, "%Y-%m-%d")
                    if args.to_date
                    else None
                )
                results = collector.collect_backfill(
                    from_date=from_date,
                    to_date=to_date,
                    max_pages=args.max_pages or 50,
                    limit=args.limit,
                    ticker=args.ticker,
                )

            logger.info("=" * 60)
            logger.info(f"backfill 완료: {len(results)}건")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"수집 실패: {e}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
