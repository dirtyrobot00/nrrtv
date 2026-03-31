#!/usr/bin/env python3
"""data/all_corp.list의 전체 종목에 대해 DART 공시를 backfill 수집하는 래퍼.

내부적으로 script/collect_all_from_dart_to_db.py 의 로직을 종목별로 반복 실행한다.
corp_code를 사용하므로 검색 기간 제한 없음.

Usage:
    # 전체 종목 2020년부터 수집
    python script/backfill_all.py

    # 기간 지정
    python script/backfill_all.py --start-year 2022 --end-year 2024

    # 특정 종목코드부터 재시작 (중단 후 이어서)
    python script/backfill_all.py --start-from 005930

    # dry-run (API 조회만, DB 저장 없음)
    python script/backfill_all.py --dry-run

    # 종목 리스트 파일 지정
    python script/backfill_all.py --corp-list data/all_corp.list
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# script/collect_all_from_dart_to_db.py 의 함수를 재사용
sys.path.insert(0, str(PROJECT_ROOT / "script"))
from collect_all_from_dart_to_db import (
    fetch_corp_code,
    collect_disclosures_for_year,
    save_to_store,
    PBLNTF_TYPE_NAMES,
)

from src.collectors.financial_report_collector import FinancialReportCollector
from src.utils.config import get_config
from src.utils.logger import setup_logging, get_logger


DEFAULT_CORP_LIST = PROJECT_ROOT / "data" / "all_corp.list"


def load_corps(list_path: Path) -> list[tuple[str, str, str]]:
    """(종목코드, 종목명, 시장) 목록 반환. 6자리 숫자 코드만."""
    corps = []
    with open(list_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|")
            code = parts[0].strip()
            name = parts[1].strip() if len(parts) > 1 else ""
            market = parts[2].strip() if len(parts) > 2 else ""
            if len(code) == 6 and code.isdigit():
                corps.append((code, name, market))
    return corps


def run_one(
    collector: FinancialReportCollector,
    stock_code: str,
    corp_code: str,
    start_year: int,
    end_year: int,
    exclude_types: list,
    dry_run: bool,
    logger,
) -> dict:
    """단일 종목에 대해 연도별 수집·저장 수행."""
    store = collector.store
    total_fetched = 0
    total_inserted = 0
    total_skipped = 0

    for year in range(start_year, end_year + 1):
        items = collect_disclosures_for_year(collector, corp_code, year, exclude_types)
        total_fetched += len(items)

        if not dry_run and items:
            ins, skp = save_to_store(store, items, stock_code)
            total_inserted += ins
            total_skipped += skp

        time.sleep(0.3)

    return {
        "fetched": total_fetched,
        "inserted": total_inserted,
        "skipped": total_skipped,
    }


def main():
    parser = argparse.ArgumentParser(description="전체 종목 DART 공시 backfill 래퍼")
    parser.add_argument("--corp-list", default=str(DEFAULT_CORP_LIST), help="종목 리스트 파일")
    parser.add_argument("--start-year", type=int, default=2020, help="수집 시작 연도 (기본: 2020)")
    parser.add_argument("--end-year", type=int, default=None, help="수집 종료 연도 (기본: 올해)")
    parser.add_argument("--exclude-types", default="D", help="제외 공시유형 콤마 구분 (기본: D)")
    parser.add_argument("--start-from", default=None, help="이 종목코드부터 시작 (재시작 용도)")
    parser.add_argument("--dry-run", action="store_true", help="DB 저장 없이 조회 건수만 확인")
    args = parser.parse_args()

    setup_logging(log_level="INFO", log_to_file=True, log_format="text")
    logger = get_logger(__name__)

    end_year = args.end_year or datetime.now().year
    exclude_types = [t.strip() for t in args.exclude_types.split(",") if t.strip()]

    # 종목 리스트 로드
    corp_list_path = Path(args.corp_list)
    if not corp_list_path.exists():
        logger.error("corp_list_not_found", path=str(corp_list_path))
        sys.exit(1)

    all_corps = load_corps(corp_list_path)

    # --start-from 적용
    if args.start_from:
        idx = next((i for i, (c, _, _) in enumerate(all_corps) if c == args.start_from), None)
        if idx is None:
            logger.error("start_from_not_found", stock_code=args.start_from)
            sys.exit(1)
        all_corps = all_corps[idx:]
        logger.info("resume_from", stock_code=args.start_from, remaining=len(all_corps))

    excl_labels = ", ".join(f"{t}({PBLNTF_TYPE_NAMES.get(t,'?')})" for t in exclude_types)

    logger.info("=" * 70)
    logger.info("전체 종목 DART 공시 backfill")
    logger.info(f"  종목 수   : {len(all_corps)}")
    logger.info(f"  수집 기간 : {args.start_year} ~ {end_year}")
    logger.info(f"  제외 유형 : {excl_labels}")
    logger.info(f"  Dry-run   : {args.dry_run}")
    logger.info("=" * 70)

    # API 키
    config = get_config()
    api_key = config.get("dart.api_key", "")
    if not api_key:
        logger.error("DART_API_KEY 미설정 — .env 또는 config/config.yaml 확인")
        sys.exit(1)

    # collector 초기화 (전 종목 공유)
    collector = FinancialReportCollector(api_key=api_key)
    if not args.dry_run:
        collector.store.create_tables()

    grand_fetched = 0
    grand_inserted = 0
    grand_skipped = 0
    errors = []

    try:
        for idx, (stock_code, corp_name, market) in enumerate(all_corps):
            progress = f"[{idx + 1}/{len(all_corps)}]"
            logger.info(
                "processing",
                progress=progress,
                stock_code=stock_code,
                corp_name=corp_name,
                market=market,
            )

            # corp_code 조회 (캐시 활용)
            try:
                corp_code = fetch_corp_code(stock_code, api_key)
            except Exception as e:
                logger.error("corp_code_fetch_failed", stock_code=stock_code, error=str(e))
                errors.append(stock_code)
                continue

            if not corp_code:
                logger.warning("corp_code_not_found", stock_code=stock_code, corp_name=corp_name)
                errors.append(stock_code)
                continue

            # 수집
            try:
                result = run_one(
                    collector=collector,
                    stock_code=stock_code,
                    corp_code=corp_code,
                    start_year=args.start_year,
                    end_year=end_year,
                    exclude_types=exclude_types,
                    dry_run=args.dry_run,
                    logger=logger,
                )
                grand_fetched += result["fetched"]
                grand_inserted += result["inserted"]
                grand_skipped += result["skipped"]
                logger.info(
                    "done",
                    stock_code=stock_code,
                    corp_name=corp_name,
                    **result,
                )
            except Exception as e:
                logger.error("collect_failed", stock_code=stock_code, corp_name=corp_name, error=str(e))
                errors.append(stock_code)

    finally:
        collector.close()

    logger.info("=" * 70)
    logger.info("완료")
    logger.info(f"  총 조회  : {grand_fetched}건")
    if not args.dry_run:
        logger.info(f"  신규 저장: {grand_inserted}건")
        logger.info(f"  중복 스킵: {grand_skipped}건")
    if errors:
        logger.warning(f"  실패 종목: {len(errors)}개 → {errors[:20]}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
