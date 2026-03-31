#!/usr/bin/env python3
"""src/collect_all_from_dart_to_db.py 래퍼.

data/all_corp.list의 종목별로, 지정 기간을 3개월 단위로 쪼개서
src/collect_all_from_dart_to_db.py --stock-code 를 subprocess로 호출한다.

DART API는 stock_code 조회 시 3개월 제한이 있으므로 자동으로 분할한다.

Usage:
    # 전체 종목 2020-01-01부터 오늘까지
    python3 script/run_dart_backfill.py

    # 기간 지정
    python3 script/run_dart_backfill.py --start-date 20230101 --end-date 20251231

    # 특정 종목코드부터 재시작
    python3 script/run_dart_backfill.py --start-from 005930

    # dry-run
    python3 script/run_dart_backfill.py --dry-run
"""

import argparse
import subprocess
import sys
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
CORP_LIST = PROJECT_ROOT / "data" / "all_corp.list"
COLLECT_SCRIPT = PROJECT_ROOT / "src" / "collect_all_from_dart_to_db.py"


def load_stock_codes(path: Path) -> list[tuple[str, str]]:
    """(종목코드, 종목명) 목록. 6자리 숫자만."""
    result = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|")
            code = parts[0].strip()
            name = parts[1].strip() if len(parts) > 1 else ""
            if len(code) == 6 and code.isdigit():
                result.append((code, name))
    return result


def make_3month_chunks(start: date, end: date) -> list[tuple[str, str]]:
    """시작일~종료일을 3개월 단위 (bgn, end) 문자열 튜플 리스트로 반환."""
    chunks = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + relativedelta(months=3) - relativedelta(days=1), end)
        chunks.append((cur.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        cur = chunk_end + relativedelta(days=1)
    return chunks


def run_collect(stock_code: str, start_date: str, end_date: str, dry_run: bool) -> bool:
    """단일 종목 + 단일 기간으로 collect 스크립트 호출. 성공 여부 반환."""
    cmd = [
        sys.executable,
        str(COLLECT_SCRIPT),
        "--stock-code", stock_code,
        "--start-date", start_date,
        "--end-date", end_date,
    ]
    if dry_run:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="DART backfill 래퍼 (종목별 × 3개월 단위)")
    parser.add_argument("--start-date", default="20200101", help="수집 시작일 YYYYMMDD (기본: 20200101)")
    parser.add_argument("--end-date", default=None, help="수집 종료일 YYYYMMDD (기본: 오늘)")
    parser.add_argument("--start-from", default=None, help="이 종목코드부터 시작 (재시작 용도)")
    parser.add_argument("--corp-list", default=str(CORP_LIST), help="종목 리스트 파일")
    parser.add_argument("--dry-run", action="store_true", help="DB 저장 없이 확인만")
    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y%m%d").date()
    end = datetime.strptime(args.end_date, "%Y%m%d").date() if args.end_date else date.today()

    corps = load_stock_codes(Path(args.corp_list))

    if args.start_from:
        idx = next((i for i, (c, _) in enumerate(corps) if c == args.start_from), None)
        if idx is None:
            print(f"[ERROR] --start-from 종목코드 '{args.start_from}'를 목록에서 찾을 수 없음")
            sys.exit(1)
        corps = corps[idx:]
        print(f"[INFO] {args.start_from}부터 재시작 (남은 종목: {len(corps)}개)")

    chunks = make_3month_chunks(start, end)

    print(f"[INFO] 대상 종목: {len(corps)}개")
    print(f"[INFO] 수집 기간: {args.start_date} ~ {end.strftime('%Y%m%d')} ({len(chunks)}개 청크)")
    print(f"[INFO] Dry-run: {args.dry_run}")
    print("=" * 70)

    failed = []

    for corp_idx, (stock_code, corp_name) in enumerate(corps):
        print(f"\n[{corp_idx + 1}/{len(corps)}] {stock_code} {corp_name}")

        for bgn, end_str in chunks:
            print(f"  {bgn} ~ {end_str} ...", end=" ", flush=True)
            ok = run_collect(stock_code, bgn, end_str, args.dry_run)
            if ok:
                print("OK")
            else:
                print("FAIL")
                failed.append((stock_code, corp_name, bgn, end_str))

    print("\n" + "=" * 70)
    print(f"완료 — 총 {len(corps)}개 종목 × {len(chunks)}개 기간")
    if failed:
        print(f"실패 {len(failed)}건:")
        for s, n, b, e in failed:
            print(f"  {s} {n}  {b}~{e}")
    print("=" * 70)


if __name__ == "__main__":
    main()
