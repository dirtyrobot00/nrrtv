"""all_corp.list 에 있는 모든 종목을 순차 backfill

사용법:
  python3 script/backfill_all_corp.py --from-date 20200101
  python3 script/backfill_all_corp.py --from-date 20200101 --market KOSPI
  python3 script/backfill_all_corp.py --from-date 20200101 --dry-run
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent.parent
CORP_LIST = ROOT / "data" / "all_corp.list"
COLLECTOR = ROOT / "src" / "collectors" / "collect_all_from_dart_to_db.py"


def load_stock_codes(market_filter: Optional[str]) -> list:
    """all_corp.list 파싱 → [(코드, 이름, 시장), ...]"""
    corps = []
    with CORP_LIST.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|")
            if len(parts) < 3:
                continue
            code, name, market = parts[0], parts[1], parts[2]
            if market_filter and market != market_filter:
                continue
            corps.append((code, name, market))
    return corps


def run_backfill(code: str, from_date: str, dry_run: bool) -> bool:
    cmd = [
        sys.executable,
        str(COLLECTOR),
        "--backfill",
        "--from-date", from_date,
        "--stock-code", code,
    ]
    if dry_run:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, cwd=ROOT)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="all_corp.list 전 종목 backfill")
    parser.add_argument("--from-date", required=True, help="수집 시작일 (YYYYMMDD)")
    parser.add_argument("--market", choices=["KOSPI", "KOSDAQ", "KONEX"],
                        help="특정 시장만 처리 (생략 시 전체)")
    parser.add_argument("--dry-run", action="store_true", help="DB 저장 없이 로그만 출력")
    args = parser.parse_args()

    corps = load_stock_codes(args.market)
    total = len(corps)
    print(f"대상 종목: {total}개 (시장={args.market or '전체'})")

    failed = []
    for idx, (code, name, market) in enumerate(corps, 1):
        print(f"\n[{idx}/{total}] {code} {name} ({market})")
        ok = run_backfill(code, args.from_date, args.dry_run)
        if not ok:
            print(f"  !! FAILED: {code} {name}")
            failed.append((code, name, market))

    print(f"\n완료: {total - len(failed)}/{total} 성공")
    if failed:
        print("실패 목록:")
        for code, name, market in failed:
            print(f"  {code}|{name}|{market}")
        sys.exit(1)


if __name__ == "__main__":
    main()
