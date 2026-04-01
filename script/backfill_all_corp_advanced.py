"""all_corp.list 전 종목 backfill (이어서 실행 지원)

중단/재시작 전략:
  - 진행 상태를 .backfill_progress/<from_date>.json 에 저장
  - DB에서 이미 수집된 종목(dart_disclosures에 레코드 존재)을 추가 확인
  - --resume 플래그 없이도 자동으로 완료 종목을 건너뜀

사용법:
  # 처음 실행 (또는 이어서)
  python3 script/backfill_all_corp_advanced.py --from-date 20200101

  # 특정 시장만
  python3 script/backfill_all_corp_advanced.py --from-date 20200101 --market KOSPI

  # 상태 파일 무시하고 전체 재실행 (DB 기반 스킵은 유지)
  python3 script/backfill_all_corp_advanced.py --from-date 20200101 --reset

  # DB 조회 없이 상태 파일만으로 이어서 실행
  python3 script/backfill_all_corp_advanced.py --from-date 20200101 --no-db-check
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Set

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------

ROOT       = Path(__file__).parent.parent
CORP_LIST  = ROOT / "data" / "all_corp.list"
COLLECTOR  = ROOT / "src" / "collectors" / "financial_report" / "collect_all_from_dart_to_db.py"
STATE_DIR  = ROOT / ".backfill_progress"


# ---------------------------------------------------------------------------
# all_corp.list 파싱
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# DB 조회 - 이미 수집된 종목 확인
# ---------------------------------------------------------------------------

def get_done_codes_from_db(from_date: str) -> Set[str]:
    """dart_disclosures 에서 from_date 이후 레코드가 있는 stock_code 집합 반환.
    DB 연결 실패 시 빈 집합 반환 (건너뜀 없이 전체 처리).
    """
    if not HAS_PSYCOPG2:
        return set()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return set()

    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        # from_date 이후 공시가 1건 이상 있고, fetch_status = 'done' 인 종목
        cur.execute(
            """
            SELECT DISTINCT stock_code
            FROM dart_disclosures
            WHERE stock_code IS NOT NULL
              AND rcept_dt >= %s
              AND fetch_status = 'done'
            """,
            (datetime.strptime(from_date, "%Y%m%d").date(),),
        )
        codes = {row[0].strip() for row in cur.fetchall() if row[0]}
        cur.close()
        conn.close()
        return codes
    except Exception as e:
        print(f"[WARN] DB 조회 실패 (무시하고 계속): {e}")
        return set()


# ---------------------------------------------------------------------------
# 진행 상태 파일
# ---------------------------------------------------------------------------

def state_path(from_date: str, market: Optional[str]) -> Path:
    suffix = f"_{market}" if market else ""
    return STATE_DIR / f"{from_date}{suffix}.json"


def load_state(path: Path) -> dict:
    if path.exists():
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    return {"completed": [], "failed": [], "started_at": None, "updated_at": None}


def save_state(path: Path, state: dict) -> None:
    STATE_DIR.mkdir(exist_ok=True)
    state["updated_at"] = datetime.now().isoformat(timespec="seconds")
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# 개별 종목 backfill 실행
# ---------------------------------------------------------------------------

def run_backfill(code: str, from_date: str, dry_run: bool) -> bool:
    cmd = [
        sys.executable,
        str(COLLECTOR),
        "--backfill",
        "--from-date", from_date,
        "--stock-code", code,
        "--doc-delay", "0.5",
    ]
    if dry_run:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, cwd=ROOT)
    return result.returncode == 0


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="all_corp.list 전 종목 backfill (이어서 실행 지원)")
    parser.add_argument("--from-date",    required=True,                             help="수집 시작일 (YYYYMMDD)")
    parser.add_argument("--market",       choices=["KOSPI", "KOSDAQ", "KONEX"],      help="특정 시장만 처리")
    parser.add_argument("--dry-run",      action="store_true",                       help="DB 저장 없이 로그만 출력")
    parser.add_argument("--reset",        action="store_true",                       help="상태 파일 초기화 후 처음부터 재실행")
    parser.add_argument("--no-db-check",  action="store_true",                       help="DB 조회 스킵 (상태 파일만 사용)")
    args = parser.parse_args()

    # 상태 파일 경로
    s_path = state_path(args.from_date, args.market)

    # --reset: 상태 파일 삭제
    if args.reset and s_path.exists():
        s_path.unlink()
        print(f"[RESET] 상태 파일 삭제: {s_path}")

    # 상태 로드
    state = load_state(s_path)
    if state["started_at"] is None:
        state["started_at"] = datetime.now().isoformat(timespec="seconds")
        save_state(s_path, state)

    completed_set: Set[str] = set(state["completed"])

    # DB에서 이미 완료된 종목 조회
    db_done: Set[str] = set()
    if not args.no_db_check and not args.dry_run:
        print("[DB] 이미 수집된 종목 조회 중...")
        db_done = get_done_codes_from_db(args.from_date)
        if db_done:
            print(f"[DB] DB에 수집 완료 종목: {len(db_done)}개 → 건너뜀")

    skip_set = completed_set | db_done

    # 전체 종목 목록
    corps = load_stock_codes(args.market)
    total = len(corps)

    # 이미 처리된 종목 제외
    remaining = [(c, n, m) for c, n, m in corps if c not in skip_set]
    skipped   = total - len(remaining)

    print(f"\n대상 종목: {total}개 (시장={args.market or '전체'})")
    print(f"  이미 완료: {skipped}개 건너뜀")
    print(f"  처리 예정: {len(remaining)}개")
    if s_path.exists():
        print(f"  상태 파일: {s_path}")
    print()

    failed_this_run = []

    for idx, (code, name, market) in enumerate(remaining, 1):
        total_done = skipped + idx - 1
        print(f"[{total_done + 1}/{total}] {code} {name} ({market})")

        ok = run_backfill(code, args.from_date, args.dry_run)

        if ok:
            state["completed"].append(code)
            if code in state["failed"]:
                state["failed"].remove(code)
        else:
            print(f"  !! FAILED: {code} {name}")
            failed_this_run.append((code, name, market))
            if code not in state["failed"]:
                state["failed"].append(code)

        save_state(s_path, state)

    # 결과 요약
    total_completed = skipped + len(remaining) - len(failed_this_run)
    print(f"\n{'='*60}")
    print(f"완료: {total_completed}/{total} 성공")
    print(f"상태 파일: {s_path}")

    if failed_this_run:
        print(f"\n실패 종목 ({len(failed_this_run)}개):")
        for code, name, market in failed_this_run:
            print(f"  {code}|{name}|{market}")
        sys.exit(1)

    # 전체 성공 시 상태 파일에 완료 표시
    state["finished_at"] = datetime.now().isoformat(timespec="seconds")
    save_state(s_path, state)
    print("모든 종목 backfill 완료.")


if __name__ == "__main__":
    main()
