"""DART 전체 공시 폴링 수집기 (드라이런 포함)

모든 상장 종목의 공시를 corp_code 없이 전체 조회한 뒤,
관심 종목 whitelist로 클라이언트 측 필터링하는 방식.

호출 효율:
  - 종목별 개별 호출: 2,500+ 종목 × 5 유형 = 12,500+ 회/사이클
  - 본 방식 (전체 조회 후 필터): ~8회/사이클 (오늘 기준)

공시 유형 (pblntf_ty):
  A: 정기공시   B: 주요사항보고  D: 지분공시
  E: 기타공시   I: 거래소공시

사용법:
  # 드라이런 1회 폴링 (실제 저장 없이 로그만 출력)
  python src/collectors/collect_all_from_dart.py --dry-run

  # 드라이런 + 관심 종목 지정
  python src/collectors/collect_all_from_dart.py --dry-run --stocks 005930,000660,035720

  # 60초 간격 폴링 (실운영)
  python src/collectors/collect_all_from_dart.py --interval 60

  # 10초 간격 폴링 (거의 실시간)
  python src/collectors/collect_all_from_dart.py --interval 10 --stocks 005930,000660
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------

DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DART_DOC_URL  = "https://opendart.fss.or.kr/api/document.xml"

# 폴링 대상 공시 유형
PBLNTF_TYPES = {
    "A": "정기공시",
    "B": "주요사항보고",
    "D": "지분공시",
    "E": "기타공시",
    "I": "거래소공시",
}

# 한 번에 가져올 최대 건수 (DART API 최대값)
PAGE_COUNT = 100

# ---------------------------------------------------------------------------
# 핵심 클래스
# ---------------------------------------------------------------------------

class DartAllDisclosurePoller:
    """DART 전체 공시 폴링 클래스.

    corp_code 없이 전체 공시를 조회하고, 클라이언트 측에서 관심 종목을 필터링.
    rcept_no 커서 방식으로 새 공시만 추출 (YYYYMMDDXXXXXXX 순번 활용).
    """

    def __init__(
        self,
        api_key: str,
        stock_whitelist: Optional[list[str]] = None,
        dry_run: bool = True,
        verbose: bool = True,
    ):
        """
        Args:
            api_key: DART Open API 인증키
            stock_whitelist: 관심 종목 코드 리스트 (None이면 전체 수집)
            dry_run: True면 XML 다운로드 없이 로그만 출력
            verbose: 상세 로그 출력 여부
        """
        self.api_key = api_key
        self.stock_whitelist = set(stock_whitelist) if stock_whitelist else None
        self.dry_run = dry_run
        self.verbose = verbose

        # rcept_no 커서: 유형별로 마지막으로 처리한 rcept_no 보관
        # 초기값은 오늘 날짜 기준 최솟값 ("YYYYMMDD0000000")
        today = datetime.now().strftime("%Y%m%d")
        self._cursors: dict[str, str] = {ty: f"{today}0000000" for ty in PBLNTF_TYPES}

        self.client = httpx.Client(timeout=30, follow_redirects=True)

        # 통계
        self._stats = {
            "total_api_calls": 0,
            "total_items_fetched": 0,
            "total_new_items": 0,
            "total_hits": 0,
            "cycles": 0,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def poll_once(self) -> list[dict]:
        """단일 폴링 사이클 실행.

        Returns:
            관심 종목에 hit된 공시 목록 (whitelist=None이면 신규 공시 전체)
        """
        cycle_start = time.time()
        self._stats["cycles"] += 1
        today = datetime.now().strftime("%Y%m%d")

        all_hits: list[dict] = []
        api_calls_this_cycle = 0
        new_items_this_cycle = 0

        self._log(f"\n{'='*60}")
        self._log(f"[사이클 #{self._stats['cycles']}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._log(f"{'='*60}")

        for ty_code, ty_name in PBLNTF_TYPES.items():
            cursor = self._cursors[ty_code]
            new_disclosures, calls = self._fetch_new_since(
                pblntf_ty=ty_code,
                bgn_de=today,
                cursor=cursor,
            )
            api_calls_this_cycle += calls
            new_items_this_cycle += len(new_disclosures)

            if new_disclosures:
                # 커서 갱신: 이번 사이클에서 가져온 것 중 가장 큰 rcept_no
                max_rcept_no = max(d["rcept_no"] for d in new_disclosures)
                self._cursors[ty_code] = max_rcept_no

            # whitelist 필터링
            hits = self._filter_by_whitelist(new_disclosures)
            all_hits.extend(hits)

            self._log(
                f"  [{ty_code}] {ty_name:10s} | "
                f"신규 {len(new_disclosures):3d}건, "
                f"hit {len(hits):3d}건, "
                f"API {calls}회 호출"
            )

        # 통계 갱신
        self._stats["total_api_calls"] += api_calls_this_cycle
        self._stats["total_new_items"] += new_items_this_cycle
        self._stats["total_hits"] += len(all_hits)

        elapsed = time.time() - cycle_start
        self._log(f"\n  → 총 API {api_calls_this_cycle}회, 신규 {new_items_this_cycle}건, hit {len(all_hits)}건 ({elapsed:.1f}s)")

        # hit 출력
        if all_hits:
            self._log(f"\n  ★ 관심 종목 공시 감지!")
            for disc in all_hits:
                self._print_hit(disc)

            if not self.dry_run:
                self._download_and_save(all_hits)
        else:
            whitelist_info = f"whitelist {len(self.stock_whitelist)}개 종목" if self.stock_whitelist else "전체 종목"
            self._log(f"  → {whitelist_info} 신규 공시 없음")

        return all_hits

    def run(self, interval_sec: int = 60, max_cycles: Optional[int] = None) -> None:
        """반복 폴링 실행.

        Args:
            interval_sec: 폴링 간격 (초)
            max_cycles: 최대 사이클 수 (None이면 무한)
        """
        mode = "DRY-RUN" if self.dry_run else "LIVE"
        whitelist_info = f"{len(self.stock_whitelist)}개 종목" if self.stock_whitelist else "전체 종목"
        self._log(f"\n{'#'*60}")
        self._log(f"# DART 전체 공시 폴링 시작 [{mode}]")
        self._log(f"# 대상: {whitelist_info}")
        self._log(f"# 간격: {interval_sec}초  |  유형: {', '.join(PBLNTF_TYPES.keys())}")
        self._log(f"{'#'*60}")

        cycle = 0
        try:
            while True:
                self.poll_once()
                cycle += 1

                if max_cycles and cycle >= max_cycles:
                    break

                self._log(f"\n  다음 폴링까지 {interval_sec}초 대기... (Ctrl+C로 종료)")
                time.sleep(interval_sec)

        except KeyboardInterrupt:
            self._log("\n\n[종료] 사용자 인터럽트")

        self._print_summary()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_new_since(
        self,
        pblntf_ty: str,
        bgn_de: str,
        cursor: str,
    ) -> tuple[list[dict], int]:
        """cursor보다 큰 rcept_no를 가진 신규 공시만 반환.

        Args:
            pblntf_ty: 공시 유형 코드
            bgn_de: 조회 시작일 (YYYYMMDD)
            cursor: 마지막으로 처리한 rcept_no

        Returns:
            (신규 공시 목록, API 호출 횟수)
        """
        new_items: list[dict] = []
        api_calls = 0
        page_no = 1

        while True:
            params = {
                "crtfc_key": self.api_key,
                "pblntf_ty": pblntf_ty,
                "bgn_de": bgn_de,
                "page_no": page_no,
                "page_count": PAGE_COUNT,
            }

            try:
                resp = self.client.get(DART_LIST_URL, params=params)
                resp.raise_for_status()
                api_calls += 1
                self._stats["total_api_calls"] += 0  # 사이클 집계는 caller에서
            except httpx.HTTPError as e:
                self._log(f"  [ERROR] API 호출 실패 (ty={pblntf_ty}, page={page_no}): {e}")
                break

            data = resp.json()

            # DART API 오류 코드 처리
            status = data.get("status", "000")
            if status == "013":
                # 조회 결과 없음 (정상)
                break
            if status != "000":
                self._log(f"  [WARN] DART API 오류 (status={status}, message={data.get('message', '')})")
                break

            items = data.get("list", [])
            self._stats["total_items_fetched"] += len(items)

            # 페이지 내 항목을 rcept_no 내림차순으로 정렬 (DART 기본 정렬 확인)
            # rcept_no > cursor인 것만 수집
            found_old = False
            for item in items:
                rcept_no = item.get("rcept_no", "")
                if rcept_no > cursor:
                    new_items.append(item)
                else:
                    # rcept_no는 순번이므로 이보다 작으면 더 볼 필요 없음
                    found_old = True

            # 총 페이지 수 확인
            total_count = int(data.get("total_count", 0))
            total_pages = (total_count + PAGE_COUNT - 1) // PAGE_COUNT

            if found_old or page_no >= total_pages:
                break

            page_no += 1

        return new_items, api_calls

    def _filter_by_whitelist(self, disclosures: list[dict]) -> list[dict]:
        """whitelist에 있는 종목만 필터링."""
        if self.stock_whitelist is None:
            return disclosures  # whitelist 없으면 전체 반환
        return [d for d in disclosures if d.get("stock_code", "").strip() in self.stock_whitelist]

    def _print_hit(self, disc: dict) -> None:
        """hit된 공시 정보 출력."""
        rcept_no   = disc.get("rcept_no", "")
        corp_name  = disc.get("corp_name", "")
        stock_code = disc.get("stock_code", "").strip()
        report_nm  = disc.get("report_nm", "")
        rcept_dt   = disc.get("rcept_dt", "")
        flr_nm     = disc.get("flr_nm", "")  # 제출인명

        # rcept_dt: YYYYMMDD → YYYY-MM-DD
        if len(rcept_dt) == 8:
            rcept_dt = f"{rcept_dt[:4]}-{rcept_dt[4:6]}-{rcept_dt[6:8]}"

        dry_tag = "[DRY-RUN] " if self.dry_run else ""
        self._log(
            f"    {dry_tag}★ [{stock_code}] {corp_name} | {report_nm} "
            f"| 제출: {flr_nm} | 접수: {rcept_dt} | no: {rcept_no}"
        )

    def _download_and_save(self, disclosures: list[dict]) -> None:
        """XML 다운로드 및 저장 (실운영 모드).

        dry_run=False일 때만 호출됨.
        """
        output_dir = Path("data/raw/dart_disclosures")
        output_dir.mkdir(parents=True, exist_ok=True)

        for disc in disclosures:
            rcept_no   = disc.get("rcept_no", "")
            corp_name  = disc.get("corp_name", "").replace(" ", "_")
            stock_code = disc.get("stock_code", "").strip()

            try:
                resp = self.client.get(
                    DART_DOC_URL,
                    params={"crtfc_key": self.api_key, "rcept_no": rcept_no},
                )
                resp.raise_for_status()

                # ZIP 응답을 파일로 저장
                filename = f"{rcept_no}_{stock_code}_{corp_name}.zip"
                file_path = output_dir / filename
                file_path.write_bytes(resp.content)

                self._log(f"    [저장] {file_path} ({len(resp.content):,} bytes)")

            except httpx.HTTPError as e:
                self._log(f"    [ERROR] 다운로드 실패 (rcept_no={rcept_no}): {e}")

    def _print_summary(self) -> None:
        """폴링 종료 후 통계 출력."""
        self._log(f"\n{'='*60}")
        self._log("# 폴링 종료 통계")
        self._log(f"{'='*60}")
        self._log(f"  총 사이클    : {self._stats['cycles']}회")
        self._log(f"  총 API 호출  : {self._stats['total_api_calls']}회")
        self._log(f"  총 조회 건수 : {self._stats['total_items_fetched']}건")
        self._log(f"  총 신규 건수 : {self._stats['total_new_items']}건")
        self._log(f"  총 hit 건수  : {self._stats['total_hits']}건")

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(msg, flush=True)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.client.close()


# ---------------------------------------------------------------------------
# CLI 진입점
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="DART 전체 공시 폴링 수집기",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="XML 다운로드 없이 로그만 출력 (기본: True)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="실운영 모드 (XML 다운로드 및 저장 수행)",
    )
    parser.add_argument(
        "--stocks",
        type=str,
        default="",
        help="관심 종목 코드 (콤마 구분, 예: 005930,000660,035720). 미지정시 전체 수집",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="폴링 간격 (초). 0이면 1회만 실행 (기본: 0)",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="최대 폴링 사이클 수 (기본: 무한)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default="",
        help="DART API 키 (미지정시 환경변수 DART_API_KEY 사용)",
    )

    args = parser.parse_args()

    # API 키 결정
    api_key = args.api_key or os.environ.get("DART_API_KEY", "")
    if not api_key:
        print("[ERROR] DART_API_KEY가 설정되지 않았습니다. .env 또는 --api-key 옵션을 사용하세요.")
        sys.exit(1)

    # 드라이런 여부 (--live 플래그가 있으면 실운영)
    dry_run = not args.live

    # 관심 종목 파싱
    stock_whitelist = None
    if args.stocks:
        stock_whitelist = [s.strip() for s in args.stocks.split(",") if s.strip()]

    with DartAllDisclosurePoller(
        api_key=api_key,
        stock_whitelist=stock_whitelist,
        dry_run=dry_run,
        verbose=True,
    ) as poller:
        if args.interval > 0:
            poller.run(interval_sec=args.interval, max_cycles=args.max_cycles)
        else:
            # 1회 실행
            poller.poll_once()
            poller._print_summary()


if __name__ == "__main__":
    main()
