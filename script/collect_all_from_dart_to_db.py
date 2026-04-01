#!/usr/bin/env python3
"""
DART 전체 공시 수집 → DB 저장 (backfill)

src/ 소스코드 활용:
  - src/collectors/financial_report_collector.py  FinancialReportCollector._call_dart_api
  - src/storage/document_store.py                 DocumentStore (documents 테이블)
  - src/utils/config.py                           설정 로드 (dart.api_key)
  - src/utils/logger.py                           structlog 로깅

Usage:
    # 삼성전자 2020년부터 전체 수집 (기본값)
    python script/collect_all_from_dart_to_db.py

    # dry-run: DB 저장 없이 조회 건수만 확인
    python script/collect_all_from_dart_to_db.py --dry-run

    # 종목·기간 직접 지정
    python script/collect_all_from_dart_to_db.py --stock 005930 --start-year 2020 --end-year 2025

    # 제외 유형 변경 (기본: D=지분공시)
    python script/collect_all_from_dart_to_db.py --exclude-types D,G
"""

import argparse
import io
import sys
import time
import uuid
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

# ─── 프로젝트 루트를 sys.path에 추가 ─────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ─── src 모듈 임포트 ─────────────────────────────────────────────────────────
from src.collectors.financial_report.financial_report_collector import FinancialReportCollector
from src.models.document import DocumentType
from src.utils.config import get_config
from src.utils.logger import setup_logging, get_logger

# ─── 상수 ────────────────────────────────────────────────────────────────────
DART_BASE_URL = "https://opendart.fss.or.kr/api"
CORP_CODE_CACHE = PROJECT_ROOT / "script" / ".corp_code_cache.xml"

PBLNTF_TYPE_NAMES = {
    "A": "정기공시",
    "B": "주요사항보고",
    "C": "발행공시",
    "D": "지분공시",
    "E": "기타공시",
    "F": "외부감사관련",
    "G": "펀드공시",
    "H": "자산유동화",
    "I": "거래소공시",
    "J": "공정위공시",
}


# ─── corp_code 조회 ───────────────────────────────────────────────────────────

def fetch_corp_code(stock_code: str, api_key: str) -> Optional[str]:
    """종목코드 → DART corp_code 변환

    corpCode.xml을 로컬 캐시(24시간)하여 반복 다운로드를 방지한다.
    """
    xml_data: Optional[bytes] = None

    if CORP_CODE_CACHE.exists():
        age = time.time() - CORP_CODE_CACHE.stat().st_mtime
        if age < 86400:
            xml_data = CORP_CODE_CACHE.read_bytes()

    if xml_data is None:
        print("  DART corpCode.xml 다운로드 중...")
        resp = requests.get(
            f"{DART_BASE_URL}/corpCode.xml",
            params={"crtfc_key": api_key},
            timeout=60,
        )
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            xml_data = zf.read("CORPCODE.xml")
        CORP_CODE_CACHE.write_bytes(xml_data)
        print(f"  캐시 저장: {CORP_CODE_CACHE.name}")

    root = ET.fromstring(xml_data)
    for corp in root.findall("list"):
        if corp.findtext("stock_code", "").strip() == stock_code:
            return corp.findtext("corp_code", "").strip()

    return None


# ─── 공시 목록 수집 ───────────────────────────────────────────────────────────

def collect_disclosures_for_year(
    collector: FinancialReportCollector,
    corp_code: str,
    year: int,
    exclude_types: list,
) -> list:
    """특정 연도의 공시 목록을 모두 가져온다 (페이지네이션 자동 처리).

    FinancialReportCollector._call_dart_api 를 재사용하므로
    rate limiting·retry 로직이 그대로 적용된다.
    pblntf_ty 가 exclude_types 에 포함된 항목은 제외한다.
    """
    all_items = []
    page_no = 1

    while True:
        params = {
            "corp_code": corp_code,
            "bgn_de": f"{year}0101",
            "end_de": f"{year}1231",
            "page_no": page_no,
            "page_count": 100,  # DART API 최대값
        }

        data = collector._call_dart_api("/list.json", params)
        status = data.get("status", "")

        if status == "013":  # 데이터 없음
            break
        if status != "000":
            print(f"  [WARN] DART API 오류 status={status}: {data.get('message', '')}")
            break

        items = data.get("list", [])
        if not items:
            break

        for item in items:
            if item.get("pblntf_ty", "") not in exclude_types:
                all_items.append(item)

        total_count = int(data.get("total_count", 0))
        if page_no * 100 >= total_count:
            break

        page_no += 1

    return all_items


# ─── DB 저장 ─────────────────────────────────────────────────────────────────

def save_to_store(store, items: list, stock_code: str) -> tuple:
    """공시 목록을 DocumentStore(documents 테이블)에 저장.

    URL(dart.fss.or.kr/dsaf001/?rcpNo=...) 기준으로 중복을 방지한다.
    반환: (inserted, skipped)
    """
    inserted = 0
    skipped = 0

    for item in items:
        rcept_no = item.get("rcept_no", "").strip()
        if not rcept_no:
            continue

        url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

        # URL 기준 중복 체크 (DocumentStore.get_document_by_url)
        if store.get_document_by_url(url):
            skipped += 1
            continue

        # 접수일 → collected_at
        collected_at = datetime.now(timezone.utc)
        rcept_dt = item.get("rcept_dt", "")
        if len(rcept_dt) == 8:
            try:
                collected_at = datetime.strptime(rcept_dt, "%Y%m%d")
            except ValueError:
                pass

        corp_name = item.get("corp_name", "")
        report_nm = item.get("report_nm", "")

        doc_data = {
            "id": str(uuid.uuid4()),
            "doc_type": DocumentType.DISCLOSURE.value,
            "source": "dart",
            "url": url,
            "file_path": None,
            "collected_at": collected_at,
            "status": "pending",
            "metadata": {
                **item,
                "stock_code": stock_code,
                "title": f"{corp_name} — {report_nm}",
            },
        }

        store.create_document(doc_data)
        inserted += 1

    return inserted, skipped


# ─── 메인 ────────────────────────────────────────────────────────────────────

def main(args):
    stock_code: str = args.stock
    start_year: int = args.start_year
    end_year: int = args.end_year or datetime.now().year
    exclude_types: list = [t.strip() for t in args.exclude_types.split(",") if t.strip()]
    dry_run: bool = args.dry_run

    setup_logging(log_level="INFO", log_to_file=True, log_format="text")
    logger = get_logger(__name__)

    # dart.api_key 는 config/config.yaml → ${DART_API_KEY} → .env 순으로 로드
    config = get_config()
    api_key: str = config.get("dart.api_key", "")
    if not api_key:
        logger.error("DART_API_KEY가 설정되지 않았습니다. .env 또는 config/config.yaml 확인")
        sys.exit(1)

    excl_labels = ", ".join(f"{t}({PBLNTF_TYPE_NAMES.get(t, '?')})" for t in exclude_types)

    logger.info("=" * 70)
    logger.info("DART 공시 backfill 수집")
    logger.info(f"  종목코드  : {stock_code}")
    logger.info(f"  수집 기간 : {start_year} ~ {end_year}")
    logger.info(f"  제외 유형 : {excl_labels}")
    logger.info(f"  Dry-run   : {dry_run}")
    logger.info("=" * 70)

    # 1. corp_code 조회
    logger.info(f"[1] corp_code 조회 ({stock_code})")
    corp_code = fetch_corp_code(stock_code, api_key)
    if not corp_code:
        logger.error(f"{stock_code}에 해당하는 corp_code를 찾을 수 없음")
        sys.exit(1)
    logger.info(f"  → corp_code = {corp_code}")

    # 2. FinancialReportCollector 초기화 (DART API 호출 + DocumentStore 포함)
    collector = FinancialReportCollector(api_key=api_key)
    store = collector.store  # BaseCollector가 생성한 DocumentStore

    if not dry_run:
        store.create_tables()

    # 3. 연도별 수집 · 저장
    total_fetched = 0
    total_inserted = 0
    total_skipped = 0

    for year in range(start_year, end_year + 1):
        logger.info(f"\n[{year}년] {year}0101 ~ {year}1231")

        items = collect_disclosures_for_year(collector, corp_code, year, exclude_types)
        logger.info(f"  조회: {len(items)}건")
        total_fetched += len(items)

        if dry_run or not items:
            time.sleep(0.5)
            continue

        ins, skp = save_to_store(store, items, stock_code)
        total_inserted += ins
        total_skipped += skp
        logger.info(f"  저장: {ins}건  /  중복 스킵: {skp}건")

        time.sleep(0.5)

    # 4. 결과 요약
    logger.info("\n" + "=" * 70)
    logger.info("완료")
    logger.info(f"  총 조회  : {total_fetched}건")
    if not dry_run:
        logger.info(f"  신규 저장: {total_inserted}건")
        logger.info(f"  중복 스킵: {total_skipped}건")
        stats = store.get_stats()
        logger.info(f"  DB 총 문서: {stats['total_documents']}건")
    logger.info("=" * 70)


def parse_args():
    parser = argparse.ArgumentParser(
        description="DART 전체 공시 수집 → DB 저장 (backfill)"
    )
    parser.add_argument(
        "--stock", type=str, default="005930", metavar="STOCK_CODE",
        help="종목코드 (기본: 005930 삼성전자)",
    )
    parser.add_argument(
        "--start-year", type=int, default=2020, metavar="YEAR",
        help="수집 시작 연도 (기본: 2020)",
    )
    parser.add_argument(
        "--end-year", type=int, default=None, metavar="YEAR",
        help="수집 종료 연도 (기본: 현재 연도)",
    )
    parser.add_argument(
        "--exclude-types", type=str, default="D", metavar="TYPES",
        help="제외할 공시유형 콤마 구분 (기본: D=지분공시)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="DB 저장 없이 조회 건수만 확인",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
