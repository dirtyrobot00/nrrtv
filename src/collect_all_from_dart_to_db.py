#!/usr/bin/env python
"""DART 전체 종목 재무보고서 백필 수집 스크립트.

data/all_corp.list의 모든 종목에 대해 20200101부터 오늘까지의
사업보고서/반기보고서/분기보고서를 DART API로 수집해 DB에 저장.

Usage:
    # 전체 종목 수집
    python src/collect_all_from_dart_to_db.py

    # 특정 종목코드부터 재시작 (중단 후 이어서)
    python src/collect_all_from_dart_to_db.py --start-from 005930

    # 건너뛸 종목 수 지정
    python src/collect_all_from_dart_to_db.py --skip 100

    # 수집 시작일 변경
    python src/collect_all_from_dart_to_db.py --start-date 20230101

    # dry-run (API 호출 없이 종목 목록만 확인)
    python src/collect_all_from_dart_to_db.py --dry-run
"""

import argparse
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

# 프로젝트 루트를 path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.collectors.financial_report.financial_report_collector import FinancialReportCollector
from src.models.document import DocumentType
from src.storage.document_store import DocumentStore
from src.utils.config import get_config
from src.utils.logger import setup_logging, get_logger


CORP_LIST_PATH = project_root / "data" / "all_corp.list"
DEFAULT_START_DATE = "20200101"


def load_stock_codes(list_path: Path) -> list[tuple[str, str, str]]:
    """종목 리스트 파일에서 (종목코드, 종목명, 시장) 목록을 읽는다.

    # 로 시작하는 주석 라인은 건너뜀.
    형식: 종목코드|종목명|시장구분
    종목코드가 6자리 숫자인 것만 반환.
    """
    corps = []
    with open(list_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|")
            if len(parts) < 1:
                continue
            code = parts[0].strip()
            name = parts[1].strip() if len(parts) > 1 else ""
            market = parts[2].strip() if len(parts) > 2 else ""
            # 6자리 숫자만
            if len(code) == 6 and code.isdigit():
                corps.append((code, name, market))
    return corps


def collect_for_stock(
    collector: FinancialReportCollector,
    stock_code: str,
    corp_name: str,
    start_date_str: str,
    end_date_str: str,
    logger,
) -> int:
    """단일 종목에 대해 DART 재무보고서를 수집해 DB에 저장.

    Returns:
        수집된 보고서 수 (신규 + 업데이트)
    """
    collected = 0
    page = 1

    while True:
        params = {
            "bgn_de": start_date_str,
            "end_de": end_date_str,
            "stock_code": stock_code,
            "pblntf_ty": "A",  # 정기공시
            "page_count": 100,
            "page_no": page,
        }

        try:
            response = collector._call_dart_api("/list.json", params)
        except Exception as e:
            logger.error("dart_api_call_failed", stock_code=stock_code, error=str(e))
            break

        if response.get("status") == "013":
            # 013 = 조회된 데이터가 없습니다
            break

        if response.get("status") != "000":
            logger.warning(
                "dart_api_non_zero_status",
                stock_code=stock_code,
                status=response.get("status"),
                message=response.get("message"),
            )
            break

        items = response.get("list", [])
        if not items:
            break

        for item in items:
            report_name = item.get("report_nm", "")
            if not collector._is_financial_report(report_name):
                continue

            rcept_no = item["rcept_no"]
            is_duplicate = collector._check_duplicate_by_rcept_no(rcept_no)

            try:
                report = collector._parse_report(item)
            except ValueError as e:
                logger.warning(
                    "report_parse_error",
                    stock_code=stock_code,
                    rcept_no=rcept_no,
                    error=str(e),
                )
                continue

            # 문서 다운로드
            xml_files = collector._download_document(report.rcept_no, report)

            try:
                if xml_files:
                    main_file = xml_files[0]
                    doc = collector._create_document(
                        DocumentType.DISCLOSURE,
                        report.original_url,
                        main_file,
                        metadata=report.model_dump(mode="json"),
                    )
                    report.document_id = doc.id
                    report.pdf_url = str(main_file)
                else:
                    doc_id = str(uuid.uuid4())
                    doc_dict = {
                        "id": doc_id,
                        "doc_type": DocumentType.DISCLOSURE.value,
                        "source": collector.source_name,
                        "url": report.original_url,
                        "file_path": None,
                        "collected_at": datetime.utcnow(),
                        "metadata": report.model_dump(mode="json"),
                    }
                    collector.store.create_document(doc_dict)
                    report.document_id = doc_id

                if is_duplicate:
                    collector.store.update_financial_report_by_rcept_no(
                        report.rcept_no, report.to_dict()
                    )
                else:
                    collector.store.create_financial_report(report.to_dict())

                collected += 1
                logger.debug(
                    "report_saved",
                    stock_code=stock_code,
                    report_nm=report_name,
                    rcept_no=rcept_no,
                    is_duplicate=is_duplicate,
                )

            except Exception as e:
                logger.warning(
                    "db_save_error",
                    stock_code=stock_code,
                    rcept_no=rcept_no,
                    error=str(e)[:120],
                )

        if len(items) < 100:
            break
        page += 1

    return collected


def main():
    parser = argparse.ArgumentParser(description="DART 전체 종목 재무보고서 백필 수집")
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="수집 시작일 YYYYMMDD (기본: 20200101)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="수집 종료일 YYYYMMDD (기본: 오늘)",
    )
    parser.add_argument(
        "--start-from",
        default=None,
        help="이 종목코드부터 시작 (이전 종목 건너뜀, 재시작 용도)",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="앞에서부터 N개 종목 건너뜀",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="종목 목록만 출력하고 실제 수집 안 함",
    )
    parser.add_argument(
        "--corp-list",
        default=str(CORP_LIST_PATH),
        help="종목 리스트 파일 경로",
    )
    parser.add_argument(
        "--stock-code",
        default=None,
        help="단일 종목코드만 처리 (래퍼에서 호출 시 사용)",
    )
    args = parser.parse_args()

    setup_logging(log_level="INFO", log_to_file=True, log_format="text")
    logger = get_logger(__name__)

    # 날짜 설정
    start_date_str = args.start_date
    end_date_str = args.end_date or datetime.now().strftime("%Y%m%d")

    # 종목 리스트 로드
    corp_list_path = Path(args.corp_list)
    if not corp_list_path.exists():
        logger.error("corp_list_not_found", path=str(corp_list_path))
        sys.exit(1)

    all_corps = load_stock_codes(corp_list_path)
    logger.info("corp_list_loaded", total=len(all_corps), path=str(corp_list_path))

    # --stock-code: 단일 종목만 처리
    if args.stock_code:
        all_corps = [(c, n, m) for c, n, m in all_corps if c == args.stock_code]
        if not all_corps:
            logger.error("stock_code_not_found", stock_code=args.stock_code)
            sys.exit(1)

    # --skip 적용
    if args.skip > 0:
        all_corps = all_corps[args.skip:]
        logger.info("skipped_corps", skip=args.skip, remaining=len(all_corps))

    # --start-from 적용
    if args.start_from:
        idx = next(
            (i for i, (code, _, _) in enumerate(all_corps) if code == args.start_from),
            None,
        )
        if idx is None:
            logger.error("start_from_not_found", stock_code=args.start_from)
            sys.exit(1)
        all_corps = all_corps[idx:]
        logger.info(
            "starting_from_corp",
            stock_code=args.start_from,
            remaining=len(all_corps),
        )

    logger.info("=" * 70)
    logger.info("DART 전체 종목 백필 수집")
    logger.info("=" * 70)
    logger.info(f"수집 기간: {start_date_str} ~ {end_date_str}")
    logger.info(f"대상 종목 수: {len(all_corps)}")
    logger.info(f"Dry-run: {args.dry_run}")
    logger.info("=" * 70)

    if args.dry_run:
        for code, name, market in all_corps[:20]:
            logger.info(f"  {code} | {name} | {market}")
        if len(all_corps) > 20:
            logger.info(f"  ... 외 {len(all_corps) - 20}개")
        return

    # 수집기 초기화
    config = get_config()
    api_key = config.get("dart.api_key")
    if not api_key:
        logger.error("dart_api_key_missing", hint="config.yaml의 dart.api_key 또는 .env의 DART_API_KEY 확인")
        sys.exit(1)

    collector = FinancialReportCollector(api_key=api_key, rate_limit=0.5)

    total_collected = 0
    total_errors = 0

    try:
        for idx, (stock_code, corp_name, market) in enumerate(all_corps):
            progress = f"[{idx + 1}/{len(all_corps)}]"
            logger.info(
                "processing_corp",
                progress=progress,
                stock_code=stock_code,
                corp_name=corp_name,
                market=market,
            )

            try:
                n = collect_for_stock(
                    collector=collector,
                    stock_code=stock_code,
                    corp_name=corp_name,
                    start_date_str=start_date_str,
                    end_date_str=end_date_str,
                    logger=logger,
                )
                total_collected += n
                logger.info(
                    "corp_done",
                    stock_code=stock_code,
                    corp_name=corp_name,
                    collected=n,
                    total_so_far=total_collected,
                )
            except Exception as e:
                total_errors += 1
                logger.error(
                    "corp_failed",
                    stock_code=stock_code,
                    corp_name=corp_name,
                    error=str(e),
                )

    finally:
        collector.close()

    logger.info("=" * 70)
    logger.info("수집 완료")
    logger.info(f"총 수집 보고서: {total_collected}")
    logger.info(f"오류 종목 수: {total_errors}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
