"""Research Report PDF 수집 스크립트.

data/raw/research_report/ 디렉토리의 PDF 파일에서 텍스트를 추출해 DB에 저장합니다.

사용법:
    # backfill: 전체 PDF 일괄 처리
    python3 script/collect_research_reports.py --mode backfill

    # backfill 테스트 (5개만)
    python3 script/collect_research_reports.py --mode backfill --limit 5

    # realtime: 오늘 날짜 파일만 처리
    python3 script/collect_research_reports.py --mode realtime

    # 특정 디렉토리 지정
    python3 script/collect_research_reports.py --mode backfill --pdf-dir data/raw/research_report

    # 중복 파일도 재처리
    python3 script/collect_research_reports.py --mode backfill --no-skip
"""

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가 (외부 실행 지원)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.collectors.research_report.report_collector import ResearchReportCollector


def run_backfill(pdf_dir: str, limit: int | None, skip_existing: bool):
    """전체 PDF 파일 일괄 처리."""
    print(f"[BACKFILL] {pdf_dir} (limit={limit or 'all'}, skip_existing={skip_existing})")
    collector = ResearchReportCollector(pdf_dir=pdf_dir)
    stats = collector.collect(limit=limit, skip_existing=skip_existing)
    _print_stats(stats)


def run_realtime(pdf_dir: str, days: int = 1, skip_existing: bool = True):
    """최근 N일 내 파일만 처리 (파일명 날짜 기준)."""
    from src.collectors.research_report.report_collector import file_checksum, parse_filename, extract_text_from_pdf
    cutoff = date.today() - timedelta(days=days - 1)
    print(f"[REALTIME] {pdf_dir} (since {cutoff})")

    collector = ResearchReportCollector(pdf_dir=pdf_dir)
    collector.store.create_tables()
    collector._migrate_content_column()

    pdf_files = []
    for f in sorted(Path(pdf_dir).glob("*.pdf")):
        meta = parse_filename(f.name)
        if meta and meta.get("report_date") and meta["report_date"] >= cutoff:
            pdf_files.append(f)

    print(f"  대상 파일: {len(pdf_files)}개")
    stats = {"total": len(pdf_files), "saved": 0, "skipped": 0, "error": 0}
    for f in pdf_files:
        try:
            result = collector._process_file(f, skip_existing=skip_existing)
            if result == "saved":
                stats["saved"] += 1
            elif result == "skipped":
                stats["skipped"] += 1
        except Exception as e:
            print(f"[ERROR] {f.name}: {e}")
            stats["error"] += 1

    _print_stats(stats)


def _print_stats(stats: dict):
    print(f"\n완료: 전체={stats['total']}, 저장={stats['saved']}, 스킵={stats['skipped']}, 오류={stats['error']}")


def main():
    parser = argparse.ArgumentParser(description="Research Report PDF → DB 수집기")
    parser.add_argument("--mode", required=True, choices=["backfill", "realtime"],
                        help="backfill: 전체 처리 / realtime: 오늘 파일만 처리")
    parser.add_argument("--pdf-dir", default="data/raw/research_report",
                        help="PDF 파일 디렉토리 (기본값: data/raw/research_report)")
    parser.add_argument("--limit", type=int, default=None,
                        help="처리할 최대 파일 수 (backfill 전용)")
    parser.add_argument("--days", type=int, default=1,
                        help="최근 N일 파일 처리 (realtime 전용, 기본값: 1)")
    parser.add_argument("--no-skip", action="store_true",
                        help="이미 저장된 파일도 재처리")
    args = parser.parse_args()

    skip_existing = not args.no_skip

    if args.mode == "backfill":
        run_backfill(args.pdf_dir, args.limit, skip_existing)
    elif args.mode == "realtime":
        run_realtime(args.pdf_dir, args.days, skip_existing)


if __name__ == "__main__":
    main()
