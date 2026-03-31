#!/usr/bin/env python3
"""공시보고서 처리 파이프라인.

data/raw/financial_report/{stock_code}_{company}/ 안의 XML 파일을 파싱하여
data/processed/financial_report/{stock_code}_{company}/ 에 저장하고
DB에 report_key와 processed_dir 메타데이터를 업데이트합니다.

report_key 형식: '{종목코드}_{연도}{보고서종류}'
  Q1 → 1분기  예) 178320_20201분기
  H1 → 2분기  예) 178320_20202분기
  Q3 → 3분기  예) 178320_20203분기
  FY → 4분기  예) 178320_20204분기

사용법:
    python src/pipeline/process_financial_reports.py --stock 178320
    python src/pipeline/process_financial_reports.py --stock 178320 --dry-run
"""

import argparse
import os
import re
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from src.storage.document_store import DocumentStore

# extractor 함수 직접 import
extractor_path = project_root / 'src' / 'extractor'
sys.path.insert(0, str(extractor_path))
from extract_financial_report_table_flattened import (
    parse_full_document,
    split_by_sections,
    clean_text,
)
from bs4 import BeautifulSoup

# period 코드 → 분기 한글 매핑
PERIOD_TO_BUNGI = {
    'Q1': '1분기',
    'H1': '2분기',
    'Q3': '3분기',
    'FY': '4분기',
}


def parse_filename(xml_path: Path):
    """파일명에서 메타데이터 추출.

    패턴: {stock_code}_{company}_{report_type}_{year}{period}_{rcept_no}.xml
    예)   178320_서진시스템_quarterly_2020Q1_20200515001557.xml
    """
    name = xml_path.stem
    pattern = r'^(\d+)_(.+?)_(annual|semi_annual|quarterly)_(\d{4})(Q1|Q3|H1|FY)_(\d+)$'
    m = re.match(pattern, name)
    if not m:
        return None
    stock_code, corp_name, report_type, year, period, rcept_no = m.groups()
    bungi = PERIOD_TO_BUNGI[period]
    return {
        'stock_code': stock_code,
        'corp_name': corp_name,
        'report_type': report_type,
        'fiscal_year': int(year),
        'period': period,
        'bungi': bungi,
        'rcept_no': rcept_no,
        'report_key': f"{stock_code}_{year}{bungi}",
    }


def migrate_db_columns(store: DocumentStore):
    """report_key, processed_dir 컬럼이 없으면 ALTER TABLE로 추가."""
    import sqlalchemy
    with store.engine.connect() as conn:
        inspector = sqlalchemy.inspect(store.engine)
        cols = [c['name'] for c in inspector.get_columns('financial_reports')]
        if 'report_key' not in cols:
            conn.execute(sqlalchemy.text(
                'ALTER TABLE financial_reports ADD COLUMN report_key VARCHAR(30)'
            ))
            conn.commit()
            print("  → DB 마이그레이션: report_key 컬럼 추가")
        if 'processed_dir' not in cols:
            conn.execute(sqlalchemy.text(
                'ALTER TABLE financial_reports ADD COLUMN processed_dir TEXT'
            ))
            conn.commit()
            print("  → DB 마이그레이션: processed_dir 컬럼 추가")


def process_xml_file(xml_path: Path, output_dir: Path) -> dict:
    """XML 파일을 파싱하여 output_dir에 저장. 저장된 파일 정보 반환."""
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(xml_path, 'r', encoding='utf-8') as f:
        xml_content = f.read()

    # 전체 문서 파싱 (평탄화)
    full_content = parse_full_document(xml_content)

    # 출력 파일명 결정 (회사명_보고서명_full_text.txt)
    soup = BeautifulSoup(xml_content, 'html.parser')
    company_tag = soup.find('company-name')
    doc_tag = soup.find('document-name')

    filename_parts = []
    if company_tag:
        name = re.sub(r'[^\w가-힣\s]', '', clean_text(company_tag.get_text())).strip()
        if name:
            filename_parts.append(name)
    if doc_tag:
        doc = re.sub(r'[^\w가-힣\s]', '', clean_text(doc_tag.get_text())).strip()
        if doc:
            filename_parts.append(doc)

    full_filename = '_'.join(filename_parts) + '_full_text.txt' if filename_parts else 'full_document.txt'
    full_output_file = output_dir / full_filename

    with open(full_output_file, 'w', encoding='utf-8') as f:
        f.write(full_content)

    # 섹션별 분리
    sections = split_by_sections(xml_content)
    for section_id, section_data in sections.items():
        section_file = output_dir / f"{section_id}.txt"
        with open(section_file, 'w', encoding='utf-8') as f:
            f.write(section_data['content'])

    # 목차 파일
    index_file = output_dir / 'index.txt'
    with open(index_file, 'w', encoding='utf-8') as f:
        f.write('=' * 80 + '\n섹션별 파일 목록\n' + '=' * 80 + '\n\n')
        for section_id, section_data in sorted(sections.items(), key=lambda x: x[1]['order']):
            sf = output_dir / f"{section_id}.txt"
            size_kb = sf.stat().st_size / 1024
            f.write(f"{section_data['order']:2d}. {section_data['title']}\n")
            f.write(f"    파일: {section_id}.txt ({size_kb:.1f} KB)\n\n")

    return {'sections': len(sections), 'full_file': str(full_output_file)}


def update_db(store: DocumentStore, rcept_no: str, report_key: str, processed_dir: str) -> int:
    """financial_reports 테이블의 report_key, processed_dir 업데이트."""
    import sqlalchemy
    with store.engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                'UPDATE financial_reports SET report_key=:rk, processed_dir=:pd WHERE rcept_no=:rn'
            ),
            {'rk': report_key, 'pd': processed_dir, 'rn': rcept_no}
        )
        conn.commit()
        return result.rowcount


def main():
    parser = argparse.ArgumentParser(description='공시보고서 XML 파싱 및 DB 업데이트')
    parser.add_argument('--stock', '-s', required=True, help='종목코드 (예: 178320)')
    parser.add_argument('--dry-run', action='store_true', help='파싱 없이 목록만 출력')
    args = parser.parse_args()

    raw_base = project_root / 'data' / 'raw' / 'financial_report'
    processed_base = project_root / 'data' / 'processed' / 'financial_report'

    # 종목 디렉토리 탐색
    candidates = list(raw_base.glob(f'{args.stock}_*'))
    if not candidates:
        print(f"오류: {raw_base}/{args.stock}_* 디렉토리를 찾을 수 없습니다.")
        sys.exit(1)
    stock_dir = candidates[0]
    stock_folder = stock_dir.name  # e.g. '178320_서진시스템'

    xml_files = sorted(stock_dir.glob('*.xml'))
    if not xml_files:
        print(f"오류: XML 파일이 없습니다: {stock_dir}")
        sys.exit(1)

    output_base = processed_base / stock_folder

    print('=' * 70)
    print('공시보고서 처리 파이프라인')
    print('=' * 70)
    print(f'종목코드     : {args.stock}')
    print(f'입력 디렉토리: {stock_dir}')
    print(f'출력 디렉토리: {output_base}')
    print(f'XML 파일 수  : {len(xml_files)}개')
    print()

    # DB 연결 및 컬럼 마이그레이션
    store = DocumentStore()
    store.create_tables()
    migrate_db_columns(store)
    print()

    success, failed = 0, 0

    for i, xml_path in enumerate(xml_files, 1):
        meta = parse_filename(xml_path)
        if not meta:
            print(f'[{i}/{len(xml_files)}] ⚠️  파일명 파싱 실패: {xml_path.name}')
            failed += 1
            continue

        label = f"{meta['fiscal_year']}{meta['bungi']}  rcept_no={meta['rcept_no']}"
        print(f'[{i}/{len(xml_files)}] report_key={meta["report_key"]}  ({label})')

        if args.dry_run:
            print('   [dry-run] skip')
            continue

        try:
            out_dir = output_base / xml_path.stem
            info = process_xml_file(xml_path, out_dir)
            rows = update_db(store, meta['rcept_no'], meta['report_key'], str(out_dir))

            rel = out_dir.relative_to(project_root)
            status = f"DB ✓" if rows else f"DB ⚠️ (rcept_no 없음)"
            print(f'   ✓ 저장: {rel}  섹션:{info["sections"]}개  {status}')
            success += 1
        except Exception as e:
            print(f'   ✗ 오류: {e}')
            import traceback
            traceback.print_exc()
            failed += 1

    print()
    print('=' * 70)
    print(f'처리 완료: 성공 {success}개 / 실패 {failed}개')
    if not args.dry_run:
        print(f'출력 위치: {output_base}')
    print('=' * 70)


if __name__ == '__main__':
    main()
