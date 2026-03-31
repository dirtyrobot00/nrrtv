#!/usr/bin/env python3
"""특정 종목의 기간별 사업보고서 수집 스크립트.

DART API를 사용하여 특정 종목의 모든 분기/반기/연간 사업보고서를 수집합니다.
감사보고서는 제외하고 사업보고서만 수집합니다.

사용법:
    # 삼성전자 2018~2025년 사업보고서 수집
    python scripts/collect_stock_reports.py --stock 005930 --start 2018 --end 2025

    # 특정 종목 전체 기간 수집
    python scripts/collect_stock_reports.py --stock 005930

    # 출력 디렉토리 지정
    python scripts/collect_stock_reports.py --stock 005930 --start 2020 --end 2024 --output ./my_reports

    # dry-run 모드 (실제 다운로드 없이 목록만 확인)
    python scripts/collect_stock_reports.py --stock 005930 --start 2020 --end 2024 --dry-run
"""

import argparse
import io
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load .env file
from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from src.storage.document_store import DocumentStore
from src.models.document import DocumentType


class StockReportCollector:
    """특정 종목의 사업보고서 수집기."""

    # 수집 대상 보고서 유형
    REPORT_TYPES = {
        "사업보고서": "annual",      # 연간 (4분기/FY)
        "반기보고서": "semi_annual", # 반기 (2분기/H1)
        "분기보고서": "quarterly",   # 분기 (1분기/Q1, 3분기/Q3)
    }

    def __init__(self, api_key: str, output_dir: Path, rate_limit: float = 1.0, save_to_db: bool = True):
        """초기화.

        Args:
            api_key: DART API 키
            output_dir: 출력 디렉토리
            rate_limit: API 호출 간격 (초)
            save_to_db: DB에 저장 여부
        """
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limit = rate_limit
        self.base_url = "https://opendart.fss.or.kr/api"
        self.last_request_time = 0
        self.save_to_db = save_to_db

        # DB 연결
        if save_to_db:
            self.store = DocumentStore()
            self.store.create_tables()
        else:
            self.store = None

    def _rate_limit_sleep(self):
        """API 호출 제한을 위한 대기."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request_time = time.time()

    def _call_api(self, endpoint: str, params: Dict) -> Dict:
        """DART API 호출.

        Args:
            endpoint: API 엔드포인트
            params: 요청 파라미터

        Returns:
            API 응답 JSON
        """
        url = f"{self.base_url}{endpoint}"
        params['crtfc_key'] = self.api_key

        self._rate_limit_sleep()

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        return response.json()

    def get_corp_code(self, stock_code: str) -> Optional[Tuple[str, str]]:
        """종목코드로 기업코드 조회.

        Args:
            stock_code: 종목코드 (예: 005930)

        Returns:
            (기업코드, 기업명) 튜플 또는 None
        """
        # DART 기업코드 목록 다운로드
        url = f"{self.base_url}/corpCode.xml"
        params = {'crtfc_key': self.api_key}

        self._rate_limit_sleep()

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()

        # ZIP 파일에서 XML 추출
        try:
            zf = zipfile.ZipFile(io.BytesIO(response.content))
            xml_data = zf.read(zf.namelist()[0])
            xml_text = xml_data.decode('utf-8')

            # 각 list 항목을 파싱하여 정확한 종목코드 매칭
            # XML 구조: <list><corp_code>...</corp_code><corp_name>...</corp_name><stock_code>...</stock_code>...</list>
            list_pattern = r'<list>(.*?)</list>'
            for list_match in re.finditer(list_pattern, xml_text, re.DOTALL):
                item = list_match.group(1)

                # 종목코드 추출
                stock_match = re.search(r'<stock_code>\s*(\d*)\s*</stock_code>', item)
                if stock_match:
                    item_stock_code = stock_match.group(1).strip()
                    if item_stock_code == stock_code:
                        # 기업코드 추출
                        corp_match = re.search(r'<corp_code>\s*(\d+)\s*</corp_code>', item)
                        # 기업명 추출
                        name_match = re.search(r'<corp_name>([^<]+)</corp_name>', item)
                        if corp_match:
                            corp_code = corp_match.group(1).strip()
                            corp_name = name_match.group(1).strip() if name_match else "Unknown"
                            return (corp_code, corp_name)

            return None

        except Exception as e:
            print(f"기업코드 조회 실패: {e}")
            return None

    def get_corp_info(self, corp_code: str) -> Optional[Dict]:
        """기업 정보 조회.

        Args:
            corp_code: 기업코드

        Returns:
            기업 정보 딕셔너리
        """
        params = {'corp_code': corp_code}
        response = self._call_api('/company.json', params)

        if response.get('status') == '000':
            return response
        return None

    def search_reports(
        self,
        corp_code: str,
        start_year: int,
        end_year: int
    ) -> List[Dict]:
        """기간 내 사업보고서 검색.

        Args:
            corp_code: 기업코드
            start_year: 시작 연도
            end_year: 종료 연도

        Returns:
            보고서 목록
        """
        reports = []

        # 검색 기간 설정 (여유있게 1년 전부터 검색)
        start_date = f"{start_year - 1}0101"
        end_date = f"{end_year + 1}1231"

        print(f"검색 기간: {start_date} ~ {end_date}")

        page = 1
        while True:
            params = {
                'corp_code': corp_code,
                'bgn_de': start_date,
                'end_de': end_date,
                'pblntf_ty': 'A',  # 정기공시
                'page_no': page,
                'page_count': 100
            }

            response = self._call_api('/list.json', params)

            if response.get('status') != '000':
                if response.get('status') == '013':  # 조회된 데이터가 없음
                    break
                print(f"API 오류: {response.get('message')}")
                break

            items = response.get('list', [])
            if not items:
                break

            # 사업보고서만 필터링 (감사보고서 제외)
            for item in items:
                report_nm = item.get('report_nm', '')

                # 사업보고서/반기보고서/분기보고서만 수집
                is_target = any(keyword in report_nm for keyword in self.REPORT_TYPES.keys())

                if is_target:
                    # 연도 추출
                    year_match = re.search(r'\((\d{4})', report_nm)
                    if year_match:
                        report_year = int(year_match.group(1))
                        if start_year <= report_year <= end_year:
                            reports.append(item)

            # 다음 페이지
            total_page = response.get('total_page', 1)
            if page >= total_page:
                break
            page += 1

        return reports

    def _parse_report_info(self, report: Dict) -> Dict:
        """보고서 정보 파싱.

        Args:
            report: DART API 응답 항목

        Returns:
            파싱된 보고서 정보
        """
        report_nm = report.get('report_nm', '')

        # 보고서 유형 결정
        report_type = None
        for keyword, type_name in self.REPORT_TYPES.items():
            if keyword in report_nm:
                report_type = type_name
                break

        # 연도 추출
        year_match = re.search(r'\((\d{4})', report_nm)
        fiscal_year = int(year_match.group(1)) if year_match else None

        # 분기 결정
        if report_type == "annual":
            period = "FY"
        elif report_type == "semi_annual":
            period = "H1"
        elif report_type == "quarterly":
            # 월 정보로 Q1/Q3 구분
            if ".03" in report_nm or ".05" in report_nm:
                period = "Q1"
            elif ".09" in report_nm or ".11" in report_nm:
                period = "Q3"
            else:
                period = "Q1"  # 기본값
        else:
            period = "UNKNOWN"

        return {
            'rcept_no': report.get('rcept_no'),
            'corp_code': report.get('corp_code'),
            'corp_name': report.get('corp_name'),
            'stock_code': report.get('stock_code'),
            'report_nm': report_nm,
            'report_type': report_type,
            'fiscal_year': fiscal_year,
            'period': period,
            'rcept_dt': report.get('rcept_dt'),
        }

    def download_report(
        self,
        rcept_no: str,
        report_info: Dict
    ) -> Optional[List[Path]]:
        """보고서 XML 다운로드.

        ZIP 파일에서 사업보고서 XML을 찾아 다운로드합니다.
        감사보고서가 아닌 사업보고서 파일을 자동으로 선택합니다.

        Args:
            rcept_no: 접수번호
            report_info: 보고서 정보

        Returns:
            다운로드된 파일 경로 리스트
        """
        url = f"{self.base_url}/document.xml"
        params = {
            'crtfc_key': self.api_key,
            'rcept_no': rcept_no
        }

        self._rate_limit_sleep()

        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()

            if len(response.content) < 100:
                print(f"  경고: 문서가 너무 작음 ({len(response.content)} bytes)")
                return None

            # ZIP 파일 처리
            zf = zipfile.ZipFile(io.BytesIO(response.content))
            downloaded_files = []

            # 사업보고서 파일 찾기
            business_report_file = None
            all_files = zf.namelist()

            for fname in all_files:
                xml_data = zf.read(fname)

                # 인코딩 시도
                try:
                    xml_text = xml_data.decode('euc-kr')
                except UnicodeDecodeError:
                    try:
                        xml_text = xml_data.decode('utf-8')
                    except UnicodeDecodeError:
                        continue

                # DOCUMENT-NAME 태그에서 문서 유형 확인
                doc_name_match = re.search(r'<DOCUMENT-NAME[^>]*>([^<]+)</DOCUMENT-NAME>', xml_text[:2000])
                if doc_name_match:
                    doc_name = doc_name_match.group(1)

                    # 사업보고서/반기보고서/분기보고서인 경우
                    if any(keyword in doc_name for keyword in self.REPORT_TYPES.keys()):
                        business_report_file = (fname, xml_text)
                        break

            if not business_report_file:
                print(f"  경고: 사업보고서 파일을 찾을 수 없음 (감사보고서만 있음)")
                return None

            # 파일명 생성
            stock_code = report_info.get('stock_code') or 'UNLISTED'
            corp_name = re.sub(r'[^\w가-힣]', '', report_info.get('corp_name', 'UNKNOWN'))
            report_type = report_info.get('report_type', 'unknown')
            fiscal_year = report_info.get('fiscal_year', 0)
            period = report_info.get('period', 'UNKNOWN')

            filename = f"{stock_code}_{corp_name}_{report_type}_{fiscal_year}{period}_{rcept_no}.xml"
            filepath = self.output_dir / filename

            # 저장
            filepath.write_text(business_report_file[1], encoding='utf-8')
            downloaded_files.append(filepath)

            return downloaded_files

        except zipfile.BadZipFile:
            print(f"  오류: 유효하지 않은 ZIP 파일")
            return None
        except Exception as e:
            print(f"  오류: {e}")
            return None

    def collect(
        self,
        stock_code: str,
        start_year: int,
        end_year: int,
        dry_run: bool = False
    ) -> List[Dict]:
        """종목의 사업보고서 수집.

        Args:
            stock_code: 종목코드
            start_year: 시작 연도
            end_year: 종료 연도
            dry_run: True면 다운로드 없이 목록만 출력

        Returns:
            수집된 보고서 정보 리스트
        """
        print("=" * 70)
        print(f"종목 사업보고서 수집")
        print("=" * 70)
        print(f"종목코드: {stock_code}")
        print(f"수집 기간: {start_year}년 ~ {end_year}년")
        print(f"출력 디렉토리: {self.output_dir}")
        print()

        # 1. 기업코드 조회
        print("1. 기업코드 조회 중...")
        result = self.get_corp_code(stock_code)
        if not result:
            print(f"오류: 종목코드 {stock_code}에 해당하는 기업을 찾을 수 없습니다.")
            return []
        corp_code, corp_name = result
        print(f"   기업코드: {corp_code}")
        print(f"   기업명: {corp_name}")

        # 2. 기업 정보 조회 (추가 정보)
        corp_info = self.get_corp_info(corp_code)
        if corp_info:
            print(f"   대표자: {corp_info.get('ceo_nm')}")
        print()

        # 3. 보고서 검색
        print("2. 사업보고서 검색 중...")
        reports = self.search_reports(corp_code, start_year, end_year)
        print(f"   검색된 보고서: {len(reports)}개")
        print()

        if not reports:
            print("검색된 보고서가 없습니다.")
            return []

        # 4. 보고서 정보 파싱 및 정렬
        parsed_reports = []
        for report in reports:
            info = self._parse_report_info(report)
            parsed_reports.append(info)

        # 연도/분기 순으로 정렬
        period_order = {'Q1': 1, 'H1': 2, 'Q3': 3, 'FY': 4}
        parsed_reports.sort(key=lambda x: (x['fiscal_year'], period_order.get(x['period'], 5)))

        # 4-1. 중복 제거: 동일 연도/분기에 여러 보고서가 있으면 최신(rcept_no가 큰) 것만 유지
        unique_reports, duplicate_reports = self._filter_duplicates(parsed_reports)

        if duplicate_reports:
            print(f"   ⚠️ 중복 보고서 {len(duplicate_reports)}개 발견 (구버전은 duplicates/로 이동됨)")

        parsed_reports = unique_reports

        # 5. 보고서 목록 출력
        print("3. 수집 대상 보고서 목록:")
        print("-" * 70)
        for i, info in enumerate(parsed_reports, 1):
            print(f"{i:3d}. {info['fiscal_year']}년 {info['period']:3s} | {info['report_type']:12s} | {info['rcept_no']} | {info['report_nm'][:30]}")
        print("-" * 70)
        print()

        if dry_run:
            print("[DRY-RUN 모드] 실제 다운로드는 수행하지 않습니다.")
            if duplicate_reports:
                print()
                print("중복(구버전) 보고서 (다운로드 시 duplicates/로 이동 예정):")
                for info in duplicate_reports:
                    print(f"  - {info['fiscal_year']}년 {info['period']} ({info['rcept_no']})")
            return parsed_reports

        # 5-1. 기존 파일 중 구버전을 duplicates/로 이동
        if duplicate_reports:
            moved_count = self._move_duplicates_to_folder(stock_code, duplicate_reports)
            if moved_count > 0:
                print(f"   📁 기존 구버전 파일 {moved_count}개를 duplicates/로 이동")

        # 6. 다운로드
        print("4. 다운로드 시작...")
        downloaded = []
        failed = []
        db_created = 0
        db_updated = 0

        for i, info in enumerate(parsed_reports, 1):
            print(f"[{i}/{len(parsed_reports)}] {info['fiscal_year']}년 {info['period']} 다운로드 중...")

            files = self.download_report(info['rcept_no'], info)

            if files:
                downloaded.append(info)
                print(f"   ✓ 완료: {files[0].name}")

                # DB 저장
                if self.save_to_db and self.store:
                    try:
                        db_result = self._save_to_database(info, files[0])
                        if db_result == 'created':
                            db_created += 1
                            print(f"   📝 DB 신규 등록")
                        elif db_result == 'updated':
                            db_updated += 1
                            print(f"   🔄 DB 업데이트")
                    except Exception as e:
                        print(f"   ⚠️ DB 저장 실패: {e}")
            else:
                failed.append(info)
                print(f"   ✗ 실패")

        # 7. 결과 요약
        print()
        print("=" * 70)
        print("수집 완료")
        print("=" * 70)
        print(f"총 대상: {len(parsed_reports)}개")
        print(f"성공: {len(downloaded)}개")
        print(f"실패: {len(failed)}개")
        print(f"출력 디렉토리: {self.output_dir}")

        if self.save_to_db:
            print()
            print(f"DB 신규 등록: {db_created}개")
            print(f"DB 업데이트: {db_updated}개")

        if failed:
            print()
            print("실패 목록:")
            for info in failed:
                print(f"  - {info['fiscal_year']}년 {info['period']} ({info['rcept_no']})")

        return downloaded

    def _save_to_database(self, report_info: Dict, file_path: Path) -> str:
        """보고서 정보를 DB에 저장.

        Args:
            report_info: 보고서 정보
            file_path: 저장된 파일 경로

        Returns:
            'created' 또는 'updated'
        """
        import uuid

        rcept_no = report_info['rcept_no']

        # 기존 레코드 확인
        existing = self.store.get_financial_report_by_rcept_no(rcept_no)

        # 보고서 데이터 생성
        report_data = {
            'id': existing['id'] if existing else str(uuid.uuid4()),
            'document_id': str(uuid.uuid4()),
            'corp_code': report_info['corp_code'],
            'corp_name': report_info['corp_name'],
            'stock_code': report_info['stock_code'] or None,
            'report_type': report_info['report_type'],
            'report_period': report_info['period'].lower() if report_info['period'] != 'FY' else 'fy',
            'rcept_no': rcept_no,
            'rcept_dt': report_info['rcept_dt'],
            'report_nm': report_info['report_nm'],
            'fiscal_year': report_info['fiscal_year'],
            'fiscal_period': self._build_fiscal_period(report_info),
            'original_url': f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
            'pdf_url': str(file_path),
            'filed_at': datetime.strptime(report_info['rcept_dt'], '%Y%m%d'),
        }

        if existing:
            # 업데이트
            self.store.update_financial_report_by_rcept_no(rcept_no, report_data)
            return 'updated'
        else:
            # 신규 생성
            # Document 레코드도 생성
            doc_dict = {
                'id': report_data['document_id'],
                'doc_type': DocumentType.DISCLOSURE.value,
                'source': 'dart_stock_reports',
                'url': report_data['original_url'],
                'file_path': str(file_path),
                'collected_at': datetime.now(timezone.utc),
                'metadata': report_info
            }
            self.store.create_document(doc_dict)
            self.store.create_financial_report(report_data)
            return 'created'

    def _move_duplicates_to_folder(self, stock_code: str, duplicate_reports: List[Dict]) -> int:
        """구버전 파일들을 duplicates/ 폴더로 이동.

        Args:
            stock_code: 종목코드
            duplicate_reports: 중복(구버전) 보고서 정보 리스트

        Returns:
            이동된 파일 수
        """
        import shutil

        duplicates_dir = self.output_dir / 'duplicates'
        duplicates_dir.mkdir(parents=True, exist_ok=True)

        moved_count = 0

        for report in duplicate_reports:
            rcept_no = report['rcept_no']

            # 해당 rcept_no를 가진 파일 찾기
            pattern = f"{stock_code}_*_{rcept_no}.xml"
            matching_files = list(self.output_dir.glob(pattern))

            for file_path in matching_files:
                dest_path = duplicates_dir / file_path.name
                try:
                    shutil.move(str(file_path), str(dest_path))
                    moved_count += 1
                    print(f"   📦 이동: {file_path.name} → duplicates/")
                except Exception as e:
                    print(f"   ⚠️ 이동 실패: {file_path.name} - {e}")

        return moved_count

    def _filter_duplicates(self, reports: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """동일 연도/분기에 여러 보고서가 있을 때 최신 것만 유지.

        Args:
            reports: 보고서 정보 리스트

        Returns:
            (유지할 보고서 리스트, 중복(구버전) 보고서 리스트)
        """
        from collections import defaultdict

        # 연도/분기별로 그룹화
        grouped = defaultdict(list)
        for report in reports:
            key = (report['fiscal_year'], report['period'])
            grouped[key].append(report)

        unique = []
        duplicates = []

        for key, group in grouped.items():
            if len(group) == 1:
                unique.append(group[0])
            else:
                # rcept_no 기준으로 정렬 (내림차순 - 최신이 먼저)
                sorted_group = sorted(group, key=lambda x: x['rcept_no'], reverse=True)
                unique.append(sorted_group[0])  # 최신 것만 유지
                duplicates.extend(sorted_group[1:])  # 나머지는 중복

        # 원래 정렬 순서 유지
        period_order = {'Q1': 1, 'H1': 2, 'Q3': 3, 'FY': 4}
        unique.sort(key=lambda x: (x['fiscal_year'], period_order.get(x['period'], 5)))

        return unique, duplicates

    def _build_fiscal_period(self, report_info: Dict) -> str:
        """회계기간 문자열 생성."""
        fiscal_year = report_info['fiscal_year']
        period = report_info['period']

        if period == 'FY':
            return f"{fiscal_year}.01.01-{fiscal_year}.12.31"
        elif period == 'H1':
            return f"{fiscal_year}.01.01-{fiscal_year}.06.30"
        elif period == 'Q1':
            return f"{fiscal_year}.01.01-{fiscal_year}.03.31"
        elif period == 'Q3':
            return f"{fiscal_year}.07.01-{fiscal_year}.09.30"
        else:
            return f"{fiscal_year}.01.01-{fiscal_year}.12.31"


def get_dart_api_key() -> str:
    """DART API 키 가져오기."""
    api_key = os.environ.get('DART_API_KEY')
    if not api_key:
        # config 파일에서 시도
        config_path = project_root / 'config' / 'config.yaml'
        if config_path.exists():
            import yaml
            with open(config_path) as f:
                config = yaml.safe_load(f)
                api_key = config.get('dart', {}).get('api_key')

    if not api_key:
        raise ValueError("DART API 키가 설정되지 않았습니다. DART_API_KEY 환경변수를 설정하세요.")

    return api_key


def main():
    parser = argparse.ArgumentParser(
        description='특정 종목의 기간별 사업보고서 수집',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--stock', '-s',
        type=str,
        required=True,
        help='종목코드 (예: 005930)'
    )

    parser.add_argument(
        '--start',
        type=int,
        default=2015,
        help='시작 연도 (기본값: 2015)'
    )

    parser.add_argument(
        '--end',
        type=int,
        default=datetime.now().year,
        help='종료 연도 (기본값: 현재 연도)'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        default=str(project_root / 'data' / 'raw' / 'financial_report'),
        help='출력 디렉토리'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='실제 다운로드 없이 목록만 확인'
    )

    parser.add_argument(
        '--rate-limit',
        type=float,
        default=1.0,
        help='API 호출 간격 (초, 기본값: 1.0)'
    )

    parser.add_argument(
        '--no-db',
        action='store_true',
        help='DB 저장 없이 파일만 다운로드'
    )

    args = parser.parse_args()

    # 연도 검증
    if args.start > args.end:
        print("오류: 시작 연도가 종료 연도보다 큽니다.")
        return 1

    try:
        api_key = get_dart_api_key()
    except ValueError as e:
        print(f"오류: {e}")
        return 1

    # 수집기 생성 및 실행
    collector = StockReportCollector(
        api_key=api_key,
        output_dir=Path(args.output),
        rate_limit=args.rate_limit,
        save_to_db=not args.no_db
    )

    results = collector.collect(
        stock_code=args.stock,
        start_year=args.start,
        end_year=args.end,
        dry_run=args.dry_run
    )

    return 0 if results else 1


if __name__ == "__main__":
    sys.exit(main())
