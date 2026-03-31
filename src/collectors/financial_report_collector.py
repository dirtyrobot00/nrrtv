"""Financial report collector module.

This module provides a collector for DART financial reports (quarterly/semi-annual/annual).
"""

import io
import re
import time
import uuid
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests

from src.collectors.base import BaseCollector, CollectorError
from src.models.document import Document, DocumentType
from src.models.financial_report import FinancialReport, ReportType, ReportPeriod
from src.storage.document_store import DocumentStore


class FinancialReportCollector(BaseCollector):
    """Collector for DART financial reports.

    Collects quarterly, semi-annual, and annual financial reports from DART Open API.
    """

    # Report keywords to filter
    REPORT_KEYWORDS = [
        "사업보고서",
        "반기보고서",
        "분기보고서"
    ]

    def __init__(
        self,
        api_key: str,
        output_dir: Optional[Path] = None,
        rate_limit: float = 1.0,
        timeout: int = 30
    ):
        """Initialize DART financial report collector.

        Args:
            api_key: DART API key
            output_dir: Output directory for PDFs
            rate_limit: Requests per second
            timeout: Request timeout in seconds
        """
        super().__init__(
            source_name="dart_financial_reports",
            output_dir=output_dir,
            rate_limit=rate_limit,
            timeout=timeout
        )
        self.api_key = api_key
        self.base_url = "https://opendart.fss.or.kr/api"

        self.logger.info(
            "dart_collector_initialized",
            api_key_prefix=api_key[:10] + "..." if api_key else None
        )

    def _is_financial_report(self, report_name: str) -> bool:
        """Check if report is a financial report (quarterly/semi-annual/annual).

        Args:
            report_name: Report name from DART

        Returns:
            True if financial report, False otherwise
        """
        return any(keyword in report_name for keyword in self.REPORT_KEYWORDS)

    def _parse_report_type(self, report_name: str) -> ReportType:
        """Parse report type from report name.

        Args:
            report_name: Report name

        Returns:
            ReportType enum

        Raises:
            ValueError: If report type cannot be determined
        """
        if "사업보고서" in report_name:
            return ReportType.ANNUAL
        elif "반기보고서" in report_name:
            return ReportType.SEMI_ANNUAL
        elif "분기보고서" in report_name:
            return ReportType.QUARTERLY
        else:
            raise ValueError(f"Unknown report type: {report_name}")

    def _parse_report_period(
        self,
        report_name: str,
        report_type: ReportType
    ) -> ReportPeriod:
        """Parse report period from report name.

        Args:
            report_name: Report name
            report_type: Report type

        Returns:
            ReportPeriod enum
        """
        if report_type == ReportType.ANNUAL:
            return ReportPeriod.FY

        if report_type == ReportType.SEMI_ANNUAL:
            return ReportPeriod.Q2

        # Quarterly - parse from date
        # "분기보고서 (2024.03)" -> Q1
        # "분기보고서 (2024.09)" -> Q3
        if ".03" in report_name or ".05" in report_name:
            return ReportPeriod.Q1
        elif ".09" in report_name or ".11" in report_name:
            return ReportPeriod.Q3
        else:
            # Default to Q1
            return ReportPeriod.Q1

    def _parse_fiscal_year(self, report_name: str) -> int:
        """Parse fiscal year from report name.

        Args:
            report_name: Report name like "사업보고서 (2024.12)"

        Returns:
            Fiscal year as integer

        Raises:
            ValueError: If year cannot be parsed
        """
        match = re.search(r'\((\d{4})', report_name)
        if match:
            return int(match.group(1))
        else:
            raise ValueError(f"Cannot parse fiscal year from: {report_name}")

    def _build_dart_url(self, rcept_no: str) -> str:
        """Build DART document URL.

        Args:
            rcept_no: DART receipt number

        Returns:
            DART document URL
        """
        return f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

    def _parse_report(self, dart_item: Dict) -> FinancialReport:
        """Parse DART API item to FinancialReport.

        Args:
            dart_item: Item from DART API response

        Returns:
            FinancialReport object
        """
        report_name = dart_item['report_nm']
        report_type = self._parse_report_type(report_name)
        report_period = self._parse_report_period(report_name, report_type)
        fiscal_year = self._parse_fiscal_year(report_name)

        # Build fiscal period string
        if report_type == ReportType.ANNUAL:
            fiscal_period = f"{fiscal_year}.01.01-{fiscal_year}.12.31"
        elif report_type == ReportType.SEMI_ANNUAL:
            fiscal_period = f"{fiscal_year}.01.01-{fiscal_year}.06.30"
        elif report_period == ReportPeriod.Q1:
            fiscal_period = f"{fiscal_year}.01.01-{fiscal_year}.03.31"
        else:  # Q3
            fiscal_period = f"{fiscal_year}.07.01-{fiscal_year}.09.30"

        # Parse filing date
        rcept_dt_str = dart_item['rcept_dt']
        filed_at = datetime.strptime(rcept_dt_str, '%Y%m%d')

        # Create report
        stock_code = dart_item.get('stock_code')
        # Convert empty string to None
        if stock_code == '':
            stock_code = None

        report = FinancialReport(
            document_id="",  # Will be set after document creation
            corp_code=dart_item['corp_code'],
            corp_name=dart_item['corp_name'],
            stock_code=stock_code,
            report_type=report_type,
            report_period=report_period,
            rcept_no=dart_item['rcept_no'],
            rcept_dt=rcept_dt_str,
            report_nm=report_name,
            fiscal_year=fiscal_year,
            fiscal_period=fiscal_period,
            original_url=self._build_dart_url(dart_item['rcept_no']),
            filed_at=filed_at
        )

        return report

    def _call_dart_api(self, endpoint: str, params: Dict) -> Dict:
        """Call DART API.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            API response JSON

        Raises:
            CollectorError: If API call fails
        """
        url = f"{self.base_url}{endpoint}"

        # Add API key
        params['crtfc_key'] = self.api_key

        # Rate limiting
        self._rate_limit_sleep()

        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()

            # Check DART API status
            if data.get('status') != '000':
                self.logger.warning(
                    "dart_api_error",
                    status=data.get('status'),
                    message=data.get('message'),
                    endpoint=endpoint
                )

            return data

        except requests.RequestException as e:
            raise CollectorError(f"DART API call failed: {e}")

    def _check_duplicate_by_rcept_no(self, rcept_no: str) -> bool:
        """Check if report already exists by receipt number.

        Args:
            rcept_no: DART receipt number

        Returns:
            True if duplicate exists
        """
        existing = self.store.get_financial_report_by_rcept_no(rcept_no)
        if existing:
            self.logger.debug(
                "duplicate_report_found",
                rcept_no=rcept_no,
                report_id=existing["id"]
            )
            return True
        return False

    def _build_filename(self, report: 'FinancialReport', file_suffix: str = "") -> str:
        """Build readable filename with stock code and company name.

        Format: {stock_code}_{corp_name}_{report_type}_{fiscal_year}Q{period}_{rcept_no}{suffix}.xml
        Example: 005930_삼성전자_quarterly_2024Q3_20241115000123.xml

        Args:
            report: FinancialReport object
            file_suffix: Optional suffix (e.g., "_00760" for additional files)

        Returns:
            Formatted filename
        """
        # Stock code (6 digits) or "UNLISTED"
        stock_code = report.stock_code if report.stock_code else "UNLISTED"

        # Sanitize company name (remove special characters)
        corp_name = re.sub(r'[^\w가-힣]', '', report.corp_name)

        # Report type
        report_type = report.report_type if isinstance(report.report_type, str) else report.report_type.value

        # Period suffix
        if report_type == "quarterly":
            period = report.report_period if isinstance(report.report_period, str) else report.report_period.value
            # Extract just the number: "q1" -> "1", "Q3" -> "3"
            period_num = period.lower().replace('q', '')
            period_suffix = f"Q{period_num}"
        elif report_type == "semi_annual":
            period_suffix = "H1"
        else:  # annual
            period_suffix = "FY"

        # Build filename
        filename = f"{stock_code}_{corp_name}_{report_type}_{report.fiscal_year}{period_suffix}_{report.rcept_no}{file_suffix}.xml"

        return filename

    def _download_document(self, rcept_no: str, report: 'FinancialReport') -> Optional[List[Path]]:
        """Download document from DART.

        DART provides documents as ZIP files containing XML files.
        Files are saved directly under output_dir with readable names.

        Args:
            rcept_no: DART receipt number
            report: FinancialReport object for filename generation

        Returns:
            List of downloaded file paths or None if not available
        """
        try:
            # Call DART document API
            url = f"{self.base_url}/document.xml"
            params = {
                'crtfc_key': self.api_key,
                'rcept_no': rcept_no
            }

            self.logger.info("downloading_document", rcept_no=rcept_no)

            # Rate limiting
            self._rate_limit_sleep()

            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            # Check if response is valid
            if len(response.content) < 100:
                self.logger.warning(
                    "document_too_small",
                    rcept_no=rcept_no,
                    size=len(response.content)
                )
                return None

            # Extract ZIP file
            try:
                zf = zipfile.ZipFile(io.BytesIO(response.content))
                downloaded_files = []

                # Extract all XML files directly to output_dir with readable names
                for idx, fname in enumerate(zf.namelist()):
                    xml_data = zf.read(fname)

                    # Try to decode with proper encoding
                    try:
                        xml_text = xml_data.decode('euc-kr')
                    except UnicodeDecodeError:
                        try:
                            xml_text = xml_data.decode('utf-8')
                        except UnicodeDecodeError:
                            self.logger.warning(
                                "encoding_error",
                                rcept_no=rcept_no,
                                filename=fname
                            )
                            continue

                    # Build readable filename
                    # First file: no suffix, additional files: _00760, _00761, etc.
                    if idx == 0:
                        file_suffix = ""
                    else:
                        # Extract suffix from original filename if exists
                        original_suffix = fname.replace('.xml', '').replace(rcept_no, '')
                        file_suffix = original_suffix if original_suffix else f"_{idx:05d}"

                    readable_filename = self._build_filename(report, file_suffix)
                    xml_path = self.output_dir / readable_filename

                    # Save XML file
                    xml_path.write_text(xml_text, encoding='utf-8')
                    downloaded_files.append(xml_path)

                self.logger.info(
                    "document_downloaded",
                    rcept_no=rcept_no,
                    files=len(downloaded_files),
                    filenames=[f.name for f in downloaded_files]
                )

                return downloaded_files

            except zipfile.BadZipFile:
                self.logger.warning(
                    "invalid_zip_file",
                    rcept_no=rcept_no
                )
                return None

        except requests.RequestException as e:
            self.logger.error(
                "document_download_failed",
                rcept_no=rcept_no,
                error=str(e)
            )
            return None

    def collect_by_date(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: Optional[int] = None
    ) -> List[FinancialReport]:
        """Collect financial reports by date range.

        Args:
            start_date: Start date
            end_date: End date
            limit: Maximum number of reports to collect

        Returns:
            List of FinancialReport objects
        """
        reports = []
        current_date = start_date

        while current_date <= end_date:
            date_str = current_date.strftime('%Y%m%d')

            self.logger.info(
                "collecting_reports_for_date",
                date=date_str
            )

            # Call DART API
            params = {
                'bgn_de': date_str,
                'end_de': date_str,
                'pblntf_ty': 'A',  # 정기공시만
                'page_count': 100
            }

            page = 1
            while True:
                params['page_no'] = page

                response = self._call_dart_api('/list.json', params)

                if response['status'] != '000':
                    self.logger.error(
                        "dart_api_error",
                        status=response['status'],
                        message=response.get('message')
                    )
                    break

                items = response.get('list', [])
                if not items:
                    break

                # Filter and parse financial reports
                for item in items:
                    report_name = item['report_nm']

                    # Filter: only financial reports
                    if not self._is_financial_report(report_name):
                        continue

                    # Check if already exists (for logging only, still proceed)
                    is_duplicate = self._check_duplicate_by_rcept_no(item['rcept_no'])
                    if is_duplicate:
                        self.logger.info(
                            "reprocessing_existing_report",
                            rcept_no=item['rcept_no']
                        )

                    # Parse report
                    report = self._parse_report(item)

                    # Download document (ZIP with XML files)
                    xml_files = self._download_document(report.rcept_no, report)

                    # Create document record
                    try:
                        if xml_files and len(xml_files) > 0:
                            # Document downloaded successfully
                            # Use the first XML file as the main file
                            main_file = xml_files[0]

                            doc = self._create_document(
                                DocumentType.DISCLOSURE,
                                report.original_url,
                                main_file,
                                metadata=report.model_dump(mode='json')  # JSON-serializable
                            )
                            report.document_id = doc.id
                            report.pdf_url = str(main_file)  # Store main file path
                        else:
                            # No PDF - create document without file
                            # Generate document ID manually
                            doc_id = str(uuid.uuid4())

                            # Create minimal document record
                            doc_dict = {
                                "id": doc_id,
                                "doc_type": DocumentType.DISCLOSURE.value,
                                "source": self.source_name,
                                "url": report.original_url,
                                "file_path": None,
                                "collected_at": datetime.utcnow(),  # Keep as datetime object for SQLAlchemy
                                "metadata": report.model_dump(mode='json')  # JSON-serializable for metadata
                            }
                            self.store.create_document(doc_dict)
                            report.document_id = doc_id

                        # Save or update financial report in database
                        if is_duplicate:
                            # Update existing record
                            self.store.update_financial_report_by_rcept_no(
                                report.rcept_no,
                                report.to_dict()
                            )
                        else:
                            # Create new record
                            self.store.create_financial_report(report.to_dict())

                        reports.append(report)
                    except Exception as e:
                        # Handle duplicate checksum or other DB errors - continue anyway
                        self.logger.warning(
                            "db_error_continuing",
                            rcept_no=item['rcept_no'],
                            error=str(e)[:100]
                        )
                        reports.append(report)  # Still count as collected

                    self.logger.info(
                        "financial_report_collected",
                        corp_name=report.corp_name,
                        report_type=report.report_type,
                        fiscal_year=report.fiscal_year,
                        rcept_no=report.rcept_no
                    )

                    # Check limit
                    if limit and len(reports) >= limit:
                        self.logger.info("reached_collection_limit", limit=limit)
                        return reports

                # Next page
                if len(items) < 100:
                    break
                page += 1

            # Next date
            current_date += timedelta(days=1)

        self.logger.info(
            "collection_completed",
            total_reports=len(reports),
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )

        return reports

    def collect(self, limit: Optional[int] = None) -> List[Document]:
        """Collect financial reports (implements abstract method).

        Args:
            limit: Maximum number of reports

        Returns:
            List of Document objects (for compatibility)
        """
        # Default: collect today's reports
        today = datetime.now()
        reports = self.collect_by_date(today, today, limit=limit)

        # Return as Document objects for compatibility
        return [Document(
            doc_type=DocumentType.DISCLOSURE,
            source=self.source_name,
            url=report.original_url,
            file_path=report.pdf_url,
            metadata=report.to_dict()
        ) for report in reports]
