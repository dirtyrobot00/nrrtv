"""PDF collector module.

This module implements PDF research report collection from Korean securities firms,
primarily focusing on Naver Finance research section.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.collectors.financial_report.base import BaseCollector, CollectorError
from src.models.document import Document, DocumentType
from src.utils.config import get_config


class PDFCollector(BaseCollector):
    """PDF research report collector.

    Collects research reports from Naver Finance and other securities firms.
    Focuses on Korean market research reports in PDF format.
    """

    def __init__(
        self,
        source_name: str = "naver_finance_research",
        output_dir: Optional[Path] = None,
        rate_limit: float = 1.0
    ):
        """Initialize PDF collector.

        Args:
            source_name: Name of the source (from sources.yaml)
            output_dir: Directory to save PDFs
            rate_limit: Requests per second limit
        """
        # Get source configuration
        config = get_config()
        self.source_config = config.get_source("pdf_sources", source_name)

        if not self.source_config:
            raise CollectorError(f"PDF source not found: {source_name}")

        if not self.source_config.get("enabled", False):
            raise CollectorError(f"PDF source is disabled: {source_name}")

        # Initialize base collector
        super().__init__(
            source_name=source_name,
            output_dir=output_dir,
            rate_limit=rate_limit or self.source_config.get("rate_limit", 1.0)
        )

        self.base_url = self.source_config.get("base_url")
        self.selectors = self.source_config.get("selectors", {})

        self.logger.info(
            "pdf_collector_initialized",
            source=source_name,
            base_url=self.base_url
        )

    def collect(self, limit: Optional[int] = None, max_pages: int = 1) -> List[Document]:
        """Collect PDF research reports.

        Args:
            limit: Maximum number of PDFs to collect
            max_pages: Maximum pages to scrape (for pagination)

        Returns:
            List of collected Document objects
        """
        self.logger.info("collection_started", source=self.source_name, limit=limit, max_pages=max_pages)

        collected_docs = []
        page = 1

        try:
            while page <= max_pages:
                # Build URL for current page
                page_url = self._build_page_url(page)
                self.logger.info("scraping_page", page=page, url=page_url)

                # Fetch page content
                response = self._make_request(page_url)
                html_content = response.text

                # Parse page and extract report links
                reports = self._parse_report_list(html_content, page_url)
                self.logger.info("reports_found", page=page, count=len(reports))

                # Collect each report
                for report_info in reports:
                    # Check limit
                    if limit and len(collected_docs) >= limit:
                        self.logger.info("limit_reached", collected=len(collected_docs))
                        return collected_docs

                    # Check for duplicates
                    if self._check_duplicate(report_info["url"]):
                        self.logger.debug("skipping_duplicate", url=report_info["url"])
                        continue

                    # Download PDF
                    try:
                        doc = self._download_pdf(report_info)
                        collected_docs.append(doc)
                        self.logger.info(
                            "pdf_collected",
                            document_id=doc.id,
                            title=report_info.get("title", "N/A")
                        )
                    except Exception as e:
                        self.logger.error(
                            "pdf_download_failed",
                            url=report_info["url"],
                            error=str(e)
                        )
                        continue

                # Move to next page
                page += 1

                # Check if pagination is enabled
                if not self.source_config.get("pagination", {}).get("enabled", False):
                    break

            self.logger.info("collection_completed", total_collected=len(collected_docs))
            return collected_docs

        except Exception as e:
            self.logger.error("collection_failed", error=str(e), exc_info=True)
            raise CollectorError(f"Collection failed: {e}")

    def _build_page_url(self, page: int) -> str:
        """Build URL for a specific page.

        Args:
            page: Page number

        Returns:
            URL string
        """
        pagination = self.source_config.get("pagination", {})

        if not pagination.get("enabled", False):
            return self.base_url

        # Add page parameter
        page_param = pagination.get("page_param", "page")
        separator = "&" if "?" in self.base_url else "?"

        return f"{self.base_url}{separator}{page_param}={page}"

    def _parse_report_list(self, html_content: str, page_url: str) -> List[Dict]:
        """Parse HTML to extract report information.

        Args:
            html_content: HTML content
            page_url: URL of the page (for building absolute URLs)

        Returns:
            List of report info dictionaries
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        reports = []

        # Find report list
        list_selector = self.selectors.get("report_list")
        if not list_selector:
            raise CollectorError("Missing selector: report_list")

        report_elements = soup.select(list_selector)
        self.logger.debug("report_elements_found", count=len(report_elements))

        for element in report_elements:
            try:
                report_info = self._extract_report_info(element, page_url)
                if report_info and report_info.get("pdf_url"):
                    reports.append(report_info)
            except Exception as e:
                self.logger.warning("failed_to_parse_report_element", error=str(e))
                continue

        return reports

    def _extract_report_info(self, element, base_url: str) -> Optional[Dict]:
        """Extract report information from HTML element.

        Args:
            element: BeautifulSoup element
            base_url: Base URL for building absolute URLs

        Returns:
            Report info dictionary or None
        """
        # Extract company name and ticker from first cell
        company_name = None
        ticker = None
        cells = element.select('td')

        if cells:
            # First cell contains company name with link to /item/main.naver?code=TICKER
            company_link = cells[0].select_one('a')
            if company_link:
                company_name = company_link.get_text(strip=True)
                # Extract ticker from link href
                href = company_link.get('href', '')
                if 'code=' in href:
                    ticker = href.split('code=')[-1].split('&')[0]

        # Extract title (report title from second cell, index 1)
        title = None
        if len(cells) > 1:
            title_elem = cells[1].select_one('a')
            title = title_elem.get_text(strip=True) if title_elem else None

        # Extract PDF link (from fourth cell, index 3)
        pdf_url = None
        if len(cells) > 3:
            pdf_elem = cells[3].select_one('a[href*=".pdf"]')
            if pdf_elem:
                pdf_url = pdf_elem.get("href")

        if not pdf_url:
            return None

        # Build absolute URL
        pdf_url = urljoin(base_url, pdf_url)

        # Extract date (from fifth cell, index 4)
        date_str = None
        if len(cells) > 4:
            date_str = cells[4].get_text(strip=True)

        # Extract firm (from third cell, index 2)
        firm = None
        if len(cells) > 2:
            firm = cells[2].get_text(strip=True)

        return {
            "url": pdf_url,
            "pdf_url": pdf_url,
            "title": title or "Unknown",
            "company_name": company_name,
            "ticker": ticker,
            "date": date_str,
            "analyst": None,
            "firm": firm
        }

    def _download_pdf(self, report_info: Dict) -> Document:
        """Download PDF file and create document record.

        Args:
            report_info: Report information dictionary

        Returns:
            Document object
        """
        pdf_url = report_info["pdf_url"]

        # Download PDF
        response = self._make_request(pdf_url)

        # Validate content type
        content_type = response.headers.get("content-type", "")
        if "pdf" not in content_type.lower():
            self.logger.warning("unexpected_content_type", url=pdf_url, content_type=content_type)

        # Generate filename
        filename = self._generate_filename(report_info)

        # Save file
        file_path = self._save_file(response.content, filename)

        # Create document record
        metadata = {
            "title": report_info.get("title"),
            "company_name": report_info.get("company_name"),
            "ticker": report_info.get("ticker"),
            "date": report_info.get("date"),
            "analyst": report_info.get("analyst"),
            "firm": report_info.get("firm"),
            "source_url": pdf_url
        }

        doc = self._create_document(
            doc_type=DocumentType.RESEARCH_REPORT,
            url=pdf_url,
            file_path=file_path,
            metadata=metadata
        )

        return doc

    def _generate_filename(self, report_info: Dict) -> str:
        """Generate filename for PDF.

        Args:
            report_info: Report information

        Returns:
            Filename string
        """
        # Extract components with safe defaults
        ticker = report_info.get("ticker") or "UNKNOWN"
        company_name = report_info.get("company_name") or "unknown"
        firm = report_info.get("firm") or "unknown"
        title = report_info.get("title") or "report"
        date_str = report_info.get("date") or ""

        # Ensure all are strings
        ticker = str(ticker) if ticker else "UNKNOWN"
        company_name = str(company_name) if company_name else "unknown"
        firm = str(firm) if firm else "unknown"
        title = str(title) if title else "report"
        date_str = str(date_str) if date_str else ""

        # Clean company name
        company_name = re.sub(r'[^\w가-힣]+', '_', company_name)[:20]

        # Clean firm name
        firm = re.sub(r'[^\w가-힣]+', '_', firm)[:20]

        # Clean title
        title = re.sub(r'[^\w가-힣]+', '_', title)[:40]

        # Parse date
        date_part = ""
        if date_str:
            # Try to extract date (format: YY.MM.DD or YYYY.MM.DD)
            match = re.search(r'(\d{2,4})[\.\-](\d{2})[\.\-](\d{2})', date_str)
            if match:
                year = match.group(1)
                # Convert YY to YYYY if needed
                if len(year) == 2:
                    year = f"20{year}"
                date_part = f"{year}{match.group(2)}{match.group(3)}_"

        # Build filename: [DATE_]TICKER_CompanyName_Firm_Title.pdf
        # Example: 20251222_005930_삼성전자_미래에셋증권_2025년_전망.pdf
        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{date_part}{ticker}_{company_name}_{firm}_{title}_{timestamp}.pdf"

        return filename

    def collect_by_ticker(self, ticker: str, limit: Optional[int] = None) -> List[Document]:
        """Collect reports for a specific ticker.

        Args:
            ticker: Stock ticker code
            limit: Maximum number of reports

        Returns:
            List of collected Document objects
        """
        # This would require ticker-specific search
        # For now, collect all and filter
        self.logger.warning(
            "ticker_filter_not_implemented",
            ticker=ticker,
            message="Collecting all reports. Ticker filtering not yet implemented."
        )

        return self.collect(limit=limit)
