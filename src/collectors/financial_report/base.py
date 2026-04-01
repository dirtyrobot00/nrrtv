"""Base collector module.

This module defines the abstract base class for all data collectors.
"""

import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import httpx

from src.models.document import Document, DocumentType
from src.storage.document_store import DocumentStore
from src.utils.config import get_config
from src.utils.logger import LoggerMixin
from src.utils.validators import sanitize_filename, validate_url


class CollectorError(Exception):
    """Raised when collection fails."""
    pass


class BaseCollector(ABC, LoggerMixin):
    """Abstract base class for data collectors.

    All collectors inherit from this class and implement the collect() method.
    Provides common functionality for rate limiting, error handling, and storage.

    Attributes:
        source_name: Name of the data source
        output_dir: Directory to save collected files
        rate_limit: Maximum requests per second
        timeout: Request timeout in seconds
        max_retries: Maximum retry attempts for failed requests
    """

    def __init__(
        self,
        source_name: str,
        output_dir: Optional[Path] = None,
        rate_limit: float = 1.0,
        timeout: int = 30,
        max_retries: int = 3
    ):
        """Initialize collector.

        Args:
            source_name: Name of the data source
            output_dir: Directory to save files (defaults to config setting)
            rate_limit: Requests per second limit
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
        """
        self.source_name = source_name
        self.rate_limit = rate_limit
        self.timeout = timeout
        self.max_retries = max_retries

        # Set output directory
        if output_dir is None:
            config = get_config()
            # Use financial_report for DART collector, fallback to pdf
            collector_key = "financial_report" if source_name == "dart_financial_reports" else "pdf"
            output_dir = Path(config.get(f"collectors.{collector_key}.output_dir", "data/raw/pdfs"))
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize HTTP client
        self.client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers=self._get_headers()
        )

        # Rate limiting
        self._last_request_time = 0.0

        # Document store
        self.store = DocumentStore()

        self.logger.info(
            "collector_initialized",
            source=source_name,
            output_dir=str(output_dir),
            rate_limit=rate_limit
        )

    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers for requests.

        Returns:
            Dictionary of HTTP headers
        """
        config = get_config()
        user_agent = config.get(
            "collectors.pdf.user_agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )

        return {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

    def _rate_limit_sleep(self) -> None:
        """Sleep to respect rate limit."""
        if self.rate_limit <= 0:
            return

        elapsed = time.time() - self._last_request_time
        min_interval = 1.0 / self.rate_limit

        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            self.logger.debug("rate_limit_sleep", sleep_time=sleep_time)
            time.sleep(sleep_time)

        self._last_request_time = time.time()

    def _make_request(
        self,
        url: str,
        method: str = "GET",
        **kwargs
    ) -> httpx.Response:
        """Make HTTP request with rate limiting and retry logic.

        Args:
            url: URL to request
            method: HTTP method
            **kwargs: Additional arguments for httpx.request

        Returns:
            HTTP response

        Raises:
            CollectorError: If request fails after all retries
        """
        if not validate_url(url):
            raise CollectorError(f"Invalid URL: {url}")

        for attempt in range(self.max_retries):
            try:
                # Rate limiting
                self._rate_limit_sleep()

                # Make request
                self.logger.debug("making_request", url=url, method=method, attempt=attempt + 1)
                response = self.client.request(method, url, **kwargs)
                response.raise_for_status()

                self.logger.info("request_success", url=url, status_code=response.status_code)
                return response

            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                self.logger.warning(
                    "http_error",
                    url=url,
                    status_code=status_code,
                    attempt=attempt + 1
                )

                # Special handling for 429 (Too Many Requests)
                if status_code == 429:
                    if attempt < self.max_retries - 1:
                        # Extract Retry-After header if available
                        retry_after = e.response.headers.get('Retry-After', '60')
                        try:
                            sleep_time = int(retry_after)
                        except ValueError:
                            sleep_time = 60

                        self.logger.warning(
                            "rate_limited",
                            url=url,
                            retry_after=sleep_time,
                            message="Hit rate limit, backing off..."
                        )
                        time.sleep(sleep_time)
                        continue
                    else:
                        raise CollectorError(f"Rate limited after {self.max_retries} attempts: {url}")

                # Special handling for 503 (Service Unavailable)
                if status_code == 503:
                    if attempt < self.max_retries - 1:
                        sleep_time = 30 * (attempt + 1)  # 30s, 60s, 90s
                        self.logger.warning(
                            "service_unavailable",
                            url=url,
                            retry_in=sleep_time,
                            message="Service temporarily unavailable"
                        )
                        time.sleep(sleep_time)
                        continue
                    else:
                        raise CollectorError(f"Service unavailable after {self.max_retries} attempts: {url}")

                # Don't retry other client errors (4xx)
                if 400 <= status_code < 500:
                    raise CollectorError(f"HTTP {status_code}: {url}")

                # Retry server errors (5xx)
                if attempt < self.max_retries - 1:
                    sleep_time = 2 ** attempt  # Exponential backoff
                    time.sleep(sleep_time)
                else:
                    raise CollectorError(f"Failed after {self.max_retries} attempts: {url}")

            except (httpx.RequestError, httpx.TimeoutException) as e:
                self.logger.warning("request_error", url=url, error=str(e), attempt=attempt + 1)

                if attempt < self.max_retries - 1:
                    sleep_time = 2 ** attempt
                    time.sleep(sleep_time)
                else:
                    raise CollectorError(f"Request failed after {self.max_retries} attempts: {url}")

        raise CollectorError(f"Unexpected error in _make_request for {url}")

    def _save_file(self, content: bytes, filename: str) -> Path:
        """Save content to file.

        Args:
            content: File content
            filename: Filename

        Returns:
            Path to saved file
        """
        # Sanitize filename
        safe_filename = sanitize_filename(filename)
        file_path = self.output_dir / safe_filename

        # Save file
        with open(file_path, 'wb') as f:
            f.write(content)

        self.logger.info("file_saved", file_path=str(file_path), size=len(content))
        return file_path

    def _check_duplicate(self, url: str, checksum: Optional[str] = None) -> bool:
        """Check if document already exists.

        Args:
            url: Document URL
            checksum: Document checksum

        Returns:
            True if duplicate exists, False otherwise
        """
        # Check by URL
        existing = self.store.get_document_by_url(url)
        if existing:
            self.logger.debug("duplicate_found_by_url", url=url, document_id=existing["id"])
            return True

        # Check by checksum
        if checksum:
            existing = self.store.get_document_by_checksum(checksum)
            if existing:
                self.logger.debug(
                    "duplicate_found_by_checksum",
                    checksum=checksum,
                    document_id=existing["id"]
                )
                return True

        return False

    def _create_document(
        self,
        doc_type: DocumentType,
        url: str,
        file_path: Path,
        metadata: Optional[Dict] = None
    ) -> Document:
        """Create document record.

        Args:
            doc_type: Document type
            url: Source URL
            file_path: Local file path
            metadata: Additional metadata

        Returns:
            Document object
        """
        doc = Document(
            doc_type=doc_type,
            source=self.source_name,
            url=url,
            file_path=str(file_path),
            collected_at=datetime.utcnow(),
            metadata=metadata or {}
        )

        # Compute checksum
        doc.compute_checksum()

        # Save to database
        self.store.create_document(doc.to_dict())

        self.logger.info("document_created", document_id=doc.id, url=url)
        return doc

    @abstractmethod
    def collect(self, limit: Optional[int] = None) -> List[Document]:
        """Collect documents from source.

        Args:
            limit: Maximum number of documents to collect

        Returns:
            List of collected Document objects

        Raises:
            CollectorError: If collection fails
        """
        pass

    def close(self) -> None:
        """Close HTTP client and cleanup resources."""
        self.client.close()
        self.logger.info("collector_closed", source=self.source_name)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
