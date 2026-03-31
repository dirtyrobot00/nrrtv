"""News collector module.

This module implements news article collection from Korean financial news sources,
primarily focusing on Naver Finance news section.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector, CollectorError
from src.models.document import Document, DocumentType
from src.utils.config import get_config


class NewsCollector(BaseCollector):
    """News article collector.

    Collects financial news articles from Naver Finance and other Korean news sources.
    Saves HTML content and metadata for later parsing.
    """

    def __init__(
        self,
        source_name: str = "naver_finance_news",
        output_dir: Optional[Path] = None,
        rate_limit: float = 2.0
    ):
        """Initialize news collector.

        Args:
            source_name: Name of the source (from sources.yaml)
            output_dir: Directory to save HTML files
            rate_limit: Requests per second limit
        """
        # Get source configuration
        config = get_config()
        self.source_config = config.get_source("news_sources", source_name)

        if not self.source_config:
            raise CollectorError(f"News source not found: {source_name}")

        if not self.source_config.get("enabled", False):
            raise CollectorError(f"News source is disabled: {source_name}")

        # Set output directory
        if output_dir is None:
            config_output = config.get("collectors.news.output_dir", "data/raw/html")
            output_dir = Path(config_output)

        # Initialize base collector
        super().__init__(
            source_name=source_name,
            output_dir=output_dir,
            rate_limit=rate_limit or self.source_config.get("rate_limit", 2.0)
        )

        self.base_url = self.source_config.get("base_url")
        self.selectors = self.source_config.get("selectors", {})
        self.params = self.source_config.get("params", {})

        self.logger.info(
            "news_collector_initialized",
            source=source_name,
            base_url=self.base_url
        )

    def collect(
        self,
        limit: Optional[int] = None,
        max_pages: int = 5,
        ticker: Optional[str] = None
    ) -> List[Document]:
        """Collect news articles.

        Args:
            limit: Maximum number of articles to collect
            max_pages: Maximum pages to scrape (for pagination)
            ticker: Optional ticker to filter news by company

        Returns:
            List of collected Document objects
        """
        self.logger.info(
            "collection_started",
            source=self.source_name,
            limit=limit,
            max_pages=max_pages,
            ticker=ticker
        )

        collected_docs = []
        page = 1

        try:
            while page <= max_pages:
                # Build URL for current page
                page_url = self._build_page_url(page, ticker)
                self.logger.info("scraping_page", page=page, url=page_url)

                # Fetch page content
                response = self._make_request(page_url)
                html_content = response.text

                # Parse page and extract article links
                articles = self._parse_article_list(html_content, page_url)
                self.logger.info("articles_found", page=page, count=len(articles))

                if not articles:
                    self.logger.info("no_articles_found_stopping", page=page)
                    break

                # Collect each article
                for article_info in articles:
                    # Check limit
                    if limit and len(collected_docs) >= limit:
                        self.logger.info("limit_reached", collected=len(collected_docs))
                        return collected_docs

                    # Check for duplicates
                    if self._check_duplicate(article_info["url"]):
                        self.logger.debug("skipping_duplicate", url=article_info["url"])
                        continue

                    # Download article
                    try:
                        doc = self._download_article(article_info)
                        collected_docs.append(doc)
                        self.logger.info(
                            "article_collected",
                            document_id=doc.id,
                            title=article_info.get("title", "N/A")[:50]
                        )
                    except Exception as e:
                        self.logger.error(
                            "article_download_failed",
                            url=article_info["url"],
                            error=str(e)
                        )
                        continue

                # Move to next page
                page += 1

                # Check if pagination is enabled
                pagination = self.source_config.get("pagination", {})
                if not pagination.get("enabled", False):
                    break

            self.logger.info("collection_completed", total_collected=len(collected_docs))
            return collected_docs

        except Exception as e:
            self.logger.error("collection_failed", error=str(e), exc_info=True)
            raise CollectorError(f"Collection failed: {e}")

    def _build_page_url(self, page: int, ticker: Optional[str] = None) -> str:
        """Build URL for a specific page.

        Args:
            page: Page number
            ticker: Optional ticker for company-specific news

        Returns:
            URL string
        """
        url = self.base_url
        pagination = self.source_config.get("pagination", {})

        # Add base params
        params = []
        for key, value in self.params.items():
            params.append(f"{key}={value}")

        # Add ticker if provided (for company-specific news)
        if ticker:
            params.append(f"code={ticker}")

        # Add page parameter
        if pagination.get("enabled", False) and page > 1:
            page_param = pagination.get("page_param", "page")
            params.append(f"{page_param}={page}")

        if params:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{'&'.join(params)}"

        return url

    def _parse_article_list(self, html_content: str, page_url: str) -> List[Dict]:
        """Parse HTML to extract article information.

        Args:
            html_content: HTML content
            page_url: URL of the page (for building absolute URLs)

        Returns:
            List of article info dictionaries
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        articles = []

        # Find article list
        list_selector = self.selectors.get("article_list")
        if not list_selector:
            raise CollectorError("Missing selector: article_list")

        article_elements = soup.select(list_selector)
        self.logger.debug("article_elements_found", count=len(article_elements))

        for element in article_elements:
            try:
                article_info = self._extract_article_info(element, page_url)
                if article_info and article_info.get("url"):
                    articles.append(article_info)
            except Exception as e:
                self.logger.warning("failed_to_parse_article_element", error=str(e))
                continue

        return articles

    def _extract_article_info(self, element, base_url: str) -> Optional[Dict]:
        """Extract article information from HTML element.

        Args:
            element: BeautifulSoup element
            base_url: Base URL for building absolute URLs

        Returns:
            Article info dictionary or None
        """
        # Extract title
        title_selector = self.selectors.get("title")
        title_elem = element.select_one(title_selector) if title_selector else None
        title = title_elem.get_text(strip=True) if title_elem else None

        if not title:
            return None

        # Extract link
        link_selector = self.selectors.get("link")
        link_elem = element.select_one(link_selector) if link_selector else None

        if not link_elem:
            return None

        # Get article URL
        article_url = link_elem.get("href")
        if not article_url:
            return None

        # Build absolute URL
        article_url = urljoin(base_url, article_url)

        # Extract date
        date_selector = self.selectors.get("date")
        date_elem = element.select_one(date_selector) if date_selector else None
        date_str = date_elem.get_text(strip=True) if date_elem else None

        # Extract summary (if available)
        summary_selector = self.selectors.get("summary")
        summary_elem = element.select_one(summary_selector) if summary_selector else None
        summary = summary_elem.get_text(strip=True) if summary_elem else None

        return {
            "url": article_url,
            "title": title,
            "date": date_str,
            "summary": summary
        }

    def _download_article(self, article_info: Dict) -> Document:
        """Download article HTML and create document record.

        Args:
            article_info: Article information dictionary

        Returns:
            Document object
        """
        article_url = article_info["url"]

        # Download article page
        response = self._make_request(article_url)
        html_content = response.text

        # Extract article content from page
        content, author, published_at = self._extract_article_content(html_content)

        # Generate filename
        filename = self._generate_filename(article_info)

        # Save HTML file
        file_path = self._save_file(html_content.encode('utf-8'), filename)

        # Create document record
        metadata = {
            "title": article_info.get("title"),
            "date": article_info.get("date"),
            "summary": article_info.get("summary"),
            "author": author,
            "published_at": published_at,
            "content_preview": content[:500] if content else None,
            "source_url": article_url
        }

        doc = self._create_document(
            doc_type=DocumentType.NEWS_ARTICLE,
            url=article_url,
            file_path=file_path,
            metadata=metadata
        )

        return doc

    def _extract_article_content(self, html_content: str) -> tuple:
        """Extract article content from HTML.

        Args:
            html_content: HTML content of article page

        Returns:
            Tuple of (content, author, published_at)
        """
        soup = BeautifulSoup(html_content, 'html.parser')

        # Try to extract article content
        # This is simplified - different news sites have different structures
        content = None
        author = None
        published_at = None

        # Common selectors for article content
        content_selectors = [
            "#newsct_article",  # Naver news
            "#articleBodyContents",  # Naver news (old)
            ".article_body",
            ".article_content",
            "#article-view-content-div"
        ]

        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Remove script and style tags
                for tag in content_elem.find_all(['script', 'style']):
                    tag.decompose()
                content = content_elem.get_text(strip=True)
                break

        # Try to extract author
        author_selectors = [
            ".byline",
            ".reporter",
            ".journalist_name",
            ".article_info .author"
        ]

        for selector in author_selectors:
            author_elem = soup.select_one(selector)
            if author_elem:
                author = author_elem.get_text(strip=True)
                break

        # Try to extract published date
        date_selectors = [
            ".media_end_head_info_datestamp_time",
            ".article_info .date",
            "span[class*='date']",
            "time"
        ]

        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                published_at = date_elem.get_text(strip=True)
                break

        return content, author, published_at

    def _generate_filename(self, article_info: Dict) -> str:
        """Generate filename for HTML file.

        Args:
            article_info: Article information

        Returns:
            Filename string
        """
        # Extract components with safe defaults
        title = article_info.get("title") or "article"
        date_str = article_info.get("date") or ""

        # Ensure strings
        title = str(title) if title else "article"
        date_str = str(date_str) if date_str else ""

        # Clean title
        title = re.sub(r'[^\w가-힣]+', '_', title)[:50]

        # Parse date
        date_part = ""
        if date_str:
            # Try to extract date (format: YYYY.MM.DD or YYYY-MM-DD)
            match = re.search(r'(\d{4})[\.\-](\d{2})[\.\-](\d{2})', date_str)
            if match:
                date_part = f"{match.group(1)}{match.group(2)}{match.group(3)}_"

        # Build filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{date_part}{title}_{timestamp}.html"

        return filename

    def collect_by_ticker(self, ticker: str, limit: Optional[int] = None, max_pages: int = 5) -> List[Document]:
        """Collect news articles for a specific ticker.

        Args:
            ticker: Stock ticker code
            limit: Maximum number of articles
            max_pages: Maximum pages to scrape

        Returns:
            List of collected Document objects
        """
        self.logger.info("collecting_by_ticker", ticker=ticker, limit=limit)
        return self.collect(limit=limit, max_pages=max_pages, ticker=ticker)

    def collect_latest(self, limit: int = 10) -> List[Document]:
        """Collect latest news articles.

        Args:
            limit: Number of latest articles to collect

        Returns:
            List of collected Document objects
        """
        self.logger.info("collecting_latest", limit=limit)
        return self.collect(limit=limit, max_pages=1)
