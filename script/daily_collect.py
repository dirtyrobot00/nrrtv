#!/usr/bin/env python
"""Daily collection script for research reports.

This script collects the latest research reports from Naver Finance.
It's designed to run daily and will collect all reports from the last
collected date to today.

Features:
- Automatically detects the last collected date
- Collects all missing reports (gaps in collection)
- Supports manual date range specification
- Prevents duplicates
- Can limit number of reports to collect

Usage:
    # Collect all new reports (default)
    python scripts/daily_collect.py

    # Collect up to 100 reports
    python scripts/daily_collect.py --limit 100

    # Collect from specific date to today
    python scripts/daily_collect.py --from-date 2025-12-20

    # Collect specific date range
    python scripts/daily_collect.py --from-date 2025-12-20 --to-date 2025-12-23

    # Dry run (see what would be collected)
    python scripts/daily_collect.py --dry-run

    # Test mode (collect 2 days worth, 1 report each)
    python scripts/daily_collect.py --test
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.collectors.pdf_collector import PDFCollector
from src.storage.document_store import DocumentStore
from src.utils.logger import setup_logging, get_logger


def parse_report_date(date_str):
    """Parse date string from report metadata.

    Supports formats:
    - YY.MM.DD (e.g., 25.12.23)
    - YYYY.MM.DD
    """
    if not date_str:
        return None

    date_str = str(date_str).strip()

    if '.' in date_str:
        parts = date_str.split('.')
        if len(parts) == 3:
            year, month, day = parts
            if len(year) == 2:
                year = f"20{year}"
            try:
                return datetime(int(year), int(month), int(day))
            except:
                pass

    return None


def get_last_collected_date(store):
    """Get the most recent report date from database.

    Args:
        store: DocumentStore instance

    Returns:
        datetime object of most recent report date, or None
    """
    # Only get research reports, not financial reports
    docs = store.list_documents(doc_type='research_report', limit=1000)

    dates = []
    for doc in docs:
        date_str = doc.get('metadata', {}).get('date')
        parsed = parse_report_date(date_str)
        if parsed:
            dates.append(parsed)

    if dates:
        return max(dates)
    return None


def format_date_for_filter(dt):
    """Format datetime to YY.MM.DD format for filtering."""
    return dt.strftime('%y.%m.%d')


def collect_daily(
    from_date=None,
    to_date=None,
    limit=None,
    dry_run=False,
    test_mode=False
):
    """Collect daily reports.

    Args:
        from_date: Start date (datetime)
        to_date: End date (datetime)
        limit: Maximum number of reports to collect
        dry_run: If True, only show what would be collected
        test_mode: If True, collect only 1 report per day for testing
    """
    logger = get_logger(__name__)

    # Initialize
    store = DocumentStore()

    # Determine date range
    if to_date is None:
        to_date = datetime.now()

    if from_date is None:
        # Auto-detect last collected date
        last_date = get_last_collected_date(store)
        if last_date:
            # Start from the day after last collected
            from_date = last_date
            logger.info(
                "auto_detected_start_date",
                last_collected=last_date.strftime('%Y-%m-%d')
            )
        else:
            # No data yet, collect from today
            from_date = to_date
            logger.warning("no_existing_data", message="Starting from today")

    # Calculate expected days
    days_to_collect = (to_date - from_date).days + 1

    logger.info("="*70)
    logger.info("DAILY COLLECTION PLAN")
    logger.info("="*70)
    logger.info(f"From Date: {from_date.strftime('%Y-%m-%d')}")
    logger.info(f"To Date: {to_date.strftime('%Y-%m-%d')}")
    logger.info(f"Days to collect: {days_to_collect}")
    logger.info(f"Limit: {limit if limit else 'No limit'}")
    logger.info(f"Dry run: {dry_run}")
    logger.info(f"Test mode: {test_mode}")
    logger.info("="*70)

    if dry_run:
        logger.info("DRY RUN MODE - No actual collection will happen")
        return

    # Initialize collector
    logger.info("Initializing PDF collector...")
    collector = PDFCollector(source_name="naver_finance_research")

    # Collection strategy:
    # Naver Finance shows reports in reverse chronological order (newest first)
    # We'll collect from page 1 until we reach reports older than from_date
    # IMPORTANT: We continue even if a page has all duplicates, because we need
    # to reach older dates

    collected_docs = []
    page = 1
    max_pages = 200  # Safety limit
    stop_collection = False
    consecutive_empty_pages = 0  # Track pages with no results at all

    logger.info("Starting collection from page 1...")

    while page <= max_pages and not stop_collection:
        logger.info(f"\nProcessing page {page}...")

        try:
            # Build page URL and scrape
            page_url = f"https://finance.naver.com/research/company_list.naver?page={page}"

            # Make request directly to get ALL reports on page (not filtered by collector)
            response = collector._make_request(page_url)
            html_content = response.text

            # Parse page to get all reports (including duplicates)
            reports_on_page = collector._parse_report_list(html_content, page_url)

            if not reports_on_page:
                consecutive_empty_pages += 1
                logger.info(f"No reports found on page {page}")

                # If we get 3 consecutive empty pages, we've likely reached the end
                if consecutive_empty_pages >= 3:
                    logger.info("Reached 3 consecutive empty pages, stopping")
                    break

                page += 1
                time.sleep(1.5)
                continue

            # Reset empty page counter
            consecutive_empty_pages = 0

            logger.info(f"Found {len(reports_on_page)} reports on page {page}")

            page_collected = 0
            page_duplicates = 0

            # Check each report on this page
            for report_info in reports_on_page:
                date_str = report_info.get('date')
                doc_date = parse_report_date(date_str)

                if not doc_date:
                    logger.warning(f"  ⚠️  Could not parse date: {date_str}")
                    continue

                # Check if we've gone past our target date range
                if doc_date < from_date:
                    logger.info(
                        f"  ⚠️  Reached date {date_str} ({doc_date.strftime('%Y-%m-%d')}) "
                        f"which is before target start date {from_date.strftime('%Y-%m-%d')}, stopping"
                    )
                    stop_collection = True
                    break

                # Only collect if within date range
                if doc_date > to_date:
                    logger.debug(f"  Skipping {date_str} (after to_date)")
                    continue

                # Check for duplicates
                if collector._check_duplicate(report_info["url"]):
                    logger.debug(f"  Duplicate: {date_str} - {report_info.get('company_name')}")
                    page_duplicates += 1
                    continue

                # Download PDF
                try:
                    doc = collector._download_pdf(report_info)
                    collected_docs.append(doc)
                    page_collected += 1

                    logger.info(
                        f"  ✅ {date_str}: {doc.metadata.get('company_name')} - "
                        f"{doc.metadata.get('title', 'N/A')[:40]}"
                    )

                    # In test mode, stop after collecting 2 different days
                    if test_mode:
                        unique_dates = set(
                            parse_report_date(d.metadata.get('date')).strftime('%Y-%m-%d')
                            for d in collected_docs
                            if parse_report_date(d.metadata.get('date'))
                        )
                        if len(unique_dates) >= 2:
                            logger.info(f"Test mode: collected 2 different days, stopping")
                            stop_collection = True
                            break

                    # Check limit
                    if limit and len(collected_docs) >= limit:
                        logger.info(f"Reached limit of {limit} documents")
                        stop_collection = True
                        break

                except Exception as e:
                    logger.error(
                        f"  ❌ Failed to download: {report_info.get('url')} - {e}"
                    )
                    continue

            logger.info(f"Page {page} summary: {page_collected} collected, {page_duplicates} duplicates")

            if stop_collection:
                break

            # Move to next page
            page += 1

            # Rate limiting
            time.sleep(1.5)

        except Exception as e:
            logger.error(f"Error on page {page}: {e}", exc_info=True)
            break

    # Summary
    logger.info("\n" + "="*70)
    logger.info("COLLECTION SUMMARY")
    logger.info("="*70)
    logger.info(f"Total documents collected: {len(collected_docs)}")
    logger.info(f"Pages processed: {page}")

    # Group by date
    date_counts = {}
    for doc in collected_docs:
        date_str = doc.metadata.get('date')
        doc_date = parse_report_date(date_str)
        if doc_date:
            date_key = doc_date.strftime('%Y-%m-%d')
            date_counts[date_key] = date_counts.get(date_key, 0) + 1

    logger.info("\nDocuments by date:")
    for date_key in sorted(date_counts.keys(), reverse=True):
        logger.info(f"  {date_key}: {date_counts[date_key]} reports")

    logger.info("="*70)

    # Close collector
    collector.close()

    return collected_docs


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Daily collection script for research reports"
    )

    parser.add_argument(
        "--from-date",
        type=str,
        help="Start date (YYYY-MM-DD). Auto-detects if not specified."
    )
    parser.add_argument(
        "--to-date",
        type=str,
        help="End date (YYYY-MM-DD). Defaults to today."
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of reports to collect"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be collected without actually collecting"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: collect 1 report from 2 different days"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level="INFO", log_to_file=True, log_format="text")
    logger = get_logger(__name__)

    try:
        # Parse dates
        from_date = None
        to_date = None

        if args.from_date:
            from_date = datetime.strptime(args.from_date, '%Y-%m-%d')

        if args.to_date:
            to_date = datetime.strptime(args.to_date, '%Y-%m-%d')

        # Run collection
        collect_daily(
            from_date=from_date,
            to_date=to_date,
            limit=args.limit,
            dry_run=args.dry_run,
            test_mode=args.test
        )

    except Exception as e:
        logger.error(f"Collection failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
