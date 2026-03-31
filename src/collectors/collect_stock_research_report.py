#!/usr/bin/env python
"""Collect research reports for target tickers.

This script collects research reports for each target company ticker
from the configured sources.

Usage:
    python scripts/collect_by_ticker.py                    # Collect for all tickers
    python scripts/collect_by_ticker.py --ticker 005930    # Collect for specific ticker
    python scripts/collect_by_ticker.py --limit 10         # Limit per ticker
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.collectors.pdf_collector import PDFCollector
from src.utils.config import get_config
from src.utils.logger import setup_logging, get_logger


def get_target_tickers():
    """Get target tickers from config.

    Returns:
        List of ticker dictionaries
    """
    config = get_config()
    return config._sources.get("company_tickers", [])


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Collect reports by ticker")
    parser.add_argument(
        "--ticker",
        type=str,
        help="Specific ticker to collect (default: all configured tickers)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of reports to collect per ticker (default: 10)"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum pages to scrape per ticker (default: 5)"
    )
    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level="INFO", log_to_file=True, log_format="text")
    logger = get_logger(__name__)

    try:
        logger.info("=== Starting Ticker-based Report Collection ===")

        # Get target tickers
        all_tickers = get_target_tickers()

        if args.ticker:
            # Filter to specific ticker
            tickers = [t for t in all_tickers if t["ticker"] == args.ticker]
            if not tickers:
                logger.error(f"Ticker {args.ticker} not found in config")
                sys.exit(1)
        else:
            tickers = all_tickers

        logger.info(f"Target tickers: {len(tickers)}")
        for ticker_info in tickers:
            logger.info(f"  - {ticker_info['name']} ({ticker_info['ticker']})")

        # Initialize collector
        logger.info("\nInitializing PDF collector...")
        collector = PDFCollector(source_name="naver_finance_research")

        # Collect for each ticker
        total_collected = 0
        results = {}

        for ticker_info in tickers:
            ticker = ticker_info["ticker"]
            name = ticker_info["name"]

            logger.info(f"\n{'='*60}")
            logger.info(f"Collecting reports for: {name} ({ticker})")
            logger.info(f"{'='*60}")

            try:
                # Note: PDFCollector.collect_by_ticker is currently not fully implemented
                # It collects all reports regardless of ticker
                # We'll collect general reports and filter later during parsing
                documents = collector.collect(limit=args.limit, max_pages=args.max_pages)

                logger.info(f"Collected {len(documents)} documents for {name}")

                results[ticker] = {
                    "name": name,
                    "count": len(documents),
                    "documents": documents
                }
                total_collected += len(documents)

                # Log details
                for i, doc in enumerate(documents, 1):
                    logger.info(f"  {i}. {doc.metadata.get('title', 'N/A')[:80]}")
                    logger.info(f"     File: {doc.file_path}")

            except Exception as e:
                logger.error(f"Failed to collect for {name} ({ticker}): {e}")
                results[ticker] = {
                    "name": name,
                    "count": 0,
                    "error": str(e)
                }
                continue

        # Summary
        logger.info("\n" + "="*60)
        logger.info("COLLECTION SUMMARY")
        logger.info("="*60)
        logger.info(f"Total documents collected: {total_collected}")

        for ticker, result in results.items():
            if "error" in result:
                logger.info(f"  {result['name']} ({ticker}): ERROR - {result['error']}")
            else:
                logger.info(f"  {result['name']} ({ticker}): {result['count']} reports")

        # Close collector
        collector.close()

        logger.info("\n=== Collection completed successfully! ===")

    except Exception as e:
        logger.error(f"Collection failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
