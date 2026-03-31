#!/usr/bin/env python
"""Find missing dates in collected reports.

This script identifies date gaps in the collection and helps fill them.

Usage:
    python scripts/find_gaps.py
    python scripts/find_gaps.py --fill
    python scripts/find_gaps.py --from-date 2024-01-01
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.storage.document_store import DocumentStore
from src.utils.logger import setup_logging, get_logger


def parse_report_date(date_str):
    """Parse date string from report metadata."""
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


def find_date_gaps(store, from_date=None, to_date=None):
    """Find missing dates in the collection.

    Args:
        store: DocumentStore instance
        from_date: Start date to check (datetime)
        to_date: End date to check (datetime)

    Returns:
        List of missing dates
    """
    logger = get_logger(__name__)

    # Get all documents
    logger.info("Loading documents from database...")
    docs = store.list_documents(limit=100000)

    # Extract dates
    dates_with_data = set()
    for doc in docs:
        date_str = doc.get('metadata', {}).get('date')
        parsed = parse_report_date(date_str)
        if parsed:
            dates_with_data.add(parsed.date())

    if not dates_with_data:
        logger.warning("No documents with dates found!")
        return []

    # Determine date range
    oldest = min(dates_with_data)
    newest = max(dates_with_data)

    if from_date:
        oldest = max(oldest, from_date.date())
    if to_date:
        newest = min(newest, to_date.date())

    logger.info(f"Checking date range: {oldest} to {newest}")

    # Find gaps
    missing_dates = []
    current_date = oldest

    while current_date <= newest:
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() < 5:  # Monday=0 to Friday=4
            if current_date not in dates_with_data:
                missing_dates.append(current_date)

        current_date += timedelta(days=1)

    return missing_dates


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Find missing dates in collection")
    parser.add_argument(
        "--from-date",
        type=str,
        help="Start date to check (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--to-date",
        type=str,
        help="End date to check (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--fill",
        action="store_true",
        help="Automatically fill gaps using daily_collect.py"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Limit number of gaps to show (default: 10)"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level="INFO", log_to_file=False, log_format="text")
    logger = get_logger(__name__)

    try:
        # Parse dates
        from_date = None
        to_date = None

        if args.from_date:
            from_date = datetime.strptime(args.from_date, '%Y-%m-%d')

        if args.to_date:
            to_date = datetime.strptime(args.to_date, '%Y-%m-%d')

        # Initialize store
        store = DocumentStore()

        # Find gaps
        logger.info("\n" + "="*70)
        logger.info("FINDING DATE GAPS")
        logger.info("="*70 + "\n")

        missing_dates = find_date_gaps(store, from_date, to_date)

        if not missing_dates:
            logger.info("✅ No gaps found! All dates have data.")
            return

        # Show gaps
        logger.info(f"Found {len(missing_dates)} missing dates (weekdays only)\n")

        logger.info("="*70)
        logger.info(f"MISSING DATES (showing first {args.limit}):")
        logger.info("="*70)

        for i, date in enumerate(missing_dates[:args.limit], 1):
            weekday = date.strftime('%A')
            logger.info(f"{i:3d}. {date} ({weekday})")

        if len(missing_dates) > args.limit:
            logger.info(f"\n... and {len(missing_dates) - args.limit} more missing dates")

        logger.info("\n" + "="*70)

        # Group by month
        logger.info("\nMissing dates by month:")
        logger.info("="*70)

        month_counts = {}
        for date in missing_dates:
            month_key = date.strftime('%Y-%m')
            month_counts[month_key] = month_counts.get(month_key, 0) + 1

        for month in sorted(month_counts.keys(), reverse=True):
            count = month_counts[month]
            bar = '█' * (count // 2)
            logger.info(f"{month}: {count:3d} days {bar}")

        logger.info("="*70 + "\n")

        # Fill option
        if args.fill:
            logger.info("Starting automatic gap filling...")
            logger.info("This will collect reports for all missing dates.\n")

            response = input("Continue? (yes/no): ")
            if response.lower() != "yes":
                logger.info("Aborted.")
                return

            # Import daily_collect
            sys.path.insert(0, str(project_root / "scripts"))
            from daily_collect import collect_daily

            # Collect for each gap
            for i, date in enumerate(missing_dates, 1):
                logger.info(f"\n[{i}/{len(missing_dates)}] Collecting for {date}...")

                try:
                    collect_daily(
                        from_date=datetime.combine(date, datetime.min.time()),
                        to_date=datetime.combine(date, datetime.min.time()),
                        limit=100
                    )
                except Exception as e:
                    logger.error(f"Failed to collect {date}: {e}")
                    continue

            logger.info("\n✅ Gap filling complete!")

        else:
            logger.info("To fill these gaps, run:")
            logger.info("  python scripts/find_gaps.py --fill")
            logger.info("\nOr manually:")

            # Show first few gaps as examples
            for date in missing_dates[:3]:
                logger.info(f"  python scripts/daily_collect.py --from-date {date} --to-date {date}")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
