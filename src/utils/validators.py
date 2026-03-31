"""Data validation utilities.

This module provides validation functions for various data types
used throughout the application.
"""

import re
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urlparse


class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


def validate_url(url: str) -> bool:
    """Validate URL format.

    Args:
        url: URL string to validate

    Returns:
        True if valid, False otherwise

    Examples:
        >>> validate_url("https://example.com/path")
        True
        >>> validate_url("not-a-url")
        False
    """
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def validate_ticker(ticker: str) -> bool:
    """Validate Korean stock ticker format.

    Korean tickers are 6-digit numbers.

    Args:
        ticker: Ticker symbol to validate

    Returns:
        True if valid, False otherwise

    Examples:
        >>> validate_ticker("005930")  # Samsung Electronics
        True
        >>> validate_ticker("12345")
        False
    """
    if not ticker:
        return False
    return bool(re.match(r'^\d{6}$', ticker))


def validate_date(date_str: str, date_format: str = "%Y-%m-%d") -> bool:
    """Validate date string format.

    Args:
        date_str: Date string to validate
        date_format: Expected date format

    Returns:
        True if valid, False otherwise

    Examples:
        >>> validate_date("2025-01-15")
        True
        >>> validate_date("15-01-2025")
        False
    """
    try:
        datetime.strptime(date_str, date_format)
        return True
    except (ValueError, TypeError):
        return False


def validate_confidence_score(score: float) -> bool:
    """Validate confidence score is between 0 and 1.

    Args:
        score: Confidence score to validate

    Returns:
        True if valid, False otherwise

    Examples:
        >>> validate_confidence_score(0.85)
        True
        >>> validate_confidence_score(1.5)
        False
    """
    try:
        return 0.0 <= float(score) <= 1.0
    except (ValueError, TypeError):
        return False


def validate_sentiment_score(score: float) -> bool:
    """Validate sentiment score is between -1 and 1.

    Args:
        score: Sentiment score to validate

    Returns:
        True if valid, False otherwise

    Examples:
        >>> validate_sentiment_score(0.5)
        True
        >>> validate_sentiment_score(-2.0)
        False
    """
    try:
        return -1.0 <= float(score) <= 1.0
    except (ValueError, TypeError):
        return False


def validate_target_price(price: float) -> bool:
    """Validate target price is positive.

    Args:
        price: Target price to validate

    Returns:
        True if valid, False otherwise

    Examples:
        >>> validate_target_price(80000)
        True
        >>> validate_target_price(-100)
        False
    """
    try:
        return float(price) > 0
    except (ValueError, TypeError):
        return False


def validate_investment_opinion(opinion: str) -> bool:
    """Validate investment opinion is one of the allowed values.

    Args:
        opinion: Investment opinion to validate

    Returns:
        True if valid, False otherwise

    Examples:
        >>> validate_investment_opinion("BUY")
        True
        >>> validate_investment_opinion("INVALID")
        False
    """
    valid_opinions = {
        "BUY", "HOLD", "SELL",
        "STRONG_BUY", "STRONG_SELL",
        "매수", "보유", "매도",
        "적극매수", "적극매도"
    }
    return opinion.upper() in valid_opinions if opinion else False


def sanitize_filename(filename: str, max_length: int = 255) -> str:
    """Sanitize filename by removing invalid characters.

    Args:
        filename: Filename to sanitize
        max_length: Maximum filename length

    Returns:
        Sanitized filename

    Examples:
        >>> sanitize_filename("report:2025-01-15.pdf")
        'report_2025-01-15.pdf'
    """
    # Remove invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)

    # Limit length
    if len(sanitized) > max_length:
        name, ext = sanitized.rsplit('.', 1) if '.' in sanitized else (sanitized, '')
        if ext:
            max_name_length = max_length - len(ext) - 1
            sanitized = name[:max_name_length] + '.' + ext
        else:
            sanitized = sanitized[:max_length]

    return sanitized


def validate_file_extension(filename: str, allowed_extensions: set) -> bool:
    """Validate file has an allowed extension.

    Args:
        filename: Filename to validate
        allowed_extensions: Set of allowed extensions (e.g., {'.pdf', '.html'})

    Returns:
        True if valid, False otherwise

    Examples:
        >>> validate_file_extension("report.pdf", {'.pdf', '.doc'})
        True
        >>> validate_file_extension("file.txt", {'.pdf'})
        False
    """
    if not filename:
        return False

    ext = filename.lower().rsplit('.', 1)[-1] if '.' in filename else ''
    return f'.{ext}' in {e.lower() for e in allowed_extensions}


def validate_checksum(checksum: str) -> bool:
    """Validate checksum format (SHA-256).

    Args:
        checksum: Checksum string to validate

    Returns:
        True if valid SHA-256 hex string, False otherwise

    Examples:
        >>> validate_checksum("a" * 64)
        True
        >>> validate_checksum("invalid")
        False
    """
    return bool(re.match(r'^[a-f0-9]{64}$', checksum.lower())) if checksum else False


def validate_required_fields(data: dict, required_fields: list) -> None:
    """Validate that all required fields are present in data.

    Args:
        data: Data dictionary to validate
        required_fields: List of required field names

    Raises:
        ValidationError: If any required field is missing

    Examples:
        >>> validate_required_fields({"name": "test", "id": 1}, ["name", "id"])
        >>> validate_required_fields({"name": "test"}, ["name", "id"])
        Traceback (most recent call last):
        ...
        ValidationError: Missing required fields: id
    """
    missing = [field for field in required_fields if field not in data or data[field] is None]
    if missing:
        raise ValidationError(f"Missing required fields: {', '.join(missing)}")


def clean_text(text: Optional[str], max_length: Optional[int] = None) -> str:
    """Clean and normalize text.

    Removes extra whitespace, newlines, and optionally truncates.

    Args:
        text: Text to clean
        max_length: Maximum length (truncates if exceeded)

    Returns:
        Cleaned text

    Examples:
        >>> clean_text("  Hello   World  \\n\\n  ")
        'Hello World'
        >>> clean_text("Long text", max_length=5)
        'Long ...'
    """
    if not text:
        return ""

    # Normalize whitespace
    cleaned = re.sub(r'\s+', ' ', text.strip())

    # Truncate if needed
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length - 3] + '...'

    return cleaned
