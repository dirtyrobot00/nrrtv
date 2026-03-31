"""Research report model.

This module defines the ResearchReport model for storing parsed
research report data.
"""

import uuid
from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

from src.utils.validators import (
    validate_confidence_score,
    validate_investment_opinion,
    validate_target_price,
    validate_ticker
)


class ResearchReport(BaseModel):
    """Research report data model.

    Represents a parsed securities research report with extracted
    investment opinion, target price, key points, and risks.

    Attributes:
        id: Unique report identifier
        document_id: Reference to the source document
        ticker: Stock ticker symbol (6-digit code for Korean stocks)
        company_name: Company name
        analyst_name: Analyst who wrote the report
        firm: Securities firm that published the report
        report_date: Date of the report
        target_price: Target stock price
        target_price_currency: Currency of target price (KRW, USD, etc.)
        investment_opinion: Investment recommendation (BUY, HOLD, SELL, etc.)
        investment_points: List of key investment points
        risk_factors: List of identified risk factors
        confidence_score: Parsing confidence score (0.0-1.0)
        extracted_at: Timestamp when data was extracted
        metadata: Additional metadata
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    document_id: str
    ticker: str
    company_name: str
    analyst_name: Optional[str] = None
    firm: str
    report_date: date
    target_price: Optional[float] = None
    target_price_currency: str = "KRW"
    investment_opinion: Optional[str] = None
    investment_points: List[str] = Field(default_factory=list)
    risk_factors: List[str] = Field(default_factory=list)
    confidence_score: float = 0.0
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict = Field(default_factory=dict)

    class Config:
        """Pydantic configuration."""
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
            datetime: lambda v: v.isoformat() if v else None,
        }

    @field_validator('ticker')
    @classmethod
    def validate_ticker_format(cls, v: str) -> str:
        """Validate ticker format."""
        if not validate_ticker(v):
            raise ValueError(f"Invalid ticker format: {v}. Expected 6-digit number.")
        return v

    @field_validator('target_price')
    @classmethod
    def validate_target_price_value(cls, v: Optional[float]) -> Optional[float]:
        """Validate target price is positive if provided."""
        if v is not None and not validate_target_price(v):
            raise ValueError(f"Invalid target price: {v}. Must be positive.")
        return v

    @field_validator('investment_opinion')
    @classmethod
    def validate_opinion(cls, v: Optional[str]) -> Optional[str]:
        """Validate investment opinion is in allowed set."""
        if v is not None and not validate_investment_opinion(v):
            raise ValueError(
                f"Invalid investment opinion: {v}. "
                f"Expected one of: BUY, HOLD, SELL, STRONG_BUY, STRONG_SELL, etc."
            )
        return v.upper() if v else None

    @field_validator('confidence_score')
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        """Validate confidence score is between 0 and 1."""
        if not validate_confidence_score(v):
            raise ValueError(f"Invalid confidence score: {v}. Must be between 0.0 and 1.0.")
        return v

    @field_validator('investment_points', 'risk_factors')
    @classmethod
    def validate_lists_not_empty_strings(cls, v: List[str]) -> List[str]:
        """Remove empty strings from lists."""
        return [item.strip() for item in v if item and item.strip()]

    def has_target_price(self) -> bool:
        """Check if report has a target price.

        Returns:
            True if target price is set and valid
        """
        return self.target_price is not None and self.target_price > 0

    def has_investment_opinion(self) -> bool:
        """Check if report has an investment opinion.

        Returns:
            True if investment opinion is set
        """
        return self.investment_opinion is not None and len(self.investment_opinion) > 0

    def has_investment_points(self) -> bool:
        """Check if report has investment points.

        Returns:
            True if at least one investment point exists
        """
        return len(self.investment_points) > 0

    def has_risk_factors(self) -> bool:
        """Check if report has risk factors.

        Returns:
            True if at least one risk factor exists
        """
        return len(self.risk_factors) > 0

    def is_complete(self) -> bool:
        """Check if report has all key fields extracted.

        Returns:
            True if report has opinion, target price, and at least one investment point
        """
        return (
            self.has_investment_opinion() and
            self.has_target_price() and
            self.has_investment_points()
        )

    def summary(self) -> str:
        """Generate a human-readable summary of the report.

        Returns:
            Summary string
        """
        parts = [
            f"Report for {self.company_name} ({self.ticker})",
            f"Date: {self.report_date}",
            f"Firm: {self.firm}",
        ]

        if self.analyst_name:
            parts.append(f"Analyst: {self.analyst_name}")

        if self.has_investment_opinion():
            parts.append(f"Opinion: {self.investment_opinion}")

        if self.has_target_price():
            parts.append(f"Target Price: {self.target_price:,.0f} {self.target_price_currency}")

        parts.append(f"Investment Points: {len(self.investment_points)}")
        parts.append(f"Risk Factors: {len(self.risk_factors)}")
        parts.append(f"Confidence: {self.confidence_score:.2f}")

        return " | ".join(parts)
