"""Financial report model.

This module defines the FinancialReport model for storing quarterly/semi-annual/annual
financial reports from DART.
"""

import uuid
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class ReportType(str, Enum):
    """Financial report type enum."""
    ANNUAL = "annual"              # 사업보고서
    SEMI_ANNUAL = "semi_annual"    # 반기보고서
    QUARTERLY = "quarterly"        # 분기보고서


class ReportPeriod(str, Enum):
    """Report period enum."""
    Q1 = "Q1"  # 1분기
    Q2 = "Q2"  # 반기
    Q3 = "Q3"  # 3분기
    FY = "FY"  # 연간 (사업보고서)


class FinancialReport(BaseModel):
    """Financial report data model.

    Represents a quarterly/semi-annual/annual financial report from DART.

    Attributes:
        id: Unique report identifier
        document_id: Reference to the source document
        corp_code: DART corporate code
        corp_name: Company name
        stock_code: Stock ticker code (6 digits)
        report_type: Type of report (annual/semi_annual/quarterly)
        report_period: Report period (FY/Q1/Q2/Q3)
        rcept_no: DART receipt number (unique identifier)
        rcept_dt: Receipt date (YYYYMMDD)
        report_nm: Report name
        fiscal_year: Fiscal year
        fiscal_period: Fiscal period string
        original_url: Original DART URL
        pdf_url: PDF download URL
        financial_summary: Financial data summary
        related_research_reports: Related research report IDs
        collected_at: Collection timestamp
        filed_at: Filing timestamp
        metadata: Additional metadata
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    document_id: str

    # Company info
    corp_code: str
    corp_name: str
    stock_code: Optional[str] = None

    # Report info
    report_type: ReportType
    report_period: ReportPeriod

    # DART info
    rcept_no: str
    rcept_dt: str  # YYYYMMDD format
    report_nm: str

    # Fiscal info
    fiscal_year: int
    fiscal_period: str  # "2024.01.01-2024.12.31"

    # URLs
    original_url: str
    pdf_url: Optional[str] = None

    # Financial data
    financial_summary: Optional[Dict] = None
    """
    Example:
    {
        "revenue": 1000000000000,       # 매출액 (원)
        "operating_income": 50000000000, # 영업이익
        "net_income": 40000000000,      # 당기순이익
        "eps": 5000,                    # 주당순이익 (원)
        "currency": "KRW"
    }
    """

    # Related reports
    related_research_reports: List[str] = Field(default_factory=list)

    # Timestamps
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    filed_at: datetime

    # Additional metadata
    metadata: Dict = Field(default_factory=dict)

    class Config:
        """Pydantic configuration."""
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }

    @field_validator('rcept_dt')
    @classmethod
    def validate_rcept_dt_format(cls, v: str) -> str:
        """Validate receipt date format is YYYYMMDD."""
        if not v or len(v) != 8 or not v.isdigit():
            raise ValueError(f"rcept_dt must be YYYYMMDD format, got: {v}")
        return v

    @field_validator('fiscal_year')
    @classmethod
    def validate_fiscal_year(cls, v: int) -> int:
        """Validate fiscal year is reasonable."""
        if v < 2000 or v > 2100:
            raise ValueError(f"Fiscal year must be between 2000-2100, got: {v}")
        return v

    @field_validator('stock_code')
    @classmethod
    def validate_stock_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate stock code is 6 digits."""
        if v is not None and (len(v) != 6 or not v.isdigit()):
            raise ValueError(f"Stock code must be 6 digits, got: {v}")
        return v

    def get_quarter_str(self) -> str:
        """Get quarter string representation.

        Returns:
            String like "2024Q3" or "2024FY"
        """
        return f"{self.fiscal_year}{self.report_period}"

    def is_annual(self) -> bool:
        """Check if this is an annual report."""
        return self.report_type == ReportType.ANNUAL

    def is_quarterly(self) -> bool:
        """Check if this is a quarterly report."""
        return self.report_type == ReportType.QUARTERLY

    def is_semi_annual(self) -> bool:
        """Check if this is a semi-annual report."""
        return self.report_type == ReportType.SEMI_ANNUAL

    def has_financial_data(self) -> bool:
        """Check if financial summary exists."""
        return self.financial_summary is not None and len(self.financial_summary) > 0

    def to_dict(self) -> Dict:
        """Convert to dictionary.

        Note: Returns dict with Python objects (datetime, etc.) for database storage.
        For JSON serialization, use model_dump(mode='json') instead.
        """
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict) -> 'FinancialReport':
        """Create from dictionary."""
        return cls(**data)

    def summary(self) -> str:
        """Generate human-readable summary.

        Returns:
            Summary string
        """
        parts = [
            f"{self.corp_name} ({self.stock_code or 'N/A'})",
            f"{self.report_type}",
            f"{self.get_quarter_str()}"
        ]

        if self.has_financial_data():
            fs = self.financial_summary
            if 'revenue' in fs:
                revenue_b = fs['revenue'] / 1_000_000_000_000  # 조 단위
                parts.append(f"매출: {revenue_b:.1f}조")
            if 'net_income' in fs:
                income_b = fs['net_income'] / 1_000_000_000_000
                parts.append(f"순이익: {income_b:.1f}조")

        return " | ".join(parts)
