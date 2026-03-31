"""Base document model.

This module defines the base Document class that all document types inherit from.
"""

import hashlib
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


class DocumentType(str, Enum):
    """Enum for document types."""
    RESEARCH_REPORT = "research_report"
    NEWS_ARTICLE = "news_article"
    DISCLOSURE = "disclosure"
    FINANCIAL_STATEMENT = "financial_statement"


class DocumentStatus(str, Enum):
    """Enum for document processing status."""
    PENDING = "pending"
    PROCESSING = "processing"
    PROCESSED = "processed"
    ERROR = "error"
    SKIPPED = "skipped"


class Document(BaseModel):
    """Base document model.

    Represents a document in the system (PDF, HTML, etc.).
    All specific document types inherit from this base class.

    Attributes:
        id: Unique document identifier (UUID)
        doc_type: Type of document (research_report, news_article, etc.)
        source: Source of the document (e.g., "naver_finance_research")
        url: Original URL where document was found
        file_path: Local file path where document is stored
        collected_at: Timestamp when document was collected
        processed_at: Timestamp when document was processed
        status: Processing status
        error_message: Error message if processing failed
        checksum: SHA-256 checksum of document content
        metadata: Additional metadata specific to the document type
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    doc_type: DocumentType
    source: str
    url: Optional[str] = None
    file_path: Optional[str] = None
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None
    status: DocumentStatus = DocumentStatus.PENDING
    error_message: Optional[str] = None
    checksum: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        """Pydantic configuration."""
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }

    @field_validator('url')
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate URL format."""
        if v is not None and not v.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid URL format: {v}")
        return v

    @field_validator('file_path')
    @classmethod
    def validate_file_path(cls, v: Optional[str]) -> Optional[str]:
        """Validate file path exists if provided."""
        if v is not None:
            path = Path(v)
            # Don't validate existence during model creation, just ensure it's a valid path
            # Actual file may not exist yet during collection
            return str(path)
        return v

    def compute_checksum(self) -> str:
        """Compute SHA-256 checksum of document content.

        Returns:
            Hex-encoded SHA-256 checksum

        Raises:
            FileNotFoundError: If file_path does not exist
        """
        if not self.file_path:
            raise ValueError("Cannot compute checksum: file_path is None")

        file_path = Path(self.file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read file in chunks to handle large files
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        checksum = sha256_hash.hexdigest()
        self.checksum = checksum
        return checksum

    def mark_processing(self) -> None:
        """Mark document as being processed."""
        self.status = DocumentStatus.PROCESSING

    def mark_processed(self) -> None:
        """Mark document as successfully processed."""
        self.status = DocumentStatus.PROCESSED
        self.processed_at = datetime.utcnow()

    def mark_error(self, error_message: str) -> None:
        """Mark document as failed with error message.

        Args:
            error_message: Error message describing what went wrong
        """
        self.status = DocumentStatus.ERROR
        self.error_message = error_message
        self.processed_at = datetime.utcnow()

    def mark_skipped(self, reason: str) -> None:
        """Mark document as skipped.

        Args:
            reason: Reason why document was skipped
        """
        self.status = DocumentStatus.SKIPPED
        self.error_message = reason
        self.processed_at = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Convert document to dictionary.

        Returns:
            Dictionary representation of document
        """
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Document':
        """Create document from dictionary.

        Args:
            data: Dictionary with document data

        Returns:
            Document instance
        """
        return cls(**data)
