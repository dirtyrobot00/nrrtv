"""News article model.

This module defines the NewsArticle model for storing parsed
news article data with sentiment and entity information.
"""

import uuid
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from src.utils.validators import validate_sentiment_score


class SentimentLabel(str, Enum):
    """Enum for sentiment labels."""
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"


class EntityType(str, Enum):
    """Enum for entity types."""
    COMPANY = "company"
    PERSON = "person"
    PRODUCT = "product"
    LOCATION = "location"
    DATE = "date"
    MONEY = "money"
    PERCENT = "percent"


class Entity(BaseModel):
    """Named entity extracted from text.

    Attributes:
        text: The entity text as it appears in the article
        entity_type: Type of entity (company, person, etc.)
        ticker: Stock ticker if entity is a company
        confidence: Confidence score of entity extraction (0.0-1.0)
    """
    text: str
    entity_type: EntityType
    ticker: Optional[str] = None
    confidence: float = 1.0

    class Config:
        """Pydantic configuration."""
        use_enum_values = True

    @field_validator('confidence')
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        """Validate confidence score."""
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {v}")
        return v


class NewsArticle(BaseModel):
    """News article data model.

    Represents a news article with extracted entities and sentiment analysis.

    Attributes:
        id: Unique article identifier
        document_id: Reference to the source document
        title: Article title
        content: Article content/body
        author: Article author
        published_at: Publication timestamp
        sentiment_score: Sentiment score (-1.0 to 1.0)
        sentiment_label: Sentiment label (positive, neutral, negative)
        entities: List of extracted entities
        extracted_at: Timestamp when data was extracted
        metadata: Additional metadata
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    document_id: str
    title: str
    content: Optional[str] = None
    author: Optional[str] = None
    published_at: Optional[datetime] = None
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[SentimentLabel] = None
    entities: List[Entity] = Field(default_factory=list)
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict = Field(default_factory=dict)

    class Config:
        """Pydantic configuration."""
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }

    @field_validator('sentiment_score')
    @classmethod
    def validate_sentiment_score_value(cls, v: Optional[float]) -> Optional[float]:
        """Validate sentiment score is between -1 and 1."""
        if v is not None and not validate_sentiment_score(v):
            raise ValueError(f"Invalid sentiment score: {v}. Must be between -1.0 and 1.0.")
        return v

    @field_validator('title')
    @classmethod
    def validate_title_not_empty(cls, v: str) -> str:
        """Validate title is not empty."""
        if not v or not v.strip():
            raise ValueError("Title cannot be empty")
        return v.strip()

    def has_sentiment(self) -> bool:
        """Check if article has sentiment analysis.

        Returns:
            True if sentiment score is set
        """
        return self.sentiment_score is not None

    def has_entities(self) -> bool:
        """Check if article has extracted entities.

        Returns:
            True if at least one entity exists
        """
        return len(self.entities) > 0

    def get_entities_by_type(self, entity_type: EntityType) -> List[Entity]:
        """Get all entities of a specific type.

        Args:
            entity_type: Type of entity to filter

        Returns:
            List of entities of the specified type
        """
        return [e for e in self.entities if e.entity_type == entity_type]

    def get_company_entities(self) -> List[Entity]:
        """Get all company entities.

        Returns:
            List of company entities
        """
        return self.get_entities_by_type(EntityType.COMPANY)

    def get_tickers(self) -> List[str]:
        """Get all stock tickers mentioned in the article.

        Returns:
            List of unique ticker symbols
        """
        tickers = [e.ticker for e in self.entities if e.ticker]
        return list(set(tickers))  # Remove duplicates

    def set_sentiment_from_score(self) -> None:
        """Set sentiment label based on sentiment score.

        Converts numeric score to categorical label:
        - score > 0.1: positive
        - score < -0.1: negative
        - otherwise: neutral
        """
        if self.sentiment_score is None:
            return

        if self.sentiment_score > 0.1:
            self.sentiment_label = SentimentLabel.POSITIVE
        elif self.sentiment_score < -0.1:
            self.sentiment_label = SentimentLabel.NEGATIVE
        else:
            self.sentiment_label = SentimentLabel.NEUTRAL

    def add_entity(
        self,
        text: str,
        entity_type: EntityType,
        ticker: Optional[str] = None,
        confidence: float = 1.0
    ) -> None:
        """Add an entity to the article.

        Args:
            text: Entity text
            entity_type: Type of entity
            ticker: Stock ticker if entity is a company
            confidence: Confidence score of extraction
        """
        entity = Entity(
            text=text,
            entity_type=entity_type,
            ticker=ticker,
            confidence=confidence
        )
        self.entities.append(entity)

    def summary(self) -> str:
        """Generate a human-readable summary of the article.

        Returns:
            Summary string
        """
        parts = [f"Article: {self.title[:50]}..."]

        if self.published_at:
            parts.append(f"Published: {self.published_at.strftime('%Y-%m-%d %H:%M')}")

        if self.author:
            parts.append(f"Author: {self.author}")

        if self.has_sentiment():
            parts.append(f"Sentiment: {self.sentiment_label} ({self.sentiment_score:.2f})")

        if self.has_entities():
            tickers = self.get_tickers()
            if tickers:
                parts.append(f"Mentioned: {', '.join(tickers)}")
            parts.append(f"Entities: {len(self.entities)}")

        return " | ".join(parts)
