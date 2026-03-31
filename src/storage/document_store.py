"""Document storage module.

This module provides database storage for documents using SQLAlchemy ORM.
Supports both SQLite (local) and PostgreSQL (production).
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean, Column, Date, DateTime, Float, ForeignKey, Integer, String, Text, create_engine
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship, sessionmaker

from src.models.document import DocumentStatus, DocumentType
from src.models.news_article import SentimentLabel
from src.utils.config import get_config
from src.utils.logger import LoggerMixin

Base = declarative_base()


class DocumentORM(Base):
    """Document table ORM model."""

    __tablename__ = "documents"

    id = Column(String(36), primary_key=True)
    doc_type = Column(String(50), nullable=False, index=True)
    source = Column(String(100), nullable=False, index=True)
    url = Column(Text, nullable=True, unique=True, index=True)
    file_path = Column(Text, nullable=True)
    collected_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    processed_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default="pending", index=True)
    error_message = Column(Text, nullable=True)
    checksum = Column(String(64), nullable=True, unique=True, index=True)
    metadata_json = Column(Text, nullable=True)  # JSON-serialized metadata

    # Relationships
    research_reports = relationship("ResearchReportORM", back_populates="document", cascade="all, delete-orphan")
    news_articles = relationship("NewsArticleORM", back_populates="document", cascade="all, delete-orphan")
    financial_reports = relationship("FinancialReportORM", back_populates="document", cascade="all, delete-orphan")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "doc_type": self.doc_type,
            "source": self.source,
            "url": self.url,
            "file_path": self.file_path,
            "collected_at": self.collected_at,
            "processed_at": self.processed_at,
            "status": self.status,
            "error_message": self.error_message,
            "checksum": self.checksum,
            "metadata": json.loads(self.metadata_json) if self.metadata_json else {}
        }


class ResearchReportORM(Base):
    """Research report table ORM model."""

    __tablename__ = "research_reports"

    id = Column(String(36), primary_key=True)
    document_id = Column(String(36), ForeignKey("documents.id"), nullable=False, index=True)
    ticker = Column(String(20), nullable=False, index=True)
    company_name = Column(String(200), nullable=False, index=True)
    analyst_name = Column(String(100), nullable=True)
    firm = Column(String(100), nullable=False, index=True)
    report_date = Column(Date, nullable=False, index=True)
    target_price = Column(Float, nullable=True)
    target_price_currency = Column(String(10), default="KRW")
    investment_opinion = Column(String(50), nullable=True)
    investment_points_json = Column(Text, nullable=True)  # JSON list
    risk_factors_json = Column(Text, nullable=True)  # JSON list
    confidence_score = Column(Float, nullable=False, default=0.0)
    extracted_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    metadata_json = Column(Text, nullable=True)

    # Relationship
    document = relationship("DocumentORM", back_populates="research_reports")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "document_id": self.document_id,
            "ticker": self.ticker,
            "company_name": self.company_name,
            "analyst_name": self.analyst_name,
            "firm": self.firm,
            "report_date": self.report_date,
            "target_price": self.target_price,
            "target_price_currency": self.target_price_currency,
            "investment_opinion": self.investment_opinion,
            "investment_points": json.loads(self.investment_points_json) if self.investment_points_json else [],
            "risk_factors": json.loads(self.risk_factors_json) if self.risk_factors_json else [],
            "confidence_score": self.confidence_score,
            "extracted_at": self.extracted_at,
            "metadata": json.loads(self.metadata_json) if self.metadata_json else {}
        }


class FinancialReportORM(Base):
    """Financial report table ORM model."""

    __tablename__ = "financial_reports"

    id = Column(String(36), primary_key=True)
    document_id = Column(String(36), ForeignKey("documents.id"), nullable=False, index=True)

    # Company info
    corp_code = Column(String(20), nullable=False, index=True)
    corp_name = Column(String(200), nullable=False, index=True)
    stock_code = Column(String(6), nullable=True, index=True)

    # Report info
    report_type = Column(String(20), nullable=False, index=True)  # annual, semi_annual, quarterly
    report_period = Column(String(5), nullable=False, index=True)  # FY, Q1, Q2, Q3

    # DART info
    rcept_no = Column(String(20), nullable=False, unique=True, index=True)
    rcept_dt = Column(String(8), nullable=False, index=True)  # YYYYMMDD
    report_nm = Column(String(200), nullable=False)

    # Fiscal info
    fiscal_year = Column(Integer, nullable=False, index=True)
    fiscal_period = Column(String(50), nullable=False)  # "2024.01.01-2024.12.31"

    # URLs
    original_url = Column(Text, nullable=False)
    pdf_url = Column(Text, nullable=True)

    # Financial data
    financial_summary_json = Column(Text, nullable=True)  # JSON object

    # Related reports
    related_research_reports_json = Column(Text, nullable=True)  # JSON array

    # Timestamps
    collected_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    filed_at = Column(DateTime, nullable=False, index=True)

    # Additional metadata
    metadata_json = Column(Text, nullable=True)

    # Report key: '{stock_code}_{fiscal_year}{분기}' e.g. '178320_20201분기'
    report_key = Column(String(30), nullable=True, unique=True, index=True)

    # Processed file path
    processed_dir = Column(Text, nullable=True)

    # Relationship
    document = relationship("DocumentORM", back_populates="financial_reports")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "document_id": self.document_id,
            "corp_code": self.corp_code,
            "corp_name": self.corp_name,
            "stock_code": self.stock_code,
            "report_type": self.report_type,
            "report_period": self.report_period,
            "rcept_no": self.rcept_no,
            "rcept_dt": self.rcept_dt,
            "report_nm": self.report_nm,
            "fiscal_year": self.fiscal_year,
            "fiscal_period": self.fiscal_period,
            "original_url": self.original_url,
            "pdf_url": self.pdf_url,
            "financial_summary": json.loads(self.financial_summary_json) if self.financial_summary_json else None,
            "related_research_reports": json.loads(self.related_research_reports_json) if self.related_research_reports_json else [],
            "collected_at": self.collected_at,
            "filed_at": self.filed_at,
            "metadata": json.loads(self.metadata_json) if self.metadata_json else {},
            "report_key": self.report_key,
            "processed_dir": self.processed_dir,
        }


class NewsArticleORM(Base):
    """News article table ORM model."""

    __tablename__ = "news_articles"

    id = Column(String(36), primary_key=True)
    document_id = Column(String(36), ForeignKey("documents.id"), nullable=False, index=True)
    title = Column(Text, nullable=False)
    content = Column(Text, nullable=True)
    author = Column(String(100), nullable=True)
    published_at = Column(DateTime, nullable=True, index=True)
    sentiment_score = Column(Float, nullable=True)
    sentiment_label = Column(String(20), nullable=True)
    entities_json = Column(Text, nullable=True)  # JSON list of entities
    extracted_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    metadata_json = Column(Text, nullable=True)

    # Relationship
    document = relationship("DocumentORM", back_populates="news_articles")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "document_id": self.document_id,
            "title": self.title,
            "content": self.content,
            "author": self.author,
            "published_at": self.published_at,
            "sentiment_score": self.sentiment_score,
            "sentiment_label": self.sentiment_label,
            "entities": json.loads(self.entities_json) if self.entities_json else [],
            "extracted_at": self.extracted_at,
            "metadata": json.loads(self.metadata_json) if self.metadata_json else {}
        }


class DocumentStore(LoggerMixin):
    """Document storage manager.

    Provides CRUD operations for documents, research reports, and news articles.
    Supports both SQLite and PostgreSQL backends.
    """

    def __init__(self, database_url: Optional[str] = None):
        """Initialize document store.

        Args:
            database_url: Database connection URL. If None, loads from config.
        """
        if database_url is None:
            config = get_config()
            db_type = config.get("database.type", "sqlite")
            if db_type == "sqlite":
                db_path = config.get("database.path", "data/narrative_insight.db")
                database_url = f"sqlite:///{db_path}"
            else:
                # PostgreSQL configuration
                host = config.get("database.host", "localhost")
                port = config.get("database.port", 5432)
                database = config.get("database.database", "narrative_insight")
                user = config.get("database.user", "postgres")
                password = config.get("database.password", "")
                database_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        self.database_url = database_url
        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        self.logger.info("document_store_initialized", database_url=database_url)

    def create_tables(self) -> None:
        """Create all database tables."""
        Base.metadata.create_all(bind=self.engine)
        self.logger.info("database_tables_created")

    def drop_tables(self) -> None:
        """Drop all database tables. WARNING: This will delete all data!"""
        Base.metadata.drop_all(bind=self.engine)
        self.logger.warning("database_tables_dropped")

    def get_session(self) -> Session:
        """Get a new database session.

        Returns:
            SQLAlchemy session

        Usage:
            with store.get_session() as session:
                # Use session
                pass
        """
        return self.SessionLocal()

    # Document CRUD operations

    def create_document(self, document_data: Dict[str, Any]) -> str:
        """Create a new document.

        Args:
            document_data: Document data dictionary

        Returns:
            Document ID
        """
        with self.get_session() as session:
            # Serialize metadata
            metadata_json = json.dumps(document_data.get("metadata", {}))

            doc = DocumentORM(
                id=document_data["id"],
                doc_type=document_data["doc_type"],
                source=document_data["source"],
                url=document_data.get("url"),
                file_path=document_data.get("file_path"),
                collected_at=document_data.get("collected_at", datetime.utcnow()),
                processed_at=document_data.get("processed_at"),
                status=document_data.get("status", "pending"),
                error_message=document_data.get("error_message"),
                checksum=document_data.get("checksum"),
                metadata_json=metadata_json
            )

            session.add(doc)
            session.commit()

            self.logger.info("document_created", document_id=doc.id, doc_type=doc.doc_type)
            return doc.id

    def get_document(self, document_id: str) -> Optional[Dict[str, Any]]:
        """Get document by ID.

        Args:
            document_id: Document ID

        Returns:
            Document data dictionary or None if not found
        """
        with self.get_session() as session:
            doc = session.query(DocumentORM).filter(DocumentORM.id == document_id).first()
            return doc.to_dict() if doc else None

    def get_document_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Get document by URL.

        Args:
            url: Document URL

        Returns:
            Document data dictionary or None if not found
        """
        with self.get_session() as session:
            doc = session.query(DocumentORM).filter(DocumentORM.url == url).first()
            return doc.to_dict() if doc else None

    def get_document_by_checksum(self, checksum: str) -> Optional[Dict[str, Any]]:
        """Get document by checksum.

        Args:
            checksum: Document checksum

        Returns:
            Document data dictionary or None if not found
        """
        with self.get_session() as session:
            doc = session.query(DocumentORM).filter(DocumentORM.checksum == checksum).first()
            return doc.to_dict() if doc else None

    def update_document(self, document_id: str, updates: Dict[str, Any]) -> bool:
        """Update document fields.

        Args:
            document_id: Document ID
            updates: Dictionary of fields to update

        Returns:
            True if updated, False if document not found
        """
        with self.get_session() as session:
            doc = session.query(DocumentORM).filter(DocumentORM.id == document_id).first()
            if not doc:
                return False

            # Handle metadata separately
            if "metadata" in updates:
                updates["metadata_json"] = json.dumps(updates.pop("metadata"))

            for key, value in updates.items():
                if hasattr(doc, key):
                    setattr(doc, key, value)

            session.commit()
            self.logger.info("document_updated", document_id=document_id, fields=list(updates.keys()))
            return True

    def delete_document(self, document_id: str) -> bool:
        """Delete document and related records.

        Args:
            document_id: Document ID

        Returns:
            True if deleted, False if document not found
        """
        with self.get_session() as session:
            doc = session.query(DocumentORM).filter(DocumentORM.id == document_id).first()
            if not doc:
                return False

            session.delete(doc)
            session.commit()
            self.logger.info("document_deleted", document_id=document_id)
            return True

    def list_documents(
        self,
        doc_type: Optional[str] = None,
        status: Optional[str] = None,
        source: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List documents with optional filtering.

        Args:
            doc_type: Filter by document type
            status: Filter by status
            source: Filter by source
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of document dictionaries
        """
        with self.get_session() as session:
            query = session.query(DocumentORM)

            if doc_type:
                query = query.filter(DocumentORM.doc_type == doc_type)
            if status:
                query = query.filter(DocumentORM.status == status)
            if source:
                query = query.filter(DocumentORM.source == source)

            query = query.order_by(DocumentORM.collected_at.desc())
            query = query.limit(limit).offset(offset)

            docs = query.all()
            return [doc.to_dict() for doc in docs]

    # Research Report CRUD operations

    def create_research_report(self, report_data: Dict[str, Any]) -> str:
        """Create a new research report.

        Args:
            report_data: Research report data dictionary

        Returns:
            Report ID
        """
        with self.get_session() as session:
            report = ResearchReportORM(
                id=report_data["id"],
                document_id=report_data["document_id"],
                ticker=report_data["ticker"],
                company_name=report_data["company_name"],
                analyst_name=report_data.get("analyst_name"),
                firm=report_data["firm"],
                report_date=report_data["report_date"],
                target_price=report_data.get("target_price"),
                target_price_currency=report_data.get("target_price_currency", "KRW"),
                investment_opinion=report_data.get("investment_opinion"),
                investment_points_json=json.dumps(report_data.get("investment_points", [])),
                risk_factors_json=json.dumps(report_data.get("risk_factors", [])),
                confidence_score=report_data.get("confidence_score", 0.0),
                extracted_at=report_data.get("extracted_at", datetime.utcnow()),
                metadata_json=json.dumps(report_data.get("metadata", {}))
            )

            session.add(report)
            session.commit()

            self.logger.info("research_report_created", report_id=report.id, ticker=report.ticker)
            return report.id

    def get_research_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Get research report by ID.

        Args:
            report_id: Report ID

        Returns:
            Report data dictionary or None if not found
        """
        with self.get_session() as session:
            report = session.query(ResearchReportORM).filter(ResearchReportORM.id == report_id).first()
            return report.to_dict() if report else None

    def list_research_reports_by_ticker(
        self,
        ticker: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List research reports for a specific ticker.

        Args:
            ticker: Stock ticker
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of report dictionaries
        """
        with self.get_session() as session:
            query = session.query(ResearchReportORM).filter(ResearchReportORM.ticker == ticker)
            query = query.order_by(ResearchReportORM.report_date.desc())
            query = query.limit(limit).offset(offset)

            reports = query.all()
            return [report.to_dict() for report in reports]

    # News Article CRUD operations

    def create_news_article(self, article_data: Dict[str, Any]) -> str:
        """Create a new news article.

        Args:
            article_data: News article data dictionary

        Returns:
            Article ID
        """
        with self.get_session() as session:
            article = NewsArticleORM(
                id=article_data["id"],
                document_id=article_data["document_id"],
                title=article_data["title"],
                content=article_data.get("content"),
                author=article_data.get("author"),
                published_at=article_data.get("published_at"),
                sentiment_score=article_data.get("sentiment_score"),
                sentiment_label=article_data.get("sentiment_label"),
                entities_json=json.dumps(article_data.get("entities", [])),
                extracted_at=article_data.get("extracted_at", datetime.utcnow()),
                metadata_json=json.dumps(article_data.get("metadata", {}))
            )

            session.add(article)
            session.commit()

            self.logger.info("news_article_created", article_id=article.id)
            return article.id

    def get_news_article(self, article_id: str) -> Optional[Dict[str, Any]]:
        """Get news article by ID.

        Args:
            article_id: Article ID

        Returns:
            Article data dictionary or None if not found
        """
        with self.get_session() as session:
            article = session.query(NewsArticleORM).filter(NewsArticleORM.id == article_id).first()
            return article.to_dict() if article else None

    def list_news_articles(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List news articles.

        Args:
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of article dictionaries
        """
        with self.get_session() as session:
            query = session.query(NewsArticleORM)
            query = query.order_by(NewsArticleORM.published_at.desc())
            query = query.limit(limit).offset(offset)

            articles = query.all()
            return [article.to_dict() for article in articles]

    # Financial Report CRUD operations

    def create_financial_report(self, report_data: Dict[str, Any]) -> str:
        """Create a new financial report.

        Args:
            report_data: Financial report data dictionary

        Returns:
            Financial report ID
        """
        with self.get_session() as session:
            # Serialize JSON fields
            financial_summary_json = json.dumps(report_data.get("financial_summary")) if report_data.get("financial_summary") else None
            related_reports_json = json.dumps(report_data.get("related_research_reports", []))
            metadata_json = json.dumps(report_data.get("metadata", {}))

            report = FinancialReportORM(
                id=report_data["id"],
                document_id=report_data["document_id"],
                corp_code=report_data["corp_code"],
                corp_name=report_data["corp_name"],
                stock_code=report_data.get("stock_code"),
                report_type=report_data["report_type"],
                report_period=report_data["report_period"],
                rcept_no=report_data["rcept_no"],
                rcept_dt=report_data["rcept_dt"],
                report_nm=report_data["report_nm"],
                fiscal_year=report_data["fiscal_year"],
                fiscal_period=report_data["fiscal_period"],
                original_url=report_data["original_url"],
                pdf_url=report_data.get("pdf_url"),
                financial_summary_json=financial_summary_json,
                related_research_reports_json=related_reports_json,
                collected_at=report_data.get("collected_at", datetime.utcnow()),
                filed_at=report_data["filed_at"],
                metadata_json=metadata_json
            )

            session.add(report)
            session.commit()

            self.logger.info(
                "financial_report_created",
                report_id=report.id,
                corp_name=report.corp_name,
                fiscal_year=report.fiscal_year
            )
            return report.id

    def get_financial_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Get financial report by ID.

        Args:
            report_id: Financial report ID

        Returns:
            Financial report data dictionary or None
        """
        with self.get_session() as session:
            report = session.query(FinancialReportORM).filter(FinancialReportORM.id == report_id).first()
            return report.to_dict() if report else None

    def get_financial_report_by_rcept_no(self, rcept_no: str) -> Optional[Dict[str, Any]]:
        """Get financial report by DART receipt number.

        Args:
            rcept_no: DART receipt number

        Returns:
            Financial report data dictionary or None
        """
        with self.get_session() as session:
            report = session.query(FinancialReportORM).filter(FinancialReportORM.rcept_no == rcept_no).first()
            return report.to_dict() if report else None

    def update_financial_report_by_rcept_no(self, rcept_no: str, report_data: Dict[str, Any]) -> bool:
        """Update financial report by DART receipt number.

        Args:
            rcept_no: DART receipt number
            report_data: Updated report data

        Returns:
            True if updated, False if not found
        """
        with self.get_session() as session:
            report = session.query(FinancialReportORM).filter(FinancialReportORM.rcept_no == rcept_no).first()
            if not report:
                return False

            # Update fields
            for key, value in report_data.items():
                if key != 'id' and key != 'rcept_no' and hasattr(report, key):
                    setattr(report, key, value)

            session.commit()
            return True

    def list_financial_reports(
        self,
        limit: int = 100,
        offset: int = 0,
        corp_name: Optional[str] = None,
        stock_code: Optional[str] = None,
        report_type: Optional[str] = None,
        fiscal_year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """List financial reports with optional filtering.

        Args:
            limit: Maximum number of reports to return
            offset: Number of reports to skip
            corp_name: Filter by company name (partial match)
            stock_code: Filter by stock code
            report_type: Filter by report type
            fiscal_year: Filter by fiscal year

        Returns:
            List of financial report dictionaries
        """
        with self.get_session() as session:
            query = session.query(FinancialReportORM)

            # Apply filters
            if corp_name:
                query = query.filter(FinancialReportORM.corp_name.like(f"%{corp_name}%"))
            if stock_code:
                query = query.filter(FinancialReportORM.stock_code == stock_code)
            if report_type:
                query = query.filter(FinancialReportORM.report_type == report_type)
            if fiscal_year:
                query = query.filter(FinancialReportORM.fiscal_year == fiscal_year)

            # Order by filed_at descending (newest first)
            query = query.order_by(FinancialReportORM.filed_at.desc())

            # Apply pagination
            query = query.offset(offset).limit(limit)

            reports = query.all()
            return [report.to_dict() for report in reports]

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics.

        Returns:
            Dictionary with statistics
        """
        with self.get_session() as session:
            total_documents = session.query(DocumentORM).count()
            pending_documents = session.query(DocumentORM).filter(DocumentORM.status == "pending").count()
            processed_documents = session.query(DocumentORM).filter(DocumentORM.status == "processed").count()
            error_documents = session.query(DocumentORM).filter(DocumentORM.status == "error").count()
            total_reports = session.query(ResearchReportORM).count()
            total_articles = session.query(NewsArticleORM).count()
            total_financial_reports = session.query(FinancialReportORM).count()

            return {
                "total_documents": total_documents,
                "pending_documents": pending_documents,
                "processed_documents": processed_documents,
                "error_documents": error_documents,
                "total_research_reports": total_reports,
                "total_news_articles": total_articles,
                "total_financial_reports": total_financial_reports
            }
