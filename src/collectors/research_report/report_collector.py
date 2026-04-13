"""Research Report PDF Collector.

PDF 파일에서 텍스트를 추출하여 research_reports 테이블에 저장합니다.
파일명 패턴: {YYYYMMDD}_{ticker}_{company}_{firm}_{title}_{id}.pdf
"""

import hashlib
import os
import uuid
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pdfplumber

from src.storage.document_store import DocumentStore


def extract_text_from_pdf(pdf_path: str) -> str:
    """pdfplumber로 PDF 전체 텍스트 추출."""
    text_parts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
    return "\n".join(text_parts)


def parse_filename(filename: str) -> dict:
    """파일명에서 메타데이터 파싱.

    패턴: {YYYYMMDD}_{ticker}_{company}_{firm}_{title...}_{id}.pdf
    예시: 20130506_000150_두산_대신증권_전자사업부의_높은_수익성_203948.pdf
    """
    stem = Path(filename).stem  # 확장자 제거
    parts = stem.split("_")

    if len(parts) < 5:
        return {}

    try:
        report_date = datetime.strptime(parts[0], "%Y%m%d").date()
    except ValueError:
        report_date = date.today()

    ticker = parts[1]
    company_name = parts[2]
    firm = parts[3]
    # 마지막 파트가 숫자면 report_id, 그 사이가 제목
    if parts[-1].isdigit():
        title = "_".join(parts[4:-1]).strip("_")
        report_id = parts[-1]
    else:
        title = "_".join(parts[4:]).strip("_")
        report_id = ""

    return {
        "report_date": report_date,
        "ticker": ticker,
        "company_name": company_name,
        "firm": firm,
        "title": title,
        "source_id": report_id,
    }


def file_checksum(path: str) -> str:
    """파일 SHA256 체크섬."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


class ResearchReportCollector:
    """로컬 PDF 파일을 읽어 research_reports 테이블에 저장하는 수집기."""

    def __init__(self, pdf_dir: Optional[str] = None, database_url: Optional[str] = None):
        self.pdf_dir = Path(pdf_dir or "data/raw/research_report")
        self.store = DocumentStore(database_url=database_url)

    def _ensure_content_column(self):
        """research_reports 테이블에 content 컬럼이 없으면 추가 (PostgreSQL)."""
        with self.store.engine.connect() as conn:
            conn.execute(
                "ALTER TABLE research_reports ADD COLUMN IF NOT EXISTS content TEXT;"
            )
            conn.commit()

    def collect(self, limit: Optional[int] = None, skip_existing: bool = True) -> dict:
        """PDF 파일들을 처리하여 DB에 저장.

        Args:
            limit: 처리할 최대 파일 수 (None이면 전체)
            skip_existing: 이미 처리된 파일(checksum 중복) 건너뛰기

        Returns:
            처리 결과 통계 dict
        """
        self.store.create_tables()
        self._migrate_content_column()

        pdf_files = sorted(self.pdf_dir.glob("*.pdf"))
        if limit:
            pdf_files = pdf_files[:limit]

        stats = {"total": len(pdf_files), "saved": 0, "skipped": 0, "error": 0}

        for pdf_path in pdf_files:
            try:
                result = self._process_file(pdf_path, skip_existing=skip_existing)
                if result == "saved":
                    stats["saved"] += 1
                elif result == "skipped":
                    stats["skipped"] += 1
            except Exception as e:
                print(f"[ERROR] {pdf_path.name}: {e}")
                stats["error"] += 1

        return stats

    def _migrate_content_column(self):
        """content 컬럼 없으면 추가 (PostgreSQL ALTER TABLE IF NOT EXISTS)."""
        try:
            from sqlalchemy import text
            with self.store.engine.connect() as conn:
                conn.execute(text(
                    "ALTER TABLE research_reports ADD COLUMN IF NOT EXISTS content TEXT;"
                ))
                conn.commit()
        except Exception:
            pass  # 이미 존재하거나 테이블 없음 — create_tables()로 처리됨

    def _process_file(self, pdf_path: Path, skip_existing: bool) -> str:
        """단일 PDF 파일 처리."""
        checksum = file_checksum(str(pdf_path))

        if skip_existing:
            existing_doc = self.store.get_document_by_checksum(checksum)
            if existing_doc:
                print(f"[SKIP] {pdf_path.name} (already in DB)")
                return "skipped"

        meta = parse_filename(pdf_path.name)
        if not meta:
            print(f"[SKIP] {pdf_path.name} (filename parse failed)")
            return "skipped"

        print(f"[READ] {pdf_path.name} ...")
        content = extract_text_from_pdf(str(pdf_path))

        if not content.strip():
            print(f"[WARN] {pdf_path.name}: no text extracted (may be image-based PDF)")

        # documents 테이블에 파일 레코드 저장
        doc_id = str(uuid.uuid4())
        self.store.create_document({
            "id": doc_id,
            "doc_type": "research_report",
            "source": "local_pdf",
            "file_path": str(pdf_path.resolve()),
            "checksum": checksum,
            "status": "processed",
            "processed_at": datetime.utcnow(),
            "metadata": {
                "filename": pdf_path.name,
                "source_id": meta.get("source_id", ""),
                "title": meta.get("title", ""),
            },
        })

        # research_reports 테이블에 내용 저장
        report_id = str(uuid.uuid4())
        self.store.create_research_report({
            "id": report_id,
            "document_id": doc_id,
            "ticker": meta["ticker"],
            "company_name": meta["company_name"],
            "firm": meta["firm"],
            "report_date": meta["report_date"],
            "content": content,
            "confidence_score": 0.0,
        })

        print(f"[SAVE] {meta['company_name']} ({meta['ticker']}) / {meta['firm']} / {meta['report_date']}")
        return "saved"
