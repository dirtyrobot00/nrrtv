"""DART 전체 공시 수집 → PostgreSQL 저장기

테이블:
  dart_disclosures        : 공시 목록 메타 (LIST API 결과)
  dart_document_sections  : 원문 XML + plain_text (DOCUMENT API ZIP 결과)

운용 플로우:
  1. LIST API → dart_disclosures INSERT ON CONFLICT DO NOTHING
  2. fetch_status='pending' → DOCUMENT API (ZIP) 다운로드
  3. ZIP 내 XML 파싱 → dart_document_sections INSERT
  4. fetch_status='done' 업데이트

사용법:
  # 2020년부터 오늘까지 backfill
  python src/collectors/collect_all_from_dart_to_db.py --backfill --from-date 20200101

  # 오늘 1회 수집
  python src/collectors/collect_all_from_dart_to_db.py --once

  # 60초 간격 polling (실운영)
  python src/collectors/collect_all_from_dart_to_db.py --interval 60

  # dry-run (DB 저장 없이 로그만 출력)
  python src/collectors/collect_all_from_dart_to_db.py --once --dry-run

  # 특정 종목만 수집 (단일)
  python src/collectors/collect_all_from_dart_to_db.py --once --stock-code 005930

  # 특정 종목 backfill
  python src/collectors/collect_all_from_dart_to_db.py --backfill --from-date 20200101 --stock-code 005930,000660
"""

import argparse
import io
import os
import sys
import tempfile
from pathlib import Path
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from typing import Optional

import httpx
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# 프로젝트 루트 및 extractor 디렉터리를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "extractor"))
from extern_extract_financial_report_table_flattened import extract as extract_xml_file

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------

DART_LIST_URL     = "https://opendart.fss.or.kr/api/list.json"
DART_DOC_URL      = "https://opendart.fss.or.kr/api/document.xml"
DART_CORPCODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"

PBLNTF_TYPES = ["A", "B", "E", "I"]  # 정기/주요사항/기타/거래소 (D=지분공시 제외)

PAGE_COUNT = 100


class DartQuotaExceededError(Exception):
    """DART API 사용한도 초과 (status=020) 시 발생."""
    pass


# ---------------------------------------------------------------------------
# 핵심 클래스
# ---------------------------------------------------------------------------

class DartToDBCollector:
    """DART 공시 수집 → PostgreSQL 저장기.

    collect_list_for_period() : LIST API → dart_disclosures 저장
    process_pending()         : pending 공시 원문 다운로드 → dart_document_sections 저장
    poll_once()               : 오늘 날짜 1회 실행 (list + process)
    backfill()                : 기간 소급 수집 (월 단위 분할)
    run()                     : 반복 polling 루프
    """

    def __init__(
        self,
        api_key: str,
        db_url: str,
        dry_run: bool = False,
        list_delay: float = 0.0,   # LIST API 호출 간 대기 (초)
        doc_delay: float = 0.5,    # DOCUMENT API 호출 간 대기 (초)
        verbose: bool = True,
        stock_codes: Optional[list[str]] = None,  # None이면 전체 종목
    ):
        self.api_key     = api_key
        self.db_url      = db_url
        self.dry_run     = dry_run
        self.list_delay  = list_delay
        self.stock_codes = set(stock_codes) if stock_codes else None
        self.doc_delay   = doc_delay
        self.verbose    = verbose

        self.client = httpx.Client(timeout=60, follow_redirects=True)
        self._conn: Optional[psycopg2.extensions.connection] = None

        # stock_code → corp_code 매핑 (지정 종목이 있을 때만 로드)
        self._corp_code_map: dict[str, str] = {}
        if self.stock_codes:
            self._corp_code_map = self._load_corp_codes(self.stock_codes)

        self._stats = {
            "list_api_calls":      0,
            "doc_api_calls":       0,
            "disclosures_upserted": 0,
            "sections_inserted":   0,
            "doc_errors":          0,
        }

    # ------------------------------------------------------------------
    # DB 연결 관리
    # ------------------------------------------------------------------

    def _load_corp_codes(self, stock_codes: set) -> dict[str, str]:
        """DART corpCode.xml 다운로드 → stock_code → corp_code 매핑 반환."""
        import xml.etree.ElementTree as ET
        self._log("[corp_code] DART 고유번호 목록 다운로드 중...")
        resp = self.client.get(DART_CORPCODE_URL, params={"crtfc_key": self.api_key})
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            xml_bytes = zf.read(zf.namelist()[0])
        root = ET.fromstring(xml_bytes)
        mapping = {}
        for item in root.findall("list"):
            sc = (item.findtext("stock_code") or "").strip()
            cc = (item.findtext("corp_code") or "").strip()
            if sc and cc and sc in stock_codes:
                mapping[sc] = cc
        self._log(f"[corp_code] {len(mapping)}개 종목 매핑 완료: {mapping}")
        return mapping

    # ------------------------------------------------------------------

    @property
    def conn(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.db_url)
        return self._conn

    # ------------------------------------------------------------------
    # DB 쓰기 메서드
    # ------------------------------------------------------------------

    def _upsert_disclosures(self, items: list[dict]) -> int:
        """dart_disclosures INSERT ON CONFLICT DO NOTHING. 시도 건수 반환."""
        if not items or self.dry_run:
            return 0

        sql = """
            INSERT INTO dart_disclosures
                (rcept_no, corp_code, corp_name, stock_code, report_nm,
                 rcept_dt, flr_nm, pblntf_ty, pblntf_detail_ty)
            VALUES
                (%(rcept_no)s, %(corp_code)s, %(corp_name)s, %(stock_code)s, %(report_nm)s,
                 %(rcept_dt)s, %(flr_nm)s, %(pblntf_ty)s, %(pblntf_detail_ty)s)
            ON CONFLICT (rcept_no) DO NOTHING
        """

        rows = []
        for item in items:
            rcept_dt_raw = item.get("rcept_dt", "")
            try:
                rcept_dt = datetime.strptime(rcept_dt_raw, "%Y%m%d").date()
            except ValueError:
                rcept_dt = None

            rows.append({
                "rcept_no":         item.get("rcept_no", ""),
                "corp_code":        item.get("corp_code", ""),
                "corp_name":        item.get("corp_name", ""),
                "stock_code":       item.get("stock_code", "").strip() or None,
                "report_nm":        item.get("report_nm", ""),
                "rcept_dt":         rcept_dt,
                "flr_nm":           item.get("flr_nm", "") or None,
                "pblntf_ty":        item.get("pblntf_ty", ""),
                "pblntf_detail_ty": item.get("pblntf_detail_ty", "") or None,
            })

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
        self.conn.commit()
        return len(rows)

    def _get_pending_rcept_nos(self, limit: int = 100) -> list[str]:
        """fetch_status='pending'인 rcept_no 목록 반환 (오래된 것부터)."""
        sql = """
            SELECT rcept_no FROM dart_disclosures
            WHERE fetch_status = 'pending'
            ORDER BY rcept_dt ASC, rcept_no ASC
            LIMIT %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (limit,))
            return [row[0] for row in cur.fetchall()]

    def _mark_fetching(self, rcept_no: str) -> None:
        if self.dry_run:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE dart_disclosures SET fetch_status='fetching' WHERE rcept_no=%s",
                (rcept_no,),
            )
        self.conn.commit()

    def _mark_done(self, rcept_no: str) -> None:
        if self.dry_run:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE dart_disclosures
                   SET fetch_status='done', fetched_at=now(), fetch_error=NULL
                   WHERE rcept_no=%s""",
                (rcept_no,),
            )
        self.conn.commit()

    def _mark_error(self, rcept_no: str, error: str) -> None:
        if self.dry_run:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE dart_disclosures
                   SET fetch_status='error', fetched_at=now(), fetch_error=%s
                   WHERE rcept_no=%s""",
                (error[:500], rcept_no),
            )
        self.conn.commit()

    def _insert_sections(self, rcept_no: str, sections: list[dict]) -> None:
        if not sections or self.dry_run:
            return

        sql = """
            INSERT INTO dart_document_sections
                (rcept_no, section_ord, section_nm, raw_xml, plain_text)
            VALUES
                (%(rcept_no)s, %(section_ord)s, %(section_nm)s, %(raw_xml)s, %(plain_text)s)
            ON CONFLICT (rcept_no, section_ord) DO NOTHING
        """
        rows = [
            {
                "rcept_no":    rcept_no,
                "section_ord": s["section_ord"],
                "section_nm":  s["section_nm"],
                "raw_xml":     s["raw_xml"],
                "plain_text":  s["plain_text"],
            }
            for s in sections
        ]
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows)
        self.conn.commit()

    # ------------------------------------------------------------------
    # DART API 호출
    # ------------------------------------------------------------------

    def _fetch_all_list(self, pblntf_ty: str, bgn_de: str, end_de: str,
                        corp_code: Optional[str] = None) -> list[dict]:
        """특정 유형/기간의 공시 목록 전체 페이지 수집.
        corp_code 지정 시 해당 종목만 조회 (전체 조회 후 필터링 불필요).
        """
        all_items: list[dict] = []
        page_no = 1

        while True:
            params = {
                "crtfc_key":  self.api_key,
                "pblntf_ty":  pblntf_ty,
                "bgn_de":     bgn_de,
                "end_de":     end_de,
                "page_no":    page_no,
                "page_count": PAGE_COUNT,
            }
            if corp_code:
                params["corp_code"] = corp_code
            try:
                resp = self.client.get(DART_LIST_URL, params=params)
                resp.raise_for_status()
                self._stats["list_api_calls"] += 1
            except httpx.HTTPError as e:
                self._log(f"  [ERROR] LIST API 실패 (ty={pblntf_ty}, page={page_no}): {e}")
                break

            data = resp.json()
            status = data.get("status", "000")

            if status == "013":   # 조회 결과 없음 (정상)
                break
            if status == "020":   # 사용한도 초과 → 즉시 중단
                msg = data.get("message", "사용한도를 초과하였습니다.")
                self._log(f"  [ERROR] DART API status=020: {msg} → 즉시 중단")
                raise DartQuotaExceededError(msg)
            if status != "000":
                self._log(f"  [WARN] DART API status={status}: {data.get('message', '')}")
                break

            items = data.get("list", [])
            for item in items:
                item["pblntf_ty"] = pblntf_ty  # API 응답에 누락될 수 있어 직접 주입
            all_items.extend(items)

            total_count = int(data.get("total_count", 0))
            total_pages = (total_count + PAGE_COUNT - 1) // PAGE_COUNT

            if page_no >= total_pages:
                break

            page_no += 1
            if self.list_delay > 0:
                time.sleep(self.list_delay)

        return all_items

    def _download_zip(self, rcept_no: str) -> bytes:
        """DOCUMENT API에서 ZIP 바이트 다운로드."""
        resp = self.client.get(
            DART_DOC_URL,
            params={"crtfc_key": self.api_key, "rcept_no": rcept_no},
        )
        resp.raise_for_status()
        self._stats["doc_api_calls"] += 1

        # DART는 오류 시 ZIP 대신 JSON을 반환하기도 함
        content_type = resp.headers.get("content-type", "")
        if "json" in content_type or ("text" in content_type and not resp.content.startswith(b"PK")):
            try:
                err = resp.json()
                raise ValueError(f"DART 오류 응답: {err.get('message', resp.text[:200])}")
            except (ValueError, Exception) as e:
                raise RuntimeError(str(e)) from e

        return resp.content

    def _zip_to_sections(self, zip_bytes: bytes) -> list[dict]:
        """ZIP 바이트 → 섹션 리스트."""
        sections = []

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            xml_names = sorted(
                n for n in zf.namelist() if n.lower().endswith(".xml")
            )

            for ord_idx, name in enumerate(xml_names):
                try:
                    raw_bytes  = zf.read(name)
                    # EUC-KR 등 non-UTF-8 파일을 UTF-8로 변환 후 저장
                    for enc in ("utf-8", "euc-kr", "cp949"):
                        try:
                            raw_xml = raw_bytes.decode(enc)
                            break
                        except UnicodeDecodeError:
                            continue
                    else:
                        raw_xml = raw_bytes.decode("utf-8", errors="replace")
                    utf8_bytes = raw_xml.encode("utf-8")
                    plain_text = self._xml_to_plain_text(utf8_bytes)

                    sections.append({
                        "section_ord": ord_idx,
                        "section_nm":  name,
                        "raw_xml":     raw_xml,
                        "plain_text":  plain_text,
                    })
                except Exception as e:
                    self._log(f"    [WARN] XML 읽기 실패 ({name}): {e}")

        return sections

    def _xml_to_plain_text(self, xml_bytes: bytes) -> str:
        """extern_extract_financial_report_table_flattened 로 텍스트 추출.

        파서가 구조를 인식하지 못해 '=== 목차 ===' 수준만 반환할 경우
        BeautifulSoup get_text() 로 폴백.
        """
        from bs4 import BeautifulSoup

        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as tmp:
            tmp.write(xml_bytes)
            tmp_path = tmp.name
        try:
            result = extract_xml_file(tmp_path)
        finally:
            os.unlink(tmp_path)

        # 실질 내용 여부 판단: 목차 헤더/공백 제거 후 100자 미만이면 fallback
        stripped = result.replace("=== 목차 ===", "").strip()
        if len(stripped) < 100:
            xml_str = xml_bytes.decode("utf-8", errors="replace")
            soup = BeautifulSoup(xml_str, "html.parser")
            result = soup.get_text(separator="\n", strip=True)

        return result

    # ------------------------------------------------------------------
    # 메인 플로우
    # ------------------------------------------------------------------

    def _filter_by_stock_code(self, items: list[dict]) -> list[dict]:
        """stock_codes 필터 적용. stock_codes=None이면 전체 반환."""
        if self.stock_codes is None:
            return items
        return [i for i in items if i.get("stock_code", "").strip() in self.stock_codes]

    def collect_list_for_period(self, bgn_de: str, end_de: str) -> int:
        """기간 내 모든 공시 유형 목록 수집 → dart_disclosures 저장 (병렬 요청).

        Returns:
            수집 시도 건수 (중복 포함)
        """
        total_items: list[dict] = []

        # 단일 종목이면 corp_code로 API 직접 필터링 (전체 조회 불필요)
        # 복수 종목이면 corp_code를 한 번에 넘길 수 없어 전체 조회 후 클라이언트 필터링
        if self._corp_code_map and len(self._corp_code_map) == 1:
            corp_code = next(iter(self._corp_code_map.values()))
            filter_fn = lambda items: items  # API가 이미 필터링함
        else:
            corp_code = None
            filter_fn = self._filter_by_stock_code

        with ThreadPoolExecutor(max_workers=len(PBLNTF_TYPES)) as pool:
            future_to_ty = {
                pool.submit(self._fetch_all_list, ty, bgn_de, end_de, corp_code): ty
                for ty in PBLNTF_TYPES
            }
            for future in as_completed(future_to_ty):
                ty = future_to_ty[future]
                items = filter_fn(future.result())  # DartQuotaExceededError 그대로 전파
                self._log(f"  [{ty}] {bgn_de}~{end_de}: {len(items)}건")
                total_items.extend(items)

        n = self._upsert_disclosures(total_items)
        self._stats["disclosures_upserted"] += n
        return len(total_items)

    def reset_bad_plain_text(self, min_content_len: int = 200, batch_size: int = 1000) -> int:
        """plain_text가 '=== 목차 ===' 수준(실질 내용 부족)인 섹션을 재처리 대기 상태로 초기화.

        1. dart_document_sections 에서 불량 plain_text 행 삭제
        2. 해당 rcept_no 의 dart_disclosures.fetch_status → 'pending'

        Args:
            min_content_len: 목차 헤더 제거 후 이 길이 미만이면 불량으로 판단 (기본 200)
            batch_size:      1회 처리 배치 크기

        Returns:
            초기화된 rcept_no 수
        """
        # 불량 판단 SQL: '=== 목차 ===' 제거 후 trim 길이가 min_content_len 미만인 섹션의 rcept_no 수집
        find_sql = """
            SELECT DISTINCT rcept_no
            FROM dart_document_sections
            WHERE length(trim(replace(plain_text, '=== 목차 ===', ''))) < %(min_len)s
        """
        delete_sql = """
            DELETE FROM dart_document_sections
            WHERE rcept_no = ANY(%(rcept_nos)s)
        """
        reset_sql = """
            UPDATE dart_disclosures
            SET fetch_status = 'pending', fetch_error = NULL
            WHERE rcept_no = ANY(%(rcept_nos)s)
              AND fetch_status IN ('done', 'error', 'fetching')
        """

        with self.conn.cursor() as cur:
            cur.execute(find_sql, {"min_len": min_content_len})
            bad_rcept_nos = [row[0] for row in cur.fetchall()]

        if not bad_rcept_nos:
            self._log("  [reset] 불량 plain_text 없음.")
            return 0

        self._log(f"  [reset] 불량 rcept_no {len(bad_rcept_nos)}건 발견 → 재처리 대기 전환 중...")

        total_reset = 0
        for i in range(0, len(bad_rcept_nos), batch_size):
            batch = bad_rcept_nos[i:i + batch_size]
            if not self.dry_run:
                with self.conn.cursor() as cur:
                    cur.execute(delete_sql, {"rcept_nos": batch})
                    deleted = cur.rowcount
                    cur.execute(reset_sql, {"rcept_nos": batch})
                    updated = cur.rowcount
                self.conn.commit()
                self._log(f"    배치 {i//batch_size + 1}: 섹션 {deleted}행 삭제, 공시 {updated}건 pending 전환")
            else:
                self._log(f"    [DRY-RUN] 배치 {i//batch_size + 1}: {len(batch)}건 스킵")
            total_reset += len(batch)

        self._log(f"  [reset] 완료: {total_reset}건 재처리 대기 전환")
        return total_reset

    def process_pending(self, limit: int = 100) -> int:
        """fetch_status='pending'인 공시 원문 다운로드 및 저장.

        Returns:
            처리 완료(done) 건수
        """
        rcept_nos = self._get_pending_rcept_nos(limit)
        if not rcept_nos:
            return 0

        self._log(f"\n  [원문 수집] pending {len(rcept_nos)}건 처리 시작...")
        done = 0

        for rcept_no in rcept_nos:
            try:
                self._mark_fetching(rcept_no)

                if self.dry_run:
                    self._log(f"    [DRY-RUN] {rcept_no} 다운로드 스킵")
                    done += 1
                    continue

                zip_bytes = self._download_zip(rcept_no)
                sections  = self._zip_to_sections(zip_bytes)
                self._insert_sections(rcept_no, sections)
                self._mark_done(rcept_no)

                self._stats["sections_inserted"] += len(sections)
                self._log(f"    ✓ {rcept_no}: {len(sections)}개 섹션 저장")
                done += 1

            except Exception as e:
                err_msg = str(e)
                self._log(f"    [ERROR] {rcept_no}: {err_msg}")
                # 트랜잭션 abort 상태 해제 후 에러 기록
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                self._mark_error(rcept_no, err_msg)
                self._stats["doc_errors"] += 1

            time.sleep(self.doc_delay)

        return done

    def poll_once(self) -> None:
        """오늘 날짜 기준 1회 polling (목록 수집 + 원문 처리)."""
        today = datetime.now().strftime("%Y%m%d")
        stock_info = f" | 종목: {','.join(sorted(self.stock_codes))}" if self.stock_codes else ""
        self._log(f"\n{'='*60}")
        self._log(f"[polling] {today}{stock_info}")
        self._log(f"{'='*60}")

        try:
            n = self.collect_list_for_period(today, today)
        except DartQuotaExceededError as e:
            self._log(f"\n[ABORT] DART 사용한도 초과: {e}")
            sys.exit(1)
        self._log(f"  → 수집 {n}건")

        done = self.process_pending()
        self._log(f"  → 원문 처리 {done}건")

    def backfill(self, from_date: str, to_date: Optional[str] = None, list_only: bool = False) -> None:
        """from_date ~ to_date 기간 소급 수집 (월 단위 분할).

        2단계로 분리 실행:
          Phase 1: 전체 기간 목록 메타 수집 (LIST API, 빠름)
          Phase 2: pending 원문 다운로드 (ZIP, 느림) → list_only=True 시 생략

        Args:
            from_date:  시작일 YYYYMMDD
            to_date:    종료일 YYYYMMDD (기본: 오늘)
            list_only:  True면 목록 수집만 수행 (원문 다운로드 생략)
        """
        if to_date is None:
            to_date = datetime.now().strftime("%Y%m%d")

        start = datetime.strptime(from_date, "%Y%m%d").date()
        end   = datetime.strptime(to_date,   "%Y%m%d").date()

        # 월 단위 구간 생성
        months: list[tuple[str, str]] = []
        cur = start.replace(day=1)
        while cur <= end:
            next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end  = next_month - timedelta(days=1)
            months.append((
                max(cur, start).strftime("%Y%m%d"),
                min(month_end, end).strftime("%Y%m%d"),
            ))
            cur = next_month

        stock_info = f" | 종목: {','.join(sorted(self.stock_codes))}" if self.stock_codes else ""
        self._log(f"\nBackfill: {from_date} ~ {to_date} ({len(months)}개월){stock_info}")

        # ── Phase 1: 목록 메타 수집 ──────────────────────────────────
        self._log(f"\n{'─'*60}")
        self._log("Phase 1: 공시 목록 수집 (LIST API)")
        self._log(f"{'─'*60}")

        total_listed = 0
        try:
            for i, (bgn, end_m) in enumerate(months, 1):
                self._log(f"  [{i:2d}/{len(months)}] {bgn} ~ {end_m}", )
                n = self.collect_list_for_period(bgn, end_m)
                total_listed += n
                self._log(f"          → {n}건  (누적 {total_listed}건)")
        except DartQuotaExceededError as e:
            self._log(f"\n[ABORT] DART 사용한도 초과로 수집 중단: {e}")
            self._log(f"  Phase 1 중단 시점 누적: {total_listed}건")
            self._print_stats()
            sys.exit(1)

        self._log(f"\n  Phase 1 완료: 총 {total_listed}건 수집")

        # ── Phase 2: 원문 다운로드 ───────────────────────────────────
        if list_only:
            self._log("\n  [list-only 모드] 원문 다운로드 생략")
            self._log("  → 나중에 --pending 으로 원문을 처리하세요.")
        else:
            self._log(f"\n{'─'*60}")
            self._log("Phase 2: 원문 다운로드 (DOCUMENT API)")
            self._log(f"{'─'*60}")

            total_done = 0
            while True:
                done = self.process_pending(limit=200)
                if done == 0:
                    break
                total_done += done
                self._log(f"  → {done}건 처리 (누적 {total_done}건)")

            self._log(f"\n  Phase 2 완료: 총 {total_done}건 원문 저장")

        self._log(f"\n{'='*60}")
        self._log("Backfill 완료")
        self._print_stats()

    def run(self, interval_sec: int = 60) -> None:
        """반복 polling 루프."""
        mode = "DRY-RUN" if self.dry_run else "LIVE"
        self._log(f"\n폴링 시작 [{mode}] | 간격: {interval_sec}초 | Ctrl+C로 종료")

        try:
            while True:
                self.poll_once()
                self._log(f"\n  다음 폴링까지 {interval_sec}초 대기...")
                time.sleep(interval_sec)
        except KeyboardInterrupt:
            self._log("\n[종료] 사용자 인터럽트")

        self._print_stats()

    # ------------------------------------------------------------------
    # 유틸
    # ------------------------------------------------------------------

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(msg, flush=True)

    def _print_stats(self) -> None:
        self._log(f"\n{'='*60}")
        self._log("수집 통계")
        self._log(f"{'='*60}")
        labels = {
            "list_api_calls":       "LIST API 호출",
            "doc_api_calls":        "DOCUMENT API 호출",
            "disclosures_upserted": "공시 목록 저장 시도",
            "sections_inserted":    "원문 섹션 저장",
            "doc_errors":           "원문 수집 오류",
        }
        for key, label in labels.items():
            self._log(f"  {label:20s}: {self._stats[key]}")

    def close(self) -> None:
        self.client.close()
        if self._conn and not self._conn.closed:
            self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="DART 전체 공시 수집 → PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # 실행 모드 (상호 배타)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--once",      action="store_true", help="오늘 1회 수집")
    mode_group.add_argument("--interval",  type=int, metavar="SEC", help="반복 polling 간격 (초)")
    mode_group.add_argument("--backfill",  action="store_true", help="기간 소급 수집")
    mode_group.add_argument("--pending",         action="store_true", help="pending 원문만 처리")
    mode_group.add_argument("--reset-bad-text",  action="store_true", help="불량 plain_text 섹션 삭제 → pending 재전환 후 즉시 재처리")

    # backfill 옵션
    parser.add_argument("--from-date",  default="20200101", help="backfill 시작일 YYYYMMDD (기본: 20200101)")
    parser.add_argument("--to-date",    default=None,       help="backfill 종료일 YYYYMMDD (기본: 오늘)")
    parser.add_argument("--list-only",  action="store_true", help="backfill 시 목록 수집만 수행 (원문 다운로드 생략, 나중에 --pending으로 처리)")

    # 종목 필터
    parser.add_argument("--stock-code", default="", metavar="CODE",
                        help="수집할 종목코드 (콤마 구분, 예: 005930,000660). 미지정시 전체 종목")

    # 공통 옵션
    parser.add_argument("--dry-run",    action="store_true", help="DB 저장 없이 로그만 출력")
    parser.add_argument("--api-key",    default="",          help="DART API 키 (기본: 환경변수 DART_API_KEY)")
    parser.add_argument("--db-url",     default="",          help="PostgreSQL URL (기본: 환경변수 DATABASE_URL)")
    parser.add_argument("--doc-delay",  type=float, default=0.5, help="원문 API 호출 간 대기 초 (기본: 0.5)")
    parser.add_argument("--list-delay", type=float, default=0.3, help="목록 API 호출 간 대기 초 (기본: 0.3)")
    parser.add_argument("--limit",      type=int, default=100, help="pending 1회 처리 건수 (기본: 100)")

    args = parser.parse_args()

    api_key     = args.api_key or os.environ.get("DART_API_KEY", "")
    db_url      = args.db_url  or os.environ.get("DATABASE_URL", "")
    stock_codes = [s.strip() for s in args.stock_code.split(",") if s.strip()] or None

    if not api_key:
        print("[ERROR] DART_API_KEY가 설정되지 않았습니다.")
        sys.exit(1)
    if not db_url:
        print("[ERROR] DATABASE_URL이 설정되지 않았습니다.")
        sys.exit(1)

    if stock_codes:
        print(f"[필터] 종목코드: {', '.join(stock_codes)}")
    else:
        print("[필터] 전체 종목")

    with DartToDBCollector(
        api_key     = api_key,
        db_url      = db_url,
        dry_run     = args.dry_run,
        list_delay  = args.list_delay,
        doc_delay   = args.doc_delay,
        verbose     = True,
        stock_codes = stock_codes,
    ) as collector:

        if args.once:
            collector.poll_once()
            collector._print_stats()

        elif args.interval:
            collector.run(interval_sec=args.interval)

        elif args.backfill:
            collector.backfill(from_date=args.from_date, to_date=args.to_date, list_only=args.list_only)

        elif args.pending:
            done = collector.process_pending(limit=args.limit)
            print(f"\n원문 처리 완료: {done}건")
            collector._print_stats()

        elif args.reset_bad_text:
            reset = collector.reset_bad_plain_text()
            if reset > 0:
                print(f"\n{reset}건 pending 전환 완료. 재처리 시작...")
                total_done = 0
                while True:
                    done = collector.process_pending(limit=args.limit)
                    if done == 0:
                        break
                    total_done += done
                    print(f"  → {done}건 처리 (누적 {total_done}건)")
                print(f"\n재처리 완료: 총 {total_done}건")
            collector._print_stats()


if __name__ == "__main__":
    main()
