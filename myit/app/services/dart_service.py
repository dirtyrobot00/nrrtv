"""
OPEN DART 공시 수집 서비스
PRD 섹션 6-B: Living Insight 데이터 소스
"""
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict

import httpx

from app.core.config import settings


class DartService:
    """금융감독원 OPEN DART API 연동"""

    BASE_URL = "https://opendart.fss.or.kr/api"

    def __init__(self):
        self.api_key = settings.DART_API_KEY

    @property
    def is_available(self) -> bool:
        return bool(self.api_key)

    async def fetch_recent_disclosures(
        self,
        corp_code: Optional[str] = None,
        begin_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page_count: int = 20,
    ) -> List[Dict]:
        """
        최근 공시 목록 조회
        https://opendart.fss.or.kr/api/list.json
        """
        if not self.is_available:
            return self._demo_disclosures()

        if not begin_date:
            begin_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        params = {
            "crtfc_key": self.api_key,
            "bgn_de": begin_date,
            "end_de": end_date,
            "page_count": page_count,
        }
        if corp_code:
            params["corp_code"] = corp_code

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{self.BASE_URL}/list.json", params=params)
            response.raise_for_status()
            data = response.json()

        if data.get("status") != "000":
            return []

        disclosures = data.get("list", [])
        return [
            {
                "source": "dart",
                "source_id": d.get("rcept_no", ""),
                "title": d.get("report_nm", ""),
                "content": f"{d.get('corp_name', '')} - {d.get('report_nm', '')}",
                "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={d.get('rcept_no', '')}",
                "tickers": json.dumps([d.get("stock_code", "")]) if d.get("stock_code") else None,
                "published_at": self._parse_date(d.get("rcept_dt")),
                "metadata_json": json.dumps(d),
            }
            for d in disclosures
        ]

    async def fetch_corp_info(self, corp_code: str) -> Optional[Dict]:
        """기업 기본 정보 조회"""
        if not self.is_available:
            return None

        params = {
            "crtfc_key": self.api_key,
            "corp_code": corp_code,
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{self.BASE_URL}/company.json", params=params)
            response.raise_for_status()
            return response.json()

    async def search_corp_code(self, corp_name: str) -> Optional[str]:
        """기업명으로 corp_code 검색 (corpCode.xml 다운로드 필요 — MVP에서는 간단 매핑)"""
        # TODO: corpCode.xml 다운로드 후 로컬 매핑 구현
        return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    def _demo_disclosures(self) -> List[Dict]:
        """데모 모드: API 키 없을 때 샘플 데이터"""
        now = datetime.now(timezone.utc)
        return [
            {
                "source": "dart",
                "source_id": "DEMO-001",
                "title": "[데모] 삼성전자 — 분기보고서 (2024.09)",
                "content": "삼성전자의 2024년 3분기 분기보고서가 공시되었습니다.",
                "url": "https://dart.fss.or.kr",
                "tickers": json.dumps(["005930"]),
                "published_at": now.isoformat(),
                "metadata_json": json.dumps({"demo": True}),
            },
            {
                "source": "dart",
                "source_id": "DEMO-002",
                "title": "[데모] SK하이닉스 — 주요사항보고서",
                "content": "SK하이닉스의 주요사항보고서가 공시되었습니다.",
                "url": "https://dart.fss.or.kr",
                "tickers": json.dumps(["000660"]),
                "published_at": now.isoformat(),
                "metadata_json": json.dumps({"demo": True}),
            },
        ]


# 싱글턴
dart_service = DartService()
