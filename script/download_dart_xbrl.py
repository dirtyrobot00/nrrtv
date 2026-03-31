#!/usr/bin/env python3
"""삼성전자(005930) DART XBRL 재무보고서 다운로드 스크립트.

OpenDART API (fnlttXbrl.xml) 를 사용하여 XBRL ZIP 파일을 다운로드합니다.
https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019019
"""

import io
import os
import sys
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load .env
project_root = Path(__file__).parent.parent
load_dotenv(project_root / ".env")

API_KEY = os.getenv("DART_API_KEY")
BASE_URL = "https://opendart.fss.or.kr/api"
STOCK_CODE = "005930"  # 삼성전자
OUTPUT_DIR = project_root / "data" / "raw" / "financial_report" / "005930_삼성전자"

# 보고서 코드
REPRT_CODE = {
    "11011": "사업보고서",
    "11012": "반기보고서",
    "11013": "1분기보고서",
    "11014": "3분기보고서",
}


def get_corp_code(stock_code: str) -> str:
    """종목코드로 DART 기업코드(corp_code) 조회 (corpCode.xml 전체 목록에서 검색)."""
    import xml.etree.ElementTree as ET

    print(f"[1/3] corp_code 조회 중 (종목코드: {stock_code})...")
    resp = requests.get(
        f"{BASE_URL}/corpCode.xml",
        params={"crtfc_key": API_KEY},
        timeout=60,
    )
    resp.raise_for_status()

    # ZIP → XML 파싱
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        xml_filename = [n for n in zf.namelist() if n.endswith(".xml")][0]
        xml_content = zf.read(xml_filename)

    root = ET.fromstring(xml_content)
    for item in root.iter("list"):
        if item.findtext("stock_code", "").strip() == stock_code:
            corp_code = item.findtext("corp_code", "").strip()
            corp_name = item.findtext("corp_name", "").strip()
            print(f"    → corp_code={corp_code}, 기업명={corp_name}")
            return corp_code

    raise RuntimeError(f"종목코드 {stock_code}에 해당하는 기업을 찾을 수 없습니다.")


def get_report_list(corp_code: str, bgn_de: str = "20200101", end_de: str = "20251231") -> list:
    """기간 내 사업보고서 목록 조회."""
    print(f"[2/3] 보고서 목록 조회 중 ({bgn_de} ~ {end_de})...")
    resp = requests.get(
        f"{BASE_URL}/list.json",
        params={
            "crtfc_key": API_KEY,
            "corp_code": corp_code,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "pblntf_ty": "A",  # 정기공시
            "page_count": 40,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") not in ("000", "013"):
        raise RuntimeError(f"목록 조회 실패: {data}")

    items = data.get("list", [])
    # 사업보고서/반기보고서/분기보고서만 필터
    target_keywords = ["사업보고서", "반기보고서", "분기보고서"]
    filtered = [
        item for item in items
        if any(kw in item.get("report_nm", "") for kw in target_keywords)
    ]
    print(f"    → 총 {len(filtered)}건 조회됨")
    for item in filtered[:5]:
        print(f"       {item['rcept_dt']} | {item['report_nm']} | rcept_no={item['rcept_no']}")
    if len(filtered) > 5:
        print(f"       ... 외 {len(filtered) - 5}건")
    return filtered


def map_reprt_code(report_nm: str) -> str:
    """보고서명으로 reprt_code 매핑."""
    if "사업보고서" in report_nm:
        return "11011"
    elif "반기보고서" in report_nm:
        return "11012"
    elif "1분기" in report_nm:
        return "11013"
    elif "3분기" in report_nm:
        return "11014"
    return "11011"


def download_xbrl(rcept_no: str, reprt_code: str, output_dir: Path) -> Path:
    """XBRL ZIP 다운로드 및 압축 해제."""
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[3/3] XBRL 다운로드: rcept_no={rcept_no}, reprt_code={reprt_code} ({REPRT_CODE.get(reprt_code, reprt_code)})")
    resp = requests.get(
        f"{BASE_URL}/fnlttXbrl.xml",
        params={
            "crtfc_key": API_KEY,
            "rcept_no": rcept_no,
            "reprt_code": reprt_code,
        },
        timeout=60,
    )
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    # 에러 응답(XML/JSON)인 경우 처리
    if "xml" in content_type or "json" in content_type:
        print(f"    ERROR 응답: {resp.text[:300]}")
        raise RuntimeError("XBRL 다운로드 실패 - API 에러 응답")

    # ZIP 압축 해제
    zip_dir = output_dir / rcept_no
    zip_dir.mkdir(exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        names = zf.namelist()
        print(f"    → ZIP 내 파일: {names}")
        zf.extractall(zip_dir)

    print(f"    → 저장 완료: {zip_dir}")
    return zip_dir


def main():
    if not API_KEY or API_KEY == "your_dart_api_key":
        print("ERROR: DART_API_KEY가 .env에 설정되지 않았습니다.")
        sys.exit(1)

    print("=" * 60)
    print(f"삼성전자({STOCK_CODE}) DART XBRL 다운로드")
    print(f"출력 디렉토리: {OUTPUT_DIR}")
    print("=" * 60)

    # 1. corp_code 조회
    corp_code = get_corp_code(STOCK_CODE)

    # 2. 보고서 목록 조회
    reports = get_report_list(corp_code)
    if not reports:
        print("조회된 보고서가 없습니다.")
        return

    # 가장 최근 사업보고서(annual) 1건만 다운로드
    annual = [r for r in reports if "사업보고서" in r.get("report_nm", "")]
    target = annual[0] if annual else reports[0]

    rcept_no = target["rcept_no"]
    reprt_code = map_reprt_code(target["report_nm"])
    print(f"\n다운로드 대상: {target['rcept_dt']} | {target['report_nm']}")

    # 3. XBRL 다운로드
    saved_dir = download_xbrl(rcept_no, reprt_code, OUTPUT_DIR)

    print("\n완료!")
    print(f"저장 위치: {saved_dir}")


if __name__ == "__main__":
    main()
