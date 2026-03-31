#!/usr/bin/env python3
"""
한국 상장 기업 종목 코드 수집 스크립트
kind.krx.co.kr에서 KOSPI/KOSDAQ/KONEX 전체 상장 종목을 가져와
data/all_corp.list 파일에 저장합니다.

저장 형식: 종목코드|종목명|시장구분
"""

import requests
import sys
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_FILE = DATA_DIR / "all_corp.list"

URL = "https://kind.krx.co.kr/corpgeneral/corpList.do"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

MARKET_MAP = {"유가": "KOSPI", "코스닥": "KOSDAQ", "코넥스": "KONEX"}


def fetch_all_corps() -> list[tuple[str, str, str]]:
    params = {"method": "download", "searchType": "13"}
    resp = requests.get(URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    resp.encoding = "euc-kr"

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = soup.find_all("tr")

    corps = []
    for row in rows[1:]:  # 헤더 제외
        tds = row.find_all("td")
        if len(tds) < 3:
            continue
        name = tds[0].get_text().strip()
        market_raw = tds[1].get_text().strip()
        code = tds[2].get_text().strip().zfill(6)
        market = MARKET_MAP.get(market_raw, market_raw)
        if name and code:
            corps.append((code, name, market))

    return corps


def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 한국 상장 종목 코드 수집 시작")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        corps = fetch_all_corps()
    except Exception as e:
        print(f"수집 실패: {e}", file=sys.stderr)
        sys.exit(1)

    if not corps:
        print("수집된 종목이 없습니다. 종료합니다.", file=sys.stderr)
        sys.exit(1)

    corps.sort(key=lambda x: x[0])

    market_counts = {}
    for _, _, market in corps:
        market_counts[market] = market_counts.get(market, 0) + 1

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"# 한국 상장 종목 코드 목록\n")
        f.write(f"# 수집일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# 총 {len(corps)}개 종목 ({', '.join(f'{m}:{c}' for m, c in sorted(market_counts.items()))})\n")
        f.write(f"# 형식: 종목코드|종목명|시장구분\n")
        for code, name, market in corps:
            f.write(f"{code}|{name}|{market}\n")

    print(f"저장 완료: {OUTPUT_FILE}")
    print(f"총 {len(corps)}개 종목: " + ", ".join(f"{m} {c}개" for m, c in sorted(market_counts.items())))


if __name__ == "__main__":
    main()
