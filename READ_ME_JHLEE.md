# NRRTV 프로젝트 분석 문서

> 한국 금융 데이터 수집 및 처리 시스템

---

## 1. 프로젝트 개요

NRRTV는 **한국 금융 데이터를 수집하고 처리하는 파이프라인 시스템**입니다. DART API, Naver Finance 등에서 뉴스, 리서치 리포트, 재무보고서를 수집하고 RAG/Knowledge Graph 구축에 최적화된 형태로 변환합니다.

### 총 코드량
- 약 **4,008줄**의 python3 코드

---

## 2. 프로젝트 구조

```
NRRTV/
├── .env                              # 환경 설정 파일
├── .claude/                          # Claude 설정
├── backup/                           # 백업 디렉토리
├── data/                             # 데이터 저장소
│   ├── raw/                          # 원본 데이터
│   └── processed/                    # 처리된 데이터
├── script/                           # 스크립트 (비어있음)
├── src/                              # 소스 코드
│   ├── agentic_rag/                  # RAG 에이전트 모듈 (미구현)
│   ├── chunker/                      # 청킹 모듈 (미구현)
│   ├── collectors/                   # 데이터 수집 모듈
│   │   ├── __init__.py
│   │   ├── base.py                   # 기본 수집기 (추상 클래스)
│   │   ├── news_collector.py         # 뉴스 수집기
│   │   ├── pdf_collector.py          # PDF/리포트 수집기
│   │   ├── financial_report_collector.py  # 재무보고서 수집기
│   │   ├── collect_stock_report.py   # 주식 보고서 수집 스크립트
│   │   └── collect_financial_report.py    # 재무 보고서 수집 스크립트
│   └── extractor/                    # 데이터 추출 모듈
│       ├── extract_financial_report_table_formatted.py   # 테이블 추출 (포맷됨)
│       └── extract_financial_report_table_flattened.py   # 테이블 추출 (평탄화)
└── test/                             # 테스트 디렉토리 (비어있음)
```

---

## 3. 기술 스택

| 분류 | 기술 |
|------|------|
| **언어** | python3 3.9.6 |
| **HTTP 클라이언트** | `httpx`, `requests` |
| **HTML/XML 파싱** | `BeautifulSoup` |
| **데이터 처리** | `zipfile`, `io` |
| **데이터베이스** | SQLAlchemy (참조됨) |

### 외부 API
- **DART Open API**: 재무보고서 수집
- **Naver Finance**: 뉴스 및 리포트 스크래핑
- **Tavily API**: 검색 기능

---

## 4. 핵심 모듈 상세

### 4.1 Collectors (데이터 수집)

#### BaseCollector (`base.py` - 340줄)
모든 수집기의 부모 클래스

**주요 기능:**
- HTTP 요청 기반 Rate Limiting (설정 가능한 초당 요청 수)
- 재시도 로직 (Exponential Backoff, 최대 3회)
- 429/503 응답 특수 처리
- 중복 검사 (URL/Checksum 기반)
- 파일 저장 및 Document 레코드 생성
- Context Manager 지원

```python3
with NewsCollector() as collector:
    docs = collector.collect()
```

---

#### NewsCollector (`news_collector.py` - 443줄)
금융 뉴스 수집기

**데이터 소스:** Naver Finance 뉴스 섹션

**추출 정보:**
- 기사 제목, 날짜, 요약
- 저자, 출처
- 종목별 뉴스 필터링

**특징:**
- CSS Selector 기반 HTML 파싱
- 페이지네이션 지원

---

#### PDFCollector (`pdf_collector.py` - 369줄)
리서치 리포트 PDF 수집기

**데이터 소스:** Naver Finance 리서치 리포트

**추출 정보:**
- 회사명, 종목코드
- 애널리스트, 발간사
- 리포트 제목, 날짜

**파일명 규칙:**
```
{종목코드}_{회사명}_{발간사}_{제목}_{타임스탬프}.pdf
```

---

#### FinancialReportCollector (`financial_report_collector.py` - 587줄)
DART 재무보고서 수집기

**데이터 소스:** DART Open API

**보고서 유형:**
| 타입 | 설명 |
|------|------|
| `ANNUAL` | 사업보고서 |
| `SEMI_ANNUAL` | 반기보고서 |
| `QUARTERLY` | 분기보고서 |

**특징:**
- ZIP 응답에서 XML 파일 추출
- EUC-KR, UTF-8 인코딩 자동 처리
- 날짜 범위 기반 수집

---

### 4.2 수집 스크립트

#### collect_financial_report.py (778줄)
주식별 재무보고서 일괄 수집

```bash
python3 src/collectors/collect_financial_report.py \
    --stock 005930 --start 2020 --end 2025
```

**기능:**
- DART 기업코드 자동 조회
- 중복 제거 (동일 연도/분기의 최신 보고서만 보관)
- 구버전 파일 → `duplicates/` 폴더 자동 이동
- Dry-run 모드 지원

---

#### collect_stock_report.py (149줄)
티커별 리포트 수집

```bash
python3 src/collectors/collect_stock_report.py \
    --ticker 005930 --limit 10
```

---

### 4.3 Extractor (데이터 추출)

#### extract_financial_report_table_formatted.py (542줄)
RAG/KG 구축 최적화 포맷으로 추출

**처리 방식:**
- `<P>` 태그: 텍스트 그대로 유지
- `<TABLE>` 태그: 구조화된 포맷으로 변환
- 테이블명 자동 추출
- 목차 자동 생성

```bash
python3 src/extractor/extract_financial_report_table_formatted.py \
    input.xml output_dir
```

---

#### extract_financial_report_table_flattened.py (800줄)
RAG 청킹 최적화 평탄화 추출

**청킹 설정:**
| 파라미터 | 값 |
|----------|-----|
| 청크 크기 | 500자 |
| 오버랩 | 50자 |
| 최소 청크 | 100자 |

**테이블 평탄화 예시:**
```
입력:
| 구분 | 영업수익 | 영업비용 |
| 2024년 | 100억 | 50억 |

출력:
"영업수익 2024년 100억, 영업비용 2024년 50억"
```

```bash
python3 src/extractor/extract_financial_report_table_flattened.py \
    input.xml output_dir
```

---

## 5. 데이터 흐름

### 5.1 수집 파이프라인

```
┌─────────────────────────────────────────────────────────────┐
│                    외부 데이터 소스                          │
│  (DART API, Naver Finance)                                  │
└─────────────────────┬───────────────────────────────────────┘
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              BaseCollector                                   │
│  (Rate Limiting, Retry 로직, 중복 검사)                      │
└─────────────────────┬───────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
┌───────────┐  ┌───────────┐  ┌─────────────────┐
│   News    │  │    PDF    │  │    Financial    │
│ Collector │  │ Collector │  │ ReportCollector │
└─────┬─────┘  └─────┬─────┘  └────────┬────────┘
      │              │                 │
      ▼              ▼                 ▼
┌───────────┐  ┌───────────┐  ┌───────────────┐
│ HTML 파일  │  │ PDF 파일   │  │   XML 파일     │
└─────┬─────┘  └─────┬─────┘  └───────┬───────┘
      │              │                 │
      └──────────────┴─────────────────┘
                      │
                      ▼
              ┌─────────────┐
              │  data/raw/  │
              └─────────────┘
```

### 5.2 추출 파이프라인

```
┌────────────────────────────────────────────────────┐
│                XML 재무보고서 파일                  │
└─────────────────────┬──────────────────────────────┘
                      ▼
┌────────────────────────────────────────────────────┐
│          BeautifulSoup (XML 파싱)                   │
└─────────────────────┬──────────────────────────────┘
                      ▼
┌────────────────────────────────────────────────────┐
│           SECTION 계층 구조 처리                    │
│  ├─ <P> 태그 → 텍스트 그대로                        │
│  └─ <TABLE> 태그 → 평탄화/포맷팅                    │
└─────────────────────┬──────────────────────────────┘
                      │
        ┌─────────────┴─────────────┐
        ▼                           ▼
┌─────────────────┐       ┌─────────────────┐
│    Formatted    │       │    Flattened    │
│  (구조화 포맷)   │       │ (청크 기반 평탄화)│
└────────┬────────┘       └────────┬────────┘
         │                         │
         └────────────┬────────────┘
                      ▼
              ┌───────────────┐
              │data/processed/│
              └───────────────┘
```

---

## 6. 설정

### 6.1 환경 변수 (.env)

```env
# 데이터베이스
DATABASE_URL=sqlite:///data/narrative_insight.db

# Neo4j (그래프 DB)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password_here

# API 키
DART_API_KEY=your_dart_api_key
NAVER_CLIENT_ID=your_naver_client_id
NAVER_CLIENT_SECRET=your_naver_client_secret
TAVILY_API_KEY=your_tavily_api_key

# 로깅
LOG_LEVEL=INFO
LOG_TO_FILE=true

# 파이프라인 설정
MAX_RETRIES=3
RETRY_DELAY=60

# 데이터 경로
RAW_DATA_DIR=data/raw
PROCESSED_DATA_DIR=data/processed

# 기능 플래그
ENABLE_OCR=true
ENABLE_SENTIMENT_ANALYSIS=true
ENABLE_KG_UPDATE=true

# 속도 제한
COLLECTOR_RATE_LIMIT=1.0
NEWS_RATE_LIMIT=2.0
```

---

## 7. 핵심 파일 요약

| 파일 | 행수 | 역할 |
|------|------|------|
| `base.py` | 340 | 모든 수집기의 부모 클래스 |
| `news_collector.py` | 443 | 뉴스 기사 수집 |
| `pdf_collector.py` | 369 | PDF 리포트 수집 |
| `financial_report_collector.py` | 587 | DART 재무보고서 수집 |
| `collect_financial_report.py` | 778 | 주식별 보고서 수집 스크립트 |
| `collect_stock_report.py` | 149 | 티커별 리포트 수집 스크립트 |
| `extract_financial_report_table_formatted.py` | 542 | 테이블 포맷 추출 |
| `extract_financial_report_table_flattened.py` | 800 | 테이블 평탄화 추출 |

---

## 8. 설계 패턴

### 8.1 추상 기본 클래스 패턴
```python3
class BaseCollector(ABC):
    @abstractmethod
    def collect(self) -> List[Document]:
        pass
```

### 8.2 Rate Limiting 전략
- 초당 요청 수 제한 (기본값: 1.0 req/sec)

### 8.3 Retry 전략
| 상황 | 대기 시간 |
|------|----------|
| 일반 에러 | 2^attempt 초 (Exponential Backoff) |
| 429 응답 | Retry-After 헤더 값 |
| 503 응답 | 30초 × (attempt + 1) |

### 8.4 중복 검사
- URL 기반 검사
- Checksum 기반 검사 (이진 파일)

---

## 9. 구현 상태

### 구현 완료
- [x] 기본 수집기 프레임워크 (BaseCollector)
- [x] 뉴스 수집기 (NewsCollector)
- [x] PDF 리포트 수집기 (PDFCollector)
- [x] DART 재무보고서 수집기 (FinancialReportCollector)
- [x] 재무제표 추출 모듈

### 미구현
- [ ] 데이터 모델 (Document, FinancialReport 클래스)
- [ ] 저장소 계층 (DocumentStore)
- [ ] 유틸리티 모듈 (config, logger, validators)
- [ ] agentic_rag 모듈
- [ ] chunker 모듈
- [ ] 테스트 코드
- [ ] Neo4j 그래프 DB 통합

---

## 10. 참조되는 미구현 모듈

코드에서 import 되지만 아직 구현되지 않은 모듈들:

```python3
# 데이터 모델
from src.models.document import Document, DocumentType
from src.models.financial_report import FinancialReport, ReportType, ReportPeriod

# 저장소
from src.storage.document_store import DocumentStore

# 유틸리티
from src.utils.config import get_config
from src.utils.logger import setup_logging, get_logger
from src.utils.validators import sanitize_filename, validate_url
```

---

## 11. 코드 품질

### 장점
- 명확한 모듈 분리 (수집, 추출, 저장)
- 포괄적인 에러 처리 및 재시도 로직
- Context Manager 패턴 사용
- 중복 검사 메커니즘
- Rate Limiting을 통한 책임있는 스크래핑

### 개선 필요
- 참조되는 많은 모듈 미구현
- requirements.txt 없음
- 테스트 코드 없음
- 설정 파일 (config.yaml) 없음

---

## 12. 향후 개발 필요사항

1. **저장소 계층 구현**
   - DocumentStore 클래스
   - SQLAlchemy 모델 정의

2. **유틸리티 모듈 구현**
   - config.py
   - logger.py
   - validators.py

3. **RAG 파이프라인 구축**
   - agentic_rag 모듈
   - chunker 모듈

4. **테스트 코드 작성**
   - 단위 테스트
   - 통합 테스트

5. **의존성 관리**
   - requirements.txt 생성
   - setup.py 또는 pyproject.toml

---

## 13. 사용 예시

### 13.1 특정 종목의 모든 재무보고서 일괄 추출 (테이블 평탄화)

특정 종목의 XML 재무보고서를 일괄로 처리하여 RAG용 청크 데이터로 변환하는 방법입니다.

**예시: 현대비앤지스틸 (004560)**

```bash
# 변수 설정
STOCK_CODE="004560"
STOCK_NAME="현대비앤지스틸"
INPUT_DIR="data/raw/financial_report/${STOCK_CODE}_${STOCK_NAME}"
OUTPUT_BASE="data/processed/financial_report/${STOCK_CODE}_${STOCK_NAME}"

# 출력 기본 디렉토리 생성
mkdir -p "$OUTPUT_BASE"

# 모든 XML 파일에 대해 일괄 처리
for xml_file in "$INPUT_DIR"/*.xml; do
    # 파일명 추출 (확장자 제외)
    filename=$(basename "$xml_file" .xml)

    # 출력 디렉토리 생성
    output_dir="${OUTPUT_BASE}/${filename}"
    mkdir -p "$output_dir"

    # 추출 실행
    echo "처리 중: $filename"
    python33 src/extractor/extract_financial_report_table_flattened.py "$xml_file" "$output_dir"
done

echo "완료!"
```

**출력 구조:**
```
data/processed/financial_report/004560_현대비앤지스틸/
├── 004560_현대비앤지스틸_annual_2020FY_20210317001107/
│   ├── 현대비앤지스틸_사업보고서_full_text.txt
│   ├── section_01_회사의 개요.txt
│   ├── section_02_사업의 내용.txt
│   ├── ...
│   └── index.txt            # 목차
├── 004560_현대비앤지스틸_quarterly_2020Q1_20200515001264/
│   └── ...
└── ...
```

**출력 파일 설명:**
| 파일 | 설명 |
|------|------|
| `*_full_text.txt` | 전체 문서 텍스트 (평탄화된 테이블 포함) |
| `section_*.txt` | SECTION-1 단위로 분리된 파일 |
| `index.txt` | 섹션별 파일 목록 |

### 13.2 텍스트 파일 RAG용 청킹 (chunker.py)

추출된 텍스트 파일을 RAG 시스템에서 사용할 수 있도록 청킹하는 프로그램입니다.

**위치:** `src/chunker/chunker.py`

#### 지원 전략

| 전략 | 설명 | 특징 |
|------|------|------|
| `recursive` | 재귀적 문자 분할 | **기본값**, 가장 범용적 (LangChain 권장) |
| `character` | 단순 문자 기반 | 줄바꿈 기준 분할 |
| `token` | 토큰 기반 | tiktoken 사용 (LLM 토큰 수 기준) |
| `sentence` | 문장 기반 | 문장 경계 유지 |
| `semantic` | 의미 기반 | OpenAI 임베딩 사용 (API 키 필요) |

#### 기본 사용법

```bash
# 단일 파일 처리 (기본: recursive 전략)
python33 src/chunker/chunker.py <입력_파일> <출력_디렉토리>

# 전략 및 파라미터 지정
python33 src/chunker/chunker.py <입력> <출력> --strategy <전략> --chunk_size <크기> --chunk_overlap <오버랩>

# 디렉토리 일괄 처리
python33 src/chunker/chunker.py <입력_디렉토리> <출력_디렉토리> --extension .txt
```

#### 파라미터

| 파라미터 | 단축 | 기본값 | 설명 |
|----------|------|--------|------|
| `--strategy` | `-s` | recursive | 청킹 전략 |
| `--chunk_size` | `-c` | 1000 | 청크 크기 (문자 수) |
| `--chunk_overlap` | `-o` | 200 | 청크 간 오버랩 |
| `--extension` | `-e` | .txt | 처리할 파일 확장자 |

#### 실제 사용 예시

```bash
# 현대비앤지스틸 사업보고서 청킹 (recursive 전략)
python33 src/chunker/chunker.py \
  "data/processed/financial_report/004560_현대비앤지스틸/004560_현대비앤지스틸_annual_2020FY_20210317001107/현대비앤지스틸_사업보고서_full_text.txt" \
  "data/processed/financial_report/004560_현대비앤지스틸/004560_현대비앤지스틸_annual_2020FY_20210317001107/chunks"

# 문장 기반 청킹 (청크 크기 1500)
python33 src/chunker/chunker.py \
  "data/processed/financial_report/004560_현대비앤지스틸/004560_현대비앤지스틸_annual_2020FY_20210317001107/현대비앤지스틸_사업보고서_full_text.txt" \
  "data/processed/financial_report/004560_현대비앤지스틸/004560_현대비앤지스틸_annual_2020FY_20210317001107/chunks" \
  --strategy sentence --chunk_size 1500
```

#### 출력 파일

| 파일 | 설명 |
|------|------|
| `*_chunks.json` | 구조화된 JSON 형식 (메타데이터 포함) |
| `*_chunks.txt` | 사람이 읽기 쉬운 텍스트 형식 |

#### JSON 출력 구조

```json
{
  "metadata": {
    "created_at": "2026-01-29T22:18:25",
    "total_chunks": 143,
    "total_chars": 117277
  },
  "chunks": [
    {
      "id": 0,
      "content": "청크 내용...",
      "metadata": {
        "source_file": "파일명.txt",
        "source_path": "전체 경로",
        "strategy": "recursive",
        "chunk_index": 0
      },
      "char_count": 971
    }
  ]
}
```

#### 의존성

```bash
# 기본 (recursive, character, sentence)
pip install langchain-text-splitters

# 토큰 기반 청킹
pip install tiktoken

# 의미 기반 청킹 (semantic)
pip install langchain-experimental langchain-openai
export OPENAI_API_KEY="your-api-key"
```

---

*작성일: 2026-01-29*
*분석자: Claude Code*

---

---

# 텔레그램 금융 채널 수집 시스템

> 추가일: 2026-04-06

---

## 개요

텔레그램 공개 채널(`t.me/s/{username}`)을 자동으로 탐색해 한국 주식/금융 관련 채널을 발견하고, PostgreSQL DB에 저장한 뒤 과거 메시지를 자동 수집하는 파이프라인.

```
[시드 채널 등록]  ← 처음 한 번만
      ↓
[discover_telegram_channels.py]  ← 1시간마다 cron 실행
      ↓  (새 채널 발견 시 자동)
[manage_telegram_channels.py backfill-missing]
      ↓
[DB: telegram_channels + telegram_messages]
```

---

## 사전 준비

`.env` 파일에 PostgreSQL DB URL이 있어야 한다.

```
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
```

`TelegramStore` 초기화 시 테이블이 없으면 자동 생성된다. 별도 마이그레이션 불필요.

---

## 처음 한 번만: 시드 채널 등록

채널 발견 에이전트가 탐색을 시작할 출발점이 필요하다. 텔레그램 채널 username (@ 없이) 을 입력한다.

```bash
# 예: @AnalystCrawler → AnalystCrawler
python3 script/manage_telegram_channels.py add AnalystCrawler --category 종목리서치

# 카테고리 목록: 주식실황 | 종목리서치 | 매크로 | IPO | 기타
```

등록 후 메타데이터(채널명, 설명, 구독자 수) 자동 조회 및 업데이트:

```bash
python3 script/manage_telegram_channels.py fetch-meta AnalystCrawler
```

---

## 자동 채널 발견: discover_telegram_channels.py

### 동작 방식

1. DB에 등록된 채널 + 시드 채널의 최신 메시지 1페이지를 수집
2. 메시지 본문 / HTML 링크에서 `@mention`, `t.me/` 패턴 추출
3. 아직 DB에 없는 후보 채널의 메타데이터를 조회
4. 한국 금융 키워드 관련성 점수(0~100) 계산 → 임계치 이상이면 DB 등록
5. 새 채널이 추가되었으면 `backfill-missing` 자동 실행

### 기본 실행

```bash
python3 script/discover_telegram_channels.py
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--dry-run` | — | DB 쓰기 없이 발견 결과만 출력 |
| `--max-new N` | 10 | 이번 실행에서 추가할 최대 채널 수 |
| `--max-candidates N` | 50 | 평가할 최대 후보 수 |
| `--min-score N` | 20 | 관련성 최소 점수 (0~100) |
| `--seed CHANNEL` | — | 탐색 출발 채널 추가 (여러 번 사용 가능) |
| `--log-level` | INFO | DEBUG / INFO / WARNING / ERROR |

### 예시

```bash
# 드라이런 — 어떤 채널이 발견될지 미리 확인
python3 script/discover_telegram_channels.py --dry-run

# 시드 채널 지정해서 실행
python3 script/discover_telegram_channels.py --seed AnalystCrawler --min-score 5
python3 script/discover_telegram_channels.py --seed koreastocknews --seed koreainvest

# 최대 5개만 추가, 점수 30 이상만 수락
python3 script/discover_telegram_channels.py --max-new 5 --min-score 30
```

### cron 설정 (1시간마다 자동 실행)

```bash
crontab -e
```

아래 줄 추가 (경로는 실제 환경에 맞게 수정):

```
0 * * * * cd /Users/koscom/Desktop/dev/NRRTV && python3 script/discover_telegram_channels.py >> logs/discovery.log 2>&1
```

---

## 원스텟 등록 + 백필: add_and_backfill.py

채널을 처음 추가할 때 가장 편리한 방법. **등록 → 메타데이터 조회 → 과거 메시지 백필**을 한 번에 실행한다.

### 지원 입력 형식

```bash
python33 script/add_and_backfill.py @KISemicon
python33 script/add_and_backfill.py KISemicon
python33 script/add_and_backfill.py https://t.me/KISemicon
python33 script/add_and_backfill.py t.me/s/KISemicon
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--since YYYY-MM-DD` | 2025-01-01 | 백필 하한 날짜 |
| `--category` | — | 카테고리 지정 (목록 아래 참조) |
| `--pages N` | 9999 | 최대 페이지 수 (1페이지 ≈ 20개) |
| `--skip-backfill` | — | 등록 + 메타 조회만 하고 백필 생략 |
| `--fetch-articles` | — | 메시지 내 기사 링크 본문도 수집 |

### 예시

```bash
# 기본 (2025-01-01까지 백필)
python33 script/add_and_backfill.py @KISemicon

# 카테고리 지정
python33 script/add_and_backfill.py @KISemicon --category 종목리서치

# 특정 날짜까지만
python33 script/add_and_backfill.py @KISemicon --since 2024-06-01

# 등록 + 메타만 (백필 생략)
python33 script/add_and_backfill.py @KISemicon --skip-backfill

# 기사 본문도 함께 수집 (속도 느려짐 주의)
python33 script/add_and_backfill.py @KISemicon --fetch-articles
```

### 이미 등록된 채널이면?

중복 등록 오류 없이 **기존 채널을 그대로 사용**하고, 메타데이터 업데이트 + 백필만 이어서 실행한다.

---

## 채널 관리: manage_telegram_channels.py

### 채널 목록 확인

```bash
python3 script/manage_telegram_channels.py list
python3 script/manage_telegram_channels.py list --category 주식실황
python3 script/manage_telegram_channels.py list --min-subs 5000
python3 script/manage_telegram_channels.py list --all   # 비활성 포함
```

### 채널 수동 등록

```bash
python3 script/manage_telegram_channels.py add <username> \
  --category 종목리서치 \
  --name "채널 표시명" \
  --characteristics "특징 설명"
```

### 채널 정보 수정

```bash
python3 script/manage_telegram_channels.py update <username> --category 매크로
```

### 채널 비활성화

```bash
python3 script/manage_telegram_channels.py deactivate <username>
```

### 메시지 수동 수집 (테스트)

```bash
# 최신 메시지 3페이지 수집 (1페이지 ≈ 20개)
python3 script/manage_telegram_channels.py scrape <username> --pages 3

# 기사 본문도 함께 수집
python3 script/manage_telegram_channels.py scrape <username> --fetch-articles
```

### 과거 메시지 백필 (단일 채널)

```bash
# 2025-01-01 이후 메시지 전체 백필
python3 script/manage_telegram_channels.py backfill <username> --since-date 2025-01-01

# 최대 100페이지만
python3 script/manage_telegram_channels.py backfill <username> --pages 100
```

### 백필 누락 채널 일괄 처리

```bash
# 2025-01-01 이전까지 백필이 안 된 채널 자동 감지 + 백필
python3 script/manage_telegram_channels.py backfill-missing

# 날짜 기준 변경
python3 script/manage_telegram_channels.py backfill-missing --since 2024-06-01

# 드라이런 (실제 수집 없이 필요 채널 목록만 출력)
python3 script/manage_telegram_channels.py backfill-missing --dry-run

# 특정 카테고리만
python3 script/manage_telegram_channels.py backfill-missing --category 주식실황
```

### 전체 통계

```bash
python3 script/manage_telegram_channels.py stats
```

---

## 카테고리

| 카테고리 | 설명 |
|----------|------|
| `주식실황` | 실시간 호가, 세력, 단타, 급등/급락 |
| `종목리서치` | 리서치, 기업분석, 가치투자, 퀀트 |
| `매크로` | 금리, 환율, 경기, 글로벌 시황 |
| `IPO` | 공모주, 청약, 스팩 |
| `기타` | 위 분류에 해당 없는 금융 채널 |

---

## 주요 파일 위치

| 파일 | 역할 |
|------|------|
| [script/discover_telegram_channels.py](script/discover_telegram_channels.py) | 채널 자동 발견 CLI |
| [script/manage_telegram_channels.py](script/manage_telegram_channels.py) | 채널/메시지 수동 관리 CLI |
| [src/collectors/telegram/channel_discovery.py](src/collectors/telegram/channel_discovery.py) | 발견 에이전트 핵심 로직 |
| [src/collectors/telegram/telegram_collector.py](src/collectors/telegram/telegram_collector.py) | HTTP 수집기 (속도제한 내장) |
| [src/storage/telegram_store.py](src/storage/telegram_store.py) | DB CRUD (SQLAlchemy ORM) |
| `logs/discovery.log` | 채널 발견 실행 로그 |

---

## DB 테이블 구조

### `telegram_channels`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `id` | int PK | |
| `username` | varchar UNIQUE | @ 없는 채널명 |
| `channel_name` | varchar | 채널 표시명 |
| `channel_url` | text | `https://t.me/s/{username}` |
| `description` | text | 채널 소개글 |
| `category` | varchar | 카테고리 |
| `is_active` | bool | 수집 활성 여부 |
| `subscriber_count` | int | 구독자 수 |
| `last_scraped_at` | datetime | 마지막 수집 시각 |

### `telegram_messages`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `id` | int PK | |
| `channel_id` | int FK | `telegram_channels.id` |
| `telegram_msg_id` | int | 텔레그램 원본 메시지 ID |
| `content` | text | 메시지 텍스트 |
| `posted_at` | datetime | 게시 시각 |
| `views` | int | 조회 수 |
| `has_media` | bool | 미디어 포함 여부 |
| `linked_article_text` | text | 링크된 기사 본문 (`--fetch-articles` 시) |

---

## 기사 본문 수집 기능 (--fetch-articles)

메시지에 외부 뉴스 링크가 포함된 경우, 해당 기사에 접속해 본문을 `linked_article_text` 컬럼에 함께 저장한다.

### 지원 커맨드

`scrape`, `backfill`, `backfill-missing`, `add_and_backfill.py` 모두 `--fetch-articles` 플래그 지원.

```bash
python33 script/manage_telegram_channels.py scrape <username> --fetch-articles
python33 script/manage_telegram_channels.py backfill <username> --since-date 2025-01-01 --fetch-articles
python33 script/manage_telegram_channels.py backfill-missing --fetch-articles
python33 script/add_and_backfill.py @KISemicon --fetch-articles
```

### 자동 스킵 규칙

다음은 기사 수집 대상에서 제외된다:

| 스킵 대상 | 이유 |
|----------|------|
| t.me, telegram.me | 텔레그램 내부 링크 |
| youtube.com, youtu.be | 동영상 |
| twitter.com, x.com | SNS |
| instagram.com, facebook.com, tiktok.com | SNS |
| linkedin.com, reddit.com | SNS |
| naver.blog, blog.naver.com | 블로그 |
| .jpg .png .gif .mp4 .pdf .zip 등 | 미디어/파일 링크 |

### 주의사항

- 기사당 최소 1초 대기 → **속도가 상당히 느려진다**
- 백필 시에는 기본적으로 꺼놓고, 필요할 때만 사용 권장
- `trafilatura` + `lxml_html_clean` 패키지 필요:
  ```bash
  pip3 install trafilatura lxml_html_clean
  ```

---

## 안전 설계 (블락 방지)

- 요청 간 **5~10초 랜덤 대기** (TelegramCollector 내장)
- HTTP 429 응답 시 **60초 자동 대기 후 재시도**
- 1회 실행당 최대 추가 채널 수 제한 (`--max-new`, 기본 10)
- 최대 평가 후보 수 제한 (`--max-candidates`, 기본 50)
- 채널 발견 시 1페이지만 스캔 (메시지 ≈ 20개)

---

## 현재 등록된 채널 (2026-04-06 기준)

| ID | Username | 채널명 | 구독자 |
|----|----------|--------|--------|
| 8 | @growthresearch | 그로쓰리서치(Growth Research) | 36,100 |
| 7 | @Samsung_Global_AI_SW | [삼성 이영진] 글로벌 AI/SW | 7,660 |
| 6 | @KISemicon | [한투 반도체] 채민숙/남채민/황준태/김연준 | 12,100 |
| 5 | @valueir | 밸류아이알(Value IR) 채널 | 3,060 |
| 4 | @kiwoom_semibat | 키움 반도체,이차전지 PRIME☀️ | 10,900 |
| 3 | @aetherjapanresearch | 에테르의 일본&미국 리서치 | 25,600 |
| 2 | @free_life59 | 프리라이프 | 8,039 |

모든 채널은 2025-01-01 기준 백필 완료.

---

*텔레그램 수집 섹션 최종 업데이트: 2026-04-06*

---

# Naver Finance 뉴스 수집 시스템

> 추가일: 2026-04-08

---

## 개요

Naver Finance 뉴스를 수집하여 `news_articles` 테이블에 DB 직접 저장합니다.
HTML 파일은 저장하지 않습니다.

**realtime**: 폴링 데몬 — 지정 간격(기본 300초)마다 반복 수집. 각 사이클에서 첫 중복 URL 발견 시 해당 사이클 종료 후 대기. Ctrl+C 또는 SIGTERM으로 종료.
**backfill**: 날짜 범위 지정 과거 수집 — `article_date < from_date` 도달 시 정지 (1회 실행)

---

## 사전 준비

`.env` 파일에 DB URL 설정:

```
DATABASE_URL=sqlite:///data/narrative_insight.db
# 또는 PostgreSQL
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
```

`NewsStore` 초기화 시 `news_articles` 테이블이 없으면 자동 생성됩니다.

---

## 실행 방법 (script/collect_news.py)

```bash
# realtime - 폴링 데몬 (300초 간격, Ctrl+C로 종료)
python3 script/collect_news.py --mode realtime

# realtime - 간격 지정 (60초마다)
python3 script/collect_news.py --mode realtime --interval 60

# realtime - 종목별 폴링
python3 script/collect_news.py --mode realtime --ticker 005930 --interval 120

# backfill - 날짜 범위 지정
python3 script/collect_news.py --mode backfill --from-date 2026-01-01 --to-date 2026-04-07

# backfill - 최대 페이지 지정
python3 script/collect_news.py --mode backfill --from-date 2026-01-01 --max-pages 100

# dry-run - 수집 계획 + DB 현황만 출력 (실제 수집 없음)
python3 script/collect_news.py --mode backfill --from-date 2026-01-01 --dry-run
```

### 옵션 목록

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--mode` | realtime | realtime / backfill |
| `--ticker` | — | 종목코드 (예: 005930). 미지정 시 전체 뉴스 |
| `--interval N` | 300 | realtime 폴링 간격 (초). realtime 전용 |
| `--limit N` | 50 (realtime) | 최대 수집 기사 수 (1사이클당) |
| `--max-pages N` | 20 (realtime) / 50 (backfill) | 최대 페이지 수 |
| `--from-date` | — | backfill 시작 날짜 (필수) |
| `--to-date` | 오늘 | backfill 종료 날짜 |
| `--dry-run` | — | 계획 출력만, 수집 없음 |
| `--log-level` | INFO | DEBUG / INFO / WARNING |

---

## 외부 Python에서 직접 호출

```python
import sys
sys.path.insert(0, "/path/to/NRRTV")
from datetime import datetime
from src.collectors.news import NaverFinanceNewsCollector

with NaverFinanceNewsCollector() as collector:
    # realtime
    articles = collector.collect_realtime(limit=10, ticker="005930")

    # backfill
    articles = collector.collect_backfill(
        from_date=datetime(2026, 1, 1),
        to_date=datetime(2026, 4, 7),
        max_pages=50,
    )
```

---

## stop 조건 차이

| 모드 | stop 조건 |
|------|-----------|
| realtime | 폴링 루프 (무한 반복). 각 사이클에서 첫 중복 URL 발견 시 해당 사이클 종료 → interval 대기 → 다시 수집 |
| backfill | `article_date < from_date` 도달 시 중단. 중복 URL은 skip하되 중단 안 함 (gap-fill) |

---

## DB 통계 확인

```python
from src.storage.news_store import NewsStore
store = NewsStore()
print(store.get_stats())
# {'total_articles': N, 'latest_published_at': ..., 'oldest_published_at': ...}

# 종목별 최신 기사 목록
articles = store.list_articles(limit=10, ticker="005930")
```

---

## 주요 파일 위치

| 파일 | 역할 |
|------|------|
| [script/collect_news.py](script/collect_news.py) | 뉴스 수집 CLI |
| [src/collectors/news/news_collector.py](src/collectors/news/news_collector.py) | 수집기 핵심 로직 |
| [src/storage/news_store.py](src/storage/news_store.py) | DB CRUD (SQLAlchemy ORM) |
| [config/sources.yaml](config/sources.yaml) | CSS 셀렉터 등 소스 설정 |

---

*뉴스 수집 섹션 최종 업데이트: 2026-04-08*

---

# 리서치 리포트 PDF 텍스트 수집 시스템

> 추가일: 2026-04-11

---

## 개요

`data/raw/research_report/` 디렉토리의 PDF 파일에서 텍스트를 추출하여 `research_reports` 테이블에 저장합니다.
파일명 패턴 `{YYYYMMDD}_{ticker}_{회사}_{증권사}_{제목}_{id}.pdf` 에서 메타데이터를 자동 파싱합니다.

**backfill**: 지정 디렉토리의 전체 PDF 일괄 처리  
**realtime**: 오늘(또는 최근 N일) 날짜 파일만 처리

---

## 사전 준비

```bash
pip3 install pdfplumber
```

`.env` 파일에 DB URL 설정:
```
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
```

---

## 실행 방법 (script/collect_research_reports.py)

```bash
# backfill - 전체 PDF 처리
python3 script/collect_research_reports.py --mode backfill

# backfill - 5개만 테스트
python3 script/collect_research_reports.py --mode backfill --limit 5

# realtime - 오늘 날짜 파일만 처리
python3 script/collect_research_reports.py --mode realtime

# realtime - 최근 3일치
python3 script/collect_research_reports.py --mode realtime --days 3

# 특정 디렉토리 지정
python3 script/collect_research_reports.py --mode backfill --pdf-dir /path/to/pdfs

# 이미 저장된 파일도 재처리
python3 script/collect_research_reports.py --mode backfill --no-skip
```

### 옵션 목록

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--mode` | (필수) | backfill / realtime |
| `--pdf-dir` | data/raw/research_report | PDF 파일 디렉토리 |
| `--limit N` | — | 처리할 최대 파일 수 (backfill 전용) |
| `--days N` | 1 | 최근 N일 파일 처리 (realtime 전용) |
| `--no-skip` | — | 중복 파일도 재처리 |

---

## 외부 Python에서 직접 호출

```python
import sys
sys.path.insert(0, "/path/to/NRRTV")
from src.collectors.research_report.report_collector import ResearchReportCollector

collector = ResearchReportCollector(pdf_dir="data/raw/research_report")
stats = collector.collect(limit=5)
# {'total': 5, 'saved': 5, 'skipped': 0, 'error': 0}
```

---

## DB 저장 구조

PDF 1개당 2개의 레코드 생성:
- `documents` 테이블: 파일 경로, 체크섬, 처리 상태
- `research_reports` 테이블: 티커, 증권사, 날짜, **content (추출 텍스트)**

중복 방지: 파일 SHA256 체크섬 기반 (`--no-skip` 없으면 재처리 안 함)

---

## 주요 파일 위치

| 파일 | 역할 |
|------|------|
| [script/collect_research_reports.py](script/collect_research_reports.py) | 수집 CLI |
| [src/collectors/research_report/report_collector.py](src/collectors/research_report/report_collector.py) | PDF 추출 + DB 저장 핵심 로직 |
| [src/storage/document_store.py](src/storage/document_store.py) | ResearchReportORM (content 컬럼 포함) |

---

*리서치 리포트 수집 섹션 최종 업데이트: 2026-04-11*
