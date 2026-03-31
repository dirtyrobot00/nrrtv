# NRRTV 프로젝트 분석 문서

> 한국 금융 데이터 수집 및 처리 시스템

---

## 1. 프로젝트 개요

NRRTV는 **한국 금융 데이터를 수집하고 처리하는 파이프라인 시스템**입니다. DART API, Naver Finance 등에서 뉴스, 리서치 리포트, 재무보고서를 수집하고 RAG/Knowledge Graph 구축에 최적화된 형태로 변환합니다.

### 총 코드량
- 약 **4,008줄**의 Python 코드

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
| **언어** | Python 3.9.6 |
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

```python
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
python src/collectors/collect_financial_report.py \
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
python src/collectors/collect_stock_report.py \
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
python src/extractor/extract_financial_report_table_formatted.py \
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
python src/extractor/extract_financial_report_table_flattened.py \
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
```python
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

```python
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
    python3 src/extractor/extract_financial_report_table_flattened.py "$xml_file" "$output_dir"
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
python3 src/chunker/chunker.py <입력_파일> <출력_디렉토리>

# 전략 및 파라미터 지정
python3 src/chunker/chunker.py <입력> <출력> --strategy <전략> --chunk_size <크기> --chunk_overlap <오버랩>

# 디렉토리 일괄 처리
python3 src/chunker/chunker.py <입력_디렉토리> <출력_디렉토리> --extension .txt
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
python3 src/chunker/chunker.py \
  "data/processed/financial_report/004560_현대비앤지스틸/004560_현대비앤지스틸_annual_2020FY_20210317001107/현대비앤지스틸_사업보고서_full_text.txt" \
  "data/processed/financial_report/004560_현대비앤지스틸/004560_현대비앤지스틸_annual_2020FY_20210317001107/chunks"

# 문장 기반 청킹 (청크 크기 1500)
python3 src/chunker/chunker.py \
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
