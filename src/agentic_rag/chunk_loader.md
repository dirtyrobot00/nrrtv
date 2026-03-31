# ChromaDB Chunk Loader

청킹된 JSON 파일을 ChromaDB 벡터 데이터베이스에 적재하는 프로그램입니다.

## 개요

- **목적**: RAG(Retrieval-Augmented Generation) 시스템을 위한 벡터 DB 적재
- **특징**: 청크 전략별로 다른 컬렉션에 저장하여 성능 비교 테스트 가능
- **입력**: `chunker.py`에서 생성한 `*_chunks.json` 파일

## 설치

```bash
# 필수
pip install chromadb

# 한국어 임베딩 사용 시
pip install sentence-transformers

# OpenAI 임베딩 사용 시
pip install openai
export OPENAI_API_KEY="your-api-key"
```

## 사용법

### 기본 명령어

```bash
# 적재
python chunk_loader.py load <입력_파일> --collection <컬렉션명>

# 컬렉션 목록
python chunk_loader.py list

# 컬렉션 정보
python chunk_loader.py info <컬렉션명>

# 검색 테스트
python chunk_loader.py query <컬렉션명> "검색어"

# 컬렉션 삭제
python chunk_loader.py delete <컬렉션명>
```

### 적재 (load)

#### 단일 파일 적재

```bash
# 기본 적재 (컬렉션명 직접 지정)
python chunk_loader.py load chunks.json --collection my_collection

# 종목 정보로 컬렉션명 자동 생성
# 결과: 004560_token (전략명은 JSON에서 자동 추출)
python chunk_loader.py load token_chunks.json --stock-code 004560

# 전체 정보로 컬렉션명 생성
# 결과: 004560_hyundai_bng_steel_token
python chunk_loader.py load token_chunks.json \
  --stock-code 004560 \
  --stock-name 현대비앤지스틸
```

#### 디렉토리 일괄 적재

```bash
# 디렉토리 내 모든 *_chunks.json 파일 처리
python chunk_loader.py load ./chunks/ --stock-code 004560
```

#### 파라미터

| 파라미터 | 단축 | 기본값 | 설명 |
|----------|------|--------|------|
| `--collection` | `-c` | - | 컬렉션 이름 (직접 지정) |
| `--stock-code` | - | - | 종목코드 (컬렉션명 자동 생성) |
| `--stock-name` | - | - | 종목명 (컬렉션명 자동 생성) |
| `--db-path` | - | ./chroma_db | ChromaDB 저장 경로 |
| `--embedding` | `-e` | default | 임베딩 모델 |
| `--batch-size` | `-b` | 100 | 배치 크기 |
| `--overwrite` | - | false | 기존 컬렉션 덮어쓰기 |

### 임베딩 모델

| 값 | 모델 | 설명 |
|----|------|------|
| `default` | all-MiniLM-L6-v2 | ChromaDB 기본, 영어 최적화 |
| `korean` | jhgan/ko-sbert-nli | 한국어 특화 |
| `openai` | text-embedding-ada-002 | OpenAI API (유료) |

```bash
# 한국어 임베딩 사용
python chunk_loader.py load chunks.json -c my_col --embedding korean

# OpenAI 임베딩 사용
export OPENAI_API_KEY="sk-..."
python chunk_loader.py load chunks.json -c my_col --embedding openai
```

### 검색 테스트 (query)

```bash
# 기본 검색 (상위 5개)
python chunk_loader.py query my_collection "매출액 현황"

# 결과 수 지정
python chunk_loader.py query my_collection "사업 내용" -n 10

# 한국어 임베딩으로 검색
python chunk_loader.py query my_collection "매출액" --embedding korean
```

## 전략별 성능 테스트 워크플로우

### 1단계: 여러 전략으로 청킹

```bash
# 입력 파일
INPUT="data/processed/.../full_text.txt"
OUTPUT="data/processed/.../chunks"

# recursive 전략
python src/chunker/chunker.py "$INPUT" "$OUTPUT" -s recursive

# token 전략
python src/chunker/chunker.py "$INPUT" "$OUTPUT" -s token -c 768 -o 77

# sentence 전략
python src/chunker/chunker.py "$INPUT" "$OUTPUT" -s sentence
```

### 2단계: 각 전략별 컬렉션 생성

```bash
cd src/agentic_rag
CHUNKS_DIR="../../data/processed/.../chunks"

# recursive 전략 적재
python chunk_loader.py load "$CHUNKS_DIR/*_recursive_*.json" \
  --stock-code 004560 --embedding korean

# token 전략 적재
python chunk_loader.py load "$CHUNKS_DIR/*_token_*.json" \
  --stock-code 004560 --embedding korean

# sentence 전략 적재
python chunk_loader.py load "$CHUNKS_DIR/*_sentence_*.json" \
  --stock-code 004560 --embedding korean
```

### 3단계: 검색 성능 비교

```bash
QUERY="현대비앤지스틸 주요 사업 내용"

# 각 컬렉션에서 동일 쿼리로 검색
python chunk_loader.py query 004560_recursive "$QUERY" --embedding korean
python chunk_loader.py query 004560_token "$QUERY" --embedding korean
python chunk_loader.py query 004560_sentence "$QUERY" --embedding korean
```

## 데이터 구조

### 입력 JSON (chunker.py 출력)

```json
{
  "metadata": {
    "created_at": "2026-01-29T22:50:14",
    "total_chunks": 131,
    "total_chars": 116006
  },
  "chunks": [
    {
      "id": 0,
      "content": "청크 내용...",
      "metadata": {
        "source_file": "파일명.txt",
        "source_path": "전체 경로",
        "strategy": "token",
        "chunk_index": 0
      },
      "char_count": 847
    }
  ]
}
```

### ChromaDB 메타데이터

적재 시 각 문서에 저장되는 메타데이터:

| 필드 | 설명 |
|------|------|
| `source_file` | 원본 파일명 |
| `source_path` | 원본 파일 전체 경로 |
| `strategy` | 청킹 전략 |
| `chunk_index` | 청크 순서 |
| `char_count` | 문자 수 |
| `loaded_at` | 적재 일시 |

### 컬렉션 네이밍 규칙

자동 생성 형식: `{stock_code}_{stock_name}_{strategy}`

예시:
- `004560_token` (종목코드 + 전략)
- `004560_hyundai_bng_steel_recursive` (종목코드 + 종목명 + 전략)

## 실제 사용 예시

### 현대비앤지스틸 사업보고서 적재

```bash
cd /Users/koscom/Desktop/dev/NRRTV/src/agentic_rag

# token 전략 청크 적재
python chunk_loader.py load \
  "../../data/processed/financial_report/004560_현대비앤지스틸/004560_현대비앤지스틸_annual_2020FY_20210317001107/chunks/현대비앤지스틸_사업보고서_full_text_token_chunks.json" \
  --stock-code 004560 \
  --stock-name 현대비앤지스틸 \
  --embedding korean

# 컬렉션 확인
python chunk_loader.py list

# 검색 테스트
python chunk_loader.py query 004560_token "매출액 현황" --embedding korean
```

## 주의사항

1. **임베딩 일관성**: 적재와 검색 시 동일한 임베딩 모델 사용 필요
2. **한국어 문서**: `--embedding korean` 권장
3. **컬렉션명**: 영문, 숫자, 언더스코어만 사용 가능 (한글 자동 제거)
4. **덮어쓰기**: 기존 컬렉션에 추가 적재 시 중복 ID 오류 발생 가능 → `--overwrite` 사용

## 의존성

```
chromadb>=0.4.0
sentence-transformers>=2.2.0  # korean 임베딩 시
openai>=1.0.0  # openai 임베딩 시
```

---

*작성일: 2026-01-29*
