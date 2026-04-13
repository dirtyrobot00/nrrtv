# DB 테이블 정의

## news_articles

뉴스 기사 저장 테이블.
`NaverFinanceNewsCollector`가 Naver Finance에서 수집한 기사를 저장합니다.

| Column | Type | Nullable | Description |
|---|---|---|---|
| id | INTEGER PK AUTOINCREMENT | NOT NULL | 내부 식별자 |
| url | TEXT UNIQUE | NOT NULL | 기사 원문 URL (중복 방지 기준) |
| title | TEXT | NOT NULL | 기사 제목 |
| content | TEXT | NULL | 기사 본문 전체 텍스트 |
| summary | TEXT | NULL | 기사 요약 (목록 페이지 발췌) |
| author | TEXT | NULL | 기자명 |
| media | TEXT(255) | NULL | 언론사 식별자 (예: `naver_oid_015`) |
| ticker | TEXT(20) | NULL | 관련 종목코드 (예: `005930`) |
| published_at | DATETIME(timezone) | NULL | 기사 발행 일시 |
| collected_at | DATETIME(timezone) | NOT NULL | 수집 일시 (default: utcnow) |
| raw_html | TEXT | NULL | 기사 페이지 원본 HTML |

### Indexes
- `idx_news_articles_published_at` on `published_at`
- `idx_news_articles_ticker` on `ticker`
- UNIQUE constraint on `url`

### ORM 위치
[src/storage/news_store.py](src/storage/news_store.py) — `NewsArticleORM` 클래스

---

## research_reports

종목 리서치 리포트 저장 테이블. PDF에서 추출한 텍스트 본문 포함.
`ResearchReportCollector`가 `data/raw/research_report/` 디렉토리의 PDF를 처리합니다.

| Column | Type | Nullable | Description |
|---|---|---|---|
| id | TEXT PK | NOT NULL | UUID |
| document_id | TEXT FK(documents.id) | NOT NULL | 원본 문서 레코드 |
| ticker | TEXT(20) | NOT NULL | 종목코드 (예: `005930`) |
| company_name | TEXT(200) | NOT NULL | 회사명 |
| firm | TEXT(100) | NOT NULL | 증권사명 |
| report_date | DATE | NOT NULL | 리포트 날짜 |
| analyst_name | TEXT(100) | NULL | 애널리스트명 |
| target_price | FLOAT | NULL | 목표주가 |
| investment_opinion | TEXT(50) | NULL | 투자의견 |
| content | TEXT | NULL | PDF에서 추출한 원문 텍스트 |
| confidence_score | FLOAT | NOT NULL | 추출 신뢰도 (기본 0.0) |
| extracted_at | DATETIME | NOT NULL | 추출 일시 |

### ORM 위치
[src/storage/document_store.py](src/storage/document_store.py) — `ResearchReportORM` 클래스

---

## documents

범용 문서 레코드 테이블. PDF 리포트, 재무보고서 등 파일 기반 문서에 사용.
뉴스 기사는 `news_articles` 테이블에 별도 저장하므로 이 테이블을 사용하지 않습니다.

### ORM 위치
[src/storage/document_store.py](src/storage/document_store.py) — `DocumentStore` 클래스

---

## telegram_channels

텔레그램 채널 메타데이터 저장 테이블.

### ORM 위치
[src/storage/telegram_store.py](src/storage/telegram_store.py) — `TelegramStore` 클래스

---

## telegram_messages

텔레그램 메시지 저장 테이블.

### ORM 위치
[src/storage/telegram_store.py](src/storage/telegram_store.py) — `TelegramStore` 클래스
