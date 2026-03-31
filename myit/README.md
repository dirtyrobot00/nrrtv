# myit — My Insightful Trader

> "내 질문이 내 지식이 되고, 최신 정보가 내 관점을 업데이트한다"

myit는 **LLM Q&A를 인사이트 카드로 자동 변환**하고, **최신 공시/뉴스가 내 가설을 자동 업데이트**하는 개인 트레이딩 에이전트 서비스입니다.

⚠️ **면책**: myit는 정보 제공 목적이며, 투자 추천이 아닙니다.

---

## 기술 스택

| 구성 | 기술 |
|---|---|
| Backend | FastAPI + SQLAlchemy (async) + SQLite |
| Search | ChromaDB (시맨틱 검색) |
| LLM | Google Gemini API |
| Data | OPEN DART API + 뉴스 RSS |
| Frontend | HTML + CSS + JavaScript (SPA) |

## 프로젝트 구조

```
myit/
├── app/
│   ├── main.py              # FastAPI 앱
│   ├── core/                # 설정, DB, 인증
│   ├── models/              # ORM 모델 (5개)
│   ├── schemas/             # Pydantic 스키마
│   ├── api/                 # REST API 엔드포인트 (6 모듈)
│   └── services/            # 비즈니스 로직 (7 서비스)
├── frontend/                # SPA 프론트엔드
│   ├── index.html
│   ├── css/styles.css
│   └── js/app.js
├── requirements.txt
└── .env.example
```

## 빠른 시작

```bash
# 1. 의존성 설치
cd myit
pip install -r requirements.txt

# 2. 환경변수 설정
cp .env.example .env
# .env 파일에서 GEMINI_API_KEY, DART_API_KEY 설정

# 3. 서버 실행
uvicorn app.main:app --reload --port 8000

# 4. 브라우저에서 확인
# 앱: http://localhost:8000
# API 문서: http://localhost:8000/docs
```

## 핵심 API

| 메서드 | 엔드포인트 | 설명 |
|---|---|---|
| POST | `/api/auth/register` | 회원가입 |
| POST | `/api/auth/login` | 로그인 |
| POST | `/api/insights/from-qa` | Q&A → 인사이트 카드 |
| GET | `/api/insights/` | 카드 목록 |
| POST | `/api/events/refresh` | 공시/뉴스 새로고침 |
| GET | `/api/search/?q=...` | 시맨틱 검색 |
| POST | `/api/trades/` | 매매 기록 |
| GET | `/api/trades/performance` | 성과 분석 |
| GET | `/api/home/feed` | 홈 피드 |
