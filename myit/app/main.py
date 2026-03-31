"""
myit — FastAPI 메인 앱
"내 질문이 내 지식이 되고, 최신 정보가 내 관점을 업데이트한다"
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from app.core.config import settings
from app.core.database import init_db
from app.api.router import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작 시 DB 테이블 생성"""
    await init_db()
    yield


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description=(
        "myit는 LLM Q&A를 인사이트 카드로 자동 변환하고, "
        "최신 공시/뉴스가 내 가설을 자동 업데이트하는 "
        "개인 트레이딩 에이전트 서비스입니다."
    ),
    lifespan=lifespan,
)

# API 라우터 등록
app.include_router(api_router)

# 프론트엔드 정적 파일 서빙
frontend_dir = Path(__file__).parent.parent / "frontend"
if frontend_dir.exists():
    app.mount("/css", StaticFiles(directory=str(frontend_dir / "css")), name="css")
    app.mount("/js", StaticFiles(directory=str(frontend_dir / "js")), name="js")

    @app.get("/")
    async def serve_frontend():
        return FileResponse(str(frontend_dir / "index.html"))


@app.get("/api/health")
async def health_check():
    return {
        "status": "ok",
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "disclaimer": "⚠️ myit는 정보 제공 목적이며, 투자 추천이 아닙니다.",
    }
