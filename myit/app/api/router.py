"""
API 라우터 통합
"""
from fastapi import APIRouter

from app.api.auth import router as auth_router
from app.api.insights import router as insights_router
from app.api.events import router as events_router
from app.api.trades import router as trades_router
from app.api.search import router as search_router
from app.api.home import router as home_router

api_router = APIRouter(prefix="/api")

api_router.include_router(auth_router)
api_router.include_router(insights_router)
api_router.include_router(events_router)
api_router.include_router(trades_router)
api_router.include_router(search_router)
api_router.include_router(home_router)
