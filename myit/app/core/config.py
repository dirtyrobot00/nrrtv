"""
myit 설정 관리 — 환경변수 기반 설정
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """myit 앱 설정"""

    # 앱 기본 설정
    APP_NAME: str = "myit - My Insightful Trader"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = True

    # 데이터베이스
    DATABASE_URL: str = "sqlite+aiosqlite:///./myit.db"

    # JWT 인증
    SECRET_KEY: str = "dev-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440  # 24시간

    # Google Gemini API
    GEMINI_API_KEY: Optional[str] = None
    GEMINI_MODEL: str = "gemini-2.0-flash"

    # OPEN DART API
    DART_API_KEY: Optional[str] = None
    DART_BASE_URL: str = "https://opendart.fss.or.kr/api"

    # ChromaDB
    CHROMA_PERSIST_DIR: str = "./chroma_data"
    EMBEDDING_MODEL: str = "jhgan/ko-sroberta-multitask"

    # 뉴스 RSS
    NEWS_RSS_URLS: str = "https://news.google.com/rss/search?q=주식&hl=ko&gl=KR&ceid=KR:ko"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
