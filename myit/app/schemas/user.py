"""
사용자 관련 Pydantic 스키마
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr


# --- 요청 스키마 ---

class UserRegister(BaseModel):
    email: str
    password: str
    nickname: Optional[str] = None
    preferred_market: str = "KR"


class UserLogin(BaseModel):
    email: str
    password: str


class UserUpdate(BaseModel):
    nickname: Optional[str] = None
    preferred_market: Optional[str] = None
    morning_briefing_time: Optional[str] = None
    notification_enabled: Optional[bool] = None


# --- 응답 스키마 ---

class UserResponse(BaseModel):
    id: int
    email: str
    nickname: Optional[str]
    preferred_market: str
    morning_briefing_time: str
    notification_enabled: bool
    created_at: datetime

    class Config:
        from_attributes = True


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse
