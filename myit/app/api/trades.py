"""
매매 기록 API — 수동 입력/CSV 업로드/성과 분석
PRD 섹션 9: 투자 성과 연동
"""
from typing import Optional
from fastapi import APIRouter, Depends, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user_id
from app.schemas.trade import (
    TradeCreate, TradeResponse, TradeListResponse, PerformanceSummary,
)
from app.services.trade_service import trade_service

router = APIRouter(prefix="/trades", tags=["매매 기록"])


@router.post("/", response_model=TradeResponse, status_code=201)
async def create_trade(
    data: TradeCreate,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """매매 기록 수동 입력"""
    trade = await trade_service.create_trade(
        db=db, user_id=user_id,
        **data.model_dump(),
    )
    return TradeResponse.model_validate(trade)


@router.post("/upload-csv")
async def upload_csv(
    file: UploadFile = File(...),
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """CSV 파일로 매매 기록 일괄 업로드"""
    content = await file.read()
    csv_text = content.decode("utf-8-sig")  # BOM 처리

    trades = await trade_service.upload_csv(db, user_id, csv_text)
    return {
        "status": "ok",
        "imported_count": len(trades),
        "message": f"{len(trades)}건의 매매 기록을 가져왔습니다",
    }


@router.get("/", response_model=TradeListResponse)
async def list_trades(
    ticker: Optional[str] = None,
    direction: Optional[str] = None,
    limit: int = 50,
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """매매 기록 조회"""
    trades = await trade_service.get_trades(
        db=db, user_id=user_id, ticker=ticker, direction=direction, limit=limit,
    )
    return TradeListResponse(
        trades=[TradeResponse.model_validate(t) for t in trades],
        total=len(trades),
    )


@router.get("/performance", response_model=PerformanceSummary)
async def get_performance(
    user_id: int = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
):
    """투자 성과 분석 — 의사결정 품질 중심"""
    return await trade_service.calculate_performance(db, user_id)
