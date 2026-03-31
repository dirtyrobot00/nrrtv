"""
매매 기록/성과 분석 서비스
PRD 섹션 9: 투자 성과 연동
"""
import csv
import io
from datetime import datetime, timezone
from typing import List, Optional, Dict
from collections import defaultdict

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.trade import Trade
from app.schemas.trade import PerformanceSummary


class TradeService:
    """매매 기록 + 성과 분석"""

    async def create_trade(
        self, db: AsyncSession, user_id: int, **kwargs
    ) -> Trade:
        """매매 기록 생성 (수동 입력)"""
        trade = Trade(user_id=user_id, **kwargs)
        trade.total_amount = trade.quantity * trade.price
        db.add(trade)
        await db.flush()
        return trade

    async def upload_csv(
        self, db: AsyncSession, user_id: int, csv_content: str
    ) -> List[Trade]:
        """
        CSV 업로드로 매매 기록 일괄 생성
        예상 컬럼: ticker, direction, quantity, price, traded_at, memo
        """
        trades = []
        reader = csv.DictReader(io.StringIO(csv_content))

        for row in reader:
            try:
                trade = Trade(
                    user_id=user_id,
                    ticker=row.get("ticker", row.get("종목코드", "")),
                    ticker_name=row.get("ticker_name", row.get("종목명", "")),
                    direction=row.get("direction", row.get("매매구분", "buy")).lower(),
                    quantity=float(row.get("quantity", row.get("수량", 0))),
                    price=float(row.get("price", row.get("단가", 0))),
                    fee=float(row.get("fee", row.get("수수료", 0))),
                    traded_at=datetime.fromisoformat(
                        row.get("traded_at", row.get("체결시각", datetime.now().isoformat()))
                    ),
                    entry_reason=row.get("memo", row.get("메모", "")),
                    source="csv",
                )
                trade.total_amount = trade.quantity * trade.price
                db.add(trade)
                trades.append(trade)
            except (ValueError, KeyError):
                continue  # 파싱 실패 행은 스킵

        await db.flush()
        return trades

    async def get_trades(
        self,
        db: AsyncSession,
        user_id: int,
        ticker: str = None,
        direction: str = None,
        limit: int = 50,
    ) -> List[Trade]:
        """매매 기록 조회"""
        query = (
            select(Trade)
            .where(Trade.user_id == user_id)
            .order_by(Trade.traded_at.desc())
        )
        if ticker:
            query = query.where(Trade.ticker == ticker)
        if direction:
            query = query.where(Trade.direction == direction)
        query = query.limit(limit)

        result = await db.execute(query)
        return list(result.scalars().all())

    async def calculate_performance(
        self, db: AsyncSession, user_id: int
    ) -> PerformanceSummary:
        """
        투자 성과 분석 — PRD "의사결정 품질" 중심
        """
        trades = await self.get_trades(db, user_id, limit=1000)

        if not trades:
            return PerformanceSummary(
                total_trades=0, win_count=0, loss_count=0,
                win_rate=0.0, total_pnl=0.0, avg_return_pct=0.0,
                avg_holding_days=0.0,
            )

        # 기본 통계
        sell_trades = [t for t in trades if t.direction == "sell" and t.realized_pnl is not None]
        win_count = len([t for t in sell_trades if t.realized_pnl > 0])
        loss_count = len([t for t in sell_trades if t.realized_pnl <= 0])
        total_pnl = sum(t.realized_pnl for t in sell_trades if t.realized_pnl)

        returns = [t.return_pct for t in sell_trades if t.return_pct is not None]
        avg_return = sum(returns) / len(returns) if returns else 0.0

        holding_days = [t.holding_days for t in sell_trades if t.holding_days is not None]
        avg_holding = sum(holding_days) / len(holding_days) if holding_days else 0.0

        # 셋업별 승률 분석
        setup_stats = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0})
        for t in sell_trades:
            tag = t.setup_tag or "미분류"
            setup_stats[tag]["total"] += 1
            if t.realized_pnl and t.realized_pnl > 0:
                setup_stats[tag]["wins"] += 1
            setup_stats[tag]["pnl"] += t.realized_pnl or 0

        setup_performance = [
            {
                "setup": tag,
                "trade_count": stats["total"],
                "win_rate": stats["wins"] / stats["total"] if stats["total"] > 0 else 0,
                "total_pnl": stats["pnl"],
            }
            for tag, stats in setup_stats.items()
        ]

        # 자주 손실 보는 상황 Top 3
        loss_setups = sorted(
            [(tag, stats["pnl"]) for tag, stats in setup_stats.items() if stats["pnl"] < 0],
            key=lambda x: x[1],
        )
        top_loss_patterns = [
            f"{tag}: 총 {pnl:,.0f}원 손실" for tag, pnl in loss_setups[:3]
        ]

        total_sell = len(sell_trades)
        return PerformanceSummary(
            total_trades=len(trades),
            win_count=win_count,
            loss_count=loss_count,
            win_rate=win_count / total_sell if total_sell > 0 else 0.0,
            total_pnl=total_pnl,
            avg_return_pct=avg_return,
            avg_holding_days=avg_holding,
            setup_performance=setup_performance,
            top_loss_patterns=top_loss_patterns,
        )


# 싱글턴
trade_service = TradeService()
