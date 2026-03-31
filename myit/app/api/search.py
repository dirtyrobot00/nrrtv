"""
시맨틱 검색 API
PRD 섹션 6-C: Search & Recall
"""
from typing import Optional
from fastapi import APIRouter, Depends

from app.core.security import get_current_user_id
from app.services.search_service import search_service

router = APIRouter(prefix="/search", tags=["검색"])


@router.get("/")
async def semantic_search(
    q: str,
    n: int = 10,
    type: Optional[str] = None,
    user_id: int = Depends(get_current_user_id),
):
    """
    시맨틱 검색
    예: "내가 삼성전자를 왜 좋게 봤지?"
    예: "작년에 반도체 이슈 때 내가 어떤 리스크를 적었지?"
    """
    results = await search_service.search(
        query=q,
        n_results=n,
        filter_type=type,
    )
    return {"query": q, "results": results, "total": len(results)}


@router.get("/similar/{card_id}")
async def find_similar(
    card_id: int,
    n: int = 5,
    user_id: int = Depends(get_current_user_id),
):
    """유사한 인사이트 카드 찾기"""
    results = await search_service.get_similar_cards(card_id, n_results=n)
    return {"card_id": card_id, "similar": results}
