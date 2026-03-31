"""
시맨틱 검색 서비스 — ChromaDB 기반 벡터 검색
PRD 섹션 6-C: Search & Recall
"""
import json
from typing import List, Dict, Optional

import chromadb
from chromadb.config import Settings as ChromaSettings

from app.core.config import settings


class SearchService:
    """ChromaDB 기반 시맨틱 검색 서비스"""

    COLLECTION_NAME = "myit_insights"

    def __init__(self):
        self.client = chromadb.PersistentClient(
            path=settings.CHROMA_PERSIST_DIR,
            settings=ChromaSettings(anonymized_telemetry=False),
        )
        self.collection = self.client.get_or_create_collection(
            name=self.COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )

    async def index_card(
        self,
        card_id: int,
        question: str,
        summary: str,
        hypothesis: str = "",
        tags: List[str] = None,
    ):
        """인사이트 카드를 벡터 인덱스에 추가"""
        # 검색 가능한 텍스트 구성
        text_parts = [question]
        if summary:
            text_parts.append(summary)
        if hypothesis:
            text_parts.append(hypothesis)

        document = "\n".join(text_parts)
        metadata = {
            "card_id": card_id,
            "type": "insight_card",
        }
        if tags:
            metadata["tags"] = json.dumps(tags)

        self.collection.upsert(
            ids=[f"card_{card_id}"],
            documents=[document],
            metadatas=[metadata],
        )

    async def index_event(self, event_id: int, title: str, summary: str = ""):
        """이벤트를 벡터 인덱스에 추가"""
        document = f"{title}\n{summary}" if summary else title
        self.collection.upsert(
            ids=[f"event_{event_id}"],
            documents=[document],
            metadatas=[{"event_id": event_id, "type": "event"}],
        )

    async def search(
        self,
        query: str,
        n_results: int = 10,
        filter_type: Optional[str] = None,
    ) -> List[Dict]:
        """
        시맨틱 검색 실행
        예: "내가 삼성전자를 왜 좋게 봤지?"
        """
        where_filter = None
        if filter_type:
            where_filter = {"type": filter_type}

        results = self.collection.query(
            query_texts=[query],
            n_results=n_results,
            where=where_filter,
        )

        search_results = []
        if results and results["ids"]:
            for i, doc_id in enumerate(results["ids"][0]):
                search_results.append({
                    "id": doc_id,
                    "document": results["documents"][0][i] if results["documents"] else "",
                    "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                    "distance": results["distances"][0][i] if results["distances"] else 0,
                })

        return search_results

    async def delete_card(self, card_id: int):
        """카드 인덱스 삭제"""
        try:
            self.collection.delete(ids=[f"card_{card_id}"])
        except Exception:
            pass

    async def get_similar_cards(self, card_id: int, n_results: int = 5) -> List[Dict]:
        """유사한 카드 찾기"""
        try:
            card_data = self.collection.get(ids=[f"card_{card_id}"])
            if card_data and card_data["documents"]:
                return await self.search(
                    card_data["documents"][0],
                    n_results=n_results + 1,
                    filter_type="insight_card",
                )
        except Exception:
            pass
        return []


# 싱글턴
search_service = SearchService()
