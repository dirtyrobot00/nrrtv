"""
LLM 서비스 — Gemini API를 사용한 Q&A → 인사이트 카드 변환
PRD 섹션 6-A: Insight Capture
"""
import json
from typing import Optional, List, Dict
import google.generativeai as genai

from app.core.config import settings


class LLMService:
    """LLM 기반 인사이트 생성 서비스"""

    def __init__(self):
        if settings.GEMINI_API_KEY:
            genai.configure(api_key=settings.GEMINI_API_KEY)
            self.model = genai.GenerativeModel(settings.GEMINI_MODEL)
        else:
            self.model = None

    async def generate_answer(self, question: str, context: str = "") -> str:
        """질문에 대한 LLM 답변 생성"""
        if not self.model:
            return "[DEMO] Gemini API 키가 설정되지 않았습니다. .env 파일에 GEMINI_API_KEY를 설정해주세요."

        prompt = f"""당신은 투자 리서치 어시스턴트입니다. 
다음 질문에 대해 객관적이고 근거 기반의 답변을 제공하세요.
반드시 출처/근거를 명시하고, 불확실한 정보는 그렇다고 표시하세요.

{f"참고 컨텍스트: {context}" if context else ""}

질문: {question}

⚠️ 면책: 이 답변은 정보 제공 목적이며, 투자 추천이 아닙니다."""

        response = self.model.generate_content(prompt)
        return response.text

    async def qa_to_card_fields(
        self, question: str, answer: str, tickers: List[str] = None
    ) -> Dict:
        """
        Q&A를 인사이트 카드 필드로 변환
        제목/요약/태그/리스크/follow-up 자동 추출
        """
        if not self.model:
            # 데모 모드: 간단한 파싱
            return {
                "title": question[:100],
                "summary": answer[:500] if answer else "",
                "tickers": tickers or [],
                "themes": [],
                "risk_rebuttal": "",
                "followup_questions": [
                    "이 종목의 최근 실적은?",
                    "경쟁사 대비 강점은?",
                    "주요 리스크 요인은?",
                ],
            }

        extraction_prompt = f"""다음 Q&A를 분석하여 JSON 형식으로 인사이트 카드 필드를 추출하세요.

질문: {question}
답변: {answer}

다음 JSON 형식으로 응답하세요 (한국어로):
{{
    "title": "핵심을 담은 카드 제목 (30자 이내)",
    "summary": "핵심 요약 3~7줄",
    "tickers": ["관련 종목코드 리스트"],
    "themes": ["관련 테마/키워드 리스트"],
    "risk_rebuttal": "이 관점에 대한 주요 리스크/반론",
    "followup_questions": ["후속 질문 3개"]
}}

JSON만 반환하세요, 다른 텍스트 없이."""

        response = self.model.generate_content(extraction_prompt)
        try:
            # JSON 파싱 시도
            text = response.text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]
            return json.loads(text)
        except (json.JSONDecodeError, Exception):
            return {
                "title": question[:100],
                "summary": answer[:500],
                "tickers": tickers or [],
                "themes": [],
                "risk_rebuttal": "",
                "followup_questions": [],
            }

    async def summarize_event(self, title: str, content: str) -> Dict:
        """이벤트(공시/뉴스) 요약 + 핵심 문장 추출"""
        if not self.model:
            return {
                "summary": content[:300] if content else title,
                "key_sentences": [title],
            }

        prompt = f"""다음 금융 이벤트를 분석하세요:

제목: {title}
내용: {content[:3000]}

JSON 형식으로 응답:
{{
    "summary": "3~5줄 핵심 요약",
    "key_sentences": ["핵심 문장 최대 3개"],
    "tickers": ["관련 종목코드"],
    "themes": ["관련 테마"]
}}

JSON만 반환하세요."""

        response = self.model.generate_content(prompt)
        try:
            text = response.text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]
            return json.loads(text)
        except Exception:
            return {
                "summary": content[:300] if content else title,
                "key_sentences": [title],
            }


# 싱글턴 인스턴스
llm_service = LLMService()
