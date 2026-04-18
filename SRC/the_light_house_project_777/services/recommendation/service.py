from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.model_router import resolve_model_for_task, run_local_model
from repositories.interfaces import ArticleRepositoryProtocol


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class ArticleRecommendationService:
    """Scores collected articles with a local LLM without coupling to ingestion flow."""

    def __init__(
        self,
        *,
        article_repository: ArticleRepositoryProtocol,
        model_name: Optional[str] = None,
        ollama_base_url: Optional[str] = None,
    ) -> None:
        self.article_repository = article_repository
        self.model_name = str(model_name or resolve_model_for_task("review_news")).strip()
        self.ollama_base_url = ollama_base_url

    def score_candidates(self, limit: int = 20) -> List[Dict[str, Any]]:
        articles = self.article_repository.list_candidates_for_recommendation(limit)
        results: List[Dict[str, Any]] = []
        for article in articles:
            recommendation = self._recommend(article)
            self.article_repository.update_recommendation(str(article["article_id"]), recommendation)
            results.append({"article_id": article["article_id"], **recommendation})
        return results

    def _recommend(self, article: Dict[str, Any]) -> Dict[str, Any]:
        prompt = self._build_prompt(article)
        response = run_local_model(
            task_type="review_news",
            prompt=prompt,
            model=self.model_name,
            ollama_base_url=self.ollama_base_url,
            format="json",
            timeout=45.0,
            temperature=0.2,
        )
        parsed = self._parse_response(response)
        if parsed is None:
            parsed = self._fallback_recommendation(article, response)
        return {
            "recommendation_score": parsed["recommendation_score"],
            "recommendation_reason": parsed["recommendation_reason"],
            "recommendation_model": self.model_name,
            "recommendation_payload": {
                "prompt": prompt,
                "raw_response": response.get("raw", ""),
                "parsed_response": parsed,
                "ok": bool(response.get("ok")),
                "error": response.get("error", ""),
            },
            "recommended_at": _now_utc(),
        }

    def _build_prompt(self, article: Dict[str, Any]) -> str:
        payload = {
            "title": article.get("title"),
            "summary_raw": article.get("summary_raw"),
            "article_content_raw": article.get("article_content_raw"),
            "article_url": article.get("canonical_url") or article.get("article_url"),
            "published_at": str(article.get("published_at") or ""),
        }
        return (
            "You are scoring Christian news candidates for a human-reviewed Facebook preparation workflow.\n"
            "Return JSON only with keys recommendation_score and recommendation_reason.\n"
            "recommendation_score must be a number from 0 to 100.\n"
            "recommendation_reason must be a short operator-facing explanation grounded in relevance, clarity, and faith-oriented usefulness.\n\n"
            f"ARTICLE_JSON:\n{json.dumps(payload, ensure_ascii=False)}"
        )

    def _parse_response(self, response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        parsed = response.get("parsed")
        if not isinstance(parsed, dict):
            raw = str(response.get("raw", "")).strip()
            if not raw:
                return None
            try:
                parsed = json.loads(raw)
            except Exception:
                return None
        if "recommendation_score" not in parsed or "recommendation_reason" not in parsed:
            return None
        try:
            score = max(0.0, min(100.0, float(parsed["recommendation_score"])))
        except Exception:
            return None
        reason = " ".join(str(parsed["recommendation_reason"]).split())
        if not reason:
            return None
        return {
            "recommendation_score": round(score, 2),
            "recommendation_reason": reason,
        }

    def _fallback_recommendation(self, article: Dict[str, Any], response: Dict[str, Any]) -> Dict[str, Any]:
        text = " ".join(
            [
                str(article.get("title", "")),
                str(article.get("summary_raw", "")),
                str(article.get("article_content_raw", "")),
            ]
        ).lower()
        high_signal_tokens = [
            "christian",
            "church",
            "faith",
            "gospel",
            "bible",
            "mission",
            "missionary",
            "pastor",
            "prayer",
            "catholic",
            "evangelical",
            "persecution",
            "ministry",
        ]
        hits = sum(1 for token in high_signal_tokens if token in text)
        score = min(92.0, 35.0 + hits * 8.5)
        if hits == 0:
            score = 28.0
        reason = "Fallback score used because the local LLM did not return valid JSON. The article still appears relevant to Christian news signals." if hits else "Fallback score used because the local LLM did not return valid JSON and the article has weak explicit Christian-news signals."
        if response.get("error"):
            reason = f"{reason} Error: {response['error']}"
        return {
            "recommendation_score": round(score, 2),
            "recommendation_reason": reason,
        }
