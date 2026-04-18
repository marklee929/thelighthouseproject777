from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from core.model_router import resolve_model_for_task, run_local_model


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class NewsReviewer:
    OUTPUT_KEYS = [
        "is_relevant",
        "relevance_score",
        "audience",
        "why_it_matters",
        "practical_value",
        "risk_of_misleading",
        "post_angle",
        "post_summary",
        "cta_type",
        "suggested_cta",
        "should_publish",
    ]

    def __init__(self, project_root: str) -> None:
        self.project_root = Path(project_root)
        self.publish_candidates_dir = self.project_root / "data" / "publish_candidates"
        self.prompts_dir = self.project_root / "prompts" / "social"
        self.publish_candidates_dir.mkdir(parents=True, exist_ok=True)
        self.prompts_dir.mkdir(parents=True, exist_ok=True)
        self.ollama_base_url = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").strip().rstrip("/")
        self.model = os.getenv("SOCIAL_REVIEW_MODEL", resolve_model_for_task("review_news")).strip()
        self.review_prompt_path = self.prompts_dir / "review_news.txt"

    def _fallback_review(self, article: Dict[str, Any]) -> Dict[str, Any]:
        text = " ".join(
            [
                str(article.get("title", "")),
                str(article.get("summary", "")),
                str(article.get("body_excerpt", "")),
                " ".join(article.get("keywords_hit") or []),
            ]
        ).lower()
        relevant = any(token in text for token in ["foreigner", "foreign", "eps", "migrant", "factory", "korea"])
        score = 0.88 if relevant else 0.34
        return {
            "is_relevant": relevant,
            "relevance_score": score,
            "audience": "Foreign workers and job seekers in Korea",
            "why_it_matters": "It affects jobs, visas, hiring access, or workplace conditions for foreign residents in Korea.",
            "practical_value": "high" if relevant else "low",
            "risk_of_misleading": "medium",
            "post_angle": "Practical update for foreigners in Korea",
            "post_summary": str(article.get("summary") or article.get("title") or "").strip(),
            "cta_type": "read_more",
            "suggested_cta": "Read the source and check whether the policy or hiring detail applies to your case.",
            "should_publish": relevant,
        }

    def _llm_review(self, article: Dict[str, Any]) -> Dict[str, Any]:
        if not self.review_prompt_path.exists():
            return self._fallback_review(article)
        prompt = self.review_prompt_path.read_text(encoding="utf-8")
        payload = {
            "prompt": f"{prompt}\n\nINPUT_JSON:\n{json.dumps(article, ensure_ascii=False)}",
        }
        try:
            body = run_local_model(
                task_type="review_news",
                prompt=payload["prompt"],
                model=self.model,
                ollama_base_url=self.ollama_base_url,
                format="json",
                timeout=30.0,
            )
            raw = str(body.get("raw", "")).strip()
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and all(key in parsed for key in self.OUTPUT_KEYS):
                return parsed
        except Exception:
            pass
        return self._fallback_review(article)

    def review_articles(self, articles: List[Dict[str, Any]]) -> Dict[str, Any]:
        reviewed: List[Dict[str, Any]] = []
        for row in articles:
            normalized = {
                "source": row.get("source", ""),
                "title": row.get("title", ""),
                "summary": row.get("summary", ""),
                "body_excerpt": row.get("body_excerpt", ""),
                "url": row.get("url", ""),
                "published_at": row.get("published_at", ""),
                "keywords_hit": row.get("keywords_hit", []),
                "category_hint": row.get("category_hint", ""),
            }
            review = self._llm_review(normalized)
            reviewed.append({**normalized, "review": review, "reviewed_at": _now_iso()})

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_path = self.publish_candidates_dir / f"{stamp}_reviewed.jsonl"
        out_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in reviewed), encoding="utf-8")
        return {"reviewed_items": reviewed, "path": str(out_path)}
