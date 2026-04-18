from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.model_router import resolve_model_for_task, run_local_model


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SocialReviewConsultant:
    RESEARCH_KEYS = [
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

    FINAL_REVIEW_KEYS = [
        "article_relevant",
        "content_quality_score",
        "risk_of_misleading",
        "post_tone",
        "approved_for_queue",
        "review_notes",
        "final_recommendation",
    ]

    def __init__(self, project_root: str) -> None:
        self.project_root = Path(project_root)
        self.prompts_dir = self.project_root / "prompts" / "social"
        self.post_candidates_dir = self.project_root / "data" / "post_candidates"
        self.post_reviews_dir = self.project_root / "data" / "post_reviews"
        self.prompts_dir.mkdir(parents=True, exist_ok=True)
        self.post_candidates_dir.mkdir(parents=True, exist_ok=True)
        self.post_reviews_dir.mkdir(parents=True, exist_ok=True)
        self.review_news_prompt = self.prompts_dir / "review_news.txt"
        self.generate_post_prompt = self.prompts_dir / "generate_facebook_post.txt"
        self.review_generated_prompt = self.prompts_dir / "review_generated_post.txt"

    def _fallback_research_review(self, article: Dict[str, Any]) -> Dict[str, Any]:
        text = " ".join(
            [
                str(article.get("title", "")),
                str(article.get("summary", "")),
                str(article.get("body_excerpt", "")),
                " ".join(article.get("keywords_hit") or []),
            ]
        ).lower()
        relevant = any(token in text for token in ["foreigner", "foreign", "eps", "migrant", "factory", "korea", "worker", "job"])
        score = 0.86 if relevant else 0.28
        return {
            "is_relevant": relevant,
            "relevance_score": score,
            "audience": "Foreign workers and job seekers in Korea",
            "why_it_matters": "It affects work access, job conditions, visa rules, or employment opportunities in Korea.",
            "practical_value": "high" if relevant else "low",
            "risk_of_misleading": "medium",
            "post_angle": "Practical explainer for foreigners in Korea",
            "post_summary": str(article.get("summary") or article.get("title") or "").strip(),
            "cta_type": "read_more",
            "suggested_cta": "Read the source and compare the rule or hiring detail with your own situation.",
            "should_publish": relevant,
        }

    def _fallback_generated_post(
        self,
        article: Dict[str, Any],
        research_review: Dict[str, Any],
        topic: str = "",
    ) -> Dict[str, Any]:
        title = str(article.get("title") or "").strip()
        summary = str(research_review.get("post_summary") or article.get("summary") or title).strip()
        why = str(research_review.get("why_it_matters") or "").strip()
        cta = str(research_review.get("suggested_cta") or "Read the source before you apply or make a decision.").strip()
        audience = str(research_review.get("audience") or "Foreign workers and job seekers in Korea").strip()
        lines = []
        if topic:
            lines.append(f"[{topic}]")
        lines.extend(
            [
                summary,
                "",
                f"Why it matters: {why}" if why else "",
                f"Who this helps: {audience}",
                "",
                cta,
            ]
        )
        post_text = "\n".join(line for line in lines if line is not None).strip()
        return {
            "generated_post": post_text,
            "summary": summary,
            "why_it_matters": why,
        }

    def _fallback_final_review(
        self,
        article: Dict[str, Any],
        research_review: Dict[str, Any],
        generated_post: str,
    ) -> Dict[str, Any]:
        article_relevant = bool(research_review.get("is_relevant"))
        risk = str(research_review.get("risk_of_misleading") or "medium").strip().lower() or "medium"
        review_notes: List[str] = []
        if not article_relevant:
            review_notes.append("Research agent marked this article as not relevant for the target audience.")
        if "job now" in generated_post.lower() or "apply now" in generated_post.lower():
            risk = "medium" if risk == "low" else risk
            review_notes.append("The CTA may sound too immediate for a policy or news article.")
        if not review_notes:
            review_notes.append("The post is useful but should stay factual and avoid promising outcomes.")
        recommendation = "publish" if article_relevant else "reject"
        if article_relevant and risk == "high":
            recommendation = "revise"
        return {
            "article_relevant": article_relevant,
            "content_quality_score": 0.82 if article_relevant else 0.35,
            "risk_of_misleading": risk,
            "post_tone": "helpful" if article_relevant else "too_vague",
            "approved_for_queue": recommendation in {"publish", "revise"},
            "review_notes": review_notes,
            "final_recommendation": recommendation,
        }

    def _load_prompt(self, path: Path) -> str:
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8").strip()

    def _call_json_task(
        self,
        task_type: str,
        prompt_text: str,
        payload: Dict[str, Any],
        required_keys: List[str],
    ) -> Optional[Dict[str, Any]]:
        if not prompt_text:
            return None
        result = run_local_model(
            task_type=task_type,
            prompt=f"{prompt_text}\n\nINPUT_JSON:\n{json.dumps(payload, ensure_ascii=False)}",
            format="json",
        )
        parsed = result.get("parsed")
        if not result.get("ok") or not isinstance(parsed, dict):
            return None
        if not all(key in parsed for key in required_keys):
            return None
        return parsed

    def research_review(self, article: Dict[str, Any]) -> Dict[str, Any]:
        normalized = {
            "source": article.get("source", ""),
            "title": article.get("title", ""),
            "summary": article.get("summary", ""),
            "body_excerpt": article.get("body_excerpt", ""),
            "url": article.get("url", ""),
            "published_at": article.get("published_at", ""),
            "keywords_hit": article.get("keywords_hit", []),
            "category_hint": article.get("category_hint", ""),
        }
        prompt = self._load_prompt(self.review_news_prompt)
        parsed = self._call_json_task("review_news", prompt, normalized, self.RESEARCH_KEYS)
        return parsed or self._fallback_research_review(normalized)

    def generate_candidate_post(
        self,
        article: Dict[str, Any],
        research_review: Dict[str, Any],
        *,
        topic: str = "",
        tone: str = "practical",
        length: str = "medium",
    ) -> Dict[str, Any]:
        input_payload = {
            "article": article,
            "research_review": research_review,
            "topic": topic,
            "tone": tone,
            "length": length,
        }
        prompt = self._load_prompt(self.generate_post_prompt)
        result = run_local_model(
            task_type="generate_post",
            prompt=f"{prompt}\n\nINPUT_JSON:\n{json.dumps(input_payload, ensure_ascii=False)}",
            format="json",
        )
        parsed = result.get("parsed")
        if isinstance(parsed, dict) and str(parsed.get("generated_post", "")).strip():
            return {
                "generated_post": str(parsed.get("generated_post", "")).strip(),
                "summary": str(parsed.get("summary", "")).strip(),
                "why_it_matters": str(parsed.get("why_it_matters", "")).strip(),
            }
        return self._fallback_generated_post(article, research_review, topic=topic)

    def review_generated_post(
        self,
        article: Dict[str, Any],
        research_review: Dict[str, Any],
        generated_post: str,
    ) -> Dict[str, Any]:
        input_payload = {
            "article": article,
            "research_review": research_review,
            "generated_post": generated_post,
        }
        prompt = self._load_prompt(self.review_generated_prompt)
        parsed = self._call_json_task("reasoning_review", prompt, input_payload, self.FINAL_REVIEW_KEYS)
        fallback = self._fallback_final_review(article, research_review, generated_post)
        if not parsed:
            return fallback
        if bool(research_review.get("is_relevant")) is False:
            parsed["article_relevant"] = False
            parsed["approved_for_queue"] = False
            parsed["final_recommendation"] = "reject"
            notes = parsed.get("review_notes") or []
            if isinstance(notes, list):
                notes.insert(0, "Research Agent marked the article as irrelevant.")
                parsed["review_notes"] = notes
        return parsed

    def consult_article(
        self,
        article: Dict[str, Any],
        *,
        topic: str = "",
        tone: str = "practical",
        length: str = "medium",
    ) -> Dict[str, Any]:
        research_review = self.research_review(article)
        research_model = resolve_model_for_task("review_news")
        if not bool(research_review.get("is_relevant")):
            final_review = {
                "article_relevant": False,
                "content_quality_score": 0.0,
                "risk_of_misleading": str(research_review.get("risk_of_misleading") or "medium"),
                "post_tone": "too_vague",
                "approved_for_queue": False,
                "review_notes": ["Research Agent rejected the article as irrelevant to the target audience."],
                "final_recommendation": "reject",
            }
            generated = {"generated_post": "", "summary": "", "why_it_matters": str(research_review.get("why_it_matters") or "")}
        else:
            generated = self.generate_candidate_post(article, research_review, topic=topic, tone=tone, length=length)
            final_review = self.review_generated_post(article, research_review, generated.get("generated_post", ""))

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        candidate_row = {
            "created_at": _now_iso(),
            "article": article,
            "research_review": research_review,
            "generated_post": generated.get("generated_post", ""),
            "summary": generated.get("summary", ""),
            "why_it_matters": generated.get("why_it_matters", ""),
            "models": {
                "research": research_model,
                "content": resolve_model_for_task("generate_post"),
                "review": resolve_model_for_task("reasoning_review"),
            },
        }
        review_row = {
            "created_at": _now_iso(),
            "article_url": article.get("url", ""),
            "final_review": final_review,
        }
        (self.post_candidates_dir / f"{stamp}_candidate.json").write_text(
            json.dumps(candidate_row, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (self.post_reviews_dir / f"{stamp}_review.json").write_text(
            json.dumps(review_row, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {
            "article": article,
            "research_review": research_review,
            "generated_post": generated.get("generated_post", ""),
            "summary": generated.get("summary", ""),
            "why_it_matters": generated.get("why_it_matters", ""),
            "final_review": final_review,
            "models": candidate_row["models"],
        }
