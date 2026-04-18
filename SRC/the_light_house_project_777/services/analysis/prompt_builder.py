from __future__ import annotations

import json
from typing import Any, Dict


class ArticleAnalysisPromptBuilder:
    """Builds the local-LLM prompt for phase-1 article selection."""

    def build_prompt(self, article: Dict[str, Any]) -> str:
        payload = {
            "source_code": article.get("source_code"),
            "source_name": article.get("source_name"),
            "source_status": article.get("source_status"),
            "feed_code": article.get("feed_code"),
            "feed_name": article.get("feed_name"),
            "title": article.get("title"),
            "summary_raw": article.get("summary_raw"),
            "article_content_raw": article.get("article_content_raw"),
            "article_url": article.get("canonical_url") or article.get("article_url"),
            "published_at": str(article.get("published_at") or ""),
        }
        return (
            "You are evaluating Christian news articles for Lighthouse Project phase 1.\n"
            "The system does not optimize for sensationalism alone. It optimizes for articles that can be transformed into curiosity-first, psychologically safe, PLD-compatible Facebook content.\n"
            "PLD guidance:\n"
            "- Entry: universal human need, light and relatable, question-based.\n"
            "- Hook: curiosity without force, no heavy claims, open loops.\n"
            "- Loop: repeated internal reflection, unresolved but safe tension.\n"
            "- Trust: clear, structured, informative, low-pressure framing.\n"
            "Cross-channel strategy guidance:\n"
            "- Lead with human needs before religious identity.\n"
            "- Curiosity + invitation + practicality beats insider-heavy language.\n"
            "- Reject conflict bait, unverifiable trust gaps, and content that cannot become curiosity-first or reflection-first.\n\n"
            "Return JSON only with this exact structure:\n"
            "{\n"
            '  "reaction_breakdown": {\n'
            '    "emotional_trigger": 0-100,\n'
            '    "inversion_surprise": 0-100,\n'
            '    "self_projection": 0-100,\n'
            '    "comparison_anxiety_hope_trigger": 0-100\n'
            "  },\n"
            '  "pld_breakdown": {\n'
            '    "entry_fit": 0-100,\n'
            '    "hook_fit": 0-100,\n'
            '    "loop_fit": 0-100,\n'
            '    "trust_fit": 0-100\n'
            "  },\n"
            '  "operational_breakdown": {\n'
            '    "content_transformation_ease": 0-100,\n'
            '    "question_based_framing_ease": 0-100,\n'
            '    "brand_safety": 0-100,\n'
            '    "moderation_platform_risk": 0-100,\n'
            '    "reviewer_confirmation_likelihood": 0-100\n'
            "  },\n"
            '  "dominant_pld_stage": "entry|hook|loop|trust",\n'
            '  "selection_summary": "short operator-facing explanation",\n'
            '  "hard_reject": true_or_false,\n'
            '  "hard_reject_reason": "empty if not rejected"\n'
            "}\n\n"
            f"ARTICLE_JSON:\n{json.dumps(payload, ensure_ascii=False)}"
        )
