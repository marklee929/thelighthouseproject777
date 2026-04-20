from __future__ import annotations

from typing import Any, Dict


class HardRejectEvaluator:
    """Applies deterministic hard-reject rules from the project brief."""

    CONFLICT_BAIT_TERMS = {
        "traitor",
        "boycott",
        "destroyed",
        "war on",
        "exposed",
        "outrage",
        "must be stopped",
        "hate",
    }
    SPAM_UNSAFE_TERMS = {
        "casino",
        "betting",
        "porn",
        "miracle cure",
        "click here",
        "buy now",
    }

    def evaluate(self, article: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
        text = " ".join(
            [
                str(article.get("title", "")),
                str(article.get("summary_raw", "")),
                str(article.get("article_content_raw", "")),
            ]
        ).lower()
        source_status = str(article.get("source_status", "")).strip().lower()
        feed_status = str(article.get("feed_status", "")).strip().lower()
        if source_status != "active" or feed_status not in {"active", ""}:
            return {"hard_reject": True, "hard_reject_reason": "Source or feed is not in an active trusted state."}
        if any(token in text for token in self.CONFLICT_BAIT_TERMS):
            return {"hard_reject": True, "hard_reject_reason": "High policy-risk conflict bait detected in the article framing."}
        if any(token in text for token in self.SPAM_UNSAFE_TERMS):
            return {"hard_reject": True, "hard_reject_reason": "Spam-like or platform-unsafe content detected."}
        operational_breakdown = dict(analysis.get("operational_breakdown") or {})
        moderation_risk = float(operational_breakdown.get("moderation_platform_risk", 0.0))
        brand_safety = float(operational_breakdown.get("brand_safety", 100.0))
        if moderation_risk >= 90.0 or brand_safety <= 20.0:
            return {"hard_reject": True, "hard_reject_reason": "Platform-risk or brand-safety risk is too high for phase-1 review."}
        return {"hard_reject": False, "hard_reject_reason": ""}
