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
    INSIDER_ONLY_TERMS = {
        "dispensational",
        "pre-tribulation",
        "propitiation",
        "intercessory warfare",
        "five-fold ministry",
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
        if sum(1 for token in self.INSIDER_ONLY_TERMS if token in text) >= 2:
            return {"hard_reject": True, "hard_reject_reason": "The article leans on insider-only religious language with weak broad-entry potential."}
        pld_breakdown = dict(analysis.get("pld_breakdown") or {})
        if max(float(pld_breakdown.get(key, 0.0)) for key in ("entry_fit", "hook_fit", "loop_fit", "trust_fit")) < 45:
            return {"hard_reject": True, "hard_reject_reason": "The article cannot be cleanly reframed into curiosity-first or reflection-first content."}
        return {"hard_reject": False, "hard_reject_reason": ""}
