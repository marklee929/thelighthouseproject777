from __future__ import annotations

from typing import Any, Mapping


class NewsCollectorPreStorageFilter:
    """Phase-1 permissive gate: keep broad collection, reject only invalid or obviously unsafe articles."""

    UNSAFE_TERMS = {
        "porn",
        "casino",
        "betting",
        "free money",
        "miracle cure",
        "click here",
        "buy now",
        "hate group",
        "beheading",
        "graphic violence",
    }
    CONFLICT_BAIT_TERMS = {
        "traitor",
        "must be stopped",
        "destroy them",
        "exterminate",
        "civil war now",
        "violent revenge",
    }

    def evaluate(self, article: Mapping[str, Any]) -> dict[str, Any]:
        title = " ".join(str(article.get("title") or "").split()).strip()
        original_url = str(article.get("canonical_url") or article.get("article_url") or "").strip()
        source_active = str(article.get("source_status") or "active").strip().lower() == "active"
        feed_active = str(article.get("feed_status") or "active").strip().lower() == "active"
        text = self._article_text(article)

        reasons: list[str] = []
        passed = True
        if not title:
            passed = False
            reasons.append("missing title")
        if not original_url:
            passed = False
            reasons.append("missing original_url")
        if not source_active or not feed_active:
            passed = False
            reasons.append("source or feed is not active")
        if any(token in text for token in self.UNSAFE_TERMS):
            passed = False
            reasons.append("obvious spam or unsafe content")
        if any(token in text for token in self.CONFLICT_BAIT_TERMS):
            passed = False
            reasons.append("obvious conflict bait")
        if passed:
            reasons.append("passed permissive phase-1 storage gate")

        return {
            "passed": passed,
            "pre_storage_reason": ", ".join(reasons),
            "pre_storage_gate_score": 100.0 if passed else 0.0,
        }

    def _article_text(self, article: Mapping[str, Any]) -> str:
        return " ".join(
            [
                str(article.get("title", "")),
                str(article.get("summary_raw", "")),
                str(article.get("article_content_raw", "")),
            ]
        ).lower()
