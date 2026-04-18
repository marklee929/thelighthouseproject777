from __future__ import annotations

from typing import Any, Dict, List


class TelegramArticleReviewMessageBuilder:
    """Builds operator-facing Telegram review messages and callback payloads."""

    def build(self, article: Dict[str, Any], reviewer_code: str) -> Dict[str, Any]:
        summary = str(article.get("summary_raw", "")).strip()
        if len(summary) > 700:
            summary = f"{summary[:697].rstrip()}..."
        selection_summary = str(article.get("selection_summary", "")).strip()
        if len(selection_summary) > 500:
            selection_summary = f"{selection_summary[:497].rstrip()}..."
        title = "Christian News Candidate"
        body = (
            f"Source: {article.get('source_name', '-')}\n"
            f"Feed: {article.get('feed_name', '-')}\n"
            f"Final Score: {article.get('final_score', '-')}\n"
            f"Reaction: {article.get('reaction_score', '-')}\n"
            f"PLD Fit: {article.get('pld_fit_score', '-')}\n"
            f"Operational: {article.get('operational_score', '-')}\n"
            f"PLD Stage: {article.get('dominant_pld_stage', '-')}\n"
            f"Published: {article.get('published_at') or '-'}\n"
            f"URL: {article.get('canonical_url') or article.get('article_url')}\n\n"
            f"Title:\n{article.get('title', '')}\n\n"
            f"Summary:\n{summary or '-'}\n\n"
            f"Operator View:\n{selection_summary or '-'}"
        )
        buttons = [
            [
                {"text": "Confirm", "callback_data": self._callback_data(reviewer_code, "confirm", str(article.get("article_id", "")))},
                {"text": "Hold", "callback_data": self._callback_data(reviewer_code, "hold", str(article.get("article_id", "")))},
                {"text": "Reject", "callback_data": self._callback_data(reviewer_code, "reject", str(article.get("article_id", "")))},
            ]
        ]
        return {"title": title, "body": body, "buttons": buttons}

    def _callback_data(self, reviewer_code: str, action: str, article_id: str) -> str:
        return f"ar:{reviewer_code}:{action}:{article_id}"
