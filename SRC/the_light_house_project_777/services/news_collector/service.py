from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from repositories.interfaces import ArticleRepositoryProtocol, ArticleReviewRepositoryProtocol
from services.review.facebook_candidate_queue import FacebookCandidateQueueService

from .candidate_projection import build_news_collector_candidate


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class NewsCollectorReviewService:
    """Owns candidate loading and operator review actions for the pre-social news collector screen."""

    def __init__(
        self,
        *,
        article_repository: ArticleRepositoryProtocol,
        article_review_repository: ArticleReviewRepositoryProtocol,
        facebook_candidate_queue_service: FacebookCandidateQueueService,
    ) -> None:
        self.article_repository = article_repository
        self.article_review_repository = article_review_repository
        self.facebook_candidate_queue_service = facebook_candidate_queue_service

    def list_candidates(self, limit: int = 24) -> list[dict[str, Any]]:
        rows = self.article_repository.list_news_collector_candidates(limit)
        return [build_news_collector_candidate(row) for row in rows]

    def approve_candidate(self, article_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        article = self.article_repository.get_article_by_id(article_id)
        if not article:
            return {"ok": False, "error": "article_not_found"}
        review_id = self.article_review_repository.create_review(
            self._build_review_record(article_id=article_id, decision="confirm", action="approve", payload=payload)
        )
        self.article_repository.update_review_status(article_id, "good")
        self.article_repository.update_selection_status(article_id, "review_confirmed")
        queue_result = self.facebook_candidate_queue_service.queue_article(article_id)
        result = {"ok": bool(queue_result.get("ok")), "article_review_id": review_id, "queue_result": queue_result}
        if not queue_result.get("ok"):
            result["error"] = queue_result.get("error", "queue_failed")
        return result

    def modify_candidate(self, article_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        article = self.article_repository.get_article_by_id(article_id)
        if not article:
            return {"ok": False, "error": "article_not_found"}
        review_id = self.article_review_repository.create_review(
            self._build_review_record(article_id=article_id, decision="hold", action="modify", payload=payload)
        )
        self.article_repository.update_review_status(article_id, "hold")
        self.article_repository.update_selection_status(article_id, "review_hold")
        return {"ok": True, "article_review_id": review_id}

    def reject_candidate(self, article_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        article = self.article_repository.get_article_by_id(article_id)
        if not article:
            return {"ok": False, "error": "article_not_found"}
        review_id = self.article_review_repository.create_review(
            self._build_review_record(article_id=article_id, decision="reject", action="reject", payload=payload)
        )
        self.article_repository.update_review_status(article_id, "bad")
        self.article_repository.update_selection_status(article_id, "review_rejected")
        return {"ok": True, "article_review_id": review_id}

    def _build_review_record(
        self,
        *,
        article_id: str,
        decision: str,
        action: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        review_summary = str(payload.get("review_summary") or "").strip()
        suggested_angle = str(payload.get("suggested_angle") or "").strip()
        suggested_question = str(payload.get("suggested_question") or "").strip()
        operator_note = str(payload.get("operator_note") or "").strip()
        reviewer_code = str(payload.get("reviewer_code") or "operator_ui").strip() or "operator_ui"
        reviewer_display_name = str(payload.get("reviewer_display_name") or "Operator UI").strip() or "Operator UI"
        return {
            "article_id": article_id,
            "decision": decision,
            "review_channel": "news_collector_ui",
            "reviewer_id": reviewer_code,
            "reviewer_code": reviewer_code,
            "reviewer_display_name": reviewer_display_name,
            "review_note": operator_note,
            "review_summary": review_summary,
            "suggested_angle": suggested_angle,
            "suggested_question": suggested_question,
            "operator_note": operator_note,
            "decision_payload": {
                "ui_module": "News Collector",
                "action": action,
                "review_summary": review_summary,
                "suggested_angle": suggested_angle,
                "suggested_question": suggested_question,
                "operator_note": operator_note,
            },
            "review_context": {
                "ui_module": "News Collector",
                "action": action,
                "persisted_from": "operator_console",
            },
            "reviewed_at": payload.get("reviewed_at") or _now_utc(),
        }
