from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from integrations.telegram_client import TelegramClient
from repositories.interfaces import (
    ArticleRepositoryProtocol,
    ArticleReviewRepositoryProtocol,
    ReviewerRepositoryProtocol,
    SystemConfigRepositoryProtocol,
    TelegramReviewDispatchRepositoryProtocol,
)

from .facebook_candidate_queue import FacebookCandidateQueueService


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class TelegramReviewDecisionService:
    """Persists Telegram confirm/reject/hold decisions and creates Facebook candidates after confirmation."""

    TELEGRAM_OFFSET_KEY = "telegram.review.last_update_id"

    def __init__(
        self,
        *,
        telegram_client: TelegramClient,
        article_repository: ArticleRepositoryProtocol,
        article_review_repository: ArticleReviewRepositoryProtocol,
        reviewer_repository: ReviewerRepositoryProtocol,
        dispatch_repository: TelegramReviewDispatchRepositoryProtocol,
        system_config_repository: SystemConfigRepositoryProtocol,
        facebook_candidate_queue_service: FacebookCandidateQueueService,
    ) -> None:
        self.telegram_client = telegram_client
        self.article_repository = article_repository
        self.article_review_repository = article_review_repository
        self.reviewer_repository = reviewer_repository
        self.dispatch_repository = dispatch_repository
        self.system_config_repository = system_config_repository
        self.facebook_candidate_queue_service = facebook_candidate_queue_service

    def poll_decisions(self, limit: int = 20) -> List[Dict[str, Any]]:
        current_offset = self.system_config_repository.get_value(self.TELEGRAM_OFFSET_KEY, {"last_update_id": 0})
        last_update_id = int((current_offset or {}).get("last_update_id", 0) or 0)
        updates = self.telegram_client.get_updates(offset=last_update_id + 1 if last_update_id else None, limit=limit, timeout=2)
        processed: List[Dict[str, Any]] = []
        if not updates.get("ok"):
            return processed
        for update in updates.get("result") or []:
            update_id = int(update.get("update_id", 0) or 0)
            callback_query = update.get("callback_query") or {}
            result = self._process_callback(callback_query, update)
            if result:
                processed.append(result)
            if update_id > last_update_id:
                last_update_id = update_id
        self.system_config_repository.set_value(self.TELEGRAM_OFFSET_KEY, {"last_update_id": last_update_id})
        return processed

    def _process_callback(self, callback_query: Dict[str, Any], update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        parsed = self._parse_callback(callback_query.get("data"))
        if not parsed:
            return None
        message = callback_query.get("message") or {}
        chat = message.get("chat") or {}
        telegram_chat_id = str(chat.get("id", "")).strip()
        telegram_message_id = int(message.get("message_id", 0) or 0)
        dispatch = None
        if telegram_chat_id and telegram_message_id:
            dispatch = self.dispatch_repository.get_dispatch_by_message(telegram_chat_id, telegram_message_id)
        reviewer = self.reviewer_repository.get_by_reviewer_code(parsed["reviewer_code"])
        article_id = str(parsed["article_id"])
        review_id = self.article_review_repository.create_review(
            {
                "article_id": article_id,
                "decision": parsed["action"],
                "review_channel": "telegram",
                "reviewer_id": str((callback_query.get("from") or {}).get("id", "")) or None,
                "reviewer_code": reviewer.get("reviewer_code") if reviewer else parsed["reviewer_code"],
                "reviewer_display_name": reviewer.get("display_name") if reviewer else parsed["reviewer_code"],
                "review_note": f"Telegram {parsed['action']} by {parsed['reviewer_code']}",
                "decision_payload": update,
                "review_context": {
                    "callback_data": callback_query.get("data"),
                    "dispatch": dispatch or {},
                },
                "dispatch_id": dispatch.get("telegram_review_dispatch_id") if dispatch else None,
                "telegram_chat_id": telegram_chat_id or None,
                "telegram_message_id": telegram_message_id or None,
                "reviewed_at": _now_utc(),
            }
        )
        if dispatch:
            self.dispatch_repository.mark_dispatch_acted(str(dispatch["telegram_review_dispatch_id"]), update)
        callback_query_id = str(callback_query.get("id", "")).strip()
        if callback_query_id:
            self.telegram_client.answer_callback_query(callback_query_id, text=f"Recorded: {parsed['action']}")
        has_confirm = self.article_review_repository.has_confirm_review(article_id)
        if has_confirm:
            self.article_repository.update_review_status(article_id, "good")
            self.article_repository.update_selection_status(article_id, "review_confirmed")
            queue_result = self.facebook_candidate_queue_service.queue_article(article_id)
        elif parsed["action"] == "reject":
            self.article_repository.update_review_status(article_id, "bad")
            self.article_repository.update_selection_status(article_id, "review_rejected")
            queue_result = {"ok": False, "error": "not_confirmed"}
        else:
            self.article_repository.update_review_status(article_id, "hold")
            self.article_repository.update_selection_status(article_id, "review_hold")
            queue_result = {"ok": False, "error": "on_hold"}
        return {
            "article_id": article_id,
            "decision": parsed["action"],
            "reviewer_code": parsed["reviewer_code"],
            "article_review_id": review_id,
            "queue_result": queue_result,
        }

    def _parse_callback(self, callback_data: Any) -> Optional[Dict[str, str]]:
        raw = str(callback_data or "").strip()
        parts = raw.split(":")
        if len(parts) != 4 or parts[0] != "ar":
            return None
        reviewer_code = parts[1].strip()
        action = parts[2].strip().lower()
        article_id = parts[3].strip()
        if action not in {"confirm", "reject", "hold"} or not reviewer_code or not article_id:
            return None
        return {"reviewer_code": reviewer_code, "action": action, "article_id": article_id}
