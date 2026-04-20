from __future__ import annotations

from typing import Any, Dict, List

from repositories.interfaces import ArticleRepositoryProtocol, ReviewCardRepositoryProtocol
from services.news_collector.selection_policy import select_candidate_batch

from .review_card_builder import build_review_card_projection, build_telegram_preview_card_payload


class TelegramPreviewCardService:
    def __init__(
        self,
        *,
        article_repository: ArticleRepositoryProtocol,
        review_card_repository: ReviewCardRepositoryProtocol,
    ) -> None:
        self.article_repository = article_repository
        self.review_card_repository = review_card_repository

    def sync_preview_cards(self, limit: int = 5) -> Dict[str, Any]:
        fetch_limit = max(limit * 6, 48)
        rows = self.article_repository.list_articles_for_telegram_preview(fetch_limit)
        batch = select_candidate_batch(rows, limit=limit)
        generated: List[Dict[str, Any]] = []
        skipped = 0
        for article in batch.get("items", []):
            existing = self.review_card_repository.get_by_article_channel_type(
                str(article.get("article_id") or ""),
                "telegram",
                "article_review",
            )
            if existing:
                skipped += 1
                continue
            payload = build_telegram_preview_card_payload(article)
            card_id = self.review_card_repository.create_review_card(
                {
                    "card_id": payload["card_id"],
                    "article_id": payload["article_id"],
                    "channel": "telegram",
                    "card_type": "article_review",
                    "payload_json": payload,
                    "status": "preview",
                }
            )
            generated.append({"card_id": card_id, "article_id": payload["article_id"]})
        return {
            "ok": True,
            "generated_count": len(generated),
            "skipped_count": skipped,
            "generated": generated,
            "selection": {
                key: value
                for key, value in batch.items()
                if key != "items"
            },
        }

    def list_preview_cards(self, limit: int = 5) -> Dict[str, Any]:
        rows = self.review_card_repository.list_review_cards(channel="telegram", card_type="article_review", limit=limit)
        items = [build_review_card_projection(row) for row in rows]
        return {"ok": True, "items": items, "count": len(items)}

    def sync_and_list_preview_cards(self, limit: int = 5) -> Dict[str, Any]:
        sync_result = self.sync_preview_cards(limit=limit)
        listing = self.list_preview_cards(limit=limit)
        return {**listing, "generated_count": sync_result.get("generated_count", 0), "skipped_count": sync_result.get("skipped_count", 0)}

    def mark_card_status_for_review_action(self, article_id: str, decision: str) -> None:
        status_map = {
            "confirm": "approved",
            "reject": "rejected",
            "hold": "hold",
            "drop": "expired",
        }
        status = status_map.get(str(decision or "").strip().lower())
        if not status:
            return
        self.review_card_repository.update_card_status_by_article(article_id, "telegram", "article_review", status)
