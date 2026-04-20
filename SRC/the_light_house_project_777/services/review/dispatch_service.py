from __future__ import annotations

from typing import Any, Dict, List

from integrations.telegram_client import TelegramClient
from repositories.interfaces import (
    ArticleRepositoryProtocol,
    ReviewerRepositoryProtocol,
    TelegramReviewDispatchRepositoryProtocol,
)
from services.news_collector.selection_policy import apply_selection_policy

from .message_builder import TelegramArticleReviewMessageBuilder
from .reviewer_registry import ReviewerRegistryLoader


class TelegramCandidateDispatchService:
    """Dispatches scored article candidates to the four Telegram reviewers."""

    def __init__(
        self,
        *,
        telegram_client: TelegramClient,
        article_repository: ArticleRepositoryProtocol,
        reviewer_repository: ReviewerRepositoryProtocol,
        dispatch_repository: TelegramReviewDispatchRepositoryProtocol,
        reviewer_registry_loader: ReviewerRegistryLoader,
        message_builder: TelegramArticleReviewMessageBuilder | None = None,
    ) -> None:
        self.telegram_client = telegram_client
        self.article_repository = article_repository
        self.reviewer_repository = reviewer_repository
        self.dispatch_repository = dispatch_repository
        self.reviewer_registry_loader = reviewer_registry_loader
        self.message_builder = message_builder or TelegramArticleReviewMessageBuilder()

    def sync_reviewers(self) -> List[Dict[str, Any]]:
        return self.reviewer_registry_loader.sync_reviewers(self.reviewer_repository)

    def dispatch_top_candidates(self, limit: int = 5, min_final_score: float = 60.0) -> List[Dict[str, Any]]:
        self.sync_reviewers()
        reviewers = [row for row in self.reviewer_repository.list_active_reviewers() if str(row.get("telegram_chat_id", "")).strip()]
        articles = self.article_repository.list_articles_for_selection_review(max(limit * 6, 24), min_final_score)
        articles = apply_selection_policy(articles, limit=limit, max_age_hours=1, bucket_minutes=10)
        results: List[Dict[str, Any]] = []
        for article in articles:
            article_sent = False
            reviewer_results: List[Dict[str, Any]] = []
            for reviewer in reviewers:
                message = self.message_builder.build(article, str(reviewer["reviewer_code"]))
                dispatch_id = self.dispatch_repository.create_dispatch(
                    {
                        "article_id": article.get("article_id"),
                        "reviewer_id": reviewer.get("reviewer_id"),
                        "telegram_chat_id": reviewer.get("telegram_chat_id"),
                        "dispatch_status": "queued",
                        "dispatch_payload": {
                            "article_snapshot": article,
                            "reviewer_snapshot": reviewer,
                        },
                    }
                )
                result = self.telegram_client.send_approval_card_to_chat(
                    str(reviewer.get("telegram_chat_id", "")),
                    title=message["title"],
                    body=message["body"],
                    buttons=message["buttons"],
                )
                if result.get("ok"):
                    article_sent = True
                    self.dispatch_repository.mark_dispatch_sent(dispatch_id, result)
                else:
                    self.dispatch_repository.mark_dispatch_failed(
                        dispatch_id,
                        str(result.get("error", "")).strip() or "telegram dispatch failed",
                        result,
                    )
                reviewer_results.append(
                    {
                        "reviewer_code": reviewer.get("reviewer_code"),
                        "dispatch_id": dispatch_id,
                        "telegram_result": result,
                    }
                )
            if article_sent:
                self.article_repository.update_selection_status(str(article["article_id"]), "review_queued")
            results.append({"article_id": article["article_id"], "sent": article_sent, "reviewer_results": reviewer_results})
        return results
