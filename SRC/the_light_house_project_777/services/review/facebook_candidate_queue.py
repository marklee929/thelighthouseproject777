from __future__ import annotations

from typing import Any, Dict, List

from repositories.interfaces import (
    ArticleRepositoryProtocol,
    ArticleReviewRepositoryProtocol,
    GeneratedContentRepositoryProtocol,
)


class FacebookCandidateQueueService:
    """Creates Facebook posting candidates from confirmed article reviews."""

    def __init__(
        self,
        *,
        article_repository: ArticleRepositoryProtocol,
        article_review_repository: ArticleReviewRepositoryProtocol,
        generated_content_repository: GeneratedContentRepositoryProtocol,
    ) -> None:
        self.article_repository = article_repository
        self.article_review_repository = article_review_repository
        self.generated_content_repository = generated_content_repository

    def queue_article(self, article_id: str) -> Dict[str, Any]:
        return self.queue_article_with_context(article_id)

    def queue_article_with_context(self, article_id: str, *, verse_suggestions: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
        existing = self.generated_content_repository.find_by_article_and_type(article_id, "facebook_post_candidate")
        if existing:
            return {"ok": True, "already_exists": True, "generated_content_id": existing["generated_content_id"]}
        if not self.article_review_repository.has_confirm_review(article_id):
            return {"ok": False, "error": "article_not_confirmed"}
        article = self.article_repository.get_article_by_id(article_id)
        if not article:
            return {"ok": False, "error": "article_not_found"}
        confirm_review = self.article_review_repository.find_latest_confirm_review(article_id)
        body_text = (
            f"Facebook candidate queue item\n"
            f"Source: {article.get('source_name', '-')}\n"
            f"Feed: {article.get('feed_name', '-')}\n"
            f"Final Score: {article.get('final_score', '-')}\n"
            f"PLD Stage: {article.get('dominant_pld_stage', '-')}\n"
            f"URL: {article.get('canonical_url') or article.get('article_url')}\n\n"
            f"Title:\n{article.get('title', '')}\n\n"
            f"Selection Summary:\n{article.get('selection_summary', '')}"
        )
        generated_content_id = self.generated_content_repository.create_generated_content(
            {
                "article_id": article_id,
                "source_review_id": confirm_review.get("article_review_id") if confirm_review else None,
                "content_type": "facebook_post_candidate",
                "generation_status": "draft",
                "generator_name": "phase1_article_selection",
                "generator_model": article.get("analysis_model"),
                "prompt_version": article.get("analysis_version"),
                "title": f"Facebook Candidate: {article.get('title', '')}",
                "body_text": body_text,
                "rendered_payload": {
                    "queue_type": "facebook_post_candidate",
                    "article_snapshot": {
                        "article_id": article.get("article_id"),
                        "source_code": article.get("source_code"),
                        "source_name": article.get("source_name"),
                        "feed_code": article.get("feed_code"),
                        "feed_name": article.get("feed_name"),
                        "title": article.get("title"),
                        "summary_raw": article.get("summary_raw"),
                        "canonical_url": article.get("canonical_url"),
                        "final_score": article.get("final_score"),
                        "reaction_score": article.get("reaction_score"),
                        "pld_fit_score": article.get("pld_fit_score"),
                        "operational_score": article.get("operational_score"),
                        "dominant_pld_stage": article.get("dominant_pld_stage"),
                        "selection_summary": article.get("selection_summary"),
                    },
                    "bible_verse_suggestions": verse_suggestions or [],
                    "confirm_review": confirm_review or {},
                },
                "version_no": 1,
                "created_by": "phase1_review_confirmation",
            }
        )
        self.article_repository.update_selection_status(article_id, "facebook_candidate_created")
        return {
            "ok": True,
            "generated_content_id": generated_content_id,
            "already_exists": False,
            "bible_verse_suggestions": verse_suggestions or [],
        }

    def queue_confirmed_articles(self, limit: int = 20) -> List[Dict[str, Any]]:
        queued: List[Dict[str, Any]] = []
        for article in self.article_repository.list_confirmed_articles_without_candidate(limit):
            queued.append({"article_id": article["article_id"], **self.queue_article(str(article["article_id"]))})
        return queued
