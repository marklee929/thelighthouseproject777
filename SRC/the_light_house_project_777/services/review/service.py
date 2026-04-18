from __future__ import annotations

from typing import Any, Dict, List

from .decision_service import TelegramReviewDecisionService
from .dispatch_service import TelegramCandidateDispatchService
from .facebook_candidate_queue import FacebookCandidateQueueService
from .reviewer_registry import ReviewerRegistryLoader


class TelegramReviewService:
    """Thin orchestration layer for reviewer sync, Telegram delivery, and confirmation queueing."""

    def __init__(
        self,
        *,
        reviewer_registry_loader: ReviewerRegistryLoader,
        dispatch_service: TelegramCandidateDispatchService,
        decision_service: TelegramReviewDecisionService,
        facebook_candidate_queue_service: FacebookCandidateQueueService,
    ) -> None:
        self.reviewer_registry_loader = reviewer_registry_loader
        self.dispatch_service = dispatch_service
        self.decision_service = decision_service
        self.facebook_candidate_queue_service = facebook_candidate_queue_service

    def sync_reviewers(self) -> List[Dict[str, Any]]:
        return self.dispatch_service.sync_reviewers()

    def send_top_candidates(self, limit: int = 10, min_score: float = 60.0) -> List[Dict[str, Any]]:
        return self.dispatch_service.dispatch_top_candidates(limit=limit, min_final_score=min_score)

    def poll_decisions(self, limit: int = 20) -> List[Dict[str, Any]]:
        return self.decision_service.poll_decisions(limit=limit)

    def queue_confirmed_candidates(self, limit: int = 20) -> List[Dict[str, Any]]:
        return self.facebook_candidate_queue_service.queue_confirmed_articles(limit=limit)
