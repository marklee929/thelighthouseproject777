from __future__ import annotations

from typing import Any, Dict, List

from services.analysis import ArticleAnalysisService
from services.review import TelegramReviewService


class Phase1ArticleSelectionService:
    """Thin orchestration for analyze -> Telegram review -> Facebook candidate queue."""

    def __init__(
        self,
        *,
        analysis_service: ArticleAnalysisService,
        telegram_review_service: TelegramReviewService,
    ) -> None:
        self.analysis_service = analysis_service
        self.telegram_review_service = telegram_review_service

    def run_selection_batch(
        self,
        *,
        analysis_limit: int = 20,
        dispatch_limit: int = 5,
        min_final_score: float = 60.0,
    ) -> Dict[str, Any]:
        analysis_results = self.analysis_service.analyze_candidates(limit=analysis_limit)
        dispatch_results = self.telegram_review_service.send_top_candidates(limit=dispatch_limit, min_score=min_final_score)
        return {
            "analysis_results": analysis_results,
            "dispatch_results": dispatch_results,
        }

    def poll_review_updates(self, *, update_limit: int = 20, queue_limit: int = 20) -> Dict[str, List[Dict[str, Any]]]:
        decision_results = self.telegram_review_service.poll_decisions(limit=update_limit)
        queue_results = self.telegram_review_service.queue_confirmed_candidates(limit=queue_limit)
        return {
            "decision_results": decision_results,
            "queue_results": queue_results,
        }
