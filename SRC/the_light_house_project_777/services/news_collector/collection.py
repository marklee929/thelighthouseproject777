from __future__ import annotations

from typing import Any

from repositories.interfaces import RssFeedRepositoryProtocol
from services.analysis import ArticleAnalysisService
from services.ingestion.service import RssIngestionService


class NewsCollectorCollectionService:
    """Runs Christian RSS collection for the News Collector screen."""

    DEFAULT_ITEM_LIMIT = 4
    DEFAULT_RECENT_HOURS = 1

    def __init__(
        self,
        *,
        rss_feed_repository: RssFeedRepositoryProtocol,
        ingestion_service: RssIngestionService,
        analysis_service: ArticleAnalysisService,
    ) -> None:
        self.rss_feed_repository = rss_feed_repository
        self.ingestion_service = ingestion_service
        self.analysis_service = analysis_service

    def collect_latest(self, *, item_limit: int | None = None, recent_hours: int | None = None) -> dict[str, Any]:
        feeds = self.rss_feed_repository.list_managed_feeds(enabled_only=True)
        if not feeds:
            return {"ok": False, "error": "no_connected_feeds"}
        effective_item_limit = max(1, int(item_limit or self.DEFAULT_ITEM_LIMIT))
        effective_recent_hours = max(1, int(recent_hours or self.DEFAULT_RECENT_HOURS))
        ingestion_results = self.ingestion_service.ingest_feed_definitions(
            feeds,
            item_limit=effective_item_limit,
            recent_hours=effective_recent_hours,
            triggered_by="news_collector_ui",
        )
        totals = {
            "feeds_processed": len(ingestion_results),
            "items_fetched": sum(int(row.get("items_fetched") or 0) for row in ingestion_results),
            "items_saved": sum(int(row.get("items_saved") or 0) for row in ingestion_results),
            "items_duplicate": sum(int(row.get("items_duplicate") or 0) for row in ingestion_results),
            "items_failed": sum(int(row.get("items_failed") or 0) for row in ingestion_results),
        }
        analysis_result: list[dict[str, Any]] = []
        analysis_error = ""
        if totals["items_saved"] > 0:
            try:
                analysis_result = self.analysis_service.analyze_candidates(limit=max(totals["items_saved"], 1))
            except Exception as exc:
                analysis_error = f"{type(exc).__name__}: {exc}"
        return {
            "ok": True,
            "item_limit": effective_item_limit,
            "recent_hours": effective_recent_hours,
            "totals": totals,
            "feeds": ingestion_results,
            "analysis_count": len(analysis_result),
            "analysis_error": analysis_error,
            "selection_note": (
                "Collection now focuses on articles published within the last hour, then ranks them for "
                "PLD compatibility, safe curiosity framing, and operator-friendly review volume."
            ),
        }
