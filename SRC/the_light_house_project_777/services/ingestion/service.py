from __future__ import annotations

from typing import Any, Dict, List, Optional

from integrations.rss import ArticleContentClient, RssFeedClient, RssFeedRegistryLoader
from repositories.interfaces import (
    ArticleRepositoryProtocol,
    IngestionRunRepositoryProtocol,
    RssFeedRepositoryProtocol,
    SourceRepositoryProtocol,
)

from .normalization import ArticleNormalizer


class RssIngestionService:
    """Thin orchestration over registry loading, RSS fetching, normalization, and persistence."""

    def __init__(
        self,
        *,
        registry_loader: RssFeedRegistryLoader,
        feed_client: RssFeedClient,
        article_client: ArticleContentClient,
        source_repository: SourceRepositoryProtocol,
        rss_feed_repository: RssFeedRepositoryProtocol,
        article_repository: ArticleRepositoryProtocol,
        ingestion_run_repository: IngestionRunRepositoryProtocol,
        normalizer: Optional[ArticleNormalizer] = None,
    ) -> None:
        self.registry_loader = registry_loader
        self.feed_client = feed_client
        self.article_client = article_client
        self.source_repository = source_repository
        self.rss_feed_repository = rss_feed_repository
        self.article_repository = article_repository
        self.ingestion_run_repository = ingestion_run_repository
        self.normalizer = normalizer or ArticleNormalizer()

    def sync_registry(self) -> List[Dict[str, str]]:
        synced: List[Dict[str, str]] = []
        for feed_definition in self.registry_loader.load_feed_definitions(enabled_only=False):
            source_id = self.source_repository.upsert_source(feed_definition)
            if feed_definition.get("feed_url"):
                rss_feed_id = self.rss_feed_repository.upsert_feed(source_id, feed_definition)
                synced.append(
                    {
                        "source_id": source_id,
                        "rss_feed_id": rss_feed_id,
                        "source_code": str(feed_definition.get("source_code", "")),
                        "feed_code": str(feed_definition.get("feed_code", "")),
                    }
                )
        return synced

    def ingest_enabled_feeds(self, *, item_limit: Optional[int] = None, triggered_by: str = "manual") -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for feed_definition in self.registry_loader.load_feed_definitions(enabled_only=True):
            source_id = self.source_repository.upsert_source(feed_definition)
            rss_feed_id = self.rss_feed_repository.upsert_feed(source_id, feed_definition)
            results.append(
                self._ingest_feed(
                    source_id=source_id,
                    rss_feed_id=rss_feed_id,
                    feed_definition=feed_definition,
                    item_limit=item_limit,
                    triggered_by=triggered_by,
                )
            )
        return results

    def _ingest_feed(
        self,
        *,
        source_id: str,
        rss_feed_id: str,
        feed_definition: Dict[str, Any],
        item_limit: Optional[int],
        triggered_by: str,
    ) -> Dict[str, Any]:
        ingestion_run_id = self.ingestion_run_repository.start_run(
            source_id=source_id,
            rss_feed_id=rss_feed_id,
            feed_url_snapshot=str(feed_definition.get("feed_url", "")),
            triggered_by=triggered_by,
            request_payload={
                "source_code": feed_definition.get("source_code"),
                "feed_code": feed_definition.get("feed_code"),
                "item_limit": item_limit,
            },
        )
        items_fetched = 0
        items_saved = 0
        items_duplicate = 0
        items_failed = 0
        error_message = ""
        try:
            raw_items = self.feed_client.fetch(feed_definition)
            if item_limit is not None:
                raw_items = raw_items[: max(0, int(item_limit))]
            items_fetched = len(raw_items)
            for raw_item in raw_items:
                try:
                    article_fetch = self.article_client.fetch(str(raw_item.get("link", "")))
                    article_payload = self.normalizer.normalize(feed_definition, raw_item, article_fetch)
                    existing = self.article_repository.get_by_dedupe_hash(str(article_payload.get("dedupe_hash", "")))
                    if existing:
                        self.article_repository.mark_duplicate_seen(str(existing["article_id"]), ingestion_run_id)
                        items_duplicate += 1
                        continue
                    self.article_repository.create_article(source_id, rss_feed_id, ingestion_run_id, article_payload)
                    items_saved += 1
                except Exception:
                    items_failed += 1
            status = "completed"
        except Exception as exc:
            status = "failed"
            error_message = f"{type(exc).__name__}: {exc}"
        self.ingestion_run_repository.complete_run(
            ingestion_run_id,
            status=status,
            items_fetched=items_fetched,
            items_saved=items_saved,
            items_duplicate=items_duplicate,
            items_failed=items_failed,
            error_message=error_message,
            result_payload={
                "source_code": feed_definition.get("source_code"),
                "feed_code": feed_definition.get("feed_code"),
            },
        )
        return {
            "ingestion_run_id": ingestion_run_id,
            "source_code": feed_definition.get("source_code"),
            "feed_code": feed_definition.get("feed_code"),
            "status": status,
            "items_fetched": items_fetched,
            "items_saved": items_saved,
            "items_duplicate": items_duplicate,
            "items_failed": items_failed,
            "error_message": error_message,
        }
