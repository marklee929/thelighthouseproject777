from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from integrations.rss import ArticleContentClient, RssFeedClient, RssFeedRegistryLoader
from repositories.interfaces import (
    ArticleRepositoryProtocol,
    IngestionRunRepositoryProtocol,
    RssFeedRepositoryProtocol,
    SourceRepositoryProtocol,
)

from .normalization import ArticleNormalizer, parse_datetime_value


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
        return self.ingest_feed_definitions(
            self.registry_loader.load_feed_definitions(enabled_only=True),
            item_limit=item_limit,
            triggered_by=triggered_by,
        )

    def ingest_feed_definitions(
        self,
        feed_definitions: Iterable[Dict[str, Any]],
        *,
        item_limit: Optional[int] = None,
        recent_hours: Optional[int] = None,
        triggered_by: str = "manual",
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for feed_definition in feed_definitions:
            source_id = self.source_repository.upsert_source(feed_definition)
            rss_feed_id = self.rss_feed_repository.upsert_feed(source_id, feed_definition)
            results.append(
                self._ingest_feed(
                    source_id=source_id,
                    rss_feed_id=rss_feed_id,
                    feed_definition=feed_definition,
                    item_limit=item_limit,
                    recent_hours=recent_hours,
                    triggered_by=triggered_by,
                )
            )
        return results

    def prepare_feed_articles(
        self,
        feed_definition: Dict[str, Any],
        *,
        item_limit: Optional[int] = None,
        recent_hours: Optional[int] = None,
    ) -> Dict[str, Any]:
        raw_items = self.feed_client.fetch(feed_definition)
        raw_items = self._filter_recent_items(raw_items, recent_hours)
        if item_limit is not None:
            raw_items = raw_items[: max(0, int(item_limit))]

        articles: List[Dict[str, Any]] = []
        items_failed = 0
        for raw_item in raw_items:
            try:
                article_fetch = self.article_client.fetch(str(raw_item.get("link", "")))
                article_payload = self.normalizer.normalize(feed_definition, raw_item, article_fetch)
                articles.append(article_payload)
            except Exception:
                items_failed += 1
        return {
            "items_fetched": len(raw_items),
            "items_failed": items_failed,
            "articles": articles,
        }

    def _ingest_feed(
        self,
        *,
        source_id: str,
        rss_feed_id: str,
        feed_definition: Dict[str, Any],
        item_limit: Optional[int],
        recent_hours: Optional[int],
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
            prepared = self.prepare_feed_articles(
                feed_definition,
                item_limit=item_limit,
                recent_hours=recent_hours,
            )
            items_fetched = int(prepared.get("items_fetched") or 0)
            items_failed = int(prepared.get("items_failed") or 0)
            for article_payload in prepared.get("articles", []):
                try:
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
                "recent_hours": recent_hours,
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

    def _filter_recent_items(self, raw_items: List[Dict[str, Any]], recent_hours: Optional[int]) -> List[Dict[str, Any]]:
        hours = int(recent_hours or 0)
        if hours <= 0:
            return raw_items
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        filtered: List[Dict[str, Any]] = []
        for raw_item in raw_items:
            published_at = parse_datetime_value(raw_item.get("published_at"))
            if published_at is None or published_at >= cutoff:
                filtered.append(raw_item)
        return filtered
