from __future__ import annotations

import re
from hashlib import sha1
from typing import Any, Mapping
from urllib.parse import urlparse

from integrations.rss import RssFeedRegistryLoader
from repositories.interfaces import RssFeedRepositoryProtocol, SourceRepositoryProtocol, SystemConfigRepositoryProtocol


def _slugify(value: str, *, fallback: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return text or fallback


def _host_code(url: str, fallback: str) -> str:
    hostname = urlparse(str(url or "").strip()).hostname or ""
    hostname = hostname.replace("www.", "")
    return _slugify(hostname, fallback=fallback)


class NewsCollectorFeedManagementService:
    """Manages phase-1 RSS sources for the News Collector operator screen."""

    REGISTRY_SEEDED_KEY = "news_collector.registry_seeded"

    def __init__(
        self,
        *,
        registry_loader: RssFeedRegistryLoader,
        source_repository: SourceRepositoryProtocol,
        rss_feed_repository: RssFeedRepositoryProtocol,
        system_config_repository: SystemConfigRepositoryProtocol,
    ) -> None:
        self.registry_loader = registry_loader
        self.source_repository = source_repository
        self.rss_feed_repository = rss_feed_repository
        self.system_config_repository = system_config_repository

    def list_feeds(self) -> list[dict[str, Any]]:
        self._seed_registry_once()
        feeds = self.rss_feed_repository.list_managed_feeds(enabled_only=False)
        items: list[dict[str, Any]] = []
        for feed in feeds:
            article_count = self.rss_feed_repository.count_articles_for_feed(str(feed.get("rss_feed_id") or ""))
            items.append(
                {
                    "rss_feed_id": str(feed.get("rss_feed_id") or ""),
                    "source_id": str(feed.get("source_id") or ""),
                    "source_code": str(feed.get("source_code") or ""),
                    "source_name": str(feed.get("source_name") or ""),
                    "feed_code": str(feed.get("feed_code") or ""),
                    "feed_name": str(feed.get("feed_name") or ""),
                    "feed_url": str(feed.get("feed_url") or ""),
                    "site_url": str(feed.get("feed_site_url") or feed.get("site_url") or ""),
                    "feed_format": str(feed.get("feed_format") or "rss"),
                    "enabled": bool(feed.get("enabled")),
                    "status": str(feed.get("feed_status") or ""),
                    "notes": str(feed.get("notes") or ""),
                    "article_count": article_count,
                }
            )
        return items

    def add_feed(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        feed_url = str(payload.get("feed_url") or "").strip()
        if not feed_url:
            return {"ok": False, "error": "feed_url_required"}
        existing = self.rss_feed_repository.find_feed_by_url(feed_url)
        if existing:
            if self._is_deleted(existing):
                self.rss_feed_repository.update_feed_connection(str(existing.get("rss_feed_id") or ""), False)
                feed = self.rss_feed_repository.get_feed_by_id(str(existing.get("rss_feed_id") or ""))
                return {"ok": True, "already_exists": True, "reactivated": True, "feed": feed}
            return {"ok": True, "already_exists": True, "feed": existing}

        source_name = str(payload.get("source_name") or "").strip() or _host_code(feed_url, fallback="christian_source")
        feed_name = str(payload.get("feed_name") or "").strip() or source_name
        site_url = str(payload.get("site_url") or "").strip()
        source_code = _slugify(
            str(payload.get("source_code") or source_name or _host_code(site_url or feed_url, fallback="christian_source")),
            fallback="christian_source",
        )
        feed_code_seed = _slugify(str(payload.get("feed_code") or feed_name), fallback="feed")
        feed_code = f"{feed_code_seed}_{sha1(feed_url.encode('utf-8')).hexdigest()[:8]}"

        feed_definition = {
            "source_code": source_code,
            "source_name": source_name,
            "source_type": "christian_news_rss",
            "site_url": site_url,
            "language_code": str(payload.get("language_code") or "en").strip() or "en",
            "region_code": str(payload.get("region_code") or "global").strip() or "global",
            "status": "active",
            "metadata": {"origin": "news_collector_manual"},
            "feed_code": feed_code,
            "feed_name": feed_name,
            "feed_url": feed_url,
            "feed_format": str(payload.get("feed_format") or "rss").strip() or "rss",
            "category": str(payload.get("category") or "christian_news").strip() or "christian_news",
            "feed_site_url": site_url,
            "feed_language_code": str(payload.get("language_code") or "en").strip() or "en",
            "feed_region_code": str(payload.get("region_code") or "global").strip() or "global",
            "enabled": False,
            "feed_status": "paused",
            "notes": str(payload.get("notes") or "Added from News Collector UI").strip(),
            "feed_metadata": {"origin": "news_collector_manual"},
        }
        source_id = self.source_repository.upsert_source(feed_definition)
        rss_feed_id = self.rss_feed_repository.upsert_feed(source_id, feed_definition)
        return {"ok": True, "already_exists": False, "feed": self.rss_feed_repository.get_feed_by_id(rss_feed_id)}

    def delete_feed(self, rss_feed_id: str) -> dict[str, Any]:
        feed = self.rss_feed_repository.get_feed_by_id(rss_feed_id)
        if not feed:
            return {"ok": False, "error": "rss_feed_not_found"}
        self.rss_feed_repository.archive_feed(rss_feed_id)
        return {"ok": True, "rss_feed_id": rss_feed_id}

    def set_connection(self, rss_feed_id: str, enabled: bool) -> dict[str, Any]:
        feed = self.rss_feed_repository.get_feed_by_id(rss_feed_id)
        if not feed:
            return {"ok": False, "error": "rss_feed_not_found"}
        self.rss_feed_repository.update_feed_connection(rss_feed_id, enabled)
        updated = self.rss_feed_repository.get_feed_by_id(rss_feed_id)
        return {"ok": True, "feed": updated, "enabled": bool(enabled)}

    def _seed_registry_once(self) -> None:
        if bool(self.system_config_repository.get_value(self.REGISTRY_SEEDED_KEY, False)):
            return
        for feed_definition in self.registry_loader.load_feed_definitions(enabled_only=False):
            if not str(feed_definition.get("feed_url") or "").strip():
                continue
            source_id = self.source_repository.upsert_source(feed_definition)
            self.rss_feed_repository.upsert_feed(source_id, feed_definition)
        self.system_config_repository.set_value(self.REGISTRY_SEEDED_KEY, True)

    def _is_deleted(self, feed: Mapping[str, Any]) -> bool:
        metadata = feed.get("feed_metadata")
        if isinstance(metadata, Mapping):
            return bool(metadata.get("deleted"))
        return False
