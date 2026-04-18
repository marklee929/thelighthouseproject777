from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List


class RssFeedRegistryLoader:
    """Loads the phase-1 RSS registry seeded from the project DOCX."""

    def __init__(self, registry_path: str | None = None) -> None:
        default_path = Path(__file__).resolve().parents[2] / "config" / "rss_feed_registry.json"
        env_path = os.getenv("LIGHTHOUSE_RSS_REGISTRY_PATH", "").strip()
        self.registry_path = Path(registry_path or env_path or default_path)

    def load_registry(self) -> Dict[str, Any]:
        with self.registry_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def load_sources(self) -> List[Dict[str, Any]]:
        registry = self.load_registry()
        return list(registry.get("sources") or [])

    def load_feed_definitions(self, *, enabled_only: bool = True) -> List[Dict[str, Any]]:
        definitions: List[Dict[str, Any]] = []
        for source in self.load_sources():
            base = {
                "source_code": str(source.get("source_code", "")).strip(),
                "source_name": str(source.get("source_name", "")).strip(),
                "source_type": str(source.get("source_type", "christian_news_rss")).strip(),
                "site_url": str(source.get("site_url", "")).strip(),
                "language_code": str(source.get("language_code", "en")).strip(),
                "region_code": str(source.get("region_code", "global")).strip(),
                "status": str(source.get("status", "active")).strip(),
                "metadata": dict(source.get("metadata") or {}),
            }
            for feed in source.get("feeds") or []:
                row = {
                    **base,
                    "feed_code": str(feed.get("feed_code", "")).strip(),
                    "feed_name": str(feed.get("feed_name", "")).strip(),
                    "feed_url": str(feed.get("feed_url", "")).strip(),
                    "feed_format": str(feed.get("feed_format", "rss")).strip(),
                    "category": str(feed.get("category", "christian_news")).strip(),
                    "feed_site_url": str(feed.get("site_url") or base["site_url"]).strip(),
                    "feed_language_code": str(feed.get("language_code") or base["language_code"]).strip(),
                    "feed_region_code": str(feed.get("region_code") or base["region_code"]).strip(),
                    "enabled": bool(feed.get("enabled", True)),
                    "feed_status": str(feed.get("status", base["status"])).strip(),
                    "notes": str(feed.get("notes", "")).strip(),
                    "feed_metadata": dict(feed.get("metadata") or {}),
                }
                if enabled_only and (not row["enabled"] or not row["feed_url"]):
                    continue
                definitions.append(row)
        return definitions
