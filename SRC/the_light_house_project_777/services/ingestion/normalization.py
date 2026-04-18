from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "gclid", "fbclid"}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def normalize_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    query_pairs = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key.lower() not in TRACKING_PARAMS]
    normalized = urlunsplit(
        (
            parts.scheme.lower() or "https",
            parts.netloc.lower(),
            parts.path or "/",
            urlencode(query_pairs),
            "",
        )
    )
    return normalized.rstrip("/")


def parse_datetime_value(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    try:
        dt = parsedate_to_datetime(raw)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def build_hash(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


class ArticleNormalizer:
    """Transforms fetched RSS items into repository-safe article payloads."""

    def normalize(
        self,
        feed_definition: Dict[str, Any],
        raw_item: Dict[str, Any],
        article_fetch: Dict[str, Any],
    ) -> Dict[str, Any]:
        article_url = normalize_url(str(raw_item.get("link", "")).strip())
        canonical_url = normalize_url(str(article_fetch.get("canonical_url") or article_fetch.get("final_url") or article_url).strip())
        title = _clean_text(raw_item.get("title"))
        summary_raw = _clean_text(raw_item.get("summary"))
        article_text = str(article_fetch.get("text") or "").strip() or summary_raw
        published_at = parse_datetime_value(raw_item.get("published_at"))
        collected_at = parse_datetime_value(raw_item.get("raw_collected_at")) or _now_utc()
        dedupe_basis = canonical_url or article_url or f"{title}|{published_at.isoformat() if published_at else ''}"
        metadata = {
            "source_code": feed_definition.get("source_code"),
            "source_name": feed_definition.get("source_name"),
            "feed_code": feed_definition.get("feed_code"),
            "feed_name": feed_definition.get("feed_name"),
            "feed_title": raw_item.get("feed_title"),
            "feed_url": feed_definition.get("feed_url"),
            "raw_item": raw_item,
            "article_fetch": {
                "ok": bool(article_fetch.get("ok")),
                "requested_url": article_fetch.get("requested_url"),
                "final_url": article_fetch.get("final_url"),
                "canonical_url": article_fetch.get("canonical_url"),
                "status_code": article_fetch.get("status_code"),
                "error": article_fetch.get("error", ""),
                "excerpt": article_fetch.get("excerpt", ""),
            },
        }
        return {
            "external_id": _clean_text(raw_item.get("external_id")) or None,
            "title": title or canonical_url or article_url,
            "author_name": _clean_text(raw_item.get("author")),
            "language_code": feed_definition.get("feed_language_code", "en"),
            "region_code": feed_definition.get("feed_region_code", "global"),
            "article_url": article_url,
            "canonical_url": canonical_url or article_url,
            "published_at": published_at,
            "collected_at": collected_at,
            "url_hash": build_hash(canonical_url or article_url),
            "dedupe_hash": build_hash(dedupe_basis),
            "summary_raw": summary_raw,
            "article_content_html": str(article_fetch.get("html") or ""),
            "article_content_raw": article_text,
            "article_metadata": metadata,
        }
