from __future__ import annotations

from typing import Any, Mapping


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _trim_summary(value: str, max_chars: int = 280) -> str:
    text = _clean_text(value)
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3].rstrip()}..."


def _pick_image_url(article_metadata: Mapping[str, Any]) -> str:
    raw_item = article_metadata.get("raw_item") if isinstance(article_metadata, Mapping) else {}
    if not isinstance(raw_item, Mapping):
        raw_item = {}
    direct_candidates = [
        raw_item.get("image_url"),
        raw_item.get("thumbnail_url"),
        raw_item.get("thumbnail"),
        article_metadata.get("image_url") if isinstance(article_metadata, Mapping) else None,
        article_metadata.get("thumbnail_url") if isinstance(article_metadata, Mapping) else None,
    ]
    for candidate in direct_candidates:
        url = str(candidate or "").strip()
        if url:
            return url
    media_thumbnail = raw_item.get("media_thumbnail")
    if isinstance(media_thumbnail, list):
        for item in media_thumbnail:
            if isinstance(item, Mapping):
                url = str(item.get("url") or "").strip()
                if url:
                    return url
    enclosures = raw_item.get("enclosures")
    if isinstance(enclosures, list):
        for item in enclosures:
            if isinstance(item, Mapping):
                url = str(item.get("url") or "").strip()
                media_type = str(item.get("type") or "").lower()
                if url and media_type.startswith("image/"):
                    return url
    return ""


def build_news_collector_candidate(row: Mapping[str, Any]) -> dict[str, Any]:
    article_metadata = row.get("article_metadata")
    metadata = article_metadata if isinstance(article_metadata, Mapping) else {}
    summary = _trim_summary(
        str(row.get("review_summary") or "").strip()
        or str(row.get("summary_raw") or "").strip()
        or str(row.get("article_content_raw") or "").strip()
    )
    original_url = str(row.get("canonical_url") or row.get("article_url") or "").strip()
    return {
        "article_id": str(row.get("article_id") or ""),
        "title": _clean_text(row.get("title")),
        "source_name": _clean_text(row.get("source_name")),
        "feed_name": _clean_text(row.get("feed_name")),
        "published_at": row.get("published_at"),
        "collected_at": row.get("collected_at"),
        "summary": summary,
        "review_summary": _clean_text(row.get("review_summary")),
        "suggested_angle": _clean_text(row.get("suggested_angle")),
        "suggested_question": _clean_text(row.get("suggested_question")),
        "operator_note": _clean_text(row.get("operator_note") or row.get("review_note")),
        "original_url": original_url,
        "image_url": _pick_image_url(metadata),
        "status": str(row.get("selection_status") or row.get("review_status") or "pending"),
        "review_status": str(row.get("review_status") or "pending"),
        "selection_status": str(row.get("selection_status") or "pending_analysis"),
        "reaction_score": row.get("reaction_score"),
        "pld_fit_score": row.get("pld_fit_score"),
        "operational_score": row.get("operational_score"),
        "final_score": row.get("final_score"),
        "popularity_proxy": row.get("popularity_proxy"),
        "age_minutes": row.get("age_minutes"),
        "dominant_pld_stage": str(row.get("dominant_pld_stage") or "").strip(),
        "selection_summary": _clean_text(row.get("selection_summary")),
        "why_selected": _clean_text(row.get("selection_summary") or row.get("selection_gate_reason")),
        "latest_review_decision": str(row.get("latest_review_decision") or "").strip(),
    }
