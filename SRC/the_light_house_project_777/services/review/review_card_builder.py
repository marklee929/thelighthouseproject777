from __future__ import annotations

from datetime import date, datetime
import uuid
from typing import Any, Mapping


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _trim_summary(value: Any, max_chars: int = 320) -> str:
    text = _clean_text(value)
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3].rstrip()}..."


def _json_scalar(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _extract_suggestion_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, Mapping)]
    if isinstance(value, Mapping):
        nested = value.get("bible_verse_suggestions") or value.get("verse_suggestions") or value.get("verses")
        if isinstance(nested, list):
            return [row for row in nested if isinstance(row, Mapping)]
    return []


def extract_existing_verse_suggestions(article: Mapping[str, Any], limit: int = 3) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for value in (
        article.get("analysis_payload"),
        article.get("article_metadata"),
    ):
        candidates.extend(_extract_suggestion_list(value))
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in candidates:
        reference = _clean_text(row.get("verse_reference") or row.get("reference"))
        if not reference or reference in seen:
            continue
        seen.add(reference)
        normalized.append(
            {
                "verse_reference": reference,
                "verse_text": _clean_text(row.get("verse_text") or row.get("text")),
                "verse_reason": _clean_text(row.get("verse_reason") or row.get("reason")),
                "verse_url": _clean_text(row.get("verse_url") or row.get("url")),
                "rank": int(row.get("rank") or len(normalized) + 1),
            }
        )
        if len(normalized) >= max(1, min(limit, 3)):
            break
    return normalized


def build_telegram_preview_card_payload(article: Mapping[str, Any]) -> dict[str, Any]:
    article_id = str(article.get("article_id") or "").strip()
    verses = extract_existing_verse_suggestions(article, limit=3)
    default_selected_verse = verses[0] if verses else None
    summary = _trim_summary(
        article.get("review_summary")
        or article.get("summary_raw")
        or article.get("article_content_raw")
    )
    why_selected = _clean_text(article.get("selection_summary") or article.get("review_summary"))
    title = _clean_text(article.get("title"))
    source_name = _clean_text(article.get("source_name"))
    cta_draft = (
        _clean_text(article.get("suggested_question"))
        or f"Would you send '{title}' to Telegram reviewers for discernment?"
    )
    return {
        "card_id": f"telegram-preview-{uuid.uuid5(uuid.NAMESPACE_URL, article_id)}",
        "article_id": article_id,
        "title": title,
        "source": source_name,
        "published_at": _json_scalar(article.get("published_at")),
        "summary": summary,
        "article_url": _clean_text(article.get("canonical_url") or article.get("article_url")),
        "verses": verses,
        "default_selected_verse": default_selected_verse,
        "verse_status": "available" if verses else "missing",
        "why_selected": why_selected,
        "cta_draft": cta_draft,
        "status": "preview",
    }


def build_review_card_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = row.get("payload_json")
    card_payload = payload if isinstance(payload, Mapping) else {}
    return {
        "card_id": str(row.get("card_id") or card_payload.get("card_id") or ""),
        "article_id": str(row.get("article_id") or card_payload.get("article_id") or ""),
        "channel": str(row.get("channel") or "telegram"),
        "card_type": str(row.get("card_type") or "article_review"),
        "status": str(row.get("status") or card_payload.get("status") or "preview"),
        "created_at": row.get("created_at"),
        "sent_at": row.get("sent_at"),
        "title": _clean_text(card_payload.get("title")),
        "source": _clean_text(card_payload.get("source")),
        "published_at": card_payload.get("published_at"),
        "summary": _clean_text(card_payload.get("summary")),
        "article_url": _clean_text(card_payload.get("article_url")),
        "verses": card_payload.get("verses") if isinstance(card_payload.get("verses"), list) else [],
        "default_selected_verse": card_payload.get("default_selected_verse"),
        "verse_status": str(card_payload.get("verse_status") or "missing"),
        "why_selected": _clean_text(card_payload.get("why_selected")),
        "cta_draft": _clean_text(card_payload.get("cta_draft")),
    }
