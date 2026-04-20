from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import log10
from typing import Any, Iterable, Mapping


SOURCE_POPULARITY_BASE = {
    "christian post": 74.0,
    "christianity today": 70.0,
    "vatican news": 68.0,
    "relevant": 63.0,
    "allafrica": 59.0,
    "uca news": 54.0,
}

VIEW_KEYS = (
    "view_count",
    "views",
    "read_count",
    "reads",
    "hit_count",
    "hits",
    "engagement_count",
)

CYCLE_WINDOW_MINUTES = 10
TARGET_CANDIDATES_PER_CYCLE = 5
FALLBACK_WINDOWS = (1, 3, 6)
MAX_SAME_SOURCE_PER_CYCLE = 2

SAFETY_REJECT_TERMS = {
    "graphic violence",
    "casino",
    "betting",
    "porn",
    "miracle cure",
    "click here",
    "buy now",
    "destroy them",
    "civil war now",
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _clip(score: float) -> float:
    return round(max(0.0, min(100.0, float(score))), 2)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _find_numeric(payload: Any) -> float | None:
    if isinstance(payload, Mapping):
        for key in VIEW_KEYS:
            if key in payload:
                try:
                    return float(payload[key])
                except Exception:
                    continue
        for value in payload.values():
            found = _find_numeric(value)
            if found is not None:
                return found
    if isinstance(payload, list):
        for item in payload:
            found = _find_numeric(item)
            if found is not None:
                return found
    return None


def _age_minutes(article: Mapping[str, Any], now: datetime) -> float:
    published = _to_datetime(article.get("published_at")) or _to_datetime(article.get("collected_at"))
    if not published:
        return 9999.0
    return max(0.0, (now - published).total_seconds() / 60.0)


def popularity_proxy(article: Mapping[str, Any]) -> float:
    metadata = article.get("article_metadata")
    raw_views = _find_numeric(metadata) if isinstance(metadata, Mapping) else None
    if raw_views is not None and raw_views > 0:
        return _clip(34.0 + log10(max(raw_views, 1.0)) * 18.0)

    source_name = _clean_text(article.get("source_name")).lower()
    source_base = 55.0
    for key, score in SOURCE_POPULARITY_BASE.items():
        if key in source_name:
            source_base = score
            break

    headline = " ".join([_clean_text(article.get("title")), _clean_text(article.get("summary_raw"))]).lower()
    urgency_bonus = 0.0
    if any(token in headline for token in ("breaking", "arrest", "crisis", "attack", "court", "election", "war")):
        urgency_bonus += 6.0
    if any(token in headline for token in ("hope", "healing", "community", "families", "church", "mission")):
        urgency_bonus += 4.0

    age = _age_minutes(article, _now_utc())
    freshness_bonus = max(0.0, 14.0 - age / 8.0)
    return _clip(source_base + urgency_bonus + freshness_bonus)


def selection_priority(article: Mapping[str, Any]) -> float:
    final_score = float(article.get("final_score") or 0.0)
    pld_fit_score = float(article.get("pld_fit_score") or 0.0)
    reaction_score = float(article.get("reaction_score") or 0.0)
    operational_score = float(article.get("operational_score") or 0.0)
    pop = popularity_proxy(article)
    age = _age_minutes(article, _now_utc())
    freshness_score = max(0.0, 100.0 - age * 0.9)
    return _clip(
        0.30 * freshness_score
        + 0.24 * final_score
        + 0.18 * pld_fit_score
        + 0.12 * reaction_score
        + 0.10 * operational_score
        + 0.06 * pop
    )


def _gate_reason(article: Mapping[str, Any], pop: float, age_minutes: float, current_window_hours: int) -> str:
    reasons = [f"within {current_window_hours}h review window", f"published {int(age_minutes)}m ago"]
    if float(article.get("final_score") or 0.0) > 0:
        reasons.append(f"final {float(article.get('final_score') or 0.0):.1f}")
    if float(article.get("pld_fit_score") or 0.0) > 0:
        reasons.append(f"PLD {float(article.get('pld_fit_score') or 0.0):.1f}")
    reasons.append(f"pop {pop:.1f}")
    return ", ".join(reasons)


def _is_safety_rejected(article: Mapping[str, Any]) -> bool:
    text = " ".join(
        [
            _clean_text(article.get("title")),
            _clean_text(article.get("summary_raw")),
            _clean_text(article.get("article_content_raw")),
            _clean_text(article.get("hard_reject_reason")),
        ]
    ).lower()
    if any(token in text for token in SAFETY_REJECT_TERMS):
        return True
    moderation_risk = float(((article.get("operational_breakdown") or {}).get("moderation_platform_risk")) or 0.0)
    brand_safety = float(((article.get("operational_breakdown") or {}).get("brand_safety")) or 100.0)
    return moderation_risk >= 90.0 or brand_safety <= 20.0


def _base_eligible(article: Mapping[str, Any], now: datetime) -> dict[str, Any] | None:
    row = dict(article)
    published = _to_datetime(row.get("published_at")) or _to_datetime(row.get("collected_at"))
    if published is None:
        return None
    latest_review = str(row.get("latest_review_decision") or "").strip().lower()
    review_status = str(row.get("review_status") or "pending").strip().lower()
    selection_status = str(row.get("selection_status") or "").strip().lower()
    if latest_review:
        return None
    if review_status != "pending":
        return None
    if selection_status in {"review_rejected", "review_confirmed", "review_dropped", "facebook_candidate_created", "hard_rejected"}:
        return None
    if _is_safety_rejected(row):
        return None

    age_minutes = _age_minutes(row, now)
    pop = popularity_proxy(row)
    row["age_minutes"] = round(age_minutes, 1)
    row["popularity_proxy"] = pop
    row["selection_priority"] = selection_priority(row)
    return row


def select_candidate_batch(
    rows: Iterable[Mapping[str, Any]],
    *,
    limit: int = TARGET_CANDIDATES_PER_CYCLE,
    fallback_windows: tuple[int, ...] = FALLBACK_WINDOWS,
    per_source_limit: int = MAX_SAME_SOURCE_PER_CYCLE,
) -> dict[str, Any]:
    now = _now_utc()
    normalized_rows = [row for row in (_base_eligible(item, now) for item in rows) if row]
    normalized_rows.sort(
        key=lambda item: (
            float(item.get("selection_priority") or 0.0),
            -float(item.get("age_minutes") or 9999.0),
            float(item.get("final_score") or 0.0),
        ),
        reverse=True,
    )

    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    source_counts: dict[str, int] = {}
    current_window_hours = fallback_windows[-1]

    for window_hours in fallback_windows:
        window_cutoff = now - timedelta(hours=max(1, int(window_hours)))
        added_in_window = False
        for row in normalized_rows:
            article_id = str(row.get("article_id") or row.get("dedupe_hash") or "").strip()
            if article_id in selected_ids:
                continue
            published = _to_datetime(row.get("published_at")) or _to_datetime(row.get("collected_at"))
            if published is None or published < window_cutoff:
                continue
            source_code = _clean_text(row.get("source_code") or row.get("source_name") or "unknown").lower()
            if source_counts.get(source_code, 0) >= per_source_limit:
                continue
            row["selection_gate_reason"] = _gate_reason(row, float(row.get("popularity_proxy") or 0.0), float(row.get("age_minutes") or 9999.0), window_hours)
            selected.append(row)
            selected_ids.add(article_id)
            source_counts[source_code] = source_counts.get(source_code, 0) + 1
            added_in_window = True
            if len(selected) >= limit:
                current_window_hours = window_hours
                break
        if selected:
            current_window_hours = window_hours
        if len(selected) >= limit or added_in_window:
            if len(selected) >= 1:
                break

    if not selected and normalized_rows:
        current_window_hours = fallback_windows[-1]
        for row in normalized_rows:
            article_id = str(row.get("article_id") or row.get("dedupe_hash") or "").strip()
            if article_id in selected_ids:
                continue
            source_code = _clean_text(row.get("source_code") or row.get("source_name") or "unknown").lower()
            if source_counts.get(source_code, 0) >= per_source_limit:
                continue
            row["selection_gate_reason"] = _gate_reason(row, float(row.get("popularity_proxy") or 0.0), float(row.get("age_minutes") or 9999.0), current_window_hours)
            selected.append(row)
            selected_ids.add(article_id)
            source_counts[source_code] = source_counts.get(source_code, 0) + 1
            if len(selected) >= limit:
                break

    fallback_used = bool(selected and current_window_hours > fallback_windows[0])
    candidates_returned = len(selected)
    return {
        "items": selected[:limit],
        "cycle_window_minutes": CYCLE_WINDOW_MINUTES,
        "target_candidates_per_cycle": limit,
        "current_window_hours": current_window_hours,
        "fallback_used": fallback_used,
        "candidates_returned": candidates_returned,
        "next_cycle_recommended": candidates_returned < 1,
    }


def apply_selection_policy(
    rows: Iterable[Mapping[str, Any]],
    *,
    limit: int,
    max_age_hours: int = 1,
    bucket_minutes: int = 10,
) -> list[dict[str, Any]]:
    del max_age_hours, bucket_minutes
    return list(select_candidate_batch(rows, limit=limit)["items"])
