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


def _bucket_start(article: Mapping[str, Any], bucket_minutes: int) -> tuple[int, int]:
    published = _to_datetime(article.get("published_at")) or _to_datetime(article.get("collected_at")) or _now_utc()
    bucket_minute = (published.minute // bucket_minutes) * bucket_minutes
    return (int(published.timestamp() // 3600), bucket_minute)


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

    headline = " ".join(
        [
            _clean_text(article.get("title")),
            _clean_text(article.get("summary_raw")),
        ]
    ).lower()
    urgency_bonus = 0.0
    if any(token in headline for token in ("breaking", "arrest", "crisis", "attack", "court", "election", "war")):
        urgency_bonus += 7.0
    if any(token in headline for token in ("hope", "healing", "community", "families", "church", "mission")):
        urgency_bonus += 4.0

    age = _age_minutes(article, _now_utc())
    freshness_bonus = max(0.0, 12.0 - age / 5.0)
    return _clip(source_base + urgency_bonus + freshness_bonus)


def selection_priority(article: Mapping[str, Any]) -> float:
    final_score = float(article.get("final_score") or 0.0)
    pld_fit_score = float(article.get("pld_fit_score") or 0.0)
    reaction_score = float(article.get("reaction_score") or 0.0)
    operational_score = float(article.get("operational_score") or 0.0)
    pop = popularity_proxy(article)
    age = _age_minutes(article, _now_utc())
    freshness_score = max(0.0, 100.0 - age * 1.4)
    return _clip(
        0.42 * final_score
        + 0.24 * pld_fit_score
        + 0.14 * pop
        + 0.10 * freshness_score
        + 0.05 * reaction_score
        + 0.05 * operational_score
    )


def _gate_reason(article: Mapping[str, Any], pop: float, age_minutes: float) -> str:
    reasons: list[str] = []
    if age_minutes <= 60.0:
        reasons.append(f"published {int(age_minutes)}m ago")
    if float(article.get("pld_fit_score") or 0.0) >= 68.0:
        reasons.append("strong PLD fit")
    if pop >= 62.0:
        reasons.append("high popularity proxy")
    if float(article.get("final_score") or 0.0) >= 72.0:
        reasons.append("high final score")
    return ", ".join(reasons) or "selected for review"


def apply_selection_policy(
    rows: Iterable[Mapping[str, Any]],
    *,
    limit: int,
    max_age_hours: int = 1,
    bucket_minutes: int = 10,
) -> list[dict[str, Any]]:
    now = _now_utc()
    recent_cutoff = now - timedelta(hours=max(1, max_age_hours))
    hold_rows: list[dict[str, Any]] = []
    fresh_rows: list[dict[str, Any]] = []

    for row in rows:
        article = dict(row)
        published = _to_datetime(article.get("published_at")) or _to_datetime(article.get("collected_at"))
        age_minutes = _age_minutes(article, now)
        pop = popularity_proxy(article)
        priority = selection_priority(article)
        pld_fit = float(article.get("pld_fit_score") or 0.0)
        final_score = float(article.get("final_score") or 0.0)
        latest_review = str(article.get("latest_review_decision") or "").strip().lower()
        selection_status = str(article.get("selection_status") or "").strip().lower()
        article["age_minutes"] = round(age_minutes, 1)
        article["popularity_proxy"] = pop
        article["selection_priority"] = priority
        article["selection_gate_reason"] = _gate_reason(article, pop, age_minutes)

        is_hold = latest_review == "hold" or selection_status == "review_hold"
        if is_hold:
            hold_rows.append(article)
            continue

        if published is None or published < recent_cutoff:
            continue
        if not (pld_fit >= 68.0 or pop >= 62.0 or final_score >= 72.0):
            continue
        fresh_rows.append(article)

    hold_rows.sort(key=lambda item: (float(item.get("selection_priority") or 0.0), float(item.get("final_score") or 0.0)), reverse=True)
    fresh_rows.sort(
        key=lambda item: (
            float(item.get("selection_priority") or 0.0),
            float(item.get("final_score") or 0.0),
            -float(item.get("age_minutes") or 9999.0),
        ),
        reverse=True,
    )

    selected: list[dict[str, Any]] = []
    seen_buckets: set[tuple[int, int]] = set()
    for article in hold_rows:
        if len(selected) >= limit:
            return selected[:limit]
        selected.append(article)

    for article in fresh_rows:
        if len(selected) >= limit:
            break
        bucket = _bucket_start(article, max(1, bucket_minutes))
        if bucket in seen_buckets:
            continue
        seen_buckets.add(bucket)
        selected.append(article)

    return selected[:limit]
