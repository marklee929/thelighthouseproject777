from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping

from repositories.interfaces import (
    ArticleRepositoryProtocol,
    IngestionRunRepositoryProtocol,
    RssFeedRepositoryProtocol,
)
from services.analysis import LocalLlmTrioArticleAnalysisService
from services.ingestion.service import RssIngestionService

from .pre_storage_filter import NewsCollectorPreStorageFilter
from .retry_policy import NewsCollectorRetryPolicy
from .selection_policy import CYCLE_WINDOW_MINUTES, TARGET_CANDIDATES_PER_CYCLE, select_candidate_batch


class NewsCollectorCollectionService:
    """Runs the phase-1 News Collector pipeline: first gate, trio scoring, then top-five persistence."""

    DEFAULT_ITEM_LIMIT = 4
    DEFAULT_RECENT_HOURS = 1
    MAX_PERSISTED_CANDIDATES = TARGET_CANDIDATES_PER_CYCLE

    def __init__(
        self,
        *,
        rss_feed_repository: RssFeedRepositoryProtocol,
        article_repository: ArticleRepositoryProtocol,
        ingestion_run_repository: IngestionRunRepositoryProtocol,
        ingestion_service: RssIngestionService,
        pre_storage_filter: NewsCollectorPreStorageFilter,
        trio_analysis_service: LocalLlmTrioArticleAnalysisService,
        retry_policy: NewsCollectorRetryPolicy | None = None,
    ) -> None:
        self.rss_feed_repository = rss_feed_repository
        self.article_repository = article_repository
        self.ingestion_run_repository = ingestion_run_repository
        self.ingestion_service = ingestion_service
        self.pre_storage_filter = pre_storage_filter
        self.trio_analysis_service = trio_analysis_service
        self.retry_policy = retry_policy or NewsCollectorRetryPolicy()

    def collect_latest(self, *, item_limit: int | None = None, recent_hours: int | None = None) -> dict[str, Any]:
        feeds = self.rss_feed_repository.list_managed_feeds(enabled_only=True)
        if not feeds:
            return {"ok": False, "error": "no_connected_feeds"}

        effective_item_limit = max(1, int(item_limit or self.DEFAULT_ITEM_LIMIT))
        attempt_windows = self.retry_policy.build_attempt_windows(recent_hours or self.DEFAULT_RECENT_HOURS)
        final_result: dict[str, Any] | None = None
        final_window_hours = attempt_windows[-1]
        candidates_after_collection = 0
        attempts_run = 0

        for window_hours in attempt_windows:
            attempts_run += 1
            attempt_result = self._collect_latest_once(
                feeds=feeds,
                item_limit=effective_item_limit,
                recent_hours=window_hours,
            )
            candidates_after_collection = int(attempt_result.get("candidates_returned") or 0)
            final_result = attempt_result
            final_window_hours = int(attempt_result.get("current_window_hours") or window_hours)
            if candidates_after_collection > 0:
                break

        if final_result is None:
            return {"ok": False, "error": "collection_not_executed"}

        fallback_used = self.retry_policy.fallback_used(attempt_windows, final_window_hours)
        next_retry_recommended = candidates_after_collection < 1
        final_result.update(
            {
                "cycle_window_minutes": CYCLE_WINDOW_MINUTES,
                "target_candidates_per_cycle": self.MAX_PERSISTED_CANDIDATES,
                "attempt_window_hours": final_window_hours,
                "current_window_hours": final_window_hours,
                "fallback_used": fallback_used,
                "candidates_after_collection": candidates_after_collection,
                "candidates_returned": candidates_after_collection,
                "next_retry_recommended": next_retry_recommended,
                "next_cycle_recommended": next_retry_recommended,
                "message": self._build_cycle_message(
                    attempt_window_hours=final_window_hours,
                    candidates_after_collection=candidates_after_collection,
                    fallback_used=fallback_used,
                    attempts_run=attempts_run,
                ),
            }
        )
        return final_result

    def _collect_latest_once(
        self,
        *,
        feeds: list[dict[str, Any]],
        item_limit: int,
        recent_hours: int,
    ) -> dict[str, Any]:
        
        feed_runs: list[dict[str, Any]] = []
        staged_candidates: list[dict[str, Any]] = []
        totals = {
            "feeds_processed": len(feeds),
            "items_fetched": 0,
            "items_duplicate": 0,
            "items_failed": 0,
            "stage1_passed": 0,
            "stage1_rejected": 0,
            "stage2_passed": 0,
            "stage2_rejected": 0,
            "items_saved": 0,
            "items_ranked_out": 0,
        }

        for feed in feeds:
            feed_result = self._collect_feed_candidates(
                feed,
                item_limit=item_limit,
                recent_hours=recent_hours,
            )
            feed_runs.append(feed_result)
            totals["items_fetched"] += int(feed_result.get("items_fetched") or 0)
            totals["items_duplicate"] += int(feed_result.get("items_duplicate") or 0)
            totals["items_failed"] += int(feed_result.get("items_failed") or 0)
            totals["stage1_passed"] += int(feed_result.get("stage1_passed") or 0)
            totals["stage1_rejected"] += int(feed_result.get("stage1_rejected") or 0)
            totals["stage2_passed"] += int(feed_result.get("stage2_passed") or 0)
            totals["stage2_rejected"] += int(feed_result.get("stage2_rejected") or 0)
            staged_candidates.extend(feed_result.get("candidate_articles") or [])

        batch = select_candidate_batch(
            staged_candidates,
            limit=self.MAX_PERSISTED_CANDIDATES,
        )
        selected_candidates = list(batch["items"])
        selected_hashes = {str(row.get("dedupe_hash", "")).strip() for row in selected_candidates}
        saved_by_run: dict[str, int] = defaultdict(int)
        ranked_out_by_run: dict[str, int] = defaultdict(int)
        selected_preview: list[dict[str, Any]] = []
        persisted_hashes: set[str] = set()

        for candidate in staged_candidates:
            dedupe_hash = str(candidate.get("dedupe_hash", "")).strip()
            run_id = str(candidate.get("ingestion_run_id", "")).strip()
            if dedupe_hash and dedupe_hash in selected_hashes and dedupe_hash not in persisted_hashes:
                article_id = self.article_repository.create_article(
                    str(candidate.get("source_id")),
                    str(candidate.get("rss_feed_id")),
                    run_id,
                    candidate,
                )
                persisted_hashes.add(dedupe_hash)
                saved_by_run[run_id] += 1
                totals["items_saved"] += 1
                selected_preview.append(
                    {
                        "article_id": article_id,
                        "title": candidate.get("title"),
                        "source_name": candidate.get("source_name"),
                        "final_score": candidate.get("final_score"),
                        "selection_summary": candidate.get("selection_summary"),
                    }
                )
            else:
                ranked_out_by_run[run_id] += 1
                totals["items_ranked_out"] += 1

        for feed_result in feed_runs:
            run_id = str(feed_result.get("ingestion_run_id", "")).strip()
            feed_result["items_saved"] = saved_by_run.get(run_id, 0)
            feed_result["items_ranked_out"] = ranked_out_by_run.get(run_id, 0)
            self.ingestion_run_repository.complete_run(
                run_id,
                status=str(feed_result.get("status") or "completed"),
                items_fetched=int(feed_result.get("items_fetched") or 0),
                items_saved=int(feed_result.get("items_saved") or 0),
                items_duplicate=int(feed_result.get("items_duplicate") or 0),
                items_failed=int(feed_result.get("items_failed") or 0),
                error_message=str(feed_result.get("error_message") or ""),
                result_payload={
                    "recent_hours": recent_hours,
                    "item_limit": item_limit,
                    "stage1_passed": int(feed_result.get("stage1_passed") or 0),
                    "stage1_rejected": int(feed_result.get("stage1_rejected") or 0),
                    "stage2_passed": int(feed_result.get("stage2_passed") or 0),
                    "stage2_rejected": int(feed_result.get("stage2_rejected") or 0),
                    "items_ranked_out": int(feed_result.get("items_ranked_out") or 0),
                    "selection_mode": "pre_storage_gate_then_llm_trio_top5",
                    "active_models": self.trio_analysis_service.get_active_model_roles(),
                },
            )
            feed_result.pop("candidate_articles", None)

        return {
            "ok": True,
            "item_limit": item_limit,
            "recent_hours": recent_hours,
            "totals": totals,
            "feeds": feed_runs,
            "selected_candidates": selected_preview,
            "analysis_count": totals["stage2_passed"],
            "analysis_error": "",
            "selection_note": (
                "Collection stays broad in phase 1, rejecting only invalid or clearly unsafe items before analysis. "
                "The active local LLM trio then ranks candidates, and each cycle keeps up to five review cards."
            ),
            "active_models": self.trio_analysis_service.get_active_model_roles(),
            "cycle_window_minutes": batch["cycle_window_minutes"],
            "target_candidates_per_cycle": batch["target_candidates_per_cycle"],
            "current_window_hours": batch["current_window_hours"],
            "fallback_used": batch["fallback_used"],
            "candidates_returned": batch["candidates_returned"],
            "next_cycle_recommended": batch["next_cycle_recommended"],
        }

    def _build_cycle_message(
        self,
        *,
        attempt_window_hours: int,
        candidates_after_collection: int,
        fallback_used: bool,
        attempts_run: int,
    ) -> str:
        if candidates_after_collection > 0:
            if fallback_used:
                return (
                    f"News Collector used a fallback window of {attempt_window_hours} hour(s) and found "
                    f"{candidates_after_collection} reviewable candidate(s) after {attempts_run} attempt(s)."
                )
            return (
                f"News Collector found {candidates_after_collection} reviewable candidate(s) in the primary "
                f"{attempt_window_hours}-hour window."
            )
        return (
            f"No reviewable candidates were found after checking {attempts_run} window(s) up to "
            f"{attempt_window_hours} hours. Another retry is recommended in about "
            f"{self.retry_policy.RETRY_INTERVAL_MINUTES} minutes."
        )

    def _collect_feed_candidates(
        self,
        feed: Mapping[str, Any],
        *,
        item_limit: int,
        recent_hours: int,
    ) -> dict[str, Any]:
        run_id = self.ingestion_run_repository.start_run(
            source_id=str(feed.get("source_id")),
            rss_feed_id=str(feed.get("rss_feed_id")),
            feed_url_snapshot=str(feed.get("feed_url", "")),
            triggered_by="news_collector_ui",
            request_payload={
                "source_code": feed.get("source_code"),
                "feed_code": feed.get("feed_code"),
                "item_limit": item_limit,
                "recent_hours": recent_hours,
                "selection_mode": "pre_storage_gate_then_llm_trio_top5",
            },
        )
        feed_result = {
            "ingestion_run_id": run_id,
            "source_code": feed.get("source_code"),
            "feed_code": feed.get("feed_code"),
            "status": "completed",
            "items_fetched": 0,
            "items_saved": 0,
            "items_duplicate": 0,
            "items_failed": 0,
            "stage1_passed": 0,
            "stage1_rejected": 0,
            "stage2_passed": 0,
            "stage2_rejected": 0,
            "items_ranked_out": 0,
            "error_message": "",
            "candidate_articles": [],
        }
        try:
            prepared = self.ingestion_service.prepare_feed_articles(
                dict(feed),
                item_limit=item_limit,
                recent_hours=recent_hours,
            )
            feed_result["items_fetched"] = int(prepared.get("items_fetched") or 0)
            feed_result["items_failed"] = int(prepared.get("items_failed") or 0)
            for article_payload in prepared.get("articles", []):
                prepared_article = self._prepare_article_context(feed, run_id, article_payload)
                existing = self.article_repository.get_by_dedupe_hash(str(prepared_article.get("dedupe_hash", "")))
                if existing:
                    self.article_repository.mark_duplicate_seen(str(existing["article_id"]), run_id)
                    feed_result["items_duplicate"] += 1
                    continue

                stage1_result = self.pre_storage_filter.evaluate(prepared_article)
                if not stage1_result.get("passed"):
                    feed_result["stage1_rejected"] += 1
                    continue

                feed_result["stage1_passed"] += 1
                try:
                    analyzed_article = dict(prepared_article)
                    analyzed_article.update(
                        {
                            "article_metadata": self._merge_metadata(prepared_article, stage1_result),
                            "selection_status": "pending_analysis",
                        }
                    )
                    trio_analysis = self.trio_analysis_service.analyze_article(analyzed_article)
                    if str(trio_analysis.get("selection_status", "")).strip().lower() != "scored":
                        feed_result["stage2_rejected"] += 1
                        continue

                    analyzed_article.update(trio_analysis)
                    analyzed_article["selection_status"] = "scored"
                    analyzed_article["review_status"] = "pending"
                    analyzed_article["analysis_payload"] = self._merge_analysis_payload(
                        analyzed_article,
                        stage1_result,
                        trio_analysis,
                    )
                    feed_result["stage2_passed"] += 1
                    feed_result["candidate_articles"].append(analyzed_article)
                except Exception:
                    feed_result["items_failed"] += 1
        except Exception as exc:
            feed_result["status"] = "failed"
            feed_result["error_message"] = f"{type(exc).__name__}: {exc}"
        return feed_result

    def _prepare_article_context(
        self,
        feed: Mapping[str, Any],
        ingestion_run_id: str,
        article_payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        prepared = dict(article_payload)
        prepared.update(
            {
                "source_id": str(feed.get("source_id")),
                "rss_feed_id": str(feed.get("rss_feed_id")),
                "ingestion_run_id": ingestion_run_id,
                "source_code": feed.get("source_code"),
                "source_name": feed.get("source_name"),
                "source_status": feed.get("status", "active"),
                "feed_code": feed.get("feed_code"),
                "feed_name": feed.get("feed_name"),
                "feed_status": feed.get("feed_status", "active"),
                "feed_url": feed.get("feed_url"),
            }
        )
        if not prepared.get("canonical_url"):
            prepared["canonical_url"] = prepared.get("article_url")
        return prepared

    def _merge_metadata(self, article: Mapping[str, Any], stage1_result: Mapping[str, Any]) -> dict[str, Any]:
        metadata = dict(article.get("article_metadata") or {})
        metadata["pre_storage_filter"] = {
            "passed": bool(stage1_result.get("passed")),
            "gate_score": stage1_result.get("pre_storage_gate_score"),
            "reason": stage1_result.get("pre_storage_reason"),
            "age_minutes": stage1_result.get("pre_storage_age_minutes"),
            "popularity_proxy": stage1_result.get("pre_storage_popularity_proxy"),
            "dominant_pld_stage": stage1_result.get("pre_storage_dominant_pld_stage"),
            "pld_breakdown": stage1_result.get("pre_storage_pld_breakdown"),
        }
        return metadata

    def _merge_analysis_payload(
        self,
        article: Mapping[str, Any],
        stage1_result: Mapping[str, Any],
        trio_analysis: Mapping[str, Any],
    ) -> dict[str, Any]:
        payload = dict(trio_analysis.get("analysis_payload") or {})
        payload["pre_storage_filter"] = {
            "gate_score": stage1_result.get("pre_storage_gate_score"),
            "reason": stage1_result.get("pre_storage_reason"),
            "age_minutes": stage1_result.get("pre_storage_age_minutes"),
            "popularity_proxy": stage1_result.get("pre_storage_popularity_proxy"),
        }
        payload["article_snapshot"] = {
            "title": article.get("title"),
            "source_name": article.get("source_name"),
            "published_at": str(article.get("published_at") or ""),
            "canonical_url": article.get("canonical_url") or article.get("article_url"),
        }
        return payload
