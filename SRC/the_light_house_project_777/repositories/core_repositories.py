from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from .postgres import PostgresConnectionFactory


def _json_payload(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False)


def _source_status(value: Any) -> str:
    status = str(value or "active").strip() or "active"
    if status in {"active", "paused", "discovery_required"}:
        return status
    if status == "verification_required":
        return "discovery_required"
    return "active"


def _feed_status(value: Any) -> str:
    status = str(value or "active").strip() or "active"
    if status in {"active", "paused", "discovery_required", "verification_required"}:
        return status
    return "active"


class PostgresSourceRepository:
    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def upsert_source(self, source: Mapping[str, Any]) -> str:
        query = """
            INSERT INTO core.sources (
                source_code,
                source_name,
                source_type,
                site_url,
                language_code,
                region_code,
                status,
                metadata_json,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP)
            ON CONFLICT (source_code)
            DO UPDATE SET
                source_name = EXCLUDED.source_name,
                source_type = EXCLUDED.source_type,
                site_url = EXCLUDED.site_url,
                language_code = EXCLUDED.language_code,
                region_code = EXCLUDED.region_code,
                status = EXCLUDED.status,
                metadata_json = EXCLUDED.metadata_json,
                updated_at = CURRENT_TIMESTAMP
            RETURNING source_id
        """
        params = (
            source.get("source_code"),
            source.get("source_name"),
            source.get("source_type", "christian_news_rss"),
            source.get("site_url"),
            source.get("language_code", "en"),
            source.get("region_code", "global"),
            _source_status(source.get("status", "active")),
            _json_payload(source.get("metadata")),
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        return str(row["source_id"])


class PostgresRssFeedRepository:
    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def upsert_feed(self, source_id: str, feed: Mapping[str, Any]) -> str:
        query = """
            INSERT INTO core.rss_feeds (
                source_id,
                feed_code,
                feed_name,
                feed_url,
                site_url,
                feed_format,
                category,
                language_code,
                region_code,
                enabled,
                status,
                notes,
                metadata_json,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP)
            ON CONFLICT (source_id, feed_code)
            DO UPDATE SET
                feed_name = EXCLUDED.feed_name,
                feed_url = EXCLUDED.feed_url,
                site_url = EXCLUDED.site_url,
                feed_format = EXCLUDED.feed_format,
                category = EXCLUDED.category,
                language_code = EXCLUDED.language_code,
                region_code = EXCLUDED.region_code,
                enabled = EXCLUDED.enabled,
                status = EXCLUDED.status,
                notes = EXCLUDED.notes,
                metadata_json = EXCLUDED.metadata_json,
                updated_at = CURRENT_TIMESTAMP
            RETURNING rss_feed_id
        """
        params = (
            source_id,
            feed.get("feed_code"),
            feed.get("feed_name"),
            feed.get("feed_url"),
            feed.get("feed_site_url"),
            feed.get("feed_format", "rss"),
            feed.get("category", "christian_news"),
            feed.get("feed_language_code", "en"),
            feed.get("feed_region_code", "global"),
            bool(feed.get("enabled", True)),
            _feed_status(feed.get("feed_status", "active")),
            feed.get("notes", ""),
            _json_payload(feed.get("feed_metadata")),
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        return str(row["rss_feed_id"])

    def list_managed_feeds(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        query = """
            SELECT
                f.rss_feed_id,
                f.source_id,
                s.source_code,
                s.source_name,
                s.source_type,
                s.site_url,
                s.language_code,
                s.region_code,
                s.status,
                s.metadata_json,
                f.feed_code,
                f.feed_name,
                f.feed_url,
                f.site_url AS feed_site_url,
                f.feed_format,
                f.category,
                f.language_code AS feed_language_code,
                f.region_code AS feed_region_code,
                f.enabled,
                f.status AS feed_status,
                f.notes,
                f.metadata_json AS feed_metadata,
                f.created_at,
                f.updated_at,
                COALESCE((f.metadata_json ->> 'deleted')::boolean, false) AS deleted
            FROM core.rss_feeds AS f
            JOIN core.sources AS s
              ON s.source_id = f.source_id
            WHERE COALESCE((f.metadata_json ->> 'deleted')::boolean, false) = false
        """
        params: list[Any] = []
        if enabled_only:
            query += " AND f.enabled = true"
        query += " ORDER BY f.enabled DESC, s.source_name ASC, f.feed_name ASC"
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def find_feed_by_url(self, feed_url: str) -> Optional[dict[str, Any]]:
        query = """
            SELECT
                f.rss_feed_id,
                f.source_id,
                s.source_code,
                s.source_name,
                f.feed_code,
                f.feed_name,
                f.feed_url,
                f.enabled,
                f.status AS feed_status,
                f.metadata_json AS feed_metadata
            FROM core.rss_feeds AS f
            JOIN core.sources AS s
              ON s.source_id = f.source_id
            WHERE f.feed_url = %s
            LIMIT 1
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (feed_url,))
                row = cur.fetchone()
        return dict(row) if row else None

    def get_feed_by_id(self, rss_feed_id: str) -> Optional[dict[str, Any]]:
        query = """
            SELECT
                f.rss_feed_id,
                f.source_id,
                s.source_code,
                s.source_name,
                s.site_url,
                f.feed_code,
                f.feed_name,
                f.feed_url,
                f.site_url AS feed_site_url,
                f.feed_format,
                f.category,
                f.language_code AS feed_language_code,
                f.region_code AS feed_region_code,
                f.enabled,
                f.status AS feed_status,
                f.notes,
                f.metadata_json AS feed_metadata
            FROM core.rss_feeds AS f
            JOIN core.sources AS s
              ON s.source_id = f.source_id
            WHERE f.rss_feed_id = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (rss_feed_id,))
                row = cur.fetchone()
        return dict(row) if row else None

    def update_feed_connection(self, rss_feed_id: str, enabled: bool) -> None:
        query = """
            UPDATE core.rss_feeds
            SET enabled = %s,
                status = %s,
                metadata_json = COALESCE(metadata_json, '{}'::jsonb) - 'deleted',
                updated_at = CURRENT_TIMESTAMP
            WHERE rss_feed_id = %s
        """
        status = "active" if enabled else "paused"
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (enabled, status, rss_feed_id))

    def archive_feed(self, rss_feed_id: str) -> None:
        query = """
            UPDATE core.rss_feeds
            SET enabled = false,
                status = 'paused',
                metadata_json = jsonb_set(COALESCE(metadata_json, '{}'::jsonb), '{deleted}', 'true'::jsonb, true),
                updated_at = CURRENT_TIMESTAMP
            WHERE rss_feed_id = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (rss_feed_id,))

    def count_articles_for_feed(self, rss_feed_id: str) -> int:
        query = """
            SELECT COUNT(*) AS article_count
            FROM core.articles
            WHERE rss_feed_id = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (rss_feed_id,))
                row = cur.fetchone()
        return int(row["article_count"]) if row else 0


class PostgresArticleRepository:
    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def list_candidates_for_analysis(self, limit: int) -> list[dict[str, Any]]:
        query = """
            SELECT
                a.article_id,
                a.title,
                a.summary_raw,
                a.article_content_raw,
                a.article_url,
                a.canonical_url,
                a.published_at,
                a.collected_at,
                a.language_code,
                a.region_code,
                a.article_metadata,
                s.source_id,
                s.source_code,
                s.source_name,
                s.status AS source_status,
                s.metadata_json AS source_metadata,
                f.rss_feed_id,
                f.feed_code,
                f.feed_name,
                f.status AS feed_status
            FROM core.articles AS a
            JOIN core.sources AS s
              ON s.source_id = a.source_id
            JOIN core.rss_feeds AS f
              ON f.rss_feed_id = a.rss_feed_id
            WHERE a.selection_status = 'pending_analysis'
               OR a.analyzed_at IS NULL
            ORDER BY a.published_at DESC NULLS LAST, a.collected_at DESC
            LIMIT %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def list_news_collector_candidates(self, limit: int) -> list[dict[str, Any]]:
        query = """
            WITH latest_reviews AS (
                SELECT DISTINCT ON (ar.article_id)
                    ar.article_id,
                    ar.article_review_id,
                    ar.decision,
                    ar.reviewed_at,
                    ar.review_summary,
                    ar.suggested_angle,
                    ar.suggested_question,
                    ar.operator_note,
                    ar.review_note
                FROM core.article_reviews AS ar
                ORDER BY ar.article_id, ar.reviewed_at DESC, ar.created_at DESC
            ),
            published_articles AS (
                SELECT DISTINCT gc.article_id
                FROM content.generated_contents AS gc
                JOIN system.publish_logs AS pl
                  ON pl.generated_content_id = gc.generated_content_id
                WHERE pl.publish_status = 'published'
            ),
            candidate_articles AS (
                SELECT DISTINCT article_id
                FROM content.generated_contents
                WHERE content_type = 'facebook_post_candidate'
            )
            SELECT
                a.article_id,
                a.title,
                a.summary_raw,
                a.article_content_raw,
                a.article_url,
                a.canonical_url,
                a.published_at,
                a.collected_at,
                a.review_status,
                a.selection_status,
                a.reaction_score,
                a.pld_fit_score,
                a.operational_score,
                a.final_score,
                a.dominant_pld_stage,
                a.selection_summary,
                a.article_metadata,
                s.source_name,
                s.source_code,
                f.feed_name,
                f.feed_code,
                lr.article_review_id AS latest_review_id,
                lr.decision AS latest_review_decision,
                lr.review_summary,
                lr.suggested_angle,
                lr.suggested_question,
                lr.operator_note,
                lr.review_note,
                lr.reviewed_at AS latest_reviewed_at
            FROM core.articles AS a
            JOIN core.sources AS s
              ON s.source_id = a.source_id
            JOIN core.rss_feeds AS f
              ON f.rss_feed_id = a.rss_feed_id
            LEFT JOIN latest_reviews AS lr
              ON lr.article_id = a.article_id
            LEFT JOIN candidate_articles AS ca
              ON ca.article_id = a.article_id
            LEFT JOIN published_articles AS pa
              ON pa.article_id = a.article_id
            WHERE a.selection_status NOT IN ('hard_rejected', 'review_rejected', 'facebook_candidate_created')
              AND a.review_status IN ('pending', 'hold')
              AND pa.article_id IS NULL
              AND ca.article_id IS NULL
            ORDER BY
                a.final_score DESC NULLS LAST,
                a.published_at DESC NULLS LAST,
                a.collected_at DESC
            LIMIT %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def get_by_dedupe_hash(self, dedupe_hash: str) -> Optional[dict[str, Any]]:
        query = """
            SELECT article_id, dedupe_hash, review_status, recommendation_score
            FROM core.articles
            WHERE dedupe_hash = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (dedupe_hash,))
                row = cur.fetchone()
        return dict(row) if row else None

    def create_article(self, source_id: str, rss_feed_id: str, ingestion_run_id: str, article: Mapping[str, Any]) -> str:
        query = """
            INSERT INTO core.articles (
                source_id,
                rss_feed_id,
                first_ingestion_run_id,
                last_seen_ingestion_run_id,
                external_id,
                title,
                author_name,
                language_code,
                region_code,
                article_url,
                canonical_url,
                published_at,
                collected_at,
                url_hash,
                dedupe_hash,
                summary_raw,
                article_content_html,
                article_content_raw,
                article_metadata,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            RETURNING article_id
        """
        params = (
            source_id,
            rss_feed_id,
            ingestion_run_id,
            ingestion_run_id,
            article.get("external_id"),
            article.get("title"),
            article.get("author_name"),
            article.get("language_code", "en"),
            article.get("region_code", "global"),
            article.get("article_url"),
            article.get("canonical_url"),
            article.get("published_at"),
            article.get("collected_at"),
            article.get("url_hash"),
            article.get("dedupe_hash"),
            article.get("summary_raw"),
            article.get("article_content_html"),
            article.get("article_content_raw"),
            _json_payload(article.get("article_metadata")),
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        return str(row["article_id"])

    def mark_duplicate_seen(self, article_id: str, ingestion_run_id: str) -> None:
        query = """
            UPDATE core.articles
            SET last_seen_ingestion_run_id = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE article_id = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (ingestion_run_id, article_id))

    def list_candidates_for_recommendation(self, limit: int) -> list[dict[str, Any]]:
        query = """
            SELECT
                article_id,
                title,
                summary_raw,
                article_content_raw,
                article_url,
                canonical_url,
                published_at,
                collected_at,
                article_metadata
            FROM core.articles
            WHERE recommendation_score IS NULL
            ORDER BY published_at DESC NULLS LAST, collected_at DESC
            LIMIT %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def update_recommendation(self, article_id: str, recommendation: Mapping[str, Any]) -> None:
        query = """
            UPDATE core.articles
            SET recommendation_score = %s,
                recommendation_reason = %s,
                recommendation_model = %s,
                recommendation_payload = %s::jsonb,
                recommended_at = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE article_id = %s
        """
        params = (
            recommendation.get("recommendation_score"),
            recommendation.get("recommendation_reason"),
            recommendation.get("recommendation_model"),
            _json_payload(recommendation.get("recommendation_payload")),
            recommendation.get("recommended_at"),
            article_id,
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

    def update_article_analysis(self, article_id: str, analysis: Mapping[str, Any]) -> None:
        query = """
            UPDATE core.articles
            SET reaction_score = %s,
                reaction_breakdown = %s::jsonb,
                pld_fit_score = %s,
                pld_breakdown = %s::jsonb,
                dominant_pld_stage = %s,
                operational_score = %s,
                operational_breakdown = %s::jsonb,
                final_score = %s,
                selection_summary = %s,
                hard_reject_reason = NULLIF(%s, ''),
                analysis_payload = %s::jsonb,
                analysis_model = %s,
                analysis_version = %s,
                analyzed_at = %s,
                selection_status = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE article_id = %s
        """
        params = (
            analysis.get("reaction_score"),
            _json_payload(analysis.get("reaction_breakdown")),
            analysis.get("pld_fit_score"),
            _json_payload(analysis.get("pld_breakdown")),
            analysis.get("dominant_pld_stage"),
            analysis.get("operational_score"),
            _json_payload(analysis.get("operational_breakdown")),
            analysis.get("final_score"),
            analysis.get("selection_summary"),
            analysis.get("hard_reject_reason", ""),
            _json_payload(analysis.get("analysis_payload")),
            analysis.get("analysis_model"),
            analysis.get("analysis_version"),
            analysis.get("analyzed_at"),
            analysis.get("selection_status"),
            article_id,
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

    def list_review_candidates(self, limit: int, min_score: float) -> list[dict[str, Any]]:
        query = """
            WITH latest_reviews AS (
                SELECT DISTINCT ON (article_id)
                    article_id,
                    decision,
                    reviewed_at
                FROM core.article_reviews
                ORDER BY article_id, reviewed_at DESC
            )
            SELECT
                a.article_id,
                a.title,
                a.summary_raw,
                a.article_url,
                a.canonical_url,
                a.published_at,
                a.recommendation_score,
                a.recommendation_reason,
                a.recommendation_model,
                s.source_name,
                f.feed_name,
                lr.decision AS latest_review_decision
            FROM core.articles AS a
            JOIN core.sources AS s
              ON s.source_id = a.source_id
            JOIN core.rss_feeds AS f
              ON f.rss_feed_id = a.rss_feed_id
            LEFT JOIN latest_reviews AS lr
              ON lr.article_id = a.article_id
            WHERE a.recommendation_score IS NOT NULL
              AND a.recommendation_score >= %s
              AND COALESCE(lr.decision, 'pending') IN ('pending', 'hold')
            ORDER BY a.recommendation_score DESC, a.published_at DESC NULLS LAST
            LIMIT %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (min_score, limit))
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def list_articles_for_selection_review(self, limit: int, min_final_score: float) -> list[dict[str, Any]]:
        query = """
            WITH latest_reviews AS (
                SELECT DISTINCT ON (article_id)
                    article_id,
                    decision,
                    reviewed_at
                FROM core.article_reviews
                ORDER BY article_id, reviewed_at DESC
            ),
            existing_dispatches AS (
                SELECT DISTINCT article_id
                FROM system.telegram_review_dispatches
                WHERE dispatch_status IN ('queued', 'sent', 'acted')
            ),
            existing_candidates AS (
                SELECT DISTINCT article_id
                FROM content.generated_contents
                WHERE content_type = 'facebook_post_candidate'
            )
            SELECT
                a.article_id,
                a.title,
                a.summary_raw,
                a.article_url,
                a.canonical_url,
                a.published_at,
                a.reaction_score,
                a.pld_fit_score,
                a.operational_score,
                a.final_score,
                a.selection_summary,
                a.hard_reject_reason,
                a.dominant_pld_stage,
                a.analysis_payload,
                s.source_name,
                s.source_code,
                f.feed_name,
                lr.decision AS latest_review_decision
            FROM core.articles AS a
            JOIN core.sources AS s
              ON s.source_id = a.source_id
            JOIN core.rss_feeds AS f
              ON f.rss_feed_id = a.rss_feed_id
            LEFT JOIN latest_reviews AS lr
              ON lr.article_id = a.article_id
            LEFT JOIN existing_dispatches AS ed
              ON ed.article_id = a.article_id
            LEFT JOIN existing_candidates AS ec
              ON ec.article_id = a.article_id
            WHERE a.selection_status IN ('scored', 'review_hold', 'review_queued')
              AND a.final_score IS NOT NULL
              AND a.final_score >= %s
              AND ec.article_id IS NULL
              AND ed.article_id IS NULL
              AND COALESCE(lr.decision, 'hold') IN ('hold')
            ORDER BY a.final_score DESC, a.published_at DESC NULLS LAST
            LIMIT %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (min_final_score, limit))
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def get_article_by_id(self, article_id: str) -> Optional[dict[str, Any]]:
        query = """
            SELECT
                a.*,
                s.source_code,
                s.source_name,
                f.feed_code,
                f.feed_name
            FROM core.articles AS a
            JOIN core.sources AS s
              ON s.source_id = a.source_id
            JOIN core.rss_feeds AS f
              ON f.rss_feed_id = a.rss_feed_id
            WHERE a.article_id = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id,))
                row = cur.fetchone()
        return dict(row) if row else None

    def list_confirmed_articles_without_candidate(self, limit: int) -> list[dict[str, Any]]:
        query = """
            WITH confirmed_articles AS (
                SELECT DISTINCT article_id
                FROM core.article_reviews
                WHERE decision = 'confirm'
            ),
            existing_candidates AS (
                SELECT DISTINCT article_id
                FROM content.generated_contents
                WHERE content_type = 'facebook_post_candidate'
            )
            SELECT
                a.article_id,
                a.title,
                a.summary_raw,
                a.article_url,
                a.canonical_url,
                a.final_score,
                a.selection_summary,
                a.analysis_payload,
                s.source_code,
                s.source_name,
                f.feed_name
            FROM core.articles AS a
            JOIN confirmed_articles AS ca
              ON ca.article_id = a.article_id
            JOIN core.sources AS s
              ON s.source_id = a.source_id
            JOIN core.rss_feeds AS f
              ON f.rss_feed_id = a.rss_feed_id
            LEFT JOIN existing_candidates AS ec
              ON ec.article_id = a.article_id
            WHERE ec.article_id IS NULL
              AND a.selection_status IN ('review_confirmed', 'facebook_candidate_created')
            ORDER BY a.final_score DESC NULLS LAST, a.published_at DESC NULLS LAST
            LIMIT %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def update_review_status(self, article_id: str, review_status: str) -> None:
        query = """
            UPDATE core.articles
            SET review_status = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE article_id = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (review_status, article_id))

    def update_selection_status(self, article_id: str, selection_status: str) -> None:
        query = """
            UPDATE core.articles
            SET selection_status = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE article_id = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (selection_status, article_id))


class PostgresArticleReviewRepository:
    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def create_review(self, review: Mapping[str, Any]) -> str:
        query = """
            INSERT INTO core.article_reviews (
                article_id,
                decision,
                review_channel,
                reviewer_id,
                reviewer_code,
                reviewer_display_name,
                review_note,
                decision_payload,
                review_context,
                review_summary,
                suggested_angle,
                suggested_question,
                operator_note,
                dispatch_id,
                telegram_chat_id,
                telegram_message_id,
                reviewed_at,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            RETURNING article_review_id
        """
        params = (
            review.get("article_id"),
            review.get("decision"),
            review.get("review_channel", "telegram"),
            review.get("reviewer_id"),
            review.get("reviewer_code"),
            review.get("reviewer_display_name"),
            review.get("review_note"),
            _json_payload(review.get("decision_payload")),
            _json_payload(review.get("review_context")),
            review.get("review_summary"),
            review.get("suggested_angle"),
            review.get("suggested_question"),
            review.get("operator_note"),
            review.get("dispatch_id"),
            review.get("telegram_chat_id"),
            review.get("telegram_message_id"),
            review.get("reviewed_at"),
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        return str(row["article_review_id"])

    def find_latest_review(self, article_id: str) -> dict[str, Any] | None:
        query = """
            SELECT
                article_review_id,
                article_id,
                decision,
                reviewer_id,
                reviewer_code,
                reviewer_display_name,
                review_note,
                review_summary,
                suggested_angle,
                suggested_question,
                operator_note,
                decision_payload,
                review_context,
                reviewed_at
            FROM core.article_reviews
            WHERE article_id = %s
            ORDER BY reviewed_at DESC, created_at DESC
            LIMIT 1
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id,))
                row = cur.fetchone()
        return dict(row) if row else None

    def has_confirm_review(self, article_id: str) -> bool:
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM core.article_reviews
                WHERE article_id = %s
                  AND decision = 'confirm'
            ) AS has_confirm
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id,))
                row = cur.fetchone()
        return bool(row["has_confirm"]) if row else False

    def find_latest_confirm_review(self, article_id: str) -> dict[str, Any] | None:
        query = """
            SELECT article_review_id, article_id, decision, reviewer_code, reviewer_display_name, reviewed_at
            FROM core.article_reviews
            WHERE article_id = %s
              AND decision = 'confirm'
            ORDER BY reviewed_at DESC
            LIMIT 1
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id,))
                row = cur.fetchone()
        return dict(row) if row else None
