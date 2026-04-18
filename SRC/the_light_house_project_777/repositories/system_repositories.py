from __future__ import annotations

import json
from typing import Any, Mapping

from .postgres import PostgresConnectionFactory


def _json_payload(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False)


class PostgresIngestionRunRepository:
    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def start_run(
        self,
        source_id: str,
        rss_feed_id: str,
        feed_url_snapshot: str,
        triggered_by: str,
        request_payload: Mapping[str, Any],
    ) -> str:
        query = """
            INSERT INTO system.ingestion_runs (
                source_id,
                rss_feed_id,
                triggered_by,
                status,
                feed_url_snapshot,
                request_payload,
                started_at,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, 'started', %s, %s::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING ingestion_run_id
        """
        params = (source_id, rss_feed_id, triggered_by, feed_url_snapshot, _json_payload(request_payload))
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        return str(row["ingestion_run_id"])

    def complete_run(
        self,
        ingestion_run_id: str,
        *,
        status: str,
        items_fetched: int,
        items_saved: int,
        items_duplicate: int,
        items_failed: int,
        result_payload: Mapping[str, Any],
        error_message: str = "",
    ) -> None:
        query = """
            UPDATE system.ingestion_runs
            SET status = %s,
                completed_at = CURRENT_TIMESTAMP,
                items_fetched = %s,
                items_saved = %s,
                items_duplicate = %s,
                items_failed = %s,
                result_payload = %s::jsonb,
                error_message = NULLIF(%s, ''),
                updated_at = CURRENT_TIMESTAMP
            WHERE ingestion_run_id = %s
        """
        params = (
            status,
            items_fetched,
            items_saved,
            items_duplicate,
            items_failed,
            _json_payload(result_payload),
            error_message,
            ingestion_run_id,
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)


class PostgresSystemConfigRepository:
    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def get_value(self, config_key: str, default: Any = None) -> Any:
        query = """
            SELECT config_value
            FROM system.system_configs
            WHERE config_key = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (config_key,))
                row = cur.fetchone()
        if not row:
            return default
        return row["config_value"]

    def set_value(self, config_key: str, config_value: Any) -> None:
        query = """
            INSERT INTO system.system_configs (config_key, config_value, created_at, updated_at)
            VALUES (%s, %s::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (config_key)
            DO UPDATE SET
                config_value = EXCLUDED.config_value,
                updated_at = CURRENT_TIMESTAMP
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (config_key, _json_payload(config_value)))


class PostgresReviewerRepository:
    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def upsert_reviewer(self, reviewer: Mapping[str, Any]) -> str:
        query = """
            INSERT INTO system.reviewers (
                reviewer_code,
                display_name,
                telegram_chat_id,
                telegram_username,
                role_name,
                active,
                reviewer_metadata,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP)
            ON CONFLICT (reviewer_code)
            DO UPDATE SET
                display_name = EXCLUDED.display_name,
                telegram_chat_id = EXCLUDED.telegram_chat_id,
                telegram_username = EXCLUDED.telegram_username,
                role_name = EXCLUDED.role_name,
                active = EXCLUDED.active,
                reviewer_metadata = EXCLUDED.reviewer_metadata,
                updated_at = CURRENT_TIMESTAMP
            RETURNING reviewer_id
        """
        params = (
            reviewer.get("reviewer_code"),
            reviewer.get("display_name"),
            reviewer.get("telegram_chat_id"),
            reviewer.get("telegram_username"),
            reviewer.get("role_name", "article_reviewer"),
            bool(reviewer.get("active", True)),
            _json_payload(reviewer.get("reviewer_metadata")),
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        return str(row["reviewer_id"])

    def list_active_reviewers(self) -> list[dict[str, Any]]:
        query = """
            SELECT reviewer_id, reviewer_code, display_name, telegram_chat_id, telegram_username, role_name, active, reviewer_metadata
            FROM system.reviewers
            WHERE active = true
            ORDER BY reviewer_code
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def get_by_reviewer_code(self, reviewer_code: str) -> dict[str, Any] | None:
        query = """
            SELECT reviewer_id, reviewer_code, display_name, telegram_chat_id, telegram_username, role_name, active, reviewer_metadata
            FROM system.reviewers
            WHERE reviewer_code = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (reviewer_code,))
                row = cur.fetchone()
        return dict(row) if row else None


class PostgresTelegramReviewDispatchRepository:
    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def create_dispatch(self, dispatch: Mapping[str, Any]) -> str:
        query = """
            INSERT INTO system.telegram_review_dispatches (
                article_id,
                reviewer_id,
                telegram_chat_id,
                telegram_message_id,
                dispatch_status,
                dispatch_payload,
                callback_payload,
                error_message,
                sent_at,
                acted_at,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NULLIF(%s, ''), %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING telegram_review_dispatch_id
        """
        params = (
            dispatch.get("article_id"),
            dispatch.get("reviewer_id"),
            dispatch.get("telegram_chat_id"),
            dispatch.get("telegram_message_id"),
            dispatch.get("dispatch_status", "queued"),
            _json_payload(dispatch.get("dispatch_payload")),
            _json_payload(dispatch.get("callback_payload")),
            dispatch.get("error_message", ""),
            dispatch.get("sent_at"),
            dispatch.get("acted_at"),
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        return str(row["telegram_review_dispatch_id"])

    def mark_dispatch_sent(self, dispatch_id: str, payload: Mapping[str, Any]) -> None:
        query = """
            UPDATE system.telegram_review_dispatches
            SET dispatch_status = 'sent',
                telegram_chat_id = %s,
                telegram_message_id = %s,
                dispatch_payload = %s::jsonb,
                sent_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE telegram_review_dispatch_id = %s
        """
        params = (
            payload.get("chat_id"),
            payload.get("message_id"),
            _json_payload(payload),
            dispatch_id,
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

    def mark_dispatch_failed(self, dispatch_id: str, error_message: str, payload: Mapping[str, Any]) -> None:
        query = """
            UPDATE system.telegram_review_dispatches
            SET dispatch_status = 'failed',
                error_message = NULLIF(%s, ''),
                dispatch_payload = %s::jsonb,
                updated_at = CURRENT_TIMESTAMP
            WHERE telegram_review_dispatch_id = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (error_message, _json_payload(payload), dispatch_id))

    def mark_dispatch_acted(self, dispatch_id: str, payload: Mapping[str, Any]) -> None:
        query = """
            UPDATE system.telegram_review_dispatches
            SET dispatch_status = 'acted',
                callback_payload = %s::jsonb,
                acted_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE telegram_review_dispatch_id = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (_json_payload(payload), dispatch_id))

    def get_dispatch_by_message(self, telegram_chat_id: str, telegram_message_id: int) -> dict[str, Any] | None:
        query = """
            SELECT
                d.telegram_review_dispatch_id,
                d.article_id,
                d.reviewer_id,
                d.telegram_chat_id,
                d.telegram_message_id,
                d.dispatch_status,
                r.reviewer_code,
                r.display_name
            FROM system.telegram_review_dispatches AS d
            JOIN system.reviewers AS r
              ON r.reviewer_id = d.reviewer_id
            WHERE d.telegram_chat_id = %s
              AND d.telegram_message_id = %s
            ORDER BY d.created_at DESC
            LIMIT 1
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (telegram_chat_id, telegram_message_id))
                row = cur.fetchone()
        return dict(row) if row else None
