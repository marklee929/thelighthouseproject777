from __future__ import annotations

from datetime import date, datetime
import json
from typing import Any, Mapping

from .postgres import PostgresConnectionFactory


def _json_payload(value: Any) -> str:
    def _default_serializer(item: Any) -> str:
        if isinstance(item, (datetime, date)):
            return item.isoformat()
        return str(item)

    return json.dumps(value or {}, ensure_ascii=False, default=_default_serializer)


class PostgresGeneratedContentRepository:
    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def find_by_article_and_type(self, article_id: str, content_type: str) -> dict[str, Any] | None:
        query = """
            SELECT generated_content_id, article_id, content_type, generation_status, created_at
            FROM content.generated_contents
            WHERE article_id = %s
              AND content_type = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id, content_type))
                row = cur.fetchone()
        return dict(row) if row else None

    def create_generated_content(self, content: Mapping[str, Any]) -> str:
        query = """
            INSERT INTO content.generated_contents (
                article_id,
                source_review_id,
                content_type,
                generation_status,
                generator_name,
                generator_model,
                prompt_version,
                title,
                body_text,
                rendered_payload,
                version_no,
                created_by,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING generated_content_id
        """
        params = (
            content.get("article_id"),
            content.get("source_review_id"),
            content.get("content_type"),
            content.get("generation_status", "draft"),
            content.get("generator_name", "local_llm"),
            content.get("generator_model"),
            content.get("prompt_version"),
            content.get("title"),
            content.get("body_text"),
            _json_payload(content.get("rendered_payload")),
            content.get("version_no", 1),
            content.get("created_by", "system"),
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        return str(row["generated_content_id"])


class PostgresReviewCardRepository:
    def __init__(self, connection_factory: PostgresConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def get_by_article_channel_type(self, article_id: str, channel: str, card_type: str) -> dict[str, Any] | None:
        query = """
            SELECT card_id, article_id, channel, card_type, payload_json, status, created_at, sent_at
            FROM content.review_cards
            WHERE article_id = %s
              AND channel = %s
              AND card_type = %s
            LIMIT 1
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (article_id, channel, card_type))
                row = cur.fetchone()
        return dict(row) if row else None

    def create_review_card(self, card: Mapping[str, Any]) -> str:
        query = """
            INSERT INTO content.review_cards (
                card_id,
                article_id,
                channel,
                card_type,
                payload_json,
                status,
                sent_at,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, CURRENT_TIMESTAMP)
            RETURNING card_id
        """
        params = (
            card.get("card_id"),
            card.get("article_id"),
            card.get("channel", "telegram"),
            card.get("card_type", "article_review"),
            _json_payload(card.get("payload_json")),
            card.get("status", "preview"),
            card.get("sent_at"),
        )
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        return str(row["card_id"])

    def list_review_cards(self, *, channel: str, card_type: str, limit: int) -> list[dict[str, Any]]:
        query = """
            SELECT
                rc.card_id,
                rc.article_id,
                rc.channel,
                rc.card_type,
                rc.payload_json,
                rc.status,
                rc.created_at,
                rc.sent_at
            FROM content.review_cards AS rc
            WHERE rc.channel = %s
              AND rc.card_type = %s
            ORDER BY rc.created_at DESC
            LIMIT %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (channel, card_type, limit))
                rows = cur.fetchall() or []
        return [dict(row) for row in rows]

    def update_card_status_by_article(self, article_id: str, channel: str, card_type: str, status: str, sent_at: Any = None) -> None:
        query = """
            UPDATE content.review_cards
            SET status = %s,
                sent_at = CASE WHEN %s IS NOT NULL THEN %s ELSE sent_at END
            WHERE article_id = %s
              AND channel = %s
              AND card_type = %s
        """
        with self.connection_factory.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (status, sent_at, sent_at, article_id, channel, card_type))
