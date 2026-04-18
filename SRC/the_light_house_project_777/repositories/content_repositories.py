from __future__ import annotations

import json
from typing import Any, Mapping

from .postgres import PostgresConnectionFactory


def _json_payload(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False)


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
