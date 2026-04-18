from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from repositories.interfaces import ReviewerRepositoryProtocol


class ReviewerRegistryLoader:
    """Loads the four-reviewer Telegram registry and syncs it into PostgreSQL."""

    def __init__(self, registry_path: str | None = None) -> None:
        default_path = Path(__file__).resolve().parents[2] / "config" / "reviewer_registry.json"
        env_path = os.getenv("LIGHTHOUSE_REVIEWER_REGISTRY_PATH", "").strip()
        self.registry_path = Path(registry_path or env_path or default_path)

    def load_config(self) -> Dict[str, Any]:
        with self.registry_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def resolve_reviewers(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for reviewer in self.load_config().get("reviewers") or []:
            chat_env_name = str(reviewer.get("telegram_chat_id_env", "")).strip()
            rows.append(
                {
                    "reviewer_code": str(reviewer.get("reviewer_code", "")).strip(),
                    "display_name": str(reviewer.get("display_name", "")).strip(),
                    "telegram_chat_id": os.getenv(chat_env_name, "").strip(),
                    "telegram_username": str(reviewer.get("telegram_username", "")).strip(),
                    "role_name": "article_reviewer",
                    "active": bool(reviewer.get("active", True)),
                    "reviewer_metadata": {
                        **dict(reviewer.get("metadata") or {}),
                        "telegram_chat_id_env": chat_env_name,
                    },
                }
            )
        return rows

    def sync_reviewers(self, reviewer_repository: ReviewerRepositoryProtocol) -> List[Dict[str, Any]]:
        synced: List[Dict[str, Any]] = []
        for reviewer in self.resolve_reviewers():
            reviewer_id = reviewer_repository.upsert_reviewer(reviewer)
            synced.append({"reviewer_id": reviewer_id, **reviewer})
        return synced
