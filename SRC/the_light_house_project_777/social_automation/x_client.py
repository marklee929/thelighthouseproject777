from __future__ import annotations

import hashlib
import os
import time
from typing import Any, Dict, List, Optional


class XApiClient:
    """
    X(Twitter) integration stub.
    Real API calls can be implemented later while keeping the same interface.
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("X_API_KEY", "").strip()
        self.api_secret = os.getenv("X_API_SECRET", "").strip()
        self.access_token = os.getenv("X_ACCESS_TOKEN", "").strip()
        self.access_token_secret = os.getenv("X_ACCESS_TOKEN_SECRET", "").strip()
        self.account_name = os.getenv("X_ACCOUNT_NAME", "main_account").strip()
        self.real_api_enabled = os.getenv("X_ENABLE_REAL_API", "false").lower() == "true"

    @property
    def configured(self) -> bool:
        required = [self.api_key, self.api_secret, self.access_token]
        return all(bool(item) for item in required)

    def publish_thread(self, *, title: str, body: str, account_alias: str) -> Dict[str, Any]:
        # Keep as stub for now; returns deterministic post id.
        hash_src = f"{title}|{body}|{account_alias}|{time.time()}"
        post_id = hashlib.sha1(hash_src.encode("utf-8")).hexdigest()[:16]
        return {
            "ok": True,
            "post_id": post_id,
            "platform": "x",
            "account_alias": account_alias,
            "dry_run": not (self.configured and self.real_api_enabled),
        }

    def get_liking_users(self, *, post_id: str, cursor: Optional[str], limit: int = 20) -> Dict[str, Any]:
        # Deterministic mock users so UI automation can be tested.
        seed_hex = hashlib.sha1(f"{post_id}:{cursor or ''}".encode("utf-8")).hexdigest()
        rows: List[Dict[str, Any]] = []
        for idx in range(min(max(limit, 1), 50)):
            base = int(seed_hex[idx % len(seed_hex)], 16)
            user_id = f"{post_id[-6:]}{idx:03d}"
            followers = 120 + (base * 31) + (idx * 7)
            following = 80 + (base * 13) + (idx * 5)
            rows.append(
                {
                    "id": user_id,
                    "username": f"user_{user_id}",
                    "followers_count": followers,
                    "following_count": max(1, following),
                    "tweet_count": 40 + (idx * 3),
                    "last_active_days": base % 21,
                    "is_bot_suspected": base in {0, 1},
                    "liked_post_id": post_id,
                }
            )
        next_cursor = f"cursor_{seed_hex[:10]}"
        return {"users": rows, "next_cursor": next_cursor}

    def get_post_like_count(self, *, post_id: str, last_like_count: int = 0) -> int:
        # Stub metric growth for monitor loop; non-decreasing count.
        seed = int(hashlib.sha1(post_id.encode("utf-8")).hexdigest()[:4], 16)
        minute = int(time.time() // 60)
        delta = ((seed + minute) % 5) + 1
        return max(int(last_like_count), int(last_like_count) + int(delta))

    def is_following(self, user_id: str) -> bool:
        # Stub behavior; a real integration would query following relationships.
        _ = user_id
        return False

    def follow_user(self, user_id: str) -> Dict[str, Any]:
        return {
            "ok": True,
            "user_id": user_id,
            "dry_run": not (self.configured and self.real_api_enabled),
            "executed_at": time.time(),
        }
