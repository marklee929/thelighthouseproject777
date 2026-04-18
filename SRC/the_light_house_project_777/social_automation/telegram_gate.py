from __future__ import annotations

import os
import time
from typing import Any, Dict

import httpx


class TelegramApprovalGate:
    """Telegram approval sender with safe dry-run fallback."""

    def __init__(self) -> None:
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.enabled = bool(self.bot_token and self.chat_id)

    def build_candidate_payload(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        user_id = str(candidate.get("id", ""))
        username = candidate.get("username", "unknown")
        followers = candidate.get("followers_count", 0)
        following = candidate.get("following_count", 0)
        post_id = candidate.get("liked_post_id", "-")

        text = (
            f"User: @{username}\n"
            f"Followers: {followers}\n"
            f"Following: {following}\n"
            f"Liked your post: {post_id}"
        )
        return {
            "chat_id": self.chat_id,
            "text": text,
            "reply_markup": {
                "inline_keyboard": [
                    [
                        {"text": "Approve Follow", "callback_data": f"approve:{user_id}"},
                        {"text": "Skip", "callback_data": f"skip:{user_id}"},
                        {"text": "Block", "callback_data": f"block:{user_id}"},
                    ]
                ]
            },
        }

    def send_candidate(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        payload = self.build_candidate_payload(candidate)
        if not self.enabled:
            return {
                "ok": True,
                "dry_run": True,
                "message_id": int(time.time() * 1000) % 1_000_000,
                "chat_id": payload.get("chat_id") or 0,
                "payload": payload,
            }

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        try:
            with httpx.Client(timeout=12.0) as client:
                resp = client.post(url, json=payload)
                resp.raise_for_status()
                data = resp.json()
                if not data.get("ok"):
                    return {"ok": False, "error": data}
                result = data.get("result") or {}
                return {
                    "ok": True,
                    "dry_run": False,
                    "message_id": result.get("message_id"),
                    "chat_id": result.get("chat", {}).get("id", self.chat_id),
                    "payload": payload,
                }
        except Exception as exc:
            return {"ok": False, "dry_run": False, "error": str(exc), "payload": payload}

