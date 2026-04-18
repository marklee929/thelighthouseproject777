from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import requests


class TelegramClient:
    """Telegram helper for alerts and future approval workflows."""

    MESSAGE_CHAR_LIMIT = 3500

    def __init__(self) -> None:
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.enabled = bool(self.bot_token)
        self.default_chat_configured = bool(self.chat_id)

    def _send_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.enabled:
            return {
                "ok": True,
                "dry_run": True,
                "chat_id": payload.get("chat_id"),
                "message_id": int(time.time() * 1000) % 1_000_000,
                "payload": payload,
            }
        if not str(payload.get("chat_id", "")).strip():
            return {"ok": False, "dry_run": False, "error": "missing chat_id", "payload": payload}
        text = str(payload.get("text", "") or "")
        if len(text) > self.MESSAGE_CHAR_LIMIT:
            payload = dict(payload)
            payload["text"] = f"{text[: self.MESSAGE_CHAR_LIMIT - 32].rstrip()}\n\n...[truncated for Telegram]"
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        try:
            httpx_logger = logging.getLogger("httpx")
            previous_level = httpx_logger.level
            if previous_level < logging.WARNING:
                httpx_logger.setLevel(logging.WARNING)
            with httpx.Client(timeout=12.0) as client:
                resp = client.post(url, json=payload)
                status_code = int(resp.status_code)
                body_text = resp.text
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                if status_code >= 400 or not data.get("ok", status_code < 400):
                    detail = ""
                    if isinstance(data, dict):
                        detail = str(data.get("description") or data.get("error_code") or "").strip()
                    return {
                        "ok": False,
                        "status_code": status_code,
                        "error": detail or f"http_{status_code}",
                        "response_body": body_text,
                        "payload": payload,
                    }
                result = data.get("result") or {}
                return {
                    "ok": True,
                    "dry_run": False,
                    "status_code": status_code,
                    "chat_id": (result.get("chat") or {}).get("id", self.chat_id),
                    "message_id": result.get("message_id"),
                    "payload": payload,
                }
        except Exception as exc:
            return {"ok": False, "dry_run": False, "error": str(exc), "payload": payload}
        finally:
            try:
                httpx_logger = logging.getLogger("httpx")
                httpx_logger.setLevel(previous_level)
            except Exception:
                pass

    def send_message(self, text: str) -> Dict[str, Any]:
        payload = {"chat_id": self.chat_id, "text": str(text or "")}
        return self._send_message(payload)

    def send_message_to_chat(self, chat_id: str, text: str) -> Dict[str, Any]:
        payload = {"chat_id": str(chat_id or "").strip(), "text": str(text or "")}
        return self._send_message(payload)

    def send_alert(self, text: str) -> Dict[str, Any]:
        return self.send_message(text)

    def test_connection(self) -> Dict[str, Any]:
        if not self.default_chat_configured:
            return {"ok": False, "error": "default TELEGRAM_CHAT_ID is not configured"}
        return self.send_message("the_light_house_project+777 Telegram test message")

    def get_updates(self, offset: Optional[int] = None, limit: int = 20, timeout: int = 2) -> Dict[str, Any]:
        if not self.enabled:
            return {"ok": True, "dry_run": True, "result": []}
        url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
        payload: Dict[str, Any] = {"limit": int(limit), "timeout": int(timeout)}
        if offset is not None:
            payload["offset"] = int(offset)
        httpx_error = ""
        try:
            httpx_logger = logging.getLogger("httpx")
            previous_level = httpx_logger.level
            if previous_level < logging.WARNING:
                httpx_logger.setLevel(logging.WARNING)
            with httpx.Client(timeout=12.0) as client:
                resp = client.post(url, json=payload)
                status_code = int(resp.status_code)
                body_text = resp.text
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                if status_code >= 400 or not data.get("ok", status_code < 400):
                    detail = ""
                    if isinstance(data, dict):
                        detail = str(data.get("description") or data.get("error_code") or "").strip()
                    return {
                        "ok": False,
                        "status_code": status_code,
                        "error": detail or f"http_{status_code}",
                        "response_body": body_text,
                    }
                return {
                    "ok": True,
                    "transport": "httpx",
                    "status_code": status_code,
                    "response_body": body_text,
                    "result": data.get("result") or [],
                }
        except Exception as exc:
            httpx_error = str(exc)
        finally:
            try:
                httpx_logger = logging.getLogger("httpx")
                httpx_logger.setLevel(previous_level)
            except Exception:
                pass
        try:
            response = requests.get(url, params=payload, timeout=max(3, int(timeout) + 1))
            status_code = int(response.status_code)
            body_text = response.text
            try:
                data = response.json()
            except Exception:
                data = {}
            if status_code >= 400 or not data.get("ok", status_code < 400):
                detail = ""
                if isinstance(data, dict):
                    detail = str(data.get("description") or data.get("error_code") or "").strip()
                return {
                    "ok": False,
                    "transport": "requests",
                    "status_code": status_code,
                    "error": detail or f"http_{status_code}",
                    "response_body": body_text,
                    "httpx_error": httpx_error,
                }
            return {
                "ok": True,
                "transport": "requests",
                "status_code": status_code,
                "response_body": body_text,
                "httpx_error": httpx_error,
                "result": data.get("result") or [],
            }
        except requests.RequestException as exc:
            return {
                "ok": False,
                "transport": "requests",
                "error": str(exc),
                "httpx_error": httpx_error,
            }

    def answer_callback_query(self, callback_query_id: str, text: str = "") -> Dict[str, Any]:
        if not self.enabled:
            return {"ok": True, "dry_run": True}
        url = f"https://api.telegram.org/bot{self.bot_token}/answerCallbackQuery"
        payload = {"callback_query_id": str(callback_query_id or "").strip(), "text": str(text or "").strip()}
        try:
            httpx_logger = logging.getLogger("httpx")
            previous_level = httpx_logger.level
            if previous_level < logging.WARNING:
                httpx_logger.setLevel(logging.WARNING)
            with httpx.Client(timeout=12.0) as client:
                resp = client.post(url, json=payload)
                status_code = int(resp.status_code)
                body_text = resp.text
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                if status_code >= 400 or not data.get("ok", status_code < 400):
                    detail = ""
                    if isinstance(data, dict):
                        detail = str(data.get("description") or data.get("error_code") or "").strip()
                    return {
                        "ok": False,
                        "status_code": status_code,
                        "error": detail or f"http_{status_code}",
                        "response_body": body_text,
                    }
                return {"ok": True, "status_code": status_code}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        finally:
            try:
                httpx_logger = logging.getLogger("httpx")
                httpx_logger.setLevel(previous_level)
            except Exception:
                pass

    def send_approval_card(self, title: str, body: str, buttons: Optional[List[List[Dict[str, str]]]] = None) -> Dict[str, Any]:
        return self.send_approval_card_to_chat(self.chat_id, title=title, body=body, buttons=buttons)

    def send_approval_card_to_chat(
        self,
        chat_id: str,
        *,
        title: str,
        body: str,
        buttons: Optional[List[List[Dict[str, str]]]] = None,
    ) -> Dict[str, Any]:
        payload = {
            "chat_id": str(chat_id or "").strip(),
            "text": f"{title}\n\n{body}".strip(),
        }
        if buttons:
            payload["reply_markup"] = {"inline_keyboard": buttons}
        return self._send_message(payload)

    def _send_media(
        self,
        *,
        method: str,
        file_field: str,
        file_path: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        path = Path(str(file_path or "").strip())
        if not path.exists():
            return {"ok": False, "error": f"file not found: {path}"}
        if not self.enabled:
            return {
                "ok": True,
                "dry_run": True,
                "chat_id": payload.get("chat_id"),
                "message_id": int(time.time() * 1000) % 1_000_000,
                "payload": payload,
                "file_path": str(path),
            }
        url = f"https://api.telegram.org/bot{self.bot_token}/{method}"
        send_payload = dict(payload)
        reply_markup = send_payload.get("reply_markup")
        if isinstance(reply_markup, dict):
            send_payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
        try:
            with path.open("rb") as handle:
                response = requests.post(
                    url,
                    data=send_payload,
                    files={file_field: (path.name, handle)},
                    timeout=20,
                )
            status_code = int(response.status_code)
            body_text = response.text
            try:
                data = response.json()
            except Exception:
                data = {}
            if status_code >= 400 or not data.get("ok", status_code < 400):
                detail = ""
                if isinstance(data, dict):
                    detail = str(data.get("description") or data.get("error_code") or "").strip()
                return {
                    "ok": False,
                    "status_code": status_code,
                    "error": detail or f"http_{status_code}",
                    "response_body": body_text,
                    "payload": payload,
                    "file_path": str(path),
                }
            result = data.get("result") or {}
            return {
                "ok": True,
                "dry_run": False,
                "status_code": status_code,
                "chat_id": (result.get("chat") or {}).get("id", self.chat_id),
                "message_id": result.get("message_id"),
                "payload": payload,
                "file_path": str(path),
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc), "payload": payload, "file_path": str(path)}

    def send_candidate(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
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
        return self.send_approval_card(
            title="X Growth Candidate",
            body=text,
            buttons=[
                [
                    {"text": "Approve Follow", "callback_data": f"approve:{user_id}"},
                    {"text": "Skip", "callback_data": f"skip:{user_id}"},
                    {"text": "Block", "callback_data": f"block:{user_id}"},
                ]
            ],
        )

    def send_draft_review(self, draft: Dict[str, Any]) -> Dict[str, Any]:
        draft_id = str(draft.get("draft_id", "")).strip()
        title_text = str(draft.get("title", "")).strip()
        body_text = str(draft.get("body", "")).strip()
        source_text = str(draft.get("source_link", "")).strip()
        notes_text = "; ".join(draft.get("review_notes") or [])
        if len(title_text) > 240:
            title_text = f"{title_text[:237].rstrip()}..."
        if len(body_text) > 1800:
            body_text = f"{body_text[:1797].rstrip()}..."
        if len(notes_text) > 600:
            notes_text = f"{notes_text[:597].rstrip()}..."
        text = (
            "[Draft Review]\n"
            f"Draft ID: {draft_id}\n"
            f"Category: {str(draft.get('category', '')).strip()}\n"
            f"Recommendation: {str(draft.get('final_recommendation', '')).strip() or 'revise'}\n"
            f"Risk: {str(draft.get('risk_of_misleading', '')).strip() or 'medium'}\n\n"
            f"Title:\n{title_text}\n\n"
            f"Body:\n{body_text}\n\n"
            f"Source:\n{source_text}\n\n"
            f"Notes:\n{notes_text}"
        )
        return self.send_approval_card(
            title="Facebook Draft Review",
            body=text,
            buttons=[
                [
                    {"text": "Approve", "callback_data": f"draft_approve:{draft_id}"},
                    {"text": "Modify", "callback_data": f"draft_modify:{draft_id}"},
                ],
                [
                    {"text": "Reject", "callback_data": f"draft_reject:{draft_id}"},
                    {"text": "Save Weekend", "callback_data": f"draft_weekend:{draft_id}"},
                ],
            ],
        )

    def send_clip_review(self, clip: Dict[str, Any]) -> Dict[str, Any]:
        clip_id = str(clip.get("clip_id", "")).strip()
        title = str(clip.get("title", "")).strip()
        summary = str(clip.get("summary", "")).strip()
        topic = str(clip.get("topic", "")).strip()
        category = str(clip.get("category", "")).strip()
        format_label = str(clip.get("format_label", "") or clip.get("format", "")).strip()
        hook = str(clip.get("hook", "")).strip()
        research_quality = str(clip.get("research_quality", "")).strip() or "unknown"
        sources_used_count = int(clip.get("sources_used_count", 0) or 0)
        visual_coverage = str(clip.get("visual_coverage", "")).strip()
        key_message = str(clip.get("key_message", "")).strip()
        render_status = str(clip.get("render_status", "")).strip() or "preview_ready"
        preview_path = str(clip.get("preview_path", "")).strip()
        video_path = str(clip.get("video_path", "")).strip()
        poster_path = str(clip.get("poster_path", "")).strip()
        caption = (
            "[WorkConnect Clips Draft]\n"
            f"Clip ID: {clip_id}\n"
            f"Mode: {str(clip.get('mode', 'workconnect_clips')).strip()}\n"
            f"Category: {category}\n"
            f"Format: {format_label}\n"
            f"Topic: {topic}\n"
            f"Title: {title}\n"
            f"Hook: {hook}\n"
            f"Research Quality: {research_quality}\n"
            f"Sources Used: {sources_used_count}\n"
            f"Visual Coverage: {visual_coverage}\n"
            f"Key Message: {key_message}\n"
            f"Summary: {summary}\n"
            f"Render Status: {render_status}"
        )
        payload = {
            "chat_id": self.chat_id,
            "caption": caption[:950],
            "reply_markup": {
                "inline_keyboard": [
                    [
                        {"text": "Approve", "callback_data": f"clip_approve:{clip_id}"},
                        {"text": "Reject", "callback_data": f"clip_reject:{clip_id}"},
                    ]
                ]
            },
        }
        if video_path:
            result = self._send_media(method="sendVideo", file_field="video", file_path=video_path, payload=payload)
            if result.get("ok"):
                return result
        if preview_path:
            result = self._send_media(method="sendAnimation", file_field="animation", file_path=preview_path, payload=payload)
            if result.get("ok"):
                return result
        if poster_path:
            return self._send_media(method="sendDocument", file_field="document", file_path=poster_path, payload=payload)
        return self.send_approval_card(title="WorkConnect Clip Review", body=caption, buttons=payload["reply_markup"]["inline_keyboard"])

    def handle_callback(self, callback_data: str) -> Dict[str, Any]:
        data = str(callback_data or "").strip()
        if ":" not in data:
            return {"ok": False, "error": "invalid callback_data"}
        action, user_id = data.split(":", 1)
        return {"ok": True, "action": action, "user_id": user_id}
