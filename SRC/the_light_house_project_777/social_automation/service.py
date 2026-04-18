from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency fallback
    load_dotenv = None

from integrations.naver_client import NaverNewsClient
from integrations.telegram_client import TelegramClient
from integrations.x_client import XClient

from .facebook_publisher import FacebookPublisher
from .lmdb_store import CrewAutomationStateStore, normalize_source_url
from .news_collector import NewsCollector
from .news_reviewer import NewsReviewer
from .platform_connector import PlatformConnector
from .review_consultant import SocialReviewConsultant
from .workconnect_clips import WorkConnectClipsGenerator


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_epoch_seconds(iso_text: Optional[str]) -> Optional[float]:
    if not iso_text:
        return None
    try:
        return datetime.fromisoformat(iso_text).timestamp()
    except Exception:
        return None


def _norm_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _topic_signature(value: str) -> str:
    stopwords = {
        "the",
        "and",
        "for",
        "with",
        "from",
        "into",
        "after",
        "over",
        "under",
        "rise",
        "rises",
        "rise",
        "new",
        "korea",
        "korean",
        "workers",
        "worker",
    }
    tokens = [token for token in re.split(r"[^a-z0-9]+", _norm_text(value)) if len(token) >= 3 and token not in stopwords]
    if not tokens:
        return ""
    ordered: List[str] = []
    for token in tokens:
        if token not in ordered:
            ordered.append(token)
        if len(ordered) >= 4:
            break
    return "|".join(ordered)


def _auth_mask(value: str, keep: int = 8) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    if len(text) <= keep:
        return text
    return f"len={len(text)} suffix={text[-keep:]}"


def _auth_debug(msg: str) -> None:
    try:
        print(f"[SOCIAL-AUTH-DEBUG] {msg}", flush=True)
    except Exception:
        pass


class SocialAutomationService:
    """Crew-only social generation + growth automation service."""

    PLATFORM_PROFILES = PlatformConnector.PLATFORM_PROFILES
    DEFAULT_PLATFORM = "facebook"
    NEWS_CATEGORIES = ("foreign_workers_korea", "eps_korea", "factory_jobs_korea")
    NEWS_MAX_AGE_SEC = 6 * 60 * 60
    NEWS_CRAWL_INTERVAL_SEC = 10 * 60
    TOPIC_COOLDOWN_SEC = 12 * 60 * 60
    POST_PUBLISH_WAIT_SEC = int(
        (os.getenv("SOCIAL_POST_INTERVAL_SECONDS", "").strip() or os.getenv("X_POST_INTERVAL_SECONDS", "3600").strip())
        or "3600"
    )
    DRAFT_PENDING_TTL_HOURS = max(1, int((os.getenv("SOCIAL_DRAFT_PENDING_TTL_HOURS", "24").strip() or "24")))
    TELEGRAM_DRAFT_RETRY_INTERVAL_MINUTES = max(
        1, int((os.getenv("TELEGRAM_DRAFT_RETRY_INTERVAL_MINUTES", "10").strip() or "10"))
    )
    GLOBAL_KEYWORDS = (
        "samsung",
        "sk hynix",
        "ai",
        "chip",
        "k-pop",
        "bts",
        "blackpink",
        "son heung-min",
        "netflix",
        "korea",
        "north korea",
    )
    CATEGORY_TONE_HINTS = {
        "entertainment": "Keep it light and pop-culture friendly for global readers.",
        "economy": "Keep it factual and explain market or industry impact.",
        "technology": "Explain why the tech matters in a global context.",
        "sports": "Use energetic short lines centered on result, player, and record.",
    }
    CONTENT_MODES = ("workconnect_clips",)
    CONTENT_RECENT_TOPIC_LIMIT = 20

    def __init__(self, project_root: str) -> None:
        self.project_root = Path(project_root)
        if load_dotenv is not None:
            load_dotenv(self.project_root / ".env", override=False)
        self.config_dir = self.project_root / "config"
        self.accounts_path = self.config_dir / "accounts.json"
        self.lmdb_dir = self.project_root / "data" / "lmdb"
        self.lmdb_dir.mkdir(parents=True, exist_ok=True)
        self.store = CrewAutomationStateStore(str(self.lmdb_dir))
        self.platform_connector = PlatformConnector(str(self.project_root))
        self.x_client = XClient()
        self.telegram = TelegramClient()
        self.naver = NaverNewsClient()
        self.news_collector = NewsCollector(str(self.project_root), self.naver)
        self.news_reviewer = NewsReviewer(str(self.project_root))
        self.review_consultant = SocialReviewConsultant(str(self.project_root))
        self.facebook_publisher = FacebookPublisher(str(self.project_root))
        self.workconnect_clips = WorkConnectClipsGenerator(str(self.project_root), self.naver)
        self._telegram_poll_inflight = False
        self._ensure_accounts_config()
        self._ensure_social_storage()

    def _ensure_accounts_config(self) -> None:
        self.config_dir.mkdir(parents=True, exist_ok=True)
        if self.accounts_path.exists():
            return
        payload = {"default_social": {"facebook_page": "main_page", "x_account": "experimental"}}
        self.accounts_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_accounts_config(self) -> Dict[str, Any]:
        try:
            data = json.loads(self.accounts_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {"default_social": {"facebook_page": "main_page", "x_account": "experimental"}}

    def _ensure_social_storage(self) -> None:
        targets = [
            self.project_root / "data" / "news_raw",
            self.project_root / "data" / "news_filtered",
            self.project_root / "data" / "publish_candidates",
            self.project_root / "data" / "post_candidates",
            self.project_root / "data" / "post_reviews",
            self.project_root / "data" / "published_logs",
            self.project_root / "data" / "content_clips",
            self.project_root / "prompts" / "social",
        ]
        for path in targets:
            path.mkdir(parents=True, exist_ok=True)

    def _default_content_config(self) -> Dict[str, Any]:
        return {
            "selected_mode": "workconnect_clips",
            "target_audience": "Foreign workers in Korea or planning to move",
            "review_channel": "telegram",
            "auto_publish": False,
        }

    def _load_content_config(self) -> Dict[str, Any]:
        raw = self.store.get_meta("meta:content:config", self._default_content_config())
        if not isinstance(raw, dict):
            return self._default_content_config()
        return {**self._default_content_config(), **raw}

    def _save_content_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        current = self._load_content_config()
        merged = {
            **current,
            "selected_mode": str(payload.get("selected_mode", current.get("selected_mode", "workconnect_clips"))).strip()
            or "workconnect_clips",
        }
        self.store.set_meta("meta:content:config", merged)
        return merged

    def _default_content_quality_profile(self) -> Dict[str, Any]:
        return {
            "mode": "workconnect_clips",
            "approved": 0,
            "rejected": 0,
            "category_stats": {},
            "format_stats": {},
            "variant_stats": {},
            "updated_at": "",
        }

    def _load_content_quality_profile(self, mode: str = "workconnect_clips") -> Dict[str, Any]:
        raw = self.store.get_meta(f"meta:content:quality:{mode}", self._default_content_quality_profile())
        if not isinstance(raw, dict):
            return self._default_content_quality_profile()
        profile = {**self._default_content_quality_profile(), **raw}
        profile["mode"] = mode
        return profile

    def _save_content_quality_profile(self, mode: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        merged = {**self._default_content_quality_profile(), **(profile or {}), "mode": mode, "updated_at": _now_iso()}
        self.store.set_meta(f"meta:content:quality:{mode}", merged)
        return merged

    def _update_content_quality_feedback(self, clip: Dict[str, Any], approved: bool) -> Dict[str, Any]:
        mode = str(clip.get("mode", "workconnect_clips")).strip() or "workconnect_clips"
        profile = self._load_content_quality_profile(mode)
        field = "approved" if approved else "rejected"
        profile[field] = int(profile.get(field, 0) or 0) + 1
        category_key = str(clip.get("category_key", "")).strip().lower()
        if category_key:
            category_stats = profile.get("category_stats")
            if not isinstance(category_stats, dict):
                category_stats = {}
            row = category_stats.get(category_key) or {"approved": 0, "rejected": 0}
            row[field] = int(row.get(field, 0) or 0) + 1
            category_stats[category_key] = row
            profile["category_stats"] = category_stats
        variant = str(clip.get("variant", "")).strip().lower()
        if variant:
            variant_stats = profile.get("variant_stats")
            if not isinstance(variant_stats, dict):
                variant_stats = {}
            row = variant_stats.get(variant) or {"approved": 0, "rejected": 0}
            row[field] = int(row.get(field, 0) or 0) + 1
            variant_stats[variant] = row
            profile["variant_stats"] = variant_stats
        clip_format = str(clip.get("format", "")).strip().lower()
        if clip_format:
            format_stats = profile.get("format_stats")
            if not isinstance(format_stats, dict):
                format_stats = {}
            row = format_stats.get(clip_format) or {"approved": 0, "rejected": 0}
            row[field] = int(row.get(field, 0) or 0) + 1
            format_stats[clip_format] = row
            profile["format_stats"] = format_stats
        return self._save_content_quality_profile(mode, profile)

    def _recent_content_topic_slugs(self, limit: int = 20) -> List[str]:
        slugs: List[str] = []
        seen: set[str] = set()
        for row in self.store.list_content_queue():
            slug = str((row or {}).get("topic_slug", "")).strip()
            if slug and slug not in seen:
                slugs.append(slug)
                seen.add(slug)
        for row in self.store.list_archived_content_items(limit=max(limit * 2, limit)):
            slug = str((row or {}).get("topic_slug", "")).strip()
            if slug and slug not in seen:
                slugs.append(slug)
                seen.add(slug)
            if len(slugs) >= limit:
                break
        return slugs[:limit]

    def list_content_queue(self) -> Dict[str, Any]:
        poll_logs: List[str] = []
        poll_result = self.poll_telegram_updates()
        poll_logs.extend(poll_result.get("logs") or [])
        return {
            "ok": True,
            "content_queue": self.store.list_content_queue(),
            "content_quality": self._load_content_quality_profile("workconnect_clips"),
            "logs": poll_logs,
        }

    def generate_workconnect_clip(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        config = self._save_content_config(payload)
        mode = str(config.get("selected_mode", "workconnect_clips")).strip() or "workconnect_clips"
        if mode != "workconnect_clips":
            return {"ok": False, "error": "unsupported_content_mode", "detail": mode}
        quality_profile = self._load_content_quality_profile(mode)
        recent_topics = self._recent_content_topic_slugs(limit=self.CONTENT_RECENT_TOPIC_LIMIT)
        logs = [
            "[AGENT] Content Agent started WorkConnect Clips pipeline",
            "[AGENT] Step 1/7 Format Planner",
            f"[AGENT] recent_topic_memory={len(recent_topics)}",
        ]
        clip = self.workconnect_clips.build_clip_package(quality_profile=quality_profile, recent_topic_slugs=recent_topics)
        clip_row = self.store.save_content_queue_item(
            clip["clip_id"],
            {
                **clip,
                "approval_status": "pending",
                "approval_channel": "telegram",
                "created_at": _now_iso(),
                "operator_decision": "",
                "published": False,
            },
        )
        self.store.upsert_task(
            f"content_{clip['clip_id']}",
            {
                "task_id": f"content_{clip['clip_id']}",
                "type": "content_generation",
                "state": "telegram_pending",
                "mode": mode,
                "clip_id": clip["clip_id"],
                "topic": clip.get("topic", ""),
                "category": clip.get("category", ""),
                "created_at": _now_iso(),
            },
        )
        logs.extend(
            [
                f"[AGENT] format={clip.get('format')} tone={clip.get('target_tone')}",
                f"[AGENT] Step 2/7 Research Agent: quality={clip.get('research_quality')} sources={clip.get('sources_used_count')}",
                f"[AGENT] Step 3/7 Content Synthesizer: topic={clip.get('topic')}",
                f"[AGENT] Step 4/7 Visual Research: coverage={clip.get('visual_coverage')}",
                "[AGENT] Step 5/7 Scene Planner completed",
                f"[AGENT] Step 6/7 Video Composer: {clip.get('render_status')}",
            ]
        )
        telegram_result = self.deliver_clip_to_telegram(clip_row)
        if telegram_result.get("ok"):
            clip_row["telegram_sent_at"] = _now_iso()
            clip_row["telegram_message_id"] = telegram_result.get("message_id")
            clip_row["telegram_chat_id"] = telegram_result.get("chat_id")
            clip_row["last_telegram_error"] = ""
            clip_row = self.store.save_content_queue_item(clip["clip_id"], clip_row)
            logs.append("[AGENT] Step 7/7 Telegram review sent")
        else:
            detail = str(telegram_result.get("error", "")).strip() or "telegram send failed"
            clip_row["last_telegram_error"] = detail
            clip_row = self.store.save_content_queue_item(clip["clip_id"], clip_row)
            logs.append("[ERROR] Telegram clip review send failed")
            logs.append(f"[ERROR] {detail}")
        return {
            "ok": True,
            "mode": mode,
            "clip": clip_row,
            "content_queue": self.store.list_content_queue(),
            "content_quality": self._load_content_quality_profile(mode),
            "logs": logs,
        }

    def deliver_clip_to_telegram(self, clip: Dict[str, Any]) -> Dict[str, Any]:
        return self.telegram.send_clip_review(dict(clip or {}))

    def resolve_content_review(self, clip_id: str, decision: str) -> Dict[str, Any]:
        cid = str(clip_id or "").strip()
        action = str(decision or "").strip().lower()
        if not cid:
            return {"ok": False, "error": "clip_id is required"}
        if action == "approve":
            return self._resolve_content_review(cid, approved=True)
        if action == "reject":
            return self._resolve_content_review(cid, approved=False)
        return {"ok": False, "error": "decision must be approve|reject"}

    def _resolve_content_review(self, clip_id: str, *, approved: bool) -> Dict[str, Any]:
        cid = str(clip_id or "").strip()
        row = self.store.get_content_queue_item(cid)
        if not row:
            return {"ok": False, "error": "clip not found"}
        row["approval_status"] = "approved" if approved else "rejected"
        row["operator_decision"] = "approve" if approved else "reject"
        row["resolved_at"] = _now_iso()
        archived = self.store.archive_content_queue_item(
            cid,
            {
                **row,
                "archive_reason": "telegram_approved" if approved else "telegram_rejected",
                "archived_by": "telegram_operator",
            },
        )
        quality = self._update_content_quality_feedback(row, approved=approved)
        return {
            "ok": True,
            "clip": archived or row,
            "content_queue": self.store.list_content_queue(),
            "content_quality": quality,
            "logs": [
                "[AUTH] Telegram operator approved WorkConnect Clip" if approved else "[AUTH] Telegram operator rejected WorkConnect Clip"
            ],
        }

    def get_platform_status(self) -> Dict[str, Any]:
        status = self.platform_connector.get_platform_status(self.DEFAULT_PLATFORM)
        return {
            "ok": True,
            "platform_auth": status,
            "default_platform": self.DEFAULT_PLATFORM,
            "experimental_platforms": ["x"],
        }

    def get_ui_bootstrap(self) -> Dict[str, Any]:
        post_meta = self.store.load_current_post_meta()
        current_post_id = post_meta.get("post_id")
        current_post_created_at = post_meta.get("created_at")
        next_cycle_at = self.store.get_meta("meta:x:next_cycle_at", "")
        reconcile = self._reconcile_publish_queue()
        queue = reconcile.get("publish_queue") or []
        platform_auth = self.platform_connector.get_platform_status(self.DEFAULT_PLATFORM)
        content_queue = self.store.list_content_queue()
        return {
            "platform_profiles": self.PLATFORM_PROFILES,
            "accounts": self._load_accounts_config(),
            "facebook_auth_config": self.facebook_publisher.get_runtime_config(),
            "content_config": self._load_content_config(),
            "content_modes": list(self.CONTENT_MODES),
            "content_queue": content_queue,
            "content_quality": self._load_content_quality_profile("workconnect_clips"),
            "lmdb_path": str(self.lmdb_dir),
            "dbi": ["users", "post_likers", "tasks", "approvals", "meta", "logs"],
            "current_post_id": current_post_id,
            "current_post_created_at": current_post_created_at,
            "next_cycle_at": next_cycle_at,
            "publish_queue": queue,
            "news_categories": list(self.NEWS_CATEGORIES),
            "news_sources": ["naver", "search"],
            "keyword_sets": self.news_collector.list_keyword_sets(),
            "social_default_platform": self.DEFAULT_PLATFORM,
            "task_queue_template": [
                "Connect Facebook Page",
                "Collect News",
                "Filter Relevant Articles",
                "Summarize Article",
                "Generate Candidate Facebook Post",
                "Multi-Agent Review",
                "Telegram Draft Review",
                "Approved For Publish",
                "Publish to Facebook",
            ],
            "news_crawl_interval_sec": self.NEWS_CRAWL_INTERVAL_SEC,
            "post_publish_wait_sec": self.POST_PUBLISH_WAIT_SEC,
            "x_post_interval_seconds": self.POST_PUBLISH_WAIT_SEC,
            "news_source": {
                "configured": bool(self.naver.configured),
                "allow_mock_news": bool(self.naver.allow_mock_news),
            },
            "platform_auth": platform_auth,
            "x_auth": platform_auth,
            "x_oauth": {"status": {"connected": False, "status": "experimental", "message": "X moved to experimental mode."}},
            "publish_queue_logs": reconcile.get("logs") or [],
            "meta_keys": [
                "meta:x:current_post_id",
                "meta:x:current_post_created_at",
                "meta:x:last_like_check_at",
                "meta:x:last_liker_fetch_at:<post_id>",
                "meta:x:last_fetched_like_count:<post_id>",
                "meta:news:last_crawl_at",
                "meta:news:last_candidate_pool",
                "x:rejected_link:<cycle_id>:<hash>",
            ],
        }

    def save_facebook_runtime_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        app_id = str(payload.get("app_id", "")).strip()
        app_secret = str(payload.get("app_secret", "")).strip()
        page_id = str(payload.get("page_id", "")).strip()
        user_long_lived_token = str(
            payload.get("user_long_lived_access_token", "") or payload.get("user_access_token", "")
        ).strip()
        result = self.facebook_publisher.save_runtime_config(
            app_id=app_id,
            app_secret=app_secret,
            page_id=page_id,
            user_long_lived_access_token=user_long_lived_token,
        )
        platform_status = self.facebook_publisher.refresh_facebook_platform_status()
        return {
            "ok": True,
            "logs": [
                "[AUTH] Facebook runtime config saved from UI",
                "[AUTH] Browser OAuth flow is disabled in runtime publish mode",
                "[AUTH] Facebook token reissue must be handled through admin/manual setup flow",
            ],
            "config": result.get("config") or {},
            "platform_auth": platform_status,
        }

    def _get_generation_cooldown(self) -> Dict[str, Any]:
        next_cycle_at = str(self.store.get_meta("meta:x:next_cycle_at", "") or "").strip()
        next_cycle_epoch = _to_epoch_seconds(next_cycle_at)
        now = time.time()
        if next_cycle_epoch is None:
            return {"active": False, "next_cycle_at": "", "wait_seconds": 0}
        wait_seconds = max(0, int(next_cycle_epoch - now))
        return {
            "active": wait_seconds > 0,
            "next_cycle_at": next_cycle_at,
            "wait_seconds": wait_seconds,
        }

    def _get_row_anchor_epoch(self, row: Dict[str, Any]) -> Optional[float]:
        candidates = [
            row.get("created_at"),
            row.get("telegram_sent_at"),
            row.get("updated_at"),
        ]
        for value in candidates:
            parsed = _to_epoch_seconds(str(value or "").strip())
            if parsed is not None:
                return parsed
        history = row.get("approval_history") or []
        if isinstance(history, list):
            for item in history:
                if not isinstance(item, dict):
                    continue
                parsed = _to_epoch_seconds(str(item.get("at", "")).strip())
                if parsed is not None:
                    return parsed
        return None

    def _reconcile_publish_queue(self) -> Dict[str, Any]:
        queue = self.store.list_publish_queue(include_published=True)
        if not queue:
            return {"ok": True, "publish_queue": [], "logs": []}

        now = time.time()
        ttl_sec = float(self.DRAFT_PENDING_TTL_HOURS * 3600)
        retry_sec = float(self.TELEGRAM_DRAFT_RETRY_INTERVAL_MINUTES * 60)
        updated_rows: List[Dict[str, Any]] = []
        logs: List[str] = []
        history = self._load_recent_article_history(limit=200, include_active=False)

        for original in queue:
            row = dict(original or {})
            draft_id = str(row.get("draft_id", "")).strip()
            approval_status = str(row.get("approval_status", "pending") or "pending").strip().lower()
            current_state = str(row.get("state", "") or "").strip().lower()
            is_resolved = bool(row.get("published", False)) or approval_status in {"rejected", "weekend"} or current_state in {
                "published",
                "rejected",
            }
            if is_resolved:
                self.store.archive_publish_queue_item(
                    draft_id,
                    {
                        **row,
                        "archive_reason": str(row.get("archive_reason", "")).strip() or "resolved_queue_cleanup",
                        "archived_by": str(row.get("archived_by", "")).strip() or "system_cleanup",
                    },
                )
                logs.append(f"[SYSTEM] Archived resolved draft from active queue: {draft_id}")
                continue
            title = str(row.get("article_title", "") or row.get("title", "")).strip()
            source_link = str(row.get("source_link", "") or row.get("url", "")).strip()
            if title and source_link and self._matches_recent_article_history(title, source_link, history):
                self.store.archive_publish_queue_item(
                    draft_id,
                    {
                        **row,
                        "approval_status": "rejected",
                        "operator_decision": str(row.get("operator_decision", "")).strip() or "system_duplicate_cleanup",
                        "state": "rejected",
                        "archive_reason": "duplicate_history_cleanup",
                        "archived_by": "system_cleanup",
                    },
                )
                logs.append(f"[SYSTEM] Archived duplicate draft already present in history: {draft_id}")
                continue
            is_unresolved = approval_status in {"pending", "modified"}
            last_retry_epoch = _to_epoch_seconds(str(row.get("last_telegram_retry_at", "")).strip())
            retry_due = last_retry_epoch is None or (now - last_retry_epoch) >= retry_sec

            if is_unresolved and not str(row.get("telegram_sent_at", "")).strip() and retry_due:
                resend = self.send_draft_review_to_telegram(row)
                if resend.get("ok"):
                    row = resend.get("draft") or row
                    logs.append(f"[AUTH] Draft re-sent to Telegram: {draft_id}")
                else:
                    row["last_telegram_retry_at"] = _now_iso()
                    row["last_telegram_error"] = str(resend.get("detail") or resend.get("error") or "unknown").strip()
                    row["approval_history"] = self._append_approval_history(
                        row,
                        "telegram_retry_failed",
                        "system",
                        row["last_telegram_error"],
                    )
                    logs.append(f"[ERROR] Telegram resend failed for draft {draft_id}: {resend.get('detail') or resend.get('error') or 'unknown'}")

            approval_status = str(row.get("approval_status", approval_status) or approval_status).strip().lower()
            is_unresolved = approval_status in {"pending", "modified"}
            anchor_epoch = self._get_row_anchor_epoch(row)
            expired = bool(
                is_unresolved
                and str(row.get("telegram_sent_at", "")).strip()
                and anchor_epoch is not None
                and (now - anchor_epoch) >= ttl_sec
            )
            if expired:
                row["approval_status"] = "rejected"
                row["operator_decision"] = "timeout"
                row["state"] = "rejected"
                row["approval_channel"] = row.get("approval_channel", "telegram") or "telegram"
                row["approved_for_queue"] = False
                row["approval_history"] = self._append_approval_history(
                    row,
                    "expired",
                    "system",
                    f"Draft expired after {self.DRAFT_PENDING_TTL_HOURS}h without operator decision.",
                )
                logs.append(f"[SYSTEM] Draft expired after TTL: {draft_id}")

            if draft_id:
                saved = self.store.save_publish_queue_item(draft_id, row)
                updated_rows.append(saved)
            else:
                updated_rows.append(row)

        return {"ok": True, "publish_queue": updated_rows, "logs": logs}

    def _append_approval_history(self, row: Dict[str, Any], event_type: str, actor: str, note: str = "") -> List[Dict[str, Any]]:
        history = row.get("approval_history") or []
        if not isinstance(history, list):
            history = []
        history.append(
            {
                "at": _now_iso(),
                "event": str(event_type or "").strip(),
                "actor": str(actor or "").strip(),
                "note": str(note or "").strip(),
            }
        )
        return history

    def send_draft_review_to_telegram(self, draft: Dict[str, Any]) -> Dict[str, Any]:
        row = dict(draft or {})
        result = self.telegram.send_draft_review(row)
        if not result.get("ok"):
            detail = str(result.get("error", "")).strip() or "telegram send failed"
            response_body = str(result.get("response_body", "")).strip()
            if response_body:
                detail = f"{detail} | response={response_body[:400]}"
            return {
                "ok": False,
                "error": "telegram_draft_send_failed",
                "detail": detail,
                "telegram_result": result,
            }
        row["approval_channel"] = "telegram"
        row["approval_status"] = "pending"
        row["telegram_sent_at"] = _now_iso()
        row["last_telegram_retry_at"] = row["telegram_sent_at"]
        row["last_telegram_error"] = ""
        row["telegram_message_id"] = result.get("message_id")
        row["telegram_chat_id"] = result.get("chat_id")
        row["operator_decision"] = ""
        row["operator_modified"] = bool(row.get("operator_modified", False))
        row["published"] = bool(row.get("published", False))
        row["approval_history"] = self._append_approval_history(row, "telegram_sent", "system", "Draft sent to Telegram for review.")
        saved = self.store.save_publish_queue_item(str(row.get("draft_id", "")).strip(), row)
        return {"ok": True, "draft": saved, "telegram_result": result}

    def get_x_auth_status(self) -> Dict[str, Any]:
        status = self.platform_connector.get_platform_status(self.DEFAULT_PLATFORM)
        return {
            "ok": True,
            "x_auth": status,
            "platform_auth": status,
            "oauth_setup": {"enabled": False, "mode": "facebook_page_runtime", "browser_oauth_disabled": True},
            "callback_routes_registered": [],
            "mode": "facebook_page_runtime",
        }

    def reissue_facebook_page_token(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        app_id = str(payload.get("app_id", "")).strip()
        app_secret = str(payload.get("app_secret", "")).strip()
        target_page_id = str(payload.get("page_id", "") or payload.get("target_page_id", "")).strip()
        user_short_lived_token = str(payload.get("user_short_lived_token", "")).strip()

        logs = [
            "[AUTH] Facebook admin reissue flow started",
            "[AUTH] Facebook runtime publish uses derived page token only",
            "[AUTH] Browser OAuth flow is disabled in runtime publish mode",
            "[AUTH] Facebook token reissue must be handled through admin/manual setup flow",
        ]
        result = self.facebook_publisher.reissue_facebook_page_token(
            app_id=app_id,
            app_secret=app_secret,
            user_short_lived_token=user_short_lived_token,
            target_page_id=target_page_id,
        )
        if result.get("ok"):
            status = result.get("platform_status") or {}
            logs.extend(
                [
                    "[AUTH] Long-lived user token exchange completed",
                    "[AUTH] Page access token fetched from user token",
                    "[AUTH] Derived Facebook page token validated",
                    f"[AUTH] Platform status: {status.get('status', 'UNKNOWN')}",
                ]
            )
            return {
                "ok": True,
                "logs": logs,
                "result": result,
                "config": self.facebook_publisher.get_runtime_config(),
                "platform_auth": status,
            }

        error = str(result.get("error", "facebook_reissue_failed")).strip()
        detail = str(result.get("detail", "")).strip()
        logs.append(f"[ERROR] Facebook admin reissue failed: {error}")
        if error == "facebook_permission_missing":
            missing_permissions = list(result.get("missing_permissions") or [])
            if missing_permissions:
                logs.append(f"[ERROR] Missing required permissions: {', '.join(missing_permissions)}")
            logs.append("[GUIDE] Reissue token with required scopes and verify page admin role.")
        elif error in {"facebook_page_not_found", "facebook_page_token_missing"}:
            logs.append("[GUIDE] Verify target page ID and confirm the app can access that page from /me/accounts.")
        else:
            logs.append("[GUIDE] Re-run admin reissue flow with app credentials, short-lived user token, and target page ID.")
        if detail:
            logs.append(f"[ERROR] {detail}")
        return {
            "ok": False,
            "logs": logs,
            "result": result,
            "config": self.facebook_publisher.get_runtime_config(),
            "error": error,
            "detail": detail,
        }

    def collect_news(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        news_source = str(payload.get("news_source", "naver")).strip().lower() or "naver"
        keyword_set = str(payload.get("keyword_set", "foreign_workers_korea")).strip().lower() or "foreign_workers_korea"
        collected = self.news_collector.collect(source=news_source, keyword_set=keyword_set)
        raw_items = collected.get("raw_items") or []
        filtered_items = collected.get("filtered_items") or []
        return {
            "ok": True,
            "news_source": news_source,
            "keyword_set": keyword_set,
            "raw_count": len(raw_items),
            "filtered_count": len(filtered_items),
            "raw_path": collected.get("raw_path"),
            "filtered_path": collected.get("filtered_path"),
            "items": filtered_items,
            "logs": [
                f"[AGENT] Collect News source={news_source} keyword_set={keyword_set}",
                f"[AGENT] raw={len(raw_items)} filtered={len(filtered_items)}",
                f"[SYSTEM] raw_store={collected.get('raw_path')}",
                f"[SYSTEM] filtered_store={collected.get('filtered_path')}",
            ],
        }

    def review_latest(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        collected = self.collect_news(payload)
        items = collected.get("items") or []
        if not items:
            return {
                "ok": True,
                "reviewed_items": [],
                "logs": list(collected.get("logs") or []) + ["[SYSTEM] No filtered news available for review."],
            }
        reviewed = self.news_reviewer.review_articles(items)
        rows = reviewed.get("reviewed_items") or []
        return {
            "ok": True,
            "reviewed_items": rows,
            "path": reviewed.get("path"),
            "logs": list(collected.get("logs") or [])
            + [
                "[AGENT] Review Relevance started",
                f"[AGENT] Reviewed articles: {len(rows)}",
                f"[SYSTEM] review_store={reviewed.get('path')}",
            ],
        }

    def consult_queue(self) -> Dict[str, Any]:
        queue = self.store.list_publish_queue()
        if not queue:
            return {"ok": True, "updated": [], "publish_queue": [], "logs": ["[SYSTEM] No queued publish candidates."]}
        updated: List[Dict[str, Any]] = []
        logs: List[str] = ["[AGENT] Review Agent re-checking queued candidates"]
        for row in queue:
            article = {
                "source": row.get("source", "queued"),
                "title": row.get("article_title", ""),
                "summary": row.get("post_summary", "") or row.get("summary", ""),
                "body_excerpt": row.get("body", ""),
                "url": row.get("source_link", ""),
                "published_at": row.get("published_at", ""),
                "keywords_hit": row.get("keywords_hit", []),
                "category_hint": row.get("category", ""),
            }
            research_review = {
                "is_relevant": bool(row.get("article_relevant", True)),
                "relevance_score": row.get("research_relevance_score", row.get("relevance_score", 0)),
                "audience": row.get("target_audience", ""),
                "why_it_matters": row.get("why_it_matters", ""),
                "practical_value": "high" if row.get("relevance_score", 0) else "medium",
                "risk_of_misleading": row.get("risk_of_misleading", "medium"),
                "post_angle": row.get("post_angle", ""),
                "post_summary": row.get("post_summary", ""),
                "cta_type": "read_more",
                "suggested_cta": row.get("suggested_cta", ""),
                "should_publish": bool(row.get("approved_for_queue", True)),
            }
            final_review = self.review_consultant.review_generated_post(article, research_review, str(row.get("body", "")).strip())
            row.update(
                {
                    "article_relevant": final_review.get("article_relevant"),
                    "content_quality_score": final_review.get("content_quality_score"),
                    "risk_of_misleading": final_review.get("risk_of_misleading"),
                    "post_tone": final_review.get("post_tone"),
                    "approved_for_queue": final_review.get("approved_for_queue"),
                    "review_notes": final_review.get("review_notes"),
                    "final_recommendation": final_review.get("final_recommendation"),
                }
            )
            saved = self.store.save_publish_queue_item(str(row.get("draft_id", "")).strip(), row)
            updated.append(saved)
        logs.append(f"[AGENT] Consulted queued candidates: {len(updated)}")
        return {"ok": True, "updated": updated, "publish_queue": self.store.list_publish_queue(), "logs": logs}

    def build_x_authorize_url(self) -> Dict[str, Any]:
        _auth_debug("[AUTHORIZE] service_build_x_authorize_url disabled oauth2")
        setup = self.x_client.get_oauth_setup_status()
        return {
            "ok": False,
            "error": "oauth2_disabled",
            "detail": "OAuth2 authorize/callback flow is disabled. Use OAuth1 direct posting.",
            "x_auth": self.x_client.get_auth_status(),
            "oauth_setup": setup,
            "logs": [
                "[AUTH] OAuth2 authorize is disabled",
                "[AUTH] X direct posting now uses OAuth1 credentials only",
            ],
        }

    def handle_x_oauth_callback(self, code: str, state: str) -> Dict[str, Any]:
        logs: List[str] = [
            "[AUTH] OAuth2 callback is disabled",
            "[AUTH] X direct posting now uses OAuth1 credentials only",
            f"[AUTH] code_present={bool(str(code or '').strip())}",
            f"[AUTH] state_present={bool(str(state or '').strip())}",
        ]
        _auth_debug("[CALLBACK] service_handle_x_oauth_callback disabled oauth2")
        setup = self.x_client.get_oauth_setup_status()
        return {
            "ok": False,
            "error": "oauth2_disabled",
            "detail": "OAuth2 callback/token exchange is disabled. Use OAuth1 direct posting.",
            "x_auth": self.x_client.get_auth_status(),
            "oauth_setup": setup,
            "logs": logs,
        }

    def _default_monitor_state(self) -> Dict[str, Any]:
        return {
            "monitoring": True,
            "closed": False,
            "last_like_count": 0,
            "last_fetch_count": 0,
            "last_fetch_at": None,
        }

    def register_published_post(self, post_id: str, created_at: Optional[str] = None) -> Dict[str, Any]:
        pid = str(post_id or "").strip()
        if not pid:
            return {"ok": False, "error": "post_id is required"}
        created = created_at or _now_iso()
        self.store.save_current_post_meta(post_id=pid, created_at=created)
        self.store.set_meta("meta:x:last_like_check_at", None)
        self.store.set_meta(f"meta:x:last_liker_fetch_at:{pid}", None)
        self.store.set_meta(f"meta:x:last_fetched_like_count:{pid}", 0)
        state = self._default_monitor_state()
        self.store.set_post_monitor_state(pid, state)
        return {"ok": True, "post_id": pid, "current_post_created_at": created, "monitor_state": state}

    def should_fetch_likers(self, like_count: int, state: Dict[str, Any]) -> Tuple[bool, str]:
        count = int(like_count or 0)
        last_fetch_count = int(state.get("last_fetch_count", 0) or 0)
        last_fetch_at = state.get("last_fetch_at")
        if count < 100:
            return False, "below_threshold"
        if not last_fetch_at:
            return True, "first_threshold_hit"
        if count - last_fetch_count >= 20:
            return True, "delta_20_plus"
        last_fetch_epoch = _to_epoch_seconds(str(last_fetch_at))
        if last_fetch_epoch is None:
            return True, "fetch_time_unknown"
        if time.time() - last_fetch_epoch >= 1800:
            return True, "30m_elapsed"
        return False, "not_needed"

    def fetch_liking_users(self, post_id: str, limit: int = 80) -> List[Dict[str, Any]]:
        result = self.x_client.get_liking_users(post_id=post_id, cursor=None, limit=limit)
        users = result.get("users") or []
        return [row for row in users if isinstance(row, dict)]

    def dedupe_saved_likers(self, post_id: str, likers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        new_rows: List[Dict[str, Any]] = []
        for row in likers:
            user_id = str(row.get("id", "")).strip()
            if not user_id:
                continue
            if self.store.has_post_liker(post_id, user_id):
                continue
            new_rows.append(row)
        return new_rows

    def save_new_likers(self, post_id: str, new_likers: List[Dict[str, Any]]) -> int:
        saved = 0
        for row in new_likers:
            user_id = str(row.get("id", "")).strip()
            if not user_id:
                continue
            username = str(row.get("username", "")).strip()
            self.store.save_post_liker(
                post_id,
                user_id,
                {
                    "username": username,
                    "captured_at": _now_iso(),
                    "source_post_id": post_id,
                },
            )
            user_state = self.store.get_user_state(user_id) or {}
            seen_posts = user_state.get("seen_posts") or []
            if not isinstance(seen_posts, list):
                seen_posts = []
            if post_id not in seen_posts:
                seen_posts.append(post_id)
            self.store.upsert_user_state(
                user_id,
                {
                    "user_id": user_id,
                    "username": username,
                    "seen_posts": seen_posts,
                    "candidate_sent": bool(user_state.get("candidate_sent", False)),
                    "approved": bool(user_state.get("approved", False)),
                    "following": bool(user_state.get("following", False)),
                },
            )
            saved += 1
        return saved

    def close_post_monitor(self, post_id: str) -> Dict[str, Any]:
        pid = str(post_id or "").strip()
        if not pid:
            return {"ok": False, "error": "post_id is required"}
        state = self.store.get_post_monitor_state(pid) or self._default_monitor_state()
        state["monitoring"] = False
        state["closed"] = True
        state["updated_at"] = _now_iso()
        self.store.set_post_monitor_state(pid, state)
        return {"ok": True, "post_id": pid, "monitor_state": state}

    def monitor_current_post(self) -> Dict[str, Any]:
        post_meta = self.store.load_current_post_meta()
        post_id = str(post_meta.get("post_id") or "").strip()
        if not post_id:
            state = self._default_monitor_state()
            state["monitoring"] = False
            return {
                "ok": True,
                "post_id": None,
                "current_post_created_at": None,
                "logs": ["[SYSTEM] No current post to monitor"],
                "monitor_state": state,
            }

        state = self.store.get_post_monitor_state(post_id) or self._default_monitor_state()
        post_created_at = post_meta.get("created_at")
        if bool(state.get("closed")):
            request_log = f"[SYSTEM] Like metric request: post={post_id}, synthetic=off"
            metrics = self.x_client.get_post_metrics(
                post_id=post_id,
                last_like_count=int(state.get("last_like_count", 0) or 0),
                allow_synthetic=False,
            )
            if not bool(metrics.get("ok")) and metrics.get("exists") is False:
                self.store.set_meta("meta:x:current_post_id", None)
                self.store.set_meta("meta:x:current_post_created_at", None)
                state["monitoring"] = False
                state["closed"] = True
                state["updated_at"] = _now_iso()
                self.store.set_post_monitor_state(post_id, state)
                return {
                    "ok": True,
                    "post_id": None,
                    "current_post_created_at": None,
                    "logs": [
                        request_log,
                        f"[SYSTEM] Like metric response: exists=false, error={metrics.get('error', 'unknown')}",
                        f"[SYSTEM] Current X post not found: {post_id}",
                        "[SYSTEM] No current post to monitor",
                    ],
                    "monitor_state": state,
                }
            return {
                "ok": True,
                "post_id": post_id,
                "current_post_created_at": post_created_at,
                "logs": [
                    request_log,
                    (
                        f"[SYSTEM] Like metric response: exists=true, like_count={int(metrics.get('like_count', 0) or 0)}, "
                        f"synthetic_applied={bool(metrics.get('synthetic_applied', False))}"
                    ),
                    f"[SYSTEM] Monitor already closed for {post_id}",
                ],
                "monitor_state": state,
            }

        prev_like = int(state.get("last_like_count", 0) or 0)
        request_log = f"[SYSTEM] Like metric request: post={post_id}, synthetic=off"
        metrics = self.x_client.get_post_metrics(
            post_id=post_id,
            last_like_count=prev_like,
            allow_synthetic=False,
        )
        if not bool(metrics.get("ok")) and metrics.get("exists") is False:
            self.store.set_meta("meta:x:current_post_id", None)
            self.store.set_meta("meta:x:current_post_created_at", None)
            state["monitoring"] = False
            state["closed"] = True
            state["updated_at"] = _now_iso()
            self.store.set_post_monitor_state(post_id, state)
            return {
                "ok": True,
                "post_id": None,
                "current_post_created_at": None,
                "logs": [
                    request_log,
                    f"[SYSTEM] Like metric response: exists=false, error={metrics.get('error', 'unknown')}",
                    f"[SYSTEM] Current X post not found: {post_id}",
                    "[SYSTEM] No current post to monitor",
                ],
                "monitor_state": state,
            }

        like_count = int(metrics.get("like_count", 0) or 0)
        now_iso = _now_iso()
        self.store.set_meta("meta:x:last_like_check_at", now_iso)
        logs = [
            request_log,
            (
                f"[SYSTEM] Like metric response: exists=true, like_count={like_count}, "
                f"synthetic_applied={bool(metrics.get('synthetic_applied', False))}"
            ),
            f"[SYSTEM] Monitoring current X post: {post_id}",
            f"[AGENT] Like count check: {like_count}",
        ]

        fetch_needed, reason = self.should_fetch_likers(like_count, state)
        saved_count = 0
        if fetch_needed:
            logs.append("[GROWTH] Like threshold reached, fetching likers")
            likers = self.fetch_liking_users(post_id)
            new_rows = self.dedupe_saved_likers(post_id, likers)
            saved_count = self.save_new_likers(post_id, new_rows)
            self.store.set_meta(f"meta:x:last_liker_fetch_at:{post_id}", now_iso)
            self.store.set_meta(f"meta:x:last_fetched_like_count:{post_id}", like_count)
            state["last_fetch_at"] = now_iso
            state["last_fetch_count"] = like_count
            logs.append(f"[GROWTH] {saved_count} new likers stored")

        state["monitoring"] = True
        state["closed"] = False
        state["last_like_count"] = like_count
        state["last_reason"] = reason
        state["updated_at"] = now_iso
        self.store.set_post_monitor_state(post_id, state)
        return {
            "ok": True,
            "post_id": post_id,
            "current_post_created_at": post_created_at,
            "like_count": like_count,
            "saved_count": saved_count,
            "logs": logs,
            "monitor_state": state,
        }

    def before_generate_next_post(self) -> Dict[str, Any]:
        post_meta = self.store.load_current_post_meta()
        post_id = str(post_meta.get("post_id") or "").strip()
        if not post_id:
            return {"ok": True, "post_id": None, "logs": ["[SYSTEM] No current post before next generation"]}

        logs = ["[SYSTEM] Final liker capture before next generation"]
        now_ts = time.time()
        state = self.store.get_post_monitor_state(post_id) or self._default_monitor_state()
        last_fetch_epoch = _to_epoch_seconds(str(state.get("last_fetch_at") or ""))
        recent_fetch = bool(last_fetch_epoch and (now_ts - last_fetch_epoch) < 60)

        saved_count = 0
        if not recent_fetch:
            logs.append(f"[SYSTEM] Like metric request: post={post_id}, synthetic=off")
            metrics = self.x_client.get_post_metrics(
                post_id=post_id,
                last_like_count=int(state.get("last_like_count", 0) or 0),
                allow_synthetic=False,
            )
            if not bool(metrics.get("ok")) and metrics.get("exists") is False:
                self.store.set_meta("meta:x:current_post_id", None)
                self.store.set_meta("meta:x:current_post_created_at", None)
                logs.append(f"[SYSTEM] Like metric response: exists=false, error={metrics.get('error', 'unknown')}")
                logs.append(f"[SYSTEM] Current X post not found: {post_id}")
                logs.append("[SYSTEM] No current post to monitor")
                close_result = self.close_post_monitor(post_id)
                if close_result.get("ok"):
                    logs.append(f"[SYSTEM] Post monitor closed: {post_id}")
                return {
                    "ok": True,
                    "post_id": None,
                    "saved_count": 0,
                    "logs": logs,
                    "monitor_state": self.store.get_post_monitor_state(post_id),
                }
            like_count = int(metrics.get("like_count", 0) or 0)
            logs.append(
                (
                    f"[SYSTEM] Like metric response: exists=true, like_count={like_count}, "
                    f"synthetic_applied={bool(metrics.get('synthetic_applied', False))}"
                )
            )
            likers = self.fetch_liking_users(post_id)
            new_rows = self.dedupe_saved_likers(post_id, likers)
            saved_count = self.save_new_likers(post_id, new_rows)
            now_iso = _now_iso()
            self.store.set_meta(f"meta:x:last_liker_fetch_at:{post_id}", now_iso)
            self.store.set_meta(f"meta:x:last_fetched_like_count:{post_id}", like_count)
            state["last_fetch_at"] = now_iso
            state["last_fetch_count"] = like_count
            state["last_like_count"] = like_count
            logs.append(f"[GROWTH] {saved_count} new likers stored")

        close_result = self.close_post_monitor(post_id)
        if close_result.get("ok"):
            logs.append(f"[SYSTEM] Post monitor closed: {post_id}")
        return {
            "ok": True,
            "post_id": post_id,
            "saved_count": saved_count,
            "logs": logs,
            "monitor_state": self.store.get_post_monitor_state(post_id),
        }

    def fetch_naver_top_news_by_category(self, category: str) -> List[Dict[str, Any]]:
        return self.naver.fetch_top_news(category=category, limit=10)

    def _is_recent_candidate(self, published_at: Optional[str]) -> bool:
        ts = _to_epoch_seconds(published_at)
        if ts is None:
            return True
        return (time.time() - ts) <= self.NEWS_MAX_AGE_SEC

    def build_news_candidate_pool(self, force: bool = False) -> List[Dict[str, Any]]:
        def _is_mock_row(row: Dict[str, Any]) -> bool:
            url = str((row or {}).get("url", "")).strip().lower()
            source = str((row or {}).get("source", "")).strip().lower()
            return source == "mock" or "news.example.com/" in url

        now_ts = time.time()
        last_crawl_at = _to_epoch_seconds(self.store.get_meta("meta:news:last_crawl_at", None))
        cached_pool = self.store.get_meta("meta:news:last_candidate_pool", [])
        cached_has_mock = bool(
            isinstance(cached_pool, list) and any(isinstance(row, dict) and _is_mock_row(row) for row in cached_pool)
        )
        if (
            not force
            and last_crawl_at is not None
            and (now_ts - last_crawl_at) < self.NEWS_CRAWL_INTERVAL_SEC
            and isinstance(cached_pool, list)
            and cached_pool
            and (self.naver.allow_mock_news or not cached_has_mock)
        ):
            return cached_pool

        pool: List[Dict[str, Any]] = []
        status_map: Dict[str, Any] = {}
        for category in self.NEWS_CATEGORIES:
            top_news = self.fetch_naver_top_news_by_category(category)
            status = self.naver.get_last_fetch_status(category)
            status_map[category] = status
            for idx, row in enumerate(top_news[:10], start=1):
                candidate = dict(row)
                candidate["category"] = category
                candidate["rank"] = int(candidate.get("rank", idx) or idx)
                if (not self.naver.allow_mock_news) and _is_mock_row(candidate):
                    continue
                pool.append(candidate)
        self.store.set_meta("meta:news:last_crawl_at", _now_iso())
        self.store.set_meta("meta:news:last_candidate_pool", pool)
        self.store.set_meta("meta:news:last_fetch_status", status_map)
        return pool

    def _title_overlap_ratio(self, left: str, right: str) -> float:
        left_tokens = {token for token in re.split(r"[^a-z0-9]+", _norm_text(left)) if token}
        right_tokens = {token for token in re.split(r"[^a-z0-9]+", _norm_text(right)) if token}
        if not left_tokens or not right_tokens:
            return 0.0
        common = left_tokens.intersection(right_tokens)
        return float(len(common)) / float(max(len(left_tokens), len(right_tokens)))

    def _load_recent_article_history(self, limit: int = 200, include_active: bool = False) -> List[Dict[str, str]]:
        history: List[Dict[str, str]] = []
        seen_keys: set[str] = set()
        for row in self.store.list_posted_links(limit=limit):
            if not isinstance(row, dict):
                continue
            title = str(row.get("title", "") or row.get("article_title", "")).strip()
            url = str(row.get("url", "") or row.get("source_link", "")).strip()
            normalized = normalize_source_url(url)
            canonical_article_id = str(row.get("canonical_article_id", "")).strip() or normalized.get("canonical_article_id", "")
            normalized_url = str(row.get("normalized_url", "")).strip() or normalized.get("normalized_url", "")
            key = canonical_article_id or f"{_norm_text(title)}|{_norm_text(normalized_url or url)}"
            if not title or key in seen_keys:
                continue
            seen_keys.add(key)
            history.append(
                {
                    "title": title,
                    "url": url,
                    "normalized_url": normalized_url,
                    "canonical_article_id": canonical_article_id,
                    "topic_key": _topic_signature(title),
                    "saved_at": str(row.get("saved_at", "")).strip(),
                }
            )
        remaining = max(0, int(limit) - len(history))
        if remaining > 0:
            for row in self.store.list_archived_publish_items(limit=remaining * 2 or limit):
                if not isinstance(row, dict):
                    continue
                title = str(row.get("article_title", "") or row.get("title", "")).strip()
                url = str(row.get("source_link", "") or row.get("url", "")).strip()
                normalized = normalize_source_url(url)
                canonical_article_id = str(row.get("canonical_article_id", "")).strip() or normalized.get("canonical_article_id", "")
                normalized_url = str(row.get("normalized_url", "")).strip() or normalized.get("normalized_url", "")
                key = canonical_article_id or f"{_norm_text(title)}|{_norm_text(normalized_url or url)}"
                if not title or key in seen_keys:
                    continue
                seen_keys.add(key)
                history.append(
                    {
                        "title": title,
                        "url": url,
                        "normalized_url": normalized_url,
                        "canonical_article_id": canonical_article_id,
                        "topic_key": _topic_signature(title),
                        "saved_at": str(row.get("archived_at", "") or row.get("updated_at", "")).strip(),
                    }
                )
                if len(history) >= limit:
                    break
        if include_active and len(history) < limit:
            for row in self.store.list_publish_queue(include_published=False):
                if not isinstance(row, dict):
                    continue
                approval_status = str(row.get("approval_status", "")).strip().lower()
                state = str(row.get("state", "")).strip().lower()
                if approval_status in {"rejected", "weekend"} or state in {"rejected", "published"}:
                    continue
                title = str(row.get("article_title", "") or row.get("title", "")).strip()
                url = str(row.get("source_link", "") or row.get("url", "")).strip()
                normalized = normalize_source_url(url)
                canonical_article_id = str(row.get("canonical_article_id", "")).strip() or normalized.get("canonical_article_id", "")
                normalized_url = str(row.get("normalized_url", "")).strip() or normalized.get("normalized_url", "")
                key = canonical_article_id or f"{_norm_text(title)}|{_norm_text(normalized_url or url)}"
                if not title or key in seen_keys:
                    continue
                seen_keys.add(key)
                history.append(
                    {
                        "title": title,
                        "url": url,
                        "normalized_url": normalized_url,
                        "canonical_article_id": canonical_article_id,
                        "topic_key": _topic_signature(title),
                        "saved_at": (
                            str(row.get("telegram_sent_at", "")).strip()
                            or str(row.get("created_at", "")).strip()
                            or str(row.get("updated_at", "")).strip()
                        ),
                    }
                )
                if len(history) >= limit:
                    break
        return history

    def _matches_recent_article_history(self, title: str, url: str, history: List[Dict[str, str]]) -> bool:
        current_title = str(title or "").strip()
        normalized = normalize_source_url(url)
        current_url = str(url or "").strip()
        current_canonical_id = normalized.get("canonical_article_id", "")
        current_normalized_url = normalized.get("normalized_url", "")
        current_topic_key = _topic_signature(current_title)
        for row in history:
            history_url = str((row or {}).get("url", "")).strip()
            history_normalized_url = str((row or {}).get("normalized_url", "")).strip()
            history_canonical_id = str((row or {}).get("canonical_article_id", "")).strip()
            history_title = str((row or {}).get("title", "")).strip()
            history_saved_at = _to_epoch_seconds(str((row or {}).get("saved_at", "")).strip())
            history_topic_key = str((row or {}).get("topic_key", "")).strip()
            if current_canonical_id and history_canonical_id and current_canonical_id == history_canonical_id:
                return True
            if history_normalized_url and current_normalized_url and history_normalized_url == current_normalized_url:
                return True
            if history_url and current_url and _norm_text(history_url) == _norm_text(current_url):
                return True
            if history_title and self._title_overlap_ratio(current_title, history_title) >= 0.8:
                return True
            if (
                current_topic_key
                and history_topic_key
                and current_topic_key == history_topic_key
                and history_saved_at is not None
                and (time.time() - history_saved_at) <= self.TOPIC_COOLDOWN_SEC
            ):
                return True
        return False

    def _filter_news_candidates(self, candidates: List[Dict[str, Any]], cycle_id: Optional[str], skip_age_check: bool) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        dedupe_keys: set[str] = set()
        dedupe_title_keys: set[str] = set()
        cycle = str(cycle_id or "").strip()
        history = self._load_recent_article_history(limit=200, include_active=True)
        for row in candidates:
            if not isinstance(row, dict):
                continue
            url = str(row.get("url", "")).strip()
            normalized_url = normalize_source_url(url).get("normalized_url", "")
            title = str(row.get("title", "")).strip()
            source = str(row.get("source", "")).strip().lower()
            if not url or not title:
                continue
            if (not self.naver.allow_mock_news) and (source == "mock" or "news.example.com/" in url.lower()):
                continue
            if self.store.is_posted_link(url):
                continue
            if cycle and self.store.is_rejected_link(cycle, url):
                continue
            if self._matches_recent_article_history(title, url, history):
                continue
            if (not skip_age_check) and (not self._is_recent_candidate(row.get("published_at"))):
                continue
            dedupe_key = f"{_norm_text(title)}|{_norm_text(normalized_url or url)}"
            if dedupe_key in dedupe_keys:
                continue
            title_key = _norm_text(title)
            if title_key in dedupe_title_keys:
                continue
            dedupe_keys.add(dedupe_key)
            dedupe_title_keys.add(title_key)
            filtered.append(row)
        return filtered

    def filter_news_candidates(self, candidates: List[Dict[str, Any]], cycle_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return self._filter_news_candidates(candidates=candidates, cycle_id=cycle_id, skip_age_check=False)

    def _headline_log_lines(self, candidates: List[Dict[str, Any]], per_category: int = 3) -> List[str]:
        grouped: Dict[str, List[str]] = {}
        for row in candidates:
            if not isinstance(row, dict):
                continue
            category = str(row.get("category", "unknown")).strip().lower() or "unknown"
            title = str(row.get("title", "")).strip()
            if not title:
                continue
            bucket = grouped.setdefault(category, [])
            if title in bucket:
                continue
            if len(bucket) < max(1, int(per_category)):
                bucket.append(title)
        lines: List[str] = []
        for category in self.NEWS_CATEGORIES:
            titles = grouped.get(category, [])
            if not titles:
                lines.append(f"[AGENT] Headlines {category}: (none)")
                continue
            joined = " | ".join(f"{idx + 1}) {title}" for idx, title in enumerate(titles))
            lines.append(f"[AGENT] Headlines {category}: {joined}")
        return lines

    def _filter_reason_counts(self, candidates: List[Dict[str, Any]], cycle_id: Optional[str], skip_age_check: bool = False) -> Dict[str, int]:
        counts = {
            "invalid": 0,
            "mock_blocked": 0,
            "posted": 0,
            "rejected": 0,
            "history_dup": 0,
            "too_old": 0,
            "dup_key": 0,
            "dup_title": 0,
            "passed": 0,
        }
        dedupe_keys: set[str] = set()
        dedupe_title_keys: set[str] = set()
        cycle = str(cycle_id or "").strip()
        history = self._load_recent_article_history(limit=200, include_active=True)
        for row in candidates:
            if not isinstance(row, dict):
                counts["invalid"] += 1
                continue
            url = str(row.get("url", "")).strip()
            normalized_url = normalize_source_url(url).get("normalized_url", "")
            title = str(row.get("title", "")).strip()
            source = str(row.get("source", "")).strip().lower()
            if not url or not title:
                counts["invalid"] += 1
                continue
            if (not self.naver.allow_mock_news) and (source == "mock" or "news.example.com/" in url.lower()):
                counts["mock_blocked"] += 1
                continue
            if self.store.is_posted_link(url):
                counts["posted"] += 1
                continue
            if cycle and self.store.is_rejected_link(cycle, url):
                counts["rejected"] += 1
                continue
            if self._matches_recent_article_history(title, url, history):
                counts["history_dup"] += 1
                continue
            if (not skip_age_check) and (not self._is_recent_candidate(row.get("published_at"))):
                counts["too_old"] += 1
                continue
            dedupe_key = f"{_norm_text(title)}|{_norm_text(normalized_url or url)}"
            if dedupe_key in dedupe_keys:
                counts["dup_key"] += 1
                continue
            title_key = _norm_text(title)
            if title_key in dedupe_title_keys:
                counts["dup_title"] += 1
                continue
            dedupe_keys.add(dedupe_key)
            dedupe_title_keys.add(title_key)
            counts["passed"] += 1
        return counts

    def score_news_candidate(self, candidate: Dict[str, Any], recent_titles: Optional[List[str]] = None) -> float:
        rank = int(candidate.get("rank", 10) or 10)
        base_score = max(0.0, 12.0 - float(rank))
        text = f"{candidate.get('title', '')} {candidate.get('summary', '')}".lower()
        keyword_bonus = 0.0
        for keyword in self.GLOBAL_KEYWORDS:
            if keyword.lower() in text:
                keyword_bonus += 4.0
        recency_bonus = 0.0
        pub_ts = _to_epoch_seconds(candidate.get("published_at"))
        if pub_ts is not None:
            age_sec = max(0.0, time.time() - pub_ts)
            if age_sec <= 2 * 60 * 60:
                recency_bonus = 3.0
        similarity_penalty = 0.0
        title = str(candidate.get("title", ""))
        for prev_title in recent_titles or []:
            overlap = self._title_overlap_ratio(title, prev_title)
            if overlap >= 0.6:
                similarity_penalty = max(similarity_penalty, 4.0)
            elif overlap >= 0.4:
                similarity_penalty = max(similarity_penalty, 2.0)
        recent_rejected = self.store.get_meta("meta:news:recent_rejected_titles", [])
        rejected_penalty = 0.0
        if isinstance(recent_rejected, list):
            for rejected_title in recent_rejected[:20]:
                overlap = self._title_overlap_ratio(title, str(rejected_title or ""))
                if overlap >= 0.6:
                    rejected_penalty = max(rejected_penalty, 6.0)
                elif overlap >= 0.4:
                    rejected_penalty = max(rejected_penalty, 3.0)
        return base_score + keyword_bonus + recency_bonus - similarity_penalty - rejected_penalty

    def select_best_candidate(self, candidates: List[Dict[str, Any]], recent_titles: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        best_row: Optional[Dict[str, Any]] = None
        best_score = -1.0
        for row in candidates:
            score = self.score_news_candidate(row, recent_titles=recent_titles)
            row["score"] = score
            if score > best_score:
                best_score = score
                best_row = row
        return best_row

    def extract_article_content(self, url: str) -> Dict[str, Any]:
        return self.naver.get_article_content(url)

    def extract_article_thumbnail(self, url: str) -> Dict[str, Any]:
        return self.naver.get_article_thumbnail(url)

    def normalize_article_data(self, article: Dict[str, Any]) -> Dict[str, Any]:
        row = dict(article or {})
        normalized = {
            "category": str(row.get("category", "general")).strip().lower() or "general",
            "title": str(row.get("title", "")).strip(),
            "summary": str(row.get("summary", "")).strip(),
            "url": str(row.get("url", "")).strip(),
            "content": str(row.get("content", "")).strip(),
            "published_at": row.get("published_at") or _now_iso(),
            "thumbnail_url": str(row.get("thumbnail_url", "")).strip(),
            "source": str(row.get("source", "naver")).strip(),
            "rank": int(row.get("rank", 10) or 10),
        }
        if not normalized["thumbnail_url"] and normalized["url"]:
            thumb = self.extract_article_thumbnail(normalized["url"])
            normalized["thumbnail_url"] = str(thumb.get("thumbnail_url", "")).strip()
        return normalized

    def enforce_x_post_limit(self, text: str, max_chars: int = 500) -> str:
        clean = str(text or "").strip()
        if len(clean) <= max_chars:
            return clean
        trimmed = clean[: max_chars - 4].rstrip()
        return f"{trimmed}..."

    def _pick_summary_sentences(self, text: str, max_lines: int = 3) -> List[str]:
        parts = re.split(r"(?<=[.!?])\s+", str(text or "").strip())
        lines = []
        for part in parts:
            clean = re.sub(r"\s+", " ", part).strip(" -")
            if len(clean) < 20:
                continue
            lines.append(clean)
            if len(lines) >= max_lines:
                break
        return lines

    def generate_english_post_draft(self, article: Dict[str, Any], config: Dict[str, Any]) -> str:
        category_key = str(article.get("category", "news")).strip().lower()
        category = category_key.title()
        title = str(article.get("title", "")).strip()
        summary = str(article.get("summary", "")).strip()
        content = str(article.get("content", "")).strip()
        source_link = str(article.get("url", "")).strip()
        tone = str(config.get("tone", "analytical")).strip().lower()
        options = config.get("options", {}) if isinstance(config.get("options"), dict) else {}

        hook_prefix = {
            "hook": "Big update from Korea:",
            "analytical": "Korea market brief:",
            "conversational": "Korea update:",
            "seo": "Korea headline:",
            "neutral": "Korea update:",
        }.get(tone, "Korea update:")
        category_hint = self.CATEGORY_TONE_HINTS.get(category_key, "")

        lines = ["🇰🇷 Korea Update", "", f"{hook_prefix} {title}".strip(), ""]
        lines.append(f"Why it matters ({category}):")
        picks = self._pick_summary_sentences(content or summary, max_lines=3)
        if not picks:
            picks = self._pick_summary_sentences(summary, max_lines=2) or [summary or title]
        for item in picks[:3]:
            lines.append(f"- {item}")
        if category_hint:
            lines.extend(["", category_hint])
        if options.get("includeLinks", True):
            lines.extend(["", "Source:", source_link])
        if options.get("includeHashtags"):
            lines.append("#KoreaUpdate #XNews")
        if options.get("includeCTA"):
            lines.append("Follow for more Korea updates.")

        return self.enforce_x_post_limit("\n".join([line for line in lines if line is not None]), max_chars=500)

    def add_draft_to_publish_queue(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        draft = payload.get("draft") if isinstance(payload.get("draft"), dict) else payload
        resend_telegram_review = bool(payload.get("resend_telegram_review"))
        draft_id = str(draft.get("draft_id", "")).strip() or f"draft_{int(time.time() * 1000)}"
        row = self.store.save_draft_queue_item(
            draft_id,
            {
                "draft_id": draft_id,
                "platform": draft.get("platform", "facebook"),
                "category": draft.get("category", "general"),
                "article_title": draft.get("article_title", ""),
                "source_link": draft.get("source_link", ""),
                "thumbnail_url": draft.get("thumbnail_url", ""),
                "title": draft.get("title", ""),
                "body": draft.get("body", ""),
                "body_preview": draft.get("body_preview", ""),
                "cycle_id": draft.get("cycle_id", ""),
                "relevance_score": draft.get("relevance_score", 0),
                "why_it_matters": draft.get("why_it_matters", ""),
                "target_audience": draft.get("target_audience", ""),
                "post_angle": draft.get("post_angle", ""),
                "post_summary": draft.get("post_summary", ""),
                "suggested_cta": draft.get("suggested_cta", ""),
                "summary": draft.get("summary", draft.get("post_summary", "")),
                "generated_post": draft.get("generated_post", draft.get("body", "")),
                "research_relevance_score": draft.get("research_relevance_score", draft.get("relevance_score", 0)),
                "risk_of_misleading": draft.get("risk_of_misleading", "medium"),
                "final_recommendation": draft.get("final_recommendation", "revise"),
                "review_notes": draft.get("review_notes", []),
                "content_quality_score": draft.get("content_quality_score", 0),
                "article_relevant": draft.get("article_relevant", False),
                "post_tone": draft.get("post_tone", "neutral"),
                "approved_for_queue": draft.get("approved_for_queue", False),
                "approval_channel": draft.get("approval_channel", "telegram"),
                "approval_status": draft.get("approval_status", "pending"),
                "approval_history": draft.get("approval_history", []),
                "created_at": draft.get("created_at", _now_iso()),
                "telegram_sent_at": draft.get("telegram_sent_at"),
                "last_telegram_retry_at": draft.get("last_telegram_retry_at"),
                "last_telegram_error": draft.get("last_telegram_error", ""),
                "telegram_message_id": draft.get("telegram_message_id"),
                "telegram_chat_id": draft.get("telegram_chat_id"),
                "operator_decision": draft.get("operator_decision", ""),
                "operator_modified": bool(draft.get("operator_modified", False)),
                "original_title": draft.get("original_title", draft.get("article_title", "")),
                "generated_title": draft.get("generated_title", draft.get("title", "")),
                "generated_body": draft.get("generated_body", draft.get("body", "")),
                "published": bool(draft.get("published", False)),
                "state": draft.get("state", "telegram_pending"),
            },
        )
        self.store.upsert_task(
            f"queue_{draft_id}",
            {
                "task_id": f"queue_{draft_id}",
                "type": "publish_queue_add",
                "state": "telegram_pending",
                "draft_id": draft_id,
                "created_at": _now_iso(),
            },
        )
        logs = ["[PUBLISH] Draft queued for Telegram review"]
        if resend_telegram_review or not row.get("telegram_sent_at"):
            telegram_sync = self.send_draft_review_to_telegram(row)
            if telegram_sync.get("ok"):
                row = telegram_sync.get("draft") or row
                logs.append("[AUTH] Draft review sent to Telegram")
            else:
                row["last_telegram_retry_at"] = _now_iso()
                row["last_telegram_error"] = str(telegram_sync.get("detail") or telegram_sync.get("error") or "unknown").strip()
                row["approval_history"] = self._append_approval_history(
                    row,
                    "telegram_send_failed",
                    "system",
                    row["last_telegram_error"],
                )
                row = self.store.save_publish_queue_item(draft_id, row)
                logs.append("[ERROR] Telegram draft send failed; will retry automatically")
                logs.append(f"[ERROR] {row['last_telegram_error']}")
        return {
            "ok": True,
            "draft": row,
            "publish_queue": self.store.list_publish_queue(),
            "logs": logs,
        }

    def list_publish_queue(self) -> Dict[str, Any]:
        poll_logs: List[str] = []
        poll_result = self.poll_telegram_updates()
        poll_logs.extend(poll_result.get("logs") or [])
        reconcile = self._reconcile_publish_queue()
        return {
            "ok": True,
            "publish_queue": reconcile.get("publish_queue") or [],
            "logs": poll_logs + (reconcile.get("logs") or []),
        }

    def test_telegram_delivery(self) -> Dict[str, Any]:
        result = self.telegram.test_connection()
        if result.get("ok"):
            return {
                "ok": True,
                "logs": ["[AUTH] Telegram test message sent"],
                "telegram": {
                    "chat_id": result.get("chat_id"),
                    "message_id": result.get("message_id"),
                    "dry_run": bool(result.get("dry_run")),
                },
            }
        detail = str(result.get("error", "")).strip() or "telegram send failed"
        response_body = str(result.get("response_body", "")).strip()
        if response_body:
            detail = f"{detail} | response={response_body[:400]}"
        return {
            "ok": False,
            "error": "telegram_test_failed",
            "detail": detail,
            "logs": ["[ERROR] Telegram test message failed", f"[ERROR] {detail}"],
        }

    def poll_telegram_updates(self) -> Dict[str, Any]:
        if not self.telegram.enabled:
            return {"ok": True, "updates_processed": 0, "logs": []}
        if self._telegram_poll_inflight:
            return {"ok": True, "updates_processed": 0, "logs": ["[SYSTEM] Telegram polling skipped: already in flight"]}
        self._telegram_poll_inflight = True
        offset = self.store.get_meta("meta:telegram:last_update_id", None)
        try:
            try:
                offset_value = int(offset) if offset is not None else None
            except Exception:
                offset_value = None
            fetched = self.telegram.get_updates(offset=offset_value, limit=20, timeout=2)
            if not fetched.get("ok"):
                detail = str(fetched.get("error", "")).strip() or "telegram getUpdates failed"
                transport = str(fetched.get("transport", "")).strip()
                response_body = str(fetched.get("response_body", "")).strip()
                httpx_error = str(fetched.get("httpx_error", "")).strip()
                if httpx_error:
                    detail = f"{detail} | httpx_error={httpx_error}"
                if response_body:
                    detail = f"{detail} | response={response_body[:600]}"
                return {
                    "ok": False,
                    "error": "telegram_poll_failed",
                    "detail": detail,
                    "logs": [
                        f"[ERROR] Telegram polling failed via {transport or 'unknown'}",
                        f"[ERROR] {detail}",
                    ],
                }

            updates = fetched.get("result") or []
            logs: List[str] = []
            processed = 0
            last_update_id = offset_value

            body_preview = str(fetched.get("response_body", "")).strip()
            if body_preview:
                logs.append(f"[AUTH] Telegram poll ok via {fetched.get('transport') or 'unknown'}")
                logs.append(f"[AUTH] Telegram poll body: {body_preview[:300]}")

            for update in updates:
                if not isinstance(update, dict):
                    continue
                update_id = update.get("update_id")
                if isinstance(update_id, int):
                    last_update_id = update_id + 1

                callback_query = update.get("callback_query") or {}
                if isinstance(callback_query, dict) and callback_query:
                    callback_data = str(callback_query.get("data", "")).strip()
                    callback_id = str(callback_query.get("id", "")).strip()
                    if callback_data:
                        logs.append(f"[AUTH] Telegram callback received: {callback_data}")
                        result = self.handle_callback_data(callback_data)
                        logs.extend(result.get("logs") or [])
                        processed += 1
                        if callback_id:
                            answer_text = "Processed" if result.get("ok") else str(result.get("error", "Failed"))[:100]
                            self.telegram.answer_callback_query(callback_id, answer_text)
                    continue

                message = update.get("message") or update.get("edited_message") or {}
                if isinstance(message, dict) and message:
                    text = str(message.get("text", "")).strip()
                    if text.startswith("/"):
                        logs.append(f"[AUTH] Telegram text command received: {text}")
                        result = self.handle_telegram_text_command(text, {})
                        if result.get("ok") or result.get("error") != "unsupported command":
                            logs.extend(result.get("logs") or [])
                            processed += 1

            if last_update_id is not None:
                self.store.set_meta("meta:telegram:last_update_id", last_update_id)
            return {"ok": True, "updates_processed": processed, "logs": logs}
        finally:
            self._telegram_poll_inflight = False

    def _set_draft_approval_state(
        self,
        draft_id: str,
        *,
        approval_status: str,
        operator_decision: str,
        note: str,
        state: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        row = self.store.get_publish_queue_item(draft_id)
        if not row:
            return {"ok": False, "error": "draft not found"}
        row["approval_status"] = approval_status
        row["operator_decision"] = operator_decision
        row["state"] = state or approval_status
        row["approval_channel"] = "telegram"
        row["approval_history"] = self._append_approval_history(row, approval_status, "telegram_operator", note)
        if extra:
            row.update(extra)
        saved = self.store.save_publish_queue_item(draft_id, row)
        return {"ok": True, "draft": saved, "publish_queue": self.store.list_publish_queue()}

    def _archive_draft_with_status(
        self,
        draft_id: str,
        *,
        approval_status: str,
        operator_decision: str,
        note: str,
        archive_reason: str,
        state: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        row = self.store.get_publish_queue_item(draft_id)
        if not row:
            return {"ok": False, "error": "draft not found"}
        row["approval_status"] = approval_status
        row["operator_decision"] = operator_decision
        row["state"] = state or approval_status
        row["approval_channel"] = "telegram"
        row["approval_history"] = self._append_approval_history(row, approval_status, "telegram_operator", note)
        if extra:
            row.update(extra)
        source_link = str(row.get("source_link", "")).strip()
        if source_link and approval_status in {"rejected", "weekend"}:
            self.store.save_rejected_link(
                cycle_id=str(row.get("cycle_id", "")).strip(),
                link=source_link,
                payload={
                    "draft_id": draft_id,
                    "title": str(row.get("article_title", "") or row.get("title", "")).strip(),
                    "approval_status": approval_status,
                    "operator_decision": operator_decision,
                },
            )
        archived = self.store.archive_publish_queue_item(
            draft_id,
            {
                **row,
                "archive_reason": archive_reason,
                "archived_by": "telegram_operator",
            },
        )
        return {
            "ok": True,
            "draft": archived or row,
            "publish_queue": self.store.list_publish_queue(),
        }

    def handle_telegram_approve(self, draft_id: str) -> Dict[str, Any]:
        result = self._set_draft_approval_state(
            str(draft_id or "").strip(),
            approval_status="approved",
            operator_decision="approve",
            note="Approved from Telegram.",
            state="approved_for_publish",
            extra={"approved_for_queue": True, "operator_override_publish": True},
        )
        if result.get("ok"):
            result["logs"] = ["[AUTH] Telegram operator approved Facebook draft"]
        return result

    def handle_telegram_reject(self, draft_id: str) -> Dict[str, Any]:
        result = self._archive_draft_with_status(
            str(draft_id or "").strip(),
            approval_status="rejected",
            operator_decision="reject",
            note="Rejected from Telegram.",
            state="rejected",
            archive_reason="rejected_from_telegram",
            extra={"approved_for_queue": False},
        )
        if result.get("ok"):
            result["logs"] = ["[AUTH] Telegram operator rejected Facebook draft"]
        return result

    def handle_telegram_save_weekend(self, draft_id: str) -> Dict[str, Any]:
        state_row = self._archive_draft_with_status(
            str(draft_id or "").strip(),
            approval_status="weekend",
            operator_decision="save_weekend",
            note="Saved for weekend article from Telegram.",
            state="rejected",
            archive_reason="saved_for_weekend",
            extra={"approved_for_queue": False},
        )
        if not state_row.get("ok"):
            return state_row
        result = self.save_for_weekend_article(str(draft_id or "").strip())
        if result.get("ok"):
            result["logs"] = ["[AUTH] Telegram operator saved draft for weekend article"]
        return result

    def handle_telegram_modify(self, draft_id: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        did = str(draft_id or "").strip()
        row = self.store.get_publish_queue_item(did)
        if not row:
            return {"ok": False, "error": "draft not found"}
        body = payload or {}
        new_title = str(body.get("title", "") or "").strip()
        new_body = str(body.get("body", "") or "").strip()
        modify_requested_only = not new_title and not new_body
        if modify_requested_only:
            row["approval_status"] = "modified"
            row["operator_decision"] = "modify"
            row["operator_modified"] = True
            row["state"] = "reviewing"
            row["approval_history"] = self._append_approval_history(
                row,
                "modified",
                "telegram_operator",
                "Modify requested from Telegram. Send title/body to save changes.",
            )
            saved = self.store.save_publish_queue_item(did, row)
            return {
                "ok": True,
                "draft": saved,
                "publish_queue": self.store.list_publish_queue(),
                "logs": ["[AUTH] Telegram operator requested draft modification"],
            }

        if new_title:
            row["title"] = new_title
            row["generated_title"] = new_title
        if new_body:
            row["body"] = new_body
            row["generated_body"] = new_body
            row["body_preview"] = new_body if len(new_body) <= 220 else f"{new_body[:220]}..."
        row["approval_status"] = "pending"
        row["operator_decision"] = "modify"
        row["operator_modified"] = True
        row["state"] = "telegram_pending"
        row["approval_history"] = self._append_approval_history(
            row,
            "modified",
            "telegram_operator",
            "Draft content updated from Telegram.",
        )
        saved = self.store.save_publish_queue_item(did, row)
        telegram_sync = self.send_draft_review_to_telegram(saved)
        if telegram_sync.get("ok"):
            saved = telegram_sync.get("draft") or saved
        return {
            "ok": True,
            "draft": saved,
            "publish_queue": self.store.list_publish_queue(),
            "logs": ["[AUTH] Telegram operator modified draft and re-sent review card"],
        }

    def handle_telegram_text_command(self, text: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        command = str(text or "").strip()
        if not command.startswith("/"):
            return {"ok": False, "error": "invalid command"}
        parts = command.split(maxsplit=2)
        verb = parts[0].lower()
        draft_id = parts[1].strip() if len(parts) >= 2 else ""
        body_payload = payload or {}
        if verb == "/approve":
            return self.handle_telegram_approve(draft_id)
        if verb == "/reject":
            return self.handle_telegram_reject(draft_id)
        if verb == "/save_weekend":
            return self.handle_telegram_save_weekend(draft_id)
        if verb == "/modify":
            return self.handle_telegram_modify(draft_id, body_payload)
        return {"ok": False, "error": "unsupported command"}

    def cancel_publish(self, draft_id: str) -> Dict[str, Any]:
        did = str(draft_id or "").strip()
        if not did:
            return {"ok": False, "error": "draft_id is required"}
        row = self.store.get_publish_queue_item(did)
        if row:
            row["approval_status"] = "rejected"
            row["operator_decision"] = "reject"
            row["state"] = "rejected"
            row["approval_channel"] = row.get("approval_channel", "ui")
            row["approval_history"] = self._append_approval_history(row, "rejected", "ui_operator", "Rejected from UI.")
            self.store.save_publish_queue_item(did, row)
            self.store.upsert_task(
                f"queue_{did}",
                {
                    "task_id": f"queue_{did}",
                    "type": "publish_queue_cancel",
                    "state": "rejected",
                    "draft_id": did,
                    "created_at": _now_iso(),
                },
            )
        return {
            "ok": bool(row),
            "draft_id": did,
            "publish_queue": self.store.list_publish_queue(),
            "logs": ["[PUBLISH] Draft marked rejected"] if row else ["[ERROR] Draft not found"],
        }

    def save_for_weekend_article(self, draft_id: str) -> Dict[str, Any]:
        did = str(draft_id or "").strip()
        if not did:
            return {"ok": False, "error": "draft_id is required"}
        row = self.store.get_publish_queue_item(did)
        if not row:
            return {"ok": False, "error": "draft not found"}
        target = self.project_root / "data" / "publish_candidates" / "weekend_articles.jsonl"
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({**row, "saved_for_weekend_at": _now_iso()}, ensure_ascii=False) + "\n")
        row["approval_status"] = "weekend"
        row["operator_decision"] = "save_weekend"
        row["state"] = "rejected"
        row["approval_history"] = self._append_approval_history(row, "weekend", "operator", "Saved for weekend article.")
        self.store.save_publish_queue_item(did, row)
        return {
            "ok": True,
            "draft_id": did,
            "publish_queue": self.store.list_publish_queue(),
            "saved_path": str(target),
            "logs": ["[PUBLISH] Draft saved for weekend article"],
        }

    def reject_draft(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        cycle_id = str(payload.get("cycle_id", "")).strip()
        draft_id = str(payload.get("draft_id", "")).strip()
        source_link = str(payload.get("source_link", "")).strip()
        article_title = str(payload.get("article_title", "")).strip()
        category = str(payload.get("category", "")).strip()
        if not cycle_id:
            return {"ok": False, "error": "cycle_id is required"}
        if not source_link:
            return {"ok": False, "error": "source_link is required"}
        if self.store.is_rejected_link(cycle_id=cycle_id, link=source_link):
            return {
                "ok": True,
                "cycle_id": cycle_id,
                "draft_id": draft_id,
                "logs": ["[PREVIEW] Draft rejected, retrying with next candidate"],
            }
        self.store.save_rejected_link(
            cycle_id=cycle_id,
            link=source_link,
            payload={
                "draft_id": draft_id,
                "article_title": article_title,
                "category": category,
            },
        )
        recent_rejected = self.store.get_meta("meta:news:recent_rejected_titles", [])
        if not isinstance(recent_rejected, list):
            recent_rejected = []
        if article_title:
            recent_rejected = [article_title] + recent_rejected
            self.store.set_meta("meta:news:recent_rejected_titles", recent_rejected[:20])
        self.store.upsert_task(
            f"reject_{draft_id or int(time.time() * 1000)}",
            {
                "task_id": f"reject_{draft_id or int(time.time() * 1000)}",
                "type": "draft_reject",
                "state": "complete",
                "cycle_id": cycle_id,
                "draft_id": draft_id,
                "source_link": source_link,
                "created_at": _now_iso(),
            },
        )
        self.telegram.send_alert(f"[PREVIEW] Draft rejected: {article_title or source_link}")
        return {
            "ok": True,
            "cycle_id": cycle_id,
            "draft_id": draft_id,
            "logs": ["[PREVIEW] Draft rejected, retrying with next candidate"],
        }

    def approve_publish(self, draft_id: str) -> Dict[str, Any]:
        did = str(draft_id or "").strip()
        if not did:
            return {"ok": False, "error": "draft_id is required"}
        row = self.store.get_publish_queue_item(did)
        if not row:
            return {"ok": False, "error": "draft not found"}
        approval_status = str(row.get("approval_status", "") or "").strip().lower()
        if approval_status != "approved":
            return {
                "ok": False,
                "error": "telegram_approval_required",
                "detail": f"approval_status={approval_status or 'pending'}",
                "publish_queue": self.store.list_publish_queue(),
                "logs": [
                    "[ERROR] Telegram approval is required before Facebook publish",
                    f"[ERROR] approval_status={approval_status or 'pending'}",
                ],
            }
        final_recommendation = str(row.get("final_recommendation", "") or "").strip().lower()
        operator_override_publish = bool(row.get("operator_override_publish", False))
        if final_recommendation and final_recommendation != "publish" and not operator_override_publish:
            return {
                "ok": False,
                "error": "review_gate_blocked",
                "detail": f"final_recommendation={final_recommendation}",
                "publish_queue": self.store.list_publish_queue(),
                "logs": [
                    "[ERROR] Publish approval blocked by review gate",
                    f"[ERROR] final_recommendation={final_recommendation}",
                ],
            }
        publish_result = self.publish_x_content({"draft": row})
        if not publish_result.get("ok"):
            fail_logs = ["[ERROR] Publish approval failed"]
            fail_logs.extend(publish_result.get("logs", []))
            publish_result["logs"] = fail_logs
            return publish_result
        row["published"] = True
        row["approval_status"] = "approved"
        row["state"] = "published"
        row["operator_override_publish"] = bool(operator_override_publish)
        row["approval_history"] = self._append_approval_history(
            row,
            "published",
            "system",
            f"Facebook published: {((publish_result.get('result') or {}).get('post_id')) or '-'}",
        )
        self.store.archive_publish_queue_item(
            did,
            {
                **row,
                "archive_reason": "published_to_facebook",
                "archived_by": "system",
            },
        )
        link = str(row.get("source_link", "")).strip()
        if link:
            self.store.save_posted_link(
                link,
                payload={
                    "draft_id": did,
                    "post_id": ((publish_result.get("result") or {}).get("post_id")),
                    "saved_at": _now_iso(),
                },
            )
        queue = self.store.list_publish_queue()
        logs = ["[PUBLISH] Telegram-approved draft sent to Facebook"]
        logs.extend(publish_result.get("logs", []))
        logs.append("[SYSTEM] current_post_id updated")
        logs.append(f"[SYSTEM] Waiting {int(self.POST_PUBLISH_WAIT_SEC // 60)} minutes before next cycle")
        self.store.set_meta(
            "meta:x:next_cycle_at",
            (datetime.now(timezone.utc) + timedelta(seconds=self.POST_PUBLISH_WAIT_SEC)).isoformat(),
        )
        return {"ok": True, "draft_id": did, "publish_result": publish_result, "publish_queue": queue, "logs": logs}

    def _build_draft_payload(self, article: Dict[str, Any], body: str, cycle_id: str) -> Dict[str, Any]:
        review = article.get("review", {}) if isinstance(article.get("review"), dict) else {}
        final_review = article.get("final_review", {}) if isinstance(article.get("final_review"), dict) else {}
        draft_id = f"draft_{int(time.time() * 1000)}"
        preview = body if len(body) <= 220 else f"{body[:220]}..."
        return {
            "draft_id": draft_id,
            "platform": "facebook",
            "category": article.get("category_hint", article.get("category", "general")),
            "article_title": article.get("title", ""),
            "original_title": article.get("title", ""),
            "source_link": article.get("url", ""),
            "thumbnail_url": article.get("thumbnail_url", ""),
            "title": article.get("title", "") or "Facebook News Candidate",
            "generated_title": article.get("title", "") or "Facebook News Candidate",
            "body": body,
            "body_preview": preview,
            "summary": article.get("summary", ""),
            "generated_post": body,
            "generated_body": body,
            "cycle_id": cycle_id,
            "published_at": article.get("published_at"),
            "relevance_score": review.get("relevance_score", 0),
            "research_relevance_score": review.get("relevance_score", 0),
            "why_it_matters": review.get("why_it_matters", ""),
            "target_audience": review.get("audience", ""),
            "post_angle": review.get("post_angle", ""),
            "post_summary": review.get("post_summary", ""),
            "suggested_cta": review.get("suggested_cta", ""),
            "review_should_publish": review.get("should_publish", False),
            "risk_of_misleading": final_review.get("risk_of_misleading", review.get("risk_of_misleading", "medium")),
            "content_quality_score": final_review.get("content_quality_score", 0),
            "final_recommendation": final_review.get("final_recommendation", "revise"),
            "review_notes": final_review.get("review_notes", []),
            "article_relevant": final_review.get("article_relevant", review.get("is_relevant", False)),
            "post_tone": final_review.get("post_tone", "neutral"),
            "approved_for_queue": final_review.get("approved_for_queue", False),
            "approval_channel": "telegram",
            "approval_status": "pending",
            "approval_history": [],
            "operator_decision": "",
            "operator_modified": False,
            "published": False,
        }

    def _generate_facebook_post(self, article: Dict[str, Any], topic: str = "") -> str:
        review = article.get("review", {}) if isinstance(article.get("review"), dict) else {}
        summary = str(review.get("post_summary") or article.get("summary") or article.get("title") or "").strip()
        why = str(review.get("why_it_matters") or "").strip()
        cta = str(review.get("suggested_cta") or "Read the full article for details.").strip()
        audience = str(review.get("audience") or "Foreign workers and job seekers in Korea").strip()
        lines = [
            f"{summary}",
            f"",
            f"Why it matters: {why}" if why else "",
            f"Audience: {audience}",
            f"",
            cta,
        ]
        if topic:
            lines.insert(0, f"[{topic}]")
        return "\n".join(line for line in lines if line is not None).strip()

    def generate_x_content(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        topic = str(payload.get("topic", "")).strip()
        tone = str(payload.get("tone", "practical")).strip().lower()
        length = str(payload.get("length", "medium")).strip().lower()
        news_source = str(payload.get("news_source", "naver")).strip().lower() or "naver"
        keyword_set = str(payload.get("keyword_set", "foreign_workers_korea")).strip().lower() or "foreign_workers_korea"
        phase = str(payload.get("phase", "normal")).strip().lower() or "normal"
        manual_run = phase == "manual"
        cycle_id = str(payload.get("cycle_id", "")).strip() or f"cycle_{int(time.time() * 1000)}"
        logs = [
            "[AGENT] Social Agent started Facebook pipeline",
            "[AGENT] Step 1/8 Connect Facebook Page",
            f"[AGENT] Facebook status: {self.platform_connector.get_platform_status(self.DEFAULT_PLATFORM).get('status')}",
            "[AGENT] Step 2/8 Collect News",
        ]
        cooldown = self._get_generation_cooldown()
        if cooldown.get("active") and not manual_run:
            wait_seconds = int(cooldown.get("wait_seconds") or 0)
            wait_minutes = max(1, int((wait_seconds + 59) // 60))
            logs.extend(
                [
                    "[SYSTEM] Generation cooldown active after recent publish",
                    f"[SYSTEM] next_cycle_at={cooldown.get('next_cycle_at')}",
                    f"[SYSTEM] Waiting about {wait_minutes} minute(s) before collecting the next article batch",
                ]
            )
            return {
                "ok": True,
                "cooldown_active": True,
                "no_candidate": True,
                "cycle_id": cycle_id,
                "generation_state": "monitoring",
                "wait_seconds": wait_seconds,
                "next_cycle_at": cooldown.get("next_cycle_at"),
                "logs": logs,
            }

        collected = self.news_collector.collect(source=news_source, keyword_set=keyword_set)
        raw_filtered = collected.get("filtered_items") or []
        filtered = self.filter_news_candidates(raw_filtered, cycle_id=cycle_id)
        logs.append(
            f"[AGENT] News collected: raw={len(collected.get('raw_items') or [])}, "
            f"collector_filtered={len(raw_filtered)}, runtime_filtered={len(filtered)}"
        )
        logs.append(f"[AGENT] raw_store={collected.get('raw_path')}")
        logs.append(f"[AGENT] filtered_store={collected.get('filtered_path')}")
        if not filtered:
            logs.append("[SYSTEM] No new candidate article available")
            return {"ok": True, "no_candidate": True, "cycle_id": cycle_id, "generation_state": "collecting", "logs": logs}

        logs.append("[AGENT] Step 3/8 Filter Relevant Articles")
        consultations: List[Dict[str, Any]] = []
        for row in filtered[:5]:
            consultation = self.review_consultant.consult_article(row, topic=topic, tone=tone, length=length)
            research = consultation.get("research_review") or {}
            final_review = consultation.get("final_review") or {}
            logs.append(
                "[AGENT] Consult result "
                f"title={row.get('title', '')} "
                f"relevant={bool(research.get('is_relevant'))} "
                f"recommendation={final_review.get('final_recommendation', 'reject')}"
            )
            consultations.append(consultation)

        accepted = [
            row
            for row in consultations
            if str(((row.get("final_review") or {}).get("final_recommendation"))).strip().lower() in {"publish", "revise"}
        ]
        if not accepted:
            logs.append("[SYSTEM] No candidate survived multi-agent review")
            return {"ok": True, "no_candidate": True, "cycle_id": cycle_id, "generation_state": "reviewing", "logs": logs}

        accepted.sort(
            key=lambda row: (
                float(((row.get("research_review") or {}).get("relevance_score")) or 0),
                float(((row.get("final_review") or {}).get("content_quality_score")) or 0),
            ),
            reverse=True,
        )
        selected = accepted[0]
        article = dict(selected.get("article") or {})
        article["review"] = selected.get("research_review") or {}
        article["final_review"] = selected.get("final_review") or {}
        article["summary"] = selected.get("summary") or article.get("summary", "")
        logs.append(f"[AGENT] Step 4/8 Summarize Article")
        logs.append(f"[AGENT] Step 5/8 Generate Candidate Facebook Post")
        draft_text = str(selected.get("generated_post") or "").strip() or self._generate_facebook_post(article=article, topic=topic)
        logs.append("[AGENT] Step 6/8 Multi-Agent Review")
        logs.append(
            "[AGENT] Review Agent verdict: "
            f"{str(((selected.get('final_review') or {}).get('final_recommendation')) or 'reject')}"
        )
        draft = self._build_draft_payload(article=article, body=draft_text, cycle_id=cycle_id)

        task_id = f"gen_{int(time.time() * 1000)}"
        self.store.upsert_task(
            task_id,
            {
                "task_id": task_id,
                "type": "facebook_generation",
                "state": "waiting_approval",
                "platform": "facebook",
                "topic": topic,
                "tone": tone,
                "length": length,
                "candidate_url": article.get("url", ""),
                "cycle_id": cycle_id,
                "news_source": news_source,
                "keyword_set": keyword_set,
                "created_at": _now_iso(),
            },
        )
        logs.append("[AGENT] Step 7/8 Telegram Draft Review")
        queue_result = self.add_draft_to_publish_queue({"draft": draft, "resend_telegram_review": True})
        logs.extend(queue_result.get("logs", []))
        logs.append("[AGENT] Step 8/8 Waiting for Telegram operator decision")
        return {
            "ok": True,
            "task_id": task_id,
            "cycle_id": cycle_id,
            "generation_state": "telegram_pending",
            "platform": "facebook",
            "logs": logs,
            "draft": queue_result.get("draft") or draft,
            "publish_queue": queue_result.get("publish_queue") or self.store.list_publish_queue(),
        }

    def publish_x_content(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        draft = payload.get("draft") if isinstance(payload.get("draft"), dict) else {}
        title = str(draft.get("title", "")).strip()
        body = str(draft.get("body", "")).strip()
        source_link = str(draft.get("source_link", "")).strip()
        if not body:
            return {"ok": False, "error": "draft.body is required"}

        publish_result = self.facebook_publisher.publish_text(text=body, link=source_link)
        task_id = f"publish_{int(time.time() * 1000)}"
        post_id = str(publish_result.get("post_id", "") or "")
        publish_ok = bool(publish_result.get("ok"))
        published = bool(publish_result.get("published"))
        dry_run = bool(publish_result.get("dry_run"))
        self.store.upsert_task(
            task_id,
            {
                "task_id": task_id,
                "type": "facebook_publish",
                "state": "complete" if (publish_ok and published) else "error",
                "platform": "facebook",
                "post_id": post_id,
                "title": title,
                "source_link": source_link,
                "publish_ok": publish_ok,
                "published": published,
                "dry_run": dry_run,
                "created_at": _now_iso(),
            },
        )
        logs = [
            "[PUBLISH] Publishing to Facebook",
            "[AUTH] Facebook runtime publish uses derived page token only",
            "[AUTH] Browser OAuth flow is disabled in runtime publish mode",
            (
                f"[PUBLISH] Facebook response: ok={publish_ok}, published={published}, "
                f"dry_run={dry_run}, post_id={post_id or '-'}"
            ),
        ]
        if publish_result.get("credential_presence"):
            logs.append(
                f"[AUTH] credential_presence={json.dumps(publish_result.get('credential_presence'), ensure_ascii=False)}"
            )
        if publish_result.get("token_status"):
            logs.append(
                "[AUTH] Facebook token status: "
                f"{publish_result.get('token_status')} ({publish_result.get('token_source', 'env')})"
            )

        if not publish_ok or not published:
            detail = str(publish_result.get("detail", "")).strip()
            error = str(publish_result.get("error", "publish_failed")).strip()
            logs.append(f"[ERROR] Facebook publish failed: {error}")
            status_code = publish_result.get("status_code")
            response_body = publish_result.get("response_body")
            if status_code is not None:
                try:
                    body_text = json.dumps(response_body, ensure_ascii=False)
                except Exception:
                    body_text = str(response_body)
                if len(body_text) > 480:
                    body_text = f"{body_text[:477]}..."
                logs.append(f"[ERROR] status={status_code}, body={body_text}")
            if error == "facebook_config_missing":
                missing_values = list(publish_result.get("missing_values") or [])
                logs.append("[ERROR] Facebook Page configuration is required.")
                if missing_values:
                    logs.append(f"[ERROR] Missing values: {', '.join(missing_values)}")
                logs.append("[SYSTEM] Auto generation remains OFF")
            if error == "facebook_token_expired":
                logs.append("[AUTH] Facebook token validation failed: expired")
                logs.append("[SYSTEM] Auto generation remains OFF")
                logs.append("[ERROR] Facebook user or derived page token is no longer valid. Manual reissue required.")
                logs.append("[GUIDE] Run admin reissue flow with short-lived user token to refresh the long-lived user token.")
            if error == "facebook_permission_missing":
                missing_permissions = list(publish_result.get("missing_permissions") or [])
                logs.append("[ERROR] Facebook publish failed: permission_invalid")
                if missing_permissions:
                    logs.append(f"[ERROR] Missing required permissions: {', '.join(missing_permissions)}")
                logs.append("[SYSTEM] Auto generation remains OFF")
                logs.append("[GUIDE] Reissue token with required scopes and verify page admin role.")
            if error == "facebook_page_not_found":
                logs.append("[ERROR] Facebook Page is not reachable with the derived page token.")
                logs.append("[SYSTEM] Auto generation remains OFF")
                logs.append("[GUIDE] Verify target page ID and confirm the app can access that page from /me/accounts.")
            if error == "facebook_page_token_missing":
                logs.append("[ERROR] Facebook Page token is missing.")
                logs.append("[SYSTEM] Auto generation remains OFF")
            if error == "post_interval_blocked":
                logs.append("[ERROR] Facebook publish blocked: 1-hour interval guard active")
            if error == "duplicate_post_text":
                logs.append("[ERROR] Facebook publish blocked: identical text matches the most recent post")
            guidance = str(publish_result.get("guidance", "")).strip()
            if guidance:
                logs.append(f"[GUIDE] {guidance}")
            if detail:
                logs.append(f"[ERROR] {detail}")
            return {
                "ok": False,
                "error": error,
                "detail": detail,
                "task_id": task_id,
                "logs": logs,
                "result": publish_result,
            }

        if post_id:
            logs.append(f"[PUBLISH] Post created successfully: {post_id}")
        monitor_result = self.register_published_post(post_id) if post_id else {"ok": False}
        if source_link and post_id:
            self.store.save_posted_link(source_link, {"post_id": post_id, "title": title})
        if post_id:
            self.store.set_meta(
                "meta:x:next_cycle_at",
                (datetime.now(timezone.utc) + timedelta(seconds=self.POST_PUBLISH_WAIT_SEC)).isoformat(),
            )
        if monitor_result.get("ok"):
            logs.append(f"[SYSTEM] Monitoring current Facebook post: {post_id}")
            logs.append(f"[SYSTEM] Waiting {int(self.POST_PUBLISH_WAIT_SEC // 60)} minutes before next cycle")
        return {
            "ok": True,
            "task_id": task_id,
            "logs": logs,
            "result": publish_result,
            "monitor": monitor_result,
        }

    def run_x_post_test(self, text: Optional[str] = None) -> Dict[str, Any]:
        message = str(text or "OAuth1 posting test from the_light_house_project+777").strip()
        result = self.x_client.post_text_to_x(message)
        logs = [
            "[TEST] OAuth1 single-post test started",
            f"[TEST] text_len={len(message)}",
            f"[TEST] ok={bool(result.get('ok'))} published={bool(result.get('published'))} dry_run={bool(result.get('dry_run'))}",
        ]
        if result.get("status_code") is not None:
            logs.append(f"[TEST] status={result.get('status_code')}")
        if result.get("response_body") is not None:
            logs.append(f"[TEST] body={json.dumps(result.get('response_body'), ensure_ascii=False)}")
        if result.get("post_id"):
            logs.append(f"[TEST] post_id={result.get('post_id')}")
        if result.get("detail"):
            logs.append(f"[TEST] detail={result.get('detail')}")
        return {"ok": bool(result.get("ok")), "logs": logs, "result": result}

    def _is_growth_candidate(self, user: Dict[str, Any]) -> Tuple[bool, str]:
        if int(user.get("last_active_days", 999)) > 30:
            return False, "inactive"
        following = max(int(user.get("following_count", 0)), 1)
        followers = int(user.get("followers_count", 0))
        ratio = followers / following
        if ratio < 0.1 or ratio > 15:
            return False, "ratio_outlier"
        if bool(user.get("is_bot_suspected")):
            return False, "bot_suspected"
        return True, "ok"

    def collect_growth_candidates(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        post_id = str(payload.get("post_id", "")).strip()
        if not post_id:
            return {"ok": False, "error": "post_id is required"}

        cursor = payload.get("cursor")
        limit = int(payload.get("limit", 20) or 20)
        liker_data = self.x_client.get_liking_users(post_id=post_id, cursor=str(cursor) if cursor else None, limit=limit)
        users = liker_data.get("users") or []
        next_cursor = liker_data.get("next_cursor")
        logs: List[str] = []
        pending: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []

        for row in users:
            if not isinstance(row, dict):
                continue
            user_id = str(row.get("id", "")).strip()
            if not user_id:
                continue

            passed, reason = self._is_growth_candidate(row)
            if not passed:
                skipped.append({"user_id": user_id, "reason": reason})
                continue

            local_state = self.store.get_user_state(user_id) or {}
            if bool(local_state.get("following")):
                skipped.append({"user_id": user_id, "reason": "already_following_local"})
                continue
            if self.store.get_following_cache(user_id):
                skipped.append({"user_id": user_id, "reason": "already_following_cache"})
                continue
            if self.x_client.is_following(user_id):
                self.store.set_following_cache(user_id, True)
                self.store.upsert_user_state(
                    user_id,
                    {
                        "username": row.get("username", ""),
                        "following": True,
                        "candidate_sent": False,
                        "approved": False,
                        "skipped": False,
                        "last_seen_post_id": post_id,
                    },
                )
                skipped.append({"user_id": user_id, "reason": "already_following_remote"})
                continue

            tg_result = self.telegram.send_candidate(row)
            if not tg_result.get("ok"):
                skipped.append({"user_id": user_id, "reason": "telegram_send_failed"})
                continue

            self.store.upsert_user_state(
                user_id,
                {
                    "user_id": user_id,
                    "username": row.get("username", ""),
                    "following": False,
                    "candidate_sent": True,
                    "approved": False,
                    "skipped": False,
                    "last_seen_post_id": post_id,
                },
            )
            approval = self.store.upsert_approval(
                user_id,
                {
                    "user_id": user_id,
                    "username": row.get("username", ""),
                    "status": "pending",
                    "message_id": tg_result.get("message_id"),
                    "chat_id": tg_result.get("chat_id"),
                    "created_at": _now_iso(),
                    "followers": row.get("followers_count", 0),
                    "following_count": row.get("following_count", 0),
                    "post_id": post_id,
                },
            )
            pending.append(approval)
            logs.append("[GROWTH] Candidate sent to Telegram")

        self.store.set_meta("meta:x:last_following_sync", _now_iso())
        self.store.set_meta(f"meta:x:last_liker_cursor:{post_id}", next_cursor)
        task_id = f"growth_{int(time.time() * 1000)}"
        self.store.upsert_task(
            task_id,
            {
                "task_id": task_id,
                "type": "x_growth_collect",
                "state": "complete",
                "post_id": post_id,
                "pending_count": len(pending),
                "skipped_count": len(skipped),
                "created_at": _now_iso(),
            },
        )
        return {
            "ok": True,
            "task_id": task_id,
            "post_id": post_id,
            "next_cursor": next_cursor,
            "pending_approvals": pending,
            "skipped": skipped,
            "logs": logs,
        }

    def list_pending_approvals(self) -> Dict[str, Any]:
        return {"ok": True, "pending_approvals": self.store.list_pending_approvals()}

    def apply_telegram_decision(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        user_id = str(payload.get("user_id", "")).strip()
        decision = str(payload.get("decision", "")).strip().lower()
        if not user_id:
            return {"ok": False, "error": "user_id is required"}
        if decision not in {"approve", "skip", "block"}:
            return {"ok": False, "error": "decision must be one of approve|skip|block"}

        approval = self.store.get_approval(user_id) or {}
        logs: List[str] = []
        status = "pending"
        follow_result: Optional[Dict[str, Any]] = None
        if decision == "approve":
            follow_result = self.x_client.follow_user(user_id)
            if follow_result.get("ok"):
                self.store.set_following_cache(user_id, True)
                self.store.upsert_user_state(
                    user_id,
                    {"following": True, "approved": True, "skipped": False, "candidate_sent": True},
                )
                status = "approved"
                logs.append("[FOLLOW] Approved follow executed")
            else:
                status = "error"
        elif decision == "skip":
            status = "skipped"
            self.store.upsert_user_state(
                user_id, {"following": False, "approved": False, "skipped": True, "candidate_sent": True}
            )
        else:
            status = "blocked"
            self.store.upsert_user_state(
                user_id,
                {"following": False, "approved": False, "skipped": True, "blocked": True, "candidate_sent": True},
            )

        updated = self.store.upsert_approval(user_id, {**approval, "status": status, "updated_at": _now_iso()})
        return {
            "ok": True,
            "user_id": user_id,
            "decision": decision,
            "approval": updated,
            "follow_result": follow_result,
            "logs": logs,
        }

    def handle_callback_data(self, callback_data: str) -> Dict[str, Any]:
        parsed = self.telegram.handle_callback(callback_data)
        if not parsed.get("ok"):
            return parsed
        action = str(parsed.get("action", "")).strip().lower()
        target_id = str(parsed.get("user_id", "")).strip()
        if action == "draft_approve":
            return self.handle_telegram_approve(target_id)
        if action == "draft_reject":
            return self.handle_telegram_reject(target_id)
        if action == "draft_weekend":
            return self.handle_telegram_save_weekend(target_id)
        if action == "draft_modify":
            return self.handle_telegram_modify(target_id)
        if action == "clip_approve":
            return self._resolve_content_review(target_id, approved=True)
        if action == "clip_reject":
            return self._resolve_content_review(target_id, approved=False)
        return self.apply_telegram_decision({"user_id": target_id, "decision": action})

    # -------- Required named hooks (aliases) --------
    def monitorCurrentPost(self) -> Dict[str, Any]:
        return self.monitor_current_post()

    def shouldFetchLikers(self, likeCount: int, state: Dict[str, Any]) -> Tuple[bool, str]:
        return self.should_fetch_likers(like_count=likeCount, state=state)

    def fetchLikingUsers(self, postId: str) -> List[Dict[str, Any]]:
        return self.fetch_liking_users(post_id=postId)

    def dedupeSavedLikers(self, postId: str, likers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self.dedupe_saved_likers(post_id=postId, likers=likers)

    def saveNewLikers(self, postId: str, newLikers: List[Dict[str, Any]]) -> int:
        return self.save_new_likers(post_id=postId, new_likers=newLikers)

    def beforeGenerateNextPost(self) -> Dict[str, Any]:
        return self.before_generate_next_post()

    def closePostMonitor(self, postId: str) -> Dict[str, Any]:
        return self.close_post_monitor(post_id=postId)

    def registerPublishedPost(self, postId: str) -> Dict[str, Any]:
        return self.register_published_post(post_id=postId)

    def fetchNaverTopNewsByCategory(self, category: str) -> List[Dict[str, Any]]:
        return self.fetch_naver_top_news_by_category(category)

    def buildNewsCandidatePool(self) -> List[Dict[str, Any]]:
        return self.build_news_candidate_pool(force=False)

    def filterNewsCandidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self.filter_news_candidates(candidates)

    def scoreNewsCandidate(self, candidate: Dict[str, Any]) -> float:
        return self.score_news_candidate(candidate)

    def selectBestCandidate(self, candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        return self.select_best_candidate(candidates)

    def extractArticleContent(self, url: str) -> Dict[str, Any]:
        return self.extract_article_content(url)

    def extractArticleThumbnail(self, url: str) -> Dict[str, Any]:
        return self.extract_article_thumbnail(url)

    def normalizeArticleData(self, article: Dict[str, Any]) -> Dict[str, Any]:
        return self.normalize_article_data(article)

    def rejectDraft(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.reject_draft(payload)

    def generateEnglishPostDraft(self, article: Dict[str, Any], config: Dict[str, Any]) -> str:
        return self.generate_english_post_draft(article, config)

    def enforceXPostLimit(self, text: str, maxChars: int = 500) -> str:
        return self.enforce_x_post_limit(text=text, max_chars=maxChars)
