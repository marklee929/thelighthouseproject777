from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import lmdb


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_TRACKING_QUERY_KEYS = {"fbclid", "gclid", "igshid", "mc_cid", "mc_eid", "ref", "ref_src", "spm"}
_STABLE_ARTICLE_ID_KEYS = {"idxno", "articleid", "id", "aid", "article_no", "news_no", "no", "seq"}


def _looks_like_timestamp_param(key: str, value: str) -> bool:
    norm_key = str(key or "").strip().lower()
    norm_value = str(value or "").strip().lower()
    if not norm_key or not norm_value:
        return False
    time_markers = ("date", "time", "ts", "timestamp", "dt", "updated", "regdate", "from")
    if any(marker in norm_key for marker in time_markers):
        return True
    digits_only = "".join(ch for ch in norm_value if ch.isdigit())
    if len(digits_only) >= 8 and digits_only == norm_value:
        return True
    if len(norm_value) >= 10 and norm_value[:4].isdigit() and norm_value[4] in {"-", "/"}:
        return True
    return False


def normalize_source_url(link: str) -> Dict[str, str]:
    raw = str(link or "").strip()
    if not raw:
        return {"raw_url": "", "normalized_url": "", "canonical_article_id": ""}
    try:
        split = urlsplit(raw)
    except Exception:
        return {"raw_url": raw, "normalized_url": raw.lower(), "canonical_article_id": ""}

    kept_params: List[Tuple[str, str]] = []
    canonical_article_id = ""
    for key, value in parse_qsl(split.query, keep_blank_values=True):
        norm_key = str(key or "").strip()
        lower_key = norm_key.lower()
        lower_value = str(value or "").strip()
        if lower_key.startswith("utm_") or lower_key in _TRACKING_QUERY_KEYS:
            continue
        if lower_key in _STABLE_ARTICLE_ID_KEYS and lower_value:
            if not canonical_article_id:
                canonical_article_id = f"{lower_key}:{lower_value}"
            kept_params.append((norm_key, lower_value))
            continue
        if _looks_like_timestamp_param(lower_key, lower_value):
            continue
        kept_params.append((norm_key, lower_value))

    normalized_query = urlencode(sorted(kept_params), doseq=True)
    normalized_url = urlunsplit(
        (
            (split.scheme or "https").lower(),
            split.netloc.lower(),
            split.path.rstrip("/") or "/",
            normalized_query,
            "",
        )
    )
    return {
        "raw_url": raw,
        "normalized_url": normalized_url,
        "canonical_article_id": canonical_article_id,
    }


class CrewAutomationStateStore:
    """LMDB-backed state storage for the_light_house_project+777 social/growth automation."""

    def __init__(self, lmdb_dir: str, map_size: int = 256 * 1024 * 1024) -> None:
        os.makedirs(lmdb_dir, exist_ok=True)
        self.env = lmdb.open(
            lmdb_dir,
            map_size=map_size,
            max_dbs=12,
            subdir=True,
            create=True,
            lock=True,
            sync=True,
        )
        self.db_users = self.env.open_db(b"users")
        self.db_post_likers = self.env.open_db(b"post_likers")
        self.db_approvals = self.env.open_db(b"approvals")
        self.db_tasks = self.env.open_db(b"tasks")
        self.db_meta = self.env.open_db(b"meta")
        self.db_logs = self.env.open_db(b"logs")

    def _put_json(self, db: Any, key: str, value: Dict[str, Any]) -> None:
        blob = json.dumps(value, ensure_ascii=False).encode("utf-8")
        with self.env.begin(write=True) as txn:
            txn.put(key.encode("utf-8"), blob, db=db)

    def _get_json(self, db: Any, key: str) -> Optional[Dict[str, Any]]:
        with self.env.begin(write=False) as txn:
            raw = txn.get(key.encode("utf-8"), db=db)
        if not raw:
            return None
        try:
            value = json.loads(raw.decode("utf-8"))
            return value if isinstance(value, dict) else None
        except Exception:
            return None

    def _iter_json(self, db: Any, prefix: str = "") -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        with self.env.begin(write=False) as txn:
            cur = txn.cursor(db=db)
            prefix_bytes = prefix.encode("utf-8")
            if prefix:
                if not cur.set_range(prefix_bytes):
                    return rows
            for key_raw, val_raw in cur:
                if prefix and not key_raw.startswith(prefix_bytes):
                    break
                try:
                    obj = json.loads(val_raw.decode("utf-8"))
                    if isinstance(obj, dict):
                        rows.append(obj)
                except Exception:
                    continue
        return rows

    def _iter_key_values(self, db: Any, prefix: str = "") -> List[Tuple[str, Dict[str, Any]]]:
        rows: List[Tuple[str, Dict[str, Any]]] = []
        with self.env.begin(write=False) as txn:
            cur = txn.cursor(db=db)
            prefix_bytes = prefix.encode("utf-8")
            if prefix:
                if not cur.set_range(prefix_bytes):
                    return rows
            for key_raw, val_raw in cur:
                if prefix and not key_raw.startswith(prefix_bytes):
                    break
                try:
                    key = key_raw.decode("utf-8")
                    obj = json.loads(val_raw.decode("utf-8"))
                    if isinstance(obj, dict):
                        rows.append((key, obj))
                except Exception:
                    continue
        return rows

    def upsert_user_state(self, user_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        key = f"x:user:{user_id}"
        current = self._get_json(self.db_users, key) or {}
        merged = {**current, **patch}
        merged["updated_at"] = _now_iso()
        self._put_json(self.db_users, key, merged)
        return merged

    def get_user_state(self, user_id: str) -> Optional[Dict[str, Any]]:
        return self._get_json(self.db_users, f"x:user:{user_id}")

    def upsert_approval(self, user_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        key = f"tg:approval:{user_id}"
        current = self._get_json(self.db_approvals, key) or {}
        merged = {**current, **patch}
        merged.setdefault("created_at", _now_iso())
        merged["updated_at"] = _now_iso()
        self._put_json(self.db_approvals, key, merged)
        return merged

    def get_approval(self, user_id: str) -> Optional[Dict[str, Any]]:
        return self._get_json(self.db_approvals, f"tg:approval:{user_id}")

    def list_pending_approvals(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self._iter_json(self.db_approvals, "tg:approval:")
        pending = [row for row in rows if (row.get("status") or "").lower() == "pending"]
        pending.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return pending[:limit]

    def upsert_task(self, task_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        key = f"task:{task_id}"
        current = self._get_json(self.db_tasks, key) or {}
        merged = {**current, **patch}
        merged["updated_at"] = _now_iso()
        self._put_json(self.db_tasks, key, merged)
        return merged

    def set_meta(self, key: str, value: Any) -> None:
        payload = {"value": value, "updated_at": _now_iso()}
        self._put_json(self.db_meta, key, payload)

    def get_meta(self, key: str, default: Any = None) -> Any:
        row = self._get_json(self.db_meta, key)
        if not row:
            return default
        return row.get("value", default)

    def get_following_cache(self, user_id: str) -> bool:
        return bool(self.get_meta(f"meta:x:following_cache:{user_id}", False))

    def set_following_cache(self, user_id: str, following: bool) -> None:
        self.set_meta(f"meta:x:following_cache:{user_id}", bool(following))

    def _posted_link_key(self, link: str) -> str:
        normalized = normalize_source_url(link)
        norm = normalized.get("canonical_article_id") or normalized.get("normalized_url") or normalized.get("raw_url", "").lower()
        digest = hashlib.sha1(norm.encode("utf-8")).hexdigest()
        return f"x:posted_link:{digest}"

    def _legacy_posted_link_key(self, link: str) -> str:
        norm = str(link or "").strip().lower()
        digest = hashlib.sha1(norm.encode("utf-8")).hexdigest()
        return f"x:posted_link:{digest}"

    def _rejected_link_key(self, cycle_id: str, link: str) -> str:
        cycle = str(cycle_id or "").strip()
        normalized = normalize_source_url(link)
        norm = normalized.get("canonical_article_id") or normalized.get("normalized_url") or normalized.get("raw_url", "").lower()
        digest = hashlib.sha1(norm.encode("utf-8")).hexdigest()
        return f"x:rejected_link:{cycle}:{digest}"

    def _legacy_rejected_link_key(self, cycle_id: str, link: str) -> str:
        cycle = str(cycle_id or "").strip()
        norm = str(link or "").strip().lower()
        digest = hashlib.sha1(norm.encode("utf-8")).hexdigest()
        return f"x:rejected_link:{cycle}:{digest}"

    def upsert_post_liker(self, post_id: str, user_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        key = f"x:post_liker:{post_id}:{user_id}"
        current = self._get_json(self.db_post_likers, key) or {}
        merged = {**current, **payload}
        merged["source_post_id"] = post_id
        merged["user_id"] = user_id
        merged.setdefault("captured_at", _now_iso())
        merged["updated_at"] = _now_iso()
        self._put_json(self.db_post_likers, key, merged)
        return merged

    def has_post_liker(self, post_id: str, user_id: str) -> bool:
        return self._get_json(self.db_post_likers, f"x:post_liker:{post_id}:{user_id}") is not None

    def list_post_likers(self, post_id: str) -> List[Dict[str, Any]]:
        return [row for _key, row in self._iter_key_values(self.db_post_likers, f"x:post_liker:{post_id}:")]

    def set_post_monitor_state(self, post_id: str, state: Dict[str, Any]) -> None:
        self.set_meta(f"meta:x:post_monitor_state:{post_id}", state)

    def get_post_monitor_state(self, post_id: str) -> Dict[str, Any]:
        state = self.get_meta(f"meta:x:post_monitor_state:{post_id}", {})
        return state if isinstance(state, dict) else {}

    # ---------- Generic wrappers (requested schema helpers) ----------
    def save_state(self, key: str, value: Dict[str, Any], dbi: str = "meta") -> None:
        target = {
            "meta": self.db_meta,
            "users": self.db_users,
            "post_likers": self.db_post_likers,
            "tasks": self.db_tasks,
            "approvals": self.db_approvals,
            "logs": self.db_logs,
        }.get(dbi, self.db_meta)
        self._put_json(target, key, value)

    def load_state(self, key: str, default: Optional[Dict[str, Any]] = None, dbi: str = "meta") -> Optional[Dict[str, Any]]:
        target = {
            "meta": self.db_meta,
            "users": self.db_users,
            "post_likers": self.db_post_likers,
            "tasks": self.db_tasks,
            "approvals": self.db_approvals,
            "logs": self.db_logs,
        }.get(dbi, self.db_meta)
        return self._get_json(target, key) or default

    def save_posted_link(self, link: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        body = payload or {}
        normalized = normalize_source_url(link)
        row = {
            "url": str(link or "").strip(),
            "normalized_url": normalized.get("normalized_url", ""),
            "canonical_article_id": normalized.get("canonical_article_id", ""),
            "saved_at": _now_iso(),
            **body,
        }
        self._put_json(self.db_meta, self._posted_link_key(link), row)
        return row

    def is_posted_link(self, link: str) -> bool:
        return self._get_json(self.db_meta, self._posted_link_key(link)) is not None or self._get_json(
            self.db_meta, self._legacy_posted_link_key(link)
        ) is not None

    def list_posted_links(self, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self._iter_json(self.db_meta, "x:posted_link:")
        rows.sort(key=lambda row: row.get("saved_at", ""), reverse=True)
        return rows[: max(1, int(limit))]

    def save_rejected_link(self, cycle_id: str, link: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        cycle = str(cycle_id or "").strip()
        body = payload or {}
        normalized = normalize_source_url(link)
        row = {
            "cycle_id": cycle,
            "url": str(link or "").strip(),
            "normalized_url": normalized.get("normalized_url", ""),
            "canonical_article_id": normalized.get("canonical_article_id", ""),
            "saved_at": _now_iso(),
            **body,
        }
        self._put_json(self.db_meta, self._rejected_link_key(cycle, link), row)
        return row

    def is_rejected_link(self, cycle_id: str, link: str) -> bool:
        cycle = str(cycle_id or "").strip()
        return self._get_json(self.db_meta, self._rejected_link_key(cycle, link)) is not None or self._get_json(
            self.db_meta, self._legacy_rejected_link_key(cycle, link)
        ) is not None

    def save_post_liker(self, post_id: str, user_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.upsert_post_liker(post_id=post_id, user_id=user_id, payload=payload)

    def get_post_likers(self, post_id: str) -> List[Dict[str, Any]]:
        return self.list_post_likers(post_id=post_id)

    def save_current_post_meta(self, post_id: str, created_at: Optional[str] = None) -> Dict[str, Any]:
        created = created_at or _now_iso()
        self.set_meta("meta:x:current_post_id", str(post_id or ""))
        self.set_meta("meta:x:current_post_created_at", created)
        return {"post_id": str(post_id or ""), "created_at": created}

    def load_current_post_meta(self) -> Dict[str, Any]:
        return {
            "post_id": self.get_meta("meta:x:current_post_id", None),
            "created_at": self.get_meta("meta:x:current_post_created_at", None),
        }

    def save_publish_queue_item(self, draft_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        key = f"publish:queue:{draft_id}"
        current = self._get_json(self.db_tasks, key) or {}
        merged = {**current, **payload}
        merged["draft_id"] = draft_id
        merged["updated_at"] = _now_iso()
        merged.setdefault("created_at", _now_iso())
        self._put_json(self.db_tasks, key, merged)
        return merged

    def get_publish_queue_item(self, draft_id: str) -> Optional[Dict[str, Any]]:
        return self._get_json(self.db_tasks, f"publish:queue:{draft_id}")

    def list_publish_queue(self, include_published: bool = False) -> List[Dict[str, Any]]:
        rows = self._iter_json(self.db_tasks, "publish:queue:")
        if not include_published:
            rows = [
                row
                for row in rows
                if not bool(row.get("published", False))
                and str(row.get("state", "")).strip().lower() != "published"
                and str(row.get("state", "")).strip().lower() != "rejected"
                and str(row.get("approval_status", "")).strip().lower() not in {"rejected", "weekend"}
            ]
        rows.sort(key=lambda row: row.get("created_at", ""))
        return rows

    def archive_publish_queue_item(self, draft_id: str, payload: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        did = str(draft_id or "").strip()
        if not did:
            return None
        current = self.get_publish_queue_item(did) or {}
        merged = {**current, **(payload or {})}
        merged["draft_id"] = did
        merged["archived_at"] = _now_iso()
        key = f"publish:archive:{did}"
        self._put_json(self.db_tasks, key, merged)
        self.remove_publish_queue_item(did)
        return merged

    def list_archived_publish_items(self, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self._iter_json(self.db_tasks, "publish:archive:")
        rows.sort(key=lambda row: row.get("archived_at", "") or row.get("updated_at", ""), reverse=True)
        return rows[: max(1, int(limit))]

    def save_draft_queue_item(self, draft_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.save_publish_queue_item(draft_id=draft_id, payload=payload)

    def load_draft_queue(self) -> List[Dict[str, Any]]:
        return self.list_publish_queue()

    def save_content_queue_item(self, clip_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        key = f"content:queue:{clip_id}"
        current = self._get_json(self.db_tasks, key) or {}
        merged = {**current, **payload}
        merged["clip_id"] = clip_id
        merged["updated_at"] = _now_iso()
        merged.setdefault("created_at", _now_iso())
        self._put_json(self.db_tasks, key, merged)
        return merged

    def get_content_queue_item(self, clip_id: str) -> Optional[Dict[str, Any]]:
        return self._get_json(self.db_tasks, f"content:queue:{clip_id}")

    def list_content_queue(self) -> List[Dict[str, Any]]:
        rows = self._iter_json(self.db_tasks, "content:queue:")
        rows = [
            row
            for row in rows
            if str(row.get("approval_status", "")).strip().lower() not in {"approved", "rejected", "archived"}
        ]
        rows.sort(key=lambda row: row.get("created_at", ""))
        return rows

    def archive_content_queue_item(self, clip_id: str, payload: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        cid = str(clip_id or "").strip()
        if not cid:
            return None
        current = self.get_content_queue_item(cid) or {}
        merged = {**current, **(payload or {})}
        merged["clip_id"] = cid
        merged["archived_at"] = _now_iso()
        key = f"content:archive:{cid}"
        self._put_json(self.db_tasks, key, merged)
        self.remove_content_queue_item(cid)
        return merged

    def list_archived_content_items(self, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self._iter_json(self.db_tasks, "content:archive:")
        rows.sort(key=lambda row: row.get("archived_at", "") or row.get("updated_at", ""), reverse=True)
        return rows[: max(1, int(limit))]

    def remove_content_queue_item(self, clip_id: str) -> bool:
        key = f"content:queue:{clip_id}".encode("utf-8")
        with self.env.begin(write=True) as txn:
            return bool(txn.delete(key, db=self.db_tasks))

    def remove_publish_queue_item(self, draft_id: str) -> bool:
        key = f"publish:queue:{draft_id}".encode("utf-8")
        with self.env.begin(write=True) as txn:
            return bool(txn.delete(key, db=self.db_tasks))

    def append_log(self, category: str, message: str) -> Dict[str, Any]:
        ts_key = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        key = f"log:{ts_key}:{category.lower()}"
        row = {"category": category, "message": message, "created_at": _now_iso()}
        self._put_json(self.db_logs, key, row)
        return row

    # ---------- camelCase aliases for requested wrapper names ----------
    def saveState(self, key: str, value: Dict[str, Any], dbi: str = "meta") -> None:
        self.save_state(key=key, value=value, dbi=dbi)

    def loadState(self, key: str, default: Optional[Dict[str, Any]] = None, dbi: str = "meta") -> Optional[Dict[str, Any]]:
        return self.load_state(key=key, default=default, dbi=dbi)

    def savePostedLink(self, link: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.save_posted_link(link=link, payload=payload)

    def isPostedLink(self, link: str) -> bool:
        return self.is_posted_link(link=link)

    def saveRejectedLink(self, cycle_id: str, link: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.save_rejected_link(cycle_id=cycle_id, link=link, payload=payload)

    def isRejectedLink(self, cycle_id: str, link: str) -> bool:
        return self.is_rejected_link(cycle_id=cycle_id, link=link)

    def savePostLiker(self, post_id: str, user_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.save_post_liker(post_id=post_id, user_id=user_id, payload=payload)

    def getPostLikers(self, post_id: str) -> List[Dict[str, Any]]:
        return self.get_post_likers(post_id=post_id)

    def saveCurrentPostMeta(self, post_id: str, created_at: Optional[str] = None) -> Dict[str, Any]:
        return self.save_current_post_meta(post_id=post_id, created_at=created_at)

    def loadCurrentPostMeta(self) -> Dict[str, Any]:
        return self.load_current_post_meta()

    def saveDraftQueueItem(self, draft_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.save_draft_queue_item(draft_id=draft_id, payload=payload)

    def loadDraftQueue(self) -> List[Dict[str, Any]]:
        return self.load_draft_queue()
