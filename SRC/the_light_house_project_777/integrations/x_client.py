from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests_oauthlib import OAuth1

from project_meta import PROJECT_ROOT


class XClient:
    """X integration client using OAuth 1.0a user-context posting."""

    DEFAULT_POST_INTERVAL_SECONDS = 3600

    def __init__(self) -> None:
        self.oauth1_client_key = os.getenv("X_OAUTH1_CLIENT_KEY", "").strip()
        self.oauth1_client_secret = os.getenv("X_OAUTH1_CLIENT_SECRET", "").strip()
        self.access_token = os.getenv("X_ACCESS_TOKEN", "").strip()
        self.access_token_secret = os.getenv("X_ACCESS_TOKEN_SECRET", "").strip()
        self.callback_url = os.getenv("X_CALLBACK_URL", "").strip()
        self.account_name = os.getenv("X_ACCOUNT_NAME", "main_account").strip()
        self.api_base_url = os.getenv("X_API_BASE_URL", "https://api.x.com").strip().rstrip("/")
        self.timeout_sec = float(os.getenv("X_TIMEOUT_SEC", "15").strip() or "15")
        self.real_api_enabled = os.getenv("X_ENABLE_REAL_API", "false").lower() == "true"
        self.allow_simulated_success = os.getenv("X_ALLOW_SIMULATED_SUCCESS", "false").lower() == "true"
        self.mock_engagement_enabled = os.getenv("X_ENABLE_FAKE_METRICS", "false").lower() == "true"
        self.post_interval_seconds = int(
            os.getenv("X_POST_INTERVAL_SECONDS", str(self.DEFAULT_POST_INTERVAL_SECONDS)).strip()
            or str(self.DEFAULT_POST_INTERVAL_SECONDS)
        )

        self.project_root = PROJECT_ROOT
        guard_store_env = os.getenv("X_POST_GUARD_PATH", "").strip()
        self.post_guard_path = (
            Path(guard_store_env)
            if guard_store_env
            else (self.project_root / "config" / "x_post_guard.json")
        )
        self.post_guard_path.parent.mkdir(parents=True, exist_ok=True)
        self._following_cache: set[str] = set()
        self._posts: Dict[str, Dict[str, Any]] = {}

    @property
    def configured(self) -> bool:
        return bool(self.oauth1_configured)

    @property
    def oauth1_configured(self) -> bool:
        return bool(
            self.oauth1_client_key
            and self.oauth1_client_secret
            and self.access_token
            and self.access_token_secret
        )

    @property
    def oauth2_configured(self) -> bool:
        return False

    def _short_json(self, obj: Any, max_len: int = 480) -> str:
        text = json.dumps(obj, ensure_ascii=False) if not isinstance(obj, str) else obj
        if len(text) <= max_len:
            return text
        return f"{text[: max_len - 3]}..."

    def _mask_suffix(self, value: str, suffix_len: int = 6) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if len(text) <= suffix_len:
            return text
        return text[-suffix_len:]

    def _x_debug(self, msg: str) -> None:
        try:
            print(f"[X-OAUTH1-DEBUG] {msg}", flush=True)
        except Exception:
            pass

    def _load_json_file(self, path: Path) -> Dict[str, Any]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_json_file(self, path: Path, payload: Dict[str, Any]) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _oauth1_auth(self) -> OAuth1:
        return OAuth1(
            self.oauth1_client_key,
            client_secret=self.oauth1_client_secret,
            resource_owner_key=self.access_token,
            resource_owner_secret=self.access_token_secret,
        )

    def load_x_oauth1_credentials(self) -> Dict[str, str]:
        return {
            "client_key": self.oauth1_client_key,
            "client_secret": self.oauth1_client_secret,
            "access_token": self.access_token,
            "access_token_secret": self.access_token_secret,
        }

    def validate_x_oauth1_config(self) -> Dict[str, Any]:
        credential_presence = {
            "client_key": bool(self.oauth1_client_key),
            "client_secret": bool(self.oauth1_client_secret),
            "access_token": bool(self.access_token),
            "access_token_secret": bool(self.access_token_secret),
        }
        missing = [key for key, present in credential_presence.items() if not present]
        return {
            "ok": not bool(missing),
            "mode": "oauth1_direct",
            "credential_presence": credential_presence,
            "missing_keys": missing,
            "real_api_enabled": bool(self.real_api_enabled),
            "post_interval_seconds": int(self.post_interval_seconds),
            "callback_url": self.callback_url,
        }

    def get_oauth_setup_status(self) -> Dict[str, Any]:
        validation = self.validate_x_oauth1_config()
        return {
            "mode": "oauth1_direct",
            "oauth2_disabled": True,
            "oauth2_configured": False,
            "callback_routes_expected": [],
            "redirect_uri": self.callback_url,
            "credential_presence": validation.get("credential_presence", {}),
            "missing_keys": validation.get("missing_keys", []),
            "post_interval_seconds": int(self.post_interval_seconds),
            "note": "OAuth2 authorize/callback is disabled. Direct OAuth1 posting is the default path.",
        }

    def get_auth_status(self) -> Dict[str, Any]:
        validation = self.validate_x_oauth1_config()
        if not validation.get("ok"):
            return {
                "connected": False,
                "status": "oauth1_config_missing",
                "message": "OAuth1 credentials are missing.",
                "mode": "oauth1_direct",
                "credential_presence": validation.get("credential_presence", {}),
                "missing_keys": validation.get("missing_keys", []),
                "real_api_enabled": bool(self.real_api_enabled),
            }
        if not self.real_api_enabled:
            return {
                "connected": False,
                "status": "dry_run_only",
                "message": "OAuth1 credentials are present but X_ENABLE_REAL_API=false.",
                "mode": "oauth1_direct",
                "credential_presence": validation.get("credential_presence", {}),
                "missing_keys": [],
                "real_api_enabled": False,
            }
        return {
            "connected": True,
            "status": "oauth1_ready",
            "message": "OAuth1 direct posting is ready.",
            "mode": "oauth1_direct",
            "credential_presence": validation.get("credential_presence", {}),
            "missing_keys": [],
            "real_api_enabled": True,
        }

    def build_authorize_url(self) -> Dict[str, Any]:
        return {
            "ok": False,
            "error": "oauth2_disabled",
            "detail": "OAuth2 authorize/callback flow is disabled. Use OAuth1 direct posting.",
            "oauth_setup": self.get_oauth_setup_status(),
            "logs": [
                "[AUTH] OAuth2 authorize is disabled",
                "[AUTH] X posting now uses OAuth1 direct API calls",
            ],
        }

    def exchange_code_for_token(self, code: str, state: str) -> Dict[str, Any]:
        return {
            "ok": False,
            "error": "oauth2_disabled",
            "detail": "OAuth2 token exchange is disabled. Use OAuth1 direct posting.",
            "code_present": bool(str(code or "").strip()),
            "state_present": bool(str(state or "").strip()),
            "oauth_setup": self.get_oauth_setup_status(),
            "logs": [
                "[AUTH] OAuth2 callback/token exchange is disabled",
                "[AUTH] X posting now uses OAuth1 direct API calls",
            ],
        }

    def refresh_access_token(self, refresh_token: Optional[str] = None) -> Dict[str, Any]:
        return {
            "ok": False,
            "error": "oauth2_disabled",
            "detail": "OAuth2 refresh is disabled. OAuth1 direct mode does not refresh access tokens.",
            "refresh_token_present": bool(str(refresh_token or "").strip()),
        }

    def get_valid_access_token(self) -> Dict[str, Any]:
        return {
            "ok": False,
            "error": "oauth2_disabled",
            "detail": "OAuth2 bearer tokens are not used in direct OAuth1 mode.",
        }

    def _request_with_oauth1(
        self,
        method: str,
        endpoint: str,
        *,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        validation = self.validate_x_oauth1_config()
        if not validation.get("ok"):
            return {
                "ok": False,
                "error": "oauth1_config_missing",
                "detail": f"Missing keys: {', '.join(validation.get('missing_keys', []))}",
                "credential_presence": validation.get("credential_presence", {}),
            }

        url = f"{self.api_base_url}{endpoint}"
        headers = {"Accept": "application/json"}
        if json_body is not None:
            headers["Content-Type"] = "application/json"

        try:
            response = requests.request(
                method=method.upper(),
                url=url,
                auth=self._oauth1_auth(),
                json=json_body,
                params=params,
                headers=headers,
                timeout=self.timeout_sec,
            )
        except Exception as exc:
            return {
                "ok": False,
                "error": "x_request_failed",
                "detail": f"{type(exc).__name__}: {exc}",
                "status_code": None,
                "response_body": None,
                "url": url,
            }

        try:
            body: Any = response.json()
        except Exception:
            body = {"raw": response.text}

        return {
            "ok": int(response.status_code or 0) in (200, 201),
            "status_code": int(response.status_code or 0),
            "response_body": body,
            "url": url,
        }

    def _classify_publish_error(self, status: int, body: Any) -> str:
        text = self._short_json(body).lower()
        if "duplicate content" in text or "duplicate" in text:
            return "duplicate_content"
        if "rate limit" in text or status == 429:
            return "rate_limited"
        if "invalid or expired token" in text or "could not authenticate" in text:
            return "oauth1_auth_failed"
        if "not authorized" in text or status in (401, 403):
            return "oauth1_access_denied"
        return "x_post_http_error"

    def _load_post_guard_state(self) -> Dict[str, Any]:
        return self._load_json_file(self.post_guard_path)

    def _save_post_guard_state(self, payload: Dict[str, Any]) -> None:
        self._save_json_file(self.post_guard_path, payload)

    def _text_hash(self, text: str) -> str:
        return hashlib.sha1(str(text or "").strip().encode("utf-8")).hexdigest()

    def _check_post_guard(self, text: str) -> Dict[str, Any]:
        state = self._load_post_guard_state()
        now = time.time()
        text_hash = self._text_hash(text)
        last_posted_at = float(state.get("last_posted_at") or 0.0)
        last_text_hash = str(state.get("last_text_hash") or "").strip()
        elapsed = now - last_posted_at if last_posted_at > 0 else None

        if last_text_hash and last_text_hash == text_hash:
            return {
                "ok": False,
                "error": "duplicate_post_text",
                "detail": "Identical text matches the most recent X post.",
                "wait_seconds": max(0, int(self.post_interval_seconds - (elapsed or 0))),
            }

        if elapsed is not None and elapsed < float(self.post_interval_seconds):
            return {
                "ok": False,
                "error": "post_interval_blocked",
                "detail": f"Posting blocked for {max(0, int(self.post_interval_seconds - elapsed))} more seconds.",
                "wait_seconds": max(0, int(self.post_interval_seconds - elapsed)),
            }

        return {"ok": True, "text_hash": text_hash}

    def _record_post_guard(self, *, text: str, post_id: str) -> None:
        payload = {
            "last_posted_at": time.time(),
            "last_post_id": str(post_id or "").strip(),
            "last_text_hash": self._text_hash(text),
            "last_text_preview": str(text or "")[:180],
        }
        self._save_post_guard_state(payload)

    def dry_run_post_to_x(self, text: str) -> Dict[str, Any]:
        post_id = hashlib.sha1(f"dry-run|{text}|{time.time()}".encode("utf-8")).hexdigest()[:16]
        row = {
            "ok": True,
            "published": True,
            "post_id": post_id,
            "platform": "x",
            "account_alias": self.account_name or "main_account",
            "text": str(text or ""),
            "dry_run": True,
            "created_at": time.time(),
            "simulated": True,
        }
        self._posts[post_id] = row
        return row

    def post_text_to_x(self, text: str, reply_to_post_id: Optional[str] = None) -> Dict[str, Any]:
        text_value = str(text or "").strip()
        alias = self.account_name or "main_account"
        validation = self.validate_x_oauth1_config()
        self._x_debug(
            "[POST] credential_presence "
            f"client_key={validation['credential_presence']['client_key']} "
            f"client_secret={validation['credential_presence']['client_secret']} "
            f"access_token={validation['credential_presence']['access_token']} "
            f"access_token_secret={validation['credential_presence']['access_token_secret']}"
        )

        if not text_value:
            return {
                "ok": False,
                "published": False,
                "post_id": "",
                "platform": "x",
                "account_alias": alias,
                "text": text_value,
                "reply_to_post_id": reply_to_post_id,
                "dry_run": not bool(self.real_api_enabled),
                "error": "empty_post_text",
                "detail": "X post text is empty.",
                "created_at": time.time(),
            }

        guard = self._check_post_guard(text_value)
        if not guard.get("ok"):
            self._x_debug(
                "[POST] guard_blocked "
                f"error={guard.get('error')} detail={guard.get('detail')}"
            )
            return {
                "ok": False,
                "published": False,
                "post_id": "",
                "platform": "x",
                "account_alias": alias,
                "text": text_value,
                "reply_to_post_id": reply_to_post_id,
                "dry_run": not bool(self.real_api_enabled),
                "error": str(guard.get("error") or "post_guard_blocked"),
                "detail": str(guard.get("detail") or ""),
                "wait_seconds": int(guard.get("wait_seconds") or 0),
                "created_at": time.time(),
            }

        if not self.real_api_enabled:
            if self.allow_simulated_success:
                row = self.dry_run_post_to_x(text_value)
                self._record_post_guard(text=text_value, post_id=str(row.get("post_id", "")))
                return row
            return {
                "ok": False,
                "published": False,
                "post_id": "",
                "platform": "x",
                "account_alias": alias,
                "text": text_value,
                "reply_to_post_id": reply_to_post_id,
                "dry_run": True,
                "error": "dry_run_publish_blocked",
                "detail": "X_ENABLE_REAL_API=false. OAuth1 real publish not executed.",
                "created_at": time.time(),
            }

        if not validation.get("ok"):
            return {
                "ok": False,
                "published": False,
                "post_id": "",
                "platform": "x",
                "account_alias": alias,
                "text": text_value,
                "reply_to_post_id": reply_to_post_id,
                "dry_run": False,
                "error": "oauth1_config_missing",
                "detail": f"Missing keys: {', '.join(validation.get('missing_keys', []))}",
                "credential_presence": validation.get("credential_presence", {}),
                "created_at": time.time(),
            }

        payload: Dict[str, Any] = {"text": text_value}
        if reply_to_post_id:
            payload["reply"] = {"in_reply_to_tweet_id": str(reply_to_post_id)}

        self._x_debug(
            "[POST] request_start "
            f"url={self.api_base_url}/2/tweets text_len={len(text_value)} reply={bool(reply_to_post_id)}"
        )
        response = self._request_with_oauth1("POST", "/2/tweets", json_body=payload)
        status = int(response.get("status_code") or 0)
        body = response.get("response_body")
        self._x_debug(f"[POST] response status={status} body={self._short_json(body)}")

        if not response.get("ok"):
            if str(response.get("error") or "").strip() == "x_request_failed":
                self._x_debug(f"[POST] failed reason_guess=x_request_failed detail={response.get('detail')}")
                return {
                    "ok": False,
                    "published": False,
                    "post_id": "",
                    "platform": "x",
                    "account_alias": alias,
                    "text": text_value,
                    "reply_to_post_id": reply_to_post_id,
                    "dry_run": False,
                    "error": "x_request_failed",
                    "detail": str(response.get("detail") or ""),
                    "status_code": status,
                    "response_body": body,
                    "created_at": time.time(),
                }
            guessed = self._classify_publish_error(status, body)
            self._x_debug(f"[POST] failed reason_guess={guessed}")
            return {
                "ok": False,
                "published": False,
                "post_id": "",
                "platform": "x",
                "account_alias": alias,
                "text": text_value,
                "reply_to_post_id": reply_to_post_id,
                "dry_run": False,
                "error": guessed,
                "detail": f"status={status}, body={self._short_json(body)}",
                "status_code": status,
                "response_body": body,
                "created_at": time.time(),
            }

        data = body.get("data") if isinstance(body, dict) else {}
        post_id = str((data or {}).get("id", "")).strip()
        if not post_id:
            self._x_debug("[POST] failed reason_guess=missing_post_id")
            return {
                "ok": False,
                "published": False,
                "post_id": "",
                "platform": "x",
                "account_alias": alias,
                "text": text_value,
                "reply_to_post_id": reply_to_post_id,
                "dry_run": False,
                "error": "missing_post_id",
                "detail": f"status={status}, body={self._short_json(body)}",
                "status_code": status,
                "response_body": body,
                "created_at": time.time(),
            }

        self._record_post_guard(text=text_value, post_id=post_id)
        row = {
            "ok": True,
            "published": True,
            "post_id": post_id,
            "platform": "x",
            "account_alias": alias,
            "text": text_value,
            "reply_to_post_id": reply_to_post_id,
            "dry_run": False,
            "created_at": time.time(),
            "like_count": 0,
            "simulated": False,
            "raw": body,
        }
        self._posts[post_id] = row
        self._x_debug(f"[POST] success post_id={post_id}")
        return row

    def create_post(self, text: str, account_alias: Optional[str] = None, reply_to_post_id: Optional[str] = None) -> Dict[str, Any]:
        row = self.post_text_to_x(text=text, reply_to_post_id=reply_to_post_id)
        row["account_alias"] = str(account_alias or self.account_name or "main_account")
        return row

    def publish_thread(self, *, title: str, body: str, account_alias: str) -> Dict[str, Any]:
        text = f"{title}\n\n{body}".strip()
        return self.create_post(text=text, account_alias=account_alias)

    def get_me(self) -> Dict[str, Any]:
        if not self.real_api_enabled:
            return {
                "ok": False,
                "error": "dry_run_only",
                "detail": "Real API disabled.",
            }
        response = self._request_with_oauth1("GET", "/2/users/me")
        if not response.get("ok"):
            return {
                "ok": False,
                "error": "x_get_me_failed",
                "detail": f"status={response.get('status_code')}, body={self._short_json(response.get('response_body'))}",
            }
        return {"ok": True, "data": (response.get("response_body") or {}).get("data", {})}

    def get_post_metrics(self, post_id: str, last_like_count: int = 0, allow_synthetic: bool = False) -> Dict[str, Any]:
        pid = str(post_id or "").strip()
        if not pid:
            return {
                "ok": False,
                "exists": False,
                "post_id": "",
                "error": "post_id is required",
                "like_count": 0,
                "retweet_count": 0,
                "reply_count": 0,
                "synthetic_enabled": bool(self.mock_engagement_enabled),
                "synthetic_applied": False,
            }

        if self.real_api_enabled and self.oauth1_configured:
            response = self._request_with_oauth1(
                "GET",
                "/2/tweets",
                params={"ids": pid, "tweet.fields": "public_metrics"},
            )
            status = int(response.get("status_code") or 0)
            body = response.get("response_body")
            if status == 404:
                return {
                    "ok": False,
                    "exists": False,
                    "post_id": pid,
                    "error": "post_not_found",
                    "like_count": 0,
                    "retweet_count": 0,
                    "reply_count": 0,
                    "synthetic_enabled": bool(self.mock_engagement_enabled),
                    "synthetic_applied": False,
                    "dry_run": False,
                }
            if response.get("ok") and isinstance(body, dict):
                rows = body.get("data") or []
                row0 = rows[0] if isinstance(rows, list) and rows else {}
                metrics = row0.get("public_metrics") if isinstance(row0, dict) else {}
                like_count = int((metrics or {}).get("like_count", 0) or 0)
                retweet_count = int((metrics or {}).get("retweet_count", 0) or 0)
                reply_count = int((metrics or {}).get("reply_count", 0) or 0)
                cached = self._posts.get(pid) or {}
                cached["like_count"] = like_count
                cached["post_id"] = pid
                cached["updated_at"] = time.time()
                self._posts[pid] = cached
                return {
                    "ok": True,
                    "exists": True,
                    "post_id": pid,
                    "like_count": like_count,
                    "retweet_count": retweet_count,
                    "reply_count": reply_count,
                    "synthetic_enabled": bool(self.mock_engagement_enabled),
                    "synthetic_applied": False,
                    "dry_run": False,
                }

        post = self._posts.get(pid)
        if post is None:
            return {
                "ok": False,
                "exists": False,
                "post_id": pid,
                "error": "post_not_found",
                "like_count": 0,
                "retweet_count": 0,
                "reply_count": 0,
                "synthetic_enabled": bool(self.mock_engagement_enabled),
                "synthetic_applied": False,
                "dry_run": True,
            }

        like_count = int(post.get("like_count", 0) or 0)
        synthetic_applied = False
        if self.real_api_enabled and self.mock_engagement_enabled and allow_synthetic:
            seed = int(hashlib.sha1(pid.encode("utf-8")).hexdigest()[:6], 16)
            minute = int(time.time() // 60)
            increment = ((seed + minute) % 5) + 1
            like_count = max(int(last_like_count), like_count + int(increment))
            post["like_count"] = like_count
            self._posts[pid] = post
            synthetic_applied = True

        return {
            "ok": True,
            "exists": True,
            "post_id": pid,
            "like_count": like_count,
            "retweet_count": max(0, like_count // 7),
            "reply_count": max(0, like_count // 11),
            "synthetic_enabled": bool(self.mock_engagement_enabled),
            "synthetic_applied": bool(synthetic_applied),
            "dry_run": not bool(self.real_api_enabled and self.oauth1_configured),
        }

    def get_post_like_count(self, *, post_id: str, last_like_count: int = 0) -> int:
        return int(self.get_post_metrics(post_id=post_id, last_like_count=last_like_count).get("like_count", 0))

    def get_liking_users(self, *, post_id: str, cursor: Optional[str], limit: int = 20) -> Dict[str, Any]:
        pid = str(post_id or "").strip()
        if self.real_api_enabled and self.oauth1_configured:
            params: Dict[str, Any] = {
                "max_results": max(5, min(int(limit or 20), 100)),
                "user.fields": "public_metrics,username",
            }
            if cursor:
                params["pagination_token"] = str(cursor)
            response = self._request_with_oauth1("GET", f"/2/tweets/{pid}/liking_users", params=params)
            status = int(response.get("status_code") or 0)
            body = response.get("response_body")
            if status == 404:
                return {"users": [], "next_cursor": None, "exists": False, "dry_run": False}
            if response.get("ok") and isinstance(body, dict):
                users_in = body.get("data") or []
                meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
                rows: List[Dict[str, Any]] = []
                for row in users_in:
                    if not isinstance(row, dict):
                        continue
                    pub = row.get("public_metrics") if isinstance(row.get("public_metrics"), dict) else {}
                    rows.append(
                        {
                            "id": str(row.get("id", "")).strip(),
                            "username": str(row.get("username", "")).strip(),
                            "followers_count": int(pub.get("followers_count", 0) or 0),
                            "following_count": int(pub.get("following_count", 0) or 0),
                            "tweet_count": int(pub.get("tweet_count", 0) or 0),
                            "last_active_days": 0,
                            "is_bot_suspected": False,
                            "liked_post_id": pid,
                        }
                    )
                return {
                    "users": [row for row in rows if row.get("id")],
                    "next_cursor": meta.get("next_token"),
                    "exists": True,
                    "dry_run": False,
                }
            return {
                "users": [],
                "next_cursor": None,
                "exists": True,
                "dry_run": False,
                "error": "x_liking_users_http_error",
                "detail": f"status={status}, body={self._short_json(body)}",
            }

        if not pid or pid not in self._posts:
            return {"users": [], "next_cursor": None, "exists": False}
        seed_hex = hashlib.sha1(f"{pid}:{cursor or ''}".encode("utf-8")).hexdigest()
        rows: List[Dict[str, Any]] = []
        for idx in range(min(max(limit, 1), 80)):
            base = int(seed_hex[idx % len(seed_hex)], 16)
            user_id = f"{pid[-6:]}{idx:03d}"
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
                    "liked_post_id": pid,
                }
            )
        return {"users": rows, "next_cursor": f"cursor_{seed_hex[:10]}"}

    def is_following(self, user_id: str) -> bool:
        return str(user_id) in self._following_cache

    def get_following_cache(self, user_id: str) -> bool:
        return self.is_following(user_id)

    def follow_user(self, user_id: str) -> Dict[str, Any]:
        uid = str(user_id or "").strip()
        if uid:
            self._following_cache.add(uid)
        return {
            "ok": bool(uid),
            "user_id": uid,
            "dry_run": not (self.configured and self.real_api_enabled),
            "executed_at": time.time(),
        }
