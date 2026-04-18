from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FacebookPublisher:
    STATUS_UNKNOWN = "UNKNOWN"
    STATUS_READY = "READY"
    STATUS_CONFIG_MISSING = "CONFIG_MISSING"
    STATUS_TOKEN_EXPIRED = "TOKEN_EXPIRED"
    STATUS_PERMISSION_INVALID = "PERMISSION_INVALID"
    STATUS_PAGE_UNREACHABLE = "PAGE_UNREACHABLE"
    STATUS_DRY_RUN_ONLY = "DRY_RUN_ONLY"

    TOKEN_UNKNOWN = "UNKNOWN"
    TOKEN_VALID = "VALID"
    TOKEN_EXPIRED = "EXPIRED"
    TOKEN_REISSUE_REQUIRED = "REISSUE_REQUIRED"

    REQUIRED_PERMISSIONS = ("pages_manage_posts", "pages_read_engagement")

    def __init__(self, project_root: str) -> None:
        self.project_root = Path(project_root)
        self.config_dir = self.project_root / "config"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir = self.project_root / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = self.project_root / "data" / "published_logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_config_path = self.data_dir / "facebook_runtime_config.json"
        self.state_path = self.data_dir / "facebook_state.json"
        self.app_id = ""
        self.app_secret = ""
        self.user_long_lived_access_token = ""
        self.page_id = ""
        self.page_access_token = ""
        self.page_name = ""
        self.token_status = self.TOKEN_UNKNOWN
        self.token_status_detail = ""
        self.platform_status = self.STATUS_UNKNOWN
        self.platform_status_detail = ""
        self.token_source = "env"
        self.graph_base_url = os.getenv("FACEBOOK_GRAPH_API_BASE_URL", "https://graph.facebook.com/v25.0").strip().rstrip("/")
        self.real_api = os.getenv("FACEBOOK_ENABLE_REAL_API", "false").strip().lower() == "true"
        self.timeout = float(os.getenv("FACEBOOK_TIMEOUT_SEC", "20").strip() or "20")
        self.interval_seconds = int(os.getenv("SOCIAL_POST_INTERVAL_SECONDS", "3600").strip() or "3600")
        self.guard_path = self.logs_dir / "facebook_publish_guard.json"
        self._bootstrap_token_config_from_env()
        self._apply_runtime_token_config()

    def _default_runtime_config(self) -> Dict[str, Any]:
        return {
            "app_id": "",
            "app_secret": "",
            "page_id": "",
            "user_long_lived_access_token": "",
            "updated_at": "",
        }

    def _normalize_runtime_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        row = self._default_runtime_config()
        row.update(payload or {})
        row["app_id"] = str(row.get("app_id", "")).strip()
        row["app_secret"] = str(row.get("app_secret", "")).strip()
        row["page_id"] = str(row.get("page_id", "")).strip()
        row["user_long_lived_access_token"] = str(row.get("user_long_lived_access_token", "")).strip()
        return row

    def _load_runtime_config(self) -> Dict[str, Any]:
        try:
            data = json.loads(self.runtime_config_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return self._normalize_runtime_config(data)
        except Exception:
            pass
        return self._default_runtime_config()

    def _save_runtime_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        row = self._normalize_runtime_config(payload)
        row["updated_at"] = _now_iso()
        self.runtime_config_path.write_text(json.dumps(row, ensure_ascii=False, indent=2), encoding="utf-8")
        return row

    def get_runtime_config(self) -> Dict[str, Any]:
        row = self._load_runtime_config()
        return {
            "app_id": str(row.get("app_id", "")).strip(),
            "app_secret": str(row.get("app_secret", "")).strip(),
            "page_id": str(row.get("page_id", "")).strip(),
            "user_long_lived_access_token": str(row.get("user_long_lived_access_token", "")).strip(),
        }

    def save_runtime_config(
        self,
        *,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
        page_id: Optional[str] = None,
        user_long_lived_access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        current = self._load_runtime_config()
        if app_id is not None:
            current["app_id"] = str(app_id or "").strip()
        if app_secret is not None:
            current["app_secret"] = str(app_secret or "").strip()
        if page_id is not None:
            current["page_id"] = str(page_id or "").strip()
        if user_long_lived_access_token is not None:
            current["user_long_lived_access_token"] = str(user_long_lived_access_token or "").strip()
        saved = self._save_runtime_config(current)
        self._apply_runtime_token_config()
        return {
            "ok": True,
            "config": {
                "app_id": str(saved.get("app_id", "")).strip(),
                "app_secret": str(saved.get("app_secret", "")).strip(),
                "page_id": str(saved.get("page_id", "")).strip(),
                "user_long_lived_access_token": str(saved.get("user_long_lived_access_token", "")).strip(),
            },
        }

    def _default_token_config(self) -> Dict[str, Any]:
        return {
            "page_access_token": "",
            "page_access_token_fetched_at": "",
            "page_name": "",
            "token_status": self.TOKEN_UNKNOWN,
            "token_status_detail": "",
            "platform_status": self.STATUS_UNKNOWN,
            "platform_status_detail": "",
            "user_token_last_debug": {},
            "page_token_last_debug": {},
            "token_error_code": None,
            "token_error_subcode": None,
            "granted_permissions": [],
            "last_error_code": None,
            "last_error_message": "",
            "last_validated_at": "",
            "updated_at": "",
        }

    def _normalize_token_status(self, value: Any) -> str:
        key = str(value or "").strip().lower()
        mapping = {
            "": self.TOKEN_UNKNOWN,
            "unknown": self.TOKEN_UNKNOWN,
            "valid": self.TOKEN_VALID,
            "expired": self.TOKEN_EXPIRED,
            "reissue_required": self.TOKEN_REISSUE_REQUIRED,
        }
        return mapping.get(key, str(value or self.TOKEN_UNKNOWN).strip().upper() or self.TOKEN_UNKNOWN)

    def _normalize_platform_status(self, value: Any) -> str:
        key = str(value or "").strip().lower()
        mapping = {
            "": self.STATUS_UNKNOWN,
            "unknown": self.STATUS_UNKNOWN,
            "ready": self.STATUS_READY,
            "facebook_ready": self.STATUS_READY,
            "config_missing": self.STATUS_CONFIG_MISSING,
            "facebook_config_missing": self.STATUS_CONFIG_MISSING,
            "token_expired": self.STATUS_TOKEN_EXPIRED,
            "facebook_token_expired": self.STATUS_TOKEN_EXPIRED,
            "permission_invalid": self.STATUS_PERMISSION_INVALID,
            "page_unreachable": self.STATUS_PAGE_UNREACHABLE,
            "facebook_page_unreachable": self.STATUS_PAGE_UNREACHABLE,
            "facebook_reissue_required": self.STATUS_TOKEN_EXPIRED,
            "dry_run_only": self.STATUS_DRY_RUN_ONLY,
        }
        return mapping.get(key, str(value or self.STATUS_UNKNOWN).strip().upper() or self.STATUS_UNKNOWN)

    def _normalize_token_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        row = self._default_token_config()
        row.update(payload or {})
        row["token_status"] = self._normalize_token_status(row.get("token_status"))
        row["platform_status"] = self._normalize_platform_status(row.get("platform_status"))
        granted_permissions = row.get("granted_permissions") if isinstance(row.get("granted_permissions"), list) else []
        row["granted_permissions"] = [str(item).strip() for item in granted_permissions if str(item).strip()]
        row["page_access_token"] = str(row.get("page_access_token", "")).strip()
        row["page_access_token_fetched_at"] = str(row.get("page_access_token_fetched_at", "")).strip()
        row["page_name"] = str(row.get("page_name", "")).strip()
        row["token_status_detail"] = str(row.get("token_status_detail", "")).strip()
        row["platform_status_detail"] = str(row.get("platform_status_detail", "")).strip()
        row["last_error_message"] = str(row.get("last_error_message", "") or row.get("last_error", "")).strip()
        row["user_token_last_debug"] = row.get("user_token_last_debug") if isinstance(row.get("user_token_last_debug"), dict) else {}
        row["page_token_last_debug"] = row.get("page_token_last_debug") if isinstance(row.get("page_token_last_debug"), dict) else {}
        return row

    def _load_token_config(self) -> Dict[str, Any]:
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return self._normalize_token_config(data)
        except Exception:
            pass
        return self._default_token_config()

    def _save_token_config(self, payload: Dict[str, Any]) -> None:
        row = self._normalize_token_config(payload)
        row["updated_at"] = _now_iso()
        self.state_path.write_text(json.dumps(row, ensure_ascii=False, indent=2), encoding="utf-8")

    def _read_dotenv_values(self) -> Dict[str, str]:
        env_path = self.project_root / ".env"
        values: Dict[str, str] = {}
        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = str(raw_line or "").strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
        except Exception:
            return {}
        return values

    def _env_values(self) -> Dict[str, str]:
        dotenv_values = self._read_dotenv_values()
        runtime_config = self._load_runtime_config()

        def _pick(name: str) -> str:
            return os.getenv(name, "").strip() or str(dotenv_values.get(name, "")).strip()

        user_long_lived_token = str(runtime_config.get("user_long_lived_access_token", "")).strip()
        if not user_long_lived_token:
            user_long_lived_token = _pick("FACEBOOK_USER_LONG_LIVED_TOKEN")
        if not user_long_lived_token:
            user_long_lived_token = _pick("FACEBOOK_USER_ACCESS_TOKEN")
        legacy_user_token = _pick("FACEBOOK_PAGE_ACCESS_TOKEN")
        if not user_long_lived_token and legacy_user_token:
            user_long_lived_token = legacy_user_token
        return {
            "app_id": str(runtime_config.get("app_id", "")).strip() or _pick("FACEBOOK_APP_ID"),
            "app_secret": str(runtime_config.get("app_secret", "")).strip() or _pick("FACEBOOK_APP_SECRET"),
            "user_long_lived_access_token": user_long_lived_token,
            "page_id": str(runtime_config.get("page_id", "")).strip() or _pick("FACEBOOK_PAGE_ID"),
        }

    def _bootstrap_token_config_from_env(self) -> None:
        if not self.state_path.exists():
            self._save_token_config(self._default_token_config())

    def _resolve_runtime_token_config(self) -> Tuple[Dict[str, Any], str]:
        state = self._load_token_config()
        env_values = self._env_values()
        merged = self._default_token_config()
        merged.update(state)
        merged["app_id"] = env_values["app_id"]
        merged["app_secret"] = env_values["app_secret"]
        merged["page_id"] = env_values["page_id"]
        merged["page_access_token"] = str(state.get("page_access_token", "")).strip()
        merged["user_long_lived_access_token"] = env_values["user_long_lived_access_token"]
        merged["user_access_token"] = merged["user_long_lived_access_token"]
        token_source = "state" if merged["page_access_token"] else "derived"
        return self._normalize_token_config(merged), token_source

    def _apply_runtime_token_config(self) -> Dict[str, Any]:
        merged, token_source = self._resolve_runtime_token_config()
        self.app_id = str(merged.get("app_id", "")).strip()
        self.app_secret = str(merged.get("app_secret", "")).strip()
        self.user_long_lived_access_token = str(merged.get("user_long_lived_access_token", "")).strip()
        self.page_id = str(merged.get("page_id", "")).strip()
        self.page_access_token = str(merged.get("page_access_token", "")).strip()
        self.page_name = str(merged.get("page_name", "")).strip()
        self.token_status = self._normalize_token_status(merged.get("token_status"))
        self.token_status_detail = str(merged.get("token_status_detail", "")).strip()
        self.platform_status = self._normalize_platform_status(merged.get("platform_status"))
        self.platform_status_detail = str(merged.get("platform_status_detail", "")).strip()
        self.token_source = token_source
        return merged

    def _update_token_state(
        self,
        *,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
        page_id: Optional[str] = None,
        page_access_token: Optional[str] = None,
        user_long_lived_access_token: Optional[str] = None,
        token_status: Optional[str] = None,
        token_detail: Optional[str] = None,
        platform_status: Optional[str] = None,
        platform_detail: Optional[str] = None,
        error_code: Optional[int] = None,
        error_subcode: Optional[int] = None,
        granted_permissions: Optional[List[str]] = None,
        page_name: Optional[str] = None,
        validated: bool = False,
        user_token_last_debug: Optional[Dict[str, Any]] = None,
        page_token_last_debug: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        current = self._load_token_config()
        if page_access_token is not None:
            current["page_access_token"] = str(page_access_token or "").strip()
            current["page_access_token_fetched_at"] = _now_iso() if str(page_access_token or "").strip() else ""
        if user_long_lived_access_token is not None:
            pass
        if token_status is not None:
            current["token_status"] = self._normalize_token_status(token_status)
        if token_detail is not None:
            current["token_status_detail"] = str(token_detail or "").strip()
        if platform_status is not None:
            current["platform_status"] = self._normalize_platform_status(platform_status)
        if platform_detail is not None:
            current["platform_status_detail"] = str(platform_detail or "").strip()
        if granted_permissions is not None:
            current["granted_permissions"] = [str(item).strip() for item in granted_permissions if str(item).strip()]
        if page_name is not None:
            current["page_name"] = str(page_name or "").strip()
        current["token_error_code"] = error_code
        current["token_error_subcode"] = error_subcode
        current["last_error_code"] = error_code
        current["last_error_message"] = str(platform_detail or token_detail or "").strip()
        if user_token_last_debug is not None:
            current["user_token_last_debug"] = user_token_last_debug if isinstance(user_token_last_debug, dict) else {}
        if page_token_last_debug is not None:
            current["page_token_last_debug"] = page_token_last_debug if isinstance(page_token_last_debug, dict) else {}
        if validated:
            current["last_validated_at"] = _now_iso()
        self._save_token_config(current)
        self._apply_runtime_token_config()
        return self.get_token_status()

    def _credential_presence(self) -> Dict[str, bool]:
        self._apply_runtime_token_config()
        return {
            "app_id": bool(self.app_id),
            "app_secret": bool(self.app_secret),
            "user_long_lived_access_token": bool(self.user_long_lived_access_token),
            "user_access_token": bool(self.user_long_lived_access_token),
            "page_id": bool(self.page_id),
            "page_access_token": bool(self.page_access_token),
            "real_api": bool(self.real_api),
        }

    def get_token_status(self) -> Dict[str, Any]:
        self._apply_runtime_token_config()
        return {
            "token_status": self.token_status,
            "token_status_detail": self.token_status_detail,
            "platform_status": self.platform_status,
            "platform_status_detail": self.platform_status_detail,
            "token_source": self.token_source,
            "token_store_path": str(self.state_path),
            "user_long_lived_token_present": bool(self.user_long_lived_access_token),
            "user_access_token_present": bool(self.user_long_lived_access_token),
            "page_access_token_present": bool(self.page_access_token),
            "page_name": self.page_name,
        }

    def _status_message(self, status: str) -> str:
        mapping = {
            self.STATUS_READY: "Facebook Page publishing is ready.",
            self.STATUS_CONFIG_MISSING: "Facebook Page configuration is required.",
            self.STATUS_TOKEN_EXPIRED: "Facebook Page token expired. Manual reissue required.",
            self.STATUS_PERMISSION_INVALID: "Facebook Page permissions are invalid. Manual reissue required.",
            self.STATUS_PAGE_UNREACHABLE: "Target Facebook Page is not reachable with the derived page token.",
            self.STATUS_DRY_RUN_ONLY: "Facebook runtime publish is configured but FACEBOOK_ENABLE_REAL_API=false.",
            self.STATUS_UNKNOWN: "Facebook Page status is unknown.",
        }
        return mapping.get(status, "Facebook Page status is unknown.")

    def _status_guidance(self, status: str, *, missing_values: Optional[List[str]] = None, missing_permissions: Optional[List[str]] = None) -> str:
        if status == self.STATUS_CONFIG_MISSING:
            missing_text = ", ".join(missing_values or []) or "FACEBOOK_APP_ID, FACEBOOK_APP_SECRET, FACEBOOK_PAGE_ID, FACEBOOK_USER_ACCESS_TOKEN"
            return f"Set the missing runtime values: {missing_text}."
        if status == self.STATUS_TOKEN_EXPIRED:
            return "Run admin/manual reissue flow with a fresh user token. Runtime page token will be re-derived from the stored user token."
        if status == self.STATUS_PERMISSION_INVALID:
            missing_text = ", ".join(missing_permissions or self.REQUIRED_PERMISSIONS)
            return f"Reissue token with required scopes and verify page admin role. Missing required permissions: {missing_text}."
        if status == self.STATUS_PAGE_UNREACHABLE:
            return "Verify target page ID and confirm the app can access that page from /me/accounts."
        if status == self.STATUS_DRY_RUN_ONLY:
            return "Enable FACEBOOK_ENABLE_REAL_API=true after runtime configuration is validated."
        return "Facebook runtime publish uses a derived page token only."

    def validate_facebook_runtime_config(self) -> Dict[str, Any]:
        self._apply_runtime_token_config()
        presence = self._credential_presence()
        missing = []
        if not self.app_id:
            missing.append("FACEBOOK_APP_ID")
        if not self.app_secret:
            missing.append("FACEBOOK_APP_SECRET")
        if not self.page_id:
            missing.append("FACEBOOK_PAGE_ID")
        if not self.user_long_lived_access_token:
            missing.append("FACEBOOK_USER_ACCESS_TOKEN")
        if missing:
            return {
                "ok": False,
                "status": self.STATUS_CONFIG_MISSING,
                "error": "facebook_config_missing",
                "message": self._status_message(self.STATUS_CONFIG_MISSING),
                "detail": f"Missing values: {', '.join(missing)}",
                "missing_values": missing,
                "credential_presence": presence,
                **self.get_token_status(),
            }
        if not self.real_api:
            return {
                "ok": True,
                "status": self.STATUS_DRY_RUN_ONLY,
                "message": self._status_message(self.STATUS_DRY_RUN_ONLY),
                "detail": "FACEBOOK_ENABLE_REAL_API=false. Runtime publish remains in dry-run mode.",
                "credential_presence": presence,
                **self.get_token_status(),
            }
        return {
            "ok": True,
            "status": self.STATUS_READY,
            "message": self._status_message(self.STATUS_READY),
            "detail": "Facebook runtime publish uses a derived page token only.",
            "credential_presence": presence,
            **self.get_token_status(),
        }

    def debug_facebook_token(
        self,
        input_token: str,
        *,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
    ) -> Dict[str, Any]:
        resolved_input_token = str(input_token or "").strip()
        resolved_app_id = str(app_id or self.app_id).strip()
        resolved_app_secret = str(app_secret or self.app_secret).strip()
        missing = []
        if not resolved_app_id:
            missing.append("FACEBOOK_APP_ID")
        if not resolved_app_secret:
            missing.append("FACEBOOK_APP_SECRET")
        if not resolved_input_token:
            missing.append("FACEBOOK_USER_ACCESS_TOKEN")
        if missing:
            return {
                "ok": False,
                "error": "facebook_config_missing",
                "detail": f"Missing values: {', '.join(missing)}",
                "missing_values": missing,
            }
        try:
            response = requests.get(
                f"{self.graph_base_url}/debug_token",
                params={
                    "input_token": resolved_input_token,
                    "access_token": f"{resolved_app_id}|{resolved_app_secret}",
                },
                timeout=self.timeout,
            )
            try:
                parsed: Any = response.json()
            except Exception:
                parsed = {"raw_text": response.text}
        except requests.RequestException as exc:
            return {"ok": False, "error": "facebook_debug_token_failed", "detail": f"{type(exc).__name__}: {exc}"}

        if int(response.status_code or 0) not in (200, 201):
            return {
                "ok": False,
                "error": "facebook_debug_token_failed",
                "status_code": int(response.status_code or 0),
                "response_body": parsed,
                "detail": str(((parsed or {}).get("error") or {}).get("message") or "Facebook token debug failed."),
            }

        data = parsed.get("data") if isinstance(parsed, dict) and isinstance(parsed.get("data"), dict) else {}
        scopes = data.get("scopes") if isinstance(data.get("scopes"), list) else []
        normalized_scopes = sorted({str(item).strip() for item in scopes if str(item).strip()})
        is_valid = bool(data.get("is_valid"))
        error_info = data.get("error") if isinstance(data.get("error"), dict) else {}
        message = str(error_info.get("message") or "").strip()
        code_raw = error_info.get("code")
        try:
            error_code = int(code_raw) if code_raw is not None else None
        except Exception:
            error_code = None
        return {
            "ok": is_valid,
            "is_valid": is_valid,
            "scopes": normalized_scopes,
            "response_body": parsed,
            "detail": message,
            "facebook_error_code": error_code,
            "data": data,
        }

    def validate_facebook_user_token(
        self,
        user_access_token: Optional[str] = None,
        *,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._apply_runtime_token_config()
        token = str(user_access_token or self.user_long_lived_access_token).strip()
        if not token:
            return {
                "ok": False,
                "error": "facebook_config_missing",
                "platform_status": self.STATUS_CONFIG_MISSING,
                "token_status": self.TOKEN_REISSUE_REQUIRED,
                "detail": "Missing values: FACEBOOK_USER_ACCESS_TOKEN",
                "missing_values": ["FACEBOOK_USER_ACCESS_TOKEN"],
                "missing_permissions": [],
                "granted_permissions": [],
                "debug_data": {},
            }

        debug_result = self.debug_facebook_token(token, app_id=app_id, app_secret=app_secret)
        debug_data = debug_result.get("data") if isinstance(debug_result.get("data"), dict) else {}
        granted_permissions = list(debug_result.get("scopes") or [])
        if not debug_result.get("ok"):
            detail = str(debug_result.get("detail", "")).strip() or "Facebook user access token is invalid."
            return {
                "ok": False,
                "error": "facebook_token_expired",
                "platform_status": self.STATUS_TOKEN_EXPIRED,
                "token_status": self.TOKEN_REISSUE_REQUIRED,
                "detail": detail,
                "missing_permissions": [],
                "granted_permissions": granted_permissions,
                "facebook_error_code": debug_result.get("facebook_error_code"),
                "debug_data": debug_data,
            }

        missing_permissions = [item for item in self.REQUIRED_PERMISSIONS if item not in granted_permissions]
        if missing_permissions:
            return {
                "ok": False,
                "error": "facebook_permission_missing",
                "platform_status": self.STATUS_PERMISSION_INVALID,
                "token_status": self.TOKEN_REISSUE_REQUIRED,
                "detail": f"Missing required permissions: {', '.join(missing_permissions)}",
                "missing_permissions": missing_permissions,
                "granted_permissions": granted_permissions,
                "debug_data": debug_data,
            }

        return {
            "ok": True,
            "platform_status": self.STATUS_READY,
            "token_status": self.TOKEN_VALID,
            "detail": "Facebook user access token validated by /debug_token.",
            "missing_permissions": [],
            "granted_permissions": granted_permissions,
            "debug_data": debug_data,
        }

    def _extract_graph_error(self, parsed: Any) -> Tuple[Optional[int], Optional[int], str]:
        if not isinstance(parsed, dict):
            return None, None, ""
        error = parsed.get("error") if isinstance(parsed.get("error"), dict) else {}
        code_raw = error.get("code")
        subcode_raw = error.get("error_subcode")
        try:
            code = int(code_raw) if code_raw is not None else None
        except Exception:
            code = None
        try:
            subcode = int(subcode_raw) if subcode_raw is not None else None
        except Exception:
            subcode = None
        message = str(error.get("message", "")).strip()
        return code, subcode, message

    def _page_validation_success(self, page_id: str, page_name: str, parsed: Any) -> Dict[str, Any]:
        resolved_page_name = page_name or str((parsed or {}).get("name", "")).strip()
        return {
            "ok": True,
            "platform_status": self.STATUS_READY,
            "token_status": self.TOKEN_VALID,
            "detail": "Facebook page token validated for runtime publish.",
            "page_id": page_id,
            "page_name": resolved_page_name,
            "response_body": parsed,
            "missing_permissions": [],
            "page_token_last_debug": parsed if isinstance(parsed, dict) else {},
        }

    def validate_facebook_page_token(
        self,
        page_id: Optional[str] = None,
        page_access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._apply_runtime_token_config()
        resolved_page_id = str(page_id or self.page_id).strip()
        resolved_page_token = str(page_access_token or self.page_access_token).strip()
        if not resolved_page_id or not resolved_page_token:
            missing = []
            if not resolved_page_id:
                missing.append("FACEBOOK_PAGE_ID")
            if not resolved_page_token:
                missing.append("RUNTIME_PAGE_ACCESS_TOKEN")
            return {
                "ok": False,
                "error": "facebook_page_token_missing",
                "platform_status": self.STATUS_CONFIG_MISSING,
                "token_status": self.TOKEN_REISSUE_REQUIRED,
                "detail": f"Missing values: {', '.join(missing)}",
                "missing_values": missing,
                "missing_permissions": [],
            }
        try:
            response = requests.get(
                f"{self.graph_base_url}/{resolved_page_id}",
                params={"fields": "id,name", "access_token": resolved_page_token},
                timeout=self.timeout,
            )
            try:
                parsed: Any = response.json()
            except Exception:
                parsed = {"raw_text": response.text}
        except requests.RequestException as exc:
            return {
                "ok": False,
                "error": "facebook_page_validation_failed",
                "platform_status": self.STATUS_PAGE_UNREACHABLE,
                "token_status": self.TOKEN_UNKNOWN,
                "detail": f"{type(exc).__name__}: {exc}",
                "missing_permissions": [],
            }

        ok = int(response.status_code or 0) in (200, 201)
        if ok:
            returned_page_id = str((parsed or {}).get("id", "")).strip()
            if returned_page_id and returned_page_id != resolved_page_id:
                return {
                    "ok": False,
                    "error": "facebook_page_not_found",
                    "platform_status": self.STATUS_PAGE_UNREACHABLE,
                    "token_status": self.TOKEN_REISSUE_REQUIRED,
                    "detail": "Returned page ID does not match target Facebook Page ID.",
                    "response_body": parsed,
                    "missing_permissions": [],
                }
            return self._page_validation_success(resolved_page_id, str((parsed or {}).get("name", "")).strip(), parsed)

        error_code, error_subcode, error_message = self._extract_graph_error(parsed)
        if error_code == 190:
            detail = error_message or "Facebook page token expired or invalid."
            return {
                "ok": False,
                "error": "facebook_token_expired",
                "platform_status": self.STATUS_TOKEN_EXPIRED,
                "token_status": self.TOKEN_EXPIRED,
                "detail": detail,
                "response_body": parsed,
                "facebook_error_code": error_code,
                "facebook_error_subcode": error_subcode,
                "missing_permissions": [],
            }
        if error_code == 200:
            missing_permissions = list(self.REQUIRED_PERMISSIONS)
            detail = error_message or "Missing required Facebook Page permissions."
            return {
                "ok": False,
                "error": "facebook_permission_missing",
                "platform_status": self.STATUS_PERMISSION_INVALID,
                "token_status": self.TOKEN_REISSUE_REQUIRED,
                "detail": detail,
                "response_body": parsed,
                "facebook_error_code": error_code,
                "facebook_error_subcode": error_subcode,
                "missing_permissions": missing_permissions,
            }
        if error_code in {10, 100, 803}:
            detail = error_message or "Facebook Page is not reachable with the derived page token."
            return {
                "ok": False,
                "error": "facebook_page_not_found",
                "platform_status": self.STATUS_PAGE_UNREACHABLE,
                "token_status": self.TOKEN_REISSUE_REQUIRED,
                "detail": detail,
                "response_body": parsed,
                "facebook_error_code": error_code,
                "facebook_error_subcode": error_subcode,
                "missing_permissions": [],
            }
        return {
            "ok": False,
            "error": "facebook_page_validation_failed",
            "platform_status": self.STATUS_PAGE_UNREACHABLE,
            "token_status": self.TOKEN_UNKNOWN,
            "detail": error_message or "Facebook page token validation failed.",
            "response_body": parsed,
            "facebook_error_code": error_code,
            "facebook_error_subcode": error_subcode,
            "missing_permissions": [],
        }

    def _try_refresh_page_token_from_user_token(
        self,
        *,
        reason: str,
        user_validation: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._apply_runtime_token_config()
        if not self.page_id or not self.user_long_lived_access_token:
            return {"ok": False, "attempted": False, "detail": ""}

        validation = user_validation or self.validate_facebook_user_token(self.user_long_lived_access_token)
        if not validation.get("ok"):
            detail = str(validation.get("detail", "")).strip() or "Stored user token is not valid for Facebook Page publishing."
            return {
                "ok": False,
                "attempted": True,
                "detail": detail,
                "validation": validation,
            }

        page_token_result = self.fetch_page_access_token_from_user_token(self.page_id, self.user_long_lived_access_token)
        if page_token_result.get("ok"):
            fresh_page_token = str(page_token_result.get("access_token", "")).strip()
            page_validation = self.validate_facebook_page_token(self.page_id, fresh_page_token)
            if page_validation.get("ok"):
                detail = f"Facebook page token refreshed from stored user token ({reason})."
                self._update_token_state(
                    page_id=self.page_id,
                    page_access_token=fresh_page_token,
                    user_long_lived_access_token=self.user_long_lived_access_token,
                    token_status=self.TOKEN_VALID,
                    token_detail=detail,
                    platform_status=self.STATUS_READY,
                    platform_detail=detail,
                    granted_permissions=list(validation.get("granted_permissions") or []),
                    page_name=str(page_validation.get("page_name", "")).strip() or str(page_token_result.get("page_name", "")).strip(),
                    user_token_last_debug=validation.get("debug_data"),
                    page_token_last_debug=page_validation.get("page_token_last_debug"),
                    validated=True,
                )
                return {
                    "ok": True,
                    "attempted": True,
                    "detail": detail,
                    "validation": page_validation,
                }
            return {
                "ok": False,
                "attempted": True,
                "detail": str(page_validation.get("detail", "")).strip() or "Generated page token validation failed.",
                "validation": page_validation,
            }

        detail = str(page_token_result.get("detail", "")).strip() or "Facebook page token refresh from stored user token failed."
        return {
            "ok": False,
            "attempted": True,
            "detail": detail,
            "validation": page_token_result,
        }

    def _platform_status_payload(
        self,
        *,
        status: str,
        connected: bool,
        message: str,
        detail: str,
        missing_values: Optional[List[str]] = None,
        missing_permissions: Optional[List[str]] = None,
        credential_presence: Optional[Dict[str, bool]] = None,
    ) -> Dict[str, Any]:
        token_state = self.get_token_status()
        return {
            "connected": connected,
            "status": status,
            "message": message,
            "detail": detail,
            "mode": "facebook_page_runtime",
            "target_page": self.page_id or "main_page",
            "target_type": "page",
            "configured_page_id": self.page_id,
            "resolved_page_id": self.page_id if token_state.get("page_access_token_present") else "",
            "runtime_auth_mode": "page_token_only",
            "browser_oauth_disabled": True,
            "admin_reissue_required": status in {self.STATUS_TOKEN_EXPIRED, self.STATUS_PERMISSION_INVALID},
            "experimental": False,
            "token_status": token_state.get("token_status"),
            "token_detail": token_state.get("token_status_detail"),
            "token_source": token_state.get("token_source"),
            "token_store_path": token_state.get("token_store_path"),
            "page_name": token_state.get("page_name"),
            "user_long_lived_token_present": token_state.get("user_long_lived_token_present"),
            "user_access_token_present": token_state.get("user_access_token_present"),
            "page_access_token_present": token_state.get("page_access_token_present"),
            "missing_values": missing_values or [],
            "missing_permissions": missing_permissions or [],
            "guidance": self._status_guidance(status, missing_values=missing_values, missing_permissions=missing_permissions),
            "credential_presence": credential_presence or self._credential_presence(),
        }

    def refresh_facebook_platform_status(self) -> Dict[str, Any]:
        self._apply_runtime_token_config()
        runtime_validation = self.validate_facebook_runtime_config()
        presence = runtime_validation.get("credential_presence", self._credential_presence())
        if runtime_validation.get("status") == self.STATUS_CONFIG_MISSING:
            detail = str(runtime_validation.get("detail", "")).strip() or self._status_message(self.STATUS_CONFIG_MISSING)
            missing_values = list(runtime_validation.get("missing_values") or [])
            self._update_token_state(
                token_status=self.TOKEN_REISSUE_REQUIRED,
                token_detail=detail,
                platform_status=self.STATUS_CONFIG_MISSING,
                platform_detail=detail,
                validated=False,
            )
            return self._platform_status_payload(
                status=self.STATUS_CONFIG_MISSING,
                connected=False,
                message=self._status_message(self.STATUS_CONFIG_MISSING),
                detail=detail,
                missing_values=missing_values,
                credential_presence=presence,
            )
        if runtime_validation.get("status") == self.STATUS_DRY_RUN_ONLY:
            detail = str(runtime_validation.get("detail", "")).strip() or self._status_message(self.STATUS_DRY_RUN_ONLY)
            token_status = self.token_status if self.token_status != self.TOKEN_UNKNOWN else self.TOKEN_VALID
            self._update_token_state(
                token_status=token_status,
                token_detail=self.token_status_detail or "Stored page token available. Real API disabled.",
                platform_status=self.STATUS_DRY_RUN_ONLY,
                platform_detail=detail,
                validated=False,
            )
            return self._platform_status_payload(
                status=self.STATUS_DRY_RUN_ONLY,
                connected=False,
                message=self._status_message(self.STATUS_DRY_RUN_ONLY),
                detail=detail,
                credential_presence=presence,
            )

        user_validation = self.validate_facebook_user_token()
        if not user_validation.get("ok"):
            status = str(user_validation.get("platform_status", self.STATUS_TOKEN_EXPIRED)).strip() or self.STATUS_TOKEN_EXPIRED
            detail = str(user_validation.get("detail", "")).strip() or self._status_message(status)
            self._update_token_state(
                token_status=str(user_validation.get("token_status", self.TOKEN_REISSUE_REQUIRED)).strip() or self.TOKEN_REISSUE_REQUIRED,
                token_detail=detail,
                platform_status=status,
                platform_detail=detail,
                error_code=user_validation.get("facebook_error_code"),
                granted_permissions=list(user_validation.get("granted_permissions") or []),
                user_token_last_debug=user_validation.get("debug_data"),
                validated=True,
            )
            return self._platform_status_payload(
                status=status,
                connected=False,
                message=self._status_message(status),
                detail=detail,
                missing_values=list(user_validation.get("missing_values") or []),
                missing_permissions=list(user_validation.get("missing_permissions") or []),
                credential_presence=presence,
            )

        if self.page_access_token:
            validation = self.validate_facebook_page_token()
            if validation.get("ok"):
                detail = str(validation.get("detail", "")).strip() or self._status_message(self.STATUS_READY)
                self._update_token_state(
                    token_status=self.TOKEN_VALID,
                    token_detail=detail,
                    platform_status=self.STATUS_READY,
                    platform_detail=detail,
                    granted_permissions=list(user_validation.get("granted_permissions") or []),
                    page_name=str(validation.get("page_name", "")).strip(),
                    user_token_last_debug=user_validation.get("debug_data"),
                    page_token_last_debug=validation.get("page_token_last_debug"),
                    validated=True,
                )
                return self._platform_status_payload(
                    status=self.STATUS_READY,
                    connected=True,
                    message=self._status_message(self.STATUS_READY),
                    detail=detail,
                    credential_presence=presence,
                )
            refresh_reason = str(validation.get("error", "page_token_invalid")).strip() or "page_token_invalid"
        else:
            validation = {
                "error": "facebook_page_token_missing",
                "platform_status": self.STATUS_TOKEN_EXPIRED,
                "token_status": self.TOKEN_REISSUE_REQUIRED,
                "detail": "Derived Facebook Page token is not cached yet.",
                "missing_permissions": [],
            }
            refresh_reason = "missing_page_token"

        if validation.get("error") in {
            "facebook_page_token_missing",
            "facebook_token_expired",
            "facebook_permission_missing",
            "facebook_page_not_found",
        }:
            token_refresh = self._try_refresh_page_token_from_user_token(
                reason=refresh_reason,
                user_validation=user_validation,
            )
            if token_refresh.get("ok"):
                promoted_detail = str(token_refresh.get("detail", "")).strip() or self._status_message(self.STATUS_READY)
                return self._platform_status_payload(
                    status=self.STATUS_READY,
                    connected=True,
                    message=self._status_message(self.STATUS_READY),
                    detail=promoted_detail,
                    credential_presence=presence,
                )
            if token_refresh.get("attempted"):
                fallback_validation = token_refresh.get("validation") if isinstance(token_refresh.get("validation"), dict) else {}
                fallback_error = str(fallback_validation.get("error", "")).strip()
                if fallback_error == "facebook_token_expired":
                    validation = dict(validation)
                    validation["detail"] = "Stored page token is invalid and the available user token could not derive a valid replacement. Manual reissue required."
                else:
                    validation = dict(validation)
                    current_detail = str(validation.get("detail", "")).strip()
                    fallback_detail = str(token_refresh.get("detail", "")).strip()
                    if fallback_detail:
                        validation["detail"] = (
                            f"{current_detail} | User-token page-token refresh failed: {fallback_detail}"
                            if current_detail
                            else fallback_detail
                        )
                    if fallback_validation.get("platform_status"):
                        validation["platform_status"] = fallback_validation.get("platform_status")
                    if fallback_validation.get("token_status"):
                        validation["token_status"] = fallback_validation.get("token_status")
                    if fallback_validation.get("missing_permissions"):
                        validation["missing_permissions"] = list(fallback_validation.get("missing_permissions") or [])

        status = str(validation.get("platform_status", self.STATUS_PAGE_UNREACHABLE)).strip() or self.STATUS_PAGE_UNREACHABLE
        detail = str(validation.get("detail", "")).strip() or self._status_message(status)
        self._update_token_state(
            token_status=str(validation.get("token_status", self.TOKEN_UNKNOWN)).strip() or self.TOKEN_UNKNOWN,
            token_detail=detail,
            platform_status=status,
            platform_detail=detail,
            error_code=validation.get("facebook_error_code"),
            error_subcode=validation.get("facebook_error_subcode"),
            granted_permissions=list(user_validation.get("granted_permissions") or []),
            user_token_last_debug=user_validation.get("debug_data"),
            page_token_last_debug=validation.get("page_token_last_debug"),
            validated=True,
        )
        return self._platform_status_payload(
            status=status,
            connected=False,
            message=self._status_message(status),
            detail=detail,
            missing_values=list(validation.get("missing_values") or []),
            missing_permissions=list(validation.get("missing_permissions") or []),
            credential_presence=presence,
        )

    def exchange_for_long_lived_user_token(
        self,
        user_short_lived_token: Optional[str] = None,
        *,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._apply_runtime_token_config()
        resolved_user_token = str(user_short_lived_token or "").strip()
        resolved_app_id = str(app_id or self.app_id).strip()
        resolved_app_secret = str(app_secret or self.app_secret).strip()
        missing = []
        if not resolved_app_id:
            missing.append("FACEBOOK_APP_ID")
        if not resolved_app_secret:
            missing.append("FACEBOOK_APP_SECRET")
        if not resolved_user_token:
            missing.append("FACEBOOK_USER_SHORT_LIVED_TOKEN")
        if missing:
            return {
                "ok": False,
                "error": "facebook_reissue_config_missing",
                "detail": f"Missing values: {', '.join(missing)}",
                "missing_values": missing,
            }
        try:
            response = requests.get(
                f"{self.graph_base_url}/oauth/access_token",
                params={
                    "grant_type": "fb_exchange_token",
                    "client_id": resolved_app_id,
                    "client_secret": resolved_app_secret,
                    "fb_exchange_token": resolved_user_token,
                },
                timeout=self.timeout,
            )
            try:
                parsed: Any = response.json()
            except Exception:
                parsed = {"raw_text": response.text}
        except requests.RequestException as exc:
            return {"ok": False, "error": "facebook_long_lived_exchange_failed", "detail": f"{type(exc).__name__}: {exc}"}

        if int(response.status_code or 0) not in (200, 201):
            return {
                "ok": False,
                "error": "facebook_long_lived_exchange_failed",
                "status_code": int(response.status_code or 0),
                "response_body": parsed,
                "detail": str(((parsed or {}).get("error") or {}).get("message") or "Long-lived user token exchange failed."),
            }
        access_token = str(parsed.get("access_token", "")).strip()
        return {
            "ok": True,
            "status_code": int(response.status_code or 0),
            "response_body": parsed,
            "access_token": access_token,
            "expires_in": parsed.get("expires_in"),
        }

    def fetch_user_permissions(self, user_access_token: str) -> Dict[str, Any]:
        token = str(user_access_token or "").strip()
        if not token:
            return {
                "ok": False,
                "error": "facebook_permission_missing",
                "detail": "FACEBOOK_USER_ACCESS_TOKEN is required to validate permissions.",
                "missing_permissions": list(self.REQUIRED_PERMISSIONS),
            }
        try:
            response = requests.get(
                f"{self.graph_base_url}/me/permissions",
                params={"access_token": token},
                timeout=self.timeout,
            )
            try:
                parsed: Any = response.json()
            except Exception:
                parsed = {"raw_text": response.text}
        except requests.RequestException as exc:
            return {"ok": False, "error": "facebook_permissions_fetch_failed", "detail": f"{type(exc).__name__}: {exc}"}

        if int(response.status_code or 0) not in (200, 201):
            return {
                "ok": False,
                "error": "facebook_permissions_fetch_failed",
                "status_code": int(response.status_code or 0),
                "response_body": parsed,
                "detail": str(((parsed or {}).get("error") or {}).get("message") or "Facebook permission fetch failed."),
            }

        rows = parsed.get("data") if isinstance(parsed, dict) and isinstance(parsed.get("data"), list) else []
        granted = sorted(
            {
                str(item.get("permission", "")).strip()
                for item in rows
                if isinstance(item, dict) and str(item.get("status", "")).strip().lower() == "granted"
            }
        )
        missing = [item for item in self.REQUIRED_PERMISSIONS if item not in granted]
        return {
            "ok": not missing,
            "granted_permissions": granted,
            "missing_permissions": missing,
            "response_body": parsed,
            "detail": "" if not missing else f"Missing required permissions: {', '.join(missing)}",
            "error": "facebook_permission_missing" if missing else "",
        }

    def fetch_page_access_token_from_user_token(self, target_page_id: str, user_access_token: str) -> Dict[str, Any]:
        resolved_page_id = str(target_page_id or "").strip()
        token = str(user_access_token or "").strip()
        missing = []
        if not resolved_page_id:
            missing.append("FACEBOOK_PAGE_ID")
        if not token:
            missing.append("FACEBOOK_USER_ACCESS_TOKEN")
        if missing:
            return {
                "ok": False,
                "error": "facebook_page_token_missing",
                "detail": f"Missing values: {', '.join(missing)}",
                "missing_values": missing,
            }
        try:
            response = requests.get(
                f"{self.graph_base_url}/me/accounts",
                params={"fields": "id,name,access_token,tasks", "access_token": token},
                timeout=self.timeout,
            )
            try:
                parsed: Any = response.json()
            except Exception:
                parsed = {"raw_text": response.text}
        except requests.RequestException as exc:
            return {"ok": False, "error": "facebook_page_token_fetch_failed", "detail": f"{type(exc).__name__}: {exc}"}

        if int(response.status_code or 0) not in (200, 201):
            return {
                "ok": False,
                "error": "facebook_page_token_fetch_failed",
                "status_code": int(response.status_code or 0),
                "response_body": parsed,
                "detail": str(((parsed or {}).get("error") or {}).get("message") or "Facebook page list fetch failed."),
            }

        pages = parsed.get("data") if isinstance(parsed, dict) and isinstance(parsed.get("data"), list) else []
        matched_page = None
        for item in pages:
            if not isinstance(item, dict):
                continue
            if str(item.get("id", "")).strip() == resolved_page_id:
                matched_page = item
                break
        if not matched_page:
            return {
                "ok": False,
                "error": "facebook_page_not_found",
                "detail": "Target Facebook Page ID was not found in /me/accounts.",
                "response_body": parsed,
            }
        page_access_token = str(matched_page.get("access_token", "")).strip()
        if not page_access_token:
            return {
                "ok": False,
                "error": "facebook_page_token_missing",
                "detail": "Matched Facebook Page does not include a page access token.",
                "response_body": matched_page,
            }
        return {
            "ok": True,
            "access_token": page_access_token,
            "page": matched_page,
            "tasks": matched_page.get("tasks") if isinstance(matched_page.get("tasks"), list) else [],
            "page_name": str(matched_page.get("name", "")).strip(),
            "response_body": parsed,
        }

    def reissue_facebook_page_token(
        self,
        *,
        app_id: str,
        app_secret: str,
        user_short_lived_token: str,
        target_page_id: str,
    ) -> Dict[str, Any]:
        exchange_result = self.exchange_for_long_lived_user_token(
            user_short_lived_token,
            app_id=app_id,
            app_secret=app_secret,
        )
        if not exchange_result.get("ok"):
            return exchange_result

        long_lived_user_token = str(exchange_result.get("access_token", "")).strip()
        user_validation = self.validate_facebook_user_token(
            long_lived_user_token,
            app_id=app_id,
            app_secret=app_secret,
        )
        if not user_validation.get("ok"):
            return {
                "ok": False,
                "error": str(user_validation.get("error", "facebook_permission_missing")).strip() or "facebook_permission_missing",
                "detail": str(user_validation.get("detail", "")).strip() or "Missing required Facebook permissions.",
                "missing_permissions": list(user_validation.get("missing_permissions") or []),
                "granted_permissions": list(user_validation.get("granted_permissions") or []),
                "exchange_result": exchange_result,
                "permissions_result": user_validation,
            }

        page_token_result = self.fetch_page_access_token_from_user_token(target_page_id, long_lived_user_token)
        if not page_token_result.get("ok"):
            return {
                "ok": False,
                "error": str(page_token_result.get("error", "facebook_page_token_fetch_failed")).strip(),
                "detail": str(page_token_result.get("detail", "")).strip() or "Facebook page token fetch failed.",
                "exchange_result": exchange_result,
                "permissions_result": user_validation,
                "page_token_result": page_token_result,
            }

        page_access_token = str(page_token_result.get("access_token", "")).strip()
        page_validation = self.validate_facebook_page_token(target_page_id, page_access_token)
        if not page_validation.get("ok"):
            return {
                "ok": False,
                "error": str(page_validation.get("error", "facebook_page_validation_failed")).strip(),
                "detail": str(page_validation.get("detail", "")).strip() or "Facebook page token validation failed.",
                "missing_permissions": list(page_validation.get("missing_permissions") or []),
                "exchange_result": exchange_result,
                "permissions_result": user_validation,
                "page_token_result": page_token_result,
                "page_validation": page_validation,
            }

        self.save_runtime_config(
            app_id=app_id,
            app_secret=app_secret,
            page_id=target_page_id,
            user_long_lived_access_token=long_lived_user_token,
        )
        self._update_token_state(
            app_id=app_id,
            app_secret=app_secret,
            page_id=target_page_id,
            page_access_token=page_access_token,
            user_long_lived_access_token=long_lived_user_token,
            token_status=self.TOKEN_VALID,
            token_detail="Facebook page token validated for runtime publish.",
            platform_status=self.STATUS_READY,
            platform_detail="Facebook Page publishing is ready.",
            granted_permissions=list(user_validation.get("granted_permissions") or []),
            page_name=str(page_token_result.get("page_name", "")).strip(),
            user_token_last_debug=user_validation.get("debug_data"),
            page_token_last_debug=page_validation.get("page_token_last_debug"),
            validated=True,
        )
        platform_status = self.refresh_facebook_platform_status()
        return {
            "ok": True,
            "status": self.STATUS_READY,
            "detail": "Facebook page token reissued and stored successfully.",
            "exchange_result": exchange_result,
            "permissions_result": user_validation,
            "page_token_result": page_token_result,
            "page_validation": page_validation,
            "platform_status": platform_status,
            "stored_page_id": target_page_id,
            "stored_page_name": str(page_token_result.get("page_name", "")).strip(),
        }

    def _load_guard(self) -> Dict[str, Any]:
        try:
            return json.loads(self.guard_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_guard(self, payload: Dict[str, Any]) -> None:
        self.guard_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _publish_once(self, body: str, outbound_link: str) -> Dict[str, Any]:
        payload = {"message": body, "access_token": self.page_access_token}
        if outbound_link:
            payload["link"] = outbound_link
        try:
            response = requests.post(
                f"{self.graph_base_url}/{self.page_id}/feed",
                data=payload,
                timeout=self.timeout,
            )
            try:
                parsed: Any = response.json()
            except Exception:
                parsed = {"raw_text": response.text}
        except requests.RequestException as exc:
            return {
                "request_failed": True,
                "error": "facebook_request_failed",
                "detail": f"{type(exc).__name__}: {exc}",
            }
        error_code, error_subcode, error_message = self._extract_graph_error(parsed)
        return {
            "request_failed": False,
            "ok": int(response.status_code or 0) in (200, 201),
            "status_code": int(response.status_code or 0),
            "response_body": parsed,
            "response_text": response.text,
            "post_id": str(parsed.get("id", "") or ""),
            "facebook_error_code": error_code,
            "facebook_error_subcode": error_subcode,
            "facebook_error_message": error_message,
        }

    def publish_facebook(self, message: str, link: Optional[str] = None) -> Dict[str, Any]:
        self._apply_runtime_token_config()
        body = str(message or "").strip()
        outbound_link = str(link or "").strip()
        if not body:
            return {"ok": False, "error": "empty_text", "detail": "message is required"}

        runtime_validation = self.validate_facebook_runtime_config()
        presence = runtime_validation.get("credential_presence", self._credential_presence())
        if runtime_validation.get("status") == self.STATUS_CONFIG_MISSING:
            detail = str(runtime_validation.get("detail", "")).strip() or "Facebook Page configuration is required."
            self._update_token_state(
                token_status=self.TOKEN_REISSUE_REQUIRED,
                token_detail=detail,
                platform_status=self.STATUS_CONFIG_MISSING,
                platform_detail=detail,
                validated=False,
            )
            return {
                "ok": False,
                "published": False,
                "dry_run": False,
                "error": "facebook_config_missing",
                "target_type": "page",
                "configured_page_id": self.page_id,
                "resolved_page_id": "",
                "detail": detail,
                "credential_presence": presence,
                "missing_values": list(runtime_validation.get("missing_values") or []),
                **self.get_token_status(),
            }

        platform_status = self.refresh_facebook_platform_status()
        if platform_status.get("status") == self.STATUS_DRY_RUN_ONLY:
            result = {
                "ok": True,
                "published": False,
                "dry_run": True,
                "post_id": "",
                "target_type": "page",
                "configured_page_id": self.page_id,
                "resolved_page_id": self.page_id if self.page_access_token else "",
                "detail": str(platform_status.get("detail", "")).strip() or self._status_message(self.STATUS_DRY_RUN_ONLY),
                "credential_presence": presence,
                "guidance": platform_status.get("guidance"),
                "missing_values": list(platform_status.get("missing_values") or []),
                "missing_permissions": list(platform_status.get("missing_permissions") or []),
                **self.get_token_status(),
            }
            self._write_log(result, body, outbound_link)
            return result

        if not platform_status.get("connected"):
            status = str(platform_status.get("status", self.STATUS_UNKNOWN)).strip() or self.STATUS_UNKNOWN
            error_map = {
                self.STATUS_CONFIG_MISSING: "facebook_config_missing",
                self.STATUS_TOKEN_EXPIRED: "facebook_token_expired",
                self.STATUS_PERMISSION_INVALID: "facebook_permission_missing",
                self.STATUS_PAGE_UNREACHABLE: "facebook_page_not_found",
            }
            result = {
                "ok": False,
                "published": False,
                "dry_run": False,
                "error": error_map.get(status, "facebook_publish_blocked"),
                "target_type": "page",
                "configured_page_id": self.page_id,
                "resolved_page_id": self.page_id if self.page_access_token else "",
                "detail": str(platform_status.get("detail", "")).strip() or self._status_message(status),
                "credential_presence": presence,
                "guidance": platform_status.get("guidance"),
                "missing_values": list(platform_status.get("missing_values") or []),
                "missing_permissions": list(platform_status.get("missing_permissions") or []),
                **self.get_token_status(),
            }
            self._write_log(result, body, outbound_link)
            return result

        guard = self._load_guard()
        now_ts = datetime.now(timezone.utc).timestamp()
        if guard.get("last_posted_at_epoch") and now_ts - float(guard.get("last_posted_at_epoch", 0)) < self.interval_seconds:
            return {
                "ok": False,
                "error": "post_interval_blocked",
                "detail": f"Posting limited to every {self.interval_seconds} seconds.",
                "credential_presence": presence,
            }
        text_hash = hashlib.sha1(body.encode("utf-8")).hexdigest()
        if guard.get("last_text_hash") == text_hash:
            return {
                "ok": False,
                "error": "duplicate_post_text",
                "detail": "The same text was already posted last time.",
                "credential_presence": presence,
            }

        publish_attempt = self._publish_once(body, outbound_link)
        if publish_attempt.get("request_failed"):
            result = {
                "ok": False,
                "published": False,
                "dry_run": False,
                "error": str(publish_attempt.get("error", "facebook_request_failed")).strip() or "facebook_request_failed",
                "target_type": "page",
                "configured_page_id": self.page_id,
                "resolved_page_id": self.page_id if self.page_access_token else "",
                "detail": str(publish_attempt.get("detail", "")).strip(),
                "credential_presence": presence,
                **self.get_token_status(),
            }
            self._write_log(result, body, outbound_link)
            return result

        ok = bool(publish_attempt.get("ok"))
        post_id = str(publish_attempt.get("post_id", "") or "")
        error_code = publish_attempt.get("facebook_error_code")
        error_subcode = publish_attempt.get("facebook_error_subcode")
        error_message = str(publish_attempt.get("facebook_error_message", "")).strip()
        response_status_code = int(publish_attempt.get("status_code") or 0)
        response_body = publish_attempt.get("response_body")
        response_text = str(publish_attempt.get("response_text", "") or "")

        if ok:
            current_token_status = self._update_token_state(
                token_status=self.TOKEN_VALID,
                token_detail="Facebook page token validated by successful publish.",
                platform_status=self.STATUS_READY,
                platform_detail="Facebook Page publishing is ready.",
                validated=True,
            )
        elif error_code == 190:
            detail = error_message or "Facebook Page token expired. Manual reissue required."
            current_token_status = self._update_token_state(
                token_status=self.TOKEN_EXPIRED,
                token_detail=detail,
                platform_status=self.STATUS_TOKEN_EXPIRED,
                platform_detail=detail,
                error_code=error_code,
                error_subcode=error_subcode,
                validated=True,
            )
        elif error_code == 200:
            detail = error_message or "Missing required Facebook Page permissions."
            current_token_status = self._update_token_state(
                token_status=self.TOKEN_REISSUE_REQUIRED,
                token_detail=detail,
                platform_status=self.STATUS_PERMISSION_INVALID,
                platform_detail=detail,
                error_code=error_code,
                error_subcode=error_subcode,
                validated=True,
            )
        elif error_code in {10, 100, 803}:
            detail = error_message or "Target Facebook Page is not reachable with the derived page token."
            current_token_status = self._update_token_state(
                token_status=self.TOKEN_REISSUE_REQUIRED,
                token_detail=detail,
                platform_status=self.STATUS_PAGE_UNREACHABLE,
                platform_detail=detail,
                error_code=error_code,
                error_subcode=error_subcode,
                validated=True,
            )
        else:
            current_token_status = self.get_token_status()

        error_name = ""
        missing_permissions: List[str] = []
        guidance = ""
        if not ok:
            if error_code == 190:
                error_name = "facebook_token_expired"
                guidance = self._status_guidance(self.STATUS_TOKEN_EXPIRED)
            elif error_code == 200:
                error_name = "facebook_permission_missing"
                missing_permissions = list(self.REQUIRED_PERMISSIONS)
                guidance = self._status_guidance(self.STATUS_PERMISSION_INVALID, missing_permissions=missing_permissions)
            elif error_code in {10, 100, 803}:
                error_name = "facebook_page_not_found"
                guidance = self._status_guidance(self.STATUS_PAGE_UNREACHABLE)
            else:
                error_name = "facebook_publish_failed"

        result = {
            "ok": ok,
            "published": ok,
            "dry_run": False,
            "target_type": "page",
            "configured_page_id": self.page_id,
            "resolved_page_id": self.page_id if self.page_access_token else "",
            "status_code": response_status_code,
            "response_body": response_body,
            "response_text": response_text,
            "post_id": post_id,
            "credential_presence": presence,
            "error": error_name,
            "detail": error_message,
            "guidance": guidance,
            "missing_permissions": missing_permissions,
            "facebook_error_code": error_code,
            "facebook_error_subcode": error_subcode,
            **current_token_status,
        }
        self._write_log(result, body, outbound_link)
        if ok:
            self._save_guard(
                {
                    "last_posted_at": _now_iso(),
                    "last_posted_at_epoch": now_ts,
                    "last_text_hash": text_hash,
                    "last_post_id": post_id,
                }
            )
        return result

    def publish_text(self, text: str, link: str = "") -> Dict[str, Any]:
        return self.publish_facebook(message=text, link=link)

    def _write_log(self, result: Dict[str, Any], text: str, link: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        path = self.logs_dir / f"facebook_publish_{stamp}.jsonl"
        row = {"created_at": _now_iso(), "text": text, "link": link, "result": result}
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
