from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from .facebook_publisher import FacebookPublisher


class PlatformConnector:
    DEFAULT_PLATFORM = "facebook"
    PLATFORM_PROFILES = {
        "facebook": {"format": "post", "tone": "practical", "length": "medium", "experimental": False},
        "x": {"format": "thread", "tone": "hook", "length": "short", "experimental": True},
    }

    def __init__(self, project_root: str) -> None:
        self.project_root = Path(project_root)
        self.config_dir = self.project_root / "config"
        self.accounts_path = self.config_dir / "accounts.json"
        self.facebook_publisher = FacebookPublisher(str(self.project_root))
        self._ensure_accounts_config()

    def _ensure_accounts_config(self) -> None:
        self.config_dir.mkdir(parents=True, exist_ok=True)
        if self.accounts_path.exists():
            return
        payload = {
            "default_social": {
                "facebook_page": "main_page",
                "x_account": "experimental",
            }
        }
        self.accounts_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def load_accounts_config(self) -> Dict[str, Any]:
        try:
            data = json.loads(self.accounts_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {"default_social": {"facebook_page": "main_page", "x_account": "experimental"}}

    def get_platform_status(self, platform: str = "facebook") -> Dict[str, Any]:
        key = str(platform or self.DEFAULT_PLATFORM).strip().lower()
        if key == "facebook":
            return self.facebook_publisher.refresh_facebook_platform_status()

        return {
            "connected": False,
            "status": "EXPERIMENTAL",
            "message": "X is available only as an experimental fallback.",
            "mode": "experimental",
            "target_page": "",
            "experimental": True,
        }
