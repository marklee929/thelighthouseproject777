import os
import yaml
from typing import Dict, Any, Tuple

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "config")

def load_configs() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Load config/app.yaml and config/permissions.yaml.
    Return empty dictionaries when the files are missing or empty.
    """
    app_cfg_path = os.path.join(CONFIG_DIR, "app.yaml")
    perm_cfg_path = os.path.join(CONFIG_DIR, "permissions.yaml")

    app_cfg = {}
    if os.path.exists(app_cfg_path):
        with open(app_cfg_path, 'r', encoding='utf-8') as f:
            app_cfg = yaml.safe_load(f) or {}

    perm_cfg = {}
    if os.path.exists(perm_cfg_path):
        with open(perm_cfg_path, 'r', encoding='utf-8') as f:
            perm_cfg = yaml.safe_load(f) or {}
            
    return app_cfg, perm_cfg

def merge_permissions(app_cfg: Dict[str, Any], perm_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge the default application permissions with the external permissions file.
    """
    perms = app_cfg.get("permissions", {}).copy()
    if isinstance(perm_cfg, dict):
        perms.update(perm_cfg)
    return perms
