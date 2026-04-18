import os
import yaml
from typing import Dict, Any, Tuple

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "config")

def load_configs() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    config/app.yaml과 config/permissions.yaml 파일을 로드합니다.
    파일이 없거나 비어있으면 빈 딕셔너리를 반환합니다.
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
    애플리케이션 설정의 기본 권한과 외부 권한 설정 파일의 내용을 병합합니다.
    """
    perms = app_cfg.get("permissions", {}).copy()
    if isinstance(perm_cfg, dict):
        perms.update(perm_cfg)
    return perms