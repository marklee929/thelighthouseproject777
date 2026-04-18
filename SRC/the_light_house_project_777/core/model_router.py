from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional, Tuple

import httpx


DEFAULT_MODELS = {
    "text": "qwen3:8b",
    "coder": "qwen3-coder",
    "reasoning": "deepseek-r1:8b",
}

DEFAULT_TASK_ROUTE = {
    "chat": "text",
    "create": "coder",
    "modify": "coder",
    "delete": "reasoning",
    "default": "text",
}

TASK_MODEL_ROUTE = {
    "summarize_news": "text",
    "review_news": "text",
    "generate_post": "text",
    "plain_english_transform": "text",
    "general_text": "text",
    "code_edit": "coder",
    "api_patch": "coder",
    "json_format": "coder",
    "automation_patch": "coder",
    "reasoning_review": "reasoning",
    "conflict_resolve": "reasoning",
    "tie_break": "reasoning",
    "risk_review": "reasoning",
}


def resolve_model_roles(app_cfg: Dict[str, Any]) -> Dict[str, str]:
    models_cfg = app_cfg.get("models", {}) or {}
    legacy_map = {
        "text": models_cfg.get("leader"),
        "coder": models_cfg.get("coder"),
        "reasoning": models_cfg.get("apprentice"),
    }
    resolved = dict(DEFAULT_MODELS)
    env_map = {
        "text": os.getenv("LOCAL_MODEL_GENERAL", "").strip(),
        "coder": os.getenv("LOCAL_MODEL_CODER", "").strip(),
        "reasoning": os.getenv("LOCAL_MODEL_REASONER", "").strip(),
    }
    for role in ("text", "coder", "reasoning"):
        candidate = env_map.get(role) or models_cfg.get(role) or legacy_map.get(role)
        if candidate:
            resolved[role] = str(candidate).strip()
    return resolved


def resolve_task_model(app_cfg: Dict[str, Any], mode: str) -> Tuple[str, str]:
    routing_cfg = app_cfg.get("routing", {}) or {}
    role = str(
        routing_cfg.get(mode)
        or routing_cfg.get("default")
        or DEFAULT_TASK_ROUTE.get(mode)
        or DEFAULT_TASK_ROUTE["default"]
    ).strip()
    models = resolve_model_roles(app_cfg)
    return role, models.get(role, DEFAULT_MODELS[DEFAULT_TASK_ROUTE["default"]])


def resolve_model_for_task(task_type: str, app_cfg: Optional[Dict[str, Any]] = None) -> str:
    """Resolve a concrete model name for a social/backend task."""
    role_env = {
        "text": os.getenv("LOCAL_MODEL_GENERAL", "").strip(),
        "coder": os.getenv("LOCAL_MODEL_CODER", "").strip(),
        "reasoning": os.getenv("LOCAL_MODEL_REASONER", "").strip(),
    }
    role = TASK_MODEL_ROUTE.get(str(task_type or "").strip().lower(), "text")
    if role_env.get(role):
        return role_env[role]
    models = resolve_model_roles(app_cfg or {})
    return models.get(role, DEFAULT_MODELS[role])


def run_local_model(
    task_type: str,
    prompt: str,
    *,
    app_cfg: Optional[Dict[str, Any]] = None,
    model: Optional[str] = None,
    ollama_base_url: Optional[str] = None,
    timeout: float = 45.0,
    format: str = "",
    temperature: float = 0.2,
) -> Dict[str, Any]:
    """Run a single local model inference for the given task."""
    model_name = str(model or resolve_model_for_task(task_type, app_cfg)).strip()
    base_url = str(ollama_base_url or os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")).strip().rstrip("/")
    payload: Dict[str, Any] = {
        "model": model_name,
        "stream": False,
        "prompt": str(prompt or ""),
        "options": {"temperature": temperature},
    }
    if format:
        payload["format"] = format
    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.post(f"{base_url}/api/generate", json=payload)
            response.raise_for_status()
            body = response.json()
        raw = str(body.get("response", "") or "").strip()
        parsed: Any = None
        if format == "json" and raw:
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = None
        return {
            "ok": True,
            "task_type": task_type,
            "model": model_name,
            "raw": raw,
            "parsed": parsed,
            "response": body,
        }
    except Exception as exc:
        return {
            "ok": False,
            "task_type": task_type,
            "model": model_name,
            "raw": "",
            "parsed": None,
            "error": f"{type(exc).__name__}: {exc}",
        }
