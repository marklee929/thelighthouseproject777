"""
LLM-facing helpers for PyQLE.
All model calls must stay inside crew_ai; pyqle_core must never touch LLMs directly.
"""
from __future__ import annotations

import time
import traceback
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

from pyqle_logger import pyqle_log


@dataclass
class CorrectionResult:
    text: str
    llm_used: bool
    is_fallback: bool
    model: str
    endpoint: str
    elapsed_ms: int
    is_timeout: bool = False


def correct_text(
    candidates: List[str],
    session_id: Optional[str] = None,
    model: Optional[str] = None,
    timeout: float = 30.0,
    mode: str = "generate",
) -> CorrectionResult:
    alias: Dict[str, str] = {
        "crew_ai_correction": "qwen3:8b",
        "mistral": "qwen3:8b",
        "llama3": "qwen3:8b",
        "qwen3": "qwen3:8b",
        "codellama": "qwen3-coder",
        "qwen3-coder": "qwen3-coder",
        "reasoning": "deepseek-r1:8b",
        "deepseek": "deepseek-r1:8b",
    }
    model_name = alias.get(model or "", model or "qwen3:8b")
    endpoint = "http://localhost:11434/api/chat"
    req_chars = sum(len(c or "") for c in candidates)

    pyqle_log(
        "info",
        "llm_invoke",
        {
            "stage": "llm_invoke",
            "session_id": session_id,
            "model": model_name,
            "candidates_n": len(candidates),
            "request_chars": req_chars,
            "llm_endpoint": endpoint,
        },
    )

    bullets = "\n".join(f"- {c}" for c in (candidates or [])[:8])
    prompt = (
        "아래 항목들은 '문장'이 아니라 문장을 만들기 위한 재료(토큰 묶음)이다.\n"
        "이 재료를 사용해 자연스러운 한국어 문장 1~2개를 새로 생성해라.\n"
        "불필요한 영어/기호를 줄이고, 매끄러운 한국어로 다듬어 줘.\n\n"
        f"{bullets}\n"
    )

    payload = {
        "model": model_name,
        "stream": False,
        "messages": [
            {
                "role": "system",
                "content": "너는 문장을 교정하는 어시스턴트다. 출력은 교정된 문장으로만 1~2문장으로 정리해라.",
            },
            {"role": "user", "content": prompt},
        ],
        "options": {"temperature": 0.2},
    }

    t0 = time.perf_counter()
    try:
        r = requests.post(endpoint, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        text = (data.get("message") or {}).get("content", "") or ""
        text = " ".join(text.split()).strip()
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        pyqle_log(
            "info",
            "llm_response",
            {
                "stage": "llm_response",
                "session_id": session_id,
                "model": model_name,
                "elapsed_ms": elapsed_ms,
                "response_chars": len(text),
                "candidates_n": len(candidates),
                "llm_endpoint": endpoint,
                "llm_used": True,
                "is_fallback": False,
                "is_timeout": False,
            },
        )
        return CorrectionResult(
            text=text if text else (candidates[0] if candidates else ""),
            llm_used=True,
            is_fallback=False if text else True,
            model=model_name,
            endpoint=endpoint,
            elapsed_ms=elapsed_ms,
            is_timeout=False,
        )
    except requests.Timeout as exc:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        tb = "\n".join(traceback.format_exc().splitlines()[:8])
        pyqle_log(
            "error",
            "llm_timeout",
            {
                "stage": "llm_timeout",
                "session_id": session_id,
                "model": model_name,
                "err": str(exc),
                "traceback_head": tb,
                "where": "crew_ai_llm",
                "elapsed_ms": elapsed_ms,
                "candidates_n": len(candidates),
                "llm_endpoint": endpoint,
                "llm_used": False,
                "is_fallback": True,
                "is_timeout": True,
            },
        )
        fallback = (candidates[0] if candidates else "").strip()

        return CorrectionResult(
            text=text or fallback,
            llm_used=bool(text),
            is_fallback=not bool(text),
            model=model_name,
            endpoint=endpoint,
            elapsed_ms=elapsed_ms,
            is_timeout=True,
        )
    except Exception as exc:  # pragma: no cover - defensive
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        tb = "\n".join(traceback.format_exc().splitlines()[:8])
        pyqle_log(
            "error",
            "llm_error",
            {
                "stage": "error",
                "session_id": session_id,
                "model": model_name,
                "err": str(exc),
                "traceback_head": tb,
                "where": "crew_ai_llm",
                "elapsed_ms": elapsed_ms,
                "candidates_n": len(candidates),
                "llm_endpoint": endpoint,
                "llm_used": False,
                "is_fallback": True,
                "is_timeout": False,
            },
        )
        fallback = (candidates[0] if candidates else "").strip()
        return CorrectionResult(
            text=fallback,
            llm_used=False,
            is_fallback=True,
            model=model_name,
            endpoint=endpoint,
            elapsed_ms=elapsed_ms,
            is_timeout=False,
        )
