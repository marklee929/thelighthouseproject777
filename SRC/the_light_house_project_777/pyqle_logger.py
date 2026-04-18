from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, Optional

from project_meta import PROJECT_DISPLAY_NAME, PROJECT_ROOT

_LOG_DIR = PROJECT_ROOT / "logs"
_LOG_FILE = _LOG_DIR / "pyqle.log"
_EMIT_FN: Optional[Callable[[str, Dict[str, Any]], None]] = None
_DEBUG_FN: Optional[Callable[[str], None]] = None

_LOGGER = logging.getLogger("the_light_house_project_777.pyqle")
if not _LOGGER.handlers:
    _LOGGER.addHandler(logging.NullHandler())
_LOGGER.setLevel(logging.INFO)


def configure_pyqle_logger(
    *,
    emit_fn: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    debug_fn: Optional[Callable[[str], None]] = None,
) -> None:
    global _EMIT_FN, _DEBUG_FN
    _EMIT_FN = emit_fn
    _DEBUG_FN = debug_fn
    _LOG_DIR.mkdir(parents=True, exist_ok=True)


def pyqle_log(level: str, message: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    record = {
        "level": str(level or "info").strip().lower() or "info",
        "message": str(message or "").strip(),
        "project": PROJECT_DISPLAY_NAME,
    }
    if isinstance(payload, dict):
        record.update(payload)

    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        with _LOG_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass

    try:
        if _EMIT_FN is not None:
            _EMIT_FN("log", {"event": "pyqle_log", **record})
    except Exception:
        pass

    if record["level"] == "error":
        try:
            _LOGGER.error("%s %s", PROJECT_DISPLAY_NAME, record["message"])
        except Exception:
            pass
        try:
            if _DEBUG_FN is not None:
                _DEBUG_FN(f"[PYQLE-ERROR] {record['message']}")
        except Exception:
            pass

    return record
