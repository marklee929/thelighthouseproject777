from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable
import json
import uuid

from .state_machine import S
from .tools.tools_browser import web_search
from .tools.tools_files import write_text
from utils.logger import get_logger

log = get_logger(__name__)

_run_session = None


def _lazy_run_session():
    """Lazy-import run_session to avoid circular imports at module import time."""
    global _run_session
    if _run_session is None:
        try:
            from .crew_session import run_session as _rs
            _run_session = _rs
        except Exception as e:
            print(f"--- IMPORT FAILED, RAISING ERROR ---")
            raise e
    return _run_session


def _disable_crewai_file_logs():
    """Ensure CrewAI doesn't emit per-session log files."""
    try:
        from crewai.cli.logger import logger
        logger.log_to_file = False
        logger.log_json = False
        # Drop file handlers if present
        logger.handlers = [h for h in logger.handlers if getattr(h, "name", "") != "file"]
    except Exception:
        # Best-effort; do not break routing
        pass


@dataclass
class Context:
    """
    Conversation context object.
    All routes share this to track state, permissions, artifacts, logs, etc.
    """
    session_id: str
    goal: str
    user_input: str = ""
    state: str = "IDLE"
    deadlines: Dict[str, int] = field(default_factory=lambda: {"research_min": 15})
    permissions: Dict[str, Any] = field(default_factory=dict)
    workdir: str = "./data"
    artifacts: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)


__all__ = ["Context"]


def route(
    context: "Context",
    payload: Dict[str, Any],
    ws: Optional[Any] = None,
    emit: Optional[Callable[[str, Any], None]] = None
) -> "Context":
    """Route incoming payload to the proper crew/state and return updated context."""
    user_text = payload.get("question", "")
    mode = payload.get("mode", "chat")
    options = payload.get("options")
    targets = payload.get("targets")

    def _emit(event_type, data):
        if emit:
            emit(event_type, data)
        log.info(f"[EMIT] {event_type}: {data}")

    _emit('log', {
        'event': 'routing_start',
        'text': f"Routing user input: '{user_text}' (mode: {mode})"
    })
    context.user_input = user_text

    # 1) Determine state
    if mode == 'chat':
        context.state = S.QUICK.value
    elif any(k in user_text for k in ["\uAC80\uC0C9", "\uCC3E\uC544\uC918", "\uB9C1\uD06C", "\uCD5C\uC2E0", "\uB274\uC2A4"]):
        context.state = S.SEARCHING.value
    elif any(k in user_text for k in ["\uCF54\uB4DC", "\uD568\uC218", "\uB9AC\uD329\uD1A0\uB9C1", "\uAC1C\uC120", "\uBB38\uC11C", "\uD30C\uC77C", "\uC791\uC131", "\uB9CC\uB4E4", "\uC0DD\uC131"]):
        context.state = S.WORKING.value
    else:
        context.state = S.MEETING.value
    _emit('log', {'event': 'state_determined', 'text': f"State determined as: {context.state}"})

    # 2) Tool permission
    tool_allowed = (
        mode in ("create", "modify", "delete")
        and context.state in (S.WORKING.value, S.SEARCHING.value)
    )
    _emit('log', {'event': 'permission_check', 'text': f"Tool usage allowed: {tool_allowed}"})

    # History string
    history = "\n".join([f"**{entry['role']}**: {entry['content']}" for entry in context.logs])

    # 3) State-based handling
    if context.state == S.SEARCHING.value and not tool_allowed:
        _emit('log', {'event': 'tool_fallback', 'text': "Tool not allowed, falling back to web search."})
        hits = web_search(user_text, 5)
        context.artifacts["search"] = hits
        out = f"Related information found for the search query:\n{json.dumps(hits, indent=2, ensure_ascii=False)}"
        context.artifacts["final"] = out
    else:
        _emit('log', {'event': 'crew_kickoff', 'text': "Handing over to crew session..."})
        try:
            _emit('log', {'event': 'crew_call', 'text': 'Invoking run_session...'})
            _disable_crewai_file_logs()
            out = _lazy_run_session()(
                user_prompt=user_text,
                state=context.state,
                history=history,
                mode=mode,
                options=options,
                targets=targets,
                tool_allowed=tool_allowed,
                ws=ws,
                emit=emit
            )
            _emit('log', {'event': 'crew_done', 'text': f"Crew finished. Snippet: {str(out)[:80]}"})
            context.artifacts["final"] = out
        except Exception as e:
            _emit('log', {'event': 'crew_error', 'text': f"Crew run_session failed: {e}"})
            raise

    _emit('log', {'event': 'routing_end', 'text': f"Produced final artifact snippet: {str(out)[:100]}..."})

    context.logs.append({"role": "user", "content": user_text})
    context.logs.append({"role": "assistant", "content": str(out)})

    return context


class Orchestrator:
    def __init__(self):
        pass

    def run(
        self,
        question: str,
        mode: str = "chat",
        options: Optional[Dict[str, Any]] = None,
        targets: Optional[List[str]] = None
    ):
        context = Context(
            session_id=uuid.uuid4().hex[:8],
            goal=question,
        )
        payload = {"question": question, "mode": mode, "options": options, "targets": targets}
        updated_context = route(context, payload)

        final_result = updated_context.artifacts.get("final", "No result found.")

        if hasattr(final_result, 'raw') and final_result.raw:
            return final_result.raw
        return str(final_result)


__all__.extend(["route", "Orchestrator"])
