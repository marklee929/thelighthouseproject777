# -*- coding: utf-8 -*-
import os, json, uuid
import appdirs
from datetime import datetime
from typing import Any, Dict, Optional, Type, List, Callable

# Force appdirs (used by crewai) to avoid Windows profile folders that may not exist.
_APPDATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".appdata"))
os.makedirs(_APPDATA_DIR, exist_ok=True)
def _project_win_folder(_const: str) -> str:
    return _APPDATA_DIR
appdirs._get_win_folder = _project_win_folder

from crewai import Agent, Task, Crew, Process, LLM
try:
    from langchain_core.callbacks.base import BaseCallbackHandler
except ImportError:
    from langchain.callbacks.base import BaseCallbackHandler
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from utils.config_loader import load_configs
from .model_router import resolve_task_model
from .tools.tools_files import write_text, read_file

# Log directory
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# --- Tools Definition ---
class WriteFileInput(BaseModel):
    file_path: str = Field(..., description="The path to the file to write.")
    content: str = Field(..., description="The content to write to the file.")

class FileWriteTool(BaseTool):
    name: str = "Write File"
    description: str = "지정된 경로에 텍스트 파일을 생성하거나 덮어씁니다. 상위 폴더가 없으면 자동으로 생성합니다."
    args_schema: Type[BaseModel] = WriteFileInput

    def _run(self, file_path: str, content: str) -> str:
        if not file_path:
            return "ERROR: file_path is a required argument."
        return write_text(file_path=file_path, content=content or "")

class ReadFileInput(BaseModel):
    file_path: str = Field(..., description="The path of the file to read.")

class FileReadTool(BaseTool):
    name: str = "Read File"
    description: str = "지정된 경로의 파일 내용을 읽어옵니다."
    args_schema: Type[BaseModel] = ReadFileInput

    def _run(self, file_path: str) -> str:
        if not file_path:
            return "ERROR: file_path is a required argument."
        return read_file(file_path)

# --- Logger ---
class JsonlLogger(BaseCallbackHandler):
    def __init__(self, session_id: str, emit: Optional[Callable[[str, Any], None]] = None):
        self.sid = session_id
        self.emit = emit
        self.j = os.path.join(LOG_DIR, f"{self.sid}.jsonl")
        self.t = os.path.join(LOG_DIR, f"{self.sid}.txt")

    def _emit_log(self, log_data: Dict[str, Any]):
        if self.emit:
            self.emit('log', log_data)

    def _j(self, rec: Dict[str, Any]):
        rec["t"] = datetime.utcnow().isoformat()
        self._emit_log(rec)

    def _t(self, line: str):
        self._emit_log({"event": "text_log", "text": line})

    def on_llm_start(self, serialized, prompts, **kw):
        m = serialized.get("name") or serialized.get("id") or "unknown"
        for p in prompts:
            self._j({"event": "llm_start", "model": m, "prompt": p})
            self._t(f"[LLM_START] {m}\n{p}\n")

    def on_llm_end(self, resp, **kw):
        try:
            text = resp.generations[0][0].text
        except Exception:
            text = str(resp)
        self._j({"event": "llm_end", "output": text})
        self._t(f"[LLM_END]\n{text}\n{'-'*60}")

    def on_agent_action(self, action, **kw):
        self._j({"event": "agent_action", "agent": kw.get('name'), "action": str(action)})
        self._t(f"[AGENT_ACTION] Agent: {kw.get('name')}, Action: {action}")

    def on_tool_start(self, serialized, **kw):
        self._j({"event": "tool_start", "tool": serialized.get('name'), "input": kw.get('input_str')})
        self._t(f"[TOOL_START] Tool: {serialized.get('name')}, Input: {kw.get('input_str')}")

    def on_tool_end(self, output, **kw):
        self._j({"event": "tool_end", "tool": kw.get('name'), "output": output})
        self._t(f"[TOOL_END] Tool: {kw.get('name')}, Output: {output}")

def run_session(
    user_prompt: str,
    state: str,
    history: str = "",
    mode: str = "chat",
    options: Optional[Dict[str, Any]] = None,
    targets: Optional[List[str]] = None,
    tool_allowed: bool = False,
    ws: Optional[Any] = None,
    emit: Optional[Callable[[str, Any], None]] = None,
):
    print(f"--- EXECUTING RUN_SESSION (mode: {mode}, tool_allowed: {tool_allowed}) ---")
    sid = f"crew_{uuid.uuid4().hex[:8]}"
    logger = JsonlLogger(sid, emit=emit)
    logger._j({"event": "run_session_start", "sid": sid, "mode": mode, "state": state})
    logger._t(f"[RUN_SESSION_START] sid={sid} mode={mode} state={state}")
    if emit:
        emit('log', {'event': 'run_session_start', 'sid': sid, 'mode': mode, 'state': state})

    app_cfg, _ = load_configs()
    active_role, active_model = resolve_task_model(app_cfg, mode)
    print(f"[MODEL_ROUTER] mode={mode} role={active_role} model={active_model}")

    # Use crewai's LLM wrapper (litellm backend) with explicit Ollama endpoint to avoid LiteLLM defaulting to OpenAI.
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    api_key = os.getenv("OPENAI_API_KEY", "dummy")  # LiteLLM requires a key even for Ollama
    active_llm = LLM(model=f"ollama/{active_model}", base_url=base_url, api_key=api_key)

    # Define tools
    file_write_tool = FileWriteTool()
    file_read_tool = FileReadTool()
    
    agent_tools = [file_read_tool, file_write_tool] if tool_allowed else []

    # Define Agents
    master = Agent(role="마스터", goal="요청 분해·배분 후 결론 도출", backstory="팀 리더", llm=active_llm, allow_delegation=True, verbose=True)
    senior = Agent(role="선배 코더", goal="설계/코드 제안과 리팩토링", backstory="경험 많은 엔지니어", llm=active_llm, verbose=True, tools=agent_tools)
    appr = Agent(role="견습 분석가", goal="근거 정리·리스크 점검·요약", backstory="빠른 요약 담당", llm=active_llm, verbose=True)

    conversation_context = f'''이전 대화 내용:\n---\n{history}\n---\n''' if history else ""

    tasks = []
    agents = []

    if mode == 'chat':
        tasks = [Task(
            description=f"""{conversation_context}다음 사용자 요청에 대해 직접적이고 친절하게 **한국어로만** 한 번 답변하세요.
- 'Thought:' 같은 접두사나 영어 설명을 쓰지 말 것.
- 최종 답변 문장만 출력하고 같은 답변을 반복하지 말 것.

[요청]
{user_prompt}""",
            agent=master,
            expected_output="한국어 최종 답변 한 문장만 출력 (Thought 등 영어 접두사 없이)."
        )]
        agents = [master]
    elif mode == 'create':
        create_description = f"""
        {conversation_context}
        [CONTEXT]
        You have access to the following tools: `Write File`.
        Your task is to create a project structure and code based on the user's request.
        
        [INSTRUCTION]
        1.  First, think step-by-step about the necessary file structure (e.g., `main.py`, `utils.py`, `index.html`).
        2.  Use the `Write File` tool for **each file** you need to create. The `file_path` should be relative to the project root, like 'apps/tetris/main.py'.
        3.  After creating all the necessary files, you MUST conclude with a `Final Answer` that summarizes the created file structure and provides instructions on how to run the code.
        
        [REQUEST]
        User's request: '{user_prompt}'
        Options: {options}
        
        **IMPORTANT**: All output, including your thoughts, actions, and final answer, must be in Korean.
        """
        tasks = [Task(
            description=create_description,
            agent=senior,
            expected_output="**한국어로 작성된** 파일 경로, 코드 블록, 그리고 실행 방법을 포함한 최종 답변."
        )]
        agents = [senior]
    elif mode == 'modify':
        tasks = [Task(
            description=f"{conversation_context}[요청] 다음 파일을 수정하라: {targets}. 요구사항: '{user_prompt}'. **반드시 한국어로** 원본과의 차이점(diff)을 포함하여 수정된 코드를 제안하라.",
            agent=senior,
            expected_output="**한국어로 작성된** 수정된 코드와 변경 사항에 대한 상세한 diff."
        )]
        agents = [senior]
    elif mode == 'delete':
        tasks = [Task(
            description=f"{conversation_context}[요청] 다음 대상을 삭제해달라: {targets}. **실제로 삭제하지 말고**, **반드시 한국어로** 안전한 삭제 계획(백업 방법 포함)을 3단계로 제안하라.",
            agent=master,
            expected_output="**한국어로 작성된** 안전한 삭제 계획 3단계."
        )]
        agents = [master]
    else: # Fallback to default chat mode
        tasks = [Task(description=f"Unknown mode. Respond as chat. Request: {user_prompt}", agent=master, expected_output="A concise answer in Korean.")]
        agents = [master]

    crew = Crew(agents=agents, tasks=tasks, process=Process.sequential, verbose=True, callbacks=[logger])
    try:
        result = crew.kickoff()
        logger._j({"event": "run_session_done", "sid": sid, "snippet": str(result)[:200]})
        logger._t(f"[RUN_SESSION_DONE] sid={sid} snippet={str(result)[:200]}")
    except Exception as e:
        import traceback, sys
        traceback.print_exc()
        logger._j({"event": "run_session_error", "sid": sid, "error": str(e)})
        logger._t(f"[RUN_SESSION_ERROR] sid={sid} error={e}")
        if emit:
            emit('log', {'event': 'crew_error', 'text': f"crew.kickoff failed: {e}"})
        print(f"crew.kickoff failed: {e}", file=sys.stderr)
        raise

    clean_json_log_path = os.path.abspath(logger.j)
    clean_txt_log_path = os.path.abspath(logger.t)
    print("\n=== FINAL RESULT ===\n", result)
    print(f"\n로그 파일:\n - {clean_json_log_path}\n - {clean_txt_log_path}")
    logger._j({"event": "run_session_return", "sid": sid, "jsonl": clean_json_log_path, "txt": clean_txt_log_path})
    logger._t(f"[RUN_SESSION_RETURN] sid={sid} jsonl={clean_json_log_path} txt={clean_txt_log_path}")
    return result

if __name__ == "__main__":
    while True:
        prompt = input("\n질문이나 요청 입력 (종료하려면 'exit'): ").strip()
        if prompt.lower() in ("exit", "quit"):
            print("종료합니다.")
            break
        if prompt:
            run_session(prompt, state="MEETING", history="", mode="chat")
