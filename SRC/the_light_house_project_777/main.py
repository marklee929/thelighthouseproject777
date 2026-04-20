# -*- coding: utf-8 -*-
# main.py
from __future__ import annotations
# Early boot trace + utility
import os
import time
import sys
import traceback
import atexit
import re
import random
import socket
from types import SimpleNamespace

# Do not force a custom gevent loop; let gevent choose the default (libuv on Windows).
if "GEVENT_LOOP" in os.environ:
    os.environ.pop("GEVENT_LOOP", None)

# Force crewai to use the project-local config path (avoid user profile resolution).
from project_meta import PROJECT_DISPLAY_NAME, PROJECT_ROOT, SOURCE_ROOT

CONFIG_HOME = str(PROJECT_ROOT / ".config")
CREWAI_CFG_PATH = str(PROJECT_ROOT / ".config" / "crewai" / "settings.json")

# Add the local package root and sibling source root so optional modules can be imported.
for path_entry in (str(PROJECT_ROOT), str(SOURCE_ROOT)):
    if path_entry not in sys.path:
        sys.path.insert(0, path_entry)

os.environ["HOME"] = str(PROJECT_ROOT)
os.environ["USERPROFILE"] = str(PROJECT_ROOT)
os.environ["XDG_CONFIG_HOME"] = CONFIG_HOME
os.environ["CREWAI_CONFIG_PATH"] = CREWAI_CFG_PATH
# Silence CrewAI session file logging unless explicitly enabled.
os.environ.setdefault("CREWAI_DISABLE_LOGS", "true")
os.environ.setdefault("CREWAI_LOG_LEVEL", "ERROR")

# Ensure the config file exists so crewai doesn't fall back to the user directory.
os.makedirs(os.path.dirname(CREWAI_CFG_PATH), exist_ok=True)
if not os.path.exists(CREWAI_CFG_PATH):
    with open(CREWAI_CFG_PATH, "w", encoding="utf-8") as f:
        f.write("{}")

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
BOOT_LOG_PATH = os.path.join(LOG_DIR, "boot_trace.log")
def boot_trace(msg: str):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        log_sink.submit_line(BOOT_LOG_PATH, f"{time.time():.3f} {msg}")
    except Exception:
        pass

boot_trace("before_monkey_patch")

# Patch only the pieces we need for gevent-websocket.
# - Leave threading unpatched because CrewAI spins up real ThreadPool workers.
# - Leave asyncio unpatched for the same reason.
# - Avoid patching queue so concurrent.futures keeps the stdlib Queue (gevent's patched queue triggers LoopExit in threads).
from gevent import monkey
monkey.patch_all(thread=False, asyncio=False, queue=False, select=False)
boot_trace("after_monkey_patch")

import asyncio
import threading
import uuid
import json
import gevent
import logging
from flask import Flask, Response, render_template, jsonify, request
from gevent.pywsgi import WSGIServer
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket.exceptions import WebSocketError
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from collections import deque
from dataclasses import dataclass, field
import uuid
import json
import hashlib

boot_trace("before_import_tools")
from core.tools.tools_files import set_allowed_roots, write_text, delete_path
boot_trace("before_import_orchestrator")
from core.orchestrator import Context, route
boot_trace("before_import_config_loader")
from utils.config_loader import load_configs, merge_permissions
boot_trace("before_import_pyqle")
try:
    from pyqle_core.runtime.main import run_pyqle_loop
    from pyqle_core.io.log_sink import start_sink, stop_sink, sink as log_sink
    from pyqle_core.brain.v2 import config as brain_v2_config
    from pyqle_core.brain.v3 import run_meditation_loop, MeditationParams
    PYQLE_CORE_AVAILABLE = True
    PYQLE_CORE_IMPORT_ERROR = ""
except Exception as exc:
    PYQLE_CORE_AVAILABLE = False
    PYQLE_CORE_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

    async def run_pyqle_loop(*_args, **_kwargs):
        raise RuntimeError("pyqle_core is not available in this project copy.")

    def start_sink() -> None:
        return None

    def stop_sink() -> None:
        return None

    class _FallbackLogSink:
        def submit_line(self, path: str, line: str) -> None:
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "a", encoding="utf-8") as handle:
                    handle.write(f"{line}\n")
            except Exception:
                pass

    log_sink = _FallbackLogSink()
    brain_v2_config = SimpleNamespace(TRACE_LOG=str(PROJECT_ROOT / "logs" / "pyqle_trace.jsonl"))

    def run_meditation_loop(*_args, **_kwargs):
        raise RuntimeError("pyqle_core is not available in this project copy.")

    class MeditationParams:  # type: ignore[override]
        pass

from pyqle_logger import pyqle_log, configure_pyqle_logger
import crew_ai
from dev_reloader import DevAutoReloader
from repositories import (
    PostgresArticleRepository,
    PostgresArticleReviewRepository,
    PostgresConnectionFactory,
    PostgresGeneratedContentRepository,
    PostgresIngestionRunRepository,
    PostgresReviewCardRepository,
    PostgresRssFeedRepository,
    PostgresSourceRepository,
    PostgresSystemConfigRepository,
)
from integrations.rss import ArticleContentClient, RssFeedClient, RssFeedRegistryLoader
from services.analysis import ArticleAnalysisService, LocalLlmTrioArticleAnalysisService
from services.ingestion.service import RssIngestionService
from services.news_collector.collection import NewsCollectorCollectionService
from services.news_collector.feed_management import NewsCollectorFeedManagementService
from services.news_collector.pre_storage_filter import NewsCollectorPreStorageFilter
from services.news_collector.service import NewsCollectorReviewService
from services.review import FacebookCandidateQueueService, TelegramPreviewCardService
from social_automation import SocialAutomationService
boot_trace("imports complete")
boot_trace("imported main.py")

# WebSocket debug logger
def ws_debug(msg):
    """Simple debug logger for WebSocket handler"""
    try:
        print(f"[WS-DEBUG] {msg}", flush=True)
    except Exception:
        try:
            safe = f"[WS-DEBUG] {msg}".encode("utf-8", "backslashreplace").decode("ascii", "backslashreplace")
            print(safe, flush=True)
        except Exception:
            pass

# Best-effort cleanup for local llama/ollama processes when the server exits
try:
    import psutil  # type: ignore
except Exception:
    psutil = None

def _kill_llama_processes():
    if not psutil:
        return
    targets = []
    try:
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            name = (proc.info.get("name") or "").lower()
            cmd = " ".join(proc.info.get("cmdline") or []).lower()
            if "ollama" in name or "ollama" in cmd or "llama" in name or "llama" in cmd:
                if proc.pid != os.getpid():
                    targets.append(proc)
                    continue
            # Check port 11434 (default for ollama)
            try:
                for conn in proc.connections(kind="inet"):
                    if getattr(conn.laddr, "port", None) == 11434:
                        if proc.pid != os.getpid():
                            targets.append(proc)
                            break
            except Exception:
                continue
        seen = set()
        for proc in targets:
            if proc.pid in seen:
                continue
            seen.add(proc.pid)
            ws_debug(f"[LLAMA-CLEANUP] Terminating PID {proc.pid} ({proc.info.get('name')})")
            try:
                proc.terminate()
            except Exception:
                pass
        # Give processes a brief moment to exit, then force kill if needed
        psutil.wait_procs([p for p in targets if p.pid in seen], timeout=3)
    except Exception as exc:
        ws_debug(f"[LLAMA-CLEANUP-ERROR] {exc}")

atexit.register(_kill_llama_processes)
atexit.register(stop_sink)

# Start log sink explicitly (no auto-start on import)
start_sink()

# --- App Setup ---
app = Flask(__name__, template_folder='web', static_folder='web/static')
boot_trace("flask app created")
SOCIAL_AUTOMATION = SocialAutomationService(str(PROJECT_ROOT))
POSTGRES_CONNECTION_FACTORY = PostgresConnectionFactory.from_env()
POSTGRES_ARTICLE_REPOSITORY = PostgresArticleRepository(POSTGRES_CONNECTION_FACTORY)
POSTGRES_ARTICLE_REVIEW_REPOSITORY = PostgresArticleReviewRepository(POSTGRES_CONNECTION_FACTORY)
POSTGRES_GENERATED_CONTENT_REPOSITORY = PostgresGeneratedContentRepository(POSTGRES_CONNECTION_FACTORY)
POSTGRES_REVIEW_CARD_REPOSITORY = PostgresReviewCardRepository(POSTGRES_CONNECTION_FACTORY)
POSTGRES_SOURCE_REPOSITORY = PostgresSourceRepository(POSTGRES_CONNECTION_FACTORY)
POSTGRES_RSS_FEED_REPOSITORY = PostgresRssFeedRepository(POSTGRES_CONNECTION_FACTORY)
POSTGRES_INGESTION_RUN_REPOSITORY = PostgresIngestionRunRepository(POSTGRES_CONNECTION_FACTORY)
POSTGRES_SYSTEM_CONFIG_REPOSITORY = PostgresSystemConfigRepository(POSTGRES_CONNECTION_FACTORY)
RSS_FEED_REGISTRY_LOADER = RssFeedRegistryLoader()
RSS_FEED_CLIENT = RssFeedClient()
RSS_ARTICLE_CONTENT_CLIENT = ArticleContentClient()
ARTICLE_ANALYSIS_SERVICE = ArticleAnalysisService(article_repository=POSTGRES_ARTICLE_REPOSITORY)
NEWS_COLLECTOR_PRE_STORAGE_FILTER = NewsCollectorPreStorageFilter()
LOCAL_LLM_TRIO_ANALYSIS_SERVICE = LocalLlmTrioArticleAnalysisService(base_analysis_service=ARTICLE_ANALYSIS_SERVICE)
RSS_INGESTION_SERVICE = RssIngestionService(
    registry_loader=RSS_FEED_REGISTRY_LOADER,
    feed_client=RSS_FEED_CLIENT,
    article_client=RSS_ARTICLE_CONTENT_CLIENT,
    source_repository=POSTGRES_SOURCE_REPOSITORY,
    rss_feed_repository=POSTGRES_RSS_FEED_REPOSITORY,
    article_repository=POSTGRES_ARTICLE_REPOSITORY,
    ingestion_run_repository=POSTGRES_INGESTION_RUN_REPOSITORY,
)
FACEBOOK_CANDIDATE_QUEUE_SERVICE = FacebookCandidateQueueService(
    article_repository=POSTGRES_ARTICLE_REPOSITORY,
    article_review_repository=POSTGRES_ARTICLE_REVIEW_REPOSITORY,
    generated_content_repository=POSTGRES_GENERATED_CONTENT_REPOSITORY,
)
TELEGRAM_PREVIEW_CARD_SERVICE = TelegramPreviewCardService(
    article_repository=POSTGRES_ARTICLE_REPOSITORY,
    review_card_repository=POSTGRES_REVIEW_CARD_REPOSITORY,
)
NEWS_COLLECTOR_FEED_MANAGEMENT_SERVICE = NewsCollectorFeedManagementService(
    registry_loader=RSS_FEED_REGISTRY_LOADER,
    source_repository=POSTGRES_SOURCE_REPOSITORY,
    rss_feed_repository=POSTGRES_RSS_FEED_REPOSITORY,
    system_config_repository=POSTGRES_SYSTEM_CONFIG_REPOSITORY,
)
NEWS_COLLECTOR_COLLECTION_SERVICE = NewsCollectorCollectionService(
    rss_feed_repository=POSTGRES_RSS_FEED_REPOSITORY,
    article_repository=POSTGRES_ARTICLE_REPOSITORY,
    ingestion_run_repository=POSTGRES_INGESTION_RUN_REPOSITORY,
    ingestion_service=RSS_INGESTION_SERVICE,
    pre_storage_filter=NEWS_COLLECTOR_PRE_STORAGE_FILTER,
    trio_analysis_service=LOCAL_LLM_TRIO_ANALYSIS_SERVICE,
)
NEWS_COLLECTOR_REVIEW_SERVICE = NewsCollectorReviewService(
    article_repository=POSTGRES_ARTICLE_REPOSITORY,
    article_review_repository=POSTGRES_ARTICLE_REVIEW_REPOSITORY,
    facebook_candidate_queue_service=FACEBOOK_CANDIDATE_QUEUE_SERVICE,
    telegram_preview_card_service=TELEGRAM_PREVIEW_CARD_SERVICE,
)


def _resolve_ui_mode(argv: Optional[List[str]] = None) -> str:
    """Resolve initial web UI mode from process args."""
    args = argv if argv is not None else sys.argv
    return "pyqle" if "--pyqle" in args else "crew"

# Shared write-store path (for eval/quality feedback)
_WRITE_STORE = None
_WRITE_STORE_PATH = str(SOURCE_ROOT / "pyqle_core" / "data" / "brain_store" / "learned_nodes.lmdb")

# Serialization helpers (match WriteStore behavior)
try:  # pragma: no cover
    import orjson  # type: ignore

    def _loads(b: bytes):
        return orjson.loads(b)

    def _dumps(obj: Any) -> bytes:
        return orjson.dumps(obj)

except Exception:  # pragma: no cover
    def _loads(b: bytes):
        return json.loads(b.decode("utf-8"))

    def _dumps(obj: Any) -> bytes:
        return json.dumps(obj, ensure_ascii=False).encode("utf-8")

# Initialize FileWriteTool permissions on startup
def _init_file_permissions():
    """Initialize allowed roots for file write operations."""
    project_data = str(PROJECT_ROOT / "data")
    project_logs = str(PROJECT_ROOT / "logs")
    project_reports = str(PROJECT_ROOT / "reports")
    project_temp = str(PROJECT_ROOT / "temp")
    
    # Ensure directories exist
    for d in [project_data, project_logs, project_reports, project_temp]:
        os.makedirs(d, exist_ok=True)
    
    set_allowed_roots([project_data, project_logs, project_reports, project_temp])
    print(f"[INIT] File write permissions set for: {[project_data, project_logs, project_reports, project_temp]}")

_init_file_permissions()
boot_trace("file_permissions_initialized")


def _should_enable_dev_reloader() -> bool:
    value = str(os.getenv("CREW_AUTO_RELOAD", "true")).strip().lower()
    return value not in {"0", "false", "no", "off"}

# --- WriteStore helper for feedback/eval ---
def _get_write_store():
    global _WRITE_STORE
    if _WRITE_STORE is None:
        from pyqle_core.brain.v2.storage.lmdb_store import WriteStore

        _WRITE_STORE = WriteStore(_WRITE_STORE_PATH)
    return _WRITE_STORE


# Exportable LMDB keyword store with temp/main edge support
class LmdbKeywordStore:
    def sample_random_keywords(self, k: int):
        keys = fetch_lmdb_keywords(top_k=300)
        random.shuffle(keys)
        return keys[:k]

    def _load_dict(self, txn, key: bytes):
        store = _get_write_store()
        try:
            raw = txn.get(key, db=store.db_relations)  # type: ignore[attr-defined]
        except Exception:
            raw = None
        if not raw:
            return {}
        try:
            obj = _loads(raw)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
        return {}

    def _store_dict(self, txn, key: bytes, data: dict) -> None:
        store = _get_write_store()
        try:
            txn.put(key, _dumps(data), db=store.db_relations)  # type: ignore[attr-defined]
        except Exception:
            pass

    def set_meta(self, key_str: str, value: Any) -> None:
        """Generic metadata setter."""
        store = _get_write_store()
        key = key_str.encode("utf-8")
        try:
            with store.env.begin(write=True) as txn:  # type: ignore[attr-defined]
                 self._store_dict(txn, key, {"v": value})
        except Exception:
            pass
            
    def save_external_event(self, seed: str, data: dict) -> None:
        """Saves out:ext meta."""
        ts = data.get("ts", time.time())
        try:
             base_key_prefix = f"out:ext:{_norm_kw(seed)}:{ts}"
             self.set_meta(f"{base_key_prefix}:query", data.get("query"))
             self.set_meta(f"{base_key_prefix}:summary", data.get("response"))
             self.set_meta(f"{base_key_prefix}:tokens", data.get("tokens"))
             self.set_meta(f"{base_key_prefix}:edges_added", data.get("edges_added"))
             self.set_meta(f"{base_key_prefix}:raw", data)
        except Exception:
            pass

    def _update_touched(self, txn, kw_norm: str, day_id: int) -> None:
        store = _get_write_store()
        touched_key = f"tmp:touched:{day_id}".encode("utf-8")
        touched = self._load_dict(txn, touched_key)
        touched[kw_norm] = 1
        self._store_dict(txn, touched_key, touched)
        meta_key = f"tmp:meta:kw:{kw_norm}:last_ts".encode("utf-8")
        try:
            txn.put(meta_key, _dumps(time.time()), db=store.db_relations)  # type: ignore[attr-defined]
        except Exception:
            pass

    def increment_edge(self, a: str, b: str, delta: float = 1.0):
        return self.increment_edge_layer(a, b, delta, layer="main")

    def increment_edge_layer(self, a: str, b: str, delta: float = 1.0, layer: str = "main"):
        store = _get_write_store()
        kw_a = _norm_kw(a)
        kw_b = _norm_kw(b)
        if not kw_a or not kw_b or kw_a == kw_b:
            return
        prefix = "out:kw" if layer == "main" else "tmp:out:kw"
        now_ts = time.time()
        day_id = int(now_ts // 86400)
        with store.env.begin(write=True) as txn:  # type: ignore[attr-defined]
            for src, dst in ((kw_a, kw_b), (kw_b, kw_a)):
                key = f"{prefix}:{src}:co_occurs".encode("utf-8")
                data = self._load_dict(txn, key)
                data[dst] = float(data.get(dst, 0.0)) + float(delta)
                self._store_dict(txn, key, data)
                if layer == "temp":
                    self._update_touched(txn, src, day_id)

    def fetch_neighbors(self, kw: str, top_n: int, include_temp: bool = True, temp_weight: float = 0.3):
        store = _get_write_store()
        kw_norm = _norm_kw(kw)
        key_main = f"out:kw:{kw_norm}:co_occurs".encode("utf-8")
        key_temp = f"tmp:out:kw:{kw_norm}:co_occurs".encode("utf-8")
        combined = {}
        try:
            with store.env.begin(write=False) as txn:  # type: ignore[attr-defined]
                main_items = self._load_dict(txn, key_main)
                temp_items = self._load_dict(txn, key_temp) if include_temp else {}
                for other, cnt in main_items.items():
                    combined[other] = combined.get(other, 0.0) + float(cnt)
                if include_temp:
                    for other, cnt in temp_items.items():
                        combined[other] = combined.get(other, 0.0) + float(cnt) * float(temp_weight)
        except Exception:
            combined = {}
        pairs = []
        for other, val in combined.items():
            if other is None:
                continue
            ok = other.decode("utf-8") if isinstance(other, (bytes, bytearray)) else str(other)
            pairs.append((ok, float(val)))
        pairs.sort(key=lambda x: x[1], reverse=True)
        return pairs[:top_n]

    def temp_decay_and_purge(self, now_ts: float, temp_decay: float = 0.98, ttl_days: int = 7, batch_limit: int = 5000):
        store = _get_write_store()
        processed = 0
        ttl_sec = ttl_days * 86400
        day_id = int(now_ts // 86400)
        allowed_days = {day_id, day_id - 1}
        with store.env.begin(write=True) as txn:  # type: ignore[attr-defined]
            cur = txn.cursor(db=store.db_relations)  # type: ignore[attr-defined]
            for key, _val in cur:
                if not key.startswith(b"tmp:touched:"):
                    continue
                try:
                    touched = self._load_dict(txn, key)
                    touched_day = int(key.decode("utf-8").split(":")[-1])
                except Exception:
                    touched = {}
                    touched_day = None
                if allowed_days and touched_day is not None and touched_day not in allowed_days:
                    continue
                for kw_norm in list(touched.keys()):
                    if processed >= batch_limit:
                        return
                    processed += 1
                    meta_key = f"tmp:meta:kw:{kw_norm}:last_ts".encode("utf-8")
                    last_raw = txn.get(meta_key, db=store.db_relations)  # type: ignore[attr-defined]
                    last_ts = 0.0
                    if last_raw:
                        try:
                            last_ts = float(_loads(last_raw))
                        except Exception:
                            last_ts = 0.0
                    temp_key = f"tmp:out:kw:{kw_norm}:co_occurs".encode("utf-8")
                    data = self._load_dict(txn, temp_key)
                    if not data:
                        continue
                    if ttl_sec and now_ts - last_ts > ttl_sec:
                        txn.delete(temp_key, db=store.db_relations)  # type: ignore[attr-defined]
                        txn.delete(meta_key, db=store.db_relations)  # type: ignore[attr-defined]
                        touched.pop(kw_norm, None)
                        continue
                    for k2 in list(data.keys()):
                        data[k2] = float(data.get(k2, 0.0)) * float(temp_decay)
                        if data[k2] <= 0.0:
                            data.pop(k2, None)
                    self._store_dict(txn, temp_key, data)
                if not touched:
                    try:
                        txn.delete(key, db=store.db_relations)  # type: ignore[attr-defined]
                    except Exception:
                        pass
                else:
                    self._store_dict(txn, key, touched)

    def temp_promote(self, now_ts: float, threshold: float = 5.0, batch_limit: int = 500, state=None, budget=None):
        store = _get_write_store()
        promoted = 0
        day_id = int(now_ts // 86400)
        with store.env.begin(write=True) as txn:  # type: ignore[attr-defined]
            cur = txn.cursor(db=store.db_relations)  # type: ignore[attr-defined]
            for key, _val in cur:
                if not key.startswith(b"tmp:touched:"):
                    continue
                touched = self._load_dict(txn, key)
                try:
                    touched_day = int(key.decode("utf-8").split(":")[-1])
                except Exception:
                    touched_day = None
                if touched_day is not None and touched_day not in {day_id, day_id - 1}:
                    continue
                for kw_norm in list(touched.keys()):
                    if promoted >= batch_limit:
                        return
                    if budget and getattr(budget, "enable", False) and getattr(budget, "block_on_exhaust", True):
                        if state is not None and getattr(state, "daily_main_edge_add", 0) >= budget.daily_main_edge_limit:
                            return
                        if state is not None and getattr(state, "daily_new_keyword_add", 0) >= budget.daily_new_keyword_limit:
                            return
                    temp_key = f"tmp:out:kw:{kw_norm}:co_occurs".encode("utf-8")
                    data = self._load_dict(txn, temp_key)
                    if not data:
                        continue
                    for other, cnt in list(data.items()):
                        if float(cnt) >= threshold and other and other != kw_norm:
                            promoted += 1
                            if budget and getattr(budget, "enable", False) and getattr(budget, "block_on_exhaust", True):
                                if state is not None and getattr(state, "daily_main_edge_add", 0) >= budget.daily_main_edge_limit:
                                    return
                                if state is not None and getattr(state, "daily_new_keyword_add", 0) >= budget.daily_new_keyword_limit:
                                    return
                            main_key = f"out:kw:{kw_norm}:co_occurs".encode("utf-8")
                            existing_main_raw = txn.get(main_key, db=store.db_relations)  # type: ignore[attr-defined]
                            main_data = self._load_dict(txn, main_key)
                            if state is not None:
                                try:
                                    if not existing_main_raw:
                                        state.daily_new_keyword_add += 1
                                except Exception:
                                    pass
                            main_data[other] = float(main_data.get(other, 0.0)) + float(cnt)
                            self._store_dict(txn, main_key, main_data)
                            # symmetric update
                            other_key = f"out:kw:{other}:co_occurs".encode("utf-8")
                            existing_other_raw = txn.get(other_key, db=store.db_relations)  # type: ignore[attr-defined]
                            other_data = self._load_dict(txn, other_key)
                            if state is not None:
                                try:
                                    if not existing_other_raw:
                                        state.daily_new_keyword_add += 1
                                except Exception:
                                    pass
                            other_data[kw_norm] = float(other_data.get(kw_norm, 0.0)) + float(cnt)
                            self._store_dict(txn, other_key, other_data)
                            if state is not None:
                                try:
                                    state.daily_main_edge_add += 1
                                except Exception:
                                    pass
                            data.pop(other, None)
                    if not data:
                        try:
                            txn.delete(temp_key, db=store.db_relations)  # type: ignore[attr-defined]
                        except Exception:
                            pass
                        meta_key = f"tmp:meta:kw:{kw_norm}:last_ts".encode("utf-8")
                        try:
                            txn.delete(meta_key, db=store.db_relations)  # type: ignore[attr-defined]
                        except Exception:
                            pass
                        touched.pop(kw_norm, None)
                if not touched:
                    try:
                        txn.delete(key, db=store.db_relations)  # type: ignore[attr-defined]
                    except Exception:
                        pass
                else:
                    self._store_dict(txn, key, touched)


def make_lmdb_keyword_store() -> LmdbKeywordStore:
    return LmdbKeywordStore()
# Cached LMDB candidates for correction auto-sentence generation
_LMDB_CAND_CACHE = {
    "ts": 0.0,
    "items": [],  # list of (mention_count:int, avg_score:float, kw:str)
}

def _norm_kw(kw: str) -> str:
    return kw.lower() if isinstance(kw, str) and kw.isascii() else kw

def _refresh_lmdb_candidates(max_scan: int = 3000) -> None:
    """
    Scan LMDB nodes/relations and cache low-frequency, low-quality keywords.
    Cached list is sorted by (mention_count asc, avg_score asc).
    """
    global _LMDB_CAND_CACHE
    store = _get_write_store()
    items = []

    with store.env.begin(write=False) as txn:  # type: ignore[attr-defined]
        cur = txn.cursor(db=store.db_nodes)  # type: ignore[attr-defined]
        for k, v in cur:
            if not k.startswith(b"node:kw:"):
                continue
            try:
                obj = _loads(v)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            stats = obj.get("stats") or {}
            mc = int(stats.get("mention_count", 0))

            raw_key = k.decode("utf-8", "ignore")
            kw = raw_key.split("node:kw:", 1)[-1]
            kw_norm = _norm_kw(kw)
            if not kw or len(kw) <= 1:
                continue
            if kw_norm in {"하다", "것", "수", "되다", "있다", "the", "and", "a", "chat", "pyqle"}:
                continue

            avg = 50.0  # neutral default so missing quality isn't treated as worst
            qkey = f"out:kw:{kw_norm}:quality".encode("utf-8")
            try:
                qraw = txn.get(qkey, db=store.db_relations)  # type: ignore[attr-defined]
            except Exception:
                qraw = None
            if qraw:
                try:
                    qobj = _loads(qraw)
                    if isinstance(qobj, dict):
                        avg = float(qobj.get("avg_score", avg))
                except Exception:
                    pass

            items.append((mc, avg, kw))
            if len(items) >= max_scan:
                break

    items.sort(key=lambda x: (x[0], x[1]))
    _LMDB_CAND_CACHE = {"ts": time.time(), "items": items}

def _pick_related_keywords(seed_kw: str, k: int = 6) -> list[str]:
    """
    Fetch related keywords from co_occurs and sample up to k neighbors.
    """
    store = _get_write_store()
    kw_norm = _norm_kw(seed_kw)
    key = f"out:kw:{kw_norm}:co_occurs".encode("utf-8")
    items = []
    try:
        with store.env.begin(write=False) as txn:  # type: ignore[attr-defined]
            raw = txn.get(key, db=store.db_relations)  # type: ignore[attr-defined]
        if raw:
            obj = _loads(raw)
            if isinstance(obj, dict):
                items = list(obj.items())
    except Exception:
        items = []

    items.sort(key=lambda x: int(x[1]) if len(x) > 1 else 0, reverse=True)
    pool = [kw for kw, _v in items[:50] if kw and len(str(kw)) > 1]
    if not pool:
        return []
    if len(pool) <= k:
        return pool
    return random.sample(pool, k)

def generate_auto_sentence() -> str:
    now = time.time()
    cache_age = now - _LMDB_CAND_CACHE.get("ts", 0.0)
    if cache_age > 30 or not _LMDB_CAND_CACHE.get("items"):
        try:
            _refresh_lmdb_candidates()
        except Exception:
            return "이 문장 은 띄어쓰기 가 이상 하다"

    items = _LMDB_CAND_CACHE.get("items") or []
    pick = items[:50] if len(items) >= 50 else items
    if not pick:
        return "이 문장 은 띄어쓰기 가 이상 하다"

    seed_kw = random.choice(pick)[2]
    related = _pick_related_keywords(seed_kw, k=24)  # Leave extra room for filtering.

    # Remove duplicates and hub terms while preserving order.
    seen = set()
    words = []
    for w in [seed_kw] + related:
        if not w:
            continue
        wn = _norm_kw(w)
        if wn in seen:
            continue
        if wn in {"하다","것","수","되다","있다","the","and","a","chat","pyqle"}:
            continue
        seen.add(wn)
        words.append(w)
        if len(words) >= 24:  # Hard upper bound.
            break

    # If the result is too short, backfill with additional candidates or fall back.
    if len(words) < 2:
        extras = [kw for _mc, _avg, kw in pick if kw and _norm_kw(kw) not in seen]
        for kw in extras:
            if len(words) >= 2:
                break
            words.append(kw)
        if len(words) < 2:
            return "이 문장 은 띄어쓰기 가 이상 하다"
    if len(words) < 6:
        pyqle_log("info", "keywords fetched (short)", {"stage": "lmdb_keywords", "keys_n": len(words), "top_preview": words[:3]})
        return " ".join(words)

    pyqle_log("info", "keywords fetched", {"stage": "lmdb_keywords", "keys_n": len(words), "top_preview": words[:3]})
    return " ".join(words)

def fetch_lmdb_keywords(top_k: int = 12) -> List[str]:
    """
    Return up to top_k filtered keywords from the LMDB cache (refreshes if stale).
    """
    now = time.time()
    cache_age = now - _LMDB_CAND_CACHE.get("ts", 0.0)
    if cache_age > 30 or not _LMDB_CAND_CACHE.get("items"):
        try:
            _refresh_lmdb_candidates()
        except Exception:
            return []
    items = _LMDB_CAND_CACHE.get("items") or []
    pick = items[:top_k] if len(items) >= top_k else items
    keywords: List[str] = []
    seen = set()
    for _mc, _avg, kw in pick:
        wn = _norm_kw(kw)
        if wn in seen or not kw:
            continue
        if wn in {"하다","것","수","되다","있다","the","and","a","chat","pyqle"}:
            continue
        seen.add(wn)
        keywords.append(kw)
        if len(keywords) >= top_k:
            break
    return keywords

def build_correction_candidates(keywords: List[str], session_id: str, m: int = 6) -> List[KeywordBundle]:
    """
    Build keyword bundles (not sentences). Each candidate holds tokens and a preview.
    """
    if not keywords:
        return []
    base = keywords[:30]
    bundles: List[KeywordBundle] = []
    for _ in range(max(m, 6)):
        k = random.randint(2, min(6, max(2, len(base))))
        pick = random.sample(base, k=k) if len(base) >= k else base
        tokens = pick
        preview = " ".join(tokens)
        bundles.append(
            KeywordBundle(
                bundle_id=str(uuid.uuid4())[:8],
                session_id=session_id,
                tokens=list(tokens),
                source="lmdb",
                preview=preview,
            )
        )
    # De-dup while preserving order based on token tuple
    seen = set()
    unique: List[KeywordBundle] = []
    for b in bundles:
        key = tuple(b.tokens)
        if key in seen:
            continue
        seen.add(key)
        unique.append(b)
        if len(unique) >= m:
            break
    return unique


def filter_correction_candidates(candidates: List[KeywordBundle]) -> (List[KeywordBundle], int, int):
    """
    Apply lightweight quality filters:
    - Length between 12 and 60 chars
    - Drop if two or more English tokens appear
    - Drop if obvious particle spam patterns repeat
    """
    kept: List[KeywordBundle] = []
    dropped = 0
    spam_patterns = ["은은", "을을", "이가", "는가", "때문에"]
    for bundle in candidates:
        tokens_joined = " ".join(bundle.tokens)
        if not tokens_joined:
            dropped += 1
            continue
        clen = len(tokens_joined)
        if clen < 12 or clen > 60:
            dropped += 1
            continue
        english_tokens = re.findall(r"[A-Za-z]+", tokens_joined)
        if len(english_tokens) >= 2:
            dropped += 1
            continue
        spam_hit = sum(tokens_joined.count(pat) for pat in spam_patterns)
        if spam_hit >= 2:
            dropped += 1
            continue
        kept.append(bundle)
    if not kept and candidates:
        kept.append(candidates[0])
    return kept, len(kept), dropped


# --- /pyqle command parser ---
def parse_pyqle_cmd(s: str):
    """
    Parse a '/pyqle --chat ...' style string into (clean_text, options dict).
    Defaults: llama ON, search OFF, multi OFF, return_all OFF.
    """
    opt = {"llama": True, "search": False, "multi": False, "return_all": False}
    raw = (s or "").strip()
    if not raw.startswith("/pyqle"):
        return raw, opt

    # Extract quoted text first
    m = re.search(r'"([^"]+)"', raw)
    if m:
        text = m.group(1)
    else:
        toks = [t for t in raw.split() if t and not t.startswith("--") and not t.startswith("/pyqle")]
        text = " ".join(toks).strip()

    for t in raw.split():
        if t == "--llama=false":
            opt["llama"] = False
        elif t == "--search":
            opt["search"] = True
        elif t == "--multiple":
            opt["multi"] = True
        elif t == "--return-all":
            opt["return_all"] = True
    if not text:
        text = "ping"
    return text, opt

# --- Global State ---
session_loops: Dict[str, Dict[str, Any]] = {}
correction_inflight: Dict[str, str] = {}
metrics_buf = deque(maxlen=200)
learn_jobs: Dict[str, Dict[str, Any]] = {}
LEARN_STATE_PATH = os.path.join(LOG_DIR, "learn_state.json")
LEARN_ROOT = r"D:\121.한국어 성능이 개선된 초거대AI 언어모델 개발 및 데이터\3.개방데이터\1.데이터\Training\refined data"
meditation_loops: Dict[str, Dict[str, Any]] = {}
meditation_v3_lock = threading.Lock()


def record_metric(latency_ms: Optional[int] = None, error: bool = False, fallback: bool = False):
    metrics_buf.append({"latency_ms": latency_ms, "error": error, "fallback": fallback})


def summarize_metrics() -> str:
    if not metrics_buf:
        return "No metrics yet."
    latencies = [m["latency_ms"] for m in metrics_buf if m.get("latency_ms") is not None]
    avg_lat = sum(latencies) / len(latencies) if latencies else 0.0
    max_lat = max(latencies) if latencies else 0.0
    err_rate = sum(1 for m in metrics_buf if m.get("error")) / len(metrics_buf) * 100.0
    fb_rate = sum(1 for m in metrics_buf if m.get("fallback")) / len(metrics_buf) * 100.0
    last10 = latencies[-10:] if latencies else []
    return (
        f"LLM latency avg={avg_lat:.1f}ms max={max_lat:.1f}ms | "
        f"error_rate={err_rate:.1f}% fallback_rate={fb_rate:.1f}% | "
        f"last10={last10}"
    )


def _append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception as exc:
        ws_debug(f"[JSONL-ERROR] {path}: {exc}")


def _save_provenance(
    sentence_id: str,
    session_id: str,
    branch: str,
    llm_mode: str,
    best_total: float,
    bundles: List[KeywordBundle],
    corrected: str,
    result: Any,
    used_tokens: Optional[List[str]] = None,
) -> None:
    try:
        tokens = used_tokens if used_tokens is not None else []
        if used_tokens is None:
            for b in bundles:
                tokens.extend(b.tokens)
            tokens = list(dict.fromkeys(tokens))
        record = {
            "sentence_id": sentence_id,
            "session_id": session_id,
            "ts": time.time(),
            "mode": "correction",
            "branch": branch,
            "llm_mode": llm_mode,
            "coherence_best_total": best_total,
            "used_bundle_ids": [b.bundle_id for b in bundles],
            "used_tokens": tokens,
            "final_text": corrected,
            "llm_used": getattr(result, "llm_used", False),
            "is_fallback": getattr(result, "is_fallback", False),
            "endpoint": getattr(result, "endpoint", None),
            "model": getattr(result, "model", None),
        }
        out_path = os.path.join(LOG_DIR, f"provenance_{time.strftime('%Y-%m-%d')}.jsonl")
        _append_jsonl(out_path, record)
        pyqle_log("info", "provenance_saved", {"stage": "provenance_saved", "session_id": session_id, "sentence_id": sentence_id, "used_tokens_n": len(tokens), "used_bundle_n": len(bundles)})
    except Exception as exc:
        tb = "\n".join(traceback.format_exc().splitlines()[:6])
        pyqle_log("error", f"provenance save failed: {exc}", {"stage": "error", "session_id": session_id, "where": "provenance", "err": str(exc), "traceback_head": tb})


def _update_keyword_stats(tokens: List[str]) -> None:
    try:
        if not tokens:
            return
        from collections import Counter
        counts = Counter(tokens)
        out_path = os.path.join(LOG_DIR, f"token_usage_{time.strftime('%Y-%m-%d')}.jsonl")
        _append_jsonl(out_path, {"ts": time.time(), "token_counts": counts})
        pyqle_log("info", "keyword_stats_updated", {"stage": "keyword_stats_updated", "updated_n": len(counts)})
    except Exception as exc:
        tb = "\n".join(traceback.format_exc().splitlines()[:6])
        pyqle_log("error", f"keyword stats failed: {exc}", {"stage": "error", "where": "keyword_stats", "err": str(exc), "traceback_head": tb})


def _strengthen_edges(tokens: List[str], best_total: float) -> None:
    try:
        toks = tokens[:30]
        if len(toks) < 2:
            return
        delta = 1.0 + best_total / 50.0
        pairs = []
        # Neighbor pairs to limit explosion; if still many, cap at 100.
        for i in range(len(toks) - 1):
            pairs.append((toks[i], toks[i + 1]))
        if len(pairs) > 100:
            pairs = pairs[:100]
        out_path = os.path.join(LOG_DIR, f"edge_updates_{time.strftime('%Y-%m-%d')}.jsonl")
        for a, b in pairs:
            _append_jsonl(out_path, {"ts": time.time(), "a": a, "b": b, "delta": delta})
        pyqle_log("info", "edge_strengthened", {"stage": "edge_strengthened", "pairs_n": len(pairs), "delta_preview": round(delta, 3)})
    except Exception as exc:
        tb = "\n".join(traceback.format_exc().splitlines()[:6])
        pyqle_log("error", f"edge strengthen failed: {exc}", {"stage": "error", "where": "edge_strengthen", "err": str(exc), "traceback_head": tb})


# --- Learning chunk ingest helpers ---
def _learn_state_load() -> Dict[str, Any]:
    try:
        with open(LEARN_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _learn_state_save(state: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(LEARN_STATE_PATH), exist_ok=True)
        state = dict(state or {})
        state["updated_ts"] = time.time()
        with open(LEARN_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        pyqle_log("info", "learn_checkpoint", {"stage": "learn_checkpoint", "state": {k: state.get(k) for k in ["current_file", "file_idx", "item_idx", "totals"]}})
    except Exception as exc:
        tb = "\n".join(traceback.format_exc().splitlines()[:6])
        pyqle_log("error", f"learn checkpoint failed: {exc}", {"stage": "error", "where": "learn_checkpoint", "err": str(exc), "traceback_head": tb})


def _learn_file_list() -> List[str]:
    order = [
        "Refined_data_01.json", "Refined_data_02.json", "Refined_data_03.json", "Refined_data_04.json",
        "Refined_data_05.json", "Refined_data_06.json", "Refined_data_07.json", "Refined_data_08.json",
        "Refined_data_09.json", "Refined_data_10.json", "Refined_data_11.json", "Refined_data_12.json",
        "Refined_data_13.json", "Refined_data_14.json", "Refined_data_15.json", "Refined_data_16.json",
        "Refined_data_17.json", "Refined_data_18.json",
        "Refined_RMdata.json", "Refined_SFTdata.json", "Refined_PPOdata.json",
    ]
    files = []
    for name in order:
        p = os.path.join(LEARN_ROOT, name)
        if os.path.exists(p):
            files.append(p)
    return files


def _learn_stream_items(path: str, start_idx: int = 0):
    """Yield items from large JSON array using ijson if available."""
    try:
        import ijson  # type: ignore
        with open(path, "rb") as f:
            for idx, item in enumerate(ijson.items(f, "item")):
                if idx < start_idx:
                    continue
                yield idx, item
    except Exception:
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception:
                return
        if isinstance(data, list):
            for idx, item in enumerate(data):
                if idx < start_idx:
                    continue
                yield idx, item


def _learn_classify(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    # Schema: question/answer.
    if "질문" in item and "대답" in item:
        return {"type": "qa", "q": item.get("질문"), "a": item.get("대답"), "meta": {k: v for k, v in item.items() if k not in ("질문", "대답")}}

    # Schema: input/output where output is a string -> prompt.
    if "입력" in item and "출력" in item and isinstance(item.get("출력"), str):
        inp = item.get("입력")
        meta = {k: v for k, v in item.items() if k not in ("입력", "출력")}
        # Parse tone/domain/goal/freq from the input using simple regex rules.
        if isinstance(inp, str):
            tone = None
            domain = None
            goal = None
            freq = None
            m = re.search(r"톤[:：]\s*([^\n]+)", inp)
            if m:
                tone = m.group(1).strip()
            m = re.search(r"도메인[:：]\s*([^\n]+)", inp)
            if m:
                domain = m.group(1).strip()
            m = re.search(r"목표[:：]\s*([^\n]+)", inp)
            if m:
                goal = m.group(1).strip()
            m = re.search(r"빈도[:：]\s*(\d+)", inp)
            if m:
                try:
                    freq = int(m.group(1))
                except Exception:
                    freq = None
            if tone or domain or goal or freq:
                meta.update({"tone": tone, "domain": domain, "goal": goal, "freq": freq, "입력_raw": inp})
            else:
                meta["입력_raw"] = inp
        return {"type": "prompt", "prompt": item.get("출력"), "meta": meta}

    # Schema: input/output where output is a list -> reward model samples.
    if "입력" in item and "출력" in item and isinstance(item.get("출력"), list):
        inp = item.get("입력") or {}
        q = ""
        if isinstance(inp, dict):
            q = inp.get("질문") or inp.get("prompt") or ""
        elif isinstance(inp, str):
            q = inp
        cands = []
        for c in item.get("출력") or []:
            if not isinstance(c, dict):
                continue
            text = c.get("내용") or c.get("text") or ""
            freq = c.get("빈도") or c.get("freq") or 0
            cands.append({"text": text, "freq": freq})
        meta = {k: v for k, v in item.items() if k not in ("입력", "출력")}
        return {"type": "rm", "q": q, "cands": cands, "meta": meta}

    # Miscellaneous: keep the existing English schema support.
    if "input" in item and "output" in item:
        return {"type": "qa", "q": item.get("input"), "a": item.get("output"), "meta": {k: v for k, v in item.items() if k not in ("input", "output")}}
    if "prompt" in item and ("chosen" in item or "rejected" in item):
        cands = []
        if isinstance(item.get("chosen"), list):
            cands.extend(item.get("chosen"))
        else:
            cands.append(item.get("chosen"))
        if "rejected" in item:
            if isinstance(item.get("rejected"), list):
                cands.extend(item.get("rejected"))
            else:
                cands.append(item.get("rejected"))
        return {"type": "rm", "q": item.get("prompt"), "cands": cands, "meta": {k: v for k, v in item.items() if k not in ("prompt", "chosen", "rejected")}}
    if "prompt" in item:
        return {"type": "prompt", "prompt": item.get("prompt"), "meta": {k: v for k, v in item.items() if k != "prompt"}}
    return None


def _learn_store(record: Dict[str, Any]) -> None:
    key = hashlib.sha256(json.dumps(record, ensure_ascii=False).encode("utf-8")).hexdigest()
    out_path = os.path.join(LOG_DIR, f"learn_ingest_{time.strftime('%Y-%m-%d')}.jsonl")
    _append_jsonl(out_path, {"key": key, **record})


def _run_learn(session_id: str, loop_on: bool) -> None:
    state = _learn_state_load()
    files = _learn_file_list()
    totals = state.get("totals") or {"processed": 0, "saved_qa": 0, "saved_rm": 0, "saved_prompt": 0, "errors": 0}
    file_idx = int(state.get("file_idx", 0))
    item_idx = int(state.get("item_idx", 0))
    if file_idx < 0:
        file_idx = 0
    if file_idx >= len(files):
        file_idx = 0
        item_idx = 0
    resumed = state.get("current_file") and os.path.basename(files[file_idx]) == os.path.basename(state.get("current_file", ""))
    if resumed:
        pyqle_log("info", "learn_resume", {"stage": "learn_resume", "session_id": session_id, "file": files[file_idx], "item_idx": item_idx})
    pyqle_log("info", "learn_start", {"stage": "learn_start", "session_id": session_id})

    try:
        for fi in range(file_idx, len(files)):
            path = files[fi]
            pyqle_log("info", "learn_file_start", {"stage": "learn_file_start", "session_id": session_id, "file": path})
            checkpoint_counter = 0
            for idx, item in _learn_stream_items(path, start_idx=item_idx if fi == file_idx else 0):
                if learn_jobs.get(session_id, {}).get("cancel"):
                    pyqle_log("info", "learn_stop", {"stage": "learn_stop", "session_id": session_id, "file": path, "item_idx": idx})
                    _learn_state_save({"current_file": path, "file_idx": fi, "item_idx": idx, "totals": totals})
                    return
                record = _learn_classify(item)
                if record is None:
                    totals["errors"] = totals.get("errors", 0) + 1
                    if totals["errors"] % 10000 == 0:
                        try:
                            pyqle_log("info", "learn_error_sample", {"stage": "learn_error_sample", "session_id": session_id, "file": path, "idx": idx, "keys": list(item.keys()) if isinstance(item, dict) else str(type(item))})
                        except Exception:
                            pass
                    continue
                _learn_store(record)
                totals["processed"] = totals.get("processed", 0) + 1
                if record["type"] == "qa":
                    totals["saved_qa"] = totals.get("saved_qa", 0) + 1
                elif record["type"] == "rm":
                    totals["saved_rm"] = totals.get("saved_rm", 0) + 1
                elif record["type"] == "prompt":
                    totals["saved_prompt"] = totals.get("saved_prompt", 0) + 1
                pyqle_log("info", "learn_item", {"stage": "learn_item", "session_id": session_id, "file": path, "idx": idx})
                checkpoint_counter += 1
                if checkpoint_counter >= 200:
                    _learn_state_save({"current_file": path, "file_idx": fi, "item_idx": idx + 1, "totals": totals})
                    checkpoint_counter = 0
                if not loop_on and checkpoint_counter == 0:
                    _learn_state_save({"current_file": path, "file_idx": fi, "item_idx": idx + 1, "totals": totals})
                    pyqle_log("info", "learn_end", {"stage": "learn_end", "session_id": session_id, "totals": totals})
                    return
            # end file
            _learn_state_save({"current_file": path, "file_idx": fi + 1, "item_idx": 0, "totals": totals})
            pyqle_log("info", "learn_file_end", {"stage": "learn_file_end", "session_id": session_id, "file": path})
            item_idx = 0
        pyqle_log("info", "learn_end", {"stage": "learn_end", "session_id": session_id, "totals": totals})
    except Exception as exc:
        tb = "\n".join(traceback.format_exc().splitlines()[:6])
        pyqle_log("error", f"learn error: {exc}", {"stage": "learn_error", "session_id": session_id, "err": str(exc), "traceback_head": tb})


def coherence_improve_loop(keywords: List[str], session_id: str, max_steps: int = 5) -> (List[KeywordBundle], CoherenceScore):
    pyqle_log("info", "coherence_loop_start", {"stage": "coherence_loop_start", "session_id": session_id, "max_steps": max_steps})
    best_score = CoherenceScore(0, 0, 0, 1, 0)
    best_bundles: List[KeywordBundle] = []
    base_keywords = list(dict.fromkeys(keywords))
    improved_any = False
    size_pool = [3, 4, 5, 6]
    for step in range(max_steps):
        # Action selection
        action = "baseline" if step == 0 else "resample"
        sample_keys = list(base_keywords)
        # If low score, try dropping noisy tokens
        if best_score.total < 60 and step > 0:
            sample_keys = [k for k in sample_keys if not re.search(r"[A-Za-z]{2,}", k)]
            action = "drop_english"
        if best_score.total < 60 and step > 1:
            spam_patterns = ["은은", "을을", "이가", "는가", "때문에"]
            sample_keys = [k for k in sample_keys if not any(pat in k for pat in spam_patterns)]
            action = "drop_spam"
        if len(sample_keys) < 2:
            sample_keys = base_keywords
            action = "fallback_resample"

        bundles = build_correction_candidates(sample_keys, session_id=session_id, m=8)
        bundles, kept_n, dropped_n = filter_correction_candidates(bundles)
        scored = [(score_bundle(b), b) for b in bundles]
        scored.sort(key=lambda x: x[0].total, reverse=True)
        top_bundles = [b for _s, b in scored[:3]] if scored else bundles[:1]
        current_best = scored[0][0] if scored else CoherenceScore(0, 0, 0, 1, 0)
        improved = current_best.total > best_score.total
        if improved:
            best_score = current_best
            best_bundles = top_bundles
            improved_any = True
        pyqle_log(
            "info",
            "coherence_loop_step",
            {
                "stage": "coherence_loop_step",
                "session_id": session_id,
                "step": step,
                "best_total": round(best_score.total, 2),
                "improved": improved,
                "action": action,
            },
        )
        if best_score.total >= 90:
            break

    pyqle_log(
        "info",
        "coherence_loop_end",
        {
            "stage": "coherence_loop_end",
            "session_id": session_id,
            "best_total": round(best_score.total, 2),
            "steps": step + 1,
            "improved_any": improved_any,
        },
    )
    if not best_bundles:
        # Fallback to a single bundle if none kept
        fallback = build_correction_candidates(base_keywords, session_id=session_id, m=1)
        best_bundles = fallback or []
    return best_bundles, best_score
def score_bundle(bundle: KeywordBundle) -> CoherenceScore:
    tokens = bundle.tokens or []
    tn = len(tokens)
    if tn == 0:
        return CoherenceScore(total=0.0, cooccur=0.0, graph=0.0, penalty=1.0, tokens_n=0)

    # Heuristic cooccur: shorter bundles slightly higher; small bonus if tokens repeat across session.
    cooccur = max(0.1, min(1.0, 0.8 - 0.05 * max(0, tn - 2)))
    english_tokens = re.findall(r"[A-Za-z]+", " ".join(tokens))
    if english_tokens:
        cooccur = max(0.05, cooccur - 0.1 * len(english_tokens))

    # Graph placeholder: fixed mid value (improve later)
    graph = 0.5

    # Penalty from spam/english
    spam_patterns = ["은은", "을을", "이가", "는가", "때문에"]
    tokens_joined = " ".join(tokens)
    spam_hit = sum(tokens_joined.count(pat) for pat in spam_patterns)
    penalty = min(1.0, 0.1 * len(english_tokens) + 0.05 * spam_hit)

    total = 100.0 * (0.6 * cooccur + 0.4 * graph) - 100.0 * penalty
    total = max(0.0, min(100.0, total))
    return CoherenceScore(total=total, cooccur=cooccur, graph=graph, penalty=penalty, tokens_n=tn)


# --- Correction keyword bundle ---
@dataclass
class KeywordBundle:
    bundle_id: str
    session_id: str
    tokens: List[str]
    source: str = "lmdb"
    created_ts: float = field(default_factory=time.time)
    preview: str = ""


@dataclass
class CoherenceScore:
    total: float
    cooccur: float
    graph: float
    penalty: float
    tokens_n: int
# Dedicated asyncio loop for PyQLE (runs in a background thread alongside gevent).
pyqle_loop = asyncio.new_event_loop()
def _pyqle_loop_worker():
    asyncio.set_event_loop(pyqle_loop)
    pyqle_loop.run_forever()
threading.Thread(target=_pyqle_loop_worker, daemon=True).start()

# --- Debug helper ---
def ws_debug(msg):
    """Lightweight debug logger for websocket lifecycle."""
    line = f"[WS-DEBUG] {msg}"
    try:
        print(line, flush=True)
    except Exception:
        # Fallback for consoles that cannot encode certain characters (e.g. emoji on cp949).
        try:
            safe_line = line.encode("utf-8", "backslashreplace").decode("ascii", "backslashreplace")
            print(safe_line, flush=True)
        except Exception:
            pass
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        log_sink.submit_line(os.path.join(LOG_DIR, "ws_debug.log"), line)
    except Exception:
        # Avoid breaking the socket for logging errors
        pass

@app.before_request
def _log_before_request():
    """Trace every incoming request to diagnose websocket issues."""
    ws_obj = request.environ.get('wsgi.websocket')
    if request.path == "/ws" or ws_obj:
        ws_debug(f"before_request path={request.path} upgrade={request.headers.get('Upgrade')} ws_obj={bool(ws_obj)}")
        # If this is a WebSocket upgrade, bypass normal Flask dispatch and handle here.
        if ws_obj and request.path == "/ws":
            ws_debug("before_request: short-circuiting to ws() handler for WebSocket upgrade")
            return ws()

@app.after_request
def _log_after_request(response):
    if request.path == "/ws" or request.environ.get('wsgi.websocket'):
        ws_debug(f"after_request path={request.path} status={response.status}")
    return response

# --- HTTP API Routes ---

@app.route('/')
def index():
    mode = _resolve_ui_mode()
    return render_template(
        'index.html',
        mode=mode,
        project_name=PROJECT_DISPLAY_NAME,
        project_root=str(PROJECT_ROOT),
        pyqle_available=PYQLE_CORE_AVAILABLE,
    )

@app.route('/api/files')
def list_files():
    return jsonify(scan_dir(str(PROJECT_ROOT)))

@app.route('/api/file', methods=['GET'])
def read_file_route():
    abspath = request.args.get('path', '')
    if not abspath: return jsonify({'ok': False, 'error': 'path required'}), 400
    try:
        with open(abspath, 'r', encoding='utf-8', errors='ignore') as f:
            return jsonify({'ok': True, 'path': abspath, 'content': f.read()})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/file', methods=['POST','PUT'])
def upsert_file():
    data = request.get_json(force=True, silent=True) or {}
    abspath = data.get('path', '')
    content = data.get('content', '')
    if not abspath: return jsonify({'ok': False, 'error': 'path required'}), 400
    try:
        os.makedirs(os.path.dirname(abspath), exist_ok=True)
        write_text(abspath, content)
        return jsonify({'ok': True, 'path': abspath})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/file', methods=['DELETE'])
def remove_file():
    abspath = request.args.get('path', '')
    if not abspath: return jsonify({'ok': False, 'error': 'path required'}), 400
    try:
        delete_path(abspath)
        return jsonify({'ok': True, 'path': abspath})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

def _read_last_jsonl(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if not path.exists():
            return None
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            # read last 8KB or entire file if smaller
            block = min(8192, size)
            f.seek(max(0, size - block), os.SEEK_SET)
            data = f.read().decode("utf-8", "ignore").strip().splitlines()
            for line in reversed(data):
                line = line.strip()
                if not line:
                    continue
                try:
                    return json.loads(line)
                except Exception:
                    continue
    except Exception:
        return None
    return None

@app.route('/api/turn/latest')
def get_latest_turn():
    """Return the latest v2 trace entry."""
    trace_obj = _read_last_jsonl(brain_v2_config.TRACE_LOG)
    if not trace_obj:
        return jsonify({"ok": False, "error": "no trace available"}), 404
    return jsonify({"ok": True, "trace": trace_obj})

@app.route('/api/turn/<trace_id>/feedback', methods=['POST'])
def post_feedback(trace_id):
    """
    Save UI feedback to eval:turn and adjust kw quality.
    feedback: good|bad|ambiguous|alt_angle
    """
    data = request.get_json(force=True, silent=True) or {}
    fb = (data.get("feedback") or "").strip().lower()
    note = data.get("note")
    if fb not in {"good", "bad", "ambiguous", "alt_angle"}:
        return jsonify({"ok": False, "error": "feedback must be one of good,bad,ambiguous,alt_angle"}), 400

    delta_map = {"good": 10, "bad": -15, "ambiguous": -8, "alt_angle": 0}
    delta = delta_map.get(fb, 0)
    now_ts = int(time.time())

    store = _get_write_store()
    try:
        with store.env.begin(write=True) as txn:
            key = f"eval:turn:{trace_id}".encode("utf-8")
            raw = txn.get(key, db=store.db_relations)
            if not raw:
                return jsonify({"ok": False, "error": "eval not found"}), 404
            try:
                ev = _loads(raw)
            except Exception as exc:
                return jsonify({"ok": False, "error": f"eval parse failed: {exc}"}), 500

            base_score = float(ev.get("score", 0.0))
            new_score = max(0.0, min(100.0, base_score + delta))
            tags = list(ev.get("tags") or [])
            tags.append(f"ui:{fb}")
            ev["score"] = new_score
            ev["tags"] = tags
            ev["ui"] = {"feedback": fb, "ts": now_ts, "note": note}
            txn.put(key, _dumps(ev), db=store.db_relations)

            snapshot = ev.get("snapshot") or {}
            seeds = snapshot.get("seeds") or []
            focus = snapshot.get("focus") or []
            words_for_quality = []
            seen_kw = set()
            for kw in list(seeds) + list(focus):
                if not kw:
                    continue
                k = kw.lower() if isinstance(kw, str) and kw.isascii() else kw
                if k in seen_kw:
                    continue
                seen_kw.add(k)
                try:
                    store.update_kw_quality(txn, kw, new_score, now_ts, tags=[f"ui:{fb}"])
                    words_for_quality.append(kw)
                except Exception:
                    continue
    except Exception as exc:
        return jsonify({"ok": False, "error": f"feedback save failed: {exc}"}), 500

    return jsonify({"ok": True, "trace_id": trace_id, "score": new_score, "updated_kw": words_for_quality})

@app.route('/test')
def health_test():
    return jsonify({"ok": True, "message": f"{PROJECT_DISPLAY_NAME} test endpoint is running"}), 200


@app.route('/api/crew/social/config', methods=['GET'])
def crew_social_config():
    try:
        return jsonify({"ok": True, **SOCIAL_AUTOMATION.get_ui_bootstrap()}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/platform/status', methods=['GET'])
def crew_social_platform_status():
    try:
        result = SOCIAL_AUTOMATION.get_platform_status()
        return jsonify(result), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/platform/facebook/config', methods=['POST'])
def crew_social_facebook_config():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.save_facebook_runtime_config(payload)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/platform/facebook/reissue', methods=['POST'])
def crew_social_facebook_reissue():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.reissue_facebook_page_token(payload)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/candidates', methods=['GET'])
def crew_news_collector_candidates():
    limit = request.args.get('limit', default=5, type=int)
    safe_limit = max(1, min(int(limit or 5), 100))
    try:
        batch = NEWS_COLLECTOR_REVIEW_SERVICE.list_candidate_batch(limit=safe_limit)
        return jsonify({"ok": True, "count": len(batch.get("items") or []), **batch}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/feeds', methods=['GET'])
def crew_news_collector_feeds():
    try:
        items = NEWS_COLLECTOR_FEED_MANAGEMENT_SERVICE.list_feeds()
        connected_count = sum(1 for item in items if item.get("enabled"))
        return jsonify({"ok": True, "items": items, "count": len(items), "connected_count": connected_count}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/feeds/add', methods=['POST'])
def crew_news_collector_add_feed():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = NEWS_COLLECTOR_FEED_MANAGEMENT_SERVICE.add_feed(payload)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/feeds/delete', methods=['POST'])
def crew_news_collector_delete_feed():
    payload = request.get_json(force=True, silent=True) or {}
    rss_feed_id = str(payload.get("rss_feed_id", "") or "").strip()
    if not rss_feed_id:
        return jsonify({"ok": False, "error": "rss_feed_id_required"}), 400
    try:
        result = NEWS_COLLECTOR_FEED_MANAGEMENT_SERVICE.delete_feed(rss_feed_id)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/feeds/connection', methods=['POST'])
def crew_news_collector_feed_connection():
    payload = request.get_json(force=True, silent=True) or {}
    rss_feed_id = str(payload.get("rss_feed_id", "") or "").strip()
    if not rss_feed_id:
        return jsonify({"ok": False, "error": "rss_feed_id_required"}), 400
    enabled = bool(payload.get("enabled"))
    try:
        result = NEWS_COLLECTOR_FEED_MANAGEMENT_SERVICE.set_connection(rss_feed_id, enabled)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/collect', methods=['POST'])
def crew_news_collector_collect():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = NEWS_COLLECTOR_COLLECTION_SERVICE.collect_latest(
            item_limit=payload.get("item_limit"),
            recent_hours=payload.get("recent_hours"),
        )
        if result.get("ok"):
            preview_result = TELEGRAM_PREVIEW_CARD_SERVICE.sync_preview_cards(limit=5)
            result["preview_cards_generated"] = int(preview_result.get("generated_count", 0))
            result["preview_cards_count"] = int(TELEGRAM_PREVIEW_CARD_SERVICE.list_preview_cards(limit=5).get("count", 0))
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/preview-cards', methods=['GET'])
def crew_news_collector_preview_cards():
    limit = request.args.get('limit', default=5, type=int)
    safe_limit = max(1, min(int(limit or 5), 50))
    try:
        result = TELEGRAM_PREVIEW_CARD_SERVICE.list_preview_cards(limit=safe_limit)
        return jsonify(result), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/approve', methods=['POST'])
def crew_news_collector_approve():
    payload = request.get_json(force=True, silent=True) or {}
    article_id = str(payload.get("article_id", "") or "").strip()
    if not article_id:
        return jsonify({"ok": False, "error": "article_id_required"}), 400
    try:
        result = NEWS_COLLECTOR_REVIEW_SERVICE.approve_candidate(article_id, payload)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/modify', methods=['POST'])
def crew_news_collector_modify():
    payload = request.get_json(force=True, silent=True) or {}
    article_id = str(payload.get("article_id", "") or "").strip()
    if not article_id:
        return jsonify({"ok": False, "error": "article_id_required"}), 400
    try:
        result = NEWS_COLLECTOR_REVIEW_SERVICE.modify_candidate(article_id, payload)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/reject', methods=['POST'])
def crew_news_collector_reject():
    payload = request.get_json(force=True, silent=True) or {}
    article_id = str(payload.get("article_id", "") or "").strip()
    if not article_id:
        return jsonify({"ok": False, "error": "article_id_required"}), 400
    try:
        result = NEWS_COLLECTOR_REVIEW_SERVICE.reject_candidate(article_id, payload)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/news-collector/drop', methods=['POST'])
def crew_news_collector_drop():
    payload = request.get_json(force=True, silent=True) or {}
    article_id = str(payload.get("article_id", "") or "").strip()
    if not article_id:
        return jsonify({"ok": False, "error": "article_id_required"}), 400
    try:
        result = NEWS_COLLECTOR_REVIEW_SERVICE.drop_candidate(article_id, payload)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/content/generate', methods=['POST'])
def crew_content_generate():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.generate_workconnect_clip(payload)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/content/queue', methods=['GET'])
def crew_content_queue():
    try:
        result = SOCIAL_AUTOMATION.list_content_queue()
        return jsonify(result), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/content/review', methods=['POST'])
def crew_content_review():
    payload = request.get_json(force=True, silent=True) or {}
    clip_id = str(payload.get("clip_id", "") or "").strip()
    decision = str(payload.get("decision", "") or "").strip()
    try:
        result = SOCIAL_AUTOMATION.resolve_content_review(clip_id, decision)
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/x/oauth/status', methods=['GET'])
def crew_social_x_oauth_status():
    try:
        result = SOCIAL_AUTOMATION.get_x_auth_status()
        return jsonify(result), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/x/oauth/authorize', methods=['POST'])
def crew_social_x_oauth_authorize():
    try:
        ws_debug("[AUTH-AUTHORIZE] request received for X OAuth authorize URL")
        result = SOCIAL_AUTOMATION.build_x_authorize_url()
        try:
            ws_debug(
                "[AUTH-AUTHORIZE] result "
                f"ok={bool(result.get('ok'))} "
                f"state_present={bool(str(result.get('state', '')).strip())} "
                f"redirect_uri={str(result.get('redirect_uri', '')).strip()}"
            )
        except Exception:
            pass
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        ws_debug(f"[AUTH-AUTHORIZE] exception type={type(exc).__name__} detail={exc}")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/x/oauth/exchange', methods=['POST'])
def crew_social_x_oauth_exchange():
    payload = request.get_json(force=True, silent=True) or {}
    code = str(payload.get("code", "") or "").strip()
    state = str(payload.get("state", "") or "").strip()
    try:
        ws_debug(
            "[AUTH-EXCHANGE] api exchange request "
            f"code_present={bool(code)} state_present={bool(state)} "
            f"state_suffix={(state[-8:] if len(state) > 8 else state)}"
        )
        result = SOCIAL_AUTOMATION.handle_x_oauth_callback(code=code, state=state)
        ws_debug(
            "[AUTH-EXCHANGE] api exchange result "
            f"ok={bool(result.get('ok'))} "
            f"error={str(result.get('error', '')).strip()} "
            f"status_code={result.get('status_code')}"
        )
        status_code = 200 if result.get("ok") else 400
        return jsonify(result), status_code
    except Exception as exc:
        ws_debug(f"[AUTH-EXCHANGE] exception type={type(exc).__name__} detail={exc}")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/x/oauth/callback', methods=['GET'])
def crew_social_x_oauth_callback():
    callback_started_at = time.time()
    code = str(request.args.get("code", "") or "").strip()
    state = str(request.args.get("state", "") or "").strip()
    try:
        ws_debug(
            f"[AUTH-CALLBACK] hit path={request.path} code_present={bool(code)} "
            f"state_present={bool(state)} state={state}"
        )
    except Exception:
        pass
    callback_logs = [
        "[AUTH] Callback received",
        "[AUTH] Callback phase reached (post-callback)",
        "[AUTH] Redirect callback reached before token exchange",
        f"[AUTH] callback_path={request.path}",
        f"[AUTH] query_state={state}",
        f"[AUTH] query_code_present={bool(code)}",
        f"[AUTH] query_state_present={bool(state)}",
    ]
    error = str(request.args.get("error", "") or "").strip()
    error_description = str(request.args.get("error_description", "") or "").strip()

    def _callback_response(payload: Dict[str, Any], status: int, title: str, message: str) -> Response:
        payload_json = json.dumps(payload, ensure_ascii=False)
        body = (
            "<html><body style='font-family:Arial;background:#0f131a;color:#d8e3f2;padding:24px;'>"
            f"<h3>{title}</h3>"
            f"<p>{message}</p>"
            f"<script>(function(){{"
            f"const payload={payload_json};"
            "try{window.opener&&window.opener.postMessage(payload,'*');}catch(e){}"
            "try{localStorage.setItem('crew_x_oauth_callback_result', JSON.stringify(payload));}catch(e){}"
            "setTimeout(function(){try{window.close();}catch(e){} if(!window.closed){window.location.replace('/');}},300);"
            "})();</script>"
            "</body></html>"
        )
        return Response(body, status=status, mimetype="text/html")

    if error:
        try:
            ws_debug(
                f"[AUTH-CALLBACK] provider_error error={error} "
                f"description={error_description}"
            )
        except Exception:
            pass
        payload = {
            "type": "x_oauth_failed",
            "logs": callback_logs
            + [
                f"[ERROR] OAuth callback error: {error}",
                (f"[ERROR] {error_description}" if error_description else "[ERROR] oauth callback denied"),
            ],
        }
        return _callback_response(
            payload=payload,
            status=400,
            title="X OAuth2 callback failed",
            message=f"Error: {error}" + (f" | {error_description}" if error_description else ""),
        )

    try:
        callback_logs.append("[AUTH] Token exchange dispatch started")
        try:
            ws_debug(
                "[AUTH-CALLBACK] dispatch_handle_x_oauth_callback "
                f"code_present={bool(code)} state_suffix={(state[-8:] if len(state) > 8 else state)}"
            )
        except Exception:
            pass
        result = SOCIAL_AUTOMATION.handle_x_oauth_callback(code=code, state=state)
        elapsed_ms = int((time.time() - callback_started_at) * 1000)
        logs = result.get("logs", []) if isinstance(result.get("logs"), list) else []
        if not result.get("ok"):
            try:
                ws_debug(
                    "[AUTH-CALLBACK] exchange_failed "
                    f"error={str(result.get('error', '')).strip()} "
                    f"status_code={result.get('status_code')} "
                    f"detail={str(result.get('detail', '')).strip()}"
                )
            except Exception:
                pass
            runtime_logs = callback_logs + [str(line) for line in logs]
            runtime_logs.append("[ERROR] token_exchange_failure_post_callback")
            runtime_logs.append(f"[AUTH] callback_to_exchange_elapsed_ms={elapsed_ms}")
            if elapsed_ms > 30000:
                runtime_logs.append("[ERROR] token_exchange_delayed_over_30s")
            payload = {"type": "x_oauth_failed", "logs": runtime_logs}
            err = str(result.get("error", "oauth2_callback_failed"))
            detail = str(result.get("detail", "") or "")
            return _callback_response(
                payload=payload,
                status=400,
                title="X OAuth2 callback failed",
                message=(f"{err}: {detail}" if detail else err),
            )

        runtime_logs = callback_logs + [str(line) for line in logs]
        runtime_logs.append(f"[AUTH] callback_to_exchange_elapsed_ms={elapsed_ms}")
        try:
            ws_debug(
                "[AUTH-CALLBACK] exchange_ok "
                f"elapsed_ms={elapsed_ms} "
                f"token_saved={bool(((result.get('tokens') or {}).get('access_token_saved')))}"
            )
        except Exception:
            pass
        if elapsed_ms > 30000:
            runtime_logs.append("[ERROR] token_exchange_delayed_over_30s")
        payload = {"type": "x_oauth_connected", "logs": runtime_logs}
        return _callback_response(
            payload=payload,
            status=200,
            title="X OAuth2 connected",
            message="Token exchange completed. Closing popup...",
        )
    except Exception as exc:
        try:
            ws_debug(f"[AUTH-CALLBACK] exception type={type(exc).__name__} detail={exc}")
        except Exception:
            pass
        payload = {
            "type": "x_oauth_failed",
            "logs": callback_logs + ["[ERROR] OAuth2 callback exception", f"[ERROR] {str(exc)}"],
        }
        return _callback_response(
            payload=payload,
            status=500,
            title="X OAuth2 callback failed",
            message=f"Exception: {str(exc)}",
        )


@app.route('/api/crew/social/generate', methods=['POST'])
def crew_social_generate():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.generate_x_content(payload)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/collect', methods=['POST'])
def crew_social_collect():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.collect_news(payload)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/review-latest', methods=['POST'])
def crew_social_review_latest():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.review_latest(payload)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/consult-queue', methods=['POST'])
def crew_social_consult_queue():
    try:
        result = SOCIAL_AUTOMATION.consult_queue()
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/publish', methods=['POST'])
def crew_social_publish():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.publish_x_content(payload)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/publish-queue', methods=['GET'])
def crew_social_publish_queue():
    try:
        result = SOCIAL_AUTOMATION.list_publish_queue()
        return jsonify(result), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/publish-queue/add', methods=['POST'])
def crew_social_publish_queue_add():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.add_draft_to_publish_queue(payload)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/publish-queue/approve', methods=['POST'])
def crew_social_publish_queue_approve():
    payload = request.get_json(force=True, silent=True) or {}
    draft_id = str(payload.get("draft_id", "")).strip()
    try:
        result = SOCIAL_AUTOMATION.approve_publish(draft_id)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/publish-queue/cancel', methods=['POST'])
def crew_social_publish_queue_cancel():
    payload = request.get_json(force=True, silent=True) or {}
    draft_id = str(payload.get("draft_id", "")).strip()
    try:
        result = SOCIAL_AUTOMATION.cancel_publish(draft_id)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/publish-queue/weekend', methods=['POST'])
def crew_social_publish_queue_weekend():
    payload = request.get_json(force=True, silent=True) or {}
    draft_id = str(payload.get("draft_id", "")).strip()
    try:
        result = SOCIAL_AUTOMATION.save_for_weekend_article(draft_id)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/draft/reject', methods=['POST'])
def crew_social_draft_reject():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.reject_draft(payload)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/register-published', methods=['POST'])
def crew_social_register_published():
    payload = request.get_json(force=True, silent=True) or {}
    post_id = str(payload.get("post_id", "")).strip()
    created_at = payload.get("created_at")
    try:
        result = SOCIAL_AUTOMATION.register_published_post(post_id=post_id, created_at=created_at)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/monitor/check', methods=['POST'])
def crew_social_monitor_check():
    try:
        result = SOCIAL_AUTOMATION.monitor_current_post()
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/social/monitor/finalize', methods=['POST'])
def crew_social_monitor_finalize():
    try:
        result = SOCIAL_AUTOMATION.before_generate_next_post()
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/growth/collect', methods=['POST'])
def crew_growth_collect():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.collect_growth_candidates(payload)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/growth/pending', methods=['GET'])
def crew_growth_pending():
    try:
        result = SOCIAL_AUTOMATION.list_pending_approvals()
        return jsonify(result), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/telegram/decision', methods=['POST'])
def crew_telegram_decision():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        result = SOCIAL_AUTOMATION.apply_telegram_decision(payload)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/telegram/draft-decision', methods=['POST'])
def crew_telegram_draft_decision():
    payload = request.get_json(force=True, silent=True) or {}
    decision = str(payload.get("decision", "")).strip().lower()
    draft_id = str(payload.get("draft_id", "")).strip()
    try:
        if decision == "approve":
            result = SOCIAL_AUTOMATION.handle_telegram_approve(draft_id)
        elif decision == "reject":
            result = SOCIAL_AUTOMATION.handle_telegram_reject(draft_id)
        elif decision == "save_weekend":
            result = SOCIAL_AUTOMATION.handle_telegram_save_weekend(draft_id)
        elif decision == "modify":
            result = SOCIAL_AUTOMATION.handle_telegram_modify(draft_id, payload)
        else:
            result = {"ok": False, "error": "decision must be approve|reject|save_weekend|modify"}
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/telegram/command', methods=['POST'])
def crew_telegram_command():
    payload = request.get_json(force=True, silent=True) or {}
    text = str(payload.get("text", "")).strip()
    try:
        result = SOCIAL_AUTOMATION.handle_telegram_text_command(text, payload)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/telegram/test', methods=['POST'])
def crew_telegram_test():
    try:
        result = SOCIAL_AUTOMATION.test_telegram_delivery()
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route('/api/crew/telegram/callback', methods=['POST'])
def crew_telegram_callback():
    payload = request.get_json(force=True, silent=True) or {}
    callback_data = str(payload.get("callback_data", "")).strip()
    try:
        result = SOCIAL_AUTOMATION.handle_callback_data(callback_data)
        code = 200 if result.get("ok") else 400
        return jsonify(result), code
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

@app.route('/ask', methods=['POST'])
def ask_endpoint():
    """HTTP endpoint for PyQLE to ask questions via POST."""
    data = request.get_json(force=True, silent=True) or {}
    question = data.get('question', '').strip()
    mode = data.get('mode', 'chat')
    
    if not question:
        return jsonify({'error': 'question is required'}), 400
    
    try:
        # Create a temporary context for this request
        app_cfg, perm_cfg = load_configs()
        perms = merge_permissions(app_cfg, perm_cfg)
        
        temp_context = Context(
            session_id=f"http_{int(time.time())}",
            goal="PyQLE Query",
            permissions=perms if isinstance(perms, dict) else {},
        )
        
        # Dummy emit function for HTTP requests (no WebSocket)
        def dummy_emit(event_type, data):
            print(f"[HTTP-EMIT] {event_type}: {data}")
        
        # Process through route function
        payload = {"question": question, "mode": mode}
        result_context = route(temp_context, payload, emit=dummy_emit)
        
        # Extract answer
        answer = result_context.artifacts.get("final", "(No answer received)")
        
        return jsonify({'answer': str(answer)}), 200
        
    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[/ask ERROR] {error_msg}")
        return jsonify({'error': str(e)}), 500

# --- WebSocket Handler ---

@app.route('/ws')
def ws():
    ws_debug("Client connected (HTTP upgrade attempt)")
    ws = request.environ.get('wsgi.websocket')
    if ws is None:
        ws_debug("Client failed: wsgi.websocket is missing (no upgrade)")
        return jsonify({'ok': False, 'error': 'WebSocket upgrade required'}), 400
    ws_debug("Client upgraded to WebSocket successfully.")
    active_job = None

    try:
        # Initialize context for the session
        app_cfg, perm_cfg = load_configs()
        perms = merge_permissions(app_cfg, perm_cfg)
        roots = perms.get("file_roots", [])
        if isinstance(roots, list) and roots:
            set_allowed_roots(roots)
        
        ws_context = Context(
            session_id=uuid.uuid4().hex[:8],
            goal="Personal Copilot",
            permissions=perms if isinstance(perms, dict) else {},
        )
    except Exception as e:
        ws_debug(f"[WS-SETUP-ERROR] Failed to initialize session: {e}")
        raise

    def emit_log(event_type, data):
        ws_debug(f"[WS-EMIT] Type: {event_type}, Data: {data}")
        try:
            if not ws.closed:
                ws.send(json.dumps({"type": event_type, "data": data}))
        except Exception as e:
            ws_debug(f"[WS-ERROR] Failed to send message: {e}")

    configure_pyqle_logger(emit_fn=emit_log, debug_fn=ws_debug)

    def _meditation_log(text: str):
        emit_log("log", {"event": "meditation_v3", "text": text})

    class _WSLogHandler(logging.Handler):
        def __init__(self, session_id: str):
            super().__init__()
            self.session_id = session_id

        def emit(self, record: logging.LogRecord) -> None:
            try:
                msg = self.format(record)
                emit_log("log", {"event": "meditation_v3", "session": self.session_id, "text": msg})
            except Exception:
                pass

    def _stop_meditation_v3(session_id: str, reason: str = "manual_cancel"):
        with meditation_v3_lock:
            state = meditation_loops.get(session_id)
            if not state:
                return
            stop_event = state.get("cancel")
            thread = state.get("thread")
        if stop_event:
            try:
                stop_event.set()
            except Exception:
                pass
        if thread and thread.is_alive():
            thread.join(timeout=1.0)
        with meditation_v3_lock:
            meditation_loops.pop(session_id, None)
        _meditation_log(f"[v3][{session_id}] stop requested ({reason})")

    def _start_meditation_v3(session_id: str, max_steps=None, sleep_sec: float = 0.3, recent_text: str = ""):
        _stop_meditation_v3(session_id, reason="restart")
        cancel_event = threading.Event()

        class LmdbKeywordStore:
            def sample_random_keywords(self, k: int):
                keys = fetch_lmdb_keywords(top_k=300)
                random.shuffle(keys)
                return keys[:k]

            def _load_dict(self, txn, key: bytes):
                store = _get_write_store()
                try:
                    raw = txn.get(key, db=store.db_relations)  # type: ignore[attr-defined]
                except Exception:
                    raw = None
                if not raw:
                    return {}
                try:
                    obj = _loads(raw)
                    if isinstance(obj, dict):
                        return obj
                except Exception:
                    pass
                return {}

            def _store_dict(self, txn, key: bytes, data: dict) -> None:
                store = _get_write_store()
                try:
                    txn.put(key, _dumps(data), db=store.db_relations)  # type: ignore[attr-defined]
                except Exception:
                    pass

            def _update_touched(self, txn, kw_norm: str, day_id: int) -> None:
                store = _get_write_store()
                touched_key = f"tmp:touched:{day_id}".encode("utf-8")
                touched = self._load_dict(txn, touched_key)
                touched[kw_norm] = 1
                self._store_dict(txn, touched_key, touched)
                meta_key = f"tmp:meta:kw:{kw_norm}:last_ts".encode("utf-8")
                try:
                    txn.put(meta_key, _dumps(time.time()), db=store.db_relations)  # type: ignore[attr-defined]
                except Exception:
                    pass

            def increment_edge(self, a: str, b: str, delta: float = 1.0):
                return self.increment_edge_layer(a, b, delta, layer="main")

            def increment_edge_layer(self, a: str, b: str, delta: float = 1.0, layer: str = "main"):
                store = _get_write_store()
                kw_a = _norm_kw(a)
                kw_b = _norm_kw(b)
                if not kw_a or not kw_b or kw_a == kw_b:
                    return
                prefix = "out:kw" if layer == "main" else "tmp:out:kw"
                now_ts = time.time()
                day_id = int(now_ts // 86400)
                with store.env.begin(write=True) as txn:  # type: ignore[attr-defined]
                    for src, dst in ((kw_a, kw_b), (kw_b, kw_a)):
                        key = f"{prefix}:{src}:co_occurs".encode("utf-8")
                        data = self._load_dict(txn, key)
                        data[dst] = float(data.get(dst, 0.0)) + float(delta)
                        self._store_dict(txn, key, data)
                        if layer == "temp":
                            self._update_touched(txn, src, day_id)

            def fetch_neighbors(self, kw: str, top_n: int, include_temp: bool = True, temp_weight: float = 0.3):
                store = _get_write_store()
                kw_norm = _norm_kw(kw)
                key_main = f"out:kw:{kw_norm}:co_occurs".encode("utf-8")
                key_temp = f"tmp:out:kw:{kw_norm}:co_occurs".encode("utf-8")
                combined = {}
                try:
                    with store.env.begin(write=False) as txn:  # type: ignore[attr-defined]
                        main_items = self._load_dict(txn, key_main)
                        temp_items = self._load_dict(txn, key_temp) if include_temp else {}
                        for other, cnt in main_items.items():
                            combined[other] = combined.get(other, 0.0) + float(cnt)
                        if include_temp:
                            for other, cnt in temp_items.items():
                                combined[other] = combined.get(other, 0.0) + float(cnt) * float(temp_weight)
                except Exception:
                    combined = {}
                pairs = []
                for other, val in combined.items():
                    if other is None:
                        continue
                    ok = other.decode("utf-8") if isinstance(other, (bytes, bytearray)) else str(other)
                    pairs.append((ok, float(val)))
                pairs.sort(key=lambda x: x[1], reverse=True)
                return pairs[:top_n]

            def temp_decay_and_purge(self, now_ts: float, temp_decay: float = 0.98, ttl_days: int = 7, batch_limit: int = 5000):
                store = _get_write_store()
                processed = 0
                ttl_sec = ttl_days * 86400
                day_id = int(now_ts // 86400)
                allowed_days = {day_id, day_id - 1}
                with store.env.begin(write=True) as txn:  # type: ignore[attr-defined]
                    cur = txn.cursor(db=store.db_relations)  # type: ignore[attr-defined]
                    for key, _val in cur:
                        if not key.startswith(b"tmp:touched:"):
                            continue
                        try:
                            touched = self._load_dict(txn, key)
                            day_id = int(key.decode("utf-8").split(":")[-1])
                        except Exception:
                            touched = {}
                            day_id = None
                        if allowed_days and day_id is not None and day_id not in allowed_days:
                            continue
                        for kw_norm in list(touched.keys()):
                            if processed >= batch_limit:
                                return
                            processed += 1
                            meta_key = f"tmp:meta:kw:{kw_norm}:last_ts".encode("utf-8")
                            last_raw = txn.get(meta_key, db=store.db_relations)  # type: ignore[attr-defined]
                            last_ts = 0.0
                            if last_raw:
                                try:
                                    last_ts = float(_loads(last_raw))
                                except Exception:
                                    last_ts = 0.0
                            temp_key = f"tmp:out:kw:{kw_norm}:co_occurs".encode("utf-8")
                            data = self._load_dict(txn, temp_key)
                            if not data:
                                continue
                            if ttl_sec and now_ts - last_ts > ttl_sec:
                                txn.delete(temp_key, db=store.db_relations)  # type: ignore[attr-defined]
                                txn.delete(meta_key, db=store.db_relations)  # type: ignore[attr-defined]
                                touched.pop(kw_norm, None)
                                continue
                            for k2 in list(data.keys()):
                                data[k2] = float(data.get(k2, 0.0)) * float(temp_decay)
                                if data[k2] <= 0.0:
                                    data.pop(k2, None)
                            self._store_dict(txn, temp_key, data)
                        # cleanup touched key if empty
                        if not touched:
                            try:
                                txn.delete(key, db=store.db_relations)  # type: ignore[attr-defined]
                            except Exception:
                                pass
                        else:
                            self._store_dict(txn, key, touched)

            def temp_promote(self, now_ts: float, threshold: float = 5.0, batch_limit: int = 500):
                store = _get_write_store()
                promoted = 0
                day_id = int(now_ts // 86400)
                with store.env.begin(write=True) as txn:  # type: ignore[attr-defined]
                    cur = txn.cursor(db=store.db_relations)  # type: ignore[attr-defined]
                    for key, _val in cur:
                        if not key.startswith(b"tmp:touched:"):
                            continue
                        touched = self._load_dict(txn, key)
                        try:
                            touched_day = int(key.decode("utf-8").split(":")[-1])
                        except Exception:
                            touched_day = None
                        if touched_day is not None and touched_day not in {day_id, day_id - 1}:
                            continue
                        for kw_norm in list(touched.keys()):
                            if promoted >= batch_limit:
                                return
                            temp_key = f"tmp:out:kw:{kw_norm}:co_occurs".encode("utf-8")
                            data = self._load_dict(txn, temp_key)
                            if not data:
                                continue
                            for other, cnt in data.items():
                                if float(cnt) >= threshold and other and other != kw_norm:
                                    promoted += 1
                                    main_key = f"out:kw:{kw_norm}:co_occurs".encode("utf-8")
                                    main_data = self._load_dict(txn, main_key)
                                    main_data[other] = float(main_data.get(other, 0.0)) + float(cnt)
                                    self._store_dict(txn, main_key, main_data)
                                    # symmetric update
                                    other_key = f"out:kw:{other}:co_occurs".encode("utf-8")
                                    other_data = self._load_dict(txn, other_key)
                                    other_data[kw_norm] = float(other_data.get(kw_norm, 0.0)) + float(cnt)
                                    self._store_dict(txn, other_key, other_data)
                                    data.pop(other, None)
                            if not data:
                                try:
                                    txn.delete(temp_key, db=store.db_relations)  # type: ignore[attr-defined]
                                except Exception:
                                    pass
                                meta_key = f"tmp:meta:kw:{kw_norm}:last_ts".encode("utf-8")
                                try:
                                    txn.delete(meta_key, db=store.db_relations)  # type: ignore[attr-defined]
                                except Exception:
                                    pass
                                touched.pop(kw_norm, None)
                        if not touched:
                            try:
                                txn.delete(key, db=store.db_relations)  # type: ignore[attr-defined]
                            except Exception:
                                pass
                        else:
                            self._store_dict(txn, key, touched)

        def _runner():
            handler = _WSLogHandler(session_id)
            logger = logging.getLogger(f"meditation_v3.{session_id}")
            logger.propagate = False
            logger.setLevel(logging.INFO)
            logger.handlers.clear()
            logger.addHandler(handler)
            _meditation_log(f"[v3][{session_id}] start (max_steps={max_steps}, sleep_sec={sleep_sec})")
            try:
                params = MeditationParams()
                run_meditation_loop(
                    max_steps=max_steps,
                    sleep_sec=sleep_sec,
                    store=LmdbKeywordStore(),
                    params=params,
                    stop_event=cancel_event,
                    logger=logger,
                )
            except Exception as exc:
                _meditation_log(f"[v3][{session_id}] error: {exc}")
            finally:
                logger.removeHandler(handler)
                with meditation_v3_lock:
                    meditation_loops.pop(session_id, None)
                _meditation_log(f"[v3][{session_id}] stopped")

        t = threading.Thread(target=_runner, name=f"meditation_v3_{session_id}", daemon=True)
        with meditation_v3_lock:
            meditation_loops[session_id] = {"cancel": cancel_event, "thread": t, "steps": 0}
        t.start()

    def _stop_v1_loop(session_id: str, reason: str = "manual_cancel"):
        state = session_loops.get(session_id)
        if not state:
            return
        try:
            cancel_event = state.get("cancel")
            task = state.get("task")
            fut = state.get("fut")
            if cancel_event and hasattr(cancel_event, "set"):
                try:
                    cancel_event.set()
                except Exception:
                    pass
            if task and hasattr(task, "cancel"):
                try:
                    task.cancel()
                except Exception:
                    pass
            if fut:
                try:
                    fut.cancel()
                except Exception:
                    pass
            pyqle_log("info", "loop_stop", {"stage": "loop_stop", "session_id": session_id, "reason": reason, "steps": state.get("steps", 0)})
        finally:
            session_loops.pop(session_id, None)

    def _start_v1_loop(session_id: str, max_steps: int = 3):
        import re

        _stop_v1_loop(session_id, reason="restart")
        cancel_event = asyncio.Event()

        async def runner():
            t0 = time.perf_counter()
            steps_seen = set()
            state = {"cancel": cancel_event, "steps": 0}
            session_loops[session_id] = state
            pyqle_log("info", "loop_start", {"stage": "loop_start", "session_id": session_id, "max_steps": max_steps})

            def emit_wrapper(evt: str, data: Any):
                emit_log(evt, data)
                try:
                    if evt == "log" and isinstance(data, dict):
                        txt = str(data.get("text", ""))
                        m = re.search(r"loop=(\d+)", txt)
                        if m:
                            n = int(m.group(1))
                            if n not in steps_seen:
                                steps_seen.add(n)
                                state["steps"] = n
                                pyqle_log("info", "loop_step", {"stage": "loop_step", "session_id": session_id, "n": n})
                    if cancel_event.is_set():
                        raise asyncio.CancelledError()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass

            task = asyncio.create_task(run_pyqle_loop(emit_wrapper))
            state["task"] = task
            try:
                while not task.done():
                    if cancel_event.is_set():
                        task.cancel()
                        break
                    if state.get("steps", 0) >= max_steps:
                        cancel_event.set()
                        task.cancel()
                        break
                    await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                pass
            finally:
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                reason = "max_steps" if state.get("steps", 0) >= max_steps else ("manual_cancel" if cancel_event.is_set() else "task_done")
                pyqle_log("info", "loop_stop", {"stage": "loop_stop", "session_id": session_id, "reason": reason, "steps": state.get("steps", 0)})
                pyqle_log("info", "loop_end", {"stage": "loop_end", "session_id": session_id, "total_steps": state.get("steps", 0), "elapsed_ms": elapsed_ms})
                session_loops.pop(session_id, None)

        fut = asyncio.run_coroutine_threadsafe(runner(), pyqle_loop)
        session_loops[session_id] = {"cancel": cancel_event, "fut": fut, "task": None, "steps": 0}

    try:
        # Main message loop
        ws_debug("Entering main message loop...")
        while not ws.closed:
            data = None
            try:
                ws_debug("Waiting for message...")
                data = ws.receive()
                
                if data is None:
                    ws_debug("receive() returned None (socket is likely closing).")
                    break

                ws_debug(f"Received raw data: {data}")
                payload = json.loads(data)
                msg_type = payload.get("type")
                session_id_for_log = getattr(ws_context, "session_id", "unknown")
                # Ensure question is available for any cmd handling below
                question = payload.get("question", "").strip()
                raw_input = payload.get("question", "") or ""

                if msg_type == "ping":
                    try:
                        ws.send(json.dumps({
                            "type": "pong",
                            "data": {
                                "ts": payload.get("ts"),
                                "server_time": time.time()
                            }
                        }))
                    except Exception as e:
                        ws_debug(f"[WS-HEARTBEAT-ERROR] Failed to send pong: {e}")
                        break
                    continue

                cmd = payload.get("cmd")
                if cmd == "meditation_v3_start":
                    try:
                        max_steps_val = payload.get("max_steps")
                        max_steps = int(max_steps_val) if max_steps_val not in (None, "") else None
                    except Exception:
                        max_steps = None
                    try:
                        sleep_val = payload.get("sleep_sec", 0.3)
                        sleep_sec = float(sleep_val)
                    except Exception:
                        sleep_sec = 0.3
                    _start_meditation_v3(session_id_for_log, max_steps=max_steps, sleep_sec=sleep_sec, recent_text=question)
                    continue
                if cmd == "meditation_v3_stop":
                    _stop_meditation_v3(session_id_for_log, reason="command")
                    continue
                ws_debug(f"Received payload after JSON parse: {payload}")
                pyqle_log("info", "incoming user message", {"stage": "recv", "session_id": session_id_for_log, "input_len": len(question)})

                if question.strip() == "/metrics":
                    summary = summarize_metrics()
                    emit_log("agent_chat", {"agent": "System", "message": summary})
                    continue

                if question == "/pyqle":
                    if not PYQLE_CORE_AVAILABLE:
                        emit_log(
                            'agent_chat',
                            {
                                'agent': 'System',
                                'message': f'PyQLE is unavailable in this copy. Missing dependency: {PYQLE_CORE_IMPORT_ERROR}',
                            },
                        )
                        continue
                    _start_v1_loop(session_id_for_log, max_steps=3)
                    emit_log('agent_chat', {'agent': 'System', 'message': 'Starting PyQLE autonomous loop...'})
                    continue

                # For normal messages, run route in a background greenlet so pings keep flowing.
                if active_job and not active_job.ready():
                    emit_log('agent_chat', {'agent': 'System', 'message': 'Previous request is still running. Please wait.'})
                    continue

                # Any new input cancels an active v1 loop for this session
                _stop_v1_loop(session_id_for_log, reason="user_input")

                def _run_payload(p):
                    nonlocal ws_context
                    try:
                        mode = (p.get("mode") or "v1").lower()
                        opt = p.get("options") or {}
                        question_raw = p.get("question", "")
                        loop_on = bool(p.get("loop", False))
                        pyqle_log("info", "route decision start", {"stage": "mode_decide", "session_id": getattr(ws_context, "session_id", "unknown")})

                        # Question mode: delegate to the v1 path.
                        if mode == "question":
                            if loop_on:
                                # Start the v1 autonomous loop (session_id scoped, single run).
                                _start_v1_loop(session_id_for_log, max_steps=3)
                                emit_log('agent_chat', {'agent': 'System', 'message': 'Starting PyQLE autonomous loop...'})
                                return
                            else:
                                _stop_v1_loop(session_id_for_log, reason="user_input")
                                ctx = route(ws_context, p, emit=emit_log)
                                ws_context = ctx
                                final_result_obj = ctx.artifacts.get("final", "(No result)")
                                ws.send(json.dumps({"type": "final_result", "data": str(final_result_obj)}))
                                return
                        if mode == "learn":
                            # Cancel any other loop
                            _stop_v1_loop(session_id_for_log, reason="user_input")
                            learn_jobs[session_id_for_log] = {"cancel": False}
                            if loop_on:
                                pyqle_log("info", "learn_start", {"stage": "learn_start", "session_id": session_id_for_log, "loop": True})
                                _run_learn(session_id_for_log, loop_on=True)
                            else:
                                pyqle_log("info", "learn_start", {"stage": "learn_start", "session_id": session_id_for_log, "loop": False})
                                _run_learn(session_id_for_log, loop_on=False)
                            return

                        # PyQle family modes (chat / correction / pyqle).
                        pyqle_modes = {"pyqle", "chat", "correction"}
                        if mode not in pyqle_modes:
                            ctx = route(ws_context, p, emit=emit_log)
                            ws_context = ctx
                            final_result_obj = ctx.artifacts.get("final", "(No result)")
                            ws.send(json.dumps({"type": "final_result", "data": str(final_result_obj)}))
                            return

                        # When invoked through the /pyqle command, extract options and body text.
                        clean_text, cmd_opt = parse_pyqle_cmd(question_raw)
                        if question_raw.strip().startswith("/pyqle"):
                            opt = cmd_opt or opt
                            question_raw = clean_text
                        clean_text = clean_text or question_raw

                        if not PYQLE_CORE_AVAILABLE:
                            if question_raw.strip().startswith("/pyqle") or mode in {"pyqle", "correction"}:
                                emit_log(
                                    'agent_chat',
                                    {
                                        'agent': 'System',
                                        'message': f'PyQLE is unavailable in this copy. Missing dependency: {PYQLE_CORE_IMPORT_ERROR}',
                                    },
                                )
                                return
                            if mode == "chat":
                                ctx = route(ws_context, {**p, "question": clean_text, "mode": "chat"}, emit=emit_log)
                                ws_context = ctx
                                final_result_obj = ctx.artifacts.get("final", "(No result)")
                                ws.send(json.dumps({"type": "final_result", "data": str(final_result_obj)}))
                                return

                        orig_cmd = (p.get("question", "") or "").strip()
                        if orig_cmd.startswith("/pyqle meditation"):
                            text_lower = orig_cmd.lower()
                            if " stop" in text_lower:
                                _stop_meditation_v3(session_id_for_log, reason="user_cmd")
                                emit_log('agent_chat', {'agent': 'System', 'message': 'Meditation v3 stopping...'})
                                return
                            # parse steps/sleep
                            steps = 500
                            sleep_val = 0.3
                            m_steps = re.search(r"steps\s*=\s*(\d+)", orig_cmd, re.IGNORECASE)
                            if m_steps:
                                try:
                                    steps = int(m_steps.group(1))
                                except Exception:
                                    steps = 500
                            m_sleep = re.search(r"sleep\s*=\s*([0-9.]+)", orig_cmd, re.IGNORECASE)
                            if m_sleep:
                                try:
                                    sleep_val = float(m_sleep.group(1))
                                except Exception:
                                    sleep_val = 0.3
                            _start_meditation_v3(session_id_for_log, max_steps=steps, sleep_sec=sleep_val, recent_text=question_raw)
                            emit_log('agent_chat', {'agent': 'System', 'message': f'Meditation v3 started (steps={steps}, sleep={sleep_val})'})
                            return

                        llama = bool(opt.get("llama", True))
                        search = bool(opt.get("search", False))
                        multi = bool(opt.get("multi", False))
                        return_all = bool(opt.get("return_all", False))

                        is_correction = (mode == "correction" and llama and not multi)
                        pyqle_log(
                            "info",
                            "mode resolved",
                            {
                                "stage": "mode_decide",
                                "session_id": getattr(ws_context, "session_id", "unknown"),
                                "mode": mode,
                                "correction": bool(is_correction),
                                "loop_enabled": loop_on,
                            },
                        )
                        if is_correction:
                            session_id = getattr(ws_context, "session_id", "unknown")
                            run_id = str(uuid.uuid4())
                            prev = correction_inflight.get(session_id)
                            if prev:
                                pyqle_log("info", "cancelled", {"stage": "cancelled", "session_id": session_id, "reason": "superseded_correction"})
                            correction_inflight[session_id] = run_id
                            pyqle_log("info", "bundle_mode", {"stage": "bundle_mode", "session_id": session_id})
                            keywords = fetch_lmdb_keywords(top_k=12)
                            pyqle_log("info", "lmdb_keywords", {"stage": "lmdb_keywords", "session_id": session_id, "keys_n": len(keywords), "top_preview": keywords[:3]})
                            # Coherence improvement loop
                            top_bundles, best_score = coherence_improve_loop(keywords, session_id=session_id, max_steps=5)
                            pyqle_log(
                                "info",
                                "coherence_scored",
                                {
                                    "stage": "coherence_scored",
                                    "session_id": session_id,
                                    "best_total": round(best_score.total, 2),
                                    "best_breakdown": {
                                        "cooccur": round(best_score.cooccur, 3),
                                        "graph": round(best_score.graph, 3),
                                        "penalty": round(best_score.penalty, 3),
                                    },
                                    "selected_n": len(top_bundles),
                                    "top_preview": [b.preview for b in top_bundles[:2]],
                                },
                            )

                            branch = "A"
                            llm_mode = "generate"
                            if best_score.total >= 90:
                                branch = "C"
                                llm_mode = "skip"
                            elif best_score.total >= 60:
                                branch = "B"
                                llm_mode = "reframe"

                            pyqle_log(
                                "info",
                                "policy_branch",
                                {
                                    "stage": "policy_branch",
                                    "session_id": session_id,
                                    "best_total": round(best_score.total, 2),
                                    "branch": branch,
                                    "llm_mode": llm_mode,
                                    "selected_n": len(top_bundles),
                                    "top_preview": [b.preview for b in top_bundles[:2]],
                                },
                            )

                            model_name = "crew_ai_correction"
                            t0 = time.perf_counter()
                            corrected = ""
                            result = None
                            elapsed_ms = 0

                            if llm_mode in ("generate", "reframe"):
                                pyqle_log("info", "llm_call_start", {"stage": "llm_call_start", "session_id": session_id, "model": model_name})
                                try:
                                    result = crew_ai.correct_text([b.preview for b in top_bundles], session_id=session_id, model=model_name, mode=llm_mode)
                                    corrected = result.text
                                    elapsed_ms = int((time.perf_counter() - t0) * 1000)
                                    pyqle_log("info", "llm_call_end", {"stage": "llm_call_end", "session_id": session_id, "model": model_name, "elapsed_ms": elapsed_ms})
                                    record_metric(latency_ms=elapsed_ms, error=False, fallback=bool(getattr(result, "is_fallback", False)))
                                except Exception as e:
                                    import traceback
                                    tb = traceback.format_exc().splitlines()[:10]
                                    pyqle_log("error", f"correction llm failed: {e}", {"stage": "error", "session_id": session_id, "where": "correction_llm", "err": str(e), "traceback_head": "\n".join(tb)})
                                    fallback_preview = top_bundles[0].preview if top_bundles else "이 문장 은 띄어쓰기 가 이상 하다"
                                    corrected = fallback_preview
                                    result = crew_ai.CorrectionResult(text=corrected, llm_used=False, is_fallback=True, model=model_name, endpoint="fallback://correction_exception", elapsed_ms=int((time.perf_counter() - t0) * 1000))
                                    elapsed_ms = result.elapsed_ms
                                    record_metric(latency_ms=elapsed_ms, error=True, fallback=True)
                            else:
                                # skip mode: no LLM call, simple cleanup of best preview
                                raw = top_bundles[0].preview if top_bundles else ""
                                corrected = " ".join(raw.split()).strip()
                                if corrected and corrected[-1] not in ".!?":
                                    corrected += "."
                                result = crew_ai.CorrectionResult(text=corrected, llm_used=False, is_fallback=True, model=model_name, endpoint="skip://high_coherence", elapsed_ms=int((time.perf_counter() - t0) * 1000))
                                elapsed_ms = result.elapsed_ms
                                record_metric(latency_ms=elapsed_ms, error=False, fallback=False)

                            if correction_inflight.get(session_id) != run_id:
                                pyqle_log("info", "cancelled", {"stage": "cancelled", "session_id": session_id, "reason": "superseded_result_ignored"})
                                correction_inflight.pop(session_id, None)
                                return

                            if getattr(result, "llm_used", False) and not getattr(result, "is_fallback", False):
                                pyqle_log("info", "writeback_apply", {"stage": "writeback_apply", "session_id": session_id, "model": result.model, "reason": "llm_ok"})
                            else:
                                skip_reason = "skip_high_coherence" if llm_mode == "skip" else "fallback_or_stub"
                                pyqle_log("info", "writeback_skip", {"stage": "writeback_skip", "session_id": session_id, "reason": skip_reason, "llm_used": getattr(result, "llm_used", False), "is_fallback": getattr(result, "is_fallback", False)})

                            pyqle_log("info", "runner_done", {"stage": "runner_done", "session_id": session_id, "elapsed_ms": elapsed_ms, "summary_preview": str(corrected)[:120]})
                            sentence_id = str(uuid.uuid4())[:8]
                            selected_tokens = []
                            for b in top_bundles:
                                selected_tokens.extend(b.tokens)
                            selected_tokens = list(dict.fromkeys(selected_tokens))
                            _save_provenance(
                                sentence_id=sentence_id,
                                session_id=session_id,
                                branch=branch,
                                llm_mode=llm_mode,
                                best_total=best_score.total,
                                bundles=top_bundles,
                                corrected=corrected,
                                result=result,
                                used_tokens=selected_tokens,
                            )
                            _update_keyword_stats(selected_tokens)
                            _strengthen_edges(selected_tokens, best_score.total)
                            ws.send(
                                json.dumps(
                                    {
                                        "type": "final_result",
                                        "data": {
                                            "message": corrected,
                                            "meta": {
                                            "mode": "correction",
                                                "candidates": [b.preview for b in top_bundles],
                                                "model": getattr(result, "model", model_name),
                                                "llm_used": getattr(result, "llm_used", False),
                                                "is_fallback": getattr(result, "is_fallback", False),
                                            },
                                        },
                                    }
                                )
                            )
                            correction_inflight.pop(session_id, None)
                            return

                        # Per-mode defaults: chat starts with llama OFF, the rest start with it ON.
                        if mode == "chat":
                            opt.setdefault("llama", False)
                        else:
                            opt.setdefault("llama", True)

                        # Recompute llama/multi and related flags after applying option defaults.
                        llama = bool(opt.get("llama", True))
                        search = bool(opt.get("search", False))
                        multi = bool(opt.get("multi", False))
                        return_all = bool(opt.get("return_all", False))

                        from pyqle_core.brain.v2 import BrainV2, TurnInput, MultiBrainRunner
                        turn = TurnInput(text=clean_text)
                        ws_debug(f"[PYQLE_FLAGS] llama={llama} search={search} multi={multi} return_all={return_all} text={clean_text[:50]!r}")
                        pyqle_log("info", f"[FLAGS] mode={mode} llama={llama} search={search} multi={multi} return_all={return_all}")
                        print("[PYQLE_FLAGS]", llama, search, multi, return_all)
                        print("[PYQLE_TEXT]", repr(clean_text))

                        if multi:
                            brains = [
                                BrainV2(enable_llama=llama, enable_search=search, enable_writeback=True, enable_eval=True),
                                BrainV2(enable_llama=llama, enable_search=search, enable_writeback=True, enable_eval=True),
                                BrainV2(enable_llama=llama, enable_search=search, enable_writeback=True, enable_eval=True),
                            ]
                            runner = MultiBrainRunner(brains=brains, mode="return_all" if return_all else "pick_best")
                            t_llm = time.perf_counter()
                            pyqle_log("info", "llm_call_start", {"stage": "llm_call_start", "session_id": getattr(ws_context, "session_id", "unknown"), "model": "BrainV2-llama", "loop_enabled": loop_on})
                            res = runner.respond(turn)
                            elapsed_ms = int((time.perf_counter() - t_llm) * 1000)
                            pyqle_log("info", "llm_call_end", {"stage": "llm_call_end", "session_id": getattr(ws_context, "session_id", "unknown"), "model": "BrainV2-llama", "elapsed_ms": elapsed_ms})
                            best = res.get("best")
                            msg = best.get("reply") if isinstance(best, dict) else ""
                            pyqle_log("info", "runner_done", {"stage": "runner_done", "session_id": getattr(ws_context, "session_id", "unknown"), "elapsed_ms": elapsed_ms, "summary_preview": str(msg)[:120]})
                            ws.send(json.dumps({"type": "final_result", "data": {"message": msg, "candidates": res.get("candidates"), "best_idx": res.get("best_idx")}}))
                        else:
                            brain = BrainV2(enable_llama=llama, enable_search=search, enable_writeback=True, enable_eval=True)
                            t_llm = time.perf_counter()
                            pyqle_log("info", "llm_call_start", {"stage": "llm_call_start", "session_id": getattr(ws_context, "session_id", "unknown"), "model": "BrainV2-llama", "loop_enabled": loop_on})
                            out = brain.respond(turn)
                            elapsed_ms = int((time.perf_counter() - t_llm) * 1000)
                            pyqle_log("info", "llm_call_end", {"stage": "llm_call_end", "session_id": getattr(ws_context, "session_id", "unknown"), "model": "BrainV2-llama", "elapsed_ms": elapsed_ms})
                            pyqle_log("info", "runner_done", {"stage": "runner_done", "session_id": getattr(ws_context, "session_id", "unknown"), "elapsed_ms": elapsed_ms, "summary_preview": str(out.reply)[:120]})
                            ws.send(json.dumps({"type": "final_result", "data": {"message": out.reply}}))
                    except Exception as e:
                        ws_debug(f"[WS-HANDLER-ERROR] Background run failed: {e}")
                        emit_log('agent_chat', {'agent': 'System', 'message': f'Server error: {e}'})

                active_job = gevent.spawn(_run_payload, payload)

            except WebSocketError:
                ws_debug("[WS-HANDLER-ERROR] WebSocket connection closed by client.")
                break
            except Exception as e:
                ws_debug(f"[WS-HANDLER-ERROR] An error occurred: {e}")
                emit_log('agent_chat', {'agent': 'System', 'message': f'Server error: {e}'})
                try:
                    import traceback
                    tb = traceback.format_exc().splitlines()[:10]
                    pyqle_log("error", f"handler error: {e}", {"stage": "error", "session_id": getattr(ws_context, "session_id", "unknown"), "where": "ws_main_loop", "err": str(e), "traceback_head": "\n".join(tb)})
                except Exception:
                    pass
                # In case of severe errors, we might break, but for now, we continue
                # break
    finally:
        ws_debug("Main message loop has exited.")
        ws_debug(f"Socket closed state: {ws.closed}")
        ws_debug("Connection handler terminated.")
        # Tear down any active PyQLE loop when the socket closes so the next connection can start fresh.
        try:
            _stop_v1_loop(getattr(ws_context, "session_id", "unknown"), reason="ws_disconnect")
        except Exception as e:
            ws_debug(f"[WS-PYQLE-CANCEL-ERROR] {e}")
        _kill_llama_processes()
    
    ws_debug("ws function is returning. This will close the socket.")
    return "WebSocket session ended."

# --- Utility & Startup ---

def scan_dir(path='.'):
    result = []
    try:
        for entry in os.scandir(path):
            if entry.name.startswith('.') or entry.name == '__pycache__': continue
            if entry.is_dir():
                children = scan_dir(entry.path)
                if children: result.append({'name': entry.name, 'type': 'directory', 'children': children})
            else:
                result.append({'name': entry.name, 'type': 'file'})
    except OSError:
        return []
    return result


def _create_http_server_with_retry(host: str, port: int, *, attempts: int = 12, delay_sec: float = 0.5):
    last_error = None
    for attempt in range(1, max(1, int(attempts)) + 1):
        try:
            return WSGIServer((host, port), app, handler_class=WebSocketHandler)
        except OSError as exc:
            last_error = exc
            if getattr(exc, "winerror", None) == 10048 and attempt < attempts:
                print(f"[SYSTEM] Port {port} still busy, retrying ({attempt}/{attempts})...", flush=True)
                time.sleep(max(0.1, float(delay_sec)))
                continue
            raise
    if last_error is not None:
        raise last_error


def _resolve_server_host() -> str:
    return str(os.getenv("APP_HOST", "0.0.0.0")).strip() or "0.0.0.0"


def _resolve_server_port(default: int = 8080) -> int:
    raw = str(os.getenv("APP_PORT", os.getenv("PORT", str(default)))).strip()
    try:
        port = int(raw)
    except Exception:
        port = int(default)
    return max(1, min(65535, port))


def _is_port_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            return True
        except OSError:
            return False

if __name__ == '__main__':
    boot_trace("enter __main__")
    host = _resolve_server_host()
    port = _resolve_server_port()
    if not _is_port_available(host, port):
        print(f"[SYSTEM] Port {port} is already in use. Set APP_PORT to a free port and retry.", flush=True)
        sys.exit(1)
    print(f"Starting {PROJECT_DISPLAY_NAME} server on http://127.0.0.1:{port}")
    dev_reloader = None
    if _should_enable_dev_reloader():
        dev_reloader = DevAutoReloader(project_root=PROJECT_ROOT).start()
        print("[SYSTEM] Dev auto reload active", flush=True)
    boot_trace("creating WSGIServer")
    try:
        http_server = _create_http_server_with_retry(host, port)
    except Exception as e:
        boot_trace(f"failed to create WSGIServer: {e}")
        traceback.print_exc()
        sys.exit(1)
    boot_trace("server created, entering serve_forever")
    try:
        http_server.serve_forever()
    except Exception as e:
        boot_trace(f"serve_forever crashed: {e}")
        traceback.print_exc()
        sys.exit(1)
    except KeyboardInterrupt:
        boot_trace("server interrupted via KeyboardInterrupt")
        print("Server stopped.")
    finally:
        try:
            if dev_reloader is not None:
                dev_reloader.stop()
        except Exception:
            pass
        boot_trace("serve_forever exited")
