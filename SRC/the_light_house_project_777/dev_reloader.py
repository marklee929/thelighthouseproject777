from __future__ import annotations

import threading
from pathlib import Path
from typing import Dict, Iterable


class DevAutoReloader:
    """
    Lightweight polling watcher.
    The original source was missing in this copy, so this keeps the app stable
    and surfaces file changes without requiring external watcher packages.
    """

    WATCH_SUFFIXES = {".py", ".html", ".css", ".js", ".json", ".yaml", ".yml", ".env", ".txt"}
    IGNORE_DIRS = {"__pycache__", ".venv", ".git", "node_modules", "temp", "logs", "data"}

    def __init__(self, project_root: str | Path, interval_sec: float = 1.0) -> None:
        self.project_root = Path(project_root)
        self.interval_sec = max(0.5, float(interval_sec))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._snapshot: Dict[str, float] = {}
        self._announced = False

    def start(self) -> "DevAutoReloader":
        self._snapshot = self._build_snapshot()
        self._thread = threading.Thread(target=self._watch_loop, name="dev_auto_reloader", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def _iter_files(self) -> Iterable[Path]:
        for path in self.project_root.rglob("*"):
            if not path.is_file():
                continue
            if any(part in self.IGNORE_DIRS for part in path.parts):
                continue
            if path.suffix.lower() not in self.WATCH_SUFFIXES and path.name != ".env":
                continue
            yield path

    def _build_snapshot(self) -> Dict[str, float]:
        snapshot: Dict[str, float] = {}
        for path in self._iter_files():
            try:
                snapshot[str(path)] = path.stat().st_mtime
            except OSError:
                continue
        return snapshot

    def _watch_loop(self) -> None:
        while not self._stop_event.wait(self.interval_sec):
            current = self._build_snapshot()
            if current != self._snapshot:
                if not self._announced:
                    print("[DEV-RELOAD] File changes detected. Restart the app to pick up updates.", flush=True)
                    self._announced = True
                self._snapshot = current
            else:
                self._announced = False
