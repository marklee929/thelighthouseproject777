from __future__ import annotations

from typing import Iterable, List


class NewsCollectorRetryPolicy:
    """Defines the phase-1 fallback collection windows for News Collector."""

    DEFAULT_WINDOWS = (1, 3, 6)
    RETRY_INTERVAL_MINUTES = 10

    def build_attempt_windows(self, requested_hours: int | None = None) -> List[int]:
        base_windows = list(self.DEFAULT_WINDOWS)
        if requested_hours is None:
            return base_windows
        normalized = max(1, min(int(requested_hours), 6))
        if normalized in base_windows:
            start_index = base_windows.index(normalized)
            return base_windows[start_index:]
        custom: List[int] = [normalized]
        custom.extend(hour for hour in base_windows if hour > normalized)
        return custom

    def fallback_used(self, attempt_windows: Iterable[int], final_window_hours: int) -> bool:
        windows = list(attempt_windows)
        return bool(windows and final_window_hours != windows[0])

