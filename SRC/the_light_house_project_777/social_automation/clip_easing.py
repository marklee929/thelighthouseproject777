from __future__ import annotations

from typing import Sequence, Tuple


def clamp01(value: float) -> float:
    try:
        numeric = float(value)
    except Exception:
        numeric = 0.0
    return max(0.0, min(1.0, numeric))


def lerp(start: float, end: float, progress: float) -> float:
    t = clamp01(progress)
    return float(start) + ((float(end) - float(start)) * t)


def ease_linear(progress: float) -> float:
    return clamp01(progress)


def ease_out_quad(progress: float) -> float:
    t = clamp01(progress)
    return 1.0 - ((1.0 - t) * (1.0 - t))


def ease_out_cubic(progress: float) -> float:
    t = clamp01(progress)
    return 1.0 - pow(1.0 - t, 3)


def ease_in_out_cubic(progress: float) -> float:
    t = clamp01(progress)
    if t < 0.5:
        return 4.0 * t * t * t
    return 1.0 - pow((-2.0 * t) + 2.0, 3) / 2.0


def ease_in_out_sine(progress: float) -> float:
    import math

    t = clamp01(progress)
    return -(math.cos(math.pi * t) - 1.0) / 2.0


def ease_out_back(progress: float) -> float:
    t = clamp01(progress)
    c1 = 1.70158
    c3 = c1 + 1.0
    shifted = t - 1.0
    return 1.0 + (c3 * pow(shifted, 3)) + (c1 * pow(shifted, 2))


_EASING_MAP = {
    "linear": ease_linear,
    "ease_out_quad": ease_out_quad,
    "ease_out_cubic": ease_out_cubic,
    "ease_in_out_cubic": ease_in_out_cubic,
    "ease_out_back": ease_out_back,
    "ease_in_out_sine": ease_in_out_sine,
}


def apply_easing(progress: float, easing: str) -> float:
    fn = _EASING_MAP.get(str(easing or "linear").strip().lower(), ease_linear)
    return clamp01(fn(progress))


def segment_progress(progress: float, start: float, end: float, easing: str = "linear") -> float:
    total = max(0.0001, float(end) - float(start))
    t = (clamp01(progress) - float(start)) / total
    return apply_easing(t, easing)


def lerp_point(start: Sequence[float], end: Sequence[float], progress: float, easing: str = "linear") -> Tuple[float, float]:
    t = apply_easing(progress, easing)
    return (
        lerp(float(start[0]), float(end[0]), t),
        lerp(float(start[1]), float(end[1]), t),
    )


def lerp_rect(
    start: Sequence[float],
    end: Sequence[float],
    progress: float,
    easing: str = "linear",
) -> Tuple[float, float, float, float]:
    t = apply_easing(progress, easing)
    return tuple(lerp(float(start[index]), float(end[index]), t) for index in range(4))
