from __future__ import annotations

from typing import Tuple

from PIL import Image

from .clip_easing import apply_easing, clamp01, lerp, lerp_point, lerp_rect, segment_progress


class ClipMotionComposer:
    """Apply lightweight scene motion and fade envelopes to clip frames."""

    def fit_background(
        self,
        image: Image.Image,
        *,
        width: int,
        height: int,
        progress: float,
        motion_style: str,
    ) -> Image.Image:
        style = str(motion_style or "fade").strip().lower()
        zoom_start, zoom_end, pan_x_start, pan_x_end, pan_y_start, pan_y_end, easing = self._motion_profile(style)
        eased = apply_easing(progress, easing)
        zoom = zoom_start + ((zoom_end - zoom_start) * eased)
        target_width = int(width * zoom)
        target_height = int(height * zoom)
        ratio = image.width / max(1, image.height)
        if ratio > (target_width / max(1, target_height)):
            resized_width, resized_height = int(target_height * ratio), target_height
        else:
            resized_width, resized_height = target_width, int(target_width / max(0.01, ratio))
        resized = image.resize((max(width, resized_width), max(height, resized_height)), Image.Resampling.LANCZOS)
        pan_x_ratio = pan_x_start + ((pan_x_end - pan_x_start) * eased)
        pan_y_ratio = pan_y_start + ((pan_y_end - pan_y_start) * eased)
        max_left = max(0, resized.width - width)
        max_top = max(0, resized.height - height)
        left = min(max_left, max(0, int(max_left * pan_x_ratio)))
        top = min(max_top, max(0, int(max_top * pan_y_ratio)))
        return resized.crop((left, top, left + width, top + height))

    def apply_scene_envelope(self, frame: Image.Image, *, progress: float, motion_style: str) -> Image.Image:
        fade_strength = self._fade_strength(progress=progress, motion_style=motion_style)
        if fade_strength <= 0:
            return frame
        overlay_alpha = int(168 * fade_strength)
        overlay = Image.new("RGBA", frame.size, (8, 10, 18, overlay_alpha))
        base = frame.convert("RGBA")
        return Image.alpha_composite(base, overlay).convert("RGB")

    def interpolate_scalar(self, start: float, end: float, *, progress: float, easing: str = "linear") -> float:
        return lerp(float(start), float(end), apply_easing(progress, easing))

    def interpolate_point(self, start: Tuple[float, float], end: Tuple[float, float], *, progress: float, easing: str = "linear") -> Tuple[float, float]:
        return lerp_point(start, end, progress, easing)

    def interpolate_rect(
        self,
        start: Tuple[float, float, float, float],
        end: Tuple[float, float, float, float],
        *,
        progress: float,
        easing: str = "linear",
    ) -> Tuple[float, float, float, float]:
        return lerp_rect(start, end, progress, easing)

    def segment(self, progress: float, start: float, end: float, easing: str = "linear") -> float:
        return segment_progress(progress, start, end, easing)

    def blend_frames(self, first: Image.Image, second: Image.Image, *, alpha: float) -> Image.Image:
        if first.size != second.size:
            second = second.resize(first.size, Image.Resampling.LANCZOS)
        return Image.blend(first.convert("RGBA"), second.convert("RGBA"), clamp01(alpha)).convert("RGB")

    def _motion_profile(self, motion_style: str) -> Tuple[float, float, float, float, float, float, str]:
        style = str(motion_style or "fade").strip().lower()
        if style == "zoom_in":
            return (1.03, 1.22, 0.50, 0.50, 0.45, 0.45, "ease_in_out_cubic")
        if style == "slow_pan":
            return (1.08, 1.15, 0.08, 0.58, 0.10, 0.26, "ease_in_out_sine")
        if style == "quick_cut":
            return (1.00, 1.05, 0.18, 0.40, 0.16, 0.20, "ease_out_quad")
        return (1.03, 1.10, 0.50, 0.50, 0.44, 0.44, "ease_in_out_sine")

    def _fade_strength(self, *, progress: float, motion_style: str) -> float:
        style = str(motion_style or "fade").strip().lower()
        if style == "quick_cut":
            return 0.0
        edge_window = 0.18 if style == "fade" else 0.12
        if progress < edge_window:
            return max(0.0, 1.0 - apply_easing(progress / edge_window, "ease_out_quad"))
        if progress > (1.0 - edge_window):
            local = (progress - (1.0 - edge_window)) / edge_window
            return max(0.0, apply_easing(local, "ease_out_quad"))
        return 0.0
