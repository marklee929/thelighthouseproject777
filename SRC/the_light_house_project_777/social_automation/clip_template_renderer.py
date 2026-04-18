from __future__ import annotations

import io
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from PIL import Image, ImageDraw, ImageFilter, ImageFont

from .clip_easing import lerp, lerp_rect, segment_progress
from .clip_motion import ClipMotionComposer

try:
    import cairosvg
except Exception:  # pragma: no cover - optional runtime dependency
    cairosvg = None


class WorkConnectClipTemplateRenderer:
    """Render the WorkConnect short-form template with layered object motion."""

    BASE_CANVAS = (540, 960)
    INTRO_DURATION_SEC = 1.5

    def __init__(self, *, canvas_size: Tuple[int, int], motion: ClipMotionComposer, logo_path: str | Path) -> None:
        self.canvas_size = tuple(canvas_size)
        self.motion = motion
        self.logo_path = Path(logo_path)
        self.logo_available = self.logo_path.exists()

    def render_frame(
        self,
        *,
        scene: Dict[str, object],
        title: str,
        palette: Tuple[str, str, str],
        background: Optional[Image.Image],
        progress: float,
        motion_style: str,
        category_label: str,
    ) -> Image.Image:
        width, height = self.canvas_size
        kind = self._scene_kind(str(scene.get("label", "")).strip())
        frame = self._build_backdrop(
            background=background,
            palette=palette,
            progress=progress,
            motion_style=motion_style,
        ).convert("RGBA")
        overlay = Image.new("RGBA", frame.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay, "RGBA")

        if kind == "intro":
            self._draw_intro(draw=draw, progress=progress, palette=palette, width=width, height=height)
        else:
            self._draw_corner_logo(draw=draw, progress=progress)
            self._draw_title_bar(
                draw=draw,
                progress=progress,
                scene=scene,
                title=title,
                category_label=category_label,
                width=width,
            )
            self._draw_image_card(
                frame=frame,
                overlay=overlay,
                progress=progress,
                scene=scene,
                palette=palette,
                background=background,
                width=width,
                height=height,
            )
            self._draw_text_layers(
                draw=draw,
                progress=progress,
                scene=scene,
                palette=palette,
                width=width,
                height=height,
                kind=kind,
            )

        composed = Image.alpha_composite(frame, overlay).convert("RGB")
        return self.motion.apply_scene_envelope(composed, progress=progress, motion_style=motion_style)

    def render_timeline_frame(
        self,
        *,
        title: str,
        palette: Tuple[str, str, str],
        category_label: str,
        current_time_sec: float,
        total_duration_sec: float,
        current_scene: Dict[str, object],
        current_background: Optional[Image.Image],
        scene_progress: float,
        transition_progress: float,
        from_scene: Optional[Dict[str, object]] = None,
        from_background: Optional[Image.Image] = None,
        to_scene: Optional[Dict[str, object]] = None,
        to_background: Optional[Image.Image] = None,
    ) -> Image.Image:
        width, height = self.canvas_size
        if float(current_time_sec) < self.INTRO_DURATION_SEC:
            return self._render_timeline_intro(current_time_sec=float(current_time_sec), palette=palette, width=width, height=height)

        header_progress = 1.0
        if float(current_time_sec) < (self.INTRO_DURATION_SEC + 0.72):
            header_progress = segment_progress(
                (float(current_time_sec) - self.INTRO_DURATION_SEC) / 0.72,
                0.0,
                1.0,
                "ease_out_cubic",
            )

        base = self._build_backdrop(
            background=current_background or to_background or from_background,
            palette=palette,
            progress=scene_progress,
            motion_style=str(current_scene.get("motion_style", "fade")),
        ).convert("RGBA")
        overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay, "RGBA")

        self._draw_timeline_header(
            draw=draw,
            title=title,
            category_label=category_label,
            width=width,
            height=height,
            reveal_progress=header_progress,
        )
        self._draw_timeline_content(
            frame=base,
            overlay=overlay,
            palette=palette,
            width=width,
            height=height,
            scene_progress=scene_progress,
            transition_progress=transition_progress,
            from_scene=from_scene or current_scene,
            from_background=from_background or current_background,
            to_scene=to_scene,
            to_background=to_background or current_background,
        )
        self._draw_timeline_footer(
            draw=draw,
            palette=palette,
            width=width,
            height=height,
            transition_progress=transition_progress,
            from_scene=from_scene or current_scene,
            to_scene=to_scene,
        )
        composed = Image.alpha_composite(base, overlay).convert("RGB")
        return self.motion.apply_scene_envelope(composed, progress=scene_progress, motion_style="fade")

    def _render_timeline_intro(self, *, current_time_sec: float, palette: Tuple[str, str, str], width: int, height: int) -> Image.Image:
        progress = max(0.0, min(1.0, float(current_time_sec) / self.INTRO_DURATION_SEC))
        base = self._build_intro_backdrop(palette=palette, width=width, height=height, progress=progress).convert("RGBA")
        overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay, "RGBA")
        fade = segment_progress(progress, 0.05, 0.55, "ease_out_cubic")
        settle = segment_progress(progress, 0.0, 0.82, "ease_out_cubic")
        logo_box = self._scale_rect(self._rect(52.0, 226.0, 488.0, 548.0), 0.96 + (0.08 * settle))
        self._draw_brand_lockup(draw=draw, box=logo_box, alpha=max(120, int(255 * fade)), compact=False)
        composed = Image.alpha_composite(base, overlay).convert("RGB")
        return composed

    def _draw_timeline_header(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        title: str,
        category_label: str,
        width: int,
        height: int,
        reveal_progress: float,
    ) -> None:
        top_height = int(height * 0.25)
        alpha = int(226 * reveal_progress)
        draw.rectangle((0, 0, width, top_height), fill=(6, 8, 14, alpha))
        self._draw_brand_lockup(
            draw=draw,
            box=self._rect(16.0, 16.0, 268.0, 134.0),
            alpha=max(100, int(255 * reveal_progress)),
            compact=True,
        )
        chip_box = (width - self._sx(168.0), self._sy(30.0), width - self._sx(24.0), self._sy(82.0))
        draw.rounded_rectangle(chip_box, radius=self._sr(18.0), fill=(28, 34, 48, int(182 * reveal_progress)))
        self._draw_text_block(
            draw=draw,
            box=(chip_box[0] + self._sx(10.0), chip_box[1] + self._sy(8.0), chip_box[2] - self._sx(10.0), chip_box[3] - self._sy(8.0)),
            text=str(category_label or "WorkConnect").upper(),
            font=self._font(self._sf(16.0), True),
            fill=(232, 239, 250, min(255, int(255 * reveal_progress))),
            align="center",
            max_lines=1,
        )
        headline = str(title or "").strip()
        if ":" in headline:
            headline = headline.split(":", 1)[1].strip()
        self._draw_text_block(
            draw=draw,
            box=(self._sx(38.0), self._sy(90.0), width - self._sx(38.0), top_height - self._sy(20.0)),
            text=headline,
            font=self._font(self._sf(38.0), True),
            fill=(255, 255, 255, min(255, int(255 * reveal_progress))),
            align="center",
            stroke_width=2,
            max_lines=2,
            min_font_size=self._sf(24.0),
        )

    def _draw_timeline_content(
        self,
        *,
        frame: Image.Image,
        overlay: Image.Image,
        palette: Tuple[str, str, str],
        width: int,
        height: int,
        scene_progress: float,
        transition_progress: float,
        from_scene: Dict[str, object],
        from_background: Optional[Image.Image],
        to_scene: Optional[Dict[str, object]],
        to_background: Optional[Image.Image],
    ) -> None:
        card_box = (self._sx(26.0), self._sy(248.0), width - self._sx(26.0), self._sy(742.0))
        transition_alpha = max(0.0, min(1.0, float(transition_progress)))
        slide_px = self._sx(42.0)
        card_from = self._compose_content_card(
            palette=palette,
            box=card_box,
            scene=from_scene,
            background=from_background,
            shift_x=0.0 if transition_alpha <= 0 else (-slide_px * transition_alpha),
            alpha=1.0 if transition_alpha <= 0 else (1.0 - transition_alpha),
        )
        frame.alpha_composite(card_from)
        if to_scene is not None and transition_alpha > 0:
            card_to = self._compose_content_card(
                palette=palette,
                box=card_box,
                scene=to_scene,
                background=to_background,
                shift_x=slide_px * (1.0 - transition_alpha),
                alpha=transition_alpha,
            )
            frame.alpha_composite(card_to)

    def _draw_timeline_footer(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        palette: Tuple[str, str, str],
        width: int,
        height: int,
        transition_progress: float,
        from_scene: Dict[str, object],
        to_scene: Optional[Dict[str, object]],
    ) -> None:
        panel_box = (self._sx(22.0), self._sy(748.0), width - self._sx(22.0), height - self._sy(22.0))
        draw.rounded_rectangle(panel_box, radius=self._sr(36.0), fill=(10, 12, 18, 220))
        chip_box = self._rect(42.0, 762.0, 186.0, 808.0)
        draw.rounded_rectangle(chip_box, radius=self._sr(22.0), fill=(*self._hex_to_rgb(palette[2]), 214))
        self._draw_text_block(
            draw=draw,
            box=self._rect(54.0, 772.0, 174.0, 800.0),
            text="KEY POINT",
            font=self._font(self._sf(18.0), True),
            fill=(16, 18, 24, 255),
            align="center",
            max_lines=1,
        )
        transition_alpha = max(0.0, min(1.0, float(transition_progress)))
        self._draw_footer_text(
            draw=draw,
            scene=from_scene,
            box=panel_box,
            alpha=1.0 if transition_alpha <= 0 else (1.0 - transition_alpha),
            shift_x=-26.0 * transition_alpha,
        )
        if to_scene is not None and transition_alpha > 0:
            self._draw_footer_text(
                draw=draw,
                scene=to_scene,
                box=panel_box,
                alpha=transition_alpha,
                shift_x=26.0 * (1.0 - transition_alpha),
            )

    def _compose_content_card(
        self,
        *,
        palette: Tuple[str, str, str],
        box: Sequence[float],
        scene: Dict[str, object],
        background: Optional[Image.Image],
        shift_x: float,
        alpha: float,
    ) -> Image.Image:
        width, height = self.canvas_size
        layer = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        x0, y0, x1, y1 = [float(value) for value in box]
        x0 += shift_x
        x1 += shift_x
        shadow = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        shadow_draw = ImageDraw.Draw(shadow, "RGBA")
        shadow_draw.rounded_rectangle((x0 + self._sx(6.0), y0 + self._sy(14.0), x1 + self._sx(6.0), y1 + self._sy(14.0)), radius=self._sr(44.0), fill=(0, 0, 0, int(118 * alpha)))
        shadow = shadow.filter(ImageFilter.GaussianBlur(radius=self._sr(16.0)))
        layer.alpha_composite(shadow)
        card_image = self._card_image(
            background=background,
            palette=palette,
            size=(int(x1 - x0), int(y1 - y0)),
            scene=scene,
        )
        rounded_mask = Image.new("L", card_image.size, 0)
        mask_draw = ImageDraw.Draw(rounded_mask)
        mask_draw.rounded_rectangle((0, 0, card_image.size[0], card_image.size[1]), radius=self._sr(42.0), fill=int(255 * alpha))
        layer.paste(card_image, (int(x0), int(y0)), mask=rounded_mask)
        border_draw = ImageDraw.Draw(layer, "RGBA")
        border_draw.rounded_rectangle((x0, y0, x1, y1), radius=self._sr(42.0), outline=(255, 255, 255, int(88 * alpha)), width=max(2, self._sw(2.0)))
        bottom_panel = (x0 + self._sx(18.0), y1 - self._sy(128.0), x1 - self._sx(18.0), y1 - self._sy(18.0))
        border_draw.rounded_rectangle(bottom_panel, radius=self._sr(30.0), fill=(8, 10, 16, int(198 * alpha)))
        self._draw_text_block(
            draw=border_draw,
            box=(x0 + self._sx(34.0), y1 - self._sy(112.0), x1 - self._sx(34.0), y1 - self._sy(82.0)),
            text=str(scene.get("label", "scene")).replace("_", " ").title(),
            font=self._font(self._sf(18.0), True),
            fill=(214, 230, 255, int(245 * alpha)),
            align="left",
            max_lines=1,
        )
        source_title = str(scene.get("source_title", "")).strip()[:72]
        if source_title:
            self._draw_text_block(
                draw=border_draw,
                box=(x0 + self._sx(34.0), y1 - self._sy(78.0), x1 - self._sx(34.0), y1 - self._sy(34.0)),
                text=source_title,
                font=self._font(self._sf(17.0), False),
                fill=(245, 245, 247, int(230 * alpha)),
                align="left",
                max_lines=2,
                min_font_size=self._sf(14.0),
            )
        return layer

    def _draw_footer_text(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        scene: Dict[str, object],
        box: Sequence[float],
        alpha: float,
        shift_x: float,
    ) -> None:
        x0, y0, x1, y1 = [float(value) for value in box]
        text_box = (x0 + self._sx(28.0) + shift_x, y0 + self._sy(70.0), x1 - self._sx(28.0) + shift_x, y1 - self._sy(28.0))
        self._draw_text_block(
            draw=draw,
            box=text_box,
            text=str(scene.get("on_screen_text", "")).strip(),
            font=self._font(self._sf(34.0), True),
            fill=(255, 255, 255, int(255 * alpha)),
            align="left",
            stroke_width=2,
            max_lines=3,
            min_font_size=self._sf(22.0),
        )

    def _build_intro_backdrop(self, *, palette: Tuple[str, str, str], width: int, height: int, progress: float) -> Image.Image:
        base = Image.new("RGB", (width, height), "#F9FBFF")
        draw = ImageDraw.Draw(base, "RGBA")
        accent_a = self._hex_to_rgb(palette[1])
        accent_b = self._hex_to_rgb(palette[2])
        draw.rectangle((0, 0, width, height), fill=(252, 253, 255, 255))
        draw.ellipse(self._rect(-40.0, 120.0, 290.0, 430.0), fill=(*accent_a, 54))
        draw.ellipse((width - self._sx(280.0), self._sy(180.0), width + self._sx(20.0), self._sy(500.0)), fill=(*accent_b, 42))
        draw.ellipse((self._sx(120.0), height - self._sy(320.0), width - self._sx(80.0), height - self._sy(40.0)), fill=(255, 255, 255, 98))
        return base.filter(ImageFilter.GaussianBlur(radius=self._sr(14.0)))

    def _build_backdrop(
        self,
        *,
        background: Optional[Image.Image],
        palette: Tuple[str, str, str],
        progress: float,
        motion_style: str,
    ) -> Image.Image:
        width, height = self.canvas_size
        if background is not None:
            fitted = self.motion.fit_background(
                background,
                width=width,
                height=height,
                progress=progress,
                motion_style=motion_style,
            ).filter(ImageFilter.GaussianBlur(radius=14))
            dark = Image.new("RGBA", fitted.size, (6, 9, 16, 118))
            return Image.alpha_composite(fitted.convert("RGBA"), dark).convert("RGB")
        return self._gradient_backdrop(palette=palette, width=width, height=height, progress=progress)

    def _draw_intro(self, *, draw: ImageDraw.ImageDraw, progress: float, palette: Tuple[str, str, str], width: int, height: int) -> None:
        intro_alpha = int(255 * segment_progress(progress, 0.0, 0.52, "ease_out_quad"))
        settle = segment_progress(progress, 0.0, 0.82, "ease_in_out_cubic")
        move = segment_progress(progress, 0.34, 1.0, "ease_out_cubic")
        center_box = (46.0, 242.0, 494.0, 520.0)
        corner_box = (18.0, 18.0, 246.0, 128.0)
        logo_box = lerp_rect(center_box, corner_box, move, "ease_out_cubic")
        scale_box = self._scale_rect(logo_box, 1.05 - (0.08 * settle))
        glow_alpha = int(84 * (1.0 - move))
        draw.ellipse((38, 206, width - 38, height - 210), fill=(*self._hex_to_rgb(palette[1]), glow_alpha))
        self._draw_brand_lockup(draw=draw, box=scale_box, alpha=max(120, intro_alpha), compact=move >= 0.8)
        caption_alpha = int(255 * segment_progress(progress, 0.18, 0.78, "ease_out_quad") * (1.0 - (move * 0.38)))
        if caption_alpha > 0:
            caption_box = (52, 560, width - 52, 704)
            self._draw_text_block(
                draw=draw,
                box=caption_box,
                text="WorkConnect clips for foreigners in Korea",
                font=self._font(34, True),
                fill=(245, 247, 255, caption_alpha),
                align="center",
                stroke_width=2,
            )

    def _draw_corner_logo(self, *, draw: ImageDraw.ImageDraw, progress: float) -> None:
        if not self.logo_available:
            return
        alpha = int(255 * segment_progress(progress, 0.0, 0.48, "ease_out_quad"))
        box = lerp_rect(
            (8.0, -44.0, 200.0, 42.0),
            (18.0, 18.0, 242.0, 122.0),
            segment_progress(progress, 0.0, 0.82, "ease_out_cubic"),
            "ease_out_cubic",
        )
        self._draw_brand_lockup(draw=draw, box=box, alpha=alpha, compact=True)

    def _draw_title_bar(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        progress: float,
        scene: Dict[str, object],
        title: str,
        category_label: str,
        width: int,
    ) -> None:
        reveal = segment_progress(progress, 0.08, 0.58, "ease_out_quad")
        if reveal <= 0:
            return
        y_offset = int(lerp(-72, 0, reveal))
        alpha = int(228 * reveal)
        bar_box = (16, 104 + y_offset, width - 16, 274 + y_offset)
        draw.rounded_rectangle(bar_box, radius=28, fill=(8, 10, 16, alpha))
        chip_alpha = int(186 * reveal)
        chip_box = (34, 126 + y_offset, 176, 170 + y_offset)
        draw.rounded_rectangle(chip_box, radius=18, fill=(20, 28, 40, chip_alpha))
        chip_text = str(category_label or "WorkConnect").strip().upper()
        self._draw_text_block(
            draw=draw,
            box=(46, 136 + y_offset, 166, 162 + y_offset),
            text=chip_text,
            font=self._font(19, True),
            fill=(233, 240, 252, min(255, int(255 * reveal))),
            align="center",
        )
        headline = self._headline_from_title(title=title, scene=scene)
        self._draw_text_block(
            draw=draw,
            box=(34, 178 + y_offset, width - 34, 254 + y_offset),
            text=headline,
            font=self._font(34, True),
            fill=(255, 255, 255, min(255, int(255 * reveal))),
            align="left",
            stroke_width=2,
        )

    def _draw_image_card(
        self,
        *,
        frame: Image.Image,
        overlay: Image.Image,
        progress: float,
        scene: Dict[str, object],
        palette: Tuple[str, str, str],
        background: Optional[Image.Image],
        width: int,
        height: int,
    ) -> None:
        kind = self._scene_kind(str(scene.get("label", "")).strip())
        if kind == "cta":
            start_y = 350.0
            end_y = 300.0
            start_scale = 0.98
            end_scale = 1.0
        else:
            start_y = 1120.0
            end_y = 258.0
            start_scale = 0.96
            end_scale = 1.0
        entrance = segment_progress(progress, 0.10, 0.82, "ease_in_out_cubic")
        card_height = 510.0 if kind != "cta" else 420.0
        card_box = self._scale_rect(
            (22.0, lerp(start_y, end_y, entrance), width - 22.0, lerp(start_y, end_y, entrance) + card_height),
            lerp(start_scale, end_scale, entrance),
        )
        card_box = (
            max(18, card_box[0]),
            max(242, card_box[1]),
            min(width - 18, card_box[2]),
            min(height - 168, card_box[3]),
        )
        if (card_box[2] - card_box[0]) < 80 or (card_box[3] - card_box[1]) < 80:
            return
        shadow = Image.new("RGBA", frame.size, (0, 0, 0, 0))
        shadow_draw = ImageDraw.Draw(shadow, "RGBA")
        shadow_draw.rounded_rectangle(
            (card_box[0] + 6, card_box[1] + 12, card_box[2] + 6, card_box[3] + 12),
            radius=44,
            fill=(0, 0, 0, 104),
        )
        shadow = shadow.filter(ImageFilter.GaussianBlur(radius=14))
        frame.alpha_composite(shadow)

        card_image = self._card_image(background=background, palette=palette, size=(int(card_box[2] - card_box[0]), int(card_box[3] - card_box[1])), scene=scene)
        card_layer = Image.new("RGBA", frame.size, (0, 0, 0, 0))
        rounded_mask = Image.new("L", card_image.size, 0)
        mask_draw = ImageDraw.Draw(rounded_mask)
        mask_draw.rounded_rectangle((0, 0, card_image.size[0], card_image.size[1]), radius=38, fill=255)
        card_layer.paste(card_image, (int(card_box[0]), int(card_box[1])), mask=rounded_mask)
        border_draw = ImageDraw.Draw(card_layer, "RGBA")
        border_draw.rounded_rectangle(card_box, radius=42, outline=(255, 255, 255, 86), width=2)
        frame.alpha_composite(card_layer)

        info_overlay = ImageDraw.Draw(overlay, "RGBA")
        bottom_panel_height = 134
        info_overlay.rounded_rectangle(
            (card_box[0] + 16, card_box[3] - bottom_panel_height, card_box[2] - 16, card_box[3] - 16),
            radius=28,
            fill=(10, 12, 18, 196),
        )
        scene_label = str(scene.get("label", "scene")).replace("_", " ").title()
        self._draw_text_block(
            draw=info_overlay,
            box=(card_box[0] + 32, card_box[3] - 118, card_box[2] - 32, card_box[3] - 78),
            text=scene_label,
            font=self._font(18, True),
            fill=(208, 224, 255, 240),
            align="left",
        )
        source_title = str(scene.get("source_title", "")).strip()
        if source_title:
            self._draw_text_block(
                draw=info_overlay,
                box=(card_box[0] + 32, card_box[3] - 80, card_box[2] - 32, card_box[3] - 26),
                text=source_title[:72],
                font=self._font(17, False),
                fill=(245, 245, 247, 222),
                align="left",
            )

    def _draw_text_layers(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        progress: float,
        scene: Dict[str, object],
        palette: Tuple[str, str, str],
        width: int,
        height: int,
        kind: str,
    ) -> None:
        text = str(scene.get("on_screen_text", "")).strip()
        if not text:
            return
        text_progress = segment_progress(progress, 0.42, 0.88, "ease_out_back")
        panel_progress = segment_progress(progress, 0.24, 0.76, "ease_in_out_cubic")
        y_shift = int(lerp(54, 0, panel_progress))
        x_shift = int(lerp(28, 0, text_progress))
        alpha = int(240 * max(text_progress, panel_progress))
        box = (22 + x_shift, 668 + y_shift, width - 22 + x_shift, height - 66 + y_shift)
        draw.rounded_rectangle(box, radius=36, fill=(10, 12, 18, min(220, alpha)))
        accent = self._hex_to_rgb(palette[2])
        draw.rounded_rectangle((box[0] + 18, box[1] + 18, box[0] + 156, box[1] + 60), radius=20, fill=(*accent, min(214, int(alpha * 0.88))))
        label = "CTA" if kind == "cta" else "POINT"
        self._draw_text_block(
            draw=draw,
            box=(box[0] + 26, box[1] + 26, box[0] + 148, box[1] + 54),
            text=label,
            font=self._font(18, True),
            fill=(16, 18, 24, min(255, alpha)),
            align="center",
        )
        text_box = (box[0] + 28, box[1] + 82, box[2] - 28, box[3] - 32)
        self._draw_text_block(
            draw=draw,
            box=text_box,
            text=text,
            font=self._font(34 if kind != "cta" else 36, True),
            fill=(255, 255, 255, min(255, alpha)),
            align="left",
            stroke_width=2,
        )
        if kind != "cta":
            sub_alpha = int(184 * text_progress)
            self._draw_text_block(
                draw=draw,
                box=(box[0] + 28, box[3] - 56, box[2] - 28, box[3] - 18),
                text=str(scene.get("visual_hint", "")).strip()[:74],
                font=self._font(17, False),
                fill=(214, 222, 236, sub_alpha),
                align="left",
            )

    def _gradient_backdrop(self, *, palette: Tuple[str, str, str], width: int, height: int, progress: float) -> Image.Image:
        top = self._hex_to_rgb(palette[0])
        mid = self._hex_to_rgb(palette[1])
        bottom = (8, 10, 16)
        image = Image.new("RGB", (width, height), bottom)
        draw = ImageDraw.Draw(image, "RGBA")
        for y in range(height):
            ratio = y / max(1, height - 1)
            if ratio < 0.45:
                mix = ratio / 0.45
                color = tuple(int(lerp(top[index], mid[index], mix)) for index in range(3))
            else:
                mix = (ratio - 0.45) / 0.55
                color = tuple(int(lerp(mid[index], bottom[index], mix)) for index in range(3))
            draw.line((0, y, width, y), fill=(*color, 255))
        orbital = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        orbital_draw = ImageDraw.Draw(orbital, "RGBA")
        progress_shift = segment_progress(progress, 0.0, 1.0, "ease_in_out_sine")
        orbital_draw.ellipse((-60, 96, 320, 430), fill=(*self._hex_to_rgb(palette[1]), 72))
        orbital_draw.ellipse((width - 280, 140 + int(progress_shift * 20), width + 30, 480 + int(progress_shift * 20)), fill=(*self._hex_to_rgb(palette[2]), 46))
        orbital_draw.ellipse((150, height - 360, width - 90, height - 60), fill=(255, 255, 255, 18))
        orbital = orbital.filter(ImageFilter.GaussianBlur(radius=26))
        return Image.alpha_composite(image.convert("RGBA"), orbital).convert("RGB")

    def _card_image(
        self,
        *,
        background: Optional[Image.Image],
        palette: Tuple[str, str, str],
        size: Tuple[int, int],
        scene: Dict[str, object],
    ) -> Image.Image:
        if background is not None:
            source = background.copy().convert("RGB")
            ratio = source.width / max(1, source.height)
            target_ratio = size[0] / max(1, size[1])
            if ratio > target_ratio:
                target_height = size[1]
                target_width = int(target_height * ratio)
            else:
                target_width = size[0]
                target_height = int(target_width / max(0.01, ratio))
            resized = source.resize((max(size[0], target_width), max(size[1], target_height)), Image.Resampling.LANCZOS)
            left = max(0, (resized.width - size[0]) // 2)
            top = max(0, (resized.height - size[1]) // 2)
            card = resized.crop((left, top, left + size[0], top + size[1]))
        else:
            card = self._gradient_backdrop(palette=palette, width=size[0], height=size[1], progress=0.5)
        vignette = Image.new("RGBA", size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(vignette, "RGBA")
        draw.rectangle((0, 0, size[0], size[1]), fill=(0, 0, 0, 28))
        draw.rectangle((0, int(size[1] * 0.58), size[0], size[1]), fill=(6, 8, 12, 124))
        if background is None:
            hint = str(scene.get("visual_hint", "")).strip() or "WorkConnect Korea"
            self._draw_text_block(
                draw=draw,
                box=(42, 48, size[0] - 42, size[1] - 48),
                text=hint,
                font=self._font(24, True),
                fill=(255, 255, 255, 200),
                align="center",
                stroke_width=2,
            )
        return Image.alpha_composite(card.convert("RGBA"), vignette).convert("RGB")

    def _draw_brand_lockup(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        box: Sequence[float],
        alpha: int,
        compact: bool,
    ) -> None:
        if alpha <= 0:
            return
        x0, y0, x1, y1 = [int(value) for value in box]
        width = max(80, x1 - x0)
        height = max(42, y1 - y0)
        asset = self._brand_asset(compact=compact)
        asset_ratio = asset.width / max(1, asset.height)
        target_ratio = width / max(1, height)
        if asset_ratio >= target_ratio:
            fitted_width = width
            fitted_height = max(1, int(round(fitted_width / asset_ratio)))
        else:
            fitted_height = height
            fitted_width = max(1, int(round(fitted_height * asset_ratio)))
        asset = asset.resize((fitted_width, fitted_height), Image.Resampling.LANCZOS)
        paste_x = x0 + max(0, (width - fitted_width) // 2)
        paste_y = y0 + max(0, (height - fitted_height) // 2)
        target = Image.new("RGBA", draw._image.size, (0, 0, 0, 0))
        alpha_mask = asset.getchannel("A").point(lambda value: int(value * (alpha / 255.0)))
        target.paste(asset, (paste_x, paste_y), alpha_mask)
        draw._image.alpha_composite(target)

    @lru_cache(maxsize=4)
    def _brand_asset(self, compact: bool) -> Image.Image:
        if not self.logo_available:
            width = 360 if compact else 540
            height = 120 if compact else 160
            image = Image.new("RGBA", (width, height), (0, 0, 0, 0))
            draw = ImageDraw.Draw(image, "RGBA")
            draw.rounded_rectangle((0, 0, width - 1, height - 1), radius=28, fill=(10, 12, 18, 228))
            draw.rounded_rectangle((8, 8, width - 9, height - 9), radius=24, outline=(255, 255, 255, 72), width=2)
            label = "LIGHT HOUSE 777" if not compact else "LH 777"
            font = ImageFont.load_default()
            text_width = draw.textlength(label, font=font)
            text_x = max(12, int((width - text_width) / 2))
            text_y = max(12, int((height - 12) / 2) - 6)
            draw.text((text_x, text_y), label, font=font, fill=(245, 247, 255, 255))
            return image
        suffix = self.logo_path.suffix.lower()
        if suffix == ".svg":
            if cairosvg is None:
                raise RuntimeError(f"SVG logo requires cairosvg: {self.logo_path}")
            data = cairosvg.svg2png(url=str(self.logo_path))
            image = Image.open(io.BytesIO(data)).convert("RGBA")
        else:
            image = Image.open(self.logo_path).convert("RGBA")
        bbox = image.getchannel("A").getbbox()
        if bbox:
            image = image.crop(bbox)
        return image

    def _draw_text_block(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        box: Sequence[float],
        text: str,
        font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
        fill: Tuple[int, int, int, int],
        align: str = "left",
        stroke_width: int = 0,
        max_lines: int = 5,
        min_font_size: int = 14,
    ) -> None:
        x0, y0, x1, y1 = [int(value) for value in box]
        lines, fitted_font = self._fit_text(
            draw=draw,
            text=text,
            font=font,
            width=max(10, x1 - x0),
            height=max(10, y1 - y0),
            max_lines=max_lines,
            min_font_size=min_font_size,
        )
        if not lines:
            return
        line_height = int((fitted_font.size if hasattr(fitted_font, "size") else 24) * 1.24)
        total_height = len(lines) * line_height
        start_y = y0 + max(0, ((y1 - y0) - total_height) // 2)
        for index, line in enumerate(lines):
            line_width = draw.textlength(line, font=fitted_font)
            if align == "center":
                line_x = x0 + max(0, int(((x1 - x0) - line_width) / 2))
            elif align == "right":
                line_x = x1 - int(line_width)
            else:
                line_x = x0
            draw.text(
                (line_x, start_y + (index * line_height)),
                line,
                fill=fill,
                font=fitted_font,
                stroke_width=stroke_width,
                stroke_fill=(12, 14, 18, min(255, int(fill[3] * 0.75))) if stroke_width else None,
            )

    def _fit_text(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        text: str,
        font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
        width: int,
        height: int,
        max_lines: int,
        min_font_size: int,
    ) -> Tuple[Tuple[str, ...], ImageFont.FreeTypeFont | ImageFont.ImageFont]:
        size = int(getattr(font, "size", 24))
        bold = "bold" in str(getattr(font, "path", "")).lower() or "bd" in str(getattr(font, "path", "")).lower()
        current_font = font
        while size >= max(8, int(min_font_size)):
            current_font = self._font(size, bold)
            lines = self._wrap_lines(draw=draw, text=text, font=current_font, width=width, max_lines=max_lines)
            line_height = int((current_font.size if hasattr(current_font, "size") else 24) * 1.24)
            if lines and len(lines) <= max_lines and (len(lines) * line_height) <= height:
                return lines, current_font
            size -= 2
        lines = self._wrap_lines(draw=draw, text=text, font=current_font, width=width, max_lines=max_lines)
        return lines, current_font

    def _wrap_lines(
        self,
        *,
        draw: ImageDraw.ImageDraw,
        text: str,
        font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
        width: int,
        max_lines: int = 5,
    ) -> Tuple[str, ...]:
        words = str(text or "").replace("\n", " ").split()
        lines = []
        current = ""
        for word in words:
            probe = f"{current} {word}".strip()
            if current and draw.textlength(probe, font=font) > width:
                lines.append(current)
                if len(lines) >= max_lines:
                    break
                current = word
            else:
                current = probe
        if current and len(lines) < max_lines:
            lines.append(current)
        if len(lines) > max_lines:
            lines = lines[:max_lines]
        if lines and len(words) > 0 and " ".join(lines) != " ".join(words):
            lines[-1] = lines[-1].rstrip(" .,:;") + "..."
        return tuple(lines[:max_lines])

    def _font(self, size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        candidates = [
            "C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf",
            "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
        ]
        for candidate in candidates:
            try:
                return ImageFont.truetype(candidate, size=size)
            except Exception:
                continue
        return ImageFont.load_default()

    def _scene_kind(self, label: str) -> str:
        clean = str(label or "").strip().lower()
        if clean == "intro":
            return "intro"
        if clean in {"hook", "topic"}:
            return "headline"
        if clean == "cta":
            return "cta"
        return "explainer"

    def _headline_from_title(self, *, title: str, scene: Dict[str, object]) -> str:
        title_text = str(title or "").strip()
        if ":" in title_text:
            title_text = title_text.split(":", 1)[1].strip()
        scene_text = str(scene.get("on_screen_text", "")).strip()
        if self._scene_kind(str(scene.get("label", "")).strip()) == "headline" and scene_text:
            return scene_text[:88]
        return title_text[:88]

    def _hex_to_rgb(self, value: str) -> Tuple[int, int, int]:
        text = str(value or "").strip().lstrip("#")
        if len(text) != 6:
            return (32, 32, 32)
        return tuple(int(text[index:index + 2], 16) for index in (0, 2, 4))

    def _scale_rect(self, rect: Sequence[float], scale: float) -> Tuple[float, float, float, float]:
        x0, y0, x1, y1 = [float(value) for value in rect]
        cx = (x0 + x1) / 2.0
        cy = (y0 + y1) / 2.0
        half_w = ((x1 - x0) * float(scale)) / 2.0
        half_h = ((y1 - y0) * float(scale)) / 2.0
        return (cx - half_w, cy - half_h, cx + half_w, cy + half_h)

    def _sx(self, value: float) -> float:
        return float(value) * (float(self.canvas_size[0]) / float(self.BASE_CANVAS[0]))

    def _sy(self, value: float) -> float:
        return float(value) * (float(self.canvas_size[1]) / float(self.BASE_CANVAS[1]))

    def _sr(self, value: float) -> int:
        scale = min(float(self.canvas_size[0]) / float(self.BASE_CANVAS[0]), float(self.canvas_size[1]) / float(self.BASE_CANVAS[1]))
        return max(1, int(round(float(value) * scale)))

    def _sf(self, value: float) -> int:
        scale = min(float(self.canvas_size[0]) / float(self.BASE_CANVAS[0]), float(self.canvas_size[1]) / float(self.BASE_CANVAS[1]))
        return max(8, int(round(float(value) * scale)))

    def _sw(self, value: float) -> int:
        return max(1, int(round(self._sx(value))))

    def _rect(self, x0: float, y0: float, x1: float, y1: float) -> Tuple[float, float, float, float]:
        return (self._sx(x0), self._sy(y0), self._sx(x1), self._sy(y1))
