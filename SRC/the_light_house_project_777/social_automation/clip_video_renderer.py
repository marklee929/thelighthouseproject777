from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

import numpy as np
from PIL import Image, ImageFilter

try:
    import imageio.v2 as imageio
except Exception:  # pragma: no cover - optional runtime dependency
    imageio = None

try:
    import imageio_ffmpeg
except Exception:  # pragma: no cover - optional runtime dependency
    imageio_ffmpeg = None

from .clip_audio import ClipAudioComposer


class ClipVideoRenderer:
    """Serialize pre-composed clip frames into preview and target media files."""

    DEFAULT_FPS = 30
    DEFAULT_VIDEO_BITRATE = "8M"

    def __init__(self, *, canvas_size: Tuple[int, int], target_size: Tuple[int, int], fps: int = DEFAULT_FPS) -> None:
        self.canvas_size = tuple(canvas_size)
        self.target_size = tuple(target_size)
        self.fps = max(6, int(fps or self.DEFAULT_FPS))
        self.audio_composer = ClipAudioComposer()

    def render_assets(
        self,
        *,
        clip_id: str,
        clip_dir: Path,
        preview_frames: Sequence[Image.Image],
        video_frames: Sequence[Image.Image],
        preview_durations_ms: Sequence[int],
        visual_coverage: float,
        bgm_style: str = "",
    ) -> Dict[str, Any]:
        if not preview_frames:
            raise ValueError("preview_frames is required")
        clip_dir.mkdir(parents=True, exist_ok=True)
        poster_path = clip_dir / f"{clip_id}_poster.png"
        preview_path = clip_dir / f"{clip_id}_preview.gif"
        preview_frames[0].save(poster_path, format="PNG")
        preview_frames[0].save(
            preview_path,
            save_all=True,
            append_images=list(preview_frames[1:]),
            duration=list(preview_durations_ms)[: len(preview_frames)],
            loop=0,
            format="GIF",
            optimize=False,
        )
        video_path = clip_dir / f"{clip_id}.mp4"
        audio_path = clip_dir / f"{clip_id}_bgm.wav"
        encoder_error, audio_error = self._write_mp4_with_audio(
            video_path=video_path,
            audio_path=audio_path,
            frames=video_frames,
            bgm_style=bgm_style,
        )
        return {
            "render_status": self._build_status(video_ready=encoder_error is None, visual_coverage=visual_coverage),
            "files": {
                "poster_path": str(poster_path),
                "preview_gif_path": str(preview_path),
                "video_path": str(video_path) if encoder_error is None else "",
                "audio_path": str(audio_path) if audio_path.exists() else "",
            },
            "encoder_error": encoder_error or "",
            "audio_error": audio_error or "",
            "fps": self.fps,
            "total_video_frames": len(video_frames),
            "duration_sec": round(float(len(video_frames)) / float(self.fps), 2) if video_frames else 0.0,
        }

    def _build_status(self, *, video_ready: bool, visual_coverage: float) -> str:
        if not video_ready:
            return "preview_ready_encoder_required"
        if float(visual_coverage or 0.0) >= 0.5:
            return "video_ready"
        return "video_ready_visual_partial"

    def _write_mp4_with_audio(
        self,
        *,
        video_path: Path,
        audio_path: Path,
        frames: Sequence[Image.Image],
        bgm_style: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        silent_path = video_path.with_name(f"{video_path.stem}_silent.mp4")
        encoder_error = self._write_mp4(video_path=silent_path, frames=frames)
        if encoder_error is not None:
            return encoder_error, None
        audio_error = None
        try:
            duration_sec = round(float(len(frames)) / float(self.fps), 2)
            self.audio_composer.synthesize_bgm(audio_path=audio_path, duration_sec=duration_sec, bgm_style=bgm_style or "clean_explainer")
            mux_error = self._mux_audio(video_path=silent_path, audio_path=audio_path, output_path=video_path)
            if mux_error is not None:
                audio_error = mux_error
                shutil.move(str(silent_path), str(video_path))
            elif silent_path.exists():
                silent_path.unlink()
        except Exception as exc:  # pragma: no cover - runtime encoder behavior
            audio_error = str(exc)
            try:
                shutil.move(str(silent_path), str(video_path))
            except Exception:
                pass
        return None, audio_error

    def _write_mp4(self, *, video_path: Path, frames: Sequence[Image.Image]) -> Optional[str]:
        if not frames:
            return "video frames missing"
        if imageio is None:
            return "imageio unavailable"
        try:
            with imageio.get_writer(
                str(video_path),
                fps=self.fps,
                codec="libx264",
                format="FFMPEG",
                pixelformat="yuv420p",
                macro_block_size=None,
                ffmpeg_log_level="error",
                ffmpeg_params=[
                    "-crf",
                    "18",
                    "-preset",
                    "medium",
                    "-b:v",
                    self.DEFAULT_VIDEO_BITRATE,
                    "-maxrate",
                    self.DEFAULT_VIDEO_BITRATE,
                    "-bufsize",
                    "16M",
                    "-movflags",
                    "+faststart",
                ],
            ) as writer:
                for frame in frames:
                    writer.append_data(self._frame_to_array(frame))
            return None
        except Exception as exc:  # pragma: no cover - runtime encoder behavior
            try:
                if video_path.exists():
                    video_path.unlink()
            except Exception:
                pass
            return str(exc)

    def _mux_audio(self, *, video_path: Path, audio_path: Path, output_path: Path) -> Optional[str]:
        if imageio_ffmpeg is None:
            return "imageio_ffmpeg unavailable"
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        temp_output = output_path.with_name(f"{output_path.stem}_audio{output_path.suffix}")
        command = [
            ffmpeg_exe,
            "-y",
            "-i",
            str(video_path),
            "-i",
            str(audio_path),
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-shortest",
            str(temp_output),
        ]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            try:
                if temp_output.exists():
                    temp_output.unlink()
            except Exception:
                pass
            return (result.stderr or result.stdout or "ffmpeg mux failed").strip()
        shutil.move(str(temp_output), str(output_path))
        return None

    def _frame_to_array(self, frame: Image.Image) -> np.ndarray:
        image = frame.convert("RGB")
        if image.size != self.target_size:
            image = image.resize(self.target_size, Image.Resampling.LANCZOS)
        image = image.filter(ImageFilter.UnsharpMask(radius=1.3, percent=145, threshold=2))
        return np.asarray(image)
