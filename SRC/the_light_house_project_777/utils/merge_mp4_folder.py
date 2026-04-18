# python .\utils\merge_mp4_folder.py C:\WORK\Tim'sJournal\clips\combined -o merged_final.mp4

from __future__ import annotations

import argparse
import gc
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path


def _resolve_ffmpeg() -> str:
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg:
        return ffmpeg
    try:
        import imageio_ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception as exc:  # pragma: no cover - runtime dependency lookup
        raise RuntimeError("ffmpeg executable not found. Install ffmpeg or imageio-ffmpeg.") from exc


def _iter_mp4_files(folder: Path) -> list[Path]:
    files = [path for path in folder.iterdir() if path.is_file() and path.suffix.lower() == ".mp4"]
    return sorted(files, key=lambda path: path.name.lower())


def _concat_list_entry(path: Path) -> str:
    normalized = path.resolve().as_posix().replace("'", "'\\''")
    return f"file '{normalized}'"


def _safe_unlink(path: Path, *, retries: int = 8, delay_sec: float = 0.25) -> None:
    for attempt in range(retries):
        try:
            path.unlink(missing_ok=True)
            return
        except FileNotFoundError:
            return
        except PermissionError:
            if attempt == retries - 1:
                raise
            gc.collect()
            time.sleep(delay_sec)


def _replace_with_retry(source_path: Path, target_path: Path, *, retries: int = 8, delay_sec: float = 0.25) -> None:
    last_error: PermissionError | None = None
    for attempt in range(retries):
        try:
            os.replace(str(source_path), str(target_path))
            return
        except PermissionError as exc:
            last_error = exc
            if attempt == retries - 1:
                break
            gc.collect()
            time.sleep(delay_sec)
    raise RuntimeError(f"Failed to finalize merged MP4 because the file is still locked: {target_path}") from last_error


def merge_mp4_folder(folder: Path, output_path: Path, *, reencode: bool = False) -> Path:
    folder = folder.resolve()
    output_path = output_path.resolve()

    if not folder.exists() or not folder.is_dir():
        raise FileNotFoundError(f"Input folder not found: {folder}")

    files = _iter_mp4_files(folder)
    if not files:
        raise FileNotFoundError(f"No .mp4 files found in: {folder}")

    if output_path in files:
        files = [path for path in files if path.resolve() != output_path]

    if not files:
        raise FileNotFoundError("No source .mp4 files remain after excluding the output path.")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if len(files) == 1:
        shutil.copy2(files[0], output_path)
        return output_path

    ffmpeg = _resolve_ffmpeg()
    temp_output_path = output_path.with_name(f"{output_path.stem}.merge_tmp{output_path.suffix}")
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".txt", delete=False) as handle:
        list_path = Path(handle.name)
        handle.write("\n".join(_concat_list_entry(path) for path in files))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    try:
        _safe_unlink(temp_output_path)
        command = [
            ffmpeg,
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_path),
        ]
        if reencode:
            command.extend(["-c:v", "libx264", "-c:a", "aac", "-movflags", "+faststart"])
        else:
            command.extend(["-c", "copy"])
        command.append(str(temp_output_path))

        result = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            stderr = (result.stderr or b"").decode("utf-8", errors="ignore").strip()
            stdout = (result.stdout or b"").decode("utf-8", errors="ignore").strip()
            detail = stderr or stdout or "ffmpeg concat failed"
            mode_hint = ""
            if not reencode:
                mode_hint = " Try again with --reencode if the input MP4 files were encoded differently."
            raise RuntimeError(f"{detail}{mode_hint}")
        result = None
        gc.collect()
        _replace_with_retry(temp_output_path, output_path)
        return output_path
    finally:
        try:
            _safe_unlink(list_path)
        except Exception:
            pass
        try:
            _safe_unlink(temp_output_path)
        except Exception:
            pass
        gc.collect()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Merge all MP4 files in a folder in filename order.",
    )
    parser.add_argument(
        "folder",
        nargs="?",
        default=".",
        help="Folder containing MP4 files. Defaults to the current directory.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="merged.mp4",
        help="Output MP4 path. Defaults to ./merged.mp4",
    )
    parser.add_argument(
        "--reencode",
        action="store_true",
        help="Re-encode video/audio instead of stream copy. Use this if concat copy fails.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    folder = Path(args.folder)
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = folder / output_path

    merged_path = merge_mp4_folder(folder=folder, output_path=output_path, reencode=bool(args.reencode))
    print(f"Merged MP4 created: {merged_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
