from __future__ import annotations

import math
import struct
import wave
from pathlib import Path
from typing import Dict, List


class ClipAudioComposer:
    """Generate lightweight royalty-free style background loops for clip review renders."""

    SAMPLE_RATE = 22050

    STYLE_LIBRARY: Dict[str, Dict[str, object]] = {
        "informative_calm": {
            "tempo": 84,
            "volume": 0.12,
            "root_notes": [196.00, 220.00, 246.94, 196.00],
            "accent": 392.00,
            "pulse": 0.20,
        },
        "clean_explainer": {
            "tempo": 92,
            "volume": 0.11,
            "root_notes": [220.00, 246.94, 261.63, 220.00],
            "accent": 440.00,
            "pulse": 0.18,
        },
        "light_alert": {
            "tempo": 106,
            "volume": 0.12,
            "root_notes": [246.94, 293.66, 261.63, 246.94],
            "accent": 493.88,
            "pulse": 0.24,
        },
        "upbeat_lifestyle": {
            "tempo": 116,
            "volume": 0.13,
            "root_notes": [261.63, 329.63, 293.66, 349.23],
            "accent": 523.25,
            "pulse": 0.26,
        },
    }

    def synthesize_bgm(self, *, audio_path: Path, duration_sec: float, bgm_style: str) -> str:
        style_key = str(bgm_style or "clean_explainer").strip().lower() or "clean_explainer"
        profile = dict(self.STYLE_LIBRARY.get(style_key) or self.STYLE_LIBRARY["clean_explainer"])
        total_samples = max(1, int(float(max(1.0, duration_sec)) * self.SAMPLE_RATE))
        tempo = float(profile["tempo"])
        beat_sec = 60.0 / tempo
        fade_in = 0.9
        fade_out = 1.2
        roots: List[float] = [float(value) for value in profile["root_notes"]]
        accent = float(profile["accent"])
        pulse_strength = float(profile["pulse"])
        master_volume = float(profile["volume"])
        audio_path.parent.mkdir(parents=True, exist_ok=True)

        with wave.open(str(audio_path), "wb") as wav_file:
            wav_file.setnchannels(2)
            wav_file.setsampwidth(2)
            wav_file.setframerate(self.SAMPLE_RATE)
            frames = bytearray()
            for sample_index in range(total_samples):
                current_sec = sample_index / float(self.SAMPLE_RATE)
                phrase_index = int(current_sec / (beat_sec * 4.0)) % max(1, len(roots))
                beat_progress = (current_sec % beat_sec) / beat_sec
                chord_root = roots[phrase_index]
                chord = (
                    math.sin(2.0 * math.pi * chord_root * current_sec)
                    + 0.55 * math.sin(2.0 * math.pi * (chord_root * 1.25) * current_sec)
                    + 0.32 * math.sin(2.0 * math.pi * (chord_root * 1.50) * current_sec)
                ) / 1.87
                pulse_envelope = max(0.0, 1.0 - pow(beat_progress, 1.4))
                pulse = math.sin(2.0 * math.pi * accent * current_sec) * pulse_envelope * pulse_strength
                shimmer = math.sin(2.0 * math.pi * (accent * 0.50) * current_sec + 0.75) * 0.06
                signal = ((chord * 0.72) + pulse + shimmer) * master_volume
                if current_sec < fade_in:
                    signal *= current_sec / fade_in
                remaining = float(duration_sec) - current_sec
                if remaining < fade_out:
                    signal *= max(0.0, remaining / fade_out)
                signal = max(-0.92, min(0.92, signal))
                left = int(signal * 32767.0)
                right = int((signal * 0.94) * 32767.0)
                frames.extend(struct.pack("<hh", left, right))
            wav_file.writeframes(frames)
        return str(audio_path)
