from __future__ import annotations

import io
import json
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx
from PIL import Image, ImageDraw, ImageFont

from project_meta import PROJECT_ROOT

from .clip_motion import ClipMotionComposer
from .clip_template_renderer import WorkConnectClipTemplateRenderer
from .clip_video_renderer import ClipVideoRenderer
from .lmdb_store import normalize_source_url


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _norm_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


class WorkConnectClipsGenerator:
    CANVAS_SIZE = (1080, 1920)
    TARGET_SIZE = (1080, 1920)
    CATEGORY_WEIGHTS = (("visa", 30), ("jobs", 30), ("life_travel", 10), ("life_shopping", 10), ("life_housing", 10))
    FORMAT_LIBRARY: Dict[str, Dict[str, Any]] = {
        "warning": {
            "label": "Warning / Alert",
            "tone": "urgent but practical",
            "bgm_style": "light_alert",
            "motion_profile": "fade_zoom_mix",
            "structure": [("intro", 0, 3, "fade"), ("hook", 3, 6, "quick_cut"), ("point_1", 6, 12, "zoom_in"), ("point_2", 12, 18, "slow_pan"), ("point_3", 18, 24, "slow_pan"), ("cta", 24, 32, "fade")],
        },
        "checklist": {
            "label": "Checklist / Explainer",
            "tone": "clear and practical",
            "bgm_style": "clean_explainer",
            "motion_profile": "steady_explainer",
            "structure": [("intro", 0, 3, "fade"), ("topic", 3, 6, "zoom_in"), ("point_1", 6, 13, "slow_pan"), ("point_2", 13, 20, "slow_pan"), ("point_3", 20, 26, "zoom_in"), ("cta", 26, 35, "fade")],
        },
        "ranked": {
            "label": "Ranked / Lifestyle",
            "tone": "helpful and upbeat",
            "bgm_style": "upbeat_lifestyle",
            "motion_profile": "quick_ranked",
            "structure": [("intro", 0, 3, "fade"), ("hook", 3, 6, "quick_cut"), ("point_1", 6, 12, "quick_cut"), ("point_2", 12, 18, "quick_cut"), ("point_3", 18, 24, "zoom_in"), ("cta", 24, 32, "fade")],
        },
    }
    CATEGORY_TO_FORMATS = {
        "visa": ("warning", "checklist"),
        "jobs": ("warning", "checklist"),
        "life_travel": ("ranked", "checklist"),
        "life_shopping": ("ranked", "checklist"),
        "life_housing": ("checklist", "warning"),
    }
    RESEARCH_QUERY_BANK = {
        "visa": ["Korea visa foreigners rules", "Korea ARC renewal foreigners", "Korea immigration visa update foreign workers"],
        "jobs": ["Korea jobs foreigners contract workers", "Korea foreign workers employment tips", "Korea factory job contract foreign worker"],
        "life_travel": ["Korea travel tips foreigners newcomer", "Korea airport transport foreigners", "Seoul travel apps for foreigners"],
        "life_shopping": ["Korea shopping tips foreigners", "Korea grocery apps foreigners", "Korea delivery app foreigner tips"],
        "life_housing": ["Korea housing tips foreigners one room", "Korea rent deposit foreigners", "Korea one room contract guide"],
    }
    TOPIC_BANK = {
        "visa": [{"topic": "3 visa mistakes that delay your Korea stay", "hook": "One small visa mistake can block your next move in Korea.", "points": ["Check passport expiry before visa requests.", "Book immigration appointments earlier than you think.", "Keep contract and address proof ready."], "cta": "Need a Korea visa checklist? Ask WorkConnect."}],
        "jobs": [{"topic": "3 contract lines foreign workers should check first", "hook": "A short contract can hide the biggest risk.", "points": ["Check overtime rules before your first day.", "Ask who pays housing and insurance.", "If salary dates are vague, verify before signing."], "cta": "Send your contract to WorkConnect before you accept."}],
        "life_travel": [{"topic": "3 first-day travel tips after landing in Korea", "hook": "Your first day in Korea is easier if you set up the basics fast.", "points": ["Get mobile data before leaving the airport.", "Charge a T-money card for buses and subway.", "Save your home address in Korean and English."], "cta": "Follow WorkConnect for simple Korea arrival tips."}],
        "life_shopping": [{"topic": "How foreigners can save money on groceries in Korea", "hook": "Shopping like a tourist in Korea costs more than you think.", "points": ["Compare mart prices with delivery apps.", "Check evening discounts on fresh food.", "Share bulk basics with roommates when possible."], "cta": "Need more Korea living tips? WorkConnect can help."}],
        "life_housing": [{"topic": "How to check a one-room contract safely", "hook": "A cheap room can still be expensive if the contract is weak.", "points": ["Confirm penalties and move-in dates first.", "Ask for utility and management fee details.", "Take damage photos before your first night."], "cta": "Ask WorkConnect before you sign a housing deal."}],
    }
    PALETTES = {
        "visa": ("#0B3954", "#1F7A8C", "#BFD7EA"),
        "jobs": ("#3D348B", "#7678ED", "#F7B801"),
        "life_travel": ("#0E6BA8", "#3FA7D6", "#F0F3BD"),
        "life_shopping": ("#5C415D", "#B5838D", "#FFCAD4"),
        "life_housing": ("#2F5233", "#519872", "#C5D86D"),
    }

    def __init__(self, project_root: str, naver_client: Optional[Any] = None) -> None:
        self.project_root = Path(project_root)
        self.repo_root = PROJECT_ROOT
        self.logo_path = self.repo_root / "assets" / "workconnect_logo.png"
        self.output_root = self.project_root / "data" / "content_clips"
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.naver = naver_client
        self.motion = ClipMotionComposer()
        self.video_renderer = ClipVideoRenderer(canvas_size=self.CANVAS_SIZE, target_size=self.TARGET_SIZE)
        self.template_renderer = WorkConnectClipTemplateRenderer(canvas_size=self.CANVAS_SIZE, motion=self.motion, logo_path=self.logo_path)

    def build_clip_package(self, quality_profile: Dict[str, Any], recent_topic_slugs: Sequence[str]) -> Dict[str, Any]:
        clip_id = f"clip_{int(time.time() * 1000)}"
        clip_dir = self.output_root / clip_id
        clip_dir.mkdir(parents=True, exist_ok=True)
        category_key = self._pick_category(quality_profile)
        format_plan = self.plan_clip_format(category=category_key, recent_content_history=recent_topic_slugs, performance_hints=quality_profile)
        research = self.research_clip_topic(format_plan=format_plan, clip_dir=clip_dir)
        content = self.synthesize_clip_content(format_plan=format_plan, research=research)
        visuals = self.collect_visual_candidates(format_plan=format_plan, research=research, content=content)
        scenes = self.plan_clip_scenes(format_plan=format_plan, content=content, visuals=visuals)
        scenes = self._build_timeline_scenes(
            scenes=scenes,
            target_duration_sec=min(30.0, max(20.0, float(content.get("duration_sec", 28.0) or 28.0))),
        )
        composition = self.compose_clip_video(clip_id=clip_id, clip_dir=clip_dir, format_plan=format_plan, content=content, scenes=scenes, visuals=visuals)
        manifest = {
            "clip_id": clip_id,
            "mode": "workconnect_clips",
            "format": format_plan["selected_format"],
            "format_label": format_plan["format_label"],
            "category": self._display_category(category_key),
            "category_key": category_key,
            "topic": content["topic"],
            "title": content["title"],
            "hook": content["hook"],
            "summary": content["summary"],
            "voiceover": content["voiceover"],
            "cta": content["cta"],
            "target_format": "mp4",
            "render_status": composition["render_status"],
            "preview_format": "gif",
            "video_format": "mp4",
            "logo_path": str(self.logo_path),
            "preview_resolution": {"width": self.CANVAS_SIZE[0], "height": self.CANVAS_SIZE[1]},
            "target_resolution": {"width": self.TARGET_SIZE[0], "height": self.TARGET_SIZE[1]},
            "target_fps": composition["fps"],
            "target_duration_sec": composition.get("duration_sec", content["duration_sec"]),
            "target_total_frames": composition.get("total_video_frames", 0),
            "topic_intent": format_plan["topic_intent"],
            "audience_intent": format_plan["audience_intent"],
            "target_tone": format_plan["target_tone"],
            "motion_profile": format_plan["motion_profile"],
            "bgm_style": format_plan["bgm_style"],
            "research_quality": research["research_quality"],
            "sources_used_count": len(research["sources_used"]),
            "source_titles": [row.get("title", "") for row in research["sources_used"]],
            "source_urls": [row.get("url", "") for row in research["sources_used"]],
            "visual_coverage": visuals["visual_coverage"],
            "fallback_used": research["fallback_used"],
            "review_status": "pending",
            "research": {"keywords": research["extracted_keywords"], "topic_summary": research["topic_summary"], "evidence_snippets": research["evidence_snippets"]},
            "visuals": visuals,
            "scenes": scenes,
            "files": composition["files"],
            "encoder_error": composition.get("encoder_error", ""),
            "created_at": _now_iso(),
        }
        manifest_path = clip_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        script_path = clip_dir / "script.txt"
        script_path.write_text(content["voiceover"], encoding="utf-8")
        return {
            "clip_id": clip_id,
            "mode": "workconnect_clips",
            "category": self._display_category(category_key),
            "category_key": category_key,
            "format": format_plan["selected_format"],
            "format_label": format_plan["format_label"],
            "topic": content["topic"],
            "topic_slug": _slugify(content["topic"]),
            "title": content["title"],
            "summary": content["summary"],
            "hook": content["hook"],
            "key_message": content["key_message"],
            "voiceover": content["voiceover"],
            "cta": content["cta"],
            "duration_sec": content["duration_sec"],
            "research_quality": research["research_quality"],
            "sources_used_count": len(research["sources_used"]),
            "source_titles": [row.get("title", "") for row in research["sources_used"]],
            "source_urls": [row.get("url", "") for row in research["sources_used"]],
            "visual_coverage": visuals["visual_coverage"],
            "fallback_used": research["fallback_used"],
            "motion_profile": format_plan["motion_profile"],
            "bgm_style": format_plan["bgm_style"],
            "target_tone": format_plan["target_tone"],
            "audience_intent": format_plan["audience_intent"],
            "topic_intent": format_plan["topic_intent"],
            "research_keywords": research["extracted_keywords"],
            "evidence_snippets": research["evidence_snippets"],
            "scenes": scenes,
            "visuals": visuals,
            "render_status": composition["render_status"],
            "preview_format": "gif",
            "video_format": "mp4",
            "logo_path": str(self.logo_path),
            "preview_path": composition["files"]["preview_gif_path"],
            "video_path": composition["files"]["video_path"],
            "poster_path": composition["files"]["poster_path"],
            "manifest_path": str(manifest_path),
            "script_path": str(script_path),
            "target_resolution": f"{self.TARGET_SIZE[0]}x{self.TARGET_SIZE[1]}",
            "preview_resolution": f"{self.CANVAS_SIZE[0]}x{self.CANVAS_SIZE[1]}",
            "target_fps": composition["fps"],
            "target_duration_sec": composition.get("duration_sec", content["duration_sec"]),
            "target_total_frames": composition.get("total_video_frames", 0),
            "encoder_error": composition.get("encoder_error", ""),
            "review_status": "pending",
        }

    def plan_clip_format(self, *, category: str, recent_content_history: Sequence[str], performance_hints: Dict[str, Any]) -> Dict[str, Any]:
        category_key = str(category or "").strip().lower() or "jobs"
        allowed_formats = self.CATEGORY_TO_FORMATS.get(category_key, ("checklist", "warning", "ranked"))
        format_stats = performance_hints.get("format_stats") if isinstance(performance_hints, dict) else {}
        scores = []
        for format_key in allowed_formats:
            row = format_stats.get(format_key) if isinstance(format_stats, dict) else {}
            approved = float((row or {}).get("approved", 0) or 0)
            rejected = float((row or {}).get("rejected", 0) or 0)
            scores.append((format_key, 1.0 + approved - (rejected * 0.6)))
        scores.sort(key=lambda item: item[1], reverse=True)
        top = scores[0][1]
        selected_format = random.choice([fmt for fmt, score in scores if score >= top - 0.3])
        meta = self.FORMAT_LIBRARY[selected_format]
        return {
            "selected_format": selected_format,
            "format_label": meta["label"],
            "category_key": category_key,
            "topic_intent": {"warning": "mistake prevention and risk reduction", "checklist": "step-by-step preparation", "ranked": "ranked lifestyle utility"}.get(selected_format, "helpful guidance"),
            "audience_intent": "foreign workers in Korea or planning to move",
            "target_tone": meta["tone"],
            "motion_profile": meta["motion_profile"],
            "bgm_style": meta["bgm_style"],
            "structure": [{"label": label, "start_sec": start, "end_sec": end, "motion_style": motion} for label, start, end, motion in meta["structure"]],
            "research_queries": self._build_research_queries(category_key, selected_format),
            "recent_topic_memory": len([item for item in recent_content_history if item]),
        }

    def research_clip_topic(self, *, format_plan: Dict[str, Any], clip_dir: Path) -> Dict[str, Any]:
        sources = self._research_sources(format_plan["research_queries"], limit=5)
        sources = self._enrich_sources(sources)
        evidence, keywords = [], []
        for row in sources:
            text = " ".join([str(row.get("title", "")), str(row.get("summary", "")), str(row.get("content", ""))]).strip()
            keywords.extend(self._extract_keywords(text))
            evidence.extend(self._extract_sentences(text, max_items=2))
        keywords = self._dedupe(keywords)[:12]
        evidence = self._dedupe(evidence)[:6]
        fallback_used = False
        research_quality = "high"
        if len(sources) < 3:
            research_quality = "low"
        elif len(evidence) < 3:
            research_quality = "medium"
        if not sources:
            fallback_used = True
            research_quality = "low"
            fallback = self._fallback_topic_row(format_plan["category_key"])
            sources = [{"title": fallback["topic"], "summary": fallback["hook"], "content": " ".join(fallback["points"]), "url": "", "thumbnail_url": "", "source": "fallback_topic_bank"}]
            evidence = [fallback["hook"], *fallback["points"]][:4]
            keywords = self._extract_keywords(" ".join([fallback["topic"], fallback["hook"], *fallback["points"]]))[:10]
        payload = {
            "sources_used": sources,
            "extracted_keywords": keywords,
            "topic_summary": "; ".join([str(row.get("title", "")) for row in sources[:3] if str(row.get("title", "")).strip()]),
            "evidence_snippets": evidence,
            "research_quality": research_quality,
            "fallback_used": fallback_used,
        }
        (clip_dir / "research.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    def synthesize_clip_content(self, *, format_plan: Dict[str, Any], research: Dict[str, Any]) -> Dict[str, Any]:
        category_key = str(format_plan.get("category_key", "")).strip().lower()
        selected_format = str(format_plan.get("selected_format", "")).strip().lower()
        fallback = self._fallback_topic_row(category_key)
        sources = research.get("sources_used") or []
        title_seed = str(((sources[0] if sources else {}) or {}).get("title", "")).strip() or fallback["topic"]
        points = self._build_points(research.get("evidence_snippets") or [], fallback["points"])
        hook = fallback["hook"]
        if selected_format == "warning":
            hook = f"Stop and check this first: {title_seed[:68].rstrip(' .,')}"
        elif selected_format == "ranked":
            hook = "These are the fastest tips to know before you try this in Korea."
        elif selected_format == "checklist":
            hook = "Here is the simple checklist before you do this in Korea."
        topic = title_seed[:72].rstrip(" .,-") or fallback["topic"]
        title = f"{self._display_category(category_key)} Clip: {topic}"
        summary = f"{self.FORMAT_LIBRARY[selected_format]['label']} clip for foreigners in Korea. Topic: {topic}. Focus: {'; '.join(points[:2])}"
        voiceover = " ".join(["WorkConnect Korea Guide.", hook, *points[:3], fallback["cta"]]).strip()
        return {"topic": topic, "title": title, "hook": hook, "points": points[:3], "cta": fallback["cta"], "summary": summary, "voiceover": voiceover, "key_message": points[0] if points else hook, "duration_sec": 35 if selected_format == "checklist" else 32}

    def collect_visual_candidates(self, *, format_plan: Dict[str, Any], research: Dict[str, Any], content: Dict[str, Any]) -> Dict[str, Any]:
        sources = research.get("sources_used") or []
        scene_visuals, coverage = [], 0
        for idx, label in enumerate(["hook", "point_1", "point_2", "point_3", "cta"]):
            row = sources[idx] if idx < len(sources) else {}
            visual_hint = self._visual_hint(label, format_plan["category_key"])
            selected = self._choose_scene_visual(
                topic=str(content.get("topic", "")).strip(),
                visual_hint=visual_hint,
                label=label,
                source_row=row,
            )
            image_url = str(selected.get("image_url", "")).strip()
            ready = bool(image_url)
            if ready:
                coverage += 1
            scene_visuals.append(
                {
                    "scene_label": label,
                    "image_url": image_url,
                    "source_title": str(selected.get("source_title", "")).strip() or str(row.get("title", "")).strip(),
                    "source_url": str(selected.get("source_url", "")).strip() or str(row.get("url", "")).strip(),
                    "scene_ready": ready,
                    "visual_hint": visual_hint,
                    "visual_score": round(float(selected.get("score", 0.0) or 0.0), 2),
                    "selected_from": str(selected.get("selected_from", "")).strip(),
                }
            )
        return {
            "tags": self._extract_keywords(" ".join([content.get("topic", ""), content.get("hook", "")]))[:8],
            "candidates": [
                {
                    "source_title": str(row.get("title", "")).strip(),
                    "source_url": str(row.get("url", "")).strip(),
                    "image_url": str(row.get("thumbnail_url", "")).strip(),
                }
                for row in sources[:6]
            ],
            "scene_visuals": scene_visuals,
            "visual_coverage": round(float(coverage) / float(max(1, len(scene_visuals))), 2),
        }

    def plan_clip_scenes(self, *, format_plan: Dict[str, Any], content: Dict[str, Any], visuals: Dict[str, Any]) -> List[Dict[str, Any]]:
        by_label = {str(row.get("scene_label", "")): row for row in visuals.get("scene_visuals") or []}
        points, scenes = list(content.get("points") or []), []
        for row in format_plan.get("structure") or []:
            label, visual = str(row.get("label", "")).strip(), by_label.get(str(row.get("label", "")).strip(), {})
            text = "WorkConnect\nKorea Guide" if label == "intro" else content.get("hook", "") if label in {"hook", "topic"} else content.get("cta", "") if label == "cta" else ""
            voiceover = "WorkConnect Korea Guide." if label == "intro" else content.get("hook", "") if label in {"hook", "topic"} else content.get("cta", "") if label == "cta" else ""
            if label == "point_1" and len(points) >= 1:
                text, voiceover = f"1. {points[0]}", points[0]
            if label == "point_2" and len(points) >= 2:
                text, voiceover = f"2. {points[1]}", points[1]
            if label == "point_3" and len(points) >= 3:
                text, voiceover = f"3. {points[2]}", points[2]
            scenes.append({"label": label, "start_sec": row.get("start_sec"), "end_sec": row.get("end_sec"), "on_screen_text": text, "voiceover_line": voiceover, "visual_hint": visual.get("visual_hint", ""), "image_url": visual.get("image_url", ""), "scene_ready": bool(visual.get("scene_ready", False)), "source_title": visual.get("source_title", ""), "source_url": visual.get("source_url", ""), "motion_style": row.get("motion_style", "fade")})
        return scenes

    def compose_clip_video(self, *, clip_id: str, clip_dir: Path, format_plan: Dict[str, Any], content: Dict[str, Any], scenes: Sequence[Dict[str, Any]], visuals: Dict[str, Any]) -> Dict[str, Any]:
        palette = self.PALETTES.get(str(format_plan.get("category_key", "")).strip().lower(), self.PALETTES["jobs"])
        category_label = self._display_category(str(format_plan.get("category_key", "")).strip().lower())
        scene_assets = []
        for scene in scenes:
            scene_assets.append({"scene": dict(scene), "background": self._load_scene_background(scene, clip_dir)})
        total_duration_sec = max(float(scenes[-1].get("end_sec", 28.0) or 28.0), 20.0) if scenes else 20.0
        total_frames = max(2, int(round(total_duration_sec * self.video_renderer.fps)))
        video_frames: List[Image.Image] = []
        for frame_index in range(total_frames):
            current_time_sec = frame_index / float(self.video_renderer.fps)
            timeline = self._resolve_timeline_state(scene_assets=scene_assets, current_time_sec=current_time_sec)
            video_frames.append(
                self.template_renderer.render_timeline_frame(
                    title=content.get("title", ""),
                    palette=palette,
                    category_label=category_label,
                    current_time_sec=current_time_sec,
                    total_duration_sec=total_duration_sec,
                    current_scene=timeline["current_scene"],
                    current_background=timeline["current_background"],
                    scene_progress=timeline["scene_progress"],
                    transition_progress=timeline["transition_progress"],
                    from_scene=timeline.get("from_scene"),
                    from_background=timeline.get("from_background"),
                    to_scene=timeline.get("to_scene"),
                    to_background=timeline.get("to_background"),
                )
            )
        preview_frames, preview_durations_ms = self._build_preview_frames(
            video_frames=video_frames,
            total_duration_sec=total_duration_sec,
        )
        return self.video_renderer.render_assets(
            clip_id=clip_id,
            clip_dir=clip_dir,
            preview_frames=preview_frames,
            video_frames=video_frames,
            preview_durations_ms=preview_durations_ms,
            visual_coverage=float(visuals.get("visual_coverage", 0.0) or 0.0),
            bgm_style=str(format_plan.get("bgm_style", "")).strip(),
        )

    def _research_sources(self, queries: Sequence[str], limit: int) -> List[Dict[str, Any]]:
        if not self.naver or not getattr(self.naver, "configured", False):
            return []
        headers = {"X-Naver-Client-Id": self.naver.client_id, "X-Naver-Client-Secret": self.naver.client_secret}
        rows, seen = [], set()
        for query in queries:
            try:
                with httpx.Client(timeout=self.naver.timeout, follow_redirects=True) as client:
                    response = client.get(self.naver.base_url, headers=headers, params={"query": query, "display": max(1, min(limit, 10)), "start": 1, "sort": "date"})
                    response.raise_for_status()
                    payload = response.json()
                for item in payload.get("items") or []:
                    url = str((item or {}).get("originallink") or (item or {}).get("link") or "").strip()
                    normalized = normalize_source_url(url)
                    key = normalized.get("canonical_article_id") or normalized.get("normalized_url") or url
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    rows.append({"query": query, "title": self.naver._strip_html((item or {}).get("title", "")), "summary": self.naver._strip_html((item or {}).get("description", "")), "url": url, "thumbnail_url": "", "published_at": self.naver._parse_pub_date((item or {}).get("pubDate")), "source": "naver_search"})
                    if len(rows) >= limit:
                        return rows
            except Exception:
                continue
        return rows

    def _enrich_sources(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        enriched = []
        for row in rows:
            current = dict(row or {})
            url = str(current.get("url", "")).strip()
            if self.naver and url:
                try:
                    detail = self.naver.get_article_content(url)
                    if detail.get("ok"):
                        current["content"] = str(detail.get("content", "")).strip()
                        current["thumbnail_url"] = str(detail.get("thumbnail_url", "") or current.get("thumbnail_url", "")).strip()
                except Exception:
                    pass
                if not current.get("thumbnail_url"):
                    try:
                        thumb = self.naver.get_article_thumbnail(url)
                        if thumb.get("ok"):
                            current["thumbnail_url"] = str(thumb.get("thumbnail_url", "")).strip()
                    except Exception:
                        pass
            enriched.append(current)
        return enriched

    def _build_research_queries(self, category_key: str, selected_format: str) -> List[str]:
        base = list(self.RESEARCH_QUERY_BANK.get(category_key) or self.RESEARCH_QUERY_BANK["jobs"])
        suffix = {"warning": "warning tips", "checklist": "checklist guide", "ranked": "best top 3"}.get(selected_format, "")
        return [f"{query} {suffix}".strip() for query in base[:4]]

    def _extract_keywords(self, text: str) -> List[str]:
        stop = {"korea", "korean", "foreigners", "foreigner", "workers", "worker", "with", "that", "this", "from", "into", "your", "before", "after"}
        return [token for token in re.split(r"[^a-z0-9]+", _norm_text(text)) if len(token) >= 3 and token not in stop]

    def _extract_sentences(self, text: str, max_items: int) -> List[str]:
        parts = re.split(r"(?<=[.!?])\s+", str(text or "").strip())
        return [self._shorten(item) for item in parts if len(item.strip()) >= 20][:max_items]

    def _fallback_topic_row(self, category_key: str) -> Dict[str, Any]:
        return dict(random.choice(list(self.TOPIC_BANK.get(category_key) or self.TOPIC_BANK["jobs"])))

    def _build_points(self, evidence: Sequence[str], fallback_points: Sequence[str]) -> List[str]:
        points = self._dedupe([self._shorten(item) for item in evidence if str(item or "").strip()])[:3]
        if len(points) < 3:
            points.extend([self._shorten(item) for item in fallback_points if self._shorten(item) not in points])
        return points[:3]

    def _visual_hint(self, label: str, category_key: str) -> str:
        base = {"visa": "immigration office, passport, visa papers", "jobs": "factory floor, contract document, workplace", "life_travel": "airport, subway, city movement", "life_shopping": "mart aisle, phone app, groceries", "life_housing": "one-room interior, housing contract, key"}.get(category_key, "Korea city and daily life")
        return "WorkConnect brand support" if label == "cta" else base

    def _search_visual_image(self, *, topic: str, visual_hint: str, label: str) -> Dict[str, Any]:
        if not self.naver:
            return {}
        query = " ".join(
            [
                str(topic or "").strip(),
                str(visual_hint or "").strip(),
                str(label or "").replace("_", " ").strip(),
                "Korea",
            ]
        ).strip()
        try:
            rows = self.naver.search_images(query, limit=6)
        except Exception:
            rows = []
        return self._pick_visual_candidate(
            rows=rows,
            topic=topic,
            visual_hint=visual_hint,
            label=label,
            selected_from="search",
            source_title="",
            source_url="",
        )

    def _load_scene_background(self, scene: Dict[str, Any], clip_dir: Path) -> Optional[Image.Image]:
        image_url = str(scene.get("image_url", "")).strip()
        if not image_url:
            return None
        try:
            with httpx.Client(timeout=8.0, follow_redirects=True) as client:
                response = client.get(image_url)
                response.raise_for_status()
            image = Image.open(io.BytesIO(response.content)).convert("RGB")
            if image.width < self.TARGET_SIZE[0] or image.height < self.TARGET_SIZE[1]:
                return None
            ratio = image.width / max(1, image.height)
            if ratio < 0.45 or ratio > 2.6:
                return None
            image.save(clip_dir / f"{str(scene.get('label', 'scene'))}.jpg", format="JPEG", quality=88)
            return image
        except Exception:
            return None

    def _build_frame(
        self,
        *,
        scene: Dict[str, Any],
        title: str,
        palette: Tuple[str, str, str],
        background: Optional[Image.Image],
        progress: float,
        motion_style: str,
    ) -> Image.Image:
        return self.template_renderer.render_frame(
            scene=scene,
            title=title,
            palette=palette,
            background=background,
            progress=progress,
            motion_style=motion_style,
            category_label=self._display_category("jobs"),
        )

    def _fit_background(self, image: Image.Image, width: int, height: int, progress: float, motion_style: str) -> Image.Image:
        return self.motion.fit_background(
            image,
            width=width,
            height=height,
            progress=progress,
            motion_style=motion_style,
        )

    def _draw_text(self, draw: ImageDraw.ImageDraw, box: Tuple[int, int, int, int], text: str, font: ImageFont.FreeTypeFont | ImageFont.ImageFont) -> None:
        x0, y0, x1, y1 = box
        lines, current = [], ""
        for word in str(text or "").split():
            probe = f"{current} {word}".strip()
            if current and draw.textlength(probe, font=font) > (x1 - x0):
                lines.append(current)
                current = word
            else:
                current = probe
        if current:
            lines.append(current)
        line_height = int((font.size if hasattr(font, "size") else 28) * 1.28)
        start_y = y0 + max(0, ((y1 - y0) - (line_height * max(1, len(lines)))) // 2)
        for idx, line in enumerate(lines or [""]):
            draw.text((x0, start_y + (idx * line_height)), line, fill="#FFFFFF", font=font)

    def _font(self, size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        for candidate in ["C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf", "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf"]:
            try:
                return ImageFont.truetype(candidate, size=size)
            except Exception:
                continue
        return ImageFont.load_default()

    def _hex_to_rgb(self, value: str) -> Tuple[int, int, int]:
        text = str(value or "").strip().lstrip("#")
        return tuple(int(text[i:i + 2], 16) for i in (0, 2, 4)) if len(text) == 6 else (32, 32, 32)

    def _display_category(self, category_key: str) -> str:
        return {"visa": "Visa", "jobs": "Jobs", "life_travel": "Life / Travel", "life_shopping": "Life / Shopping", "life_housing": "Life / Housing"}.get(category_key, "Life")

    def _shorten(self, value: str) -> str:
        text = re.sub(r"\s+", " ", str(value or "").strip())
        return f"{text[:117].rstrip(' ,;:')}..." if len(text) > 120 else text

    def _dedupe(self, rows: Sequence[str]) -> List[str]:
        out, seen = [], set()
        for row in rows:
            value = str(row or "").strip()
            key = value.lower()
            if not value or key in seen:
                continue
            seen.add(key)
            out.append(value)
        return out

    def _append_scene_frames(self, *, current_frames: List[Image.Image], scene_frames: Sequence[Image.Image], motion_style: str) -> List[Image.Image]:
        if not current_frames:
            return list(scene_frames)
        overlap = 0 if str(motion_style or "").strip().lower() == "quick_cut" else max(8, int(round(self.video_renderer.fps * 0.42)))
        overlap = min(overlap, len(current_frames), len(scene_frames))
        combined = list(current_frames)
        if overlap > 0:
            for index in range(overlap):
                alpha = (index + 1) / float(overlap + 1)
                combined[-overlap + index] = self.motion.blend_frames(
                    combined[-overlap + index],
                    scene_frames[index],
                    alpha=alpha,
                )
            combined.extend(list(scene_frames)[overlap:])
            return combined
        combined.extend(scene_frames)
        return combined

    def _build_preview_frames(self, *, video_frames: Sequence[Image.Image], total_duration_sec: float) -> Tuple[List[Image.Image], List[int]]:
        if not video_frames:
            return [], []
        sample_count = min(12, max(6, int(round(float(total_duration_sec) / 2.8))))
        indexes = sorted({int(round((len(video_frames) - 1) * (index / max(1, sample_count - 1)))) for index in range(sample_count)})
        frames = [video_frames[index].copy() for index in indexes]
        duration_ms = max(180, int((float(total_duration_sec) * 1000.0) / max(1, len(frames))))
        return frames, [duration_ms for _ in frames]

    def _build_timeline_scenes(self, *, scenes: Sequence[Dict[str, Any]], target_duration_sec: float) -> List[Dict[str, Any]]:
        intro_duration = 1.5
        rows = [dict(scene) for scene in scenes]
        if not rows:
            return []
        intro_scene = next((dict(scene) for scene in rows if str(scene.get("label", "")).strip().lower() == "intro"), {"label": "intro"})
        intro_scene["start_sec"] = 0.0
        intro_scene["end_sec"] = intro_duration
        content_rows = [dict(scene) for scene in rows if str(scene.get("label", "")).strip().lower() != "intro"]
        if not content_rows:
            return [intro_scene]
        original_start = min(float(scene.get("start_sec", 0.0) or 0.0) for scene in content_rows)
        original_end = max(float(scene.get("end_sec", original_start + 1.0) or (original_start + 1.0)) for scene in content_rows)
        original_span = max(0.1, original_end - original_start)
        content_window = max(6.0, float(target_duration_sec) - intro_duration)
        timeline_rows: List[Dict[str, Any]] = [intro_scene]
        for scene in content_rows:
            start = float(scene.get("start_sec", original_start) or original_start)
            end = float(scene.get("end_sec", start + 1.0) or (start + 1.0))
            mapped_start = intro_duration + (((start - original_start) / original_span) * content_window)
            mapped_end = intro_duration + (((end - original_start) / original_span) * content_window)
            scene["start_sec"] = round(mapped_start, 3)
            scene["end_sec"] = round(max(mapped_start + 0.8, mapped_end), 3)
            timeline_rows.append(scene)
        timeline_rows[-1]["end_sec"] = round(float(target_duration_sec), 3)
        return timeline_rows

    def _resolve_timeline_state(self, *, scene_assets: Sequence[Dict[str, Any]], current_time_sec: float) -> Dict[str, Any]:
        intro_duration = 1.5
        if float(current_time_sec) < intro_duration:
            intro_scene = next((dict(row.get("scene", {})) for row in scene_assets if str(row.get("scene", {}).get("label", "")).strip().lower() == "intro"), {"label": "intro", "start_sec": 0.0, "end_sec": intro_duration})
            return {
                "current_scene": intro_scene,
                "current_background": None,
                "scene_progress": max(0.0, min(1.0, float(current_time_sec) / intro_duration)),
                "transition_progress": 0.0,
                "from_scene": None,
                "from_background": None,
                "to_scene": None,
                "to_background": None,
            }
        content_assets = [row for row in scene_assets if str(row.get("scene", {}).get("label", "")).strip().lower() != "intro"]
        if not content_assets:
            return {
                "current_scene": {"label": "cta", "on_screen_text": "", "visual_hint": "", "motion_style": "fade"},
                "current_background": None,
                "scene_progress": 0.0,
                "transition_progress": 0.0,
                "from_scene": None,
                "from_background": None,
                "to_scene": None,
                "to_background": None,
            }
        active_index = len(content_assets) - 1
        for index, row in enumerate(content_assets):
            scene = row.get("scene", {})
            start = float(scene.get("start_sec", intro_duration) or intro_duration)
            end = float(scene.get("end_sec", start + 1.0) or (start + 1.0))
            if start <= float(current_time_sec) < end:
                active_index = index
                break
        active_row = content_assets[active_index]
        current_scene = dict(active_row.get("scene", {}))
        current_background = active_row.get("background")
        scene_start = float(current_scene.get("start_sec", intro_duration) or intro_duration)
        scene_end = float(current_scene.get("end_sec", scene_start + 1.0) or (scene_start + 1.0))
        scene_span = max(0.1, scene_end - scene_start)
        scene_progress = max(0.0, min(1.0, (float(current_time_sec) - scene_start) / scene_span))
        transition_duration = max(0.4, min(0.8, scene_span * 0.22))
        from_scene = current_scene
        from_background = current_background
        to_scene = None
        to_background = None
        transition_progress = 0.0
        if active_index > 0 and float(current_time_sec) < (scene_start + transition_duration):
            previous_row = content_assets[active_index - 1]
            from_scene = dict(previous_row.get("scene", {}))
            from_background = previous_row.get("background")
            to_scene = current_scene
            to_background = current_background
            transition_progress = max(0.0, min(1.0, (float(current_time_sec) - scene_start) / transition_duration))
        elif active_index < (len(content_assets) - 1) and float(current_time_sec) > (scene_end - transition_duration):
            next_row = content_assets[active_index + 1]
            from_scene = current_scene
            from_background = current_background
            to_scene = dict(next_row.get("scene", {}))
            to_background = next_row.get("background")
            transition_progress = max(0.0, min(1.0, (float(current_time_sec) - (scene_end - transition_duration)) / transition_duration))
        return {
            "current_scene": current_scene,
            "current_background": current_background,
            "scene_progress": scene_progress,
            "transition_progress": transition_progress,
            "from_scene": from_scene,
            "from_background": from_background,
            "to_scene": to_scene,
            "to_background": to_background,
        }

    def _choose_scene_visual(self, *, topic: str, visual_hint: str, label: str, source_row: Dict[str, Any]) -> Dict[str, Any]:
        article_candidate = self._pick_visual_candidate(
            rows=[
                {
                    "image_url": str(source_row.get("thumbnail_url", "")).strip(),
                    "title": str(source_row.get("title", "")).strip(),
                    "query": topic,
                }
            ],
            topic=topic,
            visual_hint=visual_hint,
            label=label,
            selected_from="article_thumbnail",
            source_title=str(source_row.get("title", "")).strip(),
            source_url=str(source_row.get("url", "")).strip(),
        )
        if article_candidate.get("image_url"):
            return article_candidate
        return self._search_visual_image(topic=topic, visual_hint=visual_hint, label=label)

    def _pick_visual_candidate(
        self,
        *,
        rows: Sequence[Dict[str, Any]],
        topic: str,
        visual_hint: str,
        label: str,
        selected_from: str,
        source_title: str,
        source_url: str,
    ) -> Dict[str, Any]:
        best: Dict[str, Any] = {}
        best_score = float("-inf")
        tokens = set(self._extract_keywords(" ".join([topic, visual_hint, label])))
        for row in rows or []:
            candidate_url = str(row.get("image_url") or row.get("thumbnail_url") or "").strip()
            candidate_title = str(row.get("title", "")).strip()
            width = row.get("sizewidth")
            height = row.get("sizeheight")
            reject, score = self._score_visual_candidate(
                image_url=candidate_url,
                title=candidate_title,
                width=width,
                height=height,
                topic_tokens=tokens,
            )
            if reject:
                continue
            if score > best_score:
                best_score = score
                best = {
                    "image_url": candidate_url,
                    "source_title": source_title or candidate_title,
                    "source_url": source_url,
                    "score": score,
                    "selected_from": selected_from,
                }
        return best

    def _score_visual_candidate(
        self,
        *,
        image_url: str,
        title: str,
        width: Any,
        height: Any,
        topic_tokens: Sequence[str],
    ) -> Tuple[bool, float]:
        url = str(image_url or "").strip()
        meta = f"{str(title or '').lower()} {url.lower()}".strip()
        if not url:
            return True, -10.0
        bad_terms = ("logo", "wordmark", "favicon", "header", "banner", "masthead", "sprite", "icon", "brand", "avatar")
        if any(term in meta for term in bad_terms):
            return True, -9.0
        try:
            width_value = int(width or 0)
            height_value = int(height or 0)
        except Exception:
            width_value, height_value = 0, 0
        if width_value and height_value:
            if min(width_value, height_value) < 320:
                return True, -8.0
            ratio = width_value / max(1, height_value)
            if ratio < 0.45 or ratio > 2.6:
                return True, -7.0
        score = 0.0
        if width_value and height_value:
            score += min(width_value, height_value) / 500.0
        lower = meta.lower()
        for token in topic_tokens:
            if token and token in lower:
                score += 0.35
        if any(term in lower for term in ("office", "worker", "housing", "visa", "travel", "shopping", "korea", "apartment", "airport", "contract")):
            score += 0.55
        if url.lower().endswith((".jpg", ".jpeg", ".webp")):
            score += 0.2
        return False, score

    def _scene_render_duration_sec(self, scene: Dict[str, Any]) -> float:
        try:
            start = float(scene.get("start_sec", 0) or 0)
            end = float(scene.get("end_sec", 0) or 0)
            hinted = max(0.0, end - start)
        except Exception:
            hinted = 0.0
        if hinted <= 0:
            return 1.6
        return hinted

    def _pick_category(self, quality_profile: Dict[str, Any]) -> str:
        stats = quality_profile.get("category_stats") if isinstance(quality_profile, dict) else {}
        weighted = []
        for key, base in self.CATEGORY_WEIGHTS:
            row = stats.get(key) if isinstance(stats, dict) else {}
            weighted.append((key, max(1.0, float(base) + float((row or {}).get("approved", 0) or 0) * 2.0 - float((row or {}).get("rejected", 0) or 0))))
        total = sum(weight for _, weight in weighted) or 1.0
        pick = random.random() * total
        upto = 0.0
        for key, weight in weighted:
            upto += weight
            if pick <= upto:
                return key
        return weighted[0][0]
