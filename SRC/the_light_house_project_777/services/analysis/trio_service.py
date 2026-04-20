from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping

from core.model_router import resolve_model_roles

from .hard_reject import HardRejectEvaluator
from .service import ArticleAnalysisService


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _clip(score: float) -> float:
    return round(max(0.0, min(100.0, float(score))), 2)


class LocalLlmTrioArticleAnalysisService:
    """Runs the active local three-model set and merges their article analysis into one consensus."""

    BREAKDOWN_KEYS = {
        "reaction_breakdown": (
            "emotional_trigger",
            "inversion_surprise",
            "self_projection",
            "comparison_anxiety_hope_trigger",
        ),
        "pld_breakdown": (
            "entry_fit",
            "hook_fit",
            "loop_fit",
            "trust_fit",
        ),
        "operational_breakdown": (
            "content_transformation_ease",
            "question_based_framing_ease",
            "brand_safety",
            "moderation_platform_risk",
            "reviewer_confirmation_likelihood",
        ),
    }

    def __init__(
        self,
        *,
        base_analysis_service: ArticleAnalysisService,
        app_cfg: Dict[str, Any] | None = None,
        analysis_version: str = "phase1_selection_trio_v2",
    ) -> None:
        self.base_analysis_service = base_analysis_service
        self.analysis_version = analysis_version
        self.hard_reject_evaluator = HardRejectEvaluator()
        self.active_models = resolve_model_roles(app_cfg or {})

    def get_active_model_roles(self) -> Dict[str, str]:
        return dict(self.active_models)

    def analyze_article(self, article: Mapping[str, Any]) -> Dict[str, Any]:
        article_payload = dict(article)
        per_model: list[dict[str, Any]] = []
        for role in ("text", "coder", "reasoning"):
            model_name = str(self.active_models.get(role, "")).strip()
            if not model_name:
                continue
            analysis = self.base_analysis_service.analyze_article(
                article_payload,
                model_name=model_name,
                analysis_version=f"{self.analysis_version}:{role}",
            )
            per_model.append(
                {
                    "role": role,
                    "model": model_name,
                    "analysis": analysis,
                }
            )

        if not per_model:
            raise RuntimeError("No active local LLM trio models were resolved for News Collector analysis.")

        merged = self._merge_per_model_outputs(article_payload, per_model)
        hard_reject = self.hard_reject_evaluator.evaluate(article_payload, merged)
        merged["hard_reject_reason"] = hard_reject["hard_reject_reason"]
        merged["selection_status"] = "hard_rejected" if hard_reject["hard_reject"] else "scored"
        merged["analysis_model"] = " | ".join(f"{row['role']}={row['model']}" for row in per_model)
        merged["analysis_version"] = self.analysis_version
        merged["analyzed_at"] = _now_utc()
        merged["analysis_payload"] = {
            "analysis_mode": "local_llm_trio_consensus",
            "active_models": self.get_active_model_roles(),
            "reviewer_brief": merged.get("selection_summary", ""),
            "per_model": [
                {
                    "role": row["role"],
                    "model": row["model"],
                    "reaction_breakdown": row["analysis"].get("reaction_breakdown"),
                    "pld_breakdown": row["analysis"].get("pld_breakdown"),
                    "operational_breakdown": row["analysis"].get("operational_breakdown"),
                    "dominant_pld_stage": row["analysis"].get("dominant_pld_stage"),
                    "selection_summary": row["analysis"].get("selection_summary"),
                    "hard_reject_reason": row["analysis"].get("hard_reject_reason"),
                }
                for row in per_model
            ],
            "consensus": {
                "reaction_breakdown": merged.get("reaction_breakdown"),
                "pld_breakdown": merged.get("pld_breakdown"),
                "operational_breakdown": merged.get("operational_breakdown"),
                "dominant_pld_stage": merged.get("dominant_pld_stage"),
                "reaction_score": merged.get("reaction_score"),
                "pld_fit_score": merged.get("pld_fit_score"),
                "operational_score": merged.get("operational_score"),
                "final_score": merged.get("final_score"),
            },
        }
        return merged

    def _merge_per_model_outputs(
        self,
        article: Mapping[str, Any],
        per_model: Iterable[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        rows = list(per_model)
        merged: Dict[str, Any] = {}
        for breakdown_name, keys in self.BREAKDOWN_KEYS.items():
            merged[breakdown_name] = {
                key: _clip(
                    sum(float(((row.get("analysis") or {}).get(breakdown_name) or {}).get(key, 0.0)) for row in rows)
                    / max(len(rows), 1)
                )
                for key in keys
            }

        dominant_stage = self._pick_stage(rows)
        merged["dominant_pld_stage"] = dominant_stage
        merged.update(self.base_analysis_service.score_calculator.calculate(merged))
        merged["selection_summary"] = self._build_reviewer_baseline(article, merged)
        return merged

    def _pick_stage(self, rows: Iterable[Mapping[str, Any]]) -> str:
        stages = [
            str(((row.get("analysis") or {}).get("dominant_pld_stage") or "")).strip().lower()
            for row in rows
            if str(((row.get("analysis") or {}).get("dominant_pld_stage") or "")).strip()
        ]
        if not stages:
            return "trust"
        counts = Counter(stages)
        return counts.most_common(1)[0][0]

    def _build_reviewer_baseline(self, article: Mapping[str, Any], analysis: Mapping[str, Any]) -> str:
        stage = str(analysis.get("dominant_pld_stage") or "trust").strip() or "trust"
        source_name = str(article.get("source_name") or "trusted source").strip() or "trusted source"
        title = str(article.get("title") or "").strip()
        title_hint = title[:120].strip()
        pld_fit = float(analysis.get("pld_fit_score") or 0.0)
        operational = float(analysis.get("operational_score") or 0.0)
        reaction = float(analysis.get("reaction_score") or 0.0)
        reviewer_note = "Confirm the story can stay broad-entry, question-led, and psychologically safe before escalation."
        if stage == "entry":
            angle = "Lead with a universal human need instead of explicit identity claims."
        elif stage == "hook":
            angle = "Use an open question or surprising contrast without closing the loop too early."
        elif stage == "loop":
            angle = "Frame it as a reflection trigger that can stay with the reader through the day."
        else:
            angle = "Present it as a calm clarity framework, not as a persuasive push."
        return (
            f"{source_name} has a fresh article with {stage.upper()}-stage PLD fit "
            f"(PLD {pld_fit:.1f}, reaction {reaction:.1f}, operational {operational:.1f}). "
            f"{angle} {reviewer_note} Headline focus: {title_hint or 'untitled article'}."
        )
