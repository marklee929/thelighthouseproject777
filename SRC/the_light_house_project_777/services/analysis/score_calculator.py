from __future__ import annotations

from typing import Any, Dict


def _clip(score: float) -> float:
    return round(max(0.0, min(100.0, float(score))), 2)


class ArticleScoreCalculator:
    """Pure score aggregation for phase-1 article selection."""

    def calculate(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        reaction_breakdown = dict(analysis.get("reaction_breakdown") or {})
        pld_breakdown = dict(analysis.get("pld_breakdown") or {})
        operational_breakdown = dict(analysis.get("operational_breakdown") or {})
        reaction_score = _clip(
            sum(float(reaction_breakdown.get(key, 0.0)) for key in (
                "emotional_trigger",
                "inversion_surprise",
                "self_projection",
                "comparison_anxiety_hope_trigger",
            )) / 4.0
        )
        pld_fit_score = _clip(
            sum(float(pld_breakdown.get(key, 0.0)) for key in (
                "entry_fit",
                "hook_fit",
                "loop_fit",
                "trust_fit",
            )) / 4.0
        )
        moderation_risk = float(operational_breakdown.get("moderation_platform_risk", 0.0))
        operational_score = _clip(
            (
                float(operational_breakdown.get("content_transformation_ease", 0.0))
                + float(operational_breakdown.get("question_based_framing_ease", 0.0))
                + float(operational_breakdown.get("brand_safety", 0.0))
                + float(operational_breakdown.get("reviewer_confirmation_likelihood", 0.0))
                + max(0.0, 100.0 - moderation_risk)
            )
            / 5.0
        )
        final_score = _clip(0.40 * reaction_score + 0.35 * pld_fit_score + 0.25 * operational_score)
        return {
            "reaction_score": reaction_score,
            "pld_fit_score": pld_fit_score,
            "operational_score": operational_score,
            "final_score": final_score,
        }
