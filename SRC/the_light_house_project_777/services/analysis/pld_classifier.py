from __future__ import annotations

from typing import Any, Dict


def _clip(score: float) -> float:
    return round(max(0.0, min(100.0, float(score))), 2)


class PldStageClassifier:
    """Scores PLD fit and picks the dominant stage."""

    HUMAN_NEED_TERMS = {
        "hope",
        "peace",
        "fear",
        "anxiety",
        "purpose",
        "meaning",
        "family",
        "community",
        "healing",
        "lonely",
        "justice",
        "work",
        "future",
        "youth",
        "life",
    }
    CURIOSITY_TERMS = {
        "why",
        "how",
        "what if",
        "unexpected",
        "surprising",
        "despite",
        "amid",
        "after",
        "before",
        "questions",
        "reveals",
    }
    REFLECTION_TERMS = {
        "reflect",
        "consider",
        "meaning",
        "purpose",
        "identity",
        "future",
        "hope",
        "trust",
        "what you want",
        "what matters",
    }
    INSIDER_TERMS = {
        "sanctification",
        "eschatology",
        "dispensational",
        "propitiation",
        "intercessory",
        "revival meeting",
        "anointing",
        "end times",
    }

    def classify(self, article: Dict[str, Any]) -> Dict[str, Any]:
        text = " ".join(
            [
                str(article.get("title", "")),
                str(article.get("summary_raw", "")),
                str(article.get("article_content_raw", "")),
            ]
        ).lower()
        need_hits = sum(1 for token in self.HUMAN_NEED_TERMS if token in text)
        curiosity_hits = sum(1 for token in self.CURIOSITY_TERMS if token in text)
        reflection_hits = sum(1 for token in self.REFLECTION_TERMS if token in text)
        insider_hits = sum(1 for token in self.INSIDER_TERMS if token in text)
        entry_fit = _clip(35 + need_hits * 11 - insider_hits * 6)
        hook_fit = _clip(30 + curiosity_hits * 12 + min(need_hits, 2) * 5)
        loop_fit = _clip(28 + reflection_hits * 13 + curiosity_hits * 4)
        trust_fit = _clip(50 + (20 if str(article.get("source_status", "")).strip().lower() == "active" else -20) - insider_hits * 3)
        breakdown = {
            "entry_fit": entry_fit,
            "hook_fit": hook_fit,
            "loop_fit": loop_fit,
            "trust_fit": trust_fit,
        }
        dominant_stage = max(breakdown, key=breakdown.get).replace("_fit", "")
        return {"pld_breakdown": breakdown, "dominant_pld_stage": dominant_stage}
