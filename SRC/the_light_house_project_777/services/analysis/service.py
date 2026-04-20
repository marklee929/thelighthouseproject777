from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.model_router import resolve_model_for_task, run_local_model
from repositories.interfaces import ArticleRepositoryProtocol

from .hard_reject import HardRejectEvaluator
from .pld_classifier import PldStageClassifier
from .prompt_builder import ArticleAnalysisPromptBuilder
from .score_calculator import ArticleScoreCalculator


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _clip(score: float) -> float:
    return round(max(0.0, min(100.0, float(score))), 2)


class ArticleAnalysisService:
    """Analyzes collected articles for reaction potential, PLD fit, and operational suitability."""

    EMOTION_TERMS = {"hope", "fear", "grief", "joy", "anxiety", "peace", "lonely", "healing", "crisis", "survive"}
    SURPRISE_TERMS = {"despite", "unexpected", "surprising", "after", "amid", "reversal", "reveals", "but"}
    SELF_PROJECTION_TERMS = {"people", "families", "workers", "students", "parents", "youth", "community", "anyone"}

    def __init__(
        self,
        *,
        article_repository: ArticleRepositoryProtocol,
        model_name: Optional[str] = None,
        ollama_base_url: Optional[str] = None,
        analysis_version: str = "phase1_selection_v1",
    ) -> None:
        self.article_repository = article_repository
        self.model_name = str(model_name or resolve_model_for_task("review_news")).strip()
        self.ollama_base_url = ollama_base_url
        self.analysis_version = analysis_version
        self.prompt_builder = ArticleAnalysisPromptBuilder()
        self.pld_classifier = PldStageClassifier()
        self.score_calculator = ArticleScoreCalculator()
        self.hard_reject_evaluator = HardRejectEvaluator()

    def analyze_candidates(self, limit: int = 20) -> List[Dict[str, Any]]:
        rows = self.article_repository.list_candidates_for_analysis(limit)
        outputs: List[Dict[str, Any]] = []
        for article in rows:
            analysis = self.analyze_article(article)
            self.article_repository.update_article_analysis(str(article["article_id"]), analysis)
            outputs.append({"article_id": article["article_id"], **analysis})
        return outputs

    def analyze_article(
        self,
        article: Dict[str, Any],
        *,
        model_name: Optional[str] = None,
        analysis_version: Optional[str] = None,
    ) -> Dict[str, Any]:
        effective_model = str(model_name or self.model_name).strip() or self.model_name
        effective_version = str(analysis_version or self.analysis_version).strip() or self.analysis_version
        prompt = self.prompt_builder.build_prompt(article)
        try:
            llm_response = run_local_model(
                task_type="review_news",
                prompt=prompt,
                model=effective_model,
                ollama_base_url=self.ollama_base_url,
                format="json",
                timeout=60.0,
                temperature=0.1,
            )
        except Exception as exc:
            llm_response = {"ok": False, "error": f"{type(exc).__name__}: {exc}", "raw": "", "parsed": None}
        llm_analysis = self._parse_llm_response(llm_response)
        fallback_analysis = self._fallback_analysis(article)
        merged = self._merge_analysis(fallback_analysis, llm_analysis)
        merged.update(self.score_calculator.calculate(merged))
        hard_reject = self.hard_reject_evaluator.evaluate(article, merged)
        selection_status = "hard_rejected" if hard_reject["hard_reject"] else "scored"
        merged.update(
            {
                "hard_reject_reason": hard_reject["hard_reject_reason"],
                "selection_status": selection_status,
                "analysis_model": effective_model,
                "analysis_version": effective_version,
                "analyzed_at": _now_utc(),
                "analysis_payload": {
                    "prompt": prompt,
                    "llm_ok": bool(llm_response.get("ok")),
                    "llm_error": llm_response.get("error", ""),
                    "llm_raw": llm_response.get("raw", ""),
                    "fallback_analysis": fallback_analysis,
                    "merged_analysis": {
                        "reaction_breakdown": merged.get("reaction_breakdown"),
                        "pld_breakdown": merged.get("pld_breakdown"),
                        "operational_breakdown": merged.get("operational_breakdown"),
                        "dominant_pld_stage": merged.get("dominant_pld_stage"),
                        "selection_summary": merged.get("selection_summary"),
                    },
                },
            }
        )
        return merged

    def _parse_llm_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        raw = response.get("parsed")
        if not isinstance(raw, dict):
            raw_text = str(response.get("raw", "")).strip()
            if not raw_text:
                return {}
            try:
                raw = json.loads(raw_text)
            except Exception:
                return {}
        return {
            "reaction_breakdown": self._sanitize_breakdown(raw.get("reaction_breakdown"), (
                "emotional_trigger",
                "inversion_surprise",
                "self_projection",
                "comparison_anxiety_hope_trigger",
            )),
            "pld_breakdown": self._sanitize_breakdown(raw.get("pld_breakdown"), (
                "entry_fit",
                "hook_fit",
                "loop_fit",
                "trust_fit",
            )),
            "operational_breakdown": self._sanitize_breakdown(raw.get("operational_breakdown"), (
                "content_transformation_ease",
                "question_based_framing_ease",
                "brand_safety",
                "moderation_platform_risk",
                "reviewer_confirmation_likelihood",
            )),
            "dominant_pld_stage": str(raw.get("dominant_pld_stage", "")).strip().lower(),
            "selection_summary": " ".join(str(raw.get("selection_summary", "")).split()),
            "hard_reject_reason": " ".join(str(raw.get("hard_reject_reason", "")).split()),
        }

    def _sanitize_breakdown(self, payload: Any, keys: tuple[str, ...]) -> Dict[str, float]:
        if not isinstance(payload, dict):
            return {}
        return {key: _clip(float(payload.get(key, 0.0))) for key in keys if key in payload}

    def _fallback_analysis(self, article: Dict[str, Any]) -> Dict[str, Any]:
        text = " ".join(
            [
                str(article.get("title", "")),
                str(article.get("summary_raw", "")),
                str(article.get("article_content_raw", "")),
            ]
        ).lower()
        emotion_hits = sum(1 for token in self.EMOTION_TERMS if token in text)
        surprise_hits = sum(1 for token in self.SURPRISE_TERMS if token in text)
        projection_hits = sum(1 for token in self.SELF_PROJECTION_TERMS if token in text)
        pld_classification = self.pld_classifier.classify(article)
        reaction_breakdown = {
            "emotional_trigger": _clip(35 + emotion_hits * 10),
            "inversion_surprise": _clip(28 + surprise_hits * 14),
            "self_projection": _clip(30 + projection_hits * 11),
            "comparison_anxiety_hope_trigger": _clip(32 + (emotion_hits + surprise_hits) * 8),
        }
        trust_fit = float((pld_classification["pld_breakdown"] or {}).get("trust_fit", 55.0))
        operational_breakdown = {
            "content_transformation_ease": _clip(42 + surprise_hits * 7 + projection_hits * 6),
            "question_based_framing_ease": _clip(40 + surprise_hits * 9),
            "brand_safety": _clip(trust_fit),
            "moderation_platform_risk": _clip(max(5.0, 40.0 - trust_fit / 2.0)),
            "reviewer_confirmation_likelihood": _clip(38 + emotion_hits * 6 + projection_hits * 5),
        }
        return {
            "reaction_breakdown": reaction_breakdown,
            "pld_breakdown": pld_classification["pld_breakdown"],
            "dominant_pld_stage": pld_classification["dominant_pld_stage"],
            "operational_breakdown": operational_breakdown,
            "selection_summary": "Fallback article analysis based on PLD fit, curiosity framing potential, and reviewer safety heuristics.",
            "hard_reject_reason": "",
        }

    def _merge_analysis(self, fallback: Dict[str, Any], llm_analysis: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(fallback)
        for key in ("reaction_breakdown", "pld_breakdown", "operational_breakdown"):
            merged = dict(fallback.get(key) or {})
            merged.update({k: v for k, v in dict(llm_analysis.get(key) or {}).items() if v is not None})
            result[key] = merged
        if str(llm_analysis.get("dominant_pld_stage", "")).strip():
            result["dominant_pld_stage"] = str(llm_analysis["dominant_pld_stage"]).strip().lower()
        if str(llm_analysis.get("selection_summary", "")).strip():
            result["selection_summary"] = str(llm_analysis["selection_summary"]).strip()
        if str(llm_analysis.get("hard_reject_reason", "")).strip():
            result["hard_reject_reason"] = str(llm_analysis["hard_reject_reason"]).strip()
        return result
