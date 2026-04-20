from __future__ import annotations

from typing import Any, Dict, List, Mapping


VERSE_CATALOG = (
    {
        "reference": "Matthew 11:28",
        "text": "Come to me, all who labor and are heavy laden, and I will give you rest.",
        "url": "https://www.biblegateway.com/passage/?search=Matthew%2011%3A28&version=ESV",
        "tags": {"rest", "burden", "hope", "healing", "anxiety", "peace"},
    },
    {
        "reference": "Romans 12:12",
        "text": "Rejoice in hope, be patient in tribulation, be constant in prayer.",
        "url": "https://www.biblegateway.com/passage/?search=Romans%2012%3A12&version=ESV",
        "tags": {"hope", "hardship", "patience", "prayer", "crisis"},
    },
    {
        "reference": "Micah 6:8",
        "text": "What does the Lord require of you but to do justice, and to love kindness, and to walk humbly with your God?",
        "url": "https://www.biblegateway.com/passage/?search=Micah%206%3A8&version=ESV",
        "tags": {"justice", "community", "kindness", "public life"},
    },
    {
        "reference": "Psalm 46:1",
        "text": "God is our refuge and strength, a very present help in trouble.",
        "url": "https://www.biblegateway.com/passage/?search=Psalm%2046%3A1&version=ESV",
        "tags": {"trouble", "crisis", "refuge", "strength", "fear"},
    },
    {
        "reference": "James 1:5",
        "text": "If any of you lacks wisdom, let him ask God, who gives generously to all without reproach.",
        "url": "https://www.biblegateway.com/passage/?search=James%201%3A5&version=ESV",
        "tags": {"wisdom", "questions", "guidance", "discernment"},
    },
    {
        "reference": "Isaiah 58:10",
        "text": "If you pour yourself out for the hungry and satisfy the desire of the afflicted, then shall your light rise in the darkness.",
        "url": "https://www.biblegateway.com/passage/?search=Isaiah%2058%3A10&version=ESV",
        "tags": {"care", "service", "community", "mission", "families"},
    },
)


class BibleVerseSuggestionService:
    """Generates up to three lightweight verse suggestions after an article is approved."""

    def suggest_for_article(self, article: Mapping[str, Any], *, limit: int = 3) -> List[Dict[str, Any]]:
        text = " ".join(
            [
                str(article.get("title", "")),
                str(article.get("summary_raw", "")),
                str(article.get("selection_summary", "")),
                str(article.get("dominant_pld_stage", "")),
            ]
        ).lower()

        scored: list[dict[str, Any]] = []
        for row in VERSE_CATALOG:
            tag_hits = sum(1 for tag in row["tags"] if tag in text)
            stage_bonus = 0
            dominant_stage = str(article.get("dominant_pld_stage") or "").strip().lower()
            if dominant_stage == "trust" and "wisdom" in row["tags"]:
                stage_bonus += 2
            if dominant_stage == "hook" and "questions" in row["tags"]:
                stage_bonus += 2
            if dominant_stage == "loop" and "hope" in row["tags"]:
                stage_bonus += 2
            if dominant_stage == "entry" and "community" in row["tags"]:
                stage_bonus += 2
            total = tag_hits + stage_bonus
            scored.append(
                {
                    "score": total,
                    "verse_reference": row["reference"],
                    "verse_text": row["text"],
                    "verse_reason": self._reason(row["reference"], article, total),
                    "verse_url": row["url"],
                }
            )

        scored.sort(key=lambda item: (int(item["score"]), item["verse_reference"]), reverse=True)
        suggestions = []
        for rank, row in enumerate(scored[: max(1, min(limit, 3))], start=1):
            suggestion = dict(row)
            suggestion.pop("score", None)
            suggestion["rank"] = rank
            suggestions.append(suggestion)
        return suggestions

    def _reason(self, verse_reference: str, article: Mapping[str, Any], score: int) -> str:
        title = str(article.get("title") or "this article").strip()
        if score <= 0:
            return f"{verse_reference} offers a safe Scripture bridge if {title} moves forward."
        return f"{verse_reference} aligns with the article theme and can support a reflection-first Facebook angle for {title}."
