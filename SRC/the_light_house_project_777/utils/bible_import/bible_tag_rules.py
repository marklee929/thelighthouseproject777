from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

ALLOWED_TAGS: tuple[str, ...] = (
    "money",
    "anxiety",
    "fear",
    "hope",
    "justice",
    "mercy",
    "pride",
    "wisdom",
    "work",
    "success",
    "suffering",
    "healing",
    "family",
    "forgiveness",
    "truth",
    "temptation",
    "generosity",
    "contentment",
    "identity",
    "community",
    "leadership",
    "conflict",
    "death",
    "faith",
    "prayer",
)

TAG_KEYWORDS: dict[str, tuple[str, ...]] = {
    "money": ("money", "gold", "silver", "rich", "wealth", "treasure", "coin", "wages", "buy", "sell"),
    "anxiety": ("anxious", "anxiety", "worry", "worried", "troubled", "distress", "burdened"),
    "fear": ("fear", "afraid", "terror", "dread", "frightened"),
    "hope": ("hope", "wait", "promised", "promise", "future", "restore", "salvation"),
    "justice": ("justice", "judge", "judgment", "righteousness", "oppression", "defend", "equity"),
    "mercy": ("mercy", "merciful", "compassion", "compassionate", "kindness", "steadfast love"),
    "pride": ("pride", "proud", "boast", "haughty", "arrogant", "vain"),
    "wisdom": ("wisdom", "wise", "understanding", "discernment", "instruction", "knowledge"),
    "work": ("work", "labor", "serve", "service", "field", "harvest", "craft", "build"),
    "success": ("prosper", "prosperity", "success", "fruitful", "increase", "abundance"),
    "suffering": ("suffer", "suffering", "affliction", "tribulation", "grief", "pain", "persecution"),
    "healing": ("heal", "healed", "healing", "cure", "whole", "restore health"),
    "family": ("father", "mother", "son", "daughter", "children", "household", "wife", "husband", "brother", "sister"),
    "forgiveness": ("forgive", "forgiven", "forgiveness", "pardon", "pardoned", "blot out"),
    "truth": ("truth", "true", "faithful", "testimony", "witness"),
    "temptation": ("tempt", "temptation", "snare", "entice", "evil desire", "lust"),
    "generosity": ("give", "gave", "given", "gift", "generous", "share", "offering", "charity"),
    "contentment": ("content", "contentment", "enough", "satisfied", "rest", "peace"),
    "identity": ("name", "called", "chosen", "belong", "inheritance", "people", "image", "likeness"),
    "community": ("people", "assembly", "congregation", "church", "neighbor", "together", "unity"),
    "leadership": ("leader", "lead", "rule", "ruler", "king", "elder", "overseer", "shepherd"),
    "conflict": ("war", "battle", "strife", "violence", "enemy", "attack", "sword", "quarrel"),
    "death": ("death", "die", "died", "grave", "buried", "mourning", "slain"),
    "faith": ("faith", "believe", "trusted", "trust", "hope in", "confidence", "walked with god"),
    "prayer": ("pray", "prayer", "prayed", "ask", "called on", "supplication", "petition"),
}

NON_WORD_RE = re.compile(r"[^a-z0-9\s]+")


@dataclass(slots=True)
class VerseTag:
    verse_id: str
    tag: str
    weight: float
    source: str = "rule"


def normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = NON_WORD_RE.sub(" ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def generate_rule_tags(verse_id: str, verse_text: str, max_tags: int = 5) -> list[VerseTag]:
    normalized = normalize_text(verse_text)
    scores: list[tuple[str, float]] = []
    for tag, keywords in TAG_KEYWORDS.items():
        matches = 0
        for keyword in keywords:
            pattern = rf"(?<!\w){re.escape(keyword)}(?!\w)"
            matches += len(re.findall(pattern, normalized))
        if matches:
            weight = min(5.0, 1.0 + (matches * 0.75))
            scores.append((tag, round(weight, 2)))
    scores.sort(key=lambda item: (-item[1], item[0]))
    return [VerseTag(verse_id=verse_id, tag=tag, weight=weight) for tag, weight in scores[:max_tags]]


def enrich_tags_with_local_llm(_verse_text: str) -> list[VerseTag]:
    """Placeholder for future local-LLM enrichment limited to the fixed allowed tag list."""

    return []
