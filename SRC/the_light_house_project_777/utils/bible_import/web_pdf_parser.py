from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

from bible_tag_rules import normalize_text

CANONICAL_BOOKS: tuple[str, ...] = (
    "Genesis",
    "Exodus",
    "Leviticus",
    "Numbers",
    "Deuteronomy",
    "Joshua",
    "Judges",
    "Ruth",
    "1 Samuel",
    "2 Samuel",
    "1 Kings",
    "2 Kings",
    "1 Chronicles",
    "2 Chronicles",
    "Ezra",
    "Nehemiah",
    "Esther",
    "Job",
    "Psalms",
    "Proverbs",
    "Ecclesiastes",
    "Song of Solomon",
    "Isaiah",
    "Jeremiah",
    "Lamentations",
    "Ezekiel",
    "Daniel",
    "Hosea",
    "Joel",
    "Amos",
    "Obadiah",
    "Jonah",
    "Micah",
    "Nahum",
    "Habakkuk",
    "Zephaniah",
    "Haggai",
    "Zechariah",
    "Malachi",
    "Matthew",
    "Mark",
    "Luke",
    "John",
    "Acts",
    "Romans",
    "1 Corinthians",
    "2 Corinthians",
    "Galatians",
    "Ephesians",
    "Philippians",
    "Colossians",
    "1 Thessalonians",
    "2 Thessalonians",
    "1 Timothy",
    "2 Timothy",
    "Titus",
    "Philemon",
    "Hebrews",
    "James",
    "1 Peter",
    "2 Peter",
    "1 John",
    "2 John",
    "3 John",
    "Jude",
    "Revelation",
)

APOCRYPHA_BOOKS: tuple[str, ...] = (
    "Tobit",
    "Judith",
    "Esther (Greek)",
    "Daniel (Greek)",
    "Wisdom of Solomon",
    "Sirach",
    "Baruch",
    "1 Maccabees",
    "2 Maccabees",
    "1 Esdras",
    "Prayer of Manasses",
    "Psalm 151",
    "3 Maccabees",
    "2 Esdras",
    "4 Maccabees",
)

ALL_BOOKS = tuple(list(CANONICAL_BOOKS) + list(APOCRYPHA_BOOKS))
ONE_CHAPTER_BOOKS = {"Obadiah", "Philemon", "2 John", "3 John", "Jude"}
BOOK_PATTERN = "|".join(re.escape(book) for book in sorted(ALL_BOOKS, key=len, reverse=True))
SCRIPTURE_HEADER_RE = re.compile(
    rf"^(?P<left_book>{BOOK_PATTERN}) (?P<left_chapter>\d+):(?P<left_verse>\d+)\s+\d+\s+(?P<right_book>{BOOK_PATTERN}) (?P<right_chapter>\d+):(?P<right_verse>\d+)$"
)
ONE_CHAPTER_HEADER_RE = re.compile(
    rf"^(?P<left_book>{'|'.join(re.escape(book) for book in sorted(ONE_CHAPTER_BOOKS, key=len, reverse=True))}) 1\s+\d+\s+(?P<right_book>{'|'.join(re.escape(book) for book in sorted(ONE_CHAPTER_BOOKS, key=len, reverse=True))}) (?P<right_verse>\d+)$"
)
VERSE_LINE_RE = re.compile(r"^(?P<verse>\d+)\s+(?P<text>.+)$")
FOOTNOTE_MARKER_RE = re.compile(r"[\u2470-\u24ff\u223d\u223e\u273b\*\u2020\u2021\u00a7\u00b6\u2016\u203b]+")
PAGE_ARTIFACT_RE = re.compile(r"^(?:[ivxlcdm]+|\d+)$", re.IGNORECASE)
SPAM_SIGNAL_RE = re.compile(r"(subscribe|click here|free trial|advertisement|promo code|lorem ipsum)", re.IGNORECASE)


@dataclass(slots=True)
class ParsedVerse:
    verse_id: str
    translation: str
    book: str
    chapter: int
    verse: int
    reference: str
    verse_text: str
    normalized_text: str
    source_file: str
    source_page: int


@dataclass(slots=True)
class ParseWarning:
    source_page: int
    message: str


@dataclass(slots=True)
class ParseResult:
    verses: list[ParsedVerse]
    warnings: list[ParseWarning]
    pages_processed: int


def build_verse_id(translation: str, book: str, chapter: int, verse: int) -> str:
    return f"{translation}:{book}:{chapter}:{verse}"


class WebPdfParser:
    def __init__(self, pdf_path: Path, include_apocrypha: bool = False, logger: logging.Logger | None = None):
        self.pdf_path = pdf_path
        self.include_apocrypha = include_apocrypha
        self.allowed_books = set(CANONICAL_BOOKS if not include_apocrypha else ALL_BOOKS)
        self.logger = logger or logging.getLogger(__name__)

        self.current_book: str | None = None
        self.current_chapter: int | None = None
        self.current_verse: int | None = None
        self.current_lines: list[str] = []
        self.current_page: int | None = None
        self.seen_references: set[str] = set()

    def parse(self, limit_pages: int | None = None) -> ParseResult:
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise RuntimeError("pypdf is required for WEB PDF parsing. Install pypdf.") from exc

        reader = PdfReader(str(self.pdf_path))
        max_pages = min(limit_pages or len(reader.pages), len(reader.pages))
        verses: list[ParsedVerse] = []
        warnings: list[ParseWarning] = []

        for page_index in range(max_pages):
            source_page = page_index + 1
            try:
                page_text = reader.pages[page_index].extract_text() or ""
                page_verses, page_warnings = self._parse_page(page_text=page_text, source_page=source_page)
                verses.extend(page_verses)
                warnings.extend(page_warnings)
            except Exception as exc:
                warnings.append(ParseWarning(source_page=source_page, message=f"Page parse warning: {exc}"))
                self.logger.warning("Skipping page %s due to parse warning: %s", source_page, exc)

        finalized = self._finalize_current_verse()
        if finalized:
            verses.append(finalized)

        return ParseResult(verses=verses, warnings=warnings, pages_processed=max_pages)

    def _parse_page(self, page_text: str, source_page: int) -> tuple[list[ParsedVerse], list[ParseWarning]]:
        verses: list[ParsedVerse] = []
        warnings: list[ParseWarning] = []
        lines = self._prepare_lines(page_text)
        if not lines:
            return verses, warnings

        if not self.include_apocrypha and self._is_disallowed_greek_page(lines):
            finalized = self._finalize_current_verse()
            if finalized:
                verses.append(finalized)
            self.current_book = None
            self.current_chapter = None
            return verses, warnings

        header_match = SCRIPTURE_HEADER_RE.match(lines[0])
        if header_match:
            header_book = header_match.group("left_book")
            if header_book in self.allowed_books:
                if self.current_book != header_book:
                    finalized = self._finalize_current_verse()
                    if finalized:
                        verses.append(finalized)
                self.current_book = header_book
                self.current_chapter = int(header_match.group("left_chapter"))
            else:
                finalized = self._finalize_current_verse()
                if finalized:
                    verses.append(finalized)
                self.current_book = None
                self.current_chapter = None
            lines = lines[1:]
        else:
            one_chapter_match = ONE_CHAPTER_HEADER_RE.match(lines[0])
            if one_chapter_match:
                header_book = one_chapter_match.group("left_book")
                if self.current_book != header_book:
                    finalized = self._finalize_current_verse()
                    if finalized:
                        verses.append(finalized)
                self.current_book = header_book
                self.current_chapter = 1
                lines = lines[1:]

        next_significant_cache: dict[int, str | None] = {}
        for index, line in enumerate(lines):
            if self._is_skippable_line(line):
                continue

            if self._is_non_scripture_heading(line) and self.current_book is None:
                continue

            if line in self.allowed_books:
                finalized = self._finalize_current_verse()
                if finalized:
                    verses.append(finalized)
                self.current_book = line
                if next_significant_cache.get(index) is None:
                    next_significant_cache[index] = self._peek_next_significant_line(lines, index + 1)
                next_significant = next_significant_cache[index]
                self.current_chapter = int(next_significant) if next_significant and next_significant.isdigit() else self.current_chapter
                self.current_verse = None
                self.current_lines = []
                self.current_page = None
                continue

            if line in APOCRYPHA_BOOKS and not self.include_apocrypha:
                finalized = self._finalize_current_verse()
                if finalized:
                    verses.append(finalized)
                self.current_book = None
                self.current_chapter = None
                continue

            if self.current_book is None:
                continue

            next_significant = next_significant_cache.get(index)
            if next_significant is None:
                next_significant = self._peek_next_significant_line(lines, index + 1)
                next_significant_cache[index] = next_significant

            if line.isdigit() and next_significant and VERSE_LINE_RE.match(next_significant):
                chapter_number = int(line)
                if chapter_number > 0:
                    finalized = self._finalize_current_verse()
                    if finalized:
                        verses.append(finalized)
                    self.current_chapter = chapter_number
                    continue

            verse_match = VERSE_LINE_RE.match(line)
            if verse_match:
                verse_number = int(verse_match.group("verse"))
                verse_text = verse_match.group("text").strip()
                if self.current_chapter is None:
                    warnings.append(ParseWarning(source_page=source_page, message=f"Verse without chapter context: {line[:80]}"))
                    continue
                finalized = self._finalize_current_verse()
                if finalized:
                    verses.append(finalized)
                self.current_verse = verse_number
                self.current_lines = [verse_text]
                self.current_page = source_page
                continue

            if self.current_verse is None:
                continue

            if SPAM_SIGNAL_RE.search(line):
                warnings.append(ParseWarning(source_page=source_page, message=f"Skipped suspicious line: {line[:80]}"))
                continue

            self.current_lines.append(line)

        return verses, warnings

    def _finalize_current_verse(self) -> ParsedVerse | None:
        if self.current_book is None or self.current_chapter is None or self.current_verse is None:
            self.current_verse = None
            self.current_lines = []
            self.current_page = None
            return None

        verse_text = " ".join(self.current_lines)
        verse_text = FOOTNOTE_MARKER_RE.sub("", verse_text)
        verse_text = re.sub(r"\s+", " ", verse_text).strip()
        if not verse_text:
            self.current_verse = None
            self.current_lines = []
            self.current_page = None
            return None

        reference = f"{self.current_book} {self.current_chapter}:{self.current_verse}"
        if reference in self.seen_references:
            self.current_verse = None
            self.current_lines = []
            self.current_page = None
            return None
        self.seen_references.add(reference)

        verse = ParsedVerse(
            verse_id=build_verse_id("WEB", self.current_book, self.current_chapter, self.current_verse),
            translation="WEB",
            book=self.current_book,
            chapter=self.current_chapter,
            verse=self.current_verse,
            reference=reference,
            verse_text=verse_text,
            normalized_text=normalize_text(verse_text),
            source_file=str(self.pdf_path),
            source_page=self.current_page or 0,
        )
        self.current_verse = None
        self.current_lines = []
        self.current_page = None
        return verse

    def _prepare_lines(self, page_text: str) -> list[str]:
        page_text = page_text.replace("\u00a0", " ")
        raw_lines = [line.strip() for line in page_text.splitlines()]
        lines: list[str] = []
        for raw_line in raw_lines:
            cleaned = FOOTNOTE_MARKER_RE.sub("", raw_line).strip()
            cleaned = re.sub(r"\s+", " ", cleaned)
            if cleaned:
                lines.append(cleaned)
        return lines

    @staticmethod
    def _is_disallowed_greek_page(lines: list[str]) -> bool:
        page_text = " ".join(lines[:8])
        return (
            "Esther (Greek)" in page_text
            or "Daniel (Greek)" in page_text
            or "translated from the Greek" in page_text
            or "with Greek Portions" in page_text
        )

    @staticmethod
    def _peek_next_significant_line(lines: list[str], start_index: int) -> str | None:
        for candidate in lines[start_index:]:
            if candidate and not PAGE_ARTIFACT_RE.match(candidate):
                return candidate
        return None

    @staticmethod
    def _is_skippable_line(line: str) -> bool:
        if PAGE_ARTIFACT_RE.match(line):
            return True
        if line.startswith("PDF generated using "):
            return True
        if re.match(r"^https?://", line, re.IGNORECASE):
            return True
        if line.startswith("Contents") or line.startswith("Preface"):
            return True
        if line in {"OT", "NT", "DC"}:
            return True
        if line.startswith("The First Book of Moses") or line == "Commonly Called":
            return True
        if line.startswith("Old Testament: ") or line.startswith("What are MT,TR, and NU?") or line.startswith("More Information"):
            return True
        if line.startswith("World English Bible") or line.startswith("The World English Bible"):
            return True
        return False

    @staticmethod
    def _is_non_scripture_heading(line: str) -> bool:
        return line in {"Contents", "Preface", "More Information", "Glossary", "Index"}
