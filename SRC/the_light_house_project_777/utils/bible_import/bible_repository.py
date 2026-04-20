from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Iterable

from bible_tag_rules import VerseTag
from web_pdf_parser import ParsedVerse


@dataclass(slots=True)
class ImportCounters:
    pages_processed: int = 0
    verses_parsed: int = 0
    verses_inserted: int = 0
    verses_updated: int = 0
    tags_inserted: int = 0
    warnings_count: int = 0


class BibleRepository:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def connect(self):
        try:
            psycopg = importlib.import_module("psycopg")
        except ImportError as exc:
            raise RuntimeError("psycopg is required for Bible import. Install psycopg[binary].") from exc
        return psycopg.connect(self.database_url)

    def create_import_run(self, source_file: str) -> int:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bible_import_runs (source_file, status)
                VALUES (%s, 'running')
                RETURNING id
                """,
                (source_file,),
            )
            run_id = cur.fetchone()[0]
            conn.commit()
            return int(run_id)

    def finish_import_run(self, run_id: int, status: str, counters: ImportCounters, notes: str | None = None) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bible_import_runs
                SET finished_at = CURRENT_TIMESTAMP,
                    status = %s,
                    pages_processed = %s,
                    verses_parsed = %s,
                    verses_inserted = %s,
                    verses_updated = %s,
                    tags_inserted = %s,
                    warnings_count = %s,
                    notes = %s
                WHERE id = %s
                """,
                (
                    status,
                    counters.pages_processed,
                    counters.verses_parsed,
                    counters.verses_inserted,
                    counters.verses_updated,
                    counters.tags_inserted,
                    counters.warnings_count,
                    notes,
                    run_id,
                ),
            )
            conn.commit()

    def upsert_verses(self, verses: Iterable[ParsedVerse]) -> tuple[int, int]:
        batch = list(verses)
        if not batch:
            return 0, 0
        verse_ids = [item.verse_id for item in batch]
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT verse_id FROM bible_verses WHERE verse_id = ANY(%s)", (verse_ids,))
            existing = {row[0] for row in cur.fetchall()}
            cur.executemany(
                """
                INSERT INTO bible_verses (
                    verse_id,
                    translation,
                    book,
                    chapter,
                    verse,
                    reference,
                    verse_text,
                    normalized_text,
                    source_file,
                    source_page
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (verse_id) DO UPDATE
                SET translation = EXCLUDED.translation,
                    book = EXCLUDED.book,
                    chapter = EXCLUDED.chapter,
                    verse = EXCLUDED.verse,
                    reference = EXCLUDED.reference,
                    verse_text = EXCLUDED.verse_text,
                    normalized_text = EXCLUDED.normalized_text,
                    source_file = EXCLUDED.source_file,
                    source_page = EXCLUDED.source_page
                """,
                [
                    (
                        item.verse_id,
                        item.translation,
                        item.book,
                        item.chapter,
                        item.verse,
                        item.reference,
                        item.verse_text,
                        item.normalized_text,
                        item.source_file,
                        item.source_page,
                    )
                    for item in batch
                ],
            )
            conn.commit()
        inserted = sum(1 for item in batch if item.verse_id not in existing)
        updated = len(batch) - inserted
        return inserted, updated

    def insert_tags(self, tags: Iterable[VerseTag]) -> int:
        batch = list(tags)
        if not batch:
            return 0
        inserted = 0
        with self.connect() as conn, conn.cursor() as cur:
            for item in batch:
                cur.execute(
                    """
                    INSERT INTO bible_verse_tags (verse_id, tag, weight, source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (verse_id, tag) DO NOTHING
                    RETURNING id
                    """,
                    (item.verse_id, item.tag, item.weight, item.source),
                )
                if cur.fetchone():
                    inserted += 1
            conn.commit()
        return inserted
