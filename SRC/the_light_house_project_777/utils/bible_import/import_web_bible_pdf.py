from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from bible_repository import BibleRepository, ImportCounters
from bible_tag_rules import VerseTag, generate_rule_tags
from web_pdf_parser import ParsedVerse, WebPdfParser

BATCH_SIZE = 500


def build_argument_parser(default_pdf: Path, default_log_file: Path) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import the World English Bible PDF into PostgreSQL.")
    parser.add_argument("--pdf", default=str(default_pdf), help="Source PDF path. Defaults to DOC/documents/eng-web_all.pdf")
    parser.add_argument("--database-url", help="PostgreSQL connection string. Falls back to DATABASE_URL or LIGHTHOUSE_DATABASE_DSN.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and tag without writing to PostgreSQL.")
    parser.add_argument("--limit-pages", type=int, help="Optional page limit for test runs.")
    parser.add_argument("--skip-tags", action="store_true", help="Skip rule-based tag generation.")
    parser.add_argument("--log-file", default=str(default_log_file), help="Log file path.")
    parser.add_argument("--include-apocrypha", action="store_true", help="Include Apocrypha / Deuterocanon books.")
    return parser


def configure_logging(log_file: Path) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    logger = logging.getLogger("web_bible_import")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def resolve_database_url(argument_value: str | None) -> str | None:
    return (
        (argument_value or "").strip()
        or os.getenv("DATABASE_URL", "").strip()
        or os.getenv("LIGHTHOUSE_DATABASE_DSN", "").strip()
        or None
    )


def chunked(items: list[ParsedVerse], batch_size: int) -> list[list[ParsedVerse]]:
    return [items[index : index + batch_size] for index in range(0, len(items), batch_size)]


def build_tags(verses: list[ParsedVerse]) -> list[VerseTag]:
    tags: list[VerseTag] = []
    for verse in verses:
        tags.extend(generate_rule_tags(verse_id=verse.verse_id, verse_text=verse.verse_text))
    return tags


def print_summary(counters: ImportCounters, dry_run: bool, source_file: Path) -> None:
    print("")
    print("WEB Bible import summary")
    print(f"Mode: {'dry-run' if dry_run else 'import'}")
    print(f"Source file: {source_file}")
    print(f"Pages processed: {counters.pages_processed}")
    print(f"Verses parsed: {counters.verses_parsed}")
    print(f"Verses inserted: {counters.verses_inserted}")
    print(f"Verses updated: {counters.verses_updated}")
    print(f"Tags inserted: {counters.tags_inserted}")
    print(f"Warnings: {counters.warnings_count}")


def main() -> int:
    utility_dir = Path(__file__).resolve().parent
    repo_root = utility_dir.parents[3]
    default_pdf = repo_root / "DOC" / "documents" / "eng-web_all.pdf"
    default_log_file = utility_dir / "logs" / "import_web_bible_pdf.log"
    argument_parser = build_argument_parser(default_pdf=default_pdf, default_log_file=default_log_file)
    args = argument_parser.parse_args()

    pdf_path = Path(args.pdf).expanduser().resolve()
    log_file = Path(args.log_file).expanduser().resolve()
    logger = configure_logging(log_file=log_file)

    if not pdf_path.exists():
        argument_parser.error(f"PDF not found: {pdf_path}")

    database_url = resolve_database_url(args.database_url)
    if not database_url and not args.dry_run:
        argument_parser.error("--database-url is required unless DATABASE_URL or LIGHTHOUSE_DATABASE_DSN is set.")

    logger.info("Starting WEB PDF import from %s", pdf_path)
    parser_service = WebPdfParser(pdf_path=pdf_path, include_apocrypha=args.include_apocrypha, logger=logger)
    parse_result = parser_service.parse(limit_pages=args.limit_pages)

    counters = ImportCounters(
        pages_processed=parse_result.pages_processed,
        verses_parsed=len(parse_result.verses),
        warnings_count=len(parse_result.warnings),
    )

    for warning in parse_result.warnings:
        logger.warning("Page %s: %s", warning.source_page, warning.message)

    tags: list[VerseTag] = []
    if not args.skip_tags:
        tags = build_tags(parse_result.verses)

    if args.dry_run:
        logger.info("Dry-run completed. No database writes were performed.")
        print_summary(counters=counters, dry_run=True, source_file=pdf_path)
        return 0

    repository = BibleRepository(database_url=database_url or "")
    run_id = repository.create_import_run(source_file=str(pdf_path))
    try:
        tag_map: dict[str, list[VerseTag]] = {}
        for tag in tags:
            tag_map.setdefault(tag.verse_id, []).append(tag)

        for batch in chunked(parse_result.verses, BATCH_SIZE):
            inserted, updated = repository.upsert_verses(batch)
            counters.verses_inserted += inserted
            counters.verses_updated += updated
            if args.skip_tags:
                continue
            batch_tags: list[VerseTag] = []
            for verse in batch:
                batch_tags.extend(tag_map.get(verse.verse_id, []))
            counters.tags_inserted += repository.insert_tags(batch_tags)

        repository.finish_import_run(run_id=run_id, status="completed", counters=counters, notes="WEB PDF import completed.")
        logger.info("Import completed successfully with run id %s", run_id)
        print_summary(counters=counters, dry_run=False, source_file=pdf_path)
        return 0
    except Exception as exc:
        repository.finish_import_run(
            run_id=run_id,
            status="failed",
            counters=counters,
            notes=f"Import failed: {exc}",
        )
        logger.exception("Import failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
