# WEB Bible PDF Import Utility

This utility imports the World English Bible (WEB) PDF into PostgreSQL as a standalone offline job.

It is intentionally isolated under `SRC/the_light_house_project_777/utils/bible_import/` and is not wired into the main application runtime, dashboard, RSS flow, Telegram flow, or Facebook flow.

## Source PDF

Default source path:

`DOC/documents/eng-web_all.pdf`

If you need a different source file, pass `--pdf`.

## Attribution

"Scripture text: World English Bible (WEB), public domain. Do not call modified text 'World English Bible'."

The source PDF states that the World English Bible is public domain and may be copied, distributed, posted online, and incorporated into software. If the actual text is modified, the modified result must not be called "World English Bible".

## What This Utility Does

- parses the WEB PDF page by page
- extracts recognized book / chapter / verse records
- preserves the source PDF path and page number
- upserts `bible_verses`
- applies rule-based controlled tags into `bible_verse_tags`
- records import run metadata in `bible_import_runs`

## Files

- `import_web_bible_pdf.py`: CLI entry point
- `web_pdf_parser.py`: page-by-page PDF parser
- `bible_tag_rules.py`: fixed-list rule-based tags
- `bible_repository.py`: PostgreSQL persistence
- `ddl/001_bible_verses.sql`: DDL reference

## Run the DDL

If the tables are not already present:

```powershell
@'
import importlib
import os
from pathlib import Path

psycopg = importlib.import_module("psycopg")
sql = Path(r".\SRC\the_light_house_project_777\utils\bible_import\ddl\001_bible_verses.sql").read_text(encoding="utf-8")

with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
'@ | python -
```

## Run a Dry-Run Import

```powershell
python .\SRC\the_light_house_project_777\utils\bible_import\import_web_bible_pdf.py --dry-run --limit-pages 50
```

If your database URL is not in `DATABASE_URL` or `LIGHTHOUSE_DATABASE_DSN`, provide it explicitly for full import runs:

```powershell
python .\SRC\the_light_house_project_777\utils\bible_import\import_web_bible_pdf.py --database-url "postgresql://postgres:postgres@localhost:5432/lighthouse" --dry-run --limit-pages 50
```

## Run the Full Import

```powershell
python .\SRC\the_light_house_project_777\utils\bible_import\import_web_bible_pdf.py --database-url "postgresql://postgres:postgres@localhost:5432/lighthouse"
```

Optional flags:

- `--pdf <path>`
- `--limit-pages <n>`
- `--skip-tags`
- `--include-apocrypha`
- `--log-file <path>`

## Inspect Counts After Import

```sql
SELECT COUNT(*) FROM bible_verses;
SELECT COUNT(*) FROM bible_verse_tags;
SELECT id, status, pages_processed, verses_parsed, verses_inserted, verses_updated, tags_inserted, warnings_count
FROM bible_import_runs
ORDER BY id DESC
LIMIT 10;
```

## Important Warning

PDF parsing is fallback-quality. If later you obtain a structured WEB source format with reliable verse boundaries, that structured source should be preferred over PDF extraction.
