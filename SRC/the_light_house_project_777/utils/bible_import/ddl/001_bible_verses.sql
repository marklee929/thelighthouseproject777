CREATE TABLE IF NOT EXISTS bible_verses (
    verse_id TEXT PRIMARY KEY,
    translation TEXT NOT NULL DEFAULT 'WEB',
    book TEXT NOT NULL,
    chapter INT NOT NULL,
    verse INT NOT NULL,
    reference TEXT NOT NULL,
    verse_text TEXT NOT NULL,
    normalized_text TEXT,
    source_file TEXT,
    source_page INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (translation, book, chapter, verse)
);

CREATE TABLE IF NOT EXISTS bible_verse_tags (
    id BIGSERIAL PRIMARY KEY,
    verse_id TEXT NOT NULL REFERENCES bible_verses(verse_id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    weight NUMERIC(5, 2) DEFAULT 1.0,
    source TEXT DEFAULT 'rule',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (verse_id, tag)
);

CREATE TABLE IF NOT EXISTS bible_import_runs (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'running',
    pages_processed INT DEFAULT 0,
    verses_parsed INT DEFAULT 0,
    verses_inserted INT DEFAULT 0,
    verses_updated INT DEFAULT 0,
    tags_inserted INT DEFAULT 0,
    warnings_count INT DEFAULT 0,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_bible_verses_reference
    ON bible_verses (translation, book, chapter, verse);

CREATE INDEX IF NOT EXISTS idx_bible_verse_tags_tag
    ON bible_verse_tags (tag);

CREATE INDEX IF NOT EXISTS idx_bible_import_runs_status
    ON bible_import_runs (status, started_at DESC);

ALTER TABLE bible_verses
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
