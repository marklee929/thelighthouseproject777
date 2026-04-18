-- Lighthouse Project 777
-- News Collector review screen persistence extensions
-- Assumes 001_phase1_pipeline.sql and 002_phase1_article_selection.sql have been applied first.

ALTER TABLE core.article_reviews
    ADD COLUMN IF NOT EXISTS review_summary text,
    ADD COLUMN IF NOT EXISTS suggested_angle text,
    ADD COLUMN IF NOT EXISTS suggested_question text,
    ADD COLUMN IF NOT EXISTS operator_note text;

CREATE INDEX IF NOT EXISTS article_reviews_article_latest_ui_idx
    ON core.article_reviews (article_id, reviewed_at DESC);
