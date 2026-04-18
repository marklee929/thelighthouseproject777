-- Lighthouse Project 777
-- Phase 1 PostgreSQL DDL
-- Assumes PostgreSQL 17+ with UTF-8 database encoding.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS content;
CREATE SCHEMA IF NOT EXISTS system;

CREATE TABLE IF NOT EXISTS core.sources (
    source_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_code text NOT NULL,
    source_name text NOT NULL,
    source_type text NOT NULL DEFAULT 'rss_registry',
    site_url text NOT NULL,
    language_code varchar(12) NOT NULL DEFAULT 'en',
    region_code varchar(32) NOT NULL DEFAULT 'global',
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'discovery_required')),
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sources_source_code_key UNIQUE (source_code)
);

CREATE TABLE IF NOT EXISTS core.rss_feeds (
    rss_feed_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id uuid NOT NULL REFERENCES core.sources (source_id),
    feed_code text NOT NULL,
    feed_name text NOT NULL,
    feed_url text NOT NULL,
    site_url text NOT NULL,
    feed_format text NOT NULL DEFAULT 'rss' CHECK (feed_format IN ('rss', 'atom', 'rdf', 'unknown')),
    category text NOT NULL DEFAULT 'christian_news',
    language_code varchar(12) NOT NULL DEFAULT 'en',
    region_code varchar(32) NOT NULL DEFAULT 'global',
    enabled boolean NOT NULL DEFAULT true,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'discovery_required', 'verification_required')),
    notes text NOT NULL DEFAULT '',
    metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT rss_feeds_source_feed_code_key UNIQUE (source_id, feed_code),
    CONSTRAINT rss_feeds_feed_url_key UNIQUE (feed_url)
);

CREATE TABLE IF NOT EXISTS system.ingestion_runs (
    ingestion_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id uuid NOT NULL REFERENCES core.sources (source_id),
    rss_feed_id uuid NOT NULL REFERENCES core.rss_feeds (rss_feed_id),
    triggered_by text NOT NULL DEFAULT 'manual',
    status text NOT NULL DEFAULT 'started' CHECK (status IN ('started', 'completed', 'failed')),
    feed_url_snapshot text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamptz,
    items_fetched integer NOT NULL DEFAULT 0 CHECK (items_fetched >= 0),
    items_saved integer NOT NULL DEFAULT 0 CHECK (items_saved >= 0),
    items_duplicate integer NOT NULL DEFAULT 0 CHECK (items_duplicate >= 0),
    items_failed integer NOT NULL DEFAULT 0 CHECK (items_failed >= 0),
    error_message text,
    request_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    result_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.articles (
    article_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id uuid NOT NULL REFERENCES core.sources (source_id),
    rss_feed_id uuid NOT NULL REFERENCES core.rss_feeds (rss_feed_id),
    first_ingestion_run_id uuid REFERENCES system.ingestion_runs (ingestion_run_id),
    last_seen_ingestion_run_id uuid REFERENCES system.ingestion_runs (ingestion_run_id),
    external_id text,
    title text NOT NULL,
    author_name text,
    language_code varchar(12) NOT NULL DEFAULT 'en',
    region_code varchar(32) NOT NULL DEFAULT 'global',
    article_url text NOT NULL,
    canonical_url text NOT NULL,
    published_at timestamptz,
    collected_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    url_hash char(64) NOT NULL,
    dedupe_hash char(64) NOT NULL,
    summary_raw text,
    article_content_html text,
    article_content_raw text,
    article_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    recommendation_score numeric(5,2) CHECK (recommendation_score >= 0 AND recommendation_score <= 100),
    recommendation_reason text,
    recommendation_model text,
    recommendation_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    recommended_at timestamptz,
    review_status text NOT NULL DEFAULT 'pending' CHECK (review_status IN ('pending', 'good', 'bad', 'hold')),
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT articles_dedupe_hash_key UNIQUE (dedupe_hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS articles_rss_feed_external_id_uidx
    ON core.articles (rss_feed_id, external_id)
    WHERE external_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS core.article_reviews (
    article_review_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id uuid NOT NULL REFERENCES core.articles (article_id),
    decision text NOT NULL CHECK (decision IN ('good', 'bad', 'hold')),
    review_channel text NOT NULL DEFAULT 'telegram',
    reviewer_id text,
    review_note text,
    decision_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    telegram_chat_id text,
    telegram_message_id bigint,
    reviewed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS content.generated_contents (
    generated_content_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id uuid NOT NULL REFERENCES core.articles (article_id),
    source_review_id uuid REFERENCES core.article_reviews (article_review_id),
    content_type text NOT NULL,
    generation_status text NOT NULL DEFAULT 'draft' CHECK (generation_status IN ('draft', 'approved', 'rejected', 'archived')),
    generator_name text NOT NULL DEFAULT 'local_llm',
    generator_model text,
    prompt_version text,
    title text,
    body_text text NOT NULL,
    rendered_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    version_no integer NOT NULL DEFAULT 1 CHECK (version_no >= 1),
    created_by text NOT NULL DEFAULT 'system',
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system.publish_logs (
    publish_log_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    generated_content_id uuid NOT NULL REFERENCES content.generated_contents (generated_content_id),
    platform_name text NOT NULL,
    publish_status text NOT NULL CHECK (publish_status IN ('queued', 'published', 'failed')),
    published_at timestamptz,
    request_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    response_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system.system_configs (
    config_key text PRIMARY KEY,
    config_value jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS rss_feeds_source_status_idx
    ON core.rss_feeds (source_id, status, enabled);

CREATE INDEX IF NOT EXISTS ingestion_runs_feed_started_at_idx
    ON system.ingestion_runs (rss_feed_id, started_at DESC);

CREATE INDEX IF NOT EXISTS ingestion_runs_status_started_at_idx
    ON system.ingestion_runs (status, started_at DESC);

CREATE INDEX IF NOT EXISTS articles_source_published_at_idx
    ON core.articles (source_id, published_at DESC);

CREATE INDEX IF NOT EXISTS articles_review_queue_idx
    ON core.articles (review_status, recommendation_score DESC, published_at DESC);

CREATE INDEX IF NOT EXISTS articles_url_hash_idx
    ON core.articles (url_hash);

CREATE INDEX IF NOT EXISTS article_reviews_article_reviewed_at_idx
    ON core.article_reviews (article_id, reviewed_at DESC);

CREATE INDEX IF NOT EXISTS generated_contents_article_version_idx
    ON content.generated_contents (article_id, version_no DESC);

CREATE INDEX IF NOT EXISTS publish_logs_generated_content_idx
    ON system.publish_logs (generated_content_id, created_at DESC);
