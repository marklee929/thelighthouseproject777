-- Lighthouse Project 777
-- Phase 1 article selection extensions
-- Assumes 001_phase1_pipeline.sql has been applied first.

ALTER TABLE core.articles
    ADD COLUMN IF NOT EXISTS reaction_score numeric(5,2) CHECK (reaction_score >= 0 AND reaction_score <= 100),
    ADD COLUMN IF NOT EXISTS reaction_breakdown jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS pld_fit_score numeric(5,2) CHECK (pld_fit_score >= 0 AND pld_fit_score <= 100),
    ADD COLUMN IF NOT EXISTS pld_breakdown jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS dominant_pld_stage text,
    ADD COLUMN IF NOT EXISTS operational_score numeric(5,2) CHECK (operational_score >= 0 AND operational_score <= 100),
    ADD COLUMN IF NOT EXISTS operational_breakdown jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS final_score numeric(5,2) CHECK (final_score >= 0 AND final_score <= 100),
    ADD COLUMN IF NOT EXISTS selection_summary text,
    ADD COLUMN IF NOT EXISTS hard_reject_reason text,
    ADD COLUMN IF NOT EXISTS analysis_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS analysis_model text,
    ADD COLUMN IF NOT EXISTS analysis_version text,
    ADD COLUMN IF NOT EXISTS analyzed_at timestamptz,
    ADD COLUMN IF NOT EXISTS selection_status text NOT NULL DEFAULT 'pending_analysis';

ALTER TABLE core.articles
    DROP CONSTRAINT IF EXISTS articles_selection_status_check;

ALTER TABLE core.articles
    ADD CONSTRAINT articles_selection_status_check
    CHECK (
        selection_status IN (
            'pending_analysis',
            'scored',
            'hard_rejected',
            'review_queued',
            'review_hold',
            'review_confirmed',
            'review_rejected',
            'facebook_candidate_created'
        )
    );

ALTER TABLE core.article_reviews
    DROP CONSTRAINT IF EXISTS article_reviews_decision_check;

ALTER TABLE core.article_reviews
    ADD COLUMN IF NOT EXISTS reviewer_code text,
    ADD COLUMN IF NOT EXISTS reviewer_display_name text,
    ADD COLUMN IF NOT EXISTS dispatch_id uuid,
    ADD COLUMN IF NOT EXISTS review_context jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE core.article_reviews
    ADD CONSTRAINT article_reviews_decision_check
    CHECK (decision IN ('confirm', 'reject', 'hold'));

CREATE TABLE IF NOT EXISTS system.reviewers (
    reviewer_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    reviewer_code text NOT NULL,
    display_name text NOT NULL,
    telegram_chat_id text,
    telegram_username text,
    role_name text NOT NULL DEFAULT 'article_reviewer',
    active boolean NOT NULL DEFAULT true,
    reviewer_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT reviewers_reviewer_code_key UNIQUE (reviewer_code)
);

CREATE TABLE IF NOT EXISTS system.telegram_review_dispatches (
    telegram_review_dispatch_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id uuid NOT NULL REFERENCES core.articles (article_id),
    reviewer_id uuid NOT NULL REFERENCES system.reviewers (reviewer_id),
    telegram_chat_id text,
    telegram_message_id bigint,
    dispatch_status text NOT NULL DEFAULT 'queued',
    dispatch_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    callback_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_message text,
    sent_at timestamptz,
    acted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE system.telegram_review_dispatches
    DROP CONSTRAINT IF EXISTS telegram_review_dispatches_dispatch_status_check;

ALTER TABLE system.telegram_review_dispatches
    ADD CONSTRAINT telegram_review_dispatches_dispatch_status_check
    CHECK (dispatch_status IN ('queued', 'sent', 'failed', 'acted'));

ALTER TABLE core.article_reviews
    DROP CONSTRAINT IF EXISTS article_reviews_dispatch_id_fkey;

ALTER TABLE core.article_reviews
    ADD CONSTRAINT article_reviews_dispatch_id_fkey
    FOREIGN KEY (dispatch_id)
    REFERENCES system.telegram_review_dispatches (telegram_review_dispatch_id);

CREATE INDEX IF NOT EXISTS articles_selection_queue_idx
    ON core.articles (selection_status, final_score DESC, published_at DESC);

CREATE INDEX IF NOT EXISTS articles_analysis_model_idx
    ON core.articles (analysis_model, analyzed_at DESC);

CREATE INDEX IF NOT EXISTS article_reviews_article_decision_idx
    ON core.article_reviews (article_id, decision, reviewed_at DESC);

CREATE INDEX IF NOT EXISTS reviewers_active_idx
    ON system.reviewers (active, reviewer_code);

CREATE INDEX IF NOT EXISTS telegram_review_dispatches_article_idx
    ON system.telegram_review_dispatches (article_id, reviewer_id, created_at DESC);

CREATE INDEX IF NOT EXISTS telegram_review_dispatches_message_idx
    ON system.telegram_review_dispatches (telegram_chat_id, telegram_message_id);

CREATE UNIQUE INDEX IF NOT EXISTS generated_contents_facebook_candidate_uidx
    ON content.generated_contents (article_id, content_type)
    WHERE content_type = 'facebook_post_candidate';
