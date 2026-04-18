-- Lighthouse Project 777
-- News Collector feed-management and drop-action extensions
-- Assumes 001_phase1_pipeline.sql, 002_phase1_article_selection.sql, and 003_news_collector_review_screen.sql have been applied first.

ALTER TABLE core.articles
    DROP CONSTRAINT IF EXISTS articles_review_status_check;

ALTER TABLE core.articles
    ADD CONSTRAINT articles_review_status_check
    CHECK (review_status IN ('pending', 'good', 'bad', 'hold', 'dropped'));

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
            'review_dropped',
            'facebook_candidate_created'
        )
    );

ALTER TABLE core.article_reviews
    DROP CONSTRAINT IF EXISTS article_reviews_decision_check;

ALTER TABLE core.article_reviews
    ADD CONSTRAINT article_reviews_decision_check
    CHECK (decision IN ('confirm', 'reject', 'hold', 'drop'));
