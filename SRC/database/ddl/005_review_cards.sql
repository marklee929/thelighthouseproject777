CREATE TABLE IF NOT EXISTS content.review_cards (
    card_id TEXT PRIMARY KEY,
    article_id UUID NOT NULL REFERENCES core.articles(article_id) ON DELETE CASCADE,
    channel TEXT NOT NULL DEFAULT 'telegram',
    card_type TEXT NOT NULL DEFAULT 'article_review',
    payload_json JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'preview',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMP,
    UNIQUE (article_id, channel, card_type)
);

CREATE INDEX IF NOT EXISTS idx_review_cards_channel_status_created
    ON content.review_cards (channel, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_review_cards_article
    ON content.review_cards (article_id, channel, card_type);
