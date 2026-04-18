Before implementing Lighthouse Project 777, first read the project rules under `DOC/architect/`.

Mandatory references:
- `DOC/architect/README.md`
- `DOC/architect/architect.md`
- `DOC/architect/database.md`
- `DOC/architect/design_rules.md`
- `DOC/architect/modularization_policy.md`
- `DOC/architect/versioning_policy.md`

Project context:
This is a standalone product-oriented content pipeline project.
The first version is not full automation.
It is a human-driven RSS news collection and review system for Facebook posting preparation.

Phase 1 goal:
- collect latest Christian news from RSS feeds
- store article link, metadata, and raw article content in PostgreSQL
- use local LLM (Llama-based) to generate recommendation score and recommendation reason
- send article candidates to Telegram for human review
- allow human decisions such as good / bad / hold
- store review history in the database
- preserve traceability from RSS source to article to review to generated content

Important:
Do not use Naver news collection for this phase.
Use RSS sources defined in `DOC/documents/christian_news_collection.docx` if the file exists.
If the file format is hard to parse directly, first create a placeholder feed registry structure and document the expected ingestion format.

Implementation priorities:
1. database-first
2. modular service separation
3. traceable ingestion flow
4. human review flow
5. no monolithic service files

Required pipeline:
source -> rss_feed -> article -> review -> generated_content

Database task:
Using `DOC/architect/database.md` as the main reference, generate PostgreSQL DDL for the first implementation scope.

Create explicit schemas:
- core
- content
- system

At minimum, implement these tables:
- core.sources
- core.rss_feeds
- core.articles
- core.article_reviews
- content.generated_contents
- system.ingestion_runs

Database requirements:
- use UTF-8 assumptions
- use schema-qualified naming
- create primary keys, foreign keys, useful indexes
- add dedupe support for collected articles
- preserve review history
- separate raw article data from generated content
- do not place business tables in public schema

Service task:
After DDL, implement a modular RSS ingestion path with separated responsibilities:
- integrations/rss/
- services/ingestion/
- repositories/

Expected ingestion flow:
1. load RSS feed definitions
2. fetch RSS items
3. normalize metadata
4. check duplicates
5. save article records
6. record ingestion run result

Recommendation task:
Create a separate recommendation module that:
- reads collected candidate articles
- calls local LLM
- stores recommendation score and reason in article-related fields or a dedicated recommendation-safe structure
- does not mix recommendation logic into ingestion modules

Telegram review task:
Create a separate Telegram review flow that:
- sends top candidate articles to Telegram
- allows human decision capture: good / bad / hold
- stores review history in `core.article_reviews`
- keeps review logic separate from ingestion and LLM logic

Coding constraints:
- do not create giant service files
- keep orchestration thin
- keep repositories responsible for persistence only
- keep integrations responsible for external communication only
- keep reusable logic outside orchestration files

Output order:
1. proposed folder structure
2. PostgreSQL DDL
3. repository interfaces / modules
4. RSS ingestion implementation
5. LLM recommendation implementation scaffold
6. Telegram review implementation scaffold

If any requirement is ambiguous, preserve modularity and traceability first.

Do not over-engineer for publishing automation yet. Optimize for reliable ingestion, review traceability, and operator control in phase 1.