# AI Handoff - Lighthouse Project 777

## Project Purpose
Lighthouse Project 777 is not a simple news bot.
It is a Christian meaning-based content engine.

Core direction:
Truth -> Meaning -> Gospel
Content -> Living Faith

## Current Phase
Phase 1: Human-driven filtering system.

Flow:
RSS collection
-> PostgreSQL storage
-> local LLM article analysis
-> News Collector review cards
-> Approve / Modify / Reject / Drop
-> Telegram reviewer confirmation
-> Facebook candidate generation

## Current Repo Path
Main app:
SRC/the_light_house_project_777/

Database DDL:
SRC/database/ddl/

Frontend:
SRC/the_light_house_project_777/web/static/news_collector.js
SRC/the_light_house_project_777/web/static/news_collector.css
SRC/the_light_house_project_777/web/static/crew_dashboard.js
SRC/the_light_house_project_777/web/index.html

Backend entry:
SRC/the_light_house_project_777/main.py

## Architecture Rules
- Do not expand one giant file.
- Keep main.py thin.
- Use repositories for DB access.
- Use services for business logic.
- Use integrations for external systems.
- Do not put SQL in UI.
- Do not mutate raw article truth from review UI.
- Modify means editing review-facing summary/angle/question only.

## Current Completed Features
- PostgreSQL DDL phase files exist.
- News Collector review screen exists.
- RSS feed manager exists in UI.
- Candidate cards exist.
- Approve / Modify / Reject / Drop actions exist.
- Selection policy exists:
  services/news_collector/selection_policy.py

## Current Risk
main.py and frontend JS are starting to grow.
Next changes should avoid adding more logic into:
- main.py
- news_collector.js
- crew_dashboard.js

## Next Preferred Refactor
Move News Collector API routes out of main.py into a dedicated routes module.
Do not change behavior.


Review the current News Collector dashboard UI and apply a minimal frontend-only layout cleanup.

Problem:
When the News Collector module is selected, the screen currently shows News Collector Review, Task Queue, Global Log, Publish Review Cards, and other dashboard sections together. This makes the layout confusing and visually broken.

Goal:
Make News Collector feel like a focused pre-Social review screen.

Scope:
Frontend layout/CSS/JS only.
Do not change backend logic.
Do not change database schema.
Do not change article selection policy.
Do not modify RSS ingestion behavior.

Required changes:
1. When `News Collector` is selected, hide or collapse unrelated Social publishing sections.
   - Hide `Publish Review Cards` from the main News Collector view.
   - Hide `Live Command Input` if it is not part of the News Collector workflow.
   - Keep Task Queue visible only if compact and useful.

2. Keep `News Collector Review` as the primary top section.
   It should show:
   - candidate count
   - active feed count
   - refresh button
   - clear empty state

3. Improve the empty state message.
   Current message is too vague:
   "No collected article candidates are ready for review."

   Replace with a more useful message:
   "No reviewable candidates found. Active RSS feeds exist, but the current latest-window and PLD selection filters may have returned zero articles. Try collecting again or expanding the latest window."

4. Make Global Log compact under News Collector.
   - Set a clear max-height.
   - Ensure it scrolls internally.
   - Prevent it from overlapping or visually covering lower cards.
   - It should not dominate the News Collector screen.

5. Do not merge Social publish review logic yet.
   For now, hide `Publish Review Cards` while News Collector is selected.
   Approved article candidates can remain part of the later Social flow.

6. Keep changes minimal.
   Avoid large refactors.
   Do not move backend routes.
   Do not add new features.

Files likely involved:
- SRC/the_light_house_project_777/web/static/news_collector.css
- SRC/the_light_house_project_777/web/static/crew_dashboard.css
- SRC/the_light_house_project_777/web/static/news_collector.js
- SRC/the_light_house_project_777/web/static/crew_dashboard.js
- SRC/the_light_house_project_777/web/index.html only if necessary

Expected result:
When News Collector is selected:
- News Collector Review is the main visible section.
- Empty state explains why 0 candidates may happen.
- Task Queue is compact.
- Global Log is bounded and does not break layout.
- Publish Review Cards is hidden from this view.

Implement a News Collector retry cycle for the Facebook posting pipeline.

Context:
The goal of News Collector is not just to collect articles once. The larger workflow is to produce at least one Facebook posting candidate within a 1-hour cycle.

Current problem:
If no articles are found within the current latest-window, the UI shows 0 candidates and stops. This is too weak for the posting workflow.

Goal:
Add a lightweight retry/fallback collection policy.

Scope:
Backend service logic only first.
Do not change database schema.
Do not change frontend layout in this task unless necessary.
Keep changes minimal and modular.

Required behavior:
1. Add a News Collector cycle policy:
   - Target: produce at least 1 reviewable candidate within 1 hour.
   - Retry interval concept: 10 minutes.
   - If no candidate is found, the system should be able to run collection again.

2. Add fallback latest-window logic:
   - First attempt: recent_hours = 1
   - If 0 reviewable candidates: recent_hours = 3
   - If still 0: recent_hours = 6
   - Do not expand beyond 6 hours for phase 1.

3. Keep PLD/reaction/safety scoring intact.
   Do not bypass analysis.
   Only relax freshness window when no candidate exists.

4. Update service response payload from `/api/crew/news-collector/collect` to include:
   - attempt_window_hours
   - fallback_used: true/false
   - candidates_after_collection
   - next_retry_recommended: true/false
   - message

5. Files to inspect first:
   - SRC/the_light_house_project_777/services/news_collector/collection.py
   - SRC/the_light_house_project_777/services/news_collector/service.py
   - SRC/the_light_house_project_777/services/news_collector/selection_policy.py
   - SRC/the_light_house_project_777/services/ingestion/service.py
   - SRC/the_light_house_project_777/main.py

6. Implementation preference:
   - Put retry/fallback policy inside a dedicated small module if logic grows.
   - Do not bury policy inside Flask route.
   - Keep route thin.

Expected result:
When collection returns zero reviewable candidates for the 1-hour window, backend attempts wider windows up to 3h and 6h before returning final 0.
The response should explain whether fallback was used and whether another retry should be scheduled later.

Revise the News Collector filtering strategy for Phase 1.

Current problem:
The system filters articles too aggressively before review, causing 0 candidates even when active RSS feeds exist.

New Phase 1 goal:
News Collector should support a 1-hour Facebook posting cycle:
- every 10 minutes, collect and review up to 5 article candidates
- if at least one article is approved, move it to the next process
- if none are approved, continue the next 10-minute cycle
- maximum review exposure should be around 25 articles per hour

Required strategy change:
1. Make pre-storage filtering permissive.
   Pre-storage should only reject:
   - missing title
   - missing original_url
   - duplicate original_url
   - inactive source/feed
   - obvious spam or unsafe content

2. Do not hard reject articles at storage time based on:
   - popularity_proxy
   - PLD score
   - human-need terms
   - curiosity terms
   These should be used for ranking, not exclusion.

3. Review candidate selection should use ranking, not aggressive cutoff.
   Prefer:
   - recent 1 hour first
   - fallback to 3 hours
   - fallback to 6 hours
   - max 5 candidates per cycle
   - exclude already rejected, approved, published, or reviewed articles
   - limit same-source domination

4. Keep safety filters.
   Continue excluding:
   - obvious conflict bait
   - unverifiable spam
   - platform-risk content
   But avoid rejecting normal Christian news only because it has insider religious terms.

5. Add cycle metadata to collect/review response:
   - cycle_window_minutes: 10
   - target_candidates_per_cycle: 5
   - current_window_hours
   - fallback_used
   - candidates_returned
   - next_cycle_recommended

6. Add Bible verse recommendation only after approval.
   Do not generate Bible verse recommendations for every collected article.
   After article approval, provide a service hook that can generate up to 3 Bible verse suggestions:
   - verse_reference
   - verse_text
   - verse_reason
   - verse_url
   - rank

7. Keep architecture modular.
   Files to inspect:
   - services/news_collector/collection.py
   - services/news_collector/selection_policy.py
   - services/news_collector/service.py
   - services/ingestion/service.py
   - services/review/*
   - repositories/*
   Do not bury this logic in main.py or frontend JS.

Expected result:
The system should collect broadly, rank intelligently, expose up to 5 review candidates every 10 minutes, and only generate Bible verse suggestions after human approval.

Task:
Create a standalone utility for importing the World English Bible PDF into PostgreSQL.

Important scope rule:
This must be created under a utility directory only.
Do NOT mix this with the main application runtime code.
Do NOT modify main.py.
Do NOT modify frontend JS.
Do NOT wire this into the dashboard yet.

Utility location:
- UTIL/bible_import/

Source PDF location:
- DOC/documents/eng-web_all.pdf

If the actual folder is named `docuements`, use the existing folder name exactly.
Do not move the PDF unless necessary.

Context:
The source PDF is the World English Bible (WEB).
The PDF states that WEB is public domain and may be copied, distributed, posted online, and incorporated into software.
If the actual text is modified, the modified result must not be called “World English Bible.”

Goal:
Build a standalone utility that can process the source PDF and import Bible verses into PostgreSQL in one run.

Expected directory structure:

UTIL/bible_import/
- README.md
- import_web_bible_pdf.py
- web_pdf_parser.py
- bible_tag_rules.py
- bible_repository.py
- ddl/
  - 001_bible_verses.sql
- logs/
  - .gitkeep

Database DDL:
Create DDL under:
- UTIL/bible_import/ddl/001_bible_verses.sql

Tables:

1. bible_verses
- verse_id TEXT PRIMARY KEY
- translation TEXT NOT NULL DEFAULT 'WEB'
- book TEXT NOT NULL
- chapter INT NOT NULL
- verse INT NOT NULL
- reference TEXT NOT NULL
- verse_text TEXT NOT NULL
- normalized_text TEXT
- source_file TEXT
- source_page INT
- created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
- updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
- UNIQUE (translation, book, chapter, verse)

2. bible_verse_tags
- id BIGSERIAL PRIMARY KEY
- verse_id TEXT NOT NULL REFERENCES bible_verses(verse_id) ON DELETE CASCADE
- tag TEXT NOT NULL
- weight NUMERIC(5,2) DEFAULT 1.0
- source TEXT DEFAULT 'rule'
- created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
- UNIQUE (verse_id, tag)

3. bible_import_runs
- id BIGSERIAL PRIMARY KEY
- source_file TEXT NOT NULL
- started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
- finished_at TIMESTAMP
- status TEXT NOT NULL DEFAULT 'running'
- pages_processed INT DEFAULT 0
- verses_parsed INT DEFAULT 0
- verses_inserted INT DEFAULT 0
- verses_updated INT DEFAULT 0
- tags_inserted INT DEFAULT 0
- warnings_count INT DEFAULT 0
- notes TEXT

Utility behavior:
Create a command-line script:
- UTIL/bible_import/import_web_bible_pdf.py

Default behavior:
- Use DOC/documents/eng-web_all.pdf as the default source PDF.
- Process the full PDF in one run.
- Extract verses.
- Insert or update verses.
- Generate rule-based tags.
- Insert tags.
- Print a final summary.

Arguments:
- --pdf optional path, default DOC/documents/eng-web_all.pdf
- --database-url required unless using environment variable DATABASE_URL
- --dry-run
- --limit-pages optional
- --skip-tags optional
- --log-file optional, default UTIL/bible_import/logs/import_web_bible_pdf.log

Parser requirements:
- Extract text page by page.
- Skip front matter/table of contents/preface where possible.
- Skip glossary and non-biblical sections where possible.
- Parse book/chapter/verse/reference/verse_text.
- Preserve source_page.
- Handle multi-line verse text.
- Collect parse warnings instead of crashing where possible.
- Do not silently import obviously broken records.
- PDF parsing is allowed to be slow if it improves correctness.

Important parsing notes:
- The PDF has page headers such as “Genesis 1:1”.
- Verse numbers may appear inline.
- Verse text may wrap across lines.
- Footnotes may appear at the bottom of pages.
- Parser should try to exclude footnote markers and page artifacts where possible.
- Use conservative validation:
  - book must be recognized
  - chapter must be numeric
  - verse must be numeric
  - verse_text must not be empty
  - reference must be unique per translation

Book handling:
- Support standard 66-book canon first.
- If the PDF includes Deuterocanon/Apocrypha, do not import those by default in phase 1.
- Add an optional flag:
  --include-apocrypha
  but default should be false.

Tagging:
Create rule-based tags first.
Do NOT call LLM by default.

Allowed tags:
- money
- anxiety
- fear
- hope
- justice
- mercy
- pride
- wisdom
- work
- success
- suffering
- healing
- family
- forgiveness
- truth
- temptation
- generosity
- contentment
- identity
- community
- leadership
- conflict
- death
- faith
- prayer

Tagging rules:
- Match normalized verse text against keyword lists.
- Max 3-5 tags per verse.
- Assign weights based on keyword strength/frequency.
- source should be 'rule'.
- Keep the tag list controlled; do not generate random tags.

LLM extension:
Create a placeholder for future local LLM tag enrichment.
Do not enable it now.
If added later, it must only select from the fixed allowed tag list.
No free-form tag creation.

Import safety:
- Use upsert for bible_verses.
- Use insert-on-conflict-do-nothing for bible_verse_tags.
- Use transactions in reasonable batches.
- If parsing fails on a page, log warning and continue.
- If database write fails, rollback current batch and report clearly.

README requirements:
Create UTIL/bible_import/README.md with:
1. purpose of the utility
2. source PDF path
3. WEB public domain attribution
4. how to run DDL
5. how to run dry-run import
6. how to run full import
7. how to inspect counts after import
8. warning that PDF parsing is fallback-quality and structured WEB data is preferred if later available

Attribution text:
“Scripture text: World English Bible (WEB), public domain. Do not call modified text ‘World English Bible’.”

Architecture constraints:
- Keep all utility files inside UTIL/bible_import/
- Do not modify main.py
- Do not modify dashboard/frontend files
- Do not integrate with RSS, Telegram, Facebook, or News Collector yet
- This is an offline/import utility only

Expected result:
A self-contained import utility that can process DOC/documents/eng-web_all.pdf and populate PostgreSQL tables:
- bible_verses
- bible_verse_tags
- bible_import_runs

Task:
Add a Telegram Preview Card generation step before actual Telegram dispatch.

Context:
The News Collector should not send articles directly to Telegram yet.
Before Telegram dispatch, the system must generate a preview card that I can inspect in the dashboard.

Goal:
Create a review-card generation layer for selected article candidates.

Required workflow:
RSS collection
-> article storage
-> ranking / selection
-> verse matching top 3
-> Telegram preview card generation
-> dashboard preview
-> manual Send to Telegram later

Do not send to Telegram automatically in this task.

Required behavior:
1. Generate preview cards for selected review candidates.
2. Each preview card should include:
   - article_id
   - title
   - source
   - published_at
   - summary
   - article_url
   - top 3 Bible verse suggestions
   - default selected verse
   - why_selected
   - CTA draft
3. Store preview cards in a database table.
4. Show generated preview cards in the dashboard before Telegram dispatch.
5. Add a manual action/button later or placeholder:
   - Send to Telegram
   But do not implement actual Telegram sending if not already available.

Duplicate prevention:
Already selected/generated articles must not be selected again.

Exclude articles if:
- a review card already exists for that article and channel='telegram'
- the article has already been approved
- the article has already been rejected
- the article has already been published
- the article has already been sent to Telegram

Recommended new table:
review_cards
- card_id TEXT PRIMARY KEY
- article_id TEXT NOT NULL
- channel TEXT NOT NULL DEFAULT 'telegram'
- card_type TEXT NOT NULL DEFAULT 'article_review'
- payload_json JSONB NOT NULL
- status TEXT NOT NULL DEFAULT 'preview'
- created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
- sent_at TIMESTAMP
- UNIQUE (article_id, channel, card_type)

If there is an existing review/card table, reuse it instead of duplicating.

Card status values:
- preview
- sent
- approved
- rejected
- hold
- expired

Architecture constraints:
- Do not put card generation logic in main.py.
- Do not put card generation logic in frontend JS.
- Create a service such as:
  services/review/review_card_builder.py
  or services/telegram/telegram_preview_card_service.py
- Create a repository for review_cards persistence.
- Frontend only fetches and renders stored preview cards.
- Keep changes minimal.
- Do not change unrelated dashboard layout.
- Do not implement Facebook publishing in this task.

Verse matching requirement:
If article verse suggestions already exist, include top 3.
If not yet available, include an empty verses array and mark verse_status='missing'.
Do not invent Bible verses in the card builder.

Expected result:
- selected article candidates can produce Telegram preview cards
- preview cards are visible in dashboard
- duplicate candidates are prevented
- actual Telegram sending remains manual / separate