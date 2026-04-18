# The Lighthouse Project 777 - Architecture Rules

This document defines the architecture rules for The Lighthouse Project 777.

This project is not an experimental engine repository.
It is a product-oriented content pipeline system.

The system must remain:
- modular
- traceable
- reviewable
- database-driven
- easy to operate

These rules are strict and must not be violated.

---

## 1. Project Identity

The Lighthouse Project 777 is a Gospel-oriented content pipeline platform.

Its purpose is not merely to collect news.
Its purpose is to transform news into reviewed, meaningful, publishable faith-oriented content.

Core pipeline:

`source -> article -> review -> generated content -> publish`

This pipeline must be preserved in both code structure and database structure.

---

## 2. Product Nature

This repository is a standalone product.

It must NOT be treated as:
- an extension of another engine project
- a temporary side module
- a logic dump attached to an existing codebase

If external engines or AI systems are used, they must be consumed as dependencies or services.

**Rule:**
> Lighthouse is a product. External systems are tools.

---

## 3. Top-Level Architecture Principle

System growth must happen through separated modules and layers, not by inflating central files.

Bad:
- putting ingestion, review, generation, publishing, and logging in one service file
- mixing business flow and persistence logic
- embedding platform-specific publish logic inside core domain code

Good:
- separate pipeline stages into dedicated modules
- keep orchestration thin
- isolate persistence, domain, and integration logic

**Rule:**
> Grow sideways by modules, not vertically by file size. :contentReference[oaicite:1]{index=1}

---

## 4. Mandatory Top-Level Structure

The project should be organized around responsibility.

Recommended structure:

```text
the_lighthouse_project_777/
  architect.md
  README.md

  app/
    api/
    orchestration/
    services/
    domain/
    repositories/
    integrations/
    jobs/

  database/
    ddl/
    migrations/
    seeds/
    docs/

  ui/
    operator_console/

  docs/
    decisions/
    flow/
    policies/

  tests/

  5. Layer Responsibilities
5.1 domain

Owns business entities and state definitions.

Examples:

article
source
review
generated content
publish result

Must NOT:

call external APIs directly
execute SQL directly
contain UI behavior
5.2 services

Own business use cases.

Examples:

collect articles
review article
generate content
publish content
retry publish

Must NOT:

become giant god-services
absorb repository internals
absorb platform-specific low-level logic
5.3 repositories

Own persistence access.

Examples:

article repository
review repository
publish log repository
config repository

Must:

isolate database access
be schema-aware
keep SQL and persistence logic centralized

Application logic must not scatter ad-hoc SQL everywhere.

5.4 integrations

Own external system communication.

Examples:

Telegram integration
Meta/Facebook integration
Instagram integration
AI generation provider
RSS or crawler adapter

Must NOT:

own business policy
decide approval rules
decide final publish eligibility

Rule:

Integrations connect. Services decide.

5.5 orchestration

Own high-level flow only.

Examples:

pipeline runner
approval-triggered generation flow
scheduled publish flow

Must remain thin.

It may:

call services in sequence
assemble steps
manage flow timing

It must NOT:

contain core business rules
contain SQL
contain platform-specific implementation details

Rule:

Orchestration coordinates. Modules decide.

6. Core Pipeline Modules

The first implementation must preserve these explicit pipeline modules:

ingestion
review
generation
publishing
logging/config

Suggested mapping:

app/services/ingestion/
app/services/review/
app/services/generation/
app/services/publishing/
app/services/system/

Do not merge all stages into one pipeline service.

7. Database Architecture

This project uses PostgreSQL as the primary database.

DBMS: PostgreSQL 17+
Database name: lighthouse
Encoding: UTF-8
Main schemas:
core
content
system

public must not be used as the main business schema.

Schema responsibilities:

core

Owns source and article backbone.

Tables:

sources
articles
article_reviews
content

Owns transformed and generated outputs.

Tables:

generated_contents
system

Owns operational and audit metadata.

Tables:

publish_logs
system_configs
8. Database Design Rules
8.1 Preserve the pipeline

Database structure must reflect real pipeline order:

source -> article -> review -> generated content -> publish log

Do not flatten these stages into one oversized table.

8.2 History over overwrite

Do not destroy important decisions.

Examples:

reviews must remain as records
publish attempts must remain as logs
generated content versions should be preserved when practical
8.3 Raw and generated data must be separated

Do not store generated devotional or post output inside raw article fields.

Wrong:

generated caption inside core.articles.body_raw

Correct:

raw source content in core.articles
generated output in content.generated_contents
8.4 Repositories own persistence

Application code must not depend on free-form query sprawl.

Rule:

SQL lives in repositories or migration files, not everywhere.

9. Initial Relationship Model

Initial ERD summary:

core.sources
1:N core.articles
core.articles
1:N core.article_reviews
1:N content.generated_contents
content.generated_contents
1:N system.publish_logs
system.system_configs
standalone
10. Naming Rules
table names: plural snake_case
column names: snake_case
primary key: <table_singular>_id
foreign key: <referenced_table_singular>_id
timestamps:
created_at
updated_at

Avoid vague names:

data
temp
misc
value1

Use explicit names:

publish_status
review_comment
dedupe_hash
content_type
11. File Rules

No file should absorb multiple unrelated responsibilities.

Bad:

one file handling article ingestion, review logic, AI prompting, and publish retry
one repository handling all tables
one integration file covering every platform

Good:

one responsibility per module
clear boundaries
flow composition through orchestration

Rule:

If a file grows because new logic is added, extract the new logic.

12. No Hidden Coupling

Do not create silent dependencies between stages.

Bad:

generation logic assuming direct DB table structure everywhere
publisher logic mutating article state directly
Telegram review flow bypassing review service

Good:

services communicate through explicit contracts
repositories isolate persistence
orchestration defines allowed flow

Rule:

Pipeline stages may connect, but they must not collapse into each other.

13. Platform Separation Rules

Publishing targets must be separated from core content logic.

Bad:

hardcoding Meta logic into content generation modules
mixing Telegram approval syntax into article entity logic

Good:

platform adapters under integrations
publishing service decides when to call which adapter

Examples:

integrations/telegram/
integrations/meta/
integrations/instagram/

Rule:

Platforms are adapters, not the domain.

14. Review Safety Rules

Human review is a first-class stage.

This project currently uses a human-driven filtering phase.

Therefore:

approval must be explicit
reviewer decision must be recorded
content generation must not bypass review by default
publish must not bypass approval by default

Rule:

Human review is not a side feature. It is part of the product core.

15. Operator-Centered UI Rule

The UI must reflect operator workflow, not developer convenience only.

The operator console should prioritize:

mission status
pipeline visibility
approval actions
publish results
system alerts

Flow visibility matters more than raw developer controls.

16. Initial Product Priorities

The first implementation should optimize for:

structural clarity
source traceability
human review history
generated content separation
publish auditability
low-friction operator flow

Do not optimize for maximum automation too early.

Phase 1 is human-driven filtering and data accumulation.

17. Codex Generation Rules

When generating code, migrations, or database DDL, Codex must follow these rules:

create schemas first if they do not exist
generate tables in dependency order
keep raw article data separate from generated content
place business tables only in explicit schemas
preserve pipeline order and traceability
generate repository-friendly structures
keep orchestration thin
isolate integrations from business rules
do not generate giant service files
prefer explicit constraints over hidden assumptions
separate DDL, seed data, and runtime logic
respect PostgreSQL schema-qualified naming
18. First Implementation Scope

The first implementation must include:

database
core.sources
core.articles
core.article_reviews
content.generated_contents
system.publish_logs
system.system_configs
services
ingestion service
review service
generation service
publishing service
integrations
Telegram review integration
at least one publish adapter placeholder
19. Final Rule

Before adding any new code, ask:

which layer owns this?
is this domain logic, service logic, repository logic, or integration logic?
is this reusable?
does this collapse pipeline boundaries?
does this create a new monolith?

If the answer is unclear, do not code yet.
Define the boundary first.

Final Rule:

Lighthouse must remain a modular product system, not a growing pile of features.