# Database Architecture

## 1. Database Overview
This project uses PostgreSQL as the primary database.

- DBMS: PostgreSQL 17+
- Database name: `lighthouse`
- Schema strategy:
  - `core`: source data and domain backbone
  - `content`: generated content and transformation results
  - `system`: logs, configs, and operational metadata
- Default schema usage: avoid relying on `public` except when absolutely necessary

---

## 2. Design Principles

### 2.1 Schema Separation
The database must be organized by responsibility.

- `core`
  - raw articles
  - source registry
  - review records
- `content`
  - generated posts
  - transformed messages
  - reusable output artifacts
- `system`
  - publish logs
  - config values
  - operational events

### 2.2 Clear Pipeline Preservation
The database must preserve the actual pipeline of the product:

`source -> article -> review -> generated content -> publish log`

Each stage must remain structurally independent.
Do not merge raw source data with generated or published data.

### 2.3 History Over Overwrite
Important decisions and transitions must be stored as history, not overwritten destructively.

Examples:
- review decisions must be stored in a separate review table
- publish attempts must be stored in a separate publish log table
- content versions should be preserved when practical

### 2.4 Minimal Public Schema Usage
`public` should not be used as the main business schema.
All major business tables must belong to explicit schemas.

### 2.5 Repository-Driven Access
Application code must not depend on direct ad-hoc SQL everywhere.
Database access should be mediated through repository or persistence layers.

---

## 3. Initial Entity Scope

The first implementation scope should include these core entities:

### core schema
- `sources`
- `articles`
- `article_reviews`

### content schema
- `generated_contents`

### system schema
- `publish_logs`
- `system_configs`

---

## 4. Initial Relationship Model

### core.sources
One source can have many articles.

### core.articles
One article belongs to one source.
One article can have many review records.
One article can produce many generated contents.

### core.article_reviews
Each review belongs to one article.
Reviews are append-only records of approval, rejection, or hold decisions.

### content.generated_contents
Each generated content belongs to one article.
One generated content can have many publish log records.

### system.publish_logs
Each publish log belongs to one generated content.
A single content may be published to multiple platforms.

### system.system_configs
System-wide key-value settings table for operational control.

---

## 5. Naming Conventions

- Table names: plural snake_case
- Column names: snake_case
- Primary keys: `<table_singular>_id`
- Foreign keys: `<referenced_table_singular>_id`
- Timestamp columns:
  - `created_at`
  - `updated_at`
- Status columns should use explicit text values or constrained enums
- Avoid vague names like `data`, `value1`, `temp_field`

---

## 6. Required Base Columns

Most business tables should include:

- primary key
- `created_at`
- `updated_at`

Where applicable, also include:
- `status`
- `version_no`
- `collected_at`
- `published_at`

---

## 7. Data Integrity Rules

- Every article must belong to a valid source
- Every review must belong to a valid article
- Every generated content must belong to a valid article
- Every publish log must belong to a valid generated content
- Foreign key constraints must be explicit
- Unique constraints should be added where duplicate ingestion is possible
  - example: source + external_id
  - example: dedupe_hash

---

## 8. Content and Source Separation Rules

Do not store transformed content inside raw article fields.

Wrong:
- storing generated devotional text in `articles.body_raw`

Correct:
- keep raw article data in `core.articles`
- keep generated outputs in `content.generated_contents`

---

## 9. Operational Notes

- PostgreSQL should run with UTF-8 encoding
- Business schemas should be created explicitly:
  - `core`
  - `content`
  - `system`
- Future extensions may include:
  - tags
  - schedules
  - templates
  - reviewers/users
  - automation jobs

---

## 10. Codex Generation Rules

When generating SQL or persistence code, Codex must follow these rules:

1. Create schemas first if they do not exist
2. Generate tables in dependency order
3. Add primary keys first, then foreign keys
4. Never place major domain tables in `public`
5. Preserve the pipeline:
   - source
   - article
   - review
   - generated content
   - publish log
6. Prefer explicit constraints over implicit assumptions
7. Generate idempotent SQL where practical
8. Separate DDL from seed data
9. Do not collapse raw content tables and generated content tables into one
10. Respect PostgreSQL naming and schema-qualified references

---

## 11. Initial ERD Summary

- `core.sources`
  - 1:N `core.articles`
- `core.articles`
  - 1:N `core.article_reviews`
  - 1:N `content.generated_contents`
- `content.generated_contents`
  - 1:N `system.publish_logs`
- `system.system_configs`
  - standalone

---

## 12. First Implementation Goal

The first database implementation should prioritize:

- structural clarity
- ingestion traceability
- review history
- generated content separation
- publish auditability

The goal is not maximum complexity, but a stable base for future automation.