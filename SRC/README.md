# Lighthouse Project 777 Source Overview

This directory contains the executable source code and database implementation assets for Lighthouse Project 777.

## Structure Overview

- `database/`: PostgreSQL DDL and schema migration assets
- `the_light_house_project_777/`: the main application package

## Current Implementation Direction

The project targets a standalone product-oriented content pipeline. In Phase 1, the priority flow is:

1. Load RSS source definitions
2. Collect and normalize articles
3. Store data in PostgreSQL
4. Analyze articles with a local LLM
5. Route candidates to Telegram reviewers
6. Create Facebook posting candidates

## Code Design Principles

- Keep business logic in the service layer.
- Isolate external communication under `integrations/`.
- Restrict database access to `repositories/`.
- Manage DDL and schema assets under `SRC/database/`.
- Keep orchestration thin.
- Avoid large monolithic service files.

## Key Package Examples

- `database/ddl/`: phase-specific PostgreSQL DDL
- `integrations/rss/`: RSS and article-body collection integrations
- `repositories/`: PostgreSQL persistence layer
- `services/ingestion/`: article ingestion pipeline
- `services/analysis/`: reaction, PLD, and operational scoring
- `services/review/`: Telegram review and decision handling
- `services/selection/`: phase-1 article selection orchestration
