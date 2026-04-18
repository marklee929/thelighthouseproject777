# Architecture Rules (Must Read Before Coding)

This folder defines the core architectural rules of the Lighthouse Project.

These are mandatory rules. They are not suggestions.

If you are modifying or adding code:

- DO NOT expand a single file beyond its responsibility
- DO NOT mix pipeline stages into one module
- DO NOT embed reusable logic inside orchestration layers
- ALWAYS respect schema, service, and integration boundaries

---

## Read These First (Order Matters)

- architect.md
- database.md
- design_rules.md
- modularization_policy.md

Optional (advanced / later stages):
- versioning_policy.md

---

## Core System Model

The system is a **pipeline-based content engine**:

`source -> article -> review -> generated content -> publish`

This pipeline must be preserved across:
- database design
- service design
- module boundaries

---

## Development Flow (Lighthouse)

This project is not patch-driven.

Follow this flow:

1. Identify feature or requirement
2. Locate correct layer:
   - domain
   - service
   - repository
   - integration
3. Implement the smallest complete unit in that layer
4. Ensure pipeline integrity is preserved
5. Validate data flow and traceability

---

## Coding Rules (Enforced)

- One responsibility per module
- No cross-layer logic leakage
- No hidden coupling between pipeline stages
- No business logic inside integrations
- No persistence logic outside repositories

---

## Database Rules (Critical)

- Use PostgreSQL with UTF-8
- Use explicit schemas:
  - core
  - content
  - system
- Do NOT use public schema for business logic
- Preserve pipeline stages in schema design

---

## Architecture Priority

Always prioritize:

1. structural clarity
2. traceability
3. modular separation
4. pipeline integrity

Do NOT optimize early for:
- automation
- performance
- abstraction

---

## Codex Instruction

When generating code:

- follow architect.md first
- respect database.md schema definitions
- apply design_rules.md and modularization_policy.md strictly
- generate modular, layer-separated code
- never generate monolithic service files
- never collapse pipeline stages into one structure

---

## Final Rule

Before writing code, ask:

- Which layer owns this?
- Is this reusable?
- Does this break pipeline separation?
- Does this increase coupling?

If unclear:
→ do not code yet

**Final rule:**
> Build modules, not patches.