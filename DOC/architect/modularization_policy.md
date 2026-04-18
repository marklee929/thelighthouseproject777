# Modularization Policy

This document defines mandatory module boundaries.

Its purpose is simple:

- keep orchestration thin
- move reusable logic into modules
- prevent mixed-responsibility files

These rules are strict.

---

## 1. One Responsibility Per Module

A file or module must have one primary responsibility.

Do not mix:
- orchestration
- state tracking
- scoring
- diagnostics
- export / persistence
- version-only behavior

**Rule:**
> If a component does more than one architectural job, split it.

---

## 2. Version Files Are Coordination Only

Version files exist only to coordinate flow.

They may:
- call modules
- define execution order
- assemble outputs
- apply version-specific thresholds

They must not permanently own:
- reusable scoring
- reusable diagnostics
- reusable state tracking
- reusable relation logic
- reusable export logic

**Rule:**
> Version files coordinate. Modules implement.

---

## 3. Shared Modules Own Reusable Logic

Reusable logic belongs in shared module space, not version space.

Shared modules are the default home for:
- scoring
- relation logic
- state tracking
- diagnostics builders
- export payload builders
- normalization / canonicalization

**Rule:**
> If logic is reusable across versions, it must leave the version directory.

---

## 4. Orchestration Does Not Compute Core Logic

Orchestration decides:
- what runs
- when it runs
- in what order it runs

Orchestration must not:
- compute complex scores directly
- manage reusable state directly
- build large diagnostic payloads directly
- duplicate module logic inline

**Rule:**
> Orchestration chooses flow, not core computation.

---

## 5. State Tracking Must Be Isolated

Long-lived tracking state must not accumulate inside orchestration files.

Examples:
- first seen step
- promotion history
- reinforcement count
- last observed step
- decay state

State should live in dedicated tracking modules.

**Rule:**
> Long-lived state belongs to state modules, not orchestrators.

---

## 6. Scoring Must Be Pure

Scoring modules must:
- accept explicit inputs
- return explicit outputs
- avoid hidden mutation
- avoid export or file writing
- avoid orchestration decisions

**Rule:**
> Scoring calculates. It does not coordinate.

---

## 7. Diagnostics Must Be Separate

Diagnostics are their own layer.

They may:
- read runtime state
- summarize outputs
- explain behavior

They must not silently become runtime logic.

**Rule:**
> Diagnostics observe runtime; they do not secretly become runtime.

---

## 8. Export and Persistence Must Be Separate

Export code and persistence code must not recompute runtime logic.

They should:
- serialize already computed results
- write already prepared payloads
- remain downstream from scoring and state

**Rule:**
> Export writes results. It does not create them.

---

## 9. Relation Logic Is Its Own Layer

Relation logic must stay independent from observer flow.

Relation modules may own:
- edge creation
- edge update
- direction inference
- persistence / decay
- stable / candidate transitions

They must not own:
- observer timing
- ranking policy
- search triggering
- bundle selection

**Rule:**
> Relation is a layer, not an observer feature.

---

## 10. Structure Is Above Relation

Relation and structure are not the same layer.

Relation asks:
- do edges exist?
- are they stable?
- are they directional?

Structure asks:
- do relations support trusted basins?
- do they support core formation?
- do they improve coherence?

**Rule:**
> Relation is not structure. Structure is built on top of relation.

---

## 11. Snapshots Are Outputs Only

Latest snapshots are read models.

They may:
- summarize state
- support inspection
- support downstream analysis

They must not:
- become the source of truth
- replace internal state modules

**Rule:**
> Snapshots are outputs, not architecture.

---

## 12. File Growth Is a Boundary Warning

Extraction is required when:
- multiple responsibilities appear
- repeated helper families appear
- state dictionaries keep growing
- diagnostics and runtime logic mix
- export formatting grows beside computation

**Rule:**
> The real problem is not file length, but responsibility density.

---

## 13. Dependency Direction Must Stay Clean

Preferred dependency flow:

- version orchestrator
  -> shared modules
  -> scoring / state / diagnostics / export helpers

Avoid reverse dependency:
- shared modules depending on version files
- scoring modules depending on orchestrators
- export modules computing runtime logic

**Rule:**
> Dependency flows downward from orchestration to reusable logic.

---

## 14. Refactoring Check

Before adding new logic, ask:

1. Is this version-only?
2. Is this reusable?
3. Is this state, scoring, diagnostics, or export?
4. Does an existing module family already own it?
5. Will adding it here mix responsibilities?

**Rule:**
> Place logic by responsibility first, convenience second.

---

## 15. Final Rule

Do not treat the current file as the natural home of new logic.

Always ask:
- what is this logic?
- which layer owns it?
- is it reusable?
- should it become a module?

**Final rule:**
> Put logic in the correct layer, not the nearest file.