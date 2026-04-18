# Versioning Policy (Lighthouse)

This document defines how versions (v1, v2, v3, ...) evolve in the Lighthouse Project.

Versions represent product capability evolution, not internal code complexity.

---

## 1. Core Principle

A new version is NOT created because:
- code becomes complex
- files grow
- features are added randomly

A new version is created only when:
- a new product capability is introduced
- a new stage in the pipeline becomes real
- the system behavior fundamentally changes

**Rule:**
> Versions represent product evolution, not code growth.

---

## 2. Version Meaning in Lighthouse

Each version represents a stage in the content pipeline evolution.

Example:

- v1:
  - human-driven filtering
  - manual approval
  - simple content generation

- v2:
  - assisted generation
  - partial automation
  - structured review flow

- v3:
  - automated content generation
  - rule-based validation
  - multi-platform publishing

- v4:
  - recommendation
  - personalization
  - meaning-based content routing

**Rule:**
> Each version must have a clear product-level capability shift.

---

## 3. No Premature Version Jump

Do not move to the next version until the current version is stable.

Requirements:
- pipeline works end-to-end
- data is consistent
- operator flow is validated

**Rule:**
> A version ends when it is operationally stable, not when features exist.

---

## 4. Proof Before Version Transition

Before upgrading version, prove at least one:

- capability gap:
  - current system cannot support needed feature

- operational bottleneck:
  - human workflow is too slow or inconsistent

- scalability limit:
  - system cannot handle growth

**Rule:**
> New versions require operational proof, not assumption.

---

## 5. Version Files Are Not Forks

Do not copy logic between versions.

Bad:
- duplicating services
- rewriting generation logic
- cloning pipeline flow

Good:
- reuse existing modules
- extend orchestration
- add new capabilities on top

**Rule:**
> Versions reuse modules, not duplicate them.

---

## 6. Shared Module Reuse

All reusable logic must remain shared.

Examples:
- content generation logic
- validation rules
- publishing adapters
- repository layer

**Rule:**
> If logic is reusable, it must not be version-specific.

---

## 7. Version-Specific Logic Must Be Minimal

Version differences should exist only in:
- orchestration
- policy
- configuration

They must not:
- own core business logic
- duplicate modules
- create separate data structures

**Rule:**
> Version layer must remain thin.

---

## 8. Backward Stability

When evolving:
- do not break working flows unnecessarily
- do not rewrite stable modules
- keep existing pipeline usable

**Rule:**
> Stability is more important than novelty.

---

## 9. Evolution Path (Lighthouse)

The system evolves as:

- v1:
  - collect → review → post (manual)

- v2:
  - collect → review → generate → post (assisted)

- v3:
  - collect → auto-generate → validate → post (semi-auto)

- v4:
  - intelligent routing / recommendation

**Rule:**
> Evolution follows pipeline sophistication, not internal complexity.

---

## 10. No Hidden Version Changes

Do not silently change system behavior.

Bad:
- introducing automation inside v1
- changing pipeline without version bump

Good:
- clearly define version boundary
- document changes

**Rule:**
> Version boundaries must be explicit.

---

## 11. Operator-First Transition

Before upgrading version:
- operator workflow must be understood
- failure cases must be known
- logs and traceability must exist

**Rule:**
> Measure real usage before evolving.

---

## 12. Migration Strategy

When upgrading:

1. keep existing modules
2. reuse database structure
3. extend orchestration
4. introduce new modules only if required

Avoid:
- rewriting everything
- breaking data flow

**Rule:**
> Evolution is extension, not reconstruction.

---

## 13. Version Naming Discipline

Do not create meaningless versions.

Allowed:
- v2 when real automation appears
- v3 when validation logic is real

Not allowed:
- v1.5
- temporary versions for experiments

**Rule:**
> Version names must represent real product shifts.

---

## 14. Final Rule

Before creating a new version, answer:

- what can current version NOT do?
- what new capability is required?
- which modules stay unchanged?
- what pipeline stage changes?

If unclear:
→ do not create new version

**Final Rule:**
> Versions exist to mark product evolution, not developer intent.