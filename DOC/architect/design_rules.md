# Design Rules

This document defines **mandatory architectural rules**, not suggestions.

Violating these rules will lead to structural degradation of the system.

---

## 1. No Single-File Expansion

Do not continuously expand one file just because it is the current execution entry point.

- Version-specific files such as a v3 observer must not become monolithic.
- Adding new logic directly into a large file is prohibited if it can be separated.

**Rule:**
> If logic grows, it must move outward into modules, not accumulate inward.

---

## 2. Version Files Are Orchestrators Only

Versioned components (v3, v4, etc.) must act as **orchestration layers**, not logic containers.

They are responsible for:
- calling modules
- coordinating flow
- assembling outputs

They must NOT:
- own reusable logic
- implement heavy algorithms
- store long-term reusable state logic

**Rule:**
> If future versions might reuse it, it does not belong in a version file.

---

## 3. Reusable Logic Must Be Extracted

Any logic that is:
- not tightly bound to a specific version
- likely reusable in future versions
- conceptually independent, such as tracing, scoring, or diagnostics

must be moved into **shared modules**.

Examples:
- pair tracking
- relation diagnostics
- summary calculation
- state transitions

**Rule:**
> Reusable logic must live outside version directories.

---

## 4. Separate by Responsibility, Not Location

Do not group code by where it is used. Group it by **what it does**.

Preferred separation:
- orchestration
- state tracking
- scoring
- diagnostics
- summaries
- persistence / export

**Rule:**
> A module should represent a responsibility, not a call site.

---

## 5. No Hidden Logic Inside Helpers

Do not create shallow helper functions that hide large logic inside version files.

Bad:
- small wrapper functions in modules
- real logic still buried in observer

Good:
- actual computation extracted
- modules contain real logic and state

**Rule:**
> Extraction must move real logic, not just rename it.

---

## 6. Design for Future Versions (v4, v5, ...)

Before adding any new logic, ask:

> Will a future version need this?

If yes:
- design it as a reusable module now
- avoid rewriting it later

**Rule:**
> Always optimize for forward reuse, not immediate convenience.

---

## 7. Avoid Copy-Paste Versioning

Do not duplicate logic across versions.

- No copying v3 logic into v4
- No diverging implementations of the same concept

**Rule:**
> One concept = one implementation = shared module

---

## 8. Diagnostics Are First-Class Modules

Tracking, diagnostics, and summaries are not temporary.

They must:
- be modular
- be reusable
- evolve independently of version logic

**Rule:**
> Diagnostics must not be embedded as ad-hoc logging inside large files.

---

## 9. Minimize Cross-Module Coupling

Modules should:
- have clear inputs and outputs
- avoid hidden dependencies
- avoid reaching into other modules' internal state

**Rule:**
> Modules communicate via data, not internal knowledge.

---

## 10. Prefer Additive Changes, Not Structural Damage

When extending the system:
- do not break existing flow
- do not restructure unrelated components
- do not introduce implicit behavior changes

**Rule:**
> Extend cleanly, do not mutate unpredictably.

---

## Summary

The system must evolve from:

> "Patch the current file"

to:

> "Compose reusable modules"

This is mandatory for:
- stability
- scalability
- future version development
