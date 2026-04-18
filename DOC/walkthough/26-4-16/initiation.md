Read these project documents first:
- DOC/architect/README.md
- DOC/architect/architect.md
- DOC/architect/database.md
- DOC/architect/design_rules.md
- DOC/architect/modularization_policy.md
- DOC/architect/versioning_policy.md

Also use these project references:
- DOC/documents/christian_news_collection.docx
- Progress Linear Destination (PLD) Framework.pdf
- Cross-Continental SNS Strategies of Faith-Oriented Organizations.pdf

Goal:
Implement a phase-1 article selection system for Lighthouse Project.

Phase-1 operating model:
- collect Christian news from RSS
- store article link, metadata, and raw content
- analyze each article for:
  1. reaction potential
  2. PLD stage fit
  3. operator / brand suitability
- send top candidates to Telegram for 4 reviewers
- any reviewer can mark confirm / reject / hold
- confirmed candidates become Facebook posting candidates

Selection philosophy:
The system should not optimize for sensationalism alone.
It should optimize for articles that can be transformed into curiosity-first, psychologically safe, PLD-compatible content.

Required analysis dimensions:
1. reaction_score
   - emotional trigger
   - inversion / surprise narrative
   - self-projection potential
   - comparison / anxiety / hope trigger

2. pld_fit_score
   - entry_fit
   - hook_fit
   - loop_fit
   - trust_fit

3. operational_score
   - content transformation ease
   - question-based framing ease
   - brand safety
   - moderation/platform risk
   - reviewer confirmation likelihood

Final score:
final_score = 0.40 * reaction_score + 0.35 * pld_fit_score + 0.25 * operational_score

Hard reject conditions:
- unverifiable or low-trust sources
- high policy-risk conflict bait
- insider-only religious language with weak broad-entry potential
- articles that cannot be converted into curiosity-first or reflection-first content

Expected output:
1. database field extensions needed for article scoring and review tracking
2. modular article analysis service
3. PLD stage classifier
4. Telegram candidate delivery flow for 4 reviewers
5. confirm / reject / hold persistence
6. Facebook candidate queue creation after confirmation

Constraints:
- no monolithic service files
- repository layer for persistence
- integrations separated from business logic
- orchestration must remain thin
- preserve source -> article -> review -> generated_content pipeline