# Actuals / Reconcile Tool Consolidation — Blueprint (2026-06-22)

Goal: replace ~8 fragmented buttons across the Actuals tab + Dashboard with one
cohesive **Actuals workspace** where ingestion, automatic checks, and a single
review queue work together. Decisions below were confirmed with Steven 2026-06-22.

## Confirmed decisions
1. **Code suggestions:** live in BOTH the Review queue ("Codes to confirm") AND as
   inline ✨ chips in the grid for fast power-coding.
2. **Run mode:** scanners run AUTOMATICALLY (on upload/sync + scheduled) AND there's
   a manual "Run checks" button for on-demand.
3. **Ingestion:** Import CSV / Sync QBO / manual assign MOVE INTO the unified
   surface (not kept as a separate Actuals toolbar).

## Current overlaps (from the 2026-06-22 audit)
- **Matching:** 🎯 Find matches (manual, heuristic `find_match_candidates`) · 🤖 AI
  match (`actuals_ai_match` → `ai_layer.pick_match`) · 🔎 Review matches — all write
  `match_status='suggested'`.
- **Duplicates:** 🔍 Scan for issues → `double_coded` (`scan_double_coded`, Action
  Center) · 🧹 Reconcile (`actuals_reconcile_scan`, modal, `dup_charges`/`phantom`).
- **Vendor cleanup:** ✨ Clean up data (Dashboard, docs+charges) · ✨ Clean up
  vendors (Actuals, charges only) — same `_ai_clean_document`/`_ai_clean_transaction`.
- **Coding:** ✨ Suggest codes + inline chips (not in the queue yet).

AnomalyFlag types + producers: `double_coded` (`_run_double_coded_scan`),
`budget_mismatch` (`_run_budget_mismatch_scan`), `people_line`
(`_run_people_line_scan`), `vendor_cleanup` + `data_issue` (`_ai_clean_*`).

## Target architecture: engine vs. review surface
Split the two concerns that are currently welded together one-button-each.

### A. Engine — background scanners (no per-scanner buttons)
Auto-coding, matching, cleanup, duplicates, budget-mismatch, people→line. They run:
- automatically on upload / QBO sync,
- on the scheduled job (the 4pm fallback — still needs CRON_TOKEN set on Render),
- and on demand via ONE **"Run checks"** button.

### B. One review surface — the Review queue (today's Action Center)
Every finding, whatever produced it, is a queue item with the SAME shape:
**what it found → open the evidence (doc/charge) → Confirm / Fix / Dismiss.**
Item types: **Matches · Duplicates · Codes · Vendors · Budget · People→Line.**

## The merges
1. **Matching → one concept "Matches to review."** AI match is the engine; manual
   receipt-picker becomes "pick a different receipt" INSIDE a match item (not a
   separate top-level modal). Retire 🎯 Find matches as a primary button.
2. **Duplicates → one item type.** Fold `double_coded` + Reconcile dup/phantom into
   a single "Duplicate / double-coded" item; Reconcile's side-by-side becomes the
   EXPANDED view of that item. Ideally merge `scan_double_coded` +
   `reconcile-scan` dup logic; at minimum one surface. (Both already ID-aware as
   of f2b50cb / sign-aware as of f599992.)
3. **Cleanup → one button.** Collapse the two cleanup buttons into one "Clean up"
   that always covers docs + charges.
4. **Coding → queue + inline.** Surface suggestions as "Codes to confirm" items;
   keep inline ✨ chips in the grid.

## Unified Actuals workspace layout
- **Top bar:** Import CSV · Sync QBO · **Run checks** (+ auto on upload/sync/schedule).
- **Review queue** (the consolidated Action Center): grouped by item type, consistent
  Confirm/Fix/Dismiss, each item opens its evidence; Reconcile = expanded dup view;
  manual match-pick inside match items.
- **Transaction grid** below: inline ✨ code chips + manual assign dropdown retained.
- **Dashboard** keeps the financial rollup/health + a queue count badge that
  deep-links into the workspace's Review queue.

## Phasing (smallest risk first)
- **Phase 1 (routing/UI only, no scanner logic change):** merge the two cleanup
  buttons; unify the duplicate surfaces into one item type; make "matches" one
  concept. Move ingestion controls into the workspace top bar.
- **Phase 2:** code suggestions as queue items; Reconcile becomes the expanded view
  of a duplicate item; manual match-pick folded into match items.
- **Phase 3 (automation):** scanners auto-run on upload/sync + schedule (arm the
  4pm job — needs CRON_TOKEN on Render); "Run checks" becomes optional; queue is
  always current.

## Constraints to preserve
- All AI advisory + fail-open; AI never auto-deletes or moves money.
- Memory-first: `VendorCategoryMap` (codes), `VendorAlias` (vendor names),
  `MatchRejection` (never re-propose a rejected match).
- Idempotent queue via `AnomalyFlag.dedup_key` + dismissed-suppression.
