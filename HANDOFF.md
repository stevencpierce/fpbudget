# FPBudget — Active Session Handoff

**Session ended:** 2026-05-11 — user traveling, will resume on laptop.
**Working directory (this machine):** `/Users/frameworkproductions/PythonProjects/FPBudget`
**Prod URL:** https://fp-budget.onrender.com
**Latest commit:** `881ebfc` (added this HANDOFF.md)
**Status:** No work in progress, no uncommitted changes, prod healthy on `96cb8ab`.
**Dropbox copy of this file:**
`BUDGET SOFTWARE DEVELOPMENT/FPBudget — Budget Module/HANDOFF.md`

When resuming on the laptop, this file in Dropbox is identical to the one
on GitHub — pull whichever is faster. The Dropbox copy will go stale once
work resumes; trust the repo copy after that.

---

## How to resume this on another machine

1. Clone the repo locally:
   ```bash
   git clone git@github.com:stevencpierce/fpbudget.git
   cd fpbudget
   ```
2. Start a fresh Claude Code session in that directory.
3. **Paste the contents of this file into the first message**, prefixed with:
   > "I'm continuing a session from another machine. Here's the state of play — please read it carefully and confirm understanding before doing anything."

That gives the new session full context: what's been shipped, what's broken, what's pending, conventions used, and the exact next action.

---

## What's currently shipped (recent commits, newest first)

| Commit | What |
|---|---|
| `96cb8ab` | Subtotal/grand-total heading colors adopt view color (blue/orange/red) |
| `e543747` | Sticky tabs + toolbar; preserve scroll position on budget mode switch |
| `7da5bd5` | View differentiation (body class view-estimated/working/actual) + variance basis persistence in localStorage |
| `fb6cf10` | Variance colors: negative = red, positive = blue |
| `eb09ad5` | PO #/vendor/Cap inline-editable directly on the card |
| `c677a06` | PO surfacing: visible badge in Estimated/Actual + scope note on POs page + PO tag on Actuals txn detail |
| `a0b7c4a` | PO calc rebuild: Cap/Lines/Estimates/Billed/Remaining, over-cap fires only on real spend |
| `d663d15` | Real-time variance update on inline save |
| `34915f3` | Variance basis renamed: "Estimated V Working" / "Working V Actual" / "Estimated V Actual" |
| `6ff4d90` | N+1 query elimination (bulk-fetch ScheduleDay in cross-view loops) |
| `5de6875` *(in 9bdc882)* | **THE root-cause fix** — exclude `is_actual=True` budgets from `current_working_bid` resolution. The Actual budget was being misclassified as Working because `_budget_type()` lumps `working` and `actual` modes together. Cross-view Working column was reading Actual budget data instead of Working. |
| `feea9b7` | Memory: skip redundant N+1 query loops on same-budget cross-views |
| `74ec8e7` | Gunicorn `--max-requests 100 --max-requests-jitter 20` to recycle workers before OOM |
| `cbd6692` | 502 fix (gunicorn threads 4→8), trash-purge `app_context` bug, COA transaction migration rewrite (CASE WHEN instead of buggy sequential UPDATEs) |
| `fa84feb` | UnboundLocalError fix on `peer_actual_bid` |
| `7c01e41` | Global 500 error handler with `ERR-XXXXXXXX` ref + traceback logging |
| `9194c04` | Calc-trace HTML debug page at `/admin/debug/budget/<bid>` |
| `05009fb` | JSON debug endpoint at `/admin/debug/line/<bid>` |

---

## Open threads / next features (in priority order)

### 1. Mobile uploader — agreed to build, not started yet

**Goal:** lightweight, mobile-friendly receipt/doc upload page (replaces the abandoned external "FP Document Analyzer" app). Mirrors the v2 mobile UI from `~/PythonProjects/FP_Document_Analyzer_v2_mobile.zip` (already extracted to `/tmp/da-mobile` during the session — paths in there for reference).

**User's spec (confirmed in chat):**
1. **Simple v1 + bulk upload** (multi-file)
2. **Same auth** as the budget app (use existing `docs_only` role for staff who only need this page)
3. **≥90% confidence auto-files**, lower goes to review queue (existing pipeline already does this)
4. **Keep desktop Docs tab as-is** for power users — mobile is for phone/tablet
5. **Reuse the existing upload pipeline** (`fp_analyzer.analyze_and_file_single`), only the UI is borrowed from the v2 zip

**Plan (final, ready to execute):**

**Commit 1 — Route + UI**
- `GET /upload` → mobile-optimized form
- `POST /upload` → loop over files, call `fp_analyzer.analyze_and_file_single` per file
- New template `templates/mobile_upload.html` mirroring v2:
  - 📷 Take Photo (rear camera via `capture="environment"`)
  - 🖼 Choose from Library (file picker)
  - Drag-drop zone (desktop only via media query)
  - File accumulator using `DataTransfer` — supports bulk
  - Project picker scoped to `current_user`'s `ProjectAccess` rows
- Add `/upload` and any new endpoint names to `_DOCS_ONLY_ALLOWED` (around `app.py:1513`)

**Commit 2 — PWA manifest + apple icons + nav link**
- `static/upload-manifest.json` (mirror the v2 manifest but with FP Budget branding)
- Apple touch icons (192×192, 512×512)
- Admin page nav link to mobile upload (always visible for `docs_only` users)

**Open questions parked at "agreed, ready to start":**
- None — user confirmed all the design questions in chat. Just needs sign-off to write code.

**Reference files (for resumption):**
- Reuse from budget app: `app.py:15768` (`docs_upload_post` — existing single-file upload handler), `app.py:1513` (`_DOCS_ONLY_ALLOWED`)
- Mirror the UI from: `/tmp/da-mobile/FPReceiptRouter/templates/upload.html` + `static/style.css` + `static/manifest.json` (extracted from `FP_Document_Analyzer_v2_mobile.zip` — extract again if /tmp is gone)
- Existing pipeline: `fp_analyzer.analyze_and_file_single` (look at how it's called in `app.py:15768+`)
- User model permission system: `models.py:8` (User), `models.py:52` (ProjectAccess)

### 2. "Exactly half" calc bug on Travel-Flights — workaround in place, fix pending

The `agent_pct` handling is asymmetric between `calc_line` (treats as discount, subtracts) and `calc_line_from_schedule` (treats as agent fee, adds). For travel auto-managed lines, the `or 1.0` fallback in `budget_calc.py:655` also has a half-value side effect when qty=0.

**User's chosen workaround:** apply 100% discount to zero a line out. Works correctly now (the previous Working-column $472 bug was actually the C-5 root-cause issue, not this — that turned out to be a separate concern that resolved itself).

**Fix when ready:** unify how `agent_pct` is treated across both calc paths. Decide: is it always a discount (subtract from pre) or always an agent fee (add to subtotal)? Probably depends on `is_labor`. Tag with a `_AGENT_PCT_MODE` constant rather than scattering the logic.

### 3. Render plan upgrade — user decided "fine, it's beefy software"

Memory issues during rapid switching led to a recommendation to upgrade from Starter (512 MB) to Standard (2 GB) at $25/mo. User upgraded (or planned to). With C-4 + C-6 N+1 eliminations + the bigger plan, memory should be comfortable.

If 502s recur during rapid budget switching:
- Check Render dashboard for OOM events
- The next architectural step is caching `working_line_totals` / `estimated_line_totals` per `(budget_id, updated_at)` in Redis. See "Tier 2" in the chat history.

### 4. C-7 template-side subtotal fixes — cosmetic, deferred

Section subtotal badges in the line-item view still read frozen `ln.working_total` snapshots in some paths. C-3 fixed the Top Sheet but the per-section badges weren't fully migrated. Low priority — the per-line cells are live which is the user-visible primary.

### 5. PO calc — surface PO in Actuals view further? (debatable)

Currently the line-level PO indicator (purple pill with PO#) appears in Actuals view AND each transaction-detail expansion shows the PO header. User hasn't asked for more, but if they want PO on each individual transaction row, that's a small follow-up.

---

## Conventions / things to know

### View colors
- `view-estimated` body class → `--accent` (blue, default)
- `view-working` body class → `--orange`
- `view-actual` body class → `--red`

These are applied automatically by an IIFE at the top of the budget page's main `<script>` block (around line 7926 in `templates/budget.html`).

### Variance basis
- Three options: `e_vs_w`, `w_vs_a`, `e_vs_a` (renamed 2026-05-10 — keys match the labels and the math)
- Math: `X V Y` = `X - Y`. Positive = remaining/under (BLUE). Negative = over budget (RED).
- Persisted in `localStorage` under key `fpbudget.varianceBasis`.
- JS function `window._reapplyVarianceBasis()` re-renders all variance cells from current data-attrs — call after editing cell values.

### PO data flow
- Each PO has: `total_committed` (cap), `lines_total` (assigned BudgetLine.estimated_total), `estimates_total` (attached docs with category in estimate/quote/contract/PO), `receipts_total` (category receipt/invoice), `billed_total` (Transaction.amount sum on PO's lines)
- `over_cap` fires ONLY when `billed_total > total_committed`
- Rollup pulls from the Working budget canonical only (filtered by `is_actual=False`)
- Read-only PO badge in Estimated/Actual views = purple pill; editable in Working view = green badge

### Critical bug fix to remember: `is_actual` discrimination
`_budget_type(budget_mode)` returns `'working'` for both `budget_mode='working'` AND `budget_mode='actual'`. The Actual budget is distinguished only by `is_actual=True`. Before C-5, this caused Actual budgets to win `current_working_bid` lookups when they were newer than the real Working budget (which is always the case after a doc upload auto-creates the Actual). Any new code that filters budgets for "is this a Working budget?" must filter on `is_actual=False` too.

### Memory hygiene
- Gunicorn workers recycle every 80–120 requests via `--max-requests` (in `render.yaml`)
- Same-budget cross-view computations short-circuit to reuse `line_results` (C-4)
- N+1 ScheduleDay queries eliminated via bulk fetch (C-6)

### Error handling
- Global 500 handler gives users a page with `ERR-XXXXXXXX` ref
- Full traceback logged under that ref — grep Render logs for `ERR-<ref>` to find the exception
- Skipped for HTTPExceptions (404 etc.) which have their own handlers

### Debug endpoints (super admin only)
- `/admin/debug/budget/<bid>` — HTML calc-trace page, auto-refreshes every 5s. Shows every line's calc breakdown side-by-side with frozen snapshots. Red rows = snapshot bug, yellow rows = NULL-mode schedule legacy.
- `/admin/debug/line/<bid>?account_code=N&description=...` — JSON dump of a specific line + both calc paths' outputs.

---

## Project context / memory files

The user's auto-memory is in `~/.claude/projects/-Users-frameworkproductions/memory/MEMORY.md` and references several `.md` files:

- `project_fpbudget_module.md` — overall app architecture
- `project_chart_of_accounts.md` — COA structure (recently renumbered to MMB/ShowBiz alignment)
- `project_document_analyzer.md` — context for the mobile uploader we're about to build
- `reference_ntfy.md` — `ntfy.sh/fpbudget-2UNogKZFtFM` is the user's push notification topic. Notify after deploys per their convention.
- `reference_dropbox_archive.md` — archive paths for budget software development

The user prefers:
- ntfy push after every deploy or long task
- Co-author trailer `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` on every commit
- ScheduleWakeup for verify-after-deploy steps (4–5 min cadence — Render takes ~3 min to redeploy)
- Small commits, each verified before the next (this has been HARD learned today)
- Honest pushback when the user asks for something that would degrade stability

---

## If something is broken right now

1. Open https://fp-budget.onrender.com and check `/health` → expect 200
2. If 502, wait 30s (might be deploy transition) then check again
3. If still 502, check Render dashboard for instance events
4. If 500 with an `ERR-XXXX` page, look up that ref in Render logs
5. Most recent revert pattern that works: `git revert --no-edit <bad_commit> && git push`

---

## Resume action (paste this verbatim into your laptop session after this file)

> I have just read HANDOFF.md and understand:
>
> - Where we are (commits up through 96cb8ab)
> - What's pending (mobile uploader plan above, others)
> - The bug-fix history and conventions
>
> I am ready to start. The next agreed action is: **build the mobile uploader (Commit 1 — route + template).** Confirm and I'll proceed, or pivot to one of the other open threads.
