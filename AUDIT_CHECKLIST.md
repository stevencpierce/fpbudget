# FPBudget Audit Checklist (2026-07-20)

From the six-part code & quality review. ✅ = fixed & deployed · ⬜ = open.
Keep this file updated as items land.

## 🔴 Critical
- [x] **C1 — Lock down crew / template / location routes.** Any logged-in user
  (even viewer-only) could read the whole crew contact database and edit/delete
  shared crew, budget templates, and library locations. → 10 routes now require
  admin or an editor role on at least one project.
- [x] **C2 — Call-sheet save can no longer lose data.** An empty save can't
  wipe a sheet; unknown keys survive (server merges instead of replacing); a
  stale tab gets a "sheet changed elsewhere — reload" warning instead of
  silently overwriting another editor's work.
- [x] **C3 — Cross-site-scripting holes escaped.** The budget "overridden by"
  toast and the call-sheet Send panel (history badges, version, notes, sent-by,
  results) no longer render names/notes as raw HTML.
- [x] **C4 — Safety net started.** CI now runs on every push (Python compile,
  template parse, route audit, inline-JS check — each maps to a bug that
  actually shipped). Production 500s now push to the ntfy phone alert
  (rate-limited). Render's health check now verifies the database (/readyz),
  not a static "ok".
  - [ ] C4b — Add real error monitoring (Sentry): Steven adds `SENTRY_DSN` in
    Render, then wire `sentry-sdk`.
  - [ ] C4c — Point the Playwright test suite at a staging site (it currently
    creates/deletes projects on LIVE production).
  - [ ] C4d — Make the budget-math tests actually assert numbers (several pass
    no matter what).

## 🟠 High
- [ ] H1 — Timeouts on Twilio / Dropbox / Veryfi calls; move call-sheet send
  emails/SMS to a background job (a slow provider can hang a worker mid-send).
- [ ] H2 — Cap upload size (`MAX_CONTENT_LENGTH`) and stop reading whole files
  into memory (OOM risk).
- [ ] H3 — QBO fuzzy reconcile must stop silently overwriting a user-entered
  amount and marking it confirmed — flag a discrepancy instead.
- [ ] H4 — QBO sync: add a per-project lock + insert-on-conflict so a duplicate
  row can't roll back the whole batch; serialize token refresh.
- [ ] H5 — Move migrations to Alembic (boot-time DDL silently skips failed
  ALTERs; three hand-synced copies of the column list).
- [ ] H6 — Add missing DB indexes (`budget_line.budget_id`,
  `transaction.project_id`, `transaction.budget_line_id`) + eager-load the
  recipient/rep lookups (N+1 queries on every call-sheet send).
- [ ] H7 — Persist background-job state in the DB (analyzer batches can be
  silently dropped on worker restart).
- [ ] H8 — Add CSRF protection (Flask-WTF) on state-changing routes.

## 🟡 Medium
- [ ] M1 — Split app.py (36k lines) into blueprints; start with /docs and
  /admin. Break up the 1,950-line `budget_view`.
- [ ] M2 — Collapse the 5 overlapping project-access helpers into one.
- [ ] M3 — Collapse the ~30 inconsistent JS escape helpers into one canonical
  pair.
- [ ] M4 — Pin dependencies (13 of 16 unpinned — any deploy can pull a breaking
  major version). Generate a lock file.
- [ ] M5 — Log (don't swallow) the silent `except: pass` failures in the OCR
  pipeline.
- [ ] M6 — Upload file-type allowlist + magic-byte check; ensure raw doc serving
  never returns text/html.
- [ ] M7 — Timestamps: standardize on timezone-aware UTC / TIMESTAMPTZ.
- [ ] M8 — Accessibility pass on the call sheet (labels on editable cells,
  status not conveyed by color alone).
- [ ] M9 — Extract the ~18k lines of inline JS from budget.html into cached
  static files.

## ⚪ Low / hygiene
- [x] L1 — Untrack the stale `.report.json` pytest artifact (+ .gitignore).
- [ ] L2 — Add a README / CLAUDE.md orientation doc (promote the handoff notes).
- [ ] L3 — Require `SECRET_KEY` outside Render too (currently falls back to a
  hardcoded dev value off-Render).
- [ ] L4 — Expire call-sheet/estimate share links; constant-time cron-token
  compare.
- [ ] L5 — Move top-level JS bootstrap calls into DOMContentLoaded (TDZ trap).
