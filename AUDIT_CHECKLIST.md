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
  - [x] C4b — Sentry wired (activates when Steven adds `SENTRY_DSN` in Render;
    no-op until then). ⬜ Steven: create Sentry project + add the env var.
  - [x] C4c — Test suite REFUSES to run against production (defaults to
    localhost; prod needs ALLOW_PROD_TESTS=1). ⬜ Steven: create the staging
    service on Render (duplicate the fp-budget service off a `staging` branch
    with its own free Postgres), then set BASE_URL to it for test runs.
  - [x] C4d — tests/unit/test_budget_calc.py: 10 pure unit tests pin exact
    dollar amounts for calc_line/_effective_days (labor/non-labor, fringes,
    discounts, weeks, rounding); runs in CI on every push.

## 🟠 High
- [x] H1a — Timeouts added: Twilio 15s, Dropbox 30s.
- [ ] H1b — Move call-sheet send emails/SMS to a background job.
- [x] H2 — Upload/request body capped at 50 MB (413 above).
  - [ ] H2b — Stream large uploads instead of reading into memory.
- [x] H3 — Fuzzy reconcile with a differing amount now lands as 'suggested'
  with a review note instead of silently 'confirmed'.
- [x] H4 — Per-project sync lock (second click gets 'already running') +
  token refresh serialized under an advisory lock.
  - [ ] H4b — insert-on-conflict for qbo_txn_id (belt & suspenders).
- [x] H5a — Alembic live as the GO-FORWARD migration system (baseline rev +
  `alembic upgrade head` in preDeploy; boot-DDL lists frozen; CLAUDE.md
  documents the workflow).
  - [ ] H5b — Eventually port the frozen legacy boot-DDL lists into revisions.
- [x] H6a — The three missing hot-path indexes now created at boot.
  - [ ] H6b — Eager-load the recipient/rep lookups (N+1 on call-sheet send).
- [ ] H7 — Persist background-job state in the DB (analyzer batches can be
  silently dropped on worker restart).
- [x] H8 — CSRF protection live: session token + fetch/form shim on every
  mutating request; public token routes (call-sheet confirm, estimate respond,
  cron, OAuth) exempt.

## 🟡 Medium
- [ ] M1 — Split app.py (36k lines) into blueprints; start with /docs and
  /admin. Break up the 1,950-line `budget_view`.
- [ ] M2 — Collapse the 5 overlapping project-access helpers into one.
- [ ] M3 — Collapse the ~30 inconsistent JS escape helpers into one canonical
  pair.
- [ ] M4 — Pin dependencies (13 of 16 unpinned — any deploy can pull a breaking
  major version). Generate a lock file.
- [x] M5 — fp_analyzer bare `except:` converted to `except Exception:` (no
  longer swallows SystemExit/KeyboardInterrupt).
- [ ] M6 — Upload file-type allowlist + magic-byte check; ensure raw doc serving
  never returns text/html.
- [ ] M7 — Timestamps: standardize on timezone-aware UTC / TIMESTAMPTZ.
- [ ] M8 — Accessibility pass on the call sheet (labels on editable cells,
  status not conveyed by color alone).
- [ ] M9 — Extract the ~18k lines of inline JS from budget.html into cached
  static files.

## ⚪ Low / hygiene
- [x] L1 — Untrack the stale `.report.json` pytest artifact (+ .gitignore).
- [x] L2 — CLAUDE.md orientation doc added (verify pipeline, Alembic workflow,
  monitoring, gotchas).
- [x] L3 — Dev SECRET_KEY fallback is now random-per-boot (was a forgeable
  hardcoded constant).
- [x] L4a — Cron token compare is constant-time (hmac.compare_digest).
  - [ ] L4b — Expire call-sheet/estimate share links.
- [ ] L5 — Move top-level JS bootstrap calls into DOMContentLoaded (TDZ trap).

## 🧾 Docs/receipts model (2026-07-20, from live FIFA use)
- [x] v1 — "📎 Backup for an invoice" picker action: excludes the receipt's own
  charge from all rollups (receipt stays filed; note names the invoice).
- [x] v2 — Backup linkage: transaction.backup_of_txn_id column; chooser
  dialog ranks likely targets (invoice sublines first, by amount closeness);
  Line Ledger shows 📎 backup receipts beneath the charge they document.
- [ ] v3 — Duplicate scanner suggests "backup?" when a receipt ≈ an invoice
  subline (vendor/date/amount) instead of leaving a parallel charge.
- [ ] v4 — Rename/clarify queue language around Steven's activate model:
  receipts sit passive until matched to a charge, marked backup, or ACTIVATED
  into a standalone charge (cash/reimbursement).
