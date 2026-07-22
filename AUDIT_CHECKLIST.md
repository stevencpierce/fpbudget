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
- [x] H1b — Call-sheet delivery (PDF builds, Dropbox archive, email/SMS loop)
  runs in a background worker with per-recipient status commits ('sent' /
  '⚠ send failed' badges live-update in Send History); the send request
  returns instantly.
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
  - [x] H6b — Rep view resolution is ONE eager-loaded query per send
  (_rep_view_maps) instead of lines×assignments×contacts lazy scans per rep.
- [x] H7 — Analyzer batch state is durable: AnalyzerBatch table (Alembic rev
  0002) write-through-mirrors _raw_pending/_pending; any worker rehydrates a
  batch it doesn't hold in memory, so recycles/restarts no longer drop
  uploaded batches. Stale rows pruned after 7 days.
- [x] H8 — CSRF protection live: session token + fetch/form shim on every
  mutating request; public token routes (call-sheet confirm, estimate respond,
  cron, OAuth) exempt.

## 🟡 Medium
- [x] M1a — Monolith-split pattern established + first slice landed:
  routes/budget_templates.py registers on the same app object (endpoint
  names/URLs byte-identical — verified via scripts/route_snapshot.py: 361
  routes, zero drift). CLAUDE.md documents the slice workflow; route audit
  covers routes/*.py in CI.
  - [x] M1b-1 — Crew & support-contact routes (8 routes, 274 lines) →
    routes/crew.py; the shared _require_global_editor guard stays in app.py.
    Route map re-verified IDENTICAL (361 routes).
  - [ ] M1b-2 — Continue slicing: /docs (36 routes), /admin (59), actuals.
  - [ ] M1c — Break up the 1,950-line `budget_view`.
  - [ ] M1d — Move boot/migration code (~2.6k lines) into boot.py.
- [ ] M2 — Collapse the 5 overlapping project-access helpers into one.
- [ ] M3 — Collapse the ~30 inconsistent JS escape helpers into one canonical
  pair.
- [x] M4 — Every dependency ceiling-pinned (< next breaking major; no floors,
  so current resolution stays valid). Resolver verified clean. ⬜ Optional
  later: true-pin via `pip freeze` in a Render shell → requirements.lock.
- [x] M5 — fp_analyzer bare `except:` converted to `except Exception:` (no
  longer swallows SystemExit/KeyboardInterrupt).
- [x] M6 — Upload allowlist + magic-byte validation on all 7 upload routes
  (documents / images / CSV kinds; HTML/SVG rejected; stream rewound); raw
  doc + logo serving force non-PDF/non-raster-image content to
  application/octet-stream + attachment; global X-Content-Type-Options:
  nosniff.
- [ ] M7 — Timestamps: standardize on timezone-aware UTC / TIMESTAMPTZ.
- [ ] M8 — Accessibility pass on the call sheet (labels on editable cells,
  status not conveyed by color alone).
- [x] M9 — 20 script blocks (~14,000 lines, incl. the 4.7k and 3.5k engines)
  extracted to static/budget-js/*.js (cache-busted, classic scripts, document
  order — semantics identical). Jinja values hoisted via window.__BJ inline
  preambles so name/scope stay in place. ~4.7k genuinely template-driven
  lines remain inline. js_check now lints the static files in CI.

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
- [x] v3 — 📎 Backup suggestions: uncoded doc-only receipts matching an
  itemized invoice subline (amount ±$3/10%, date ±10d, receipt vendor ≈ the
  SUBLINE's description/vendor) get an advisory one-click "Backup of …?"
  chip that runs mark-backup with the matched target. Already-backed
  sublines excluded; advisory only, never automatic.
- [x] v4 — Queue speaks the match/backup/activate model: receipts section
  retitled "Receipts — waiting (not counted yet)" with the three fates named;
  doc-only rows' picker reads "⚡ Activate — pick budget line"; the Match-view
  legend states that receipts count $0 until matched / marked backup /
  activated.

## 💳 Actualizing 2.0 (2026-07-20, owner design: documents vs. expenses)
Model: invoices/receipts are EVIDENCE, never directly codeable. A document
either MATCHES an imported charge (its proof) or the user CREATES an expense
from it (cash/reimbursement/accrual) — and every expense gets coded + backed.
- [x] A1 — Enforced: set-line/set-coa reject coding a doc-born row unless the
  Create-expense step runs (create_expense flag stamps activated_at; the doc
  rides along as backup evidence). Picker on document rows reads "＋ Create
  expense — pick budget line"; success toast names what happened. Alembic
  0003 adds activated_at and GRANDFATHERS already-coded doc rows. Match stays
  its own separate path (owner decision; "activate" renamed "create expense").
- [ ] A2 — Unify evidence: general doc→expense attachment replaces the
  backup_of_txn_id special case; itemization auto-attaches the invoice to its
  sublines; Line Ledger shows evidence uniformly.
- [ ] A3 — "Awaiting payment" state on created-but-unreconciled invoice
  expenses; every-expense-needs-backup exception chip.
