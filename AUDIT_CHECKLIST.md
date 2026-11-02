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
  - [x] M1b-2 (actuals, 2026-10-30) — all 60 /projects/<pid>/actuals/...
    routes + /register (4,063 lines) → routes/actuals.py; the super-admin
    pool tools (7 routes) → routes/actuals_admin.py. Route map re-verified
    IDENTICAL (423 routes). Still to slice: /docs (36 routes), /admin (rest).
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
- [x] A2 — Unified evidence: expense_evidence table (Alembic 0004, backfilled
  from all three legacy shapes with ON CONFLICT dedupe) is THE doc→expense
  attachment (kinds: primary/backup/itemized). Write-through at every choke
  point — Create expense, mark-backup, itemization (invoice auto-attached to
  each subline), match-confirm. Line Ledger merges evidence rows beneath each
  expense with kind-aware 📎 labels; backup_of_txn_id stays populated in
  parallel (removal deferred until nothing legacy reads it).
- [x] A3 — "⏳ awaiting payment" chip on invoice/receipt-born expenses with
  no bank charge reconciled yet (Actuals rows + Line Ledger markers; clears
  naturally when QBO reconcile merges the payment). Line Ledger summary gains
  exception counts: "⚠ No backup N" (expenses with no evidence) and
  "⏳ Awaiting payment N". Actualizing 2.0 model complete (A1+A2+A3).
- [x] A4 — Invoice-side backups (owner 2026-07-22: "you should be able to do
  it from the receipt itself, or from the invoice itself"): the doc-detail
  modal gains a "📎 Backup documents" panel showing backups attached to the
  invoice total AND each itemized subline, with attach (ranked candidate
  docs; a still-uncoded receipt's own charge is absorbed exactly like the
  receipt-side flow) and detach (restores an absorbed charge to the queue).
  Shared _absorb_receipt_as_backup helper backs both directions.

## 🧾 Actuals Phase 4 — fringes to 6500 Administrative (2026-11-01, Steven's Oct 30 decision)
- [x] A4-1 — Schema: `budget_line.fringe_for_section` (indexed) + `auto_kind`
  (alembic `0023a_admin_fringe_lines`, mirrored in `_web_worker_essential_columns`).
  Data migration is the boot task `_migrate_fringes_to_admin` (system_task_log
  marker `migrate_fringes_to_admin_2026_11` + advisory lock, idempotent): one
  `Payroll Fringes — <Department>` line in 6500 per labor department on every
  project's target budget (Working, else Estimated), the four hidden auto-calc
  lines, legacy per-section fringe lines adopted (id kept), parsed
  "employer fringes" rows re-pointed from section:6500 / the old 6500 pot when
  the person's "— payroll" sibling resolves the department; unresolved rows are
  counted in the task log and left in place.
- [x] A4-2 — Calc: `calc_line` / `calc_line_from_schedule` never put fringe in the
  line total; `calc_top_sheet` pools every department's pot into 6500
  (`fringe_by_section` keyed by labor section, `fringe_home_section`); lines
  tagged `fringe_rollup` / `auto_calc` calc as zero (`COMPUTED_LINE_TAGS`) and
  carry their number on `estimated_total`, written through by
  `_sync_fringe_lines` on each render. `__fringe_rollup__` sentinel and the
  `ProjectSheet.fringe_rollup` toggle are retired (column stays, ignored).
- [x] A4-3 — Grid / Top Sheet / PDF: labor sections show a read-only `<sec>-FR`
  summary linking to 6500; 6500 shows the per-department rows + fee / pcf; 6000
  shows WC / PLI; every auto row has an Actual column fed by its hidden line and
  in-place editing through the one Settings POST (`budget_settings`).
- [x] A4-4 — Parser: `docs_parse_people` targets the department line, the WC
  line and the fee line; `extract_people` returns `workers_comp` / `platform_fee`
  separately; `actuals.parse_people_split` tie-out keeps the split, books the
  difference to the person and returns a delta the UI flags yellow.
- [x] A4-5 — Register / workspace: computed lines list under 6500 (6000) as
  coding targets with their department name; labor sections offer no fringe row.
- [ ] A4-6 — Deferred: the Estimated-only view of a project that has a Working
  budget shows the fringe rows from its own calc but the lines live only on the
  Working budget (by design); FEATURES rows for the old per-section fringe row
  are superseded by this entry. Confirm in a deploy log that
  `migrate_fringes_to_admin_2026_11` ran once in production.

## 🧾 Documents Phase 3 — payment attribution, QuickBooks vendor mirror, 1099 sheet (2026-11-02, Steven's Nov 1 decision)
- [x] D3-1 — Schema: `transaction.payee_id` (FK crew_member, indexed), `payee_source`
  (`qbo | line | doc | alias | manual`), `qbo_entity_ref`; `vendor_alias.payee_id`;
  `qbo_connection.vendor_cdc_since`; new `qbo_vendor` mirror table (alembic
  `0024_txn_payee`, mirrored in `_web_worker_essential_columns`). The sqlite branch
  adds the FK columns as plain integers (sqlite cannot ALTER in a constraint).
- [x] D3-2 — Backfill: boot task `_backfill_txn_payee` (system_task_log marker
  `backfill_txn_payee_2026_11` + advisory lock 20261102, sqlite-safe body): sets
  `payee_id` where the document owner, the line's one crew member, a linked alias
  or an exact vendor-name match name ONE payee; disagreeing signals are logged to
  the ActivityLog (`attribution_conflict`, once per row) and left NULL. Confirm in
  a deploy log that the marker ran once in production.
- [x] D3-3 — Precedence (`actuals.set_txn_payee`): fill NULL; a writer replaces only
  the sources it names (`qbo` → `qbo`, `line` → `line`, the doc editor's pick → every
  automatic source); a `manual` value is never replaced by automation.
  `paid_by_payee` prefers `payee_id` and falls back to the old inference only for
  NULL rows, so nothing that counted before disappears.
- [x] D3-4 — Vendor mirror: `qbo_sync.sync_vendors` (full pull → CDC, watermark
  minus 5 min, re-pulls in full when the watermark is older than the CDC window);
  QuickBooks → FPBudget only; linked payee refreshed (legal name, company, email,
  phone, address, Vendor1099, Active for vendor / loan-out kinds; `name` never),
  exact DisplayName links an unlinked non-W-2 payee, ambiguous / unknown wait for
  `/admin/qbo-vendors`. Runs ahead of the project syncs in the 12-hour loop.
  `sync_project` stamps `qbo_entity_ref` and attributes `qbo` through
  `CrewMember.qbo_vendor_id`.
- [x] D3-5 — The one write: `qbo_sync.ensure_vendor_for_payee` → link an exact
  DisplayName match or `create_vendor` (POST /vendor with a body that carries no
  Id / SyncToken / sparse; the helper refuses one that does). No update helper
  exists in the module. `POST /payees/<id>/qbo-create` (409 for linked / W-2),
  the New-payee checkbox and the Payee page button use it; mirrored fields are
  read-only in the edit form and refused by `apply_payee_fields` / `crew_edit`.
- [x] D3-6 — Write paths: link / unlink (`line`), confirm_match + itemize +
  payroll parse + doc editor / inbox owner pick (`doc`), CSV import + manual +
  doc-born rows (`alias` from `VendorAlias.payee_id` or an exact name), manual
  expense `payee_id` (`manual`), vendor-name link / unlink on the Payee page.
  Wrap Book 1099 review shows the QuickBooks state beside the W-9.
- [ ] D3-7 — Deferred: a payee picker on the Actuals workspace row (the
  `manual` source is reachable only through the manual-expense body and the
  document editor today); mirroring a QuickBooks value that was EMPTIED (an
  empty value never blanks the local copy); QuickBooks "Deleted" CDC entries
  are skipped, not unlinked.

## 🧹 Cleanup A — safety (2026-11-02, branch `cleanup/a-safety`)
From the read-only audit of 2026-11-02: data-loss paths, cross-project
access and money rules. Pure rules live in `actuals.py` ("Safety rules"
banner) with `tests/unit/test_safety_rules.py`; the routes are walked end to
end in `tests/unit/test_safety_app.py`.

**Data loss**
- [x] A1 — Receipt edits write through ONLY to the doc's own ledger row
  (`source='doc_upload'`, `actuals.doc_edit_writes_through_to`); a matched
  bank charge keeps the bank's vendor / amount / date (a blank doc date used
  to wipe it).
- [x] A2 — Docs wipe deletes only doc-born rows, unlinks bank / manual rows
  (coding kept), commits BEFORE any Dropbox move, logs to Activity; the
  confirm text in `b03.js` / `docs_upload.html` says exactly that. Super-admin
  only (unchanged).
- [x] A3 — `/admin/cleanup-estimate-txns` deletes doc-born rows only, unlinks
  bank rows, clears evidence + splits; the page shows delete vs unlink per row.
- [x] A4 — Review-queue resolve: the transaction must belong to the flag
  (`actuals.anomaly_flag_txn_ids`), `delete_txn` refused (400) for non-doc
  sources, `accept_amount` / `apply_vendor` write to the doc's own row only.
- [x] A5 — Deleting a budget version: owner / admin only; 409 `needs_force`
  when transactions or splits are coded to it, `?force=1` + explicit UI
  confirm (budget.html, gantt.html); activity-logged with a snapshot.
- [x] A6 / A22 — `project_delete` and `projects_bulk_delete` share
  `_delete_project_rows` (FK-safe order, now including `budget_comment`,
  `schedule_waypoint` and doc evidence); render archive PDFs → delete →
  commit → THEN move the Dropbox folder.
- [x] A7 — delete-duplicate needs a surviving twin (`actuals.is_duplicate_twin`:
  same |amount|, date, vendor or card) or 409; survivor inherits coding;
  splits / evidence / backup refs cleared.
- [x] A8 — QuickBooks purge: super_admin only, `project=<id>` required
  ('all' refused), releases only claims tied to deleted `(qbo_txn_id,
  qbo_txn_type)` pairs, clears evidence + splits, activity-logged.

**Access**
- [x] A9 — Budget-in-project check (`Budget.query.filter_by(id=bid,
  project_id=pid).first_or_404()`) added to `delete_line`, `toggle_sync_omit`,
  `budget_lever_delete`, `set_schedule_label`, `assign_crew`,
  `direct_contact_delete`, plus `delete_tax_credit` and `set_role_number`
  (assignment joined to a line of the budget) found by the sweep.
- [x] A10 — Version lock: `_budget_lock_error` (pure core
  `actuals.budget_lock_denial`) runs in the before_request gate AND inside
  `upsert_line` / `delete_line`; `delete_line`, `api_budget_line_save`,
  `api_budget_line_delete` added to `_BUDGET_WRITE_ENDPOINTS`.
- [x] A11 — `_docs_check_row_access(require_editor=True)` on /update, /reocr,
  /parse-people, attach-backup, detach-backup — viewers get 403.
- [x] A12 — `/crew/<cid>/profile.json` requires `_require_global_editor`.
- [x] A13 — POST `/actuals/qbo-accounts` is admin only.
- [x] A14 — `/projects/<pid>/archive` and bulk-archive: owner / admin only.
- [x] A15 — Cron token accepted ONLY in `X-Cron-Token` (401 otherwise); API
  tokens revoked on every password change (`_revoke_api_tokens`); CSRF
  "transition" seed-and-allow path removed.
  - [ ] A15b — `api_token.expires_at` (schema change; left for a later phase).

**Money**
- [x] A16 — Amount edits: refused on bank-sourced rows; a split row's new
  amount must equal its split sum (`actuals.amount_edit_allowed`).
- [x] A17 — Wrap-book export and the "Code expenses" bucket totals use
  `signed_amount_value` (credits net; they counted $0 / positive before).

**Hygiene**
- [x] A18 — `_delete_budget_cascade` and `delete_line` clear
  `transaction_split`, `budget_comment`, `sub_budget_line`, `estimate_share`,
  `schedule_waypoint`, `budget_lever` and unpin `sub_budget`.
- [x] A19 — `analyzer_batch` created in `_web_worker_essential_columns`.

## 🧹 Cleanup B — links and legacy panels (2026-11-02, branch `cleanup/b-panels`)
Every Docs / Actuals link opens the new screens; the hidden legacy panes on
the budget page and their JS are gone. Route snapshot: 5 removals, 0 additions.

**Links**
- [x] B1 — Budget-page Dashboard cards: Uncoded → `/projects/<pid>/actuals`,
  Unmatched → `?view=needs_backup`, Docs to review → `/projects/<pid>/docs`,
  Matches to review → `?view=receipts`. They are plain `<a class="dash-card">`.
- [x] B2 — `_project_tabs.html` (docs inbox / doc editor strip): Docs and
  Actuals go to the inbox / workspace; POs and Sub-Budgets to their pages;
  Schedule and Call Sheets through `/projects/<pid>/budget?page=schedule|callsheets`
  (`project_budget_redirect` resolves the budget id — no new route).
- [x] B3 — callsheet / gantt / pos / sub_budgets / docs_upload tab strips,
  the PO page's "Manage" link (`/actuals?txn=`), the doc editor's Back link
  (inbox) and the inbox's "Classic list" link (removed).
- [x] B4 — Old bookmarks: `?tab=docs` / `?tab=actuals` on the budget page
  `location.replace` to the new screens.
- [x] B5 — Top nav: Crew → Payees (`/payees`).

**Legacy panels**
- [x] B6 — `#tab-docs` (~870 lines) and `#tab-actuals` (~1,420 lines) deleted
  from `budget.html` with their hidden tab buttons and the `?tab=` lazy-load
  plumbing (`active_tab`, `data-needs-load`). Kept: the document detail modal
  (`#docDetailOverlay` + `_doc_editor.html`, opened by line receipt links,
  People packets, Dashboard queue, Line Ledger) and `#actualsLineOptsTpl`
  (the modal's pickers clone from it).
- [x] B7 — `b02.js` deleted; `b03.js` 2,468 → 297 lines (modal glue only);
  `b04.js` 4,891 → 935 lines (Dashboard / Action Center, Find-duplicates
  modal, picker glue, toast); `b14.js` `_watchPanel` removed. Dead per the
  audit and gone: `actualsCleanup`, `codeFilterNeeds`, `actualsFindReceipt`,
  `actualsAutoCode`, `actualsRunAutoMatch`, `openDupTxnReview`, `docsFmtSize`.
- [x] B8 — `budget_view` no longer builds the transaction list, uploader
  filter, doc-coding parity map, QBO status, line suggestions, backup
  suggestions, code buckets, doc groups or line-label maps the panes read.
- [x] B9 — Routes removed: `/projects/<pid>/reconcile`, `/register`,
  `reconcile/workspace.json`, `actuals/init` (GET that created a legacy
  Actual budget), `actuals/reconcile-columns.json`. Templates removed:
  `reconcile.html`, `crew.html` (never rendered). `routes/crew.py` stays —
  its JSON / edit / delete / docs routes back the Payee page.
- [x] B10 — Tests: `tests/test_actuals.py` deleted; `tests/test_docs.py`
  rewritten against the Docs inbox.
- [ ] B11 — Follow-ups seen, not done here: `_buildLocationDocs` /
  `_buildVendorView` (budget.html People tab) still read `#docsHistory .doc-row`,
  which has been empty on every normal load since the June lazy-load — they
  should read `/actuals/docs.json` like `_buildPacketView`; `gantt.js` calls
  `fetchTotals()` before `init()` sets `_pid/_bid` (404 on
  `/projects/undefined/...` on every schedule load, pre-existing).

## 🧹 Cleanup C — one live Working, the new budget page header (2026-11-02, branch `cleanup/c-version-model`)
The version model has ONE rule ("current" = newest of its kind) and the budget
page has the approved header: version strip, one-row tab strip, Overview,
Budget control bar with ONE variance control, per-line `+` / `⋯` actions,
regrouped Settings. Route snapshot: 12 removals, 0 additions.

**Model**
- [x] C1 — `budget_mode` is only `estimated` | `working`: the ~84
  `in ('working','actual')` checks are `== 'working'` (app.py, budget.html,
  gantt.html, callsheet.html, actuals.py, routes/actuals.py); models.py:358
  comment; `_budget_type` (app.py:25251).
- [x] C2 — Dead Actual paths removed: `clone_estimated_to_working`,
  `clone_working_to_actual`, `working_to_actual_line`,
  `_materialize_missing_actual_line`, `ensure_actual_mirrors`,
  `sync_working_to_actual`, `get_current_actual_budget` (actuals.py); the 11
  repair routes (`/admin/repair-phantom-workings`, `/projects/<pid>/debug-mirror`,
  `actual-audit`, `rebuild-actual-mirror`, `sync-actual-from-working`,
  `repair-clone-links`, `migrate-actuals`, `/projects/migrate-actuals/scan`,
  `/budget/repair-line-links`, `sync-line-order`, `crew-po-mismatch-report`) and
  `/projects/<pid>/budget/<bid>/working/init` — all unreferenced from templates / JS.
  `budget_view` lost its legacy Actual rollups and the `is_actual_view`
  branches (46 in budget.html); an `is_actual` URL redirects to the live
  budget (app.py:7958). `link_transaction_to_line`'s translate-an-Actual-pick
  branch stays (actuals.py:1611). `manual_actual` is out of the line-update
  field list and `manual_by_section` (app.py:8895); the column stays.
- [x] C3 — `_run_budget_mismatch_scan` (app.py:12581) scans the LIVE budget
  through the pure `actuals.line_overages` rule — it looked for a current
  Actual and had not fired since July.
- [x] C4 — `actuals.get_current_working_budget` / `get_current_estimated_budget`
  / `get_live_budget` (actuals.py:1320–1356, NULL-safe: current flag first, then
  highest version_number, then newest id) replace the ~12 copy-pasted queries
  (their old `budget_mode IS NULL AND parent_budget_id IS NOT NULL` filter is
  gone); `pick_current` (1359) is the pure twin budget_view / gantt / callsheet
  use on an already-loaded list.
- [x] C5 — "Current" = newest of its kind: Estimated v2 can exist while Working
  v1 stays live. The header says so (state B) and offers ONE action, the
  strip's "Start Working vN" (→ `create_working_from_estimated`, app.py:6342);
  the version dropdown's "+ Initialize Working" row and the Top Sheet banner are
  gone. The Viewing toggle and the strip share ONE rule,
  `actuals.working_exists_for_version` (1378) — never `working_exists`.
- [x] C6 — `_resolve_sister_budget` / `_mirror_line_move` (app.py:10406 /
  10450) pair by `version_number` only.
- [x] C7 — ONE variance control (Budget tab bar, budget.html:1388) with ONE
  key set `vs_estimated` · `vs_working` · `est_to_working`
  (`actuals.VARIANCE_BASES`, 1395; aliases for the old page + export keys);
  the Top Sheet and the Export dialog read the same stored choice
  (localStorage `fpbudget.varianceBasis.<pid>`); the PDF route accepts the
  aliases (app.py:13755) and every basis reads + = under (budget_pdf.html:353).
- [x] C8 — "Show N archived" renders archived rows in the budget page's
  version picker (budget.html:196, is_actual excluded); the call sheet picker
  uses `_version_groups` (callsheet.html:351, `version_groups` passed from
  callsheet_view); `project_duplicate` skips `is_actual` budgets (app.py:5303).
- [x] C9 — `SubBudget.budget_id` is no longer written (app.py:22065,
  sub_budgets.html no longer posts it; models.py:1647) — "always on the
  current Working" is the label on the page.

**UI** (templates/budget.html, static/style.css:5941+, static/budget-js/b12.js)
- [x] C10 — Version strip (810) states A/B/C from `version_strip`
  (app.py:9559); Viewing toggle; Notes button with count (header + control bar).
- [x] C11 — One-row tab strip (864) with More ▾ (Travel, POs, Catering,
  Sub-Budgets, Locations, Call Sheets, Assumptions, Activity, Tools; counts);
  every `data-tab` key kept.
- [x] C12 — Overview (903) replaces the Dashboard: three cards, By-department,
  Needs you (counts from `routes.actuals.actuals_counts`, factored out of
  `actuals_workspace_json` → `_build_workspace`, and `_docs_inbox_review_count`),
  Recent activity (5 rows via `_scoped_activity_query`), review queue kept
  (still fed by `/projects/<pid>/anomalies`). Clean up / Scan moved to
  Settings → Maintenance (4581).
- [x] C13 — Budget control bar (1359) per the artboard; grid untouched apart
  from the per-line actions.
- [x] C14 — Per-line `+` / `⋯` (1689, menus 9252 / 9260; b12.js:53+) in a
  sticky trailing `col-rowact` cell (always in reach when the grid scrolls);
  Discussion / Lever chips; lock-dot + hover card from `comment_latest`;
  Esc / arrow keys; flag, note, lever, PO, sub-budget, move, delete wired to
  the existing endpoints. Reconcile / Line ledger stay on the Actual cell's ▸.
- [x] C15 — Settings regrouped (4285+): Budget Info · Display · Payroll and
  fees · Project Fringes · Maintenance; note bar → Admin; the Production
  Company Profile card moved to Admin (admin.html:288, same JSON endpoints).
- [x] C16 — Tests: `tests/unit/test_version_model.py` (pure) and
  `tests/unit/test_version_model_app.py` (app-booting, self-skipping).
- [ ] C17 — Follow-ups seen, not done here: viewing an older Estimated still
  auto-promotes it to `version_status='current'` (pre-existing), which makes it
  the live Estimated for `pick_current`; the budget grid still overflows
  horizontally below ~1500px (pre-existing — the row actions are sticky now so
  they stay reachable); `gantt.js` 404 on `/projects/undefined/...` (B11).
