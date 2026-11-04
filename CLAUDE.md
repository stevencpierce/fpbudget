# FPBudget — orientation for AI/dev sessions

Flask + SQLAlchemy + Postgres on Render (auto-deploys `main`). ~36k-line
`app.py`; `templates/budget.html` and `callsheet.html` carry heavy inline JS.
Owner: Steven Pierce (steven@thefp.tv), non-programmer — live production system.

## Verify before every deploy (CI runs these too — .github/workflows/ci.yml)
1. `python -m py_compile app.py models.py budget_calc.py actuals.py`
   then `python -m pyflakes app.py actuals.py budget_calc.py qbo_sync.py routes/*.py | grep "undefined name"` must print nothing (a NameError on one project's data shipped twice)
2. Jinja-parse changed templates
3. `python scripts/route_audit.py app.py routes/*.py`   (route-stealing + duplicate-URL guard)
4. `python scripts/js_check.py`             (inline-JS syntax)
5. `python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""`
   (needs `pip install -r requirements-ci.txt` — the app-booting modules
   must RUN, not skip; CI fails on any skip)
6. Browser suite against the seeded local app (`tests/README.md`):
   `python scripts/seed_local.py --port 5055 &` then
   `BASE_URL=http://127.0.0.1:5055 ADMIN_EMAIL=local-admin@thefp.tv ADMIN_PASSWORD=localpass-123 TEST_EMAIL=local-editor@thefp.tv TEST_PASSWORD=localpass-123 python -m pytest tests -q --ignore=tests/unit -o addopts=""`

## Schema changes (2026-07-20 onward)
Use **Alembic**: add a revision in `alembic/versions/` (copy 0001_baseline's
shape), never the legacy boot-DDL lists in app.py (those are FROZEN — they
remain only for the columns they already manage). `alembic upgrade head` runs
in Render's preDeployCommand before the app boots.

**⚠ Found live 2026-08-18: production Postgres has NO alembic_version table —
no revision has EVER applied there.** preDeploy's alembic ran against the
sqlite fallback in alembic.ini (DATABASE_URL absent from its env) and
"succeeded". env.py now hard-fails on Render when DATABASE_URL is missing.
Until someone confirms a deploy log showing alembic touching Postgres, do
NOT assume your revision ran in production — schema your feature needs must
also be covered by the per-worker essential-pass (app.py, search
"_web_worker_essential_columns") or verified via /api/v1/health-style probes.

## Monitoring
Sentry (SENTRY_DSN env) — errors are captured in the global 500 handler and
tagged `err_ref` matching the user-facing ERR-XXXX code. Test route:
/admin/sentry-test (super-admin). ntfy was retired 2026-07-20.

## Gotchas that have shipped bugs (details: SESSION_HANDOFF_2026-07-14.md §5)
- callsheet.html renders in THREE modes (internal / recipient / pdf) — every
  change must work in all three; pdf_mode has no JS.
- Call-sheet saves are whole-payload with a `_rev` concurrency token.
- `crew_call_times` / `talent_times` are DICTS keyed `sec||role||name`.
- Never insert a helper `def` between @app.route and its handler.
- Boot DDL must stay idempotent; data migrations need marker + advisory lock.
- Money: `db.Numeric` only. AI features: advisory + fail-open, never
  auto-confirm financial mutations.
- Versions (Cleanup C, 2026-11-02): "current" = the NEWEST of its kind —
  `actuals.get_current_working_budget` / `get_current_estimated_budget` /
  `get_live_budget` are the ONE rule (never copy the query). The live
  Estimated and Working may carry DIFFERENT version numbers. The budget
  page header rule is `working_exists_for_version` ("a Working exists for
  THIS version"), not "any Working at all"; `budget_mode` is only
  `estimated` | `working` (legacy Actual budgets are `is_actual`, archived).
- Docs and Actuals are their own screens (`/projects/<pid>/docs`,
  `/projects/<pid>/actuals`). The budget page has NO in-page Docs/Actuals
  panes any more (Cleanup B, 2026-11-02) — link there, never `?tab=docs` /
  `?tab=actuals`. The document editor MODAL on the budget page stays
  (`#docDetailOverlay`, b03.js) and needs `#actualsLineOptsTpl` + the picker
  glue in b04.js.
- One rule per decision (Cleanup D, 2026-11-02): `counts_as_spend`,
  `canon_vendor` (+ `vendor_match_key` / `payee_name_key`),
  `section_for_code`, `FP.esc` / `FP.money` (static/fp-utils.js),
  `DOCUMENT_TYPES` / `DOC_TYPE_LABELS`, `PAYMENT_METHODS`, `MODE_META` —
  never a private copy. The lists reach templates through
  `app.inject_globals` and JS through `window.__FP_*` in base.html.
  The budget page's float bar (`#budget-float-bar`, 2026-11-04) has ONE
  writer, `FP.budgetFloatBar.update({estimated, working, actual, subtotal,
  fee, basis, view, feeText})` in `static/budget-js/b21.js`; its variance
  is `window.computeVariance` (the Cleanup C rule) — never write its spans.
- Matching and duplicates (Actuals Phase 5, 2026-11-03): matching is
  `actuals.match_candidates` (score_match / rank_candidates / suggest_pairs),
  ONE rule for the chip, the candidate panel, Find backup, Review matches and
  the auto-matcher (`run_auto_match` = `refresh_match_suggestions`); it writes
  `suggested`, never auto-confirms. Duplicates = `is_duplicate_twin` (3-day
  window for two QuickBooks rows, same qbo id is never a twin) + the keeper
  rule (`pick_keeper`: receipt → coded → earlier → lower id) via
  `find_duplicate_groups`; deleting an extra goes through `merge_duplicate_into`
  and `txn_snapshot` so `activity_undo` can recreate it. QuickBooks deletions
  are keyed on (qbo_txn_id, qbo_txn_type): `qbo_sync.apply_qbo_deletions`.
- Company-wide = Admin (Cleanup E, 2026-11-02): the company profile,
  QuickBooks connection, integrations status and every maintenance /
  diagnostic tool live on `/admin` and `/admin/maintenance`
  (`routes/admin_maintenance.py` TOOLS registry), never typed URLs and
  never inside a project. A new admin tool = a route + a TOOLS entry. The
  one-shot repairs that already ran are deleted; do not resurrect them.
- Company ledger (2026-11-03): ONE row per QuickBooks transaction for the
  whole company. `Transaction.project_id` on a QuickBooks row means
  "claimed by"; NULL = in the pool. The ONE rules: `actuals.pool_filter(pid)`
  (every per-project transaction query — a project's own rows + the
  unclaimed pool; rollups by budget_line_id need nothing), `actuals.claim_for`
  / `actuals.release` (every claim signal — line, section, split, receipt,
  not-project — claims; the last signal gone releases; never write
  project_id by hand), `qbo_sync.sync_company` (the ONE sync — per-project
  Sync buttons and the scheduled loop call it; account set + watermarks on
  `QBOConnection`; `ProjectSheet.qbo_account_ids / sync_through / last_synced
  / last_cdc_sync` and `claimed_by_project_id` are retired — kept, unread).
  `_sync_claim_state`, siblings and `uq_transaction_qbo` are gone; the unique
  index is `uq_transaction_qbo_company` (partial on `claim_conflict IS NOT
  TRUE`). The data step (`ledger.run_migration`) NEVER runs at boot: the
  owner previews and runs it from Admin → Maintenance → "Company ledger";
  until then the per-project copies may still exist and the index is not
  created (the essential pass only creates it once no duplicate group
  remains). Conflicts (two projects / payee differs) are flagged
  `claim_conflict` and settled on /admin/ledger/conflicts; every removal,
  demotion or release goes through `transaction_merge_log` + "Undo a
  ledger merge".
- Claim signals are line, section, split, receipt — NEVER "Not project"
  (fix 2026-11-04: counting it made 577 fake conflicts and deleted real
  rows). "Not project" on a QuickBooks row is a per-project
  `transaction_pool_dismissal` (`actuals.mark_not_project` — the ONE write;
  on a row this project held it releases first): the charge leaves only
  that project's Unclaimed, stays in the pool for the rest, Restore deletes
  the dismissal. `pool_filter` leaves dismissed rows out
  (`include_dismissed=True` for the Not project tab / Restore). Never set
  `not_project_expense` on a QuickBooks row; `release` turns a legacy mark
  into a dismissal.
- ONE split mechanism (2026-11-04): a charge is split with
  `transaction_split` rows; the one document attached to it backs every
  piece. A piece targets a line OR a section (`account_code`, Alembic 0027
  + essential pass) — exactly one (`validate_splits`); section pieces count
  in the section like section-only coding (`split_section_sums`,
  `split_sums_by_section_code`). The legacy `invoice_split` children live
  only on a document with NO charge yet (accrual) — and on payroll packages
  (per-person payee rows, `_is_payroll_itemization`). Attaching a document
  (link-doc / confirm_match) folds its children into the charge's split
  (`actuals.fold_invoice_children_onto_charge`) and deletes them — never
  both; the doc modal's itemize on an attached doc writes the charge's split
  (`_itemize_onto_charge`). A split parent is UNCODED (no line / section):
  `link_transaction_to_line` raises `SplitParentError`, set-line refuses with
  `SPLIT_PARENT_MESSAGE`, drill-downs list a parent only as its pieces.
  Invoices counted twice = `actuals.find_counted_twice` (one rule for the
  workspace suggestion, Review matches, the detail window and Maintenance →
  "Find invoices counted twice"). Undo of these moves rides on
  `before['recreate_rows' / 'recreate_evidence' / 'evidence']`
  (`app._undo_restore_rows`).
- The detail window (2026-11-04) is ONE component (`static/txn-detail.js`,
  `FPDetail`) over ONE payload (`routes/actuals_detail.py`
  `…/transaction/<tid>/detail.json`, `…/doc/<did>/detail.json`) for Actuals
  rows and the budget page's drill-down. Every picker (Assign dialog, split
  rows, the window) builds options with `FP.linePickerOptions` /
  `FP.LinePicker` — never a private `<select>`.
- Payee identity = `actuals.same_payee` (2026-11-04) — ONE rule for "these
  two names / QuickBooks vendor ids pay the same payee": equal
  `vendor_match_key` (or `payee_key`, the name without LLC / Inc), the same
  QuickBooks vendor id, or a person and their LOAN-OUT company
  (`payee_loanout`, Alembic 0028 + essential pass; read only through
  `actuals.loanout_index()`, cached per request on flask.g —
  `invalidate_loanout_index` after a write). It feeds the ledger's
  payee-differs flag (`ledger.payees_differ`), the matching engine's vendor
  score (`vendor_score` → "same payee (loan-out)"), the duplicate twin rule,
  document attribution (`payee_for_vendor_string`, `_crew_suggest_for`) and
  QuickBooks EntityRef attribution (`qbo_sync._payee_by_qbo_vendor` maps a
  loan-out's vendor to its PERSON). LLC / Inc / Productions are never a
  "shared word" (`payee_tokens`): You Choose, LLC ≠ Sarah Caye Productions.
  Write loan-outs only with `actuals.add_loanout` / `remove_loanout`
  (they re-attribute charges); a company belongs to one person; QuickBooks
  is never written (the vendor is picked from the `qbo_vendor` mirror).
  Display is `payee_display` / `loanout_via`: "Danielle Miller · via Two
  Foot Shuffle LLC". Payee-differs → "Same person" / "Different" is
  `ledger.resolve_same_person` / `resolve_different` (POST
  /admin/ledger/conflicts/same-person | different); after any loan-out
  write call `ledger.recheck_payee_flags`.
- Compare duplicates (2026-11-04): ONE panel (`static/dup-compare.js`,
  `FPCompare`, the `.fpd` shell) over ONE payload
  (`…/actuals/duplicates/compare.json`, built by `actuals.compare_columns`
  from `_project_dup_groups` = `find_duplicate_groups` over `_ws_pool_rows`,
  the same pool selection as the workspace). Keep this one =
  `…/duplicates/settle` → `_merge_extra_into_keeper` per extra (the ONE
  removal path, shared with merge-duplicate; logged, undo-batch). Keep both
  = the dismissal; `dup:<ids>` keys count in EVERY project
  (`_dup_dismissed_keys`) since a pool row is one row company-wide. The row
  flag chip is hidden under the hover actions, so a duplicate row's hover
  action is Compare; D opens it too.

- Dropbox paths follow the folder (2026-11-04): every project-folder move
  — rename, wrap, archive, bulk archive, reactivate — goes through
  `_move_project_folder(p, dest)`: it moves, records where the folder IS
  (`ProjectSheet.dropbox_root`, Alembic 0029 + essential pass; NULL = at
  the ops root) and rewrites every stored path inside it
  (`_remap_project_dropbox_paths(pid, old_root, new_root)` — DocUpload
  filed / archive paths, trash_meta move sources, call-sheet archives;
  counts land on the activity row). Read a project's folder ONLY through
  `_project_root_for(project)` (never `/<slug>` by hand);
  `_project_dbx_root(slug)` is the ops-root form. A file that is still not
  where its row says → Admin → Maintenance → "Re-link documents to
  Dropbox" (`routes/docs_relink.py`, Preview then Run; updates rows only,
  never moves files; undoable per document and per project). When a
  project's folder is gone it looks under `_WRAPPED PROJECTS` /
  `_ARCHIVED` for `<date>_<slug>` (`_find_moved_project_folder`, shared
  with reactivate): one match → `method: "wrapped"` rows + a
  `project_changes` entry that records `dropbox_root`; several → a person.
  preview-link / raw answer a path error with `DOC_MISSING_MESSAGE` +
  `missing: true` (`_dbx_is_path_error`). `_INBOX` is created ONLY by
  `_create_drop_folder` (fresh slug from the DB, project root must exist)
  — a sweep holding a pre-rename row used to re-create the old folder.

## Splitting app.py (M1, ongoing)
Route slices move to `routes/<name>.py` — plain modules using `@app.route` on
the SAME app object (endpoint names/URLs unchanged; NO blueprints — url_for
must keep working). Import the module at app.py's BOTTOM. Verify every slice:
`python scripts/route_snapshot.py save b.json` (before) → cut → `diff b.json`
must print IDENTICAL. First slice: routes/budget_templates.py.

Status ledger: AUDIT_CHECKLIST.md. Deploy = push `main`; verify /readyz sha.
