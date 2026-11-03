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

## Splitting app.py (M1, ongoing)
Route slices move to `routes/<name>.py` — plain modules using `@app.route` on
the SAME app object (endpoint names/URLs unchanged; NO blueprints — url_for
must keep working). Import the module at app.py's BOTTOM. Verify every slice:
`python scripts/route_snapshot.py save b.json` (before) → cut → `diff b.json`
must print IDENTICAL. First slice: routes/budget_templates.py.

Status ledger: AUDIT_CHECKLIST.md. Deploy = push `main`; verify /readyz sha.
