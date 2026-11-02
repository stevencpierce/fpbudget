# FPBudget — orientation for AI/dev sessions

Flask + SQLAlchemy + Postgres on Render (auto-deploys `main`). ~36k-line
`app.py`; `templates/budget.html` and `callsheet.html` carry heavy inline JS.
Owner: Steven Pierce (steven@thefp.tv), non-programmer — live production system.

## Verify before every deploy (CI runs these too — .github/workflows/ci.yml)
1. `python -m py_compile app.py models.py budget_calc.py actuals.py`
2. Jinja-parse changed templates
3. `python scripts/route_audit.py app.py routes/*.py`   (route-stealing + duplicate-URL guard)
4. `python scripts/js_check.py`             (inline-JS syntax)
5. `python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""`

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

## Splitting app.py (M1, ongoing)
Route slices move to `routes/<name>.py` — plain modules using `@app.route` on
the SAME app object (endpoint names/URLs unchanged; NO blueprints — url_for
must keep working). Import the module at app.py's BOTTOM. Verify every slice:
`python scripts/route_snapshot.py save b.json` (before) → cut → `diff b.json`
must print IDENTICAL. First slice: routes/budget_templates.py.

Status ledger: AUDIT_CHECKLIST.md. Deploy = push `main`; verify /readyz sha.
