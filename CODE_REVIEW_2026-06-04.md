# FPBudget Full Code Review — 2026-06-04

## REMEDIATION STATUS (updated as fixes land — each is its own revertable commit)

| Finding | Status | Commit |
|---|---|---|
| CR-1 COA remap corrupting coding on every boot | ✅ FIXED (removed; tombstone comment) | 6315e7e |
| CR-2 IDOR — missing project auth on ~70 routes | ✅ FIXED (central before_request gate + project_share owner-gate) | f96307c |
| CR-3 Refunds inflate/vanish from actuals | ✅ FIXED (signed rollups; QBO Credit flag; BillPayment; CreditCardCredit) | c67714c |
| CR-4 Filename dates parsed as dollar amounts | ✅ FIXED (hardened parser, consolidated 3 copies) | 0cbdbb6 |
| CR-5 XSS (Representation modal, attach-receipt vendor) | ✅ FIXED | 692e1d7 |
| Frontend regressions (section filter, dead queries, Cmd+A, drag-drop revert, review counts, bulk bar) | ✅ FIXED | 692e1d7 |
| Archive: /admin/backup/transactions.json (super_admin, read-only) | ✅ ADDED | 6315e7e |
| HIGH confirm_match data loss + purge deletes matched + cross-project coding | ✅ FIXED | ea09be7 |
| HIGH CSV sign polarity (Amex) + dedup drops identical same-day charges | ✅ FIXED | af04d80 |
| OLD REFUNDS repair tool (/admin/qbo-imports/repair-refunds, dry-run) | ✅ ADDED | caf0c27 |
| HIGH filename-collision-as-duplicate + error-upload archive pointer | ✅ FIXED | 4a58564 |
| HIGH _walk_dbx_files truncation guard; source_path scoping; prod secret fail-fast; /readyz | ✅ FIXED | 4929811 |
| HIGH concurrent Actual-clone duplicate (unique constraints) | ⏳ pending | — |
| HIGH manual-merge children/qbo_txn_id; QBO pagination | ⏳ pending | — |
| HIGH CSRF protection | ⏳ pending (most invasive) | — |
| Reliability: per-worker DDL on every boot; drain single-flight race | ⏳ pending | — |
| Pipeline: retry-filing dead; gating-list divergence; releases invisible | ⏳ pending | — |
| MEDIUM/LOW items | ⏳ pending | — |

Rollback: `git revert <commit>` for any single fix; DB state via Render PITR
and the /admin/backup/transactions.json snapshots (one pulled 2026-06-10
16:58Z: 1,866 txns, 415 coded).


Six parallel review passes (security, financial correctness, reliability/infra,
frontend, document pipeline/integrations, architecture) over ~62k lines.
Findings verified against actual code; the two most severe were independently
re-verified by hand. Ordered by priority within each section.

---

## CRITICAL — fix immediately

### CR-1. COA renumber UPDATE corrupts transaction coding on every worker boot
`app.py:20279-20337` (inside `_web_worker_essential_columns`, runs on EVERY
worker boot AND every `--max-requests 5000` recycle — i.e., many times per day).
The CASE remap's legacy source codes (1000, 2000, 3000, 3100, 3200, 3300, 4000,
4500, 5000, 6000) are ALSO valid CURRENT codes. A transaction coded today to
2000 (Production Staff) is silently rewritten to 2600 (Camera Equipment) on the
next worker recycle; 3300 (Locations) → 2800, 6000 (Insurance) → 3400; legacy
rows chain-migrate across boots (1000→2000→2600). The comment claims idempotency
("rows with old codes vanish") — false, because new coding re-creates "old"
codes. The properly-guarded one-time version exists at app.py:19166-19270
(CoaMigrationLog). **Fix: delete this statement from the per-worker pass** (or
guard with CoaMigrationLog). Then audit/repair affected rows via coa_change_log
& activity log where possible.

### CR-2. Missing project-level authorization on ~70+ AJAX routes (IDOR)
`app.py` — e.g. `upsert_line` (6251), `delete_line` (7190), `delete_budget`
(4392), `actuals_set_line` (7473), `budget_lines_json` (12310), `export_csv`
(9318), plus most `/projects/<pid>/budget/<bid>/…`, `/projects/<pid>/actuals/…`,
gantt, travel, catering, locations, tax-credit, callsheet AJAX routes.
They use only `@login_required` — no `ProjectAccess` check. Any authenticated
user (even with zero project access) can read/edit/delete ANY project's
financial data by changing ids in the URL. The page route `budget_view` checks
access; the AJAX routes it calls do not.
**Fix: central decorator** that resolves pid (directly or via the child row) and
enforces access; apply to every `/projects/<pid>/...` route. Mutations require
editor+.

### CR-3. QBO refunds imported as positive expenses; credits excluded from all rollups
- `qbo_sync.py:207-208`: CC refunds (Purchase with `Credit: true`) imported as
  positive expenses — a $300 refund ADDS $300 to actuals. The `Credit` flag is
  never read. CreditCardCredit entity is never queried despite comments claiming
  it (qbo_sync.py:301-306 vs 306 entity tuple).
- `app.py:4449-4459` (+5118, 5172, 5198, 8603): every rollup filters
  `is_expense == True` — CSV-imported refunds (stored correctly as
  is_expense=False) are never subtracted. Net spend is always overstated.
- Also: BillPayments synced as is_expense=False → vendor-bill spend invisible.
**Fix: rollups use signed sums (expense − credit); honor Credit flag on
Purchase; BillPayment → is_expense=True; add CreditCardCredit to sync.**

### CR-4. Filename "amounts" override OCR totals — dates become dollar amounts
`app.py:1791-1793` (+1964-1966 drain copy): `amount = max(_fn_amts)/100.0`
overrides the OCR total whenever the filename matches `\d+\.\d{2}`.
`Receipt 2026.05.14 Hilton.pdf` → $2,026.05 written to the ledger. Same regex
feeds the skip-if-amount-in-system filter (1711-1715), so dot-date tokens can
also silently EXCLUDE genuinely-missing receipts from import. Negative amounts
sign-stripped; `25.8` (one decimal) missed.
**Fix: reject tokens in date context (preceded/followed by `.\d`), capture
sign, only override OCR when OCR is absent — and only with a strict currency
pattern.**

### CR-5. Stored XSS in Representation modal
`templates/budget.html:16643-16662` (`loadSupportContacts`): renders contact
name/company/phone/email into innerHTML with NO escaping (every other list uses
`_esc`). A crafted contact name executes JS for anyone opening the modal.
Also: vendor re-injected unescaped at budget.html:8869 (attach-receipt modal).
**Fix: wrap in `_esc`; replace the JSON.stringify-in-onclick hack with data-id
lookup.**

---

## HIGH

### Security
- **No CSRF protection** on any state-changing POST (no CSRFProtect anywhere;
  SameSite=Lax only partial cover). Fix: Flask-WTF CSRFProtect + X-CSRFToken
  header on fetch() calls.
- **project_share privilege escalation** (app.py:18870-18904): any viewer can
  grant collaborator access to any account. Gate behind owner/admin.
- **source_path not scoped** (app.py:1422 _resolve_dbx_path + its 3 callers):
  a project editor can point source-audit/import at ANY other project's Dropbox
  folder. Assert resolved path is under the project's own root.
- **SECRET_KEY / ADMIN_PASSWORD weak fallbacks** (app.py:538, 624): fail fast in
  prod if unset instead of defaulting to known values.

### Financial correctness
- **Concurrent Actual-budget cloning can duplicate budgets/lines**
  (actuals.py:136-175, 491-556): no unique constraint on (budget_id,
  source_line_id), no "one current Actual per project" constraint; 2 workers.
  Add unique partial indexes; catch IntegrityError and re-query.
- **confirm_match destroys sister-row data irrecoverably; unmatch restore is
  lossy** (actuals.py:793-804, 856-867): if both rows were coded, the sister's
  coding/note/card4 silently lost; confirm→unmatch doesn't round-trip. Snapshot
  the sister before delete.
- **confirm_match leaves source='qbo_sync'** → `/admin/qbo-imports/purge`
  DELETES confirmed-matched ledger rows (receipt's txn was merged away; nothing
  recreated). Fix: confirm sets source='reconciled' (like the other two match
  paths) or purge skips rows with doc_upload_id.
- **CSV sign convention assumes negative=charge** (app.py:22768-22771): Amex
  (positive charges) imports a whole month as credits → spend vanishes (per
  CR-3 rollups). Add heuristic/toggle.
- **CSV dedup fingerprint drops legit identical same-day charges**
  (app.py:22836-22839): two identical coffees → one imported. Count occurrences
  per fingerprint.
- **link_transaction_to_line doesn't validate line.project == txn.project**
  (actuals.py:394-414): cross-project coding possible from stale UI/crafted
  request.
- **Manual merge** (app.py:7838-7846): doesn't detach invoice_split children
  (FK error on PG mid-flow) and drops the loser's qbo_txn_id → purged/duplicate
  re-import on next sync.

### Reliability
- **Drain single-flight race** (app.py:2040-2097): heartbeat check passes
  during the initial minutes-long Dropbox walk → second drain can start and
  double-import. No cancel mechanism. Use pg advisory lock / system_task_log
  claim (pattern already exists for trash purge); add stop route.
- **Killed drain strands receipts forever** (app.py:1898 + fp_analyzer staging):
  worker recycle kills the daemon thread after staging to _SOURCE_ARCHIVE but
  before DB rows; next drain's proc_hashes walk INCLUDES the archive copy →
  file matched as "in software", never imported. Exclude _source_archive from
  the proc_hashes walk + drain-state row in DB.
- **_walk_dbx_files swallows all errors** (app.py:1446-1465): truncated walks
  silently shrink dedup sets → mass re-import, or wrong audit numbers. Propagate
  a truncated flag; refuse to import with a partial exclusion set.
- **~150 DDL statements on EVERY worker boot/recycle** (app.py:19849-20364):
  ACCESS EXCLUSIVE locks on hot tables behind live traffic → periodic site-wide
  stalls; plus unconditional full-table UPDATE backfills (incl. CR-1). Run once
  per deploy via system_task_log claim; per-worker pass becomes a cheap check.
- **Long synchronous Dropbox/OCR routes** (scan-audit, import-missing,
  source-audit/import, reconcile, sync-now, auto-match): exceed the ~100s proxy
  timeout; gthread workers never abort them; user retries overlap partial
  state. Move to the drain/claim pattern.
- **estimate-status route is fine, but /health is static** — no DB check, Render
  keeps routing to a broken worker.

### Document pipeline
- **Error-status uploads lose their _SOURCE_ARCHIVE pointer**
  (fp_analyzer.py:1291-1307 → app.py:20676): staged bytes exist but no DB row
  points at them; invisible to every recovery tool. Return staged_path in error
  results.
- **retry-filing is dead** (app.py:20863-20909): fetches from removed R2; sets
  out-of-vocabulary status 'filed'. Rewrite to copy from source_archive_path.
- **Filename-collision treated as duplicate** (fp_analyzer.py:1095 →
  app.py:20684): two distinct same-vendor/date/total receipts → second flagged
  is_duplicate (no peer id), no Transaction, excluded from matching = silent
  missing spend. Only hash matches should set is_duplicate.
- **QBO sync: no pagination (>1000 dropped), watermark advances over failed
  entity queries** (qbo_sync.py:306-322, 758-765): permanent gaps after
  lookback ages out. Paginate; don't advance watermark on partial failure.

### Frontend
- **Section filter hides coded rows with unpopulated lazy pickers**
  (budget.html:7401-7416): filter needs OPTGROUP parent; seeded single options
  have none → coded rows wrongly hidden when filtering by section. Put
  data-section on the row server-side.
- **#actuals-txn-list-scoped queries dead after sectionize** (7170, 5045):
  suggestion-chip refresh no-ops; doc-modal prev/next permanently disabled in
  Actuals. Scope to document or #actuals-sections.
- **Cmd/Ctrl+A guard checks style.display, tabs toggle via class** (8473-8486):
  intercepts select-all on every tab and mutates hidden-tab selection. Check
  classList.contains('active').
- **Stale _docPoAttachDocId hijacks line→PO picker** (11512, 11702-11763):
  open-then-cancel doc attach, later "+ PO" on a line attaches the OLD document
  instead. Clear the flag on open/close.
- **Duplicate showSupportModal** (16121 vs 16620): legacy support-contact flow
  is dead code; fields (notify_callsheet etc.) silently unreachable.

---

## MEDIUM (grouped)

- Dispersed-fee snapshot/PDF drift: per-row fee rounding ≠ total (penny drift);
  flat-fee + zero-base edge omits the whole fee from rows while grand total
  includes it; estimate snapshot only appends fee line when not dispersed
  (budget_calc.py:1348-1372; app.py:9938-9943). Largest-remainder allocation.
- calc_line zeroes negative-rate credit lines (budget_calc.py:787-795) — use
  rate != 0.
- Schedule-driven lines bill rounded AVERAGE headcount × days, not true
  crew-days (budget_calc.py:586-658).
- Codes <1000 silently dropped from Top Sheet/snapshot (budget_calc.py:1228;
  app.py:9948) — add "Unmapped" bucket.
- Doc-type → Transaction gating differs across 6 ingestion paths; import-missing
  even deletes txns other paths legitimately created (app.py:1372/1394-1414 vs
  20715 vs 2334; actuals.py:688). One shared constant + helper.
- Upload duplicate race not closed (app.py:20641-20692): concurrent identical
  uploads both pass; add partial unique index (project_id, file_hash).
- SocketIO with 2 workers and no message queue → split-brain presence/emits
  (app.py:541-552); the /live poll is the de-facto transport.
- docs page loads full veryfi_data for every row (app.py:20475) — defer().
- preDeploy migration failures swallowed silently with 5s timeout
  (app.py:19105) — log loudly, distinguish duplicate-column from real failure.
- QBO token refresh race (two workers) can persist a stale rotated token
  (qbo_sync.py:97-132) — advisory lock.
- _parse_analyzer_filename: bare-int amounts, negatives, unmapped CAPS tokens,
  missing COI/PAYROLL/DOC mappings (app.py:1137-1166, 1129-1134).
- Releases (02_PRE-PRODUCTION/TALENT_RELEASES) invisible to scan/import/
  reconcile (all walk only 01_ADMIN/PROCESSED DOCUMENTS).
- needs_review docs: txn created at upload but file stays unfiled in archive;
  no queue/aging surface for stranded reviews.
- Frontend mediums: _selectedTids retains removed rows (9437, 5552); exit-review
  reveals empty bulk bar (7608); dismiss doesn't refresh review counts
  (9450-9472); drag-drop coding misses dataset.current (9163-9187 — picker
  visually reverts on focus); O(n²) banner lookup in filter hot path (7426);
  PDF footer quote-escape no-op (budget_pdf.html:11); PDF detail variance column
  label vs computed basis mismatch (budget_pdf.html:528 vs 712).
- Merge UI reads cells via td indexes that don't exist in the div layout
  (rowInfo in actualsBulkMerge) — cosmetic confirm text.

## LOW (selected)

- Float money math + banker's rounding throughout budget_calc; displayed
  columns can disagree with displayed totals by a cent.
- DocUpload.amount Numeric(10,2) vs Transaction.amount Numeric(12,2) mismatch;
  no app-level bounds.
- txn_date is String(10) unvalidated; malformed dates silently excluded from
  matching.
- WC/payroll-fee/production-insurance enter the company-fee base by default.
- Exception text leaked in some error responses; no rate limiting on public
  token routes (token entropy is strong).
- qbo sync activity log always "+0 new" (key mismatch app.py:9073 vs
  qbo_sync.py:793).
- external_export: nondeterministic code-only fallback; lexicographic section
  sort ("1000" < "200" < "999").
- estimate_share healing DDL matches model except index name drift (harmless).
- /health static; gunicorn_workers.py dead; allure-results/.pytest_cache/
  budget.db committed.

---

## ARCHITECTURE (incremental, no rewrites)

1. **Blueprint extractions from app.py, in order:** actuals routes
   (7473-9320, ~25 routes — hottest churn, cleanest seam), docs-admin tooling
   (826-2390), /admin/* (~17110-18870), gantt (10978-13450), callsheets.
   Verbatim moves; JS mostly uses hardcoded fetch paths so url_for churn is
   minimal.
2. **budget.html script extractions:** Travel+Catering block (18781-20072, ZERO
   Jinja tags — free win), Actuals IIFE (6764-9574, 1 tag), Docs panel
   (3241-5684, 4 constant tags). Pattern already proven (collab.js,
   import-csv.js): hoist constants into one `window.FPCTX` inline block.
3. **Unit tests today (no infra needed):** calc_line (recently bitten),
   _vendor_similarity, _parse_analyzer_filename, _resolve_dbx_path,
   _amounts_in_name, section_for_code, payroll/OT calc. The Playwright E2E
   suite exists but points at PRODUCTION by default (conftest.py:30) — also
   worth changing.
4. **Deduplicate:** _section_for_code ×4 (divergent fallbacks!),
   _amounts_in_name ×3, two Dropbox client builders (app.py vs fp_analyzer),
   ledger-category gating ×8, section-totals builders ×4. Canonical homes:
   budget_calc (sections), fp_analyzer (dbx client), models/actuals (category
   constants).
5. **Migrations:** keep the pattern, tighten: ADD COLUMN IF NOT EXISTS
   everywhere; ALL DDL behind RUN_BOOT_TASKS (preDeploy only); per-worker does
   ZERO DDL; generalize CoaMigrationLog into a schema_migration(key) guard.
   Alembic not worth it at this team size.
6. **Ops hygiene:** pin requirements (pip freeze → requirements.lock — two
   incidents already from floating deps); add Sentry (3 lines) or ntfy
   exception hook; scheduled pg_dump to R2 (~30-line cron; client exists);
   config.py that fails fast on missing prod env.
7. **Delete stale one-shots:** /admin/migrate/split-labor*, resync-all,
   /debug-mirror, /rebuild-actual-mirror (most dangerous leftover),
   ?w2e=1 branch, cleanup-estimate-txns, cross-project-claim-backfill,
   gunicorn_workers.py.

---

## DONE WELL (consensus across reviewers)

- Decision archaeology: dated, root-caused comments on nearly every weird
  construct — the single highest-value habit in the codebase.
- Source-archive-first upload design; never auto-deleted; server-side copies.
- Layered dedup (SHA-256 + content_hash + partial unique index + watermark +
  CDC sweep); QBO watermark design is genuinely careful.
- Strong token entropy + frozen snapshots on the public estimate portal; no
  SQL injection anywhere (bound params throughout); escaping discipline in the
  newer Actuals JS.
- The guarded one-time COA migration is textbook — and the template for fixing
  CR-1.
- render.yaml operational maturity (preDeploy separation, max-requests
  recycling with jitter, documented thread archaeology).
