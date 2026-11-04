# FPBudget Actuals Build Handoff

Oct 30, 2026 · @Steve

Most of the Concur-style flow is already in the codebase: QuickBooks ingest, a company-wide transaction table with a cross-project claim, line assignment, not-project, receipt backup and create-expense-from-receipt all exist. The job is to finish and consolidate that work into one clean Actuals workspace, fix the handful of rules that are wrong, and redesign the screen. This handoff is written for Claude Code, with a separate brief for Claude Design.

## What exists today

The app is Flask plus Jinja templates and vanilla JS (no React; the only React is the Expo mobile app under `mobile/`), Postgres on Render, one squashed commit. `app.py` is about 42,800 lines; all 56 actuals routes live there, none in `routes/` yet. `actuals.py` (1,990 lines) is pure helpers with no routes.

| Capability asked for | Status | Where it lives |
| --- | --- | --- |
| Pull QuickBooks transactions | Built | `qbo_sync.py` `sync_project()`; `POST /projects/<pid>/actuals/sync-now`; auto-sync loop every 12 h (`_qbo_auto_sync_loop`, app.py \~34979) |
| One pool, unclaimed list per project | Built, partly | `Transaction` table (models.py line 113) with `claimed_by_project_id`; `_sync_claim_state()` (app.py 11109) locks a charge once coded; the register page `/projects/<pid>/register` (`templates/reconcile.html`) lists charges |
| Assign to a budget line | Built | `actuals.link_transaction_to_line()` (actuals.py 837) codes straight to the current Working line; routes `set-line`, `set-coa`, `set-line-bulk` |
| Drag and drop onto lines | Built, but only on the legacy tab | Native HTML5 DnD in `static/budget-js/b04.js` \~3260-3790 (`_coaApplyDrop`) and `templates/budget.html` \~8585-8720. The new register page has none; it uses a `<select>` per row |
| Not a project expense | Half built | `mark-not-project` (app.py 11589) sets the flag. There is no unmark route and no recoverable list; the register shows only a count, and the button exists only on receipts |
| Receipt as backup | Built | `DocUpload` + `ExpenseEvidence` (models.py 1171, 1501); `upload-receipt`, `link-doc`, `mark-backup`; Veryfi OCR and Dropbox filing via `fp_analyzer.analyze_and_file_single` |
| Create an expense from a receipt (cash, personal card) | Built | `POST /docs/upload/<uid>/log-expense` (app.py 24380) requires `reason_kind` cash / check / other\_card / other plus detail, writes `no_charge_reason` |
| Hand-typed manual expense with no receipt | Missing | `source='manual_entry'` rows exist as legacy data; nothing creates them now |
| Super-admin view across projects | Missing | Roles exist (`User.role` = super\_admin, admin, line\_producer, dept\_head, docs\_only; `super_admin_required` at app.py 3558) but no cross-project transaction page |
| Wrapbook payroll parsing by person and fringe | Built, needs cleanup | `docs_parse_people` (app.py 37505) and `ai_layer.extract_people` (line 519); fringes have no line to land on, see Payroll packages and fringes |

**Rules that are wrong today**

- `link_transaction_to_line()` never clears `not_project_expense`, so a charge marked not-project and then given a line stays excluded from every total. `set-coa` does clear it (app.py 14198); `set-line` does not.
- Two handlers share `GET /projects/<pid>/actuals/reconcile.json` (app.py 14537 and 15654). Flask serves the first; the second is dead, and `b04.js:2461` expects the dead one's shape.
- `actuals_sync_now` logs `+0 new` every time because it reads result keys (`added`, `inserted`) that `sync_project()` does not return.
- Three different "counts as spend" filters: `_actuals_by_section_code` (app.py 6936), the per-line rollup in `budget_view` (\~8770-8860), and `line-totals.json` (12023) disagree on `_exclude_estimate_linked_txns_clause`, un-activated `doc_upload` rows and `backup_of_txn_id` rows. Credits count as zero, not negative (`_signed_amount`, a June 2026 reversal).
- `tests/test_actuals.py` targets the hidden legacy `#tab-actuals` and is probably failing. No tests cover `qbo_sync.py`, `actuals.py`, or the register.
- `HANDOFF.md` and `FEATURES.md` (both dated 2026-05-11) still describe the cloned Actual budget; the code moved to direct Working-line coding ("Phase 1").

Useful prior planning in the repo: `CONSOLIDATION_PLAN_2026-06.md` (the Actuals workspace blueprint) and the Actualizing 2.0 items A1-A4 in `AUDIT_CHECKLIST.md` (documents are evidence, not expenses). Item M1b-2 there already calls for slicing actuals out of `app.py`.

## What we borrow from Concur and QuickBooks

Concur's core idea is the one FPBudget is missing: card charges arrive on their own, sit in a pool called Available Expenses, and a person pulls each one onto a report and attaches its backup afterward. Everything else in Concur (travel requests, approval chains, per-diem rules) is enterprise weight we skip.

| Concur concept | What it does there | FPBudget equivalent |
| --- | --- | --- |
| Available Expenses | Card feeds post automatically and wait, unassigned, until a user adds them to a report ([CSUSM guide](https://www.csusm.edu/travel/documents/concur_expense_guide_2021.pdf)) | The Unclaimed list every project sees; the pool is company-wide, each project filters it |
| Add to report | Select transactions, click Add Expense | Drag a transaction onto a budget line (keep the existing drag-and-drop) |
| Allocate | Split one charge across cost centers by percent or amount; splits must total the charge | Split one transaction across lines or projects; splits must sum to the amount |
| Itemize | Break one receipt (a hotel folio) into several expense types | Same thing, same rule: itemize before you allocate |
| Attach Receipt / Available Receipts | Receipts upload separately, then get attached to a charge; emailed and photographed receipts wait in their own pool | The Receipts pass: an unmatched-receipts inbox, match to a transaction, or turn into a manual expense |
| Missing Receipt Declaration | A signed note stands in for a receipt the user cannot produce | A Missing backup reason field; the line stays flagged until backup or a reason exists |
| Red and yellow flags | Red exceptions block submission; yellow warnings do not | Red: unassigned, no backup, split does not sum. Yellow: receipt amount differs from charge, backup date far from charge date |
| Smart matching | Card charge, e-receipt and booking merge into one entry ([Concur](https://www.concur.com/blog/article/how-concur-expense-works)) | Suggested matches by amount and date, confirmed by a click, never silent |

From QuickBooks we take two things: its transaction record is the source of truth for amount, date, vendor and which card or account paid, and its Attachments model (one file linked to one transaction) is the shape receipts should take.

## The model

One company-wide pool of transactions, claimed by projects. A transaction is a fact (it happened, QuickBooks says so); an assignment is a decision (which project, which line); a receipt is evidence. Keep the three apart and every rule below falls out.

- A transaction belongs to the company, not to a project. Every project reads the same pool and filters out what other projects have claimed.
- Claiming is exclusive. Once Project A assigns a transaction, Project B never sees it. The super-admin view is the only place that shows claimed transactions across projects, with the claiming project named.
- Not a project expense is a claim too, by a reserved bucket rather than a project. It leaves every project's Unclaimed list and sits in a recoverable Not project expenses tab. Recovery is one click and puts it back in the pool.
- A split is several assignments on one transaction that sum to its amount (Concur's Allocate). A partial split leaves the remainder unclaimed and visible.
- Budget-vs-actual on a line is the sum of assignments to that line, never the sum of transactions, so a split counts once per line.
- Backup is a separate pass. A transaction can be assigned with no receipt; it just carries a red flag until a receipt is matched or a missing-backup reason is entered. Receipts can arrive before their transaction and wait in an inbox.
- Manual expenses (cash, a personal card, a crew member's reimbursement) are transactions whose source is manual instead of QuickBooks. They flow through the same assignment and backup rules and carry a payment method so they can be reconciled later.
- Nothing is deleted. Unassign, exclude and recover are all reversible moves with an audit trail of who and when.

## Transaction lifecycle

[Transaction state diagram lives in the Claude doc: https://claude.ai/artifact/1yTJ6V736MNYN84oihT1iL — states: Imported → Unclaimed ↔ Assigned to a line → Backed up; Unclaimed ↔ Not a project expense (recoverable); Assigned by another project = Claimed elsewhere, visible to super-admin only]

Assign and mark-not-project are the two moves a line producer makes; unassign and recover undo them. The receipt pass runs beside this, not inside it: a transaction can be assigned without backup and backed up without being assigned. In code these states map to `budget_line_id` (assigned), `not_project_expense` (excluded), `claimed_by_project_id` (claimed elsewhere), and the presence of an `ExpenseEvidence` row or a `no_charge_reason` (backed up).

## Data model

No new tables for the core flow; the `Transaction`, `DocUpload` and `ExpenseEvidence` tables already carry it. The changes are a few columns, one new table for splits, and one shared predicate.

| Change | Why | Where |
| --- | --- | --- |
| `Transaction.excluded_at`, `excluded_by_user_id`, `excluded_reason` (nullable) | Makes not-project an auditable, recoverable state instead of a bare boolean; `not_project_expense` stays as the fast filter | Alembic `0020_not_project_recovery.py`, down\_revision `0019_no_charge_reason`; mirror in `_web_worker_essential_columns()` (app.py 32272) because production has never run Alembic |
| `Transaction.payment_method` (`company_card`, `cash`, `personal_card`, `check`, `ach`, `other`) and `paid_by_crew_member_id` | A manual expense must say how it was paid so it can be reimbursed and reconciled; today only `no_charge_reason` text holds this | Same migration |
| New table `transaction_split` (`id`, `transaction_id`, `budget_line_id`, `project_id`, `amount` Numeric(12,2), `note`, audit columns) with a check that splits for one transaction sum to its amount | Concur's Allocate: one charge across lines or projects. Replaces the invoice-split hack of child Transaction rows for the simple case; `parent_transaction_id` itemization stays for receipt-level breakdowns | Same migration |
| `actuals.counts_as_spend()` returning one SQLAlchemy clause | The three rollups disagree today; one predicate (`not_project_expense == False`, `activated_at` set or source not `doc_upload`, `backup_of_txn_id` is null, not a split parent) feeds all of them | `actuals.py`, used by `_actuals_by_section_code`, `budget_view`, `line-totals.json` |
| Partial unique index `uq_transaction_qbo` promoted into the migration | It lives only in boot DDL today | Same migration, `IF NOT EXISTS` |

Keep money as `db.Numeric`. Keep `claimed_by_project_id` as the cross-project lock and keep calling `_sync_claim_state()` after every coding change, including recover. Splits are within one project only for now: every `transaction_split` row carries the parent's `project_id`, and the UI offers lines from this project alone. Credits and refunds count as negative amounts, as in QuickBooks: `_signed_amount` becomes `CASE WHEN is_expense THEN amount ELSE -amount`, and a refund assigned to a line reduces that line's actual.

## Company ledger (2026-11-03)

Supersedes the per-project copies described below. ONE row per QuickBooks
transaction for the whole company; `project_id` on a QuickBooks row is
"claimed by" (NULL = the pool every project's Actuals shows). Rules and
helpers: `actuals.pool_filter`, `actuals.claim_for` / `release`,
`qbo_sync.sync_company`, `ledger.py` (migration preview / run, review
actions, merge log). The migration is started by hand from Admin →
Maintenance → "Company ledger — preview / run" after reading the preview;
it never runs at deploy. See CLAUDE.md "Company ledger" for the full rule
set. Owner-decided live resolutions (Lyft 28032 → GrubHub, 28026 → GrubHub
with GINTS's SiegeWorks receipt staying coded as its own row, 28088 → GINTS,
the two $4,500 bill payments released to unclaimed with their invoices back
to Receipts to place) are applied BY HAND on /admin/ledger/conflicts and the
"Release a charge to unclaimed" tool after the run — never by the migration.

### Claim rule fix (2026-11-04)
The production preview counted "Not project" as a claim. The rule now: claim
signals are line / section / split / receipt only; a legacy not_project mark
on any QuickBooks copy becomes a `transaction_pool_dismissal` for that copy's
project on the one kept pool row; a deleted copy's person-set vendor / note
moves to the keeper when the keeper's is blank or processor noise; a
reconciled row is preferred over a bare qbo_sync row when nobody claims the
charge; payee-differs is flagged only against a real QuickBooks vendor
(EntityRef), never processor text. Preview adds `by_project`,
`dismissals_to_create` and a per-row action + why.

## QuickBooks sync

The sync already does what the request asks, with one limit worth stating plainly: the QuickBooks Online API exposes only transactions that have been accepted into the books. Bank-feed items still sitting in QuickBooks' For Review tab are not reachable by any API, so a charge appears in FPBudget only after someone (or a QuickBooks bank rule) accepts it. Accepting is enough; it does not need to be categorized correctly in QuickBooks, because FPBudget assigns the budget line.

**What is pulled today** (`qbo_sync.py`, `fetch_transactions` line 327, `fetch_transactions_cdc` line 476): Purchase (card charges, checks, cash expenses), Deposit, BillPayment, CreditCardCredit. JournalEntry and Transfer are skipped on purpose. Bill (unpaid accounts payable) is skipped, which is correct: a bill is a commitment, the BillPayment is the spend.

**Cadence**: on demand via `POST /projects/<pid>/actuals/sync-now`, and automatically every 12 hours (`_qbo_auto_sync_loop`). The window never passes yesterday, overlaps 14 days, and runs a change-data-capture sweep of up to 29 days so edits in QuickBooks flow through.

**Dedupe**: `(project_id, qbo_txn_id, qbo_txn_type)`. Since the pool is company-wide, the same QuickBooks transaction is one row per project that syncs the account; `_sync_claim_state()` is what makes the claim exclusive across those rows. This is workable but it is the root of the "shows up in every project until claimed" behavior, and the super-admin view must group by `qbo_txn_id` to show one charge, not N.

**Decision for the build**: keep per-project rows (changing to one global row touches every rollup and is not worth it now), but make the super-admin page and the Unclaimed list both key on `qbo_txn_id` so the person never sees duplicates.

**Fixes**

- `actuals_sync_now` (app.py 16201) must read `imported`, `enriched`, `reconciled`, `cdc_additions` from the result; it logs `+0 new` today.
- `QBO_CLIENT_ID`, `QBO_CLIENT_SECRET`, `QBO_REDIRECT_URI`, `QBO_ENVIRONMENT` are set only in the Render dashboard, not `render.yaml`. Add them to `render.yaml` as `sync: false` entries so a fresh service does not silently lose the connection.
- Tokens sit in `qbo_connection` as plain text. Acceptable for a single-tenant app on a private database; note it in `CLAUDE.md` rather than build encryption now.
- Add `tests/unit/test_qbo_sync.py` with fixture JSON for each entity type, asserting `_extract_txn_fields` and `is_expense` sign handling (a Purchase with `Credit=true` is a refund).

## Screens

One Actuals workspace per project at `/projects/<pid>/actuals`, replacing both the legacy `#tab-actuals` in `budget.html` and the current `/register` page. Two panes: transactions on the left, the budget (sections and lines, with live actual totals) on the right. Drag from left to right. The legacy tab's drop logic (`_coaApplyDrop` in `b04.js`) is the part to keep.

| View | What it lists | Actions |
| --- | --- | --- |
| Unclaimed (default) | Every pool transaction with no line, no split, not excluded, not claimed elsewhere; grouped by `qbo_txn_id`; suggested line shown as a ghost chip | Drag to a line or section; Assign (keyboard); Split; Not project; Attach receipt |
| Assigned | Transactions coded to this project, grouped by section then line, with a red Needs backup flag | Unassign; Reassign by drag; Split; Attach receipt; Add missing-backup reason |
| Needs backup | Assigned with no `ExpenseEvidence` and no `no_charge_reason` | Attach, link an inbox receipt, or give a reason |
| Receipts inbox | `DocUpload` rows with no transaction (the Available Receipts idea) | Link to a selected transaction; Create expense (cash or personal card); Not project |
| Not project expenses | Excluded transactions, newest first, with who and when | Recover (one click, returns to Unclaimed) |
| Sync | Last sync time, window, counts, accounts enabled | Sync now; pick accounts |

**Super-admin view** at `/admin/transactions` (gated by `super_admin_required`): the whole pool across projects and dates, one row per `qbo_txn_id`, with columns for claiming project, line, backup state and source. A toggle hides claimed rows. Search by vendor, amount, last four, date. Clicking a row opens it in the claiming project's workspace. This is the "which project took this?" view.

**Rules the screen must enforce**

- Red flags block nothing but never disappear: unassigned, needs backup, split does not sum. Yellow: receipt amount differs from charge, receipt date more than 7 days from charge date, duplicate suspected.
- Multi-select with shift and command, then one drag or one bulk action; the register's `set-line-bulk` already exists.
- Every action has an undo toast wired to `ActivityLog` (`_log_activity`, app.py 3719).
- Keyboard path for everything the drag does, so a laptop on a folding table on set still works.
- The per-line Reconcile drawer in the budget page (`b17.js`) stays and points at the same endpoints.

## Receipts and backup

Backup is its own pass, after assignment, and the plumbing for it already exists. What changes is the shape on screen and three rules.

**Pipeline today**: upload (`POST /docs/<pid>/upload`, mobile, or the Dropbox `_INBOX` sweep) runs Veryfi OCR, renames the file `YYYY-MM-DD_RECEIPT_Category_Vendor_Amount.pdf`, files it in the project's Dropbox folder (`01_ADMIN/PROCESSED DOCUMENTS/` for receipts, `CONTRACTS & INVOICES/` for invoices), keeps the original in `_SOURCE_ARCHIVE/`, and writes a `DocUpload`. `sync_project()` then tries a receipt-first match (±5 days, ±$1) and auto-confirms only on an exact amount.

**Rules for the build**

1. A receipt links to a transaction through `ExpenseEvidence` (kind `primary`, `backup`, `itemized`), never by copying the amount. Keep `_delete_evidence_for_txns` in every delete path; the table has no cascade.
2. Suggested matches are shown, never applied. Both the ±$1 near-match and the exact match set `match_status='suggested'` and wait for a click; remove the auto-confirm branch in `sync_project()`. Confirming is one click per row or one bulk action (`matches/confirm-bulk` exists), and `unmatch` undoes it.
3. Needs backup is the red state for any assigned transaction with no evidence row and no `no_charge_reason`. The Concur Missing Receipt Declaration becomes a short reason field on the transaction; it clears the red flag and shows as a yellow chip.
4. Create expense from a receipt stays on `POST /docs/upload/<uid>/log-expense`, extended to write the new `payment_method` and optional `paid_by_crew_member_id`. The modal asks three things: how it was paid, who paid (if not the company), and which line. Cash and personal card are the two common answers; both end in the Needs backup = satisfied state because the receipt is the backup.
5. Hand-typed manual expense with no receipt is a new, rare path: `POST /projects/<pid>/actuals/manual-expense` creating `source='manual_entry'` with `payment_method` required and a reason required when the method is cash. It lands in Needs backup until a receipt is linked.
6. A receipt matched to a transaction claimed by another project is a conflict, not a silent reassignment: show it in the inbox with a note naming the project.

**Itemization** (one receipt, several lines): since 2026-11-04 a document attached to a charge splits THE CHARGE (`transaction_split`, see "Detail window + one split mechanism"). `_itemize_apply` (child rows with `source='invoice_split'` and `ExpenseEvidence` kind `itemized`, strict mode refusing a total above the document) remains only for a document with no charge yet and for payroll packages; see the next section.

## Payroll packages and fringes

The Wrapbook parse works and stays. Fringes cannot land beside a person's staffing line because fringe has no row of its own: it is a computed number inside each labor line, and the only real fringe line is one per section, created only when the project's fringe rollup setting is on (off by default).

**How the parse works today**: the doc-detail modal's Parse payroll package button (`budget.html` 3846) posts to `/docs/upload/<uid>/parse-people` (`docs_parse_people`, app.py 37505). `ai_layer.extract_people` (line 519) reads the Veryfi text and returns one row per person with `amount`, `wages` and employer `fringes` (FICA, Medicare, FUTA, SUI, workers' comp, platform fee). Names are matched to labor lines through the crew roster. The route writes nothing; the person reviews the rows in the itemize table and saves, which goes through `_itemize_apply` and `link_transaction_to_line` like any itemized invoice.

**Where the fringe goes today**

- Rollup off (default): one pooled fringe row coded `section:6500` with no line. The register is lines-only, so it lands in Parked on a section, while the estimate for that fringe sits inside each labor line in its own department. Labor looks under budget; 6500 looks over.
- Rollup on: one row per person, `<name> — employer fringes`, coded to that section's `fringe_rollup` line (`_ensure_fringe_rollup_line`, app.py 7327), which draws at the foot of the section.
- The wages / fringe split is used only when `wages + fringes` equals `amount` within $1; otherwise the whole amount goes on the person's line with no warning. The status text in `b03.js` always claims fringes went to 6500.
- Workers' comp, production insurance and the payroll service fee are auto-calc rows on the top sheet, not budget lines, so a parsed workers' comp or platform fee has nowhere to land except a section.

**Decision (Steven, Oct 30): fringes live in 6500 Administrative, one line per department that has labor, with an abbreviated read-only summary at the foot of each labor section.** A department's fringe line appears when that department has labor lines and disappears when it does not. Workers' comp, production insurance and the payroll service fee stay auto-calculated.

| Piece | Change |
| --- | --- |
| Fringe lines | In 6500 Administrative, one `BudgetLine` per labor department: `line_tag='fringe_rollup'`, `account_code=6500`, `fringe_for_section=<labor section code>` (new nullable column), description `Payroll Fringes — <Department>`, estimate = that section's fringe pot from `calc_top_sheet`, `sync_omit=True`. Created and removed automatically by `_ensure_fringe_rollup_line` whenever the department's labor lines change. Rollup becomes the only mode: retire the `ProjectSheet.fringe_rollup` toggle and make `calc_line` always return fringe separately |
| Section footer | Each labor section keeps a one-row read-only summary (`<sec>-FR`) showing estimate, actual and variance for its fringes, reading from the 6500 line. Not a drop target; clicking it jumps to the line in 6500 |
| Auto-calc rows | Workers' Comp, Production Insurance, Payroll Service Fee and Production Company Fee stay computed, not budget lines. Each gets a hidden real line (`line_tag='auto_wc'`, `auto_ins`, `auto_fee`, `auto_pcf`; `sync_omit=True`; estimate written from the calc) so actuals can be coded to it and the row shows an Actual column. Their percentage and flat settings become editable on the budget page and the top sheet as well as Settings; one source of truth in `Budget.*_pct` fields, three places to edit |
| Parser target | `docs_parse_people` sends each person's fringe to the 6500 line for that person's department, workers' comp to the WC line, platform fee to the fee line. Ask `extract_people` to return `workers_comp` and `platform_fee` as their own fields instead of folding them into `fringes` |
| Tie-out | When `wages + fringes` does not tie to `amount`, keep the split, book the difference to the person's line and show a yellow flag with the delta, instead of silently merging |
| Register | Fringe lines list under 6500 with their department name; the labor section no longer lists a fringe row as a target |

The rejected alternative (a fringe child line under every person, `parent_line_id` = the staffing line) keeps department totals whole but adds a line per crew member and makes the register twice as long. Worth revisiting only if a client ever needs fringe reported by department.

## Build plan for Claude Code

Five phases, each shippable on its own, each ending with the CLAUDE.md pre-deploy checks (`py_compile`, Jinja parse, `scripts/route_audit.py`, `scripts/js_check.py`, `pytest tests/unit`) and a `/readyz` sha check after Render deploys. Follow the repo's standing rules: Alembic for schema plus a mirror in `_web_worker_essential_columns()`, `db.Numeric` for money, no blueprints, no helper `def` between `@app.route` and its handler, `_log_activity` on every mutation, AI advisory and fail-open.

**Phase 0 — Fix what is wrong (half a day)**

- [ ] `actuals.link_transaction_to_line()`: set `not_project_expense = False` and clear the new `excluded_*` columns on assign; call `_sync_claim_state()` after.
- [ ] Delete the dead second `actuals_reconcile_json` handler (app.py 15654) or rename its URL; fix `b04.js:2461` to the live shape. Add a duplicate-URL check to `scripts/route_audit.py`.
- [ ] `actuals_sync_now`: log the real result keys.
- [ ] Add `actuals.counts_as_spend()` and use it in `_actuals_by_section_code`, the `budget_view` rollup, and `actuals_line_totals_batch`. Unit-test it in `tests/unit/test_actuals_rules.py` with `SimpleNamespace` stubs.
- [ ] Rewrite the stale docstrings in `actuals.py` that still mention Working → Actual cloning.

* [ ] `_signed_amount`: credits become negative (`-amount` when `is_expense` is false) so a refund nets against its line. Confirm `repair_is_expense` in `qbo_sync.py` still classifies CreditCardCredit and Purchase `Credit=true` as credits.
* [ ] `sync_project()`: remove the exact-amount auto-confirm; every receipt match lands as `suggested`.

**Phase 1 — Not project, recoverable (one day)**

- [ ] Migration `0020_not_project_recovery`: `excluded_at`, `excluded_by_user_id`, `excluded_reason`, `payment_method`, `paid_by_crew_member_id`, the `uq_transaction_qbo` index.
- [ ] `POST /projects/<pid>/actuals/transaction/<tid>/recover`: clears the flag and the `excluded_*` columns, returns the row to Unclaimed, logs with undo.
- [ ] `GET /projects/<pid>/actuals/not-project.json`: excluded rows for this project, newest first.
- [ ] Extend `mark-not-project` to accept an optional reason and to work on charges, not only receipts.

**Phase 2 — Slice actuals out of app.py (one to two days)**

- [ ] Move the actuals routes into `routes/actuals.py` and the admin ones into `routes/actuals_admin.py` using `@app.route` on the same app object, imported at the bottom of `app.py`. Verify with `scripts/route_snapshot.py` before and after; the diff must print IDENTICAL. This closes `AUDIT_CHECKLIST.md` item M1b-2.
- [ ] `GET /projects/<pid>/actuals/workspace.json`: one payload for the new screen, built from `reconcile_workspace_json` (app.py 14647), grouped by `qbo_txn_id`, with the six views as filters and every flag precomputed server-side.
- [ ] `GET /admin/transactions` and `/admin/transactions.json` behind `super_admin_required`.
- [ ] `POST /projects/<pid>/actuals/transaction/<tid>/split` writing `transaction_split` rows; rollups read splits first, then direct coding.
- [ ] `POST /projects/<pid>/actuals/manual-expense`.

**Phase 3 — The new workspace screen (two to three days, after the Claude Design pass)**

- [ ] `templates/actuals.html` plus `static/actuals-js/a01.js` onward, the same split-file pattern as `budget-js`. Port `_coaApplyDrop`, multi-select, recent targets and file-drop from `b04.js`; drop the `<select>` per row.
- [ ] Budget page: the Actuals tab becomes a link to `/projects/<pid>/actuals`; delete the hidden `#tab-actuals` markup and `b04.js` 3260-3790 once the new screen is live. Keep the per-line Reconcile drawer (`b17.js`).
- [ ] Retire `/register` with a redirect.
- [ ] Replace `tests/test_actuals.py` with Playwright tests for: drag to line, not project then recover, split that must sum, needs-backup flag clears on link, super-admin sees a claimed row with the project name.

**Phase 4 — Fringes to Administrative (one to two days)** — built 2026-11-01 (PR "Actuals Phase 4"); the migration shipped as `0023a_admin_fringe_lines` with the data step as the boot task `_migrate_fringes_to_admin`, and the hidden lines carry `line_tag='auto_calc'` + `auto_kind` (`wc` / `ins` / `fee` / `pcf`) rather than four tags.

- [x] Migration `0021_admin_fringe_lines`: `BudgetLine.fringe_for_section` (nullable Int); a data migration (marker plus advisory lock, per CLAUDE.md) that creates one `Payroll Fringes — <Department>` line in 6500 per labor section on every current Working budget, creates the hidden `auto_wc` / `auto_ins` / `auto_fee` / `auto_pcf` lines, and moves existing `section:6500` fringe rows and per-section `fringe_rollup` lines onto the new 6500 lines.
- [x] `_ensure_fringe_rollup_line`: now keyed by `(budget_id, fringe_for_section)`, always `account_code=6500`; add the removal side so a department with no labor lines loses its fringe line (only when the line has no transactions; otherwise keep it and flag it).
- [x] `budget_calc.calc_line`: always return fringe separately; `calc_top_sheet`: each section's fringe pot becomes the estimate of its 6500 line and the labor section's `<sec>-FR` summary row; remove the `fringe_rollup` branches and the `__fringe_rollup__` sentinel.
- [x] Auto-calc settings (workers' comp %, production insurance, payroll fee %, production company fee) editable on the budget page and the top sheet as well as Settings; all three write the same `Budget.*` fields.
- [x] `docs_parse_people`: new targets, the tie-out delta rule, `workers_comp` and `platform_fee` fields from `extract_people`; fix the `b03.js` status text.
- [x] Grid: the `<sec>-FR` footer row becomes a read-only summary that links to the 6500 line; auto-calc rows gain an Actual column fed by their hidden lines.
- [x] Update `FEATURES.md` rows 85-94, `HANDOFF.md`, and `AUDIT_CHECKLIST.md` A1-A4 to match.

**Phase 5 — Matching and duplicates (Nov 3)** — built 2026-11-03 (PR "Actuals Phase 5"). Steven, Nov 3: "On the receipts page it says Link to selected, but there's nowhere to select a charge. It says possible duplicate but there's no way to address it — suggest one we should keep, let me delete or move the extra. These numbers should match almost exactly, dates really similar: matching backup with the QuickBooks transaction should be turnkey. Suggest, let me review, click approve." And Nov 2: Ryan Mauskopf's $4,500 appeared twice from two BillPayments (1300000319 Oct 19, 1300000322 Oct 20); QuickBooks showed one — the other was voided after import, and the sync never removed anything.

- [x] Matching engine, one rule: `actuals.score_match` (amount exact 1.0 · tip range / within $5 0.7 · else no candidate; date same day 1.0 · ±3 days 0.8 "posted later" · ±7 0.5; vendor `vendor_match_key` equal 1.0 · word overlap 0.6 · else 0.2; same card +0.1), `rank_candidates` (≥ 0.55 suggests; top ≥ 0.85 with a 0.20 gap is strong), `suggest_pairs` (greedy one-to-one), `match_candidates(txn | doc)` and `refresh_match_suggestions` over the DB. Pure, unit-tested (`tests/unit/test_matching.py`).
- [x] Runs everywhere, never attaches by itself: after every upload (`_ingest_document_bytes` is the one ingest path — web, mobile, Dropbox sweep, zip), after every sync (Sync now and the auto-sync loop) and on every workspace load; writes `match_status='suggested'` + `match_confidence` + the doc id, recomputed for every unmatched receipt and every charge without backup within 400 days. AI-made suggestions and confirmed links are left alone.
- [x] Suggestions on both sides: the charge chip "Receipt 92% · Approve · No"; the receipt row (Receipts view and the Docs inbox) "Matches charge · Lyft · Sep 28 · $88.44 · 100% · strong" with Confirm / Not a match; the "N matches to review" chip opens the Review-matches dialog (receipt | charge | confidence + reasons; Approve / Not a match / Approve all strong → `matches/confirm-bulk`). Approve = `confirm_match`, which absorbs the receipt's placeholder row.
- [x] Receipts view links for real: selecting a receipt swaps the budget pane for the candidate panel (`doc/<did>/charge-candidates`: ranked top 8 with Link buttons + search over every unmatched charge); Link to selected needs a selected receipt AND a ticked charge (tooltip "Select a receipt and a charge" until then); a receipt row drags onto a panel charge. Find backup (row menu, Needs backup row action) opens the same panel from the charge side (`receipt-candidates`, now on the engine, inbox receipts included).
- [x] Duplicates view (`view=duplicates`, chip beside Not project): `find_duplicate_groups` on `is_duplicate_twin` — same `vendor_match_key` (or card) + |amount| within 3 days for two QuickBooks rows, same day for CSV / manual, DIFFERENT QuickBooks ids (the same id in two projects is a sibling and is left alone for the ledger-collapse PR). Keeper rule `pick_keeper`: receipt confirmed → coded to a line → earlier date → lower id. Keep both (remembered via `TransactionDupDismissal`, key `dup:<ids>`), Delete the extra (hard delete only for qbo_sync / csv_import rows with no coding and no receipt; otherwise `merge_duplicate_into` re-points line / splits / receipt / evidence / reason / payer / note to the keeper, then deletes; both steps logged, undoable — `activity_undo` recreates a deleted row from `txn_snapshot`), Not a project expense. Confirm dialog shows both rows. Yellow Possible duplicate on Unclaimed / Assigned rows opens the group.
- [x] Sync honours QuickBooks deletions: `parse_cdc_payload` reads the `status: "Deleted"` stubs for Purchase / BillPayment / Deposit / CreditCardCredit; `apply_qbo_deletions` (keyed on qbo id) deletes an uncoded row with no receipt (logged, undoable) and otherwise keeps it with `not_project_expense=True`, `no_charge_reason='voided in QuickBooks'`, red "Voided in QuickBooks", listed under Duplicates → Voided (Keep it = recover, Leave excluded). Maintenance tool "Reconcile with QuickBooks" (`routes/actuals_admin.py` + the TOOLS entry): re-fetches by id the records held from the last 120 days for one project or all, dry run / Apply, fail-open.
- [x] Keyboard: `A` assign, `N` not project, `M` approve the selected suggestion (charge or receipt side), `D` open the selected row's duplicate group.
- No schema change: `match_confidence`, `match_source`, `no_charge_reason`, `TransactionDupDismissal` already existed.

**Detail window + one split mechanism (Nov 4)** — built 2026-11-04 (PR "Actuals: charge/receipt detail window, split by invoice"). Steven, Nov 4: "I should be able to open these up and see what they're linked to … chain of custody … remove it, interact with it, change it. What if the amount is wrong?" · "even a split invoice could be used as backup across multiple lines … she has one invoice with both labor and something else" · "split across lines should work exactly the same way as assign … I can't assign it to a main section. I can't search" · "Rare Medium … assigned to the 4700 post-production services section … I can't see it".

- [x] Detail window: ONE component (`static/txn-detail.js`, `FPDetail.open`) over ONE payload (`GET …/transaction/<tid>/detail.json`; `GET …/doc/<did>/detail.json` for a receipt with no row; `routes/actuals_detail.py`). Opened from every Actuals view (row Details button, double-click, Enter; while open, a click on another row swaps it) and from the budget page's Actual drill-down (the vendor). Header: source badge, amount, date, card / account, QuickBooks payee + type + id, FPBudget Payee. Backup: each document with an inline preview (Dropbox temp link → raw route → placeholder), Open / Replace / Detach (`POST …/detach-doc`: the receipt goes back to Receipts to place and its primary evidence goes, so the charge shows Needs backup; a backup doc's evidence is removed) / Find backup (receipt-candidates) / Upload. Coding chain: Charge → line or section → document, each split piece its own row; Open / Change (shared picker) / Unlink. Amount: a bank row is read-only ("the bank's figure — change the receipt or split instead"); a receipt / manual row is editable through `edit` (`amount_edit_allowed`), logged, undoable (undo restores vendor / amount / date / memo now). History: the row's activity newest first with the AI Agent badge and Undo on the latest undoable entry.
- [x] ONE split mechanism. GINTS (300) ACH 7246 $993.35 was split 900 / 93.35 while its invoice (doc 3485, row 4544) was itemized 3 × $300 to the same line and never attached → $1,800 on a $900 line (repaired by hand Nov 4). Now: attaching a document whose row carries `invoice_split` children (link-doc, confirm_match, Review matches, the counted-twice Fix) folds them into the charge's `transaction_split` (`fold_invoice_children_onto_charge`, amounts / lines / sections / notes kept; the pieces must sum to the charge — a tip goes to the remainder piece, else the charge's own coding, else the largest piece, noted) and deletes them; a charge that is already split keeps its split and the children go. The doc modal's itemize on an ATTACHED document writes the charge's split (`_itemize_onto_charge`); an unattached document (accrual) keeps `_itemize_apply`. Exception: payroll packages (per-person payee rows) keep children. The detail window offers "Split this charge by the invoice" (prefilled from Veryfi line items or the itemization) and "Turn into a split" for a charge's legacy children (`POST …/fold-itemization`).
- [x] A split parent is uncoded: `link_transaction_to_line` raises `SplitParentError` (covers set-line, bulk, itemize main line), set-line → 409 "This charge is split — edit the split instead.", confirm_match never promotes the sister's line onto a split parent, `write_splits` clears the parent. Drill-downs (`line/<lid>/transactions.json`, `ledger.json`) list a split parent only as its pieces. Maintenance → "Split charges still coded" (preview / Fix = uncode the parent, logged).
- [x] Invoices counted twice — `actuals.find_counted_twice`: (a) a charge with transaction_split AND invoice_split children; (b) a coded / itemized document row with no charge + a counted charge, same project, same `vendor_match_key`, |amount| within $0.01, within 30 days, no other receipt on the charge. Shown as a yellow "Counted twice" chip on both rows, in Review matches ("Attach"), in the detail window ("Attach this invoice to the charge"), and Maintenance → "Find invoices counted twice" (`/admin/actuals/counted-twice`: preview cards with every touched line's total before / after; Fix per row, confirmed, logged, Undo on the card). Live candidates to read in the preview: GrubHub (309) Gotham Sound $217.75 (charge 4452 / doc row 4499), Lyft $88.44 (4484 / 4492), Delta $668.79 (4443 / 4442) — not fixed by this PR.
- [x] Split picker = Assign picker: `FP.linePickerOptions` + `FP.LinePicker` (search over names / codes / section names, "All <section>", other COA sections by search, "+ New line…" via `budget-line/new`) in the Assign dialog, the split dialog and the detail window. A piece may target a SECTION: `transaction_split.account_code` (Alembic 0027 + essential pass), exactly one of line / section (`validate_splits`); `split_section_sums` / `split_sums_by_section_code` / the section drill-down / budget_view's section-coded row count them.
- [x] Section-only coding in the Actuals budget pane: each section shows "Section-coded $X (n)" under its header (charges coded to the section + section split pieces, counted in the section's Actual), expandable to the charges (click one → the detail window), droppable.
- [x] Resizable panes and columns (`a05.js`): drag the divider (arrow keys too), drag a header column edge; widths per user in localStorage (`fpb.aw.<uid>.*`, try/catch); double-click resets. Header / views / filter bar / table on one gutter (`--aw-gutter`), checked at 1440 / 1024 / 768.
- Not done: PO links in the chain (transactions carry no PO); the budget page's Line Ledger panel rows (b19) still open the old ledger, not the detail window.

**Loan-outs + Compare duplicates in place (Nov 4)** — built 2026-11-04 (PR "Loan-outs ↔ people; compare duplicates in place"). Steven, Nov 4: "Danielle Miller is Two Foot Shuffle LLC … we want the individuals, but then we still need the company attached somehow" · "You Choose, LLC is not Sarah Caye Productions" · "when I go to Actuals → Unclaimed I see 'possible duplicate' … Can I compare them? … compare and settle this right in this view, and then apply that other places".

- [x] `payee_loanout` (Alembic 0028 + essential pass): a person Payee's loan-out companies, each optionally tied to a vendor from the QuickBooks mirror (never created / changed in QuickBooks). ONE identity rule `actuals.same_payee` (equal key, same QuickBooks vendor id, a person and their loan-out, two loan-outs of one person; LLC / Inc / Productions never count as a shared word). Used by the ledger payee-differs flag, `vendor_score` / `score_match`, `is_duplicate_twin`, `payee_for_vendor_string`, crew-suggest and the QuickBooks EntityRef map.
- [x] "Receipt payee differs from QuickBooks" now offers "Same person — Two Foot Shuffle LLC is Danielle Miller's loan-out" (links it, clears this flag and every other one it explains via `ledger.recheck_payee_flags`, attributes the charge to the person) or "Different" (flag kept, offer stops) — in the detail window and on /admin/ledger/conflicts. Live case: BillPayment 1300000304, $4,800, Sep 21 (Global Run Strong 2026) — settle it there after deploy; nothing is changed automatically.
- [x] Display everywhere: Actuals rows (sub-line), detail window Payee, docs inbox Payee chip / suggestion, Payee page ("Paid via", Loan-out companies card with paid-through per year), directory ("via …", a company record shows "loan-out of …"), 1099 CSV column "Paid <year> via loan-out".
- [x] Compare panel (`static/dup-compare.js`): row hover → Compare (the chip itself hides under the hover actions — why it never opened), D, the row menu, the Duplicates group's Compare button. Columns per charge, differences highlighted, suggested keeper + reason, "If kept" effect; Keep this one (`…/duplicates/settle`, click-to-confirm, Undo toast), Keep both (remembered company-wide for `dup:` keys), Not project for one. ←/→, Enter, Esc. Two-up with horizontal scroll at 1024 when there are more than two.
- Not done: a person picker in the "Same person" offer when neither name is a person Payee (it points at the Payee page instead); the legacy `CrewMember.loan_out_vendor_id` / `ProjectCrewMember.loan_out_vendor_id` links are NOT read by `same_payee` (only `payee_loanout` rows are).

**Phase 6 — Later: upload pathway for external users.** Not in this build. One entry point for crew and vendors to drop receipts and invoices from the app or a computer, replacing the three paths above with one. Scope it after Phase 3 ships.

**Acceptance**

- A charge synced into two projects appears once in each Unclaimed list and disappears from Project B the moment Project A assigns it.
- Marking not project removes it everywhere; Recover brings it back with its history intact.
- A split across two lines totals the charge or the save is refused.
- An assigned charge with no receipt shows red; linking a receipt or entering a reason clears it; the top sheet number does not change either way.
- A Wrapbook package parses into wages on each person's line and fringes on 6500 Payroll Fringes, and the labor section variance no longer moves when fringes post.
- `scripts/route_snapshot.py` diff prints IDENTICAL after the slice.

## Claude Design brief

Mock one screen and one modal, then the admin list. The output should be HTML and CSS that drops into Jinja with vanilla JS, so no framework components and no icon fonts; inline SVG icons only. Match the existing budget page's type and spacing so the two feel like one product.

**Screen 1 — Project Actuals workspace** (desktop, 1440 wide; also check 1280)

- Two panes. Left: the transaction list with the six views as a segmented control (Unclaimed, Assigned, Needs backup, Receipts, Not project, Sync) and a count on each. Right: the budget as sections and lines with estimate, actual, variance per line, and a drop zone state on hover.
- A transaction row shows date, vendor, amount, card last four, source chip (QBO, CSV, manual, receipt), suggested line as a quiet ghost chip, and the flag chip (red or yellow). Rows are dense: 36 px, not cards.
- Multi-select state with a floating action bar: Assign, Split, Not project, Attach.
- Drag feedback: the dragged bundle shows count and total; the target line highlights; a drop on a section header is allowed and shown as a lighter state.
- Empty states for each view, one line each, no illustration.
- Keep from today: the drag-to-line idea and the recently used targets strip. Fix from today: the `<select>` per row, the emoji buttons, the single flat table with tabs, the wall of small gray text.

**Modal — Create expense from a receipt**: receipt preview on the left; on the right, how it was paid (company card, cash, personal card, check, other), who paid (crew picker, shown only for personal card), line picker, amount and date prefilled from OCR, a one-line reason when cash. One primary button.

**Screen 2 — Super-admin All transactions**: a plain searchable table across projects, one row per charge, with claiming project, line, backup state, source, and a toggle Hide claimed. A row click opens the project workspace.

States to show for every component: default, hover, selected, drop-target, disabled, red flag, yellow flag. Deliver light mode first; dark can follow the budget page's existing variables.

## Decisions (Steven, Oct 30, 2026)

- Fringes: one line per department that has labor, all under 6500 Administrative; the line disappears when the department has no labor. Each labor section keeps an abbreviated read-only fringe summary at its foot.
- Workers' comp, production insurance, payroll service fee and production company fee stay auto-calculated. Their settings become editable on the budget page and the top sheet, not only in Settings.
- Splits: within one project only for now.
- Credits and refunds act as credits against the line, as in QuickBooks (negative, not zero).
- Receipt matches, including exact ones, require a click to confirm.
- Upload paths: Dropbox `_INBOX` sweep, web upload and mobile upload are all in use and all stay. A later phase cleans this pathway up so external users can upload from the app or a computer.
- QuickBooks For Review: Steven will keep charges accepted manually; no bank-rule work in this build.

## Sources

- Repo `stevencpierce/fpbudget` at commit `5fbfa57`, read directly: `CLAUDE.md`, `HANDOFF.md`, `FEATURES.md`, `AUDIT_CHECKLIST.md`, `CONSOLIDATION_PLAN_2026-06.md`, `models.py`, `actuals.py`, `qbo_sync.py`, `budget_calc.py`, `ai_layer.py`, `fp_analyzer.py`, `app.py`, `templates/reconcile.html`, `templates/budget.html`, `static/budget-js/b03.js`, `b04.js`, `b17.js`, `alembic/versions/0019_no_charge_reason.py`, `tests/`.
- [Concur: How Concur Expense works](https://www.concur.com/blog/article/how-concur-expense-works)
- [CSUSM Concur Expense guide (2021)](https://www.csusm.edu/travel/documents/concur_expense_guide_2021.pdf) for the Available Expenses, Allocate, Itemize and flag behavior.
