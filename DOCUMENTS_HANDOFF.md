# FPBudget Documents & People Build Handoff

Oct 30, 2026 · @Steve

The documents module has every piece it needs and no spine: a document belongs to a project, a vendor is a text string, and a W-9 is recognized three different ways. This handoff gives documents an owner that outlives the project, a person-or-vendor record with projects, amounts paid and paperwork on file, and a list of the logic faults to fix first. It does not build a tax engine: QuickBooks stays the source of truth for vendors and generates the 1099s, Wrapbook stays the source of truth for employees, and FPBudget mirrors both so the three agree. Sequence: the Actuals handoff ships first; this one's Phase 0 fixes ride along because they close data-loss paths; the rest follows. Written for Claude Code with a Claude Design brief at the end.

## What exists today

The pipeline is good: upload from web, mobile or the Dropbox `_INBOX` sweep, OCR (Claude first, Veryfi fallback), type detection, a rename to `{date}_{PREFIX}_{vendor}_{total}`, filing into the project's Dropbox tree, a source archive that is never deleted, and three layers of duplicate detection. What is missing is ownership.

| Piece | Status | Where |
| --- | --- | --- |
| Document record | Built, project-locked | `DocUpload` (models.py 1171): `project_id` is NOT NULL, so no document can exist outside a project. Person link is `crew_member_id`; vendor is a free string |
| People | Built, global | `CrewMember` (models.py 495) is global, with `is_vendor` for loan-outs and companies. `ProjectCrewMember` holds per-project employment type and union status. No tax fields anywhere |
| Vendors | Not a thing | No Vendor table. A vendor is either a `CrewMember` with `is_vendor=True` or a string on `Transaction.vendor`, `DocUpload.vendor`, `PurchaseOrder.vendor_name`. `canon_vendor` + `VendorAlias` (actuals.py 27-110) clean the strings; nothing ties them to a record. `/vendors/audit` (app.py 38650) exists with no UI |
| Docs Inbox | Built, needs cleanup | `/projects/<pid>/docs` → `docs_inbox.html`; data from `docs_inbox_json` (app.py 33767) with one state per row (duplicate, excluded, needs\_review, needs\_match, needs\_coding, done). Bulk bar: Confirm matches, Set type, Not project. No bulk assign-to-person |
| Doc editing | Two half-matching editors | The budget.html modal (3738-3976) has itemize, parse payroll, backups, travel, PO, log-expense. The standalone `doc_editor.html` has itemize and OCR pull only. Shared field spec in `static/doc-form.js` (`FIELD_SPEC`, line 25) |
| Attach to a person | Built, one path | `POST /docs/upload/<uid>/update` saves `crew_member_id`; the Person/Vendor picker shows only for invoice, contract, tax\_form, payroll, timecard, release, employee\_vendor\_doc. `crew-suggest` (app.py 36077) fuzzy-matches at 0.80, advisory only |
| Crew Database (Dropbox) | Built | `_copy_doc_to_crew_folder` (app.py 1255) copies a person-linked doc to `<OPS>/Crew Database/<Person>/<Year>[/<Project>]/`; per-person sync `/crew/<cid>/docs/sync`; super-admin vault modal in `crew.html` 583-648 |
| Person page | Partial | Global profile drawer `crew_profile_json` (app.py 38255): all docs across projects, completeness (contract, tax\_form, photo\_id, invoice), engagements per project. No paid amounts, no year filter. Per-project `person_profile` (38089) has paid amounts for that project only |
| 1099 logic | One advisory | `tax_check` in the Wrap Book (app.py 5402-5423): payees with doc-linked spend over $600 and no tax form, per project, no calendar year, W-2 employees not excluded |
| Exports | None for people | `external_export.py` is budget lines only (MMB, ShowBiz). Docs CSV (37065) has no person column |

The doc types in use: receipt, invoice, estimate, purchase\_order, contract, insurance, tax\_form, payroll, timecard, legal, release, employee\_vendor\_doc, misc. Filing folders live in `fp_analyzer.DOCUMENT_TYPES` (line 58), keyed by uploader, not payee.

## Logic issues found

Fix these before any new feature; two of them destroy data.

| Severity | Issue | Where |
| --- | --- | --- |
| Data loss | Changing a document's type to a non-ledger category deletes every Transaction linked to it, including matched `qbo_sync` and `csv_import` bank rows. The single-delete path correctly removes only `source='doc_upload'` rows and unlinks the rest | `docs_upload_update`, app.py 42219-42223; compare 36771 |
| Data loss | Bulk delete and purge-ghosts hard-delete `DocUpload` rows (no Trash) and all linked Transactions, bank rows included | `/docs/<pid>/bulk-delete` 36932-36973, `/docs/<pid>/purge-ghosts` 37018 |
| Wrong totals | Five copies of the non-ledger type list disagree. `_NON_LEDGER_DOC_CATEGORIES` (23723) omits `employee_vendor_doc`; the admin reconcile path (3104) omits estimate, quote and purchase\_order, so it creates Transactions for estimates and POs. Timecard docs create Transactions | 34497, 42211, 36479, actuals.py 1859, b03.js 2015, 23723, 3104 |
| Broken feature | `timecard` is in every UI type list but not in `fp_analyzer.DOCUMENT_TYPES`, so bulk Set type → Timecard returns 400 and timecards file into MISC with a DOC prefix | `_INBOX_CATEGORIES` 34049, `_doc_intended_path` 39742 |
| Wrong year | Two places read `DocUpload.created_at`, which does not exist (the column is `uploaded_at`): the crew docs JSON buckets undated docs under "—" and the Crew Database copy files them under the current year | routes/crew.py 302, app.py 1284 |
| Misses the goal | Person-doc duplicate guard is project-scoped, so a W-9 collected again on another project is not recognized | `_flag_person_doc_duplicate` 36299, 36313 |
| Three truths | "W-9 on file" means three things: completeness checks the `tax_form` category only; the vendor badge regexes `/w-?9/` on sub-type or filename client-side; the wrap 1099 check accepts any `employee_vendor_doc`, so a driver's license counts | 38175, budget.html 12376, app.py 5416 |
| Field overloading | For `employee_vendor_doc`, `DocUpload.vendor` holds the sub-type ("W-9", "Driver's License"), so filenames, rollups and alias logic treat "W-9" as a vendor name | doc-form.js `vendorValue()` |
| Crash | `crew_delete` nulls BudgetLine and CrewAssignment but not `DocUpload.crew_member_id`, `ProjectCrewMember`, `Timecard`, `ScheduleDay.crew_member_id` or loan-out references: FK error or orphans for anyone with documents | routes/crew.py 186 |
| Unreachable | Pending and confirmed duplicates both map to inbox state `duplicate`, so the `dup_pending → needs_review` branch never runs | 33943 |
| Privacy | The tax\_form field label invites manual SSN entry ("Tax ID (EIN/SSN)") into `doc_number`, stored in plain text, while OCR deliberately suppresses SSNs | doc-form.js 33; fp\_analyzer 801, 835 |
| Attribution | `Transaction` has no person FK. Paid-per-person is inferred from the doc link or the line link, which double-counts two people on one line and misses `name_override`-only assignments. Parsed payroll rows carry no `crew_member_id` | models.py 131, app.py 38089, 37505 |
| Dead code | `handle_duplicates_auto`, `mark_known_dupes`, `file_confirmed`, `analyze_files` in fp\_analyzer are never called; `retry-filing` checks a legacy `filed` status; `ProjectClientContact` is retired | fp\_analyzer 729-1375, app.py 35379 |

## The model

One idea fixes most of this: a **Payee** is anyone the company pays, person or vendor, and a Payee owns its paperwork. Projects borrow a Payee; they never own one.

- `CrewMember` becomes the Payee record. It already is global and already has `is_vendor`; it gains a `kind` (person, loan\_out, vendor), tax fields, and an address. No second table: a loan-out is a vendor Payee linked from a person Payee, which `loan_out_vendor_id` already does.
- A document has an owner or it has a project, usually both. `DocUpload.project_id` becomes nullable; a W-9 uploaded from the Payee page has no project. A receipt has a project and usually no owner. An invoice has both.
- Three document classes decide the rules: **evidence** (receipt, invoice, payroll, timecard) backs a transaction and lives with the project; **paperwork** (tax\_form, employee\_vendor\_doc, contract, insurance, release, legal) lives with the Payee and is reused across projects; **estimates and POs** are commitments, never spend. The one list of which types create a Transaction lives in one place.
- Payment attribution is explicit. `Transaction.payee_id` is set when a transaction is assigned to a labor line with a crew assignment, when its document names a Payee, when a payroll package is parsed, or by hand. Paid-per-Payee is the sum of transactions with that `payee_id`, never inferred at read time.
- QuickBooks is the system of record for vendors and 1099s. Every vendor Payee carries a `qbo_vendor_id` and mirrors the QuickBooks Vendor record (display name, company, address, the `Vendor1099` flag, active). FPBudget never computes a 1099; it shows what QuickBooks will report (paid by calendar year, non-card, against the IRS threshold: $600 through 2025, $2,000 from 2026, indexed after) so a missing W-9 is caught in FPBudget months before year end, and the W-9 PDF lives with the Payee. Wrapbook is the system of record for employees; a person Payee who is on Wrapbook payroll is marked `is_w2_employee` and left out of vendor reporting. Pulling Wrapbook's roster and pay data into FPBudget is a later phase.
- W-9 on file means exactly one thing in FPBudget: a document owned by the Payee with `category='tax_form'` and `tax_form_kind='w9'` (or `w8ben`, `w8bene`), with a `received_on` date. The sub-type stops living in the `vendor` column. The QuickBooks `Vendor1099` flag is shown beside it; when the two disagree (W-9 here, flag off there, or the reverse) the Payee shows a yellow Mismatch chip, and the fix is made in QuickBooks.
- Vendor strings stay as the matching layer. `VendorAlias` gains `payee_id`, so once a vendor string is linked to a Payee every future transaction from that vendor attributes automatically. QuickBooks already names the vendor on every Purchase and BillPayment it sends (EntityRef), so for synced charges the attribution comes free: EntityRef → qbo\_vendor\_id → Payee, no fuzzy match needed.
- Nothing is hard-deleted. Documents go to Trash; a Payee is deactivated, never removed while anything references it.

## How a document flows

[Document-flow diagram lives in the Claude doc: https://claude.ai/artifact/4Xm7CQPx1fMMZGTzTPhyuC — Upload → OCR and type → Which class? → Evidence (receipt, invoice, payroll) → matched to a charge or logged as cash → project owns it, payee attributed via the charge; Paperwork (W-9, COI, contract, release) → Payee owns it, reused on every project, filed to Crew Database; Commitment (estimate, PO) → never spend, loops back when the invoice arrives]

The class decision replaces the five disagreeing type lists: evidence creates a `source='doc_upload'` Transaction that waits for a match or a cash log, paperwork creates none and attaches to a Payee, commitments create none and attach to a PO or estimate. The commitment path loops back to the upload step because a PO later arrives as an invoice, which is evidence.

## Data model changes

All in Alembic revisions chained after the Actuals handoff's `0021`, each mirrored in `_web_worker_essential_columns()` (app.py 32272) because production may never have run Alembic. Money stays `db.Numeric`.

| Change | Why | Migration |
| --- | --- | --- |
| `CrewMember.kind` (`person`, `loan_out`, `vendor`; default from `is_vendor`), `qbo_vendor_id` (String, indexed, unique when set), `qbo_sync_token`, `qbo_synced_at`, `qbo_vendor_1099` (Bool, mirrored), `qbo_origin` (`qbo` = first seen by the mirror, `fpbudget` = created in QuickBooks from here), `legal_name`, `address_line1`, `address_line2`, `city`, `state`, `postal_code`, `w9_received_on` (Date), `w9_doc_upload_id` (FK), `is_w2_employee` (Bool), `wrapbook_worker_id` (String, nullable, for the later phase), `notes` | Makes CrewMember the Payee and the mirror of a QuickBooks Vendor. No TIN is stored at all: QuickBooks holds it and prints the 1099; FPBudget holds the W-9 PDF | `0022_payee_fields` |
| `DocUpload.project_id` nullable; new `owner_payee_id` (FK CrewMember, replaces the meaning of `crew_member_id`, which stays as an alias column until the UI is moved), `tax_form_kind` (`w9`, `w8ben`, `w8bene`, `w4`, `i9`, `other`), `doc_subtype`, `received_on` (Date), `expires_on` (Date, for COIs) | A W-9 can exist with no project; a COI can expire; the sub-type leaves the `vendor` column | `0023_doc_ownership`; a data migration copies `employee_vendor_doc` sub-types out of `vendor` and clears `vendor` |
| `DocUpload.doc_class` computed in code, not stored: `evidence`, `paperwork`, `commitment` from `category`. One function `actuals.doc_class(category)` replaces the five non-ledger lists | One truth for which types create a Transaction | code only |
| `Transaction.payee_id` (FK CrewMember, nullable, indexed), `payee_source` (`qbo_entity`, `line`, `doc`, `payroll_parse`, `vendor_alias`, `manual`), `qbo_entity_ref` (the Vendor id QuickBooks sent on the Purchase or BillPayment) | Explicit attribution; synced charges attribute from `EntityRef` with no matching at all | `0024_txn_payee` plus a backfill task (marker + advisory lock) |
| `VendorAlias.payee_id` (FK, nullable) | For CSV and manual rows, linking a vendor string to a Payee once attributes every later charge | `0024_txn_payee` |
| `timecard` added to `fp_analyzer.DOCUMENT_TYPES` (folder `TIMECARDS`, prefix `TIMECARD`) | Unbreaks bulk Set type and filing | code only |
| `DocUpload` bulk delete and purge write `deleted_at` and `trash_meta`, never `DELETE`; category change to non-ledger deletes only `source='doc_upload'` rows and unlinks the rest | Stops the two data-loss paths | code only, Phase 0 |

**QuickBooks vendor mirror**: `qbo_sync.py` gains `sync_vendors()` using the same OAuth token and scope (`com.intuit.quickbooks.accounting` already covers the Vendor entity): query `SELECT * FROM Vendor` on first run, then CDC on each 12-hour sync. Match by `qbo_vendor_id`; on first run match existing `CrewMember` rows by exact display name, then by `VendorAlias`, and leave the rest for a one-time Link screen. Fields mirrored: `DisplayName`, `CompanyName`, `PrimaryEmailAddr`, `PrimaryPhone`, `BillAddr`, `Vendor1099`, `Active`. Direction for existing records is QuickBooks → FPBudget only. The one write FPBudget makes is Create: a Payee with no qbo_vendor_id gets a Create in QuickBooks button (and the New payee form a checkbox) that POSTs a new Vendor from the Payee's fields, stores the Id as qbo_vendor_id and sets qbo_origin='fpbudget'. After that the record is QuickBooks's: edits here never push, and the mirror overwrites local copies of mirrored fields on each sync. An exact DisplayName match in QuickBooks is linked, never duplicated or updated. Employees go to Wrapbook, so the button is hidden for is_w2_employee. A Payee with no `qbo_vendor_id` and non-card spend shows a yellow Not in QuickBooks chip, because QuickBooks cannot 1099 someone it does not know.

**Reporting view, read-only**: for a calendar year, per Payee with `is_w2_employee = False`: `SUM(amount)` of transactions with `payee_id`, `counts_as_spend()`, `txn_date` in the year, grouped by `payment_method` into card and non-card; W-9 on file = `w9_received_on IS NOT NULL`; `Vendor1099` from the mirror; flag when the non-card total meets that year's threshold and either the W-9 or the QuickBooks flag is missing. Thresholds live in a small `reporting_threshold` table (`tax_year`, `amount`, `source`), seeded 2025 = 600 and 2026 = 2000, editable by a super-admin for the indexed years. This is a preview of what QuickBooks will file, not the filing.

## Screens

Three screens, and the existing `/crew` list becomes the first one.

**Payee directory** at `/payees` (admin and super-admin; replaces `/crew`): one table of people and vendors with a kind filter, search, and columns Name, Kind, Department or category, Projects (count), Paid this year, Paid all time, W-9 (date or a red dot), COI (expires), Last activity. Sorting and a year picker at the top. A `Needs W-9` chip shows only Payees at or above the chosen year's threshold in non-card payments without one; the threshold in force shows beside the year picker. Export CSV gives the 1099 prep sheet for the chosen year.

**Payee page** at `/payees/<id>` (the global profile drawer, promoted to a page):

| Section | Content | Source |
| --- | --- | --- |
| Header | Name, kind, legal name, address, QuickBooks vendor link and 1099 flag, W-2 badge if employee, loan-out link | `CrewMember` fields |
| Paid by year | One row per calendar year: card, non-card, total, 1099 flag. Current year first | the reporting query |
| Projects | One row per project: role or lines, dates worked (from `CrewAssignment` and `ScheduleDay`), budgeted, paid; click opens the project People tab | `person_profile` logic, made global |
| Documents on file | Paperwork grouped by type: tax forms (kind, received, link), COI (expires, red when past), contracts, IDs, releases. Each shows which Dropbox copy it is and a Sync status | `DocUpload` with `owner_payee_id`, plus the Crew Database copy |
| Evidence trail | Invoices, timecards and payroll rows across projects, newest first, each with its project, amount and the charge it matched | `DocUpload` evidence class plus `Transaction.payee_id` |
| Vendor names | The raw vendor strings linked to this Payee, with an Unlink action and a field to add one | `VendorAlias.payee_id` |

Upload a document from this page: no project required; it lands as paperwork owned by the Payee and files to the Crew Database folder.

**Docs Inbox** at `/projects/<pid>/docs` keeps its shape and loses its mess:

- One editor. The budget.html modal and `doc_editor.html` become one component with every action (itemize, parse payroll, backups, travel, PO, log expense, assign payee) so nothing depends on which door the person came in through.
- Payee is a first-class field on every document type, with the fuzzy suggestion shown as a ghost chip and confirmed by click; bulk Assign payee joins the bulk bar beside Confirm matches, Set type, Not project.
- The type picker groups by class (Evidence, Paperwork, Commitments) and shows what each class does in one line.
- Pending duplicates land in Needs review as the comment already intends; confirmed duplicates stay in Duplicates.
- Timecard works everywhere it is offered.
- The tax-form fields are kind and date received; nothing asks for a tax ID, which lives in QuickBooks.

Mobile upload gains an optional Payee picker (it has only a doc-type list today), so a crew member photographing their own W-9 or invoice tags it on the spot.

## Build plan for Claude Code

Same rules as the Actuals handoff: CLAUDE.md pre-deploy checks after every phase, Alembic plus the essential-columns mirror, `db.Numeric`, no blueprints, `_log_activity` on every mutation, `scripts/route_snapshot.py` diff IDENTICAL after any slice. Run Phase 0 before anything in the Actuals plan, since it closes data-loss paths.

**Phase 0 — Stop the bleeding (half a day)**

- [ ] `docs_upload_update` (app.py 42219): on a category change to non-ledger, delete only `Transaction.source == 'doc_upload'` rows and set `doc_upload_id = NULL` on the rest, the same as the single-delete path at 36771. Call `_delete_evidence_for_txns` first.
- [ ] `/docs/<pid>/bulk-delete` (36932) and `/docs/<pid>/purge-ghosts` (37018): soft-delete through the same helper the single delete uses; never `DELETE` a DocUpload; never touch bank-sourced Transactions.
- [ ] Replace the five non-ledger lists with one `actuals.doc_class(category)` and use it at 34497, 42211, 36479, actuals.py 1859, 23723, 3104, and `b03.js` 2015 (serve the list to JS from one endpoint or a template constant).
- [ ] Add `timecard` to `fp_analyzer.DOCUMENT_TYPES` and `DOC_PREFIXES`; add it to the restore path at 36854 alongside payroll.
- [ ] `created_at` → `uploaded_at` in routes/crew.py 302 and app.py 1284.
- [ ] `crew_delete` (routes/crew.py 186): refuse when any DocUpload, Timecard, ScheduleDay, ProjectCrewMember or loan-out reference exists; offer deactivate instead.
- [ ] Fix the unreachable `dup_pending → needs_review` branch at 33943.
- [ ] Unit tests in `tests/unit/test_doc_class.py` and `test_doc_delete_rules.py` with `SimpleNamespace` stubs.

**Phase 1 — Payee record (one to two days)**

- [ ] Migration `0022_payee_fields` on `crew_member`; `kind` backfilled from `is_vendor`.
- [ ] `GET /payees`, `GET /payees/<id>`, `POST /payees/<id>/update` in a new `routes/payees.py`; `/crew` redirects to `/payees`. Keep `/crew/new` and the `/crew/<cid>/*` JSON routes working under both paths for one release.
- [ ] Payee page sections from `crew_profile_json` (38255) plus `person_profile` (38089) made global; paid-by-year from the reporting helper.
- [ ] W-9 on file becomes one function `payee_w9_status(payee)` used by completeness (38175), the vendor badge (replace the client-side regex at budget.html 12376), and the wrap `tax_check` (5402).

**Phase 2 — Documents follow the Payee (one to two days)**

- [ ] Migration `0023_doc_ownership`: `project_id` nullable, `owner_payee_id`, `tax_form_kind`, `doc_subtype`, `received_on`, `expires_on`; data migration moves sub-types out of `vendor`.
- [ ] `_flag_person_doc_duplicate` (36299) becomes global per Payee and sub-type, and the new copy of an existing W-9 asks "replace or keep both".
- [ ] Upload from the Payee page: `POST /payees/<id>/docs/upload` runs `_ingest_document_bytes` with no project and files to `<OPS>/Crew Database/<Payee>/<Year>/`.
- [ ] `_doc_intended_path` (39742): paperwork files under the Payee, evidence under the project; stop keying folders by uploader for paperwork.
- [ ] Payee field in `FIELD_SPEC` (doc-form.js 25) for every type; bulk `assign_payee` action in `docs_inbox_batch` (34011); suggestion from `crew-suggest` shown as a ghost chip.
- [ ] Merge `doc_editor.html` and the budget.html modal into one include with every action; both pages mount it.
- [ ] Mobile: optional `payee_id` on `/api/v1/projects/<pid>/docs/upload` and a picker in `UploadScreen.tsx`.

**Phase 3 — Payment attribution and the 1099 sheet (one to two days)** — built 2026-11-02 (PR "Documents Phase 3"). `payee_source` values shipped as `qbo | line | doc | alias | manual` (the payroll parse writes `doc` with the person's id per row; the exact-name match counts as `alias`). The vendor mirror keeps every QuickBooks Vendor in a new `qbo_vendor` table (the Link screen reads it) and its CDC watermark on `qbo_connection.vendor_cdc_since`; the mirror never overwrites a Payee's `name` (the QuickBooks DisplayName shows from `qbo_vendor`) and an empty QuickBooks value never blanks a local one.

- [x] Migration `0024_txn_payee`: `Transaction.payee_id`, `payee_source`, `qbo_entity_ref`; `VendorAlias.payee_id`. Backfill task with marker and advisory lock (`_backfill_txn_payee`, marker `backfill_txn_payee_2026_11`); conflicts logged to `ActivityLog` (`action='attribution_conflict'`), not guessed. Also links a learned alias to the one Payee whose name its clean name equals.
- [x] `qbo_sync.sync_vendors()`: full pull on first run, CDC after, inside the existing 12-hour loop (runs before the project syncs, fail-open); `_extract_txn_fields` keeps `EntityRef` as `qbo_entity_ref` and `sync_project` sets `payee_id` from it (fills NULL, replaces a prior `qbo` only). One-time Link screen at `/admin/qbo-vendors` (super-admin) for QuickBooks vendors with no matching Payee: link to an existing Payee, Create payee from this vendor, Sync now. A W-2 employee is never auto-linked by name.
- [x] `link_transaction_to_line` sets `payee_id` from the line's `CrewAssignment` or `assigned_crew_id` when there is exactly one (and clears a stale `line` value when the new line has no single person); `docs_parse_people` returns `crew_member_id` per row and `_itemize_apply` writes it (else the document's owner); `confirm_match` copies the doc's owner; the doc editor / inbox payee pick propagates to the document's charges; CSV and manual rows use `VendorAlias.payee_id` (or an exact name match); a manual expense accepts `payee_id` (`manual`). Vendor names link / unlink to a Payee from the Payee page.
- [x] Reporting helper `actuals.paid_by_payee(year)` prefers `Transaction.payee_id` and falls back to the doc-owner / single-line inference only for rows still NULL; `GET /payees/report.csv?year=` and the Payee directory read it; Wrap Book `tax_check` scopes to calendar year, excludes `is_w2_employee`, and shows the QuickBooks state (Not in QuickBooks / Linked · 1099 on / off) beside the W-9.
- [x] POST /payees/<id>/qbo-create: refuses 409 when qbo_vendor_id is set or is_w2_employee; exact DisplayName match → link, else create Vendor (legal_name or name, CompanyName, email, phone, BillAddr, Vendor1099 true when a W-9 is on file); stores Id and qbo_origin='fpbudget'; _log_activity. New payee form gets a Create in QuickBooks checkbox (hidden for W-2). Mirrored fields on a linked Payee are read-only in the edit form and refused by the server. `qbo_sync` has no update / sparse-update / delete helper at all.
- [x] Update `FEATURES.md`, `AUDIT_CHECKLIST.md` (D3-1..D3-7). `HANDOFF.md` untouched (it predates this build).

**Loan-out companies (Nov 4)** — built 2026-11-04. A person Payee lists the companies they invoice through (`payee_loanout`: company name + optional QuickBooks vendor from the mirror, Alembic 0028). The model note above ("a loan-out is a vendor Payee linked from a person Payee, which `loan_out_vendor_id` already does") is superseded for identity: `actuals.same_payee` reads only `payee_loanout`. Charges and documents from the company attribute to the PERSON (QuickBooks EntityRef of the company's vendor → the person; an invoice from the company → the person via `payee_for_vendor_string`; crew-suggest proposes the person "via" the company); reports group by the person; the 1099 CSV and the Payee page show the company each payment went to. Add / remove on the Payee page (`POST /payees/<id>/loanouts`, `…/loanouts/<lid>/remove`, admin; logged). See ACTUALS_HANDOFF "Loan-outs + Compare duplicates".

**Phase 4 — Later: Wrapbook handshake.** Not in this build. Wrapbook stays the system of record for employees; the goal is one way, Wrapbook → FPBudget: the worker roster (to set `is_w2_employee` and `wrapbook_worker_id` on person Payees) and each payroll run's per-person gross and employer fringe, replacing the PDF parse with structured data. Scope it once the Actuals build ships and after confirming what Wrapbook exposes (API, scheduled CSV export, or webhook).

**Phase 5 — Wrap sweep and the Documents tab overhaul (after the Actuals tab is right).** Scoped Nov 2, 2026 from Steven's first live test; not started.

- [ ] **Discard duplicate on the row.** The inbox's "Possible duplicate" chip gains the action it is missing: a **Discard duplicate** button on the row and in the bulk bar (with **Keep both** beside it) that calls the existing `resolve-duplicate` `confirm` path — the file moves to the project's `_DUPLICATES/` folder in Dropbox and the row leaves the inbox; the coded-or-matched guard stays. `_DUPLICATES/` stays per project.
- [ ] **Dropbox matches the record at Wrap, not on every edit.** Editing vendor, amount, date, type or payee in FPBudget does not touch Dropbox. A **Wrap sweep** (button on the Wrap Book, dry-run report first, then Apply) renames and re-folders every document to match its record — `{date}_{PREFIX}_{vendor}_{total}` and the type's folder, paperwork under the Payee — and lists anything it could not reconcile. The point: at wrap, searching Finder or Dropbox finds the file by what the OCR and the edits say it is.
- [ ] **OCR pulls everything useful.** Keep every field the OCR returns as document metadata (PO number, invoice number, payee, line items, tax, payment method, notes), not only the fields the filename uses; show them in the editor. Low-confidence pulls raise a review pop-up instead of filing silently.
- [ ] **Documents tab overhaul, in the Actuals idiom.** The Documents tab gets the same treatment the Actuals tab got: one workspace with views, a row-level action set, bulk bar, keyboard shortcuts and drag-and-drop where it makes sense — and tools that line up with the Actuals tab (a receipt dropped on a charge there is the same object as a charge linked from a receipt here). Design pass on the Claude Design canvas first, then build.

**Acceptance**

- Changing a matched invoice to a contract leaves the bank charge in place and unlinked.
- A W-9 uploaded from a Payee page with no project shows on every project's People tab for that person and in the Crew Database folder, and the same person's second W-9 on another project is flagged as a duplicate.
- The Payee directory for 2026 lists every non-employee paid $2,000 or more by non-card means, with W-9 yes or no, and its CSV matches the page; switching the year to 2025 applies $600.
- A Wrapbook package parsed into six people shows six Payees with that week's wages on their pages.
- Bulk Set type → Timecard succeeds and the file lands in TIMECARDS.

## Claude Design brief

Same rules as the Actuals canvas: FPBudget's own light-theme tokens and Inter, HTML and CSS that drops into Jinja with vanilla JS, inline SVG icons, 36 px rows, states for default, hover, selected, disabled, red and yellow.

**Screen 1 — Payee page** (1440 wide): header band with name, kind chip, legal name, address, a QuickBooks chip (linked, 1099 on or off, or Not in QuickBooks), W-2 or loan-out badge, and an Upload document button. Below, a two-column layout: left, Paid by year as a compact table with the 1099 flag, then Projects; right, Documents on file as grouped cards (tax forms, COI with an expiry state, contracts, IDs, releases) each with a Dropbox sync dot, then Vendor names with unlink. Evidence trail as a full-width table at the bottom.

**Screen 2 — Payee directory** (1440 wide): year picker, kind filter (People, Loan-outs, Vendors), search, a `Needs W-9` chip with a count, the table, Export CSV. Two rows red-flagged, one vendor row, one employee row with the W-2 badge.

**Screen 3 — Docs Inbox, cleaned** (1440 wide): the current chips and facets kept, the row gains a Payee column with a ghost suggestion chip, the bulk bar gains Assign payee, and the type picker opens grouped by Evidence, Paperwork, Commitments with a one-line description under each group.

**Modal — Document editor**: one editor for both doors, with the preview left and fields right, the Payee field with its suggestion, and an action row (Itemize, Parse payroll, Backups, Travel, PO, Log expense) that shows only the actions valid for the document's class.

## Decisions (Steven, Oct 30, 2026)

- QuickBooks is the source of truth for vendors and 1099 generation; FPBudget mirrors QuickBooks Vendors onto the Payee record. FPBudget may create a new vendor or loan-out in QuickBooks from a Payee (Nov 1) but never updates or overwrites a record that exists there.
- Wrapbook is the source of truth for employees; its feed into FPBudget is Phase 4, later.
- Actualization ships first; only this doc's Phase 0 data-loss fixes ride along.
- 1099 scope follows IRS rules by tax year: $600 through 2025, $2,000 from 2026, inflation-indexed after; non-card payments only; W-2 employees excluded. The threshold is a per-year setting, never a constant.
- No TIN stored in FPBudget; QuickBooks holds it, FPBudget holds the W-9 PDF.
- Payee directory and Payee pages are admin and super-admin only.
- `/crew` redirects to `/payees`; the per-project People tab stays as the project view and links to the Payee page.

## Decisions (Steven, Nov 2, 2026 — after the first live test)

- The Actuals screen is "definitely better, still a little clunky"; get the Actuals tab right first, then overhaul the Documents tab the same way. The two work hand in hand and need one interface language and tools that make sense together.
- Dropbox renaming and re-foldering happens at Wrap only (for now), not on each edit; names carry what the OCR and the edits say.
- Pull every useful OCR field into metadata; more data is better. Low confidence → review pop-up.
- A possible duplicate needs a one-click Discard that moves the file to `_DUPLICATES/` in Dropbox and off the inbox.

## Sources

- Repo `stevencpierce/fpbudget` at commit `5fbfa57`, read directly: `models.py`, `app.py`, `actuals.py`, `fp_analyzer.py`, `routes/crew.py`, `routes/api_v1.py`, `templates/docs_inbox.html`, `templates/doc_editor.html`, `templates/_doc_fields.html`, `templates/crew.html`, `templates/budget.html`, `static/doc-form.js`, `static/budget-js/b03.js`, `b17.js`, `alembic/versions/`.
