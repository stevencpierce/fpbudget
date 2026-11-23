# FPBudget Intake / Wrap Build Handoff

Nov 23, 2026 · @Steve

Intake sits in front of Actuals and Documents; Wrap sits behind them. Intake works out which project, payee and budget line an incoming QuickBooks charge or document most likely belongs to, using context from email and calendar, and queues that as a suggestion a person approves. Wrap is the definition of done: a gate of flags that must be cleared, a reference number on every expense, files renamed to match, a ledger spreadsheet in Dropbox that works without the software, and the project locked. Build order: project code, stage and reference numbers first; then the wrap gate, run on GrubHub; then the Intake queue. Written for Claude Code from Steven's dictated notes (Nov 23); a Claude Design pass comes before any new screen. A fuller draft lives in the claude.ai Project (`claude/INTAKE_WRAP_HANDOFF.md`); where it differs from this file, this file wins.

## What exists today

| Piece | State | Where |
|---|---|---|
| Project lifecycle | `ProjectSheet.status` = `active` \| `wrapped` \| `archived`. No Estimate / Awarded stage, no project code, no project dates | `models.py` `ProjectSheet` |
| Wrap | Built, **no gate**. `POST /projects/<pid>/wrap` renders the Wrap Book PDF into `01_ADMIN/WRAP BOOK/`, moves the folder with `_move_project_folder` to `/_WRAPPED PROJECTS/<date>_<slug>` (`DROPBOX_WRAP_PATH`), sets `status='wrapped'`, logs activity. Wrap fails open: a Wrap Book error still wraps | `app.py` `project_wrap`, `_build_wrap_book_data`, `/wrap-review`, `/wrap/book.pdf` |
| Wrap Book exceptions | Missing backup per line, `tax_check` (calendar year, excludes W-2, shows QuickBooks state) | `_build_wrap_book_data` |
| Document ingest | One path for web, mobile, Dropbox `_INBOX` and payee uploads | `app.py` `_ingest_document_bytes` |
| Matching engine | Suggests, never auto-confirms; one rule for every surface | `actuals.match_candidates`, `run_auto_match` |
| Duplicates | Twin rule + keeper rule, merge with undo, compare panel | `actuals.is_duplicate_twin`, `find_duplicate_groups`, `static/dup-compare.js` |
| Payee identity | Person ↔ loan-out, QuickBooks vendor id | `actuals.same_payee`, `payee_loanout` |
| AI Agent login | `User.role='ai_agent'` (`claude@thefp.tv`); can post through `/api/v1` | `models.py` `User`, `routes/api_v1.py` |
| User roles | `super_admin`, `admin`, `ai_agent`, `line_producer`, `dept_head`, `docs_only`; per-project `owner` / `editor` / `viewer` / `docs_only` | `models.py` |

Nothing renames documents to a reference number today; there is no reference number.

## The model

- **Stage** on every project: `estimate` → `awarded` → `wrapping` → `wrapped`. An Estimate project cannot receive charges or documents and is never offered as a target. Dates help but are never required.
- **Project code**, assigned when the stage moves to Awarded: client initials + year and month of production start. GrubHub Fee Breakdown is `GH-2609`; a second project for that client in that month is `GH-2609B`. The code goes on everything the app sends (call sheets, deal memos, POs) and people are asked to put it in email subjects and on invoices. Recognise it only against the list of real codes, never a loose letters-digits pattern (GrubHub's own PO was `WG-5465`).
- **Reference number** per expense: project code + running four-digit number, unique per project, `GH-2609-0147`. Assigned on first assignment to a line, section or split; never reused, never renumbered. One number per expense, not per line: a split keeps one number and the ledger shows each piece on its own row. Account code is a ledger column, not part of the number. Documents carry the number of the expense they back; a second document takes a letter (`GH-2609-0147a`).
- **Suggestion**: Intake's proposal of project, payee and line for one charge or document, with reasons and evidence (thread link, sender, subject, date, one-line reason, attachment). Email bodies never move into FPBudget. Every suggestion queues for a person. Auto-assign comes later and always carries a visible flag (CLAUDE.md: AI is advisory, never auto-confirms money).
- **Wrap gate**: the list of open flags. Wrap is blocked until each is fixed or waived with a typed reason; waivers go in the ledger.
- **Wrap run**: dry-run report, then Apply. Numbers any stragglers, renames and re-folders documents, writes the wrap package, moves the folder, sets the stage to Wrapped. The project is then read-only; reopen is super-admin only and logged.

## How an item flows

1. A person accepts the charge in QuickBooks (unchanged), or a document arrives by upload, mobile, `_INBOX`, or Claude from email.
2. Duplicate check at the door: the same document arriving a second way attaches to the existing record as another source.
3. Intake proposes project, payee and line with reasons; the suggestion queues.
4. Approve runs the existing actions (`claim_for`, assign to line, `confirm_match`, set payee). Reject takes a reason.
5. First assignment gives the expense its reference number.
6. Normal Actuals and Documents work continues.
7. Stage → Wrapping; the gate lists every open flag; flags are fixed or waived; the wrap run applies.

## Data model (proposed — confirm before migrating)

Alembic revision plus the essential-columns mirror, per CLAUDE.md; production has never applied a revision.

| Change | Why |
|---|---|
| `ProjectSheet.stage` (`estimate` \| `awarded` \| `wrapping` \| `wrapped`), `project_code` (unique when set), `client_initials`, `start_date`, `end_date` | Gate, code, dates. Keep `status` for archived and the existing wrapped check; decide whether `stage='wrapped'` replaces `status='wrapped'` or mirrors it |
| `ProjectSheet.next_ref_seq` (Integer) or a counter table with a row lock | Running number with no gaps from races |
| `Transaction.ref_number` (unique per project) | The expense's number |
| `DocUpload.ref_number`, `DocUpload.fingerprint` (content hash), a sources list per document | File names at wrap; stop duplicates at the door |
| `intake_suggestion` (target kind + id, proposed project / payee / line, confidence, reasons JSON, evidence JSON, state, decided_by, decided_at, reject_reason) | The queue |
| `wrap_waiver` (project, flag kind, entity id, reason, user, at) | Waivers in the ledger |

## Wrap gate flags

Steven's words: "this line isn't actualized, this line is missing something, this line is off."

- Line not actualized: estimate above zero, no actual, not marked no-spend.
- Charge assigned with no backup and no missing-backup reason.
- Split does not sum (`validate_splits`).
- Possible duplicate, counted twice (`find_counted_twice`) or voided in QuickBooks: unsettled.
- Receipts or invoices still unplaced.
- Unclaimed pool charges inside the project's dates that carry a suggestion for this project.
- Open or failed bill payments to this project's payees.
- Payee over the 1099 threshold with no W-9, or not in QuickBooks (today's `tax_check` becomes this flag).
- Payroll package tie-out delta.
- Line off: variance beyond a tolerance with no note. Tolerance **not decided**.

## What wrap produces

- **Cost ledger** in the project's Dropbox folder as `.xlsx` and `.csv`, plain values, no macros. One row per expense or split piece: reference number, date, payee, paid via (loan-out), amount, account code, section, budget line, payment method, QuickBooks type and id, file name(s), backup status, and a blank column for qualified / non-qualified cost (no tagging in this build). Further sheets: by line (estimate, working, actual, variance, references), payees (total paid, W-9, QuickBooks vendor), documents index, waivers, read-me. Modelled loosely on a New York tax-credit audit: general ledger in Excel, invoice plus proof of payment per sampled expense.
- **Receipts folder**: every expense document named reference first, then the existing pattern: `GH-2609-0147_2026-10-01_INVOICE_Igor-Kropotov_6400.00.pdf`.
- **Budget vs actual PDF** as it stood at wrap (the existing Wrap Book can carry this).
- The folder moves to `/_WRAPPED PROJECTS/<date>_<slug>`, as `project_wrap` already does.
- The test: with the app off, someone opens the Dropbox folder, picks a budget line in the spreadsheet and finds every receipt behind it.

Not discussed, so **not decided**: returned deposits, PO close-out, a formal payroll reconciliation step.

## Links to other modules

- **Budget**: lines are the targets; the line's Actual drill-down lists its reference numbers (line → numbers → file names); the gate reads estimate vs actual through the Cleanup C version rule.
- **Actuals**: Intake proposes; the existing claim / assign / split / match actions apply. Reuse the matching-engine pattern, the duplicates tools and the detail window (`FPDetail`).
- **Documents**: hooks into `_ingest_document_bytes`; adds the fingerprint and sources list; Payee attribution is a main signal.
- **Call sheets, deal memos, POs**: print the project code. Call sheets render in three modes; the code must show in all three.
- **QuickBooks**: read-only for Intake (accepted charges, vendor mirror, open and failed bill payments). No new writes.
- **Dropbox**: rename and re-folder at wrap only (Documents decision, Nov 2); every move goes through `_move_project_folder`.
- **Gmail and Calendar**: read by Claude outside the app, never by FPBudget.

## Screens

- **Intake queue**, per project and company-wide (admin). Same idiom as the Actuals workspace: dense rows, reasons inline, evidence in the detail window, approve / reject by keyboard, bulk approve for one payee or one thread.
- **Wrap gate** on the Wrap Book: the checklist, each flag linking to where it is fixed, waive with a reason, dry run, Apply.
- **Project settings**: stage, code, dates.
- **Admin → Maintenance tool**: estimate-stage projects that already hold charges or documents, to move or release by hand (a route + a TOOLS entry).

## Build plan for Claude Code

Same rules as the other handoffs: CLAUDE.md pre-deploy checks after every phase, Alembic plus the essential-columns mirror, `db.Numeric`, no blueprints, `_log_activity` on every mutation, one rule per decision.

**Phase 1 — Code, stage, dates, reference numbers, ledger export** — built 2026-11-23 on branch `claude/relaxed-meitner-7rdg3m` (not yet on `main`). The rules live in `project_codes.py`; the screen is `routes/project_setup.py` (Project setup, in the project nav's More menu and on each dashboard card); Alembic `0030_project_stage_code_refs` + the essential pass.
- [x] Stage, project code, client initials and dates on the project; Project setup page; code assigned at Award. **Stage sits beside status** (Steven, Nov 23): `stage` = estimate | awarded | wrapping, NULL reads as awarded (so every existing project keeps working); wrapped stays `status='wrapped'`. Read it only through `project_codes.project_stage(p)`. New and duplicated projects start as Estimates; projects imported from Dropbox folders do not.
- [x] Estimate projects refused as a target: `project_codes.accepts_spend` is checked in `claim_for`, `pool_filter` / `in_pool` (an Estimate sees only what it holds, so the matcher suggests nothing), `_ingest_document_bytes` (409 before anything is saved), the `_INBOX` sweep (files left in place until Award), manual expense and bank CSV import. One banner in `_project_nav.html`. There are no cross-project move pickers today.
- [x] Maintenance tool: "Estimates holding charges or documents" (`/admin/projects/estimate-holdings`), review only.
- [x] Reference numbers: not inside `claim_for` after all — many paths code a row by writing `budget_line_id` directly, and non-QuickBooks rows never reach `claim_for`. Instead ONE write, `project_codes.assign_ref_numbers(pid)`, runs from a SQLAlchemy flush hook for every project whose transactions or split pieces were touched, and at Award (which numbers the existing coded expenses oldest first — the backfill, previewed on the Award form with a count). Stored as `transaction.ref_project_id` + `ref_seq`; shown as `<code>-0001` through `ref_label` / `ref_labels`. Kept through uncode / recode; a row re-coded by another project gets that project's next number; never reused.
- [x] Detail window, the budget line's Actual drill-down and the reconcile tiles show reference numbers.
- [x] Ledger export any time: `GET /projects/<pid>/ledger.xlsx|csv` (`ledger_export.py`), linked from Project setup and the Actuals header. Sheets: Ledger, By line (working vs actual + references), Payees, Documents, Waivers (empty until Phase 2), Read me. The By line sheet has no Estimate column yet — matching Estimated lines to Working lines needs a rule first.
- [x] Project code on call sheets (pages 1 and 2, all three modes, and the default email subject) and on the POs page. **FPBudget has no deal memo feature** and POs are an internal list, not a document that goes out — revisit when either exists.
- [x] **Award is automatic** (Steven, Nov 23: "award is basically the same as going to a working budget"). The first Working budget — new, or a budget switched to Working, by any path — awards the project through the same flush hook (`project_codes.auto_award`): stage awarded, code from the client's initials (reused from the client's earlier projects) + the production start month (project start date, else a budget's, else the month in its Dropbox folder name, shown as "guessed" on Project setup), then its coded expenses are numbered. Without initials or a start month the project is still awarded and a banner asks for the code. The Award button stays for doing it early or by hand.
- [x] Existing projects: Admin → Maintenance → "Give existing projects their codes" (`/admin/projects/codes`, `backfill_plan` / `backfill_apply`). Preview, then Run — never at boot. Every active or wrapped project with a Working budget gets its code, earliest start first (plain code, then letters), and its coded expenses numbered; projects missing initials or a start month are listed for a person; an active project with no Working budget that holds nothing becomes an Estimate.

**Phase 2 — Wrap gate and wrap run (run it on GrubHub)**
- [ ] Gate flags as above, one function per flag, listed on the Wrap Book with links to the fix.
- [ ] Waive with a typed reason; waivers in the ledger.
- [ ] `project_wrap` refuses while flags are open, and stops failing open on a Wrap Book error.
- [ ] Wrap run: dry run, then Apply — number stragglers, rename and re-folder documents (Documents Phase 5 Wrap sweep moves here), write the ledger, receipts folder and budget-vs-actual PDF, move the folder, set stage Wrapped.
- [ ] Wrapped = read-only, never a target; reopen super-admin only, logged.

**Phase 3 — Intake queue**
- [ ] `intake_suggestion` table, queue screens, approve / reject running the existing actions.
- [ ] Fingerprint at ingest; a second arrival attaches as a source instead of a new record (moves the prevention half of Documents' duplicate work here).
- [ ] `/api/v1` endpoint for the AI Agent to post suggestions with evidence and attachments.

**Phase 4 — Sweeps and collaborators**
- [ ] Claude's scheduled sweep of Gmail / Calendar / QuickBooks.
- [ ] James's Claude connected as a second suggester.
- [ ] Per-project upload link carrying the code (replaces Actuals Phase 6).

**Phase 5 — Later: auto-assign** high-confidence items, always with a visible flag and undo.

**Acceptance**
- GrubHub moves to Wrapping, shows its open flags, clears or waives each, and wraps; the Dropbox folder holds the ledger and every receipt named by reference number.
- With the app off, a budget line in the spreadsheet leads to every file behind it.
- An Estimate project cannot be picked as a target anywhere.
- A document uploaded twice by two routes is one record with two sources.

## GrubHub, first project through the gate

Steven wants GrubHub wrapped now. From his Nov 23 sweep of Gmail against QuickBooks; none of these are fixed. Each one should show up as a gate flag.

- Hyo Jin An / ARTra Productions, sound, $1,554.64: ACH failed Nov 18; the bill is open.
- Rare Medium, color, invoice 4215, $3,000 (Oct 22, net 30): no bill or bill payment found. It may have been paid by card.
- Matt Yonks invoice 2, $5,000 check: shipped Nov 18, not yet cleared.
- Emily Giangreco's deal carries a 10% commission on $2,800. Hyphen was paid $2,800, so $280 is unbilled. Christian McQueen's side (About Artists) was paid $3,080.
- Sarah Caye's reimbursement receipts are in her Google Drive folder, not in Dropbox.
- Elliemade $2,800 is not GrubHub (it is Global Industrial Run Strong). AGTworks $4,000 is the GrubHub set build (Steven is fairly sure). Steven Pierce $20,000 on Oct 30 is the GrubHub director fee.
- Vendor payments tied to GrubHub total $94,509.63, payroll not included.

## Who does what

Partly decided.

- **Steven (EP / owner)**: accepts charges in QuickBooks; approves suggestions for now; runs wrap.
- **James (Head of Production) and whoever is producing**: approve suggestions on their projects; push people to use the code; James is the first collaborator to connect his own Claude.
- **Crew and vendors**: upload only.
- **Claude (`claude@thefp.tv`, `ai_agent`)**: reads Gmail, Calendar and QuickBooks; posts suggestions and attachments; proposes only.
- Coordinator and accountant: **not decided**. Map to the existing roles above and ask; do not invent permissions.

## Claude Design brief

Mock before building, as with Actuals: the Intake queue (project and company-wide), the wrap gate with waive and dry-run report, and project settings with stage, code and dates. Same interface language as the Actuals workspace and the detail window. No sketches yet.

## Decisions (Steven, Nov 23, 2026)

- Humans keep accepting charges in QuickBooks; automation starts after that.
- Claude proposes project, payee and line from email, calendar and QuickBooks context.
- Everything queues for approval at first. Auto-assign comes later, flagged.
- Projects are gated so estimates stop collecting receipts. Dates help but are not mandatory.
- Project code: client initials + year-month of production start, a letter for a second project.
- Reference numbers run per project and carry the project code.
- Project codes are used from the start of a job, especially in email subjects.
- Wrap is blocked until its flags are cleared. Wrap renames, moves the folder and leaves a future-proof spreadsheet.
- Collaborators can simply upload; some connect their own Claude.

## Open questions

- Client initials for existing clients.
- Does the project code freeze at award if the start date moves? (Recommended yes.)
- Do a collaborator's suggestions land straight in the project queue (Steven's leaning) or pass through him first?
- Variance tolerance for "line off".
- Tail after the last project date for late invoices (the Project draft says 45 days).
- Roles for coordinator and accountant.
- Start-paperwork checklist at intake (deal memos, I-9s, insurance certs) and who chases it.
- Deposits, PO close-out and payroll reconciliation at wrap.
- Wrapped-projects path: the code already uses `/_WRAPPED PROJECTS/<date>_<slug>`; confirm it stays.

## Sources

- Steven's dictated notes, Nov 23, 2026, written up in the claude.ai FP Budget Software project.
- Repo `stevencpierce/fpbudget` at commit `63ecaf0`, read directly: `CLAUDE.md`, `models.py`, `app.py` (`project_wrap`, `_build_wrap_book_data`, `_ingest_document_bytes`, `_DBX_WRAP_ROOT`), `actuals.py` (`is_duplicate_twin`), `ACTUALS_HANDOFF.md`, `DOCUMENTS_HANDOFF.md`.
