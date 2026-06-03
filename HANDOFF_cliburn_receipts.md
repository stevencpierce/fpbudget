# Handoff: Cliburn 25 receipt audit + Dropbox bulk importer

Paste this into the chat that has live access to the FPBudget site, database,
and Dropbox. It is self-contained — you should not need the originating
conversation.

---

## 1. Goal / situation

Repo: `stevencpierce/fpbudget` (Flask + PostgreSQL + Veryfi OCR + Dropbox).
Working branch with all new code: **`claude/wizardly-davinci-4p6aI`** (pushed).

Two related jobs:

1. **Bulk-import** a Dropbox folder of receipts through the existing document
   pipeline, server-side (no local download), tagged as a batch that's
   expected to contain many duplicates — we're fishing for the *missing*
   receipts.
2. **Investigate a suspected data-loss problem**: receipts that "went through
   the processor" are not showing up in the project's RECEIPTS folder. Confirm
   whether anything is truly lost and find the root cause.

Target project: **"Cliburn 25"** (Dropbox folder `250519_FPCL_CLIBURN 25`).
Operator user: **steven@thefp.tv**.

Source backup folder (same logical Dropbox path regardless of local mount):
```
.../_FP OPERATIONS FOLDER/250519_FPCL_CLIBURN 25/RECEIPTS FOLDER/TEXAS BACKUP/05_NON-WAGE BACKUP
```

---

## 2. Root-cause findings (why processed receipts aren't in RECEIPTS)

The app files processed receipts to
`<project>/01_ADMIN/PROCESSED DOCUMENTS/RECEIPTS/...`. A receipt can be fully
"processed" yet absent from that folder in five ways (most→least likely):

1. **Misclassified type.** Veryfi/inference labels the receipt `invoice` /
   `misc` / `estimate`, so it files into `PROCESSED DOCUMENTS/INVOICES` (etc.),
   not `/RECEIPTS`. See `fp_analyzer.py` `DOCUMENT_TYPES` (~line 58) and
   `auto_file_high_confidence` (~line 1074-1087).
2. **Low confidence → stranded in review.** Confidence < 0.85 (and no strong
   keyword match) ⇒ `needs_review=True` (`fp_analyzer.py:499-503`). The file is
   staged into `_SOURCE_ARCHIVE/` but **never copied to a type folder until a
   human completes the review in the UI**. An Actuals transaction *is* still
   created, so it looks processed in the ledger while the file never reaches
   RECEIPTS. **Prime suspect.**
3. **Filing hiccup silently demoted to review.** If the copy step throws,
   `analyze_and_file_single` swallows it and returns `needs_review`
   (`fp_analyzer.py:1315-1352`) — same stranded end-state as #2.
4. **Hard filing error → status `error`** (`fp_analyzer.py:951-960`). Not in
   RECEIPTS; bytes still safe in `_SOURCE_ARCHIVE`.
5. **Legacy duplicates** were moved to `PROCESSED DOCUMENTS/Duplicate/`
   (`fp_analyzer.py:989-991`) before the 2026-05-29 change.

**Why nothing is (almost certainly) permanently lost:** every upload's original
bytes are copied to `_SOURCE_ARCHIVE/` *before* anything else and that archive
is never auto-deleted (documented fail-safe, `fp_analyzer.py:524-530`,
`945-946`). So #2/#3/#4 are recoverable. The only genuine-loss path is a
failure during the very first staging step (Dropbox down at upload) — and in
that case the original still lives in the `05_NON-WAGE BACKUP` source folder.

**The actual workflow bug to fix:** #2/#3 strand a receipt with no alert. Worth
surfacing a "stranded receipts" count in the Docs UI so it can't happen
quietly. (Not yet built — see Next steps.)

---

## 3. What was built on branch `claude/wizardly-davinci-4p6aI`

### a. Schema: batch tagging (`models.py`)
- New model **`DocImportBatch`** (table `doc_import_batch`): one row per
  folder sweep — `project_id`, `created_by_user_id`, `label`, `source_path`,
  `expect_duplicates` (default True), `status`, and live tallies
  (`total_files`, `processed`, `filed`, `review`, `duplicates`, `errors`,
  `skipped`), plus `finished_at`.
- **`DocUpload.import_batch_id`** FK back-pointer (NULL for interactive
  uploads), with relationship `import_batch` ↔ `DocImportBatch.uploads`.

### b. Migration (`app.py`)
- `db.create_all()` creates the new table; the `import_batch_id` column is
  added via the standard idempotent `ALTER TABLE` list **and** the
  essential-column healing pass (`ADD COLUMN IF NOT EXISTS` +
  `CREATE INDEX IF NOT EXISTS ix_doc_upload_import_batch`). Runs automatically
  on app boot. **The audit relies on this column existing, so the branch must
  be deployed (boot-migrated) before running either script.**

### c. `import_dropbox_batch.py` — server-side bulk importer
Mirrors the interactive upload route `docs_upload_post` exactly (same SHA-256
dedup rules, same auto-Transaction gating), but reads bytes straight from
Dropbox and tags every row with a `DocImportBatch`. Duplicates are flagged for
review, never buried (matches current app behavior). Key flags:
`--project`, `--path`, `--user`, `--label`, `--expect-duplicates` (default on),
`--skip-existing` (resume: skip originals already filed in this project),
`--limit N`, `--dry-run`.

### d. `audit_receipts.py` — read-only reconciliation (makes NO writes)
Reconciles DocUpload rows ↔ actual Dropbox files ↔ source backup originals and
buckets every document: filed-in-RECEIPTS, filed-under-another-type, stranded
in review, errored, duplicate, archive-only. With `--verify-dropbox` it
confirms each filed/archive path physically exists and separates **recoverable**
(archive present) from **true loss** (both gone). With `--path` it hashes the
source originals to flag any that **never produced a DocUpload row**.

Path handling: both scripts auto-resolve a full local path (CloudStorage or
`/Volumes/...`) by stripping everything up to and including `_FP OPERATIONS
FOLDER`, because the app's Dropbox client is namespace-scoped to that folder.
So the path above resolves to
`/250519_FPCL_CLIBURN 25/RECEIPTS FOLDER/TEXAS BACKUP/05_NON-WAGE BACKUP`.

---

## 4. Commands to run (in the live environment, secrets present)

**Step 1 — read-only audit first (safe, repeatable):**
```bash
python audit_receipts.py --project "Cliburn 25" --verify-dropbox \
  --path "/Volumes/FP_95_32TB/Framework Production Dropbox/Steven Pierce/_FP OPERATIONS FOLDER/250519_FPCL_CLIBURN 25/RECEIPTS FOLDER/TEXAS BACKUP/05_NON-WAGE BACKUP"
```
Read the summary: how many landed in RECEIPTS, how many under other types,
how many stranded in review/error, any TRUE LOSS, and how many originals never
produced a DB row.

**Step 2 — fill only the gaps (after reviewing the audit):**
Dry run first:
```bash
python import_dropbox_batch.py --project "Cliburn 25" --user steven@thefp.tv \
  --label "Cliburn 25 — Texas non-wage backup" --skip-existing --dry-run \
  --path "/Volumes/FP_95_32TB/Framework Production Dropbox/Steven Pierce/_FP OPERATIONS FOLDER/250519_FPCL_CLIBURN 25/RECEIPTS FOLDER/TEXAS BACKUP/05_NON-WAGE BACKUP"
```
Then drop `--dry-run` to run for real. `--skip-existing` means it only files
originals not already present, so no duplicate pile-up. Consider `--limit 5`
for a first live test.

---

## 5. Decisions already made (don't re-ask)
- Importer form: **standalone CLI script** (not an admin endpoint).
- Duplicate handling: **real batch tag** (`DocImportBatch.expect_duplicates`)
  on top of the existing per-file flag-for-review dedup.
- Project: **"Cliburn 25" already exists**; resolve by name fragment or id.

## 6. Open / next steps
- [ ] Deploy branch `claude/wizardly-davinci-4p6aI` so the migration runs and
      the scripts exist in the server shell (a PR was NOT yet opened — open one
      if that's the deploy path).
- [ ] Run the audit (Step 1), interpret, then the targeted import (Step 2).
- [ ] (Recommended) Build the "stranded receipts" indicator in the Docs UI so
      review/error strands (#2/#3) are visible and can't recur silently.
- [ ] (Optional) Add a Docs UI filter/chip to view documents by import batch —
      data model supports it; no UI yet.

## 7. Note on the originating sandbox
The code was written in an isolated, ephemeral container with no DB, no
Dropbox/Veryfi credentials, and no network path to production — which is why
the scripts could be built and validated (mappers/DDL/path logic all
unit-checked) but not executed against live data there.
