#!/usr/bin/env python3
"""Server-side bulk document import from a Dropbox folder.

Sweeps every document in a Dropbox folder and runs it through the SAME
pipeline as the interactive uploader (``docs_upload_post`` in app.py):

    Veryfi OCR  →  auto-file into the project's Dropbox tree  →
    DocUpload row  →  auto-created Actuals Transaction

Every row produced is tagged with a ``DocImportBatch`` so the whole run is
groupable, auditable, and resumable.

Why this exists / why it's server-side
--------------------------------------
The browser uploader is one file at a time and pulls bytes through the
operator's machine. When the source receipts already live in Dropbox and
there are hundreds of them, downloading them locally just to re-upload is
pointless and slow. This script runs where the bytes already are: it
downloads each file from Dropbox *to the server*, OCRs and files it, and
never touches the operator's local disk or internet.

Duplicates
----------
The batch is flagged ``expect_duplicates`` by default because the whole
point is fishing for the *missing* receipts inside a pile that's mostly
already filed. This flag does NOT change behaviour — the existing per-file
hash dedup still runs: byte-identical files are filed in place and flagged
``is_duplicate`` for review (NOT silently buried), exactly like an
interactive upload. The flag just records intent so the Docs UI can render
the dup count as "expected" rather than alarming.

Running it
----------
Run from the deployed environment (e.g. the Render shell) where the
``DROPBOX_*`` / ``VERYFI_*`` / ``DATABASE_URL`` secrets are present::

    python import_dropbox_batch.py \
        --project "Cliburn 25" \
        --path "/Users/.../_FP OPERATIONS FOLDER/250519_FPCL_CLIBURN 25/RECEIPTS FOLDER/TEXAS BACKUP/05_NON-WAGE BACKUP" \
        --user steven@thefp.tv \
        --label "Cliburn 25 — Texas non-wage backup"

Always do a ``--dry-run`` first: it lists exactly what would be processed
without calling Veryfi, writing to Dropbox, or touching the database.

The source ``--path`` may be the full local CloudStorage path (as Dropbox
shows it on a Mac) or a path relative to the ``_FP OPERATIONS FOLDER``
namespace root — everything up to and including ``_FP OPERATIONS FOLDER``
is stripped automatically so the namespace-scoped client resolves it.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime

# Document extensions Veryfi can OCR. Anything else in the folder
# (spreadsheets, .DS_Store, zips, etc.) is skipped and counted under
# `skipped` rather than failed.
DEFAULT_EXTS = {
    ".pdf", ".jpg", ".jpeg", ".png", ".heic", ".heif",
    ".tif", ".tiff", ".webp", ".gif", ".bmp",
}

# The shared-folder namespace the app's Dropbox client is scoped to has the
# operations folder as its root, so namespace-relative paths begin just
# below this marker.
OPS_MARKER = "_FP OPERATIONS FOLDER"

log = logging.getLogger("import_dropbox_batch")


# ── path / lookup helpers ──────────────────────────────────────────────────

def namespace_relative(raw_path: str) -> str:
    """Turn a user-supplied source path into one the app's Dropbox client
    can resolve.

    The app scopes its client to the ``_FP OPERATIONS FOLDER`` namespace
    (see fp_analyzer.get_dropbox_client). So a Mac CloudStorage path like
    ``/Users/x/.../Steven Pierce/_FP OPERATIONS FOLDER/250519_.../05_NON-WAGE
    BACKUP`` becomes ``/250519_.../05_NON-WAGE BACKUP``.

    In the non-namespace (user-root) fallback, the operations path is
    prepended so the absolute Dropbox path is correct there too.
    """
    p = (raw_path or "").replace("\\", "/").strip()
    if OPS_MARKER in p:
        p = p.split(OPS_MARKER, 1)[1]
    rel = "/" + p.strip("/")
    if not os.getenv("DROPBOX_NAMESPACE_ID", "").strip():
        ops = os.getenv("DROPBOX_OPERATIONS_PATH", "/_FP OPERATIONS FOLDER").rstrip("/")
        rel = ops + rel
    return rel


def resolve_project(ProjectSheet, ident: str):
    """Find the project by numeric id, exact name, dropbox_folder, or a
    case-insensitive name/folder contains-match (so "Cliburn 25" finds
    "250519_FPCL_CLIBURN 25")."""
    if ident.isdigit():
        return ProjectSheet.query.get(int(ident))
    q = ProjectSheet.query
    exact = (q.filter(ProjectSheet.name == ident).first()
             or q.filter(ProjectSheet.dropbox_folder == ident).first())
    if exact:
        return exact
    like = f"%{ident}%"
    hits = (q.filter(db_or(ProjectSheet.name.ilike(like),
                           ProjectSheet.dropbox_folder.ilike(like)))
            .all())
    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1:
        names = ", ".join(f"#{h.id} {h.name!r}" for h in hits)
        raise SystemExit(f"Ambiguous --project {ident!r}; matches: {names}. "
                         f"Re-run with the project id.")
    return None


def iter_files(dbx, dropbox_dir, recursive, exts):
    """Yield Dropbox FileMetadata for candidate documents under a folder."""
    import dropbox as _dbx
    result = dbx.files_list_folder(dropbox_dir, recursive=recursive)
    while True:
        for entry in result.entries:
            if not isinstance(entry, _dbx.files.FileMetadata):
                continue
            name = entry.name
            if name.startswith(".") or name.startswith("~$"):
                continue
            ext = os.path.splitext(name)[1].lower()
            yield entry, (ext in exts)
        if not result.has_more:
            break
        result = dbx.files_list_folder_continue(result.cursor)


# `or_` from sqlalchemy, imported lazily so the module imports cleanly even
# if someone runs --help without a DB configured.
def db_or(*clauses):
    from sqlalchemy import or_
    return or_(*clauses)


# ── per-file processing (mirrors docs_upload_post in app.py) ────────────────

def process_file(*, data, filename, project, user, batch, dry_run,
                 deps):
    """Process one document end-to-end. Returns a short status string:
    'filed' | 'review' | 'duplicate' | 'error'. Mirrors the interactive
    upload route so behaviour stays identical to a browser upload."""
    (db, DocUpload, Transaction, analyze_and_file_single,
     extract_card_last4, sync_claim_state, log_activity) = deps

    file_hash = hashlib.sha256(data).hexdigest()

    # Pre-analysis duplicate check — same strict rule as the route: a hash
    # match against a row in THIS project that actually has a filed file
    # and isn't a not-yet-confirmed review row.
    duplicate_of = (
        DocUpload.query
        .filter_by(project_id=project.id, file_hash=file_hash)
        .filter(DocUpload.filed_dropbox_path.isnot(None))
        .filter(DocUpload.status != 'review')
        .first()
    )

    if dry_run:
        tag = "DUP?" if duplicate_of else "    "
        log.info(f"  [dry-run] {tag} would process {filename} "
                 f"({len(data)} bytes, sha={file_hash[:10]})")
        return "duplicate" if duplicate_of else "filed"

    ext = os.path.splitext(filename)[1].lower()
    r2_key = f"docs/{project.id}/{uuid.uuid4().hex}{ext}"
    safe_user = re.sub(r"[^\w\- ]", "", (user.name or user.email.split('@')[0]
                                         or 'unknown')) or "unknown"
    data_size = len(data)

    result = analyze_and_file_single(
        file_bytes=data,
        filename=filename,
        project_name=project.dropbox_folder,
        user_name=safe_user,
    )

    status_map = {"filed": "done", "needs_review": "review", "error": "error"}
    upload_status = status_map.get(result.get("status"), "error")

    vr = result.get("vr") or {}
    vendor_name = amount = doc_date = doc_number = None
    if vr:
        v = vr.get("vendor") or {}
        vendor_name = v.get("name") or v.get("raw_name")
        try:
            amount = float(vr.get("total")) if vr.get("total") is not None else None
        except Exception:
            amount = None
        try:
            _d = vr.get("date") or ""
            doc_date = datetime.strptime(_d[:10], "%Y-%m-%d").date() if _d else None
        except Exception:
            doc_date = None
        doc_number = (vr.get("invoice_number") or vr.get("purchase_order_number")
                      or vr.get("tax_id") or vr.get("ein") or None)
        if doc_number:
            doc_number = str(doc_number)[:100]
    card_last4 = extract_card_last4(vr) if vr else None

    # Late-stage duplicate re-check (race window) — same as the route.
    if not duplicate_of:
        duplicate_of = (
            DocUpload.query
            .filter_by(project_id=project.id, file_hash=file_hash)
            .filter(DocUpload.filed_dropbox_path.isnot(None))
            .filter(DocUpload.status != 'review')
            .first()
        )

    upload = DocUpload(
        project_id=project.id,
        uploader_id=user.id,
        import_batch_id=batch.id,
        r2_key=r2_key,
        original_filename=filename,
        file_size=data_size,
        content_type="application/octet-stream",
        file_hash=file_hash,
        status=upload_status,
        veryfi_data=json.dumps(vr) if vr else None,
        vendor=vendor_name,
        amount=amount,
        doc_date=doc_date,
        doc_number=doc_number,
        card_last4=card_last4,
        confidence=round(float(result.get("confidence") or 0) * 100, 2),
        category=result.get("doc_type"),
        veryfi_category=(vr.get("category") if vr else None),
        filed_filename=result.get("new_filename") or None,
        filed_dropbox_path=result.get("filed_path") or result.get("staged_path"),
        filed_at=datetime.utcnow() if result.get("filed_path") else None,
        source_archive_path=result.get("staged_path"),
        is_duplicate=bool(duplicate_of) or bool(result.get("duplicate")),
        duplicate_of_id=duplicate_of.id if duplicate_of else None,
    )
    db.session.add(upload)
    db.session.commit()

    # Auto-create the Actuals Transaction, same gating as the route: skip
    # duplicates, errors, and non-ledger paperwork types.
    _NON_LEDGER_TYPES = {'tax_form', 'contract', 'release', 'legal',
                         'insurance', 'misc', 'employee_vendor_doc',
                         'estimate', 'quote', 'purchase_order'}
    if (upload.status in ('done', 'review')
            and not upload.is_duplicate
            and (upload.category or '') not in _NON_LEDGER_TYPES):
        try:
            auto_txn = Transaction(
                project_id=project.id,
                source='doc_upload',
                doc_upload_id=upload.id,
                vendor=upload.vendor,
                amount=upload.amount,
                txn_date=upload.doc_date.isoformat() if upload.doc_date else None,
                card_last4=upload.card_last4,
                is_expense=True,
                note=upload.note,
                match_status='unmatched',
                created_via_user_id=user.id,
            )
            db.session.add(auto_txn)
            db.session.flush()
            try:
                sync_claim_state(auto_txn)
            except Exception as ce:
                log.warning(f"    cross-project claim failed for txn: {ce}")
            db.session.commit()
        except Exception as te:
            log.warning(f"    auto-Transaction failed for upload #{upload.id}: {te}")
            db.session.rollback()

    # Flag (don't bury) byte-identical duplicates — same note the route sets.
    if duplicate_of and result.get("filed_path"):
        upload.note = (f"Possible duplicate of upload #{duplicate_of.id} "
                       f"({duplicate_of.filed_filename or duplicate_of.original_filename}). "
                       f"Review: keep both, or confirm it's a duplicate.")
        db.session.commit()

    try:
        log_activity(
            action='create', entity_type='doc_upload', entity_id=upload.id,
            entity_label=(upload.filed_filename or upload.original_filename),
            project_id=project.id, before=None,
            after={'status': upload.status, 'vendor': upload.vendor,
                   'amount': float(upload.amount or 0), 'category': upload.category,
                   'import_batch_id': batch.id},
            note=f'Bulk-imported "{filename}" (batch #{batch.id})')
    except Exception:
        pass

    if upload.status == 'error':
        return "error"
    if upload.is_duplicate:
        return "duplicate"
    return "review" if upload.status == 'review' else "filed"


# ── main ────────────────────────────────────────────────────────────────────

def main(argv=None):
    ap = argparse.ArgumentParser(description="Bulk-import a Dropbox folder of "
                                             "documents through the FPBudget pipeline.")
    ap.add_argument("--path", required=True,
                    help="Source Dropbox folder (full CloudStorage path or "
                         "namespace-relative path under _FP OPERATIONS FOLDER).")
    ap.add_argument("--project", required=True,
                    help="Target project: id, exact name, or a name fragment "
                         "(e.g. 'Cliburn 25').")
    ap.add_argument("--user", required=True,
                    help="Email of the user to attribute uploads to.")
    ap.add_argument("--label", default=None, help="Human label for this batch run.")
    ap.add_argument("--expect-duplicates", dest="expect_dup", action="store_true",
                    default=True, help="Flag the batch as likely-many-duplicates "
                                       "(default: on).")
    ap.add_argument("--no-expect-duplicates", dest="expect_dup", action="store_false")
    ap.add_argument("--no-recursive", dest="recursive", action="store_false",
                    default=True, help="Only the top folder, don't descend.")
    ap.add_argument("--skip-existing", action="store_true",
                    help="Skip files whose bytes are already filed in this "
                         "project (clean resume; off by default so intra-folder "
                         "duplicates are still flagged).")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N documents (0 = no limit). Good for a "
                         "test run.")
    ap.add_argument("--dry-run", action="store_true",
                    help="List what would be processed without OCR/Dropbox/DB writes.")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # App context + deps. Imported here so --help works without secrets.
    from app import app, db, _sync_claim_state, _log_activity
    from models import DocUpload, DocImportBatch, Transaction, ProjectSheet, User
    from fp_analyzer import (analyze_and_file_single, extract_card_last4,
                             get_dropbox_client)

    deps = (db, DocUpload, Transaction, analyze_and_file_single,
            extract_card_last4, _sync_claim_state, _log_activity)

    with app.app_context():
        project = resolve_project(ProjectSheet, args.project)
        if not project:
            raise SystemExit(f"No project matched --project {args.project!r}.")
        if not project.dropbox_folder:
            raise SystemExit(f"Project #{project.id} {project.name!r} has no "
                             f"dropbox_folder set — the pipeline can't file to it.")
        user = User.query.filter_by(email=args.user).first()
        if not user:
            raise SystemExit(f"No user with email {args.user!r}.")

        src_dir = namespace_relative(args.path)
        log.info(f"Project : #{project.id} {project.name!r} "
                 f"(files to /{project.dropbox_folder})")
        log.info(f"User    : {user.email}")
        log.info(f"Source  : {src_dir}")
        log.info(f"Mode    : {'DRY-RUN' if args.dry_run else 'LIVE'} "
                 f"| recursive={args.recursive} | expect_duplicates={args.expect_dup}")

        dbx = get_dropbox_client()

        # Verify the source folder exists / is reachable before creating a
        # batch row, so a bad path or namespace mismatch fails loudly.
        try:
            probe = dbx.files_get_metadata(src_dir)
        except Exception as e:
            raise SystemExit(
                f"Could not read source folder {src_dir!r}: {e}\n"
                f"  • Check the path is inside '_FP OPERATIONS FOLDER'.\n"
                f"  • Check the app's Dropbox token (DROPBOX_NAMESPACE_ID) has "
                f"access to it.")
        log.info(f"Found   : {getattr(probe, 'path_display', src_dir)}\n")

        batch = None
        if not args.dry_run:
            batch = DocImportBatch(
                project_id=project.id,
                created_by_user_id=user.id,
                label=args.label or f"Dropbox import: {os.path.basename(src_dir)}",
                source_path=src_dir[:600],
                expect_duplicates=args.expect_dup,
                status='running',
            )
            db.session.add(batch)
            db.session.commit()
            log.info(f"Batch   : #{batch.id} created\n")

        counts = dict(total=0, processed=0, filed=0, review=0,
                      duplicates=0, errors=0, skipped=0)

        try:
            for entry, is_doc in iter_files(dbx, src_dir, args.recursive, DEFAULT_EXTS):
                if not is_doc:
                    counts["skipped"] += 1
                    log.info(f"  skip (non-doc): {entry.name}")
                    continue
                counts["total"] += 1
                if args.limit and counts["processed"] >= args.limit:
                    log.info(f"  reached --limit {args.limit}, stopping.")
                    break

                path = entry.path_display or entry.path_lower

                if args.skip_existing and not args.dry_run:
                    # Cheap resume guard: a content-hash match already filed
                    # in this project means we processed it on a prior run.
                    try:
                        meta, resp = dbx.files_download(path)
                        data = resp.content
                    except Exception as e:
                        counts["errors"] += 1
                        log.warning(f"  download failed: {entry.name} → {e}")
                        continue
                    h = hashlib.sha256(data).hexdigest()
                    exists = (DocUpload.query
                              .filter_by(project_id=project.id, file_hash=h)
                              .filter(DocUpload.filed_dropbox_path.isnot(None))
                              .first())
                    if exists:
                        counts["skipped"] += 1
                        log.info(f"  skip (already filed #{exists.id}): {entry.name}")
                        continue
                else:
                    data = None  # fetched below unless dry-run

                if args.dry_run:
                    status = process_file(
                        data=b"", filename=entry.name, project=project,
                        user=user, batch=batch, dry_run=True, deps=deps)
                    counts["processed"] += 1
                    continue

                if data is None:
                    try:
                        meta, resp = dbx.files_download(path)
                        data = resp.content
                    except Exception as e:
                        counts["errors"] += 1
                        log.warning(f"  download failed: {entry.name} → {e}")
                        continue

                try:
                    status = process_file(
                        data=data, filename=entry.name, project=project,
                        user=user, batch=batch, dry_run=False, deps=deps)
                except Exception as e:
                    db.session.rollback()
                    counts["errors"] += 1
                    log.warning(f"  ERROR processing {entry.name}: {e}")
                    continue
                finally:
                    data = None  # release bytes before the next file

                counts["processed"] += 1
                # Roll status into the right tally bucket.
                if status == "filed":
                    counts["filed"] += 1
                elif status == "review":
                    counts["review"] += 1
                elif status == "duplicate":
                    counts["duplicates"] += 1
                elif status == "error":
                    counts["errors"] += 1

                log.info(f"  [{counts['processed']:>4}] {status:<9} {entry.name}")

                # Persist running tallies onto the batch every 10 files so a
                # crash leaves an accurate partial record.
                if batch and counts["processed"] % 10 == 0:
                    _save_counts(db, batch, counts)
                    db.session.commit()

        finally:
            if batch and not args.dry_run:
                _save_counts(db, batch, counts)
                batch.status = 'done'
                batch.finished_at = datetime.utcnow()
                db.session.commit()

        log.info("\n── Summary ─────────────────────────────")
        log.info(f"  candidates found : {counts['total']}")
        log.info(f"  processed        : {counts['processed']}")
        log.info(f"  filed            : {counts['filed']}")
        log.info(f"  needs review     : {counts['review']}")
        log.info(f"  flagged duplicate: {counts['duplicates']}")
        log.info(f"  errors           : {counts['errors']}")
        log.info(f"  skipped non-docs : {counts['skipped']}")
        if batch and not args.dry_run:
            log.info(f"  batch id         : #{batch.id}")


def _save_counts(db, batch, c):
    batch.total_files = c["total"]
    batch.processed = c["processed"]
    batch.filed = c["filed"]
    batch.review = c["review"]
    batch.duplicates = c["duplicates"]
    batch.errors = c["errors"]
    batch.skipped = c["skipped"]


if __name__ == "__main__":
    sys.exit(main())
