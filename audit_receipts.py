#!/usr/bin/env python3
"""Read-only receipt reconciliation audit.

Answers the question "we processed these receipts but they're not in the
RECEIPTS folder — where did they actually go, and are any truly lost?"
without writing anything. Safe to run as many times as you like.

It reconciles three sources of truth for one project:

  1. The DocUpload rows in the database (what the app thinks it processed).
  2. The actual files in Dropbox (does each row's filed/archived path
     really exist?).
  3. (optional) the original files in a source backup folder (were any
     never processed at all?).

and buckets every document so you can see the disposition at a glance:

  • filed correctly into PROCESSED DOCUMENTS/RECEIPTS
  • filed, but under a DIFFERENT type folder (INVOICES / MISC / ...) —
    i.e. Veryfi classified the receipt as something else
  • stranded in review (status='review' — staged only, never filed)
  • errored (status='error')
  • flagged duplicate
  • filed path MISSING from Dropbox (but source archive present → recoverable)
  • filed path AND archive BOTH missing → TRUE LOSS (alarm)

Run it server-side (Render shell) where the DROPBOX_*/DATABASE_URL
secrets live::

    python audit_receipts.py --project "Cliburn 25" --verify-dropbox

To also catch originals that never made it into the system, point it at
the backup folder (it downloads each original server-side to hash it and
match against DocUpload.file_hash)::

    python audit_receipts.py --project "Cliburn 25" --verify-dropbox \
        --path "/Volumes/.../_FP OPERATIONS FOLDER/250519_FPCL_CLIBURN 25/RECEIPTS FOLDER/TEXAS BACKUP/05_NON-WAGE BACKUP"
"""

import argparse
import hashlib
import logging
import os
import sys

log = logging.getLogger("audit_receipts")

RECEIPTS_SEGMENT = "/PROCESSED DOCUMENTS/RECEIPTS/"
PROCESSED_SEGMENT = "/PROCESSED DOCUMENTS/"
ARCHIVE_SEGMENT = "/_SOURCE_ARCHIVE/"
DUPLICATE_SEGMENT = "/PROCESSED DOCUMENTS/Duplicate/"


def classify_path(p):
    """Bucket a filed_dropbox_path by where it physically lives."""
    if not p:
        return "no_path"
    if DUPLICATE_SEGMENT in p:
        return "duplicate_folder"
    if RECEIPTS_SEGMENT in p:
        return "receipts"
    if ARCHIVE_SEGMENT in p:
        # path points only at the source archive, never copied to a type folder
        return "archive_only"
    if PROCESSED_SEGMENT in p:
        # extract the type folder name after PROCESSED DOCUMENTS/
        tail = p.split(PROCESSED_SEGMENT, 1)[1]
        seg = tail.split("/", 1)[0]
        return f"other_type:{seg}"
    return "elsewhere"


def dbx_exists(dbx, path):
    """True if a Dropbox path resolves; None if the check itself errored."""
    if not path:
        return False
    try:
        dbx.files_get_metadata(path)
        return True
    except Exception as e:
        msg = str(e).lower()
        if "not_found" in msg or "not found" in msg:
            return False
        return None  # ambiguous (permissions / transient) — don't cry loss


def main(argv=None):
    ap = argparse.ArgumentParser(description="Read-only receipt reconciliation audit.")
    ap.add_argument("--project", required=True, help="Project id, name, or fragment.")
    ap.add_argument("--path", default=None,
                    help="Optional source backup folder to reconcile originals against.")
    ap.add_argument("--verify-dropbox", action="store_true",
                    help="Confirm each filed/archived path actually exists in Dropbox.")
    ap.add_argument("--no-recursive", dest="recursive", action="store_false", default=True)
    ap.add_argument("--limit", type=int, default=0, help="Cap rows checked (0=all).")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    from app import app, db
    from models import DocUpload, ProjectSheet
    from fp_analyzer import get_dropbox_client
    from import_dropbox_batch import resolve_project, iter_files, namespace_relative, DEFAULT_EXTS

    with app.app_context():
        project = resolve_project(ProjectSheet, args.project)
        if not project:
            raise SystemExit(f"No project matched --project {args.project!r}.")
        log.info(f"Project : #{project.id} {project.name!r} → /{project.dropbox_folder}\n")

        rows = (DocUpload.query
                .filter_by(project_id=project.id)
                .order_by(DocUpload.uploaded_at.asc())
                .all())
        if args.limit:
            rows = rows[:args.limit]
        log.info(f"DocUpload rows in this project: {len(rows)}\n")

        dbx = get_dropbox_client() if (args.verify_dropbox or args.path) else None

        buckets = {}
        status_counts = {}
        problems = []          # rows that aren't cleanly filed in RECEIPTS
        true_loss = []         # filed + archive both gone
        recoverable = []       # filed gone but archive present
        hashes_done = set()    # file_hashes that are filed in RECEIPTS, status done

        for r in rows:
            status_counts[r.status] = status_counts.get(r.status, 0) + 1
            bucket = classify_path(r.filed_dropbox_path)
            if r.status == 'review':
                bucket = "stranded_review"
            elif r.status == 'error':
                bucket = "errored"
            buckets[bucket] = buckets.get(bucket, 0) + 1

            healthy = (r.status == 'done' and bucket == "receipts")
            if healthy and r.file_hash:
                hashes_done.add(r.file_hash)

            if args.verify_dropbox:
                filed_ok = dbx_exists(dbx, r.filed_dropbox_path)
                arch_ok = dbx_exists(dbx, r.source_archive_path)
                if filed_ok is False and arch_ok is False:
                    true_loss.append(r)
                elif filed_ok is False and arch_ok:
                    recoverable.append(r)

            if not healthy:
                problems.append((r, bucket))

        # ── Disposition report ───────────────────────────────────────────
        log.info("── Status counts ───────────────────────────")
        for s, n in sorted(status_counts.items()):
            log.info(f"  {s:<10} {n}")
        log.info("\n── Where the files physically landed ───────")
        for b, n in sorted(buckets.items(), key=lambda kv: -kv[1]):
            label = {
                "receipts": "filed in RECEIPTS (healthy)",
                "stranded_review": "STRANDED in review (staged, never filed)",
                "errored": "ERROR status",
                "archive_only": "archive only (not copied to a type folder)",
                "duplicate_folder": "moved to legacy Duplicate/ folder",
                "no_path": "no Dropbox path on row",
            }.get(b, b if not b.startswith("other_type:")
                   else f"filed under OTHER type → {b.split(':',1)[1]}")
            log.info(f"  {n:>5}  {label}")

        if problems:
            log.info("\n── Documents NOT cleanly in RECEIPTS ───────")
            for r, bucket in problems[:200]:
                log.info(f"  #{r.id:<6} {r.status:<8} {bucket:<22} "
                         f"{(r.vendor or '?')[:24]:<24} {r.original_filename}")
            if len(problems) > 200:
                log.info(f"  … and {len(problems) - 200} more")

        if args.verify_dropbox:
            log.info("\n── Dropbox existence check ─────────────────")
            log.info(f"  recoverable (filed copy gone, ARCHIVE present): {len(recoverable)}")
            for r in recoverable[:50]:
                log.info(f"    #{r.id} {r.original_filename} | archive: {r.source_archive_path}")
            log.info(f"  TRUE LOSS (filed AND archive both gone): {len(true_loss)}")
            for r in true_loss[:50]:
                log.info(f"    !! #{r.id} {r.original_filename} (status={r.status})")
            if not true_loss:
                log.info("  ✓ No permanently-lost documents detected on the DB side.")

        # ── Source-folder reconciliation ─────────────────────────────────
        if args.path:
            src_dir = namespace_relative(args.path)
            log.info(f"\n── Source backup reconciliation ────────────")
            log.info(f"  folder: {src_dir}")
            all_hashes = {r.file_hash for r in rows if r.file_hash}
            n_src = n_match = n_stranded = n_missing = 0
            missing = []
            for entry, is_doc in iter_files(dbx, src_dir, args.recursive, DEFAULT_EXTS):
                if not is_doc:
                    continue
                n_src += 1
                try:
                    meta, resp = dbx.files_download(entry.path_display or entry.path_lower)
                    h = hashlib.sha256(resp.content).hexdigest()
                except Exception as e:
                    log.warning(f"    could not read {entry.name}: {e}")
                    continue
                if h in hashes_done:
                    n_match += 1
                elif h in all_hashes:
                    n_stranded += 1   # processed, but not healthily filed in RECEIPTS
                else:
                    n_missing += 1
                    missing.append(entry.name)
            log.info(f"  originals in folder        : {n_src}")
            log.info(f"  → filed healthily in RECEIPTS: {n_match}")
            log.info(f"  → processed but stranded     : {n_stranded} "
                     f"(review/error/other-type — see above)")
            log.info(f"  → NEVER processed (no DB row): {n_missing}")
            for name in missing[:100]:
                log.info(f"      missing: {name}")
            if n_missing:
                log.info(f"\n  These {n_missing} originals never produced a DocUpload row. "
                         f"Re-run import_dropbox_batch.py --skip-existing to file just these.")


if __name__ == "__main__":
    sys.exit(main())
