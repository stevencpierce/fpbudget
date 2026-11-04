"""Admin → Maintenance → "Re-link documents to Dropbox" (2026-11-04).

A document row remembers where its file is in Dropbox (DocUpload
.filed_dropbox_path, and the _SOURCE_ARCHIVE copy in source_archive_path).
When the file is no longer there — a project folder renamed before the
rename rewrote paths (project 307, 2026-11-03), a file moved by hand — every
preview fails. This tool finds the file again and updates the ROW; it never
moves, renames or creates anything in Dropbox.

  GET  /admin/docs/relink[?project=<pid>]          Preview: every document
       whose recorded path is not in Dropbox, with the proposed new path
  POST /admin/docs/relink?project=<pid>&apply=1    Fix: write the proposed
       paths (one activity_log row per document, before = the old path, so
       the project's activity log can Undo it)

How a file is found (ONE rule, `propose_relink`):
  1. slug remap — the stored path is under a slug that is not the project's
     current dropbox_folder (a rename) and the same relative path exists
     under the current folder;
  2. else a filename search inside the project's current folder: exactly
     one file with that basename → proposed; several → "needs a person",
     listed with the candidates; none → "needs a person".
Existence is checked against ONE recursive listing of the project's folder
(one Dropbox call per project, paged) instead of one files_get_metadata per
document — exact, and unlike files_search_v2 it is not behind the search
index, which lags right after a folder move. files_get_metadata is used,
throttled, only for a path OUTSIDE the project's current folder (the old
slug) — and once per old root first, so a folder that is gone costs one
call, not one per document.

Same module pattern as every routes/ slice: @app.route on the one app
object, imported at the bottom of app.py.
"""
import logging
import time
from datetime import datetime

from flask import jsonify, request
from flask_login import login_required

from app import (app, maintenance_required, _log_activity, _dbx_client,
                 _project_dbx_root, _dbx_is_path_error, _PROJECT_DBX_PATH_COLUMNS,
                 _DBX_NAMESPACE_ID, _DBX_OPS_ROOT)
from models import db, DocUpload, ProjectSheet


_META_SLEEP_S = 0.1          # between files_get_metadata calls (rate limit)


def _norm(path):
    """Stored paths from the pre-namespace era carry the ops-root prefix;
    in namespace mode the API wants them root-relative (the same rule the
    preview-link / raw routes apply)."""
    if not path:
        return path
    ops = (_DBX_OPS_ROOT or "").rstrip("/") if _DBX_NAMESPACE_ID else ""
    if ops and path.startswith(ops + "/"):
        return path[len(ops):]
    return path


def _split_slug(norm_path):
    """'/<slug>/rest' (namespace) or '<ops root>/<slug>/rest' → (slug, rest);
    (None, None) when the path is not under a project folder."""
    if not norm_path:
        return None, None
    p = norm_path
    if not _DBX_NAMESPACE_ID:
        ops = (_DBX_OPS_ROOT or "").rstrip("/")
        if not ops or not p.lower().startswith(ops.lower() + "/"):
            return None, None
        p = p[len(ops):]
    parts = p.strip("/").split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None, None
    return parts[0], parts[1]


def list_project_files(dbx, root):
    """{path_lower: path_display} of every FILE under `root`, recursive and
    paged. None when the root itself is not in Dropbox."""
    import dropbox as _dbx_mod
    try:
        res = dbx.files_list_folder(root, recursive=True)
    except Exception as e:
        if _dbx_is_path_error(e):
            return None
        raise
    entries = list(res.entries)
    while getattr(res, "has_more", False):
        res = dbx.files_list_folder_continue(res.cursor)
        entries.extend(res.entries)
    out = {}
    for e in entries:
        if isinstance(e, _dbx_mod.files.FileMetadata):
            out[(e.path_lower or e.path_display or "").lower()] = e.path_display or e.path_lower
    return out


class _Exists:
    """files_get_metadata with a per-root short-circuit and a throttle, for
    paths outside the project's current folder."""

    def __init__(self, dbx):
        self.dbx = dbx
        self.root_ok = {}      # '/<old slug>' → bool
        self.calls = 0

    def _meta(self, path):
        for attempt in (1, 2):
            try:
                self.calls += 1
                self.dbx.files_get_metadata(path)
                time.sleep(_META_SLEEP_S)
                return True
            except Exception as e:
                if _dbx_is_path_error(e):
                    return False
                backoff = getattr(e, "backoff", None)
                if attempt == 1 and backoff:
                    time.sleep(min(float(backoff), 10.0))
                    continue
                raise

    def __call__(self, norm_path):
        slug, _rest = _split_slug(norm_path)
        if slug:
            root = _project_dbx_root(slug)
            if root not in self.root_ok:
                self.root_ok[root] = self._meta(root)
            if not self.root_ok[root]:
                return False
        return self._meta(norm_path)


def propose_relink(doc, field, cur_slug, root, files):
    """ONE rule for where a missing file went. Returns a row dict with
    new_path (or None), method ('slug' | 'search' | None), reason and
    candidates."""
    old = getattr(doc, field, None)
    norm = _norm(old)
    row = {"doc_id": doc.id, "project_id": doc.project_id, "field": field,
           "filename": doc.filed_filename or doc.original_filename,
           "old_path": old, "new_path": None, "method": None,
           "reason": None, "candidates": []}
    if files is None:
        row["reason"] = f"the project folder {root} is not in Dropbox"
        return row
    slug, rest = _split_slug(norm)
    if slug and rest and slug.lower() != (cur_slug or "").lower():
        cand = f"{root}/{rest}"
        if cand.lower() in files:
            row.update(new_path=files[cand.lower()], method="slug",
                       reason=f"folder renamed {slug} → {cur_slug}")
            return row
    base = (norm or "").rsplit("/", 1)[-1].lower()
    hits = [disp for low, disp in files.items() if low.rsplit("/", 1)[-1] == base] if base else []
    if len(hits) == 1:
        row.update(new_path=hits[0], method="search",
                   reason="one file with this name in the project folder")
    elif len(hits) > 1:
        row.update(candidates=sorted(hits)[:20],
                   reason=f"{len(hits)} files with this name in the project folder — needs a person")
    else:
        row["reason"] = "no file with this name in the project folder — needs a person"
    return row


def scan(pid=None, dbx=None):
    """The preview: every live document whose recorded path(s) are not in
    Dropbox, with the proposal. Scoped to one project or every project."""
    dbx = dbx or _dbx_client()
    q = DocUpload.query.filter(DocUpload.status != "deleted")
    q = q.filter(db.or_(DocUpload.filed_dropbox_path.isnot(None),
                        DocUpload.source_archive_path.isnot(None)))
    if pid:
        q = q.filter(DocUpload.project_id == pid)
    docs = q.order_by(DocUpload.project_id, DocUpload.id).all()
    by_project = {}
    skipped = {"no_project": 0}
    for d in docs:
        if not d.project_id:
            skipped["no_project"] += 1
            continue
        by_project.setdefault(d.project_id, []).append(d)
    projects = {p.id: p for p in ProjectSheet.query.filter(
        ProjectSheet.id.in_(list(by_project.keys()) or [0])).all()}
    exists = _Exists(dbx)
    rows, checked, ok_count = [], 0, 0
    project_notes = []
    for proj_id, pdocs in by_project.items():
        p = projects.get(proj_id)
        if not p or not p.dropbox_folder:
            skipped["no_project"] += len(pdocs)
            continue
        cur_slug = p.dropbox_folder.strip("/")
        root = _project_dbx_root(cur_slug)
        files = list_project_files(dbx, root)
        if files is None:
            project_notes.append({"project_id": p.id, "project": p.name,
                                  "note": f"project folder {root} is not in Dropbox"})
        root_l = root.lower() + "/"
        for d in pdocs:
            for field in _PROJECT_DBX_PATH_COLUMNS:
                old = getattr(d, field, None)
                if not old:
                    continue
                checked += 1
                norm = _norm(old)
                if files is not None and norm.lower().startswith(root_l):
                    present = norm.lower() in files
                else:
                    present = exists(norm)
                if present:
                    ok_count += 1
                    continue
                row = propose_relink(d, field, cur_slug, root, files)
                row["project"] = p.name
                rows.append(row)
    fixable = sum(1 for r in rows if r["new_path"])
    return {"ok": True, "dry_run": True,
            "scope": {"project": pid, "projects": len(by_project)},
            "checked": checked, "present": ok_count, "missing": len(rows),
            "fixable": fixable, "needs_person": len(rows) - fixable,
            "metadata_calls": exists.calls,
            "skipped": skipped, "project_notes": project_notes, "rows": rows,
            "fix": ("Run updates the recorded path of every row with a new_path "
                    "(logged per document; undoable from the project's activity "
                    "log). Nothing moves in Dropbox.")}


def apply_fixes(preview):
    """Write the proposed paths. One commit + one activity row per document
    field, `before` = the old path. Rows whose stored path changed since the
    preview are skipped (stale)."""
    fixed, stale = [], []
    for r in preview["rows"]:
        if not r.get("new_path"):
            continue
        d = DocUpload.query.get(r["doc_id"])
        if not d or getattr(d, r["field"], None) != r["old_path"]:
            stale.append(r["doc_id"])
            continue
        setattr(d, r["field"], r["new_path"][:500])
        db.session.commit()
        act = _log_activity(action="update", entity_type="doc_upload", entity_id=d.id,
                            entity_label=(r.get("filename") or f"Document #{d.id}")[:300],
                            project_id=d.project_id,
                            before={r["field"]: r["old_path"]},
                            after={r["field"]: r["new_path"], "relink_method": r["method"]},
                            note=(f"Re-linked to Dropbox ({r['method']}): "
                                  f"{r['field']} {r['old_path']} → {r['new_path']}"))
        fixed.append({"doc_id": d.id, "field": r["field"], "old_path": r["old_path"],
                      "new_path": r["new_path"], "method": r["method"],
                      "activity_id": (act.id if act else None)})
    return fixed, stale


@app.route("/admin/docs/relink", methods=["GET", "POST"])
@login_required
@maintenance_required
def admin_docs_relink():
    try:
        pid = int(request.args.get("project") or request.args.get("pid") or 0) or None
    except (TypeError, ValueError):
        pid = None
    try:
        preview = scan(pid)
    except Exception as e:
        logging.exception("[docs/relink] scan failed")
        db.session.rollback()
        return jsonify({"ok": False, "error": f"Dropbox scan failed: {type(e).__name__}: {e}"}), 502
    if request.method == "GET" or str(request.args.get("apply") or "") != "1":
        return jsonify(preview)
    fixed, stale = apply_fixes(preview)
    needs_person = [r for r in preview["rows"] if not r.get("new_path")]
    return jsonify({"ok": True, "dry_run": False, "scope": preview["scope"],
                    "fixed": len(fixed), "rows": fixed, "stale": stale,
                    "needs_person": len(needs_person), "needs_person_rows": needs_person,
                    "finished_at": datetime.utcnow().isoformat() + "Z"})
