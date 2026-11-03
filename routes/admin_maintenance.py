"""Admin → Maintenance (Cleanup E, 2026-11-02).

ONE page that gives every company-wide maintenance / diagnostic tool a
home: a card with a one-line description, the last-run time when the tool
logs one (system_task_log), and a Run button (confirmed when destructive).
The tools themselves are the routes that already existed; nothing here
re-implements them. Nobody should ever have to type a /admin/... URL.

Routes (super-admin only):
  GET /admin/maintenance        the page
  GET /admin/maintenance.json   the tool registry + last-run times (for the
                                page's JS and for a quick health look)

Also exported for the Admin hub (app.py admin_panel imports it lazily):
  company_status()  → QuickBooks / Dropbox / Veryfi / AI / cron status dict

See routes/__init__.py for the module pattern (plain @app.route on the
same app object, imported at the bottom of app.py).
"""
import os
from datetime import datetime

from flask import jsonify, render_template, request
from flask_login import login_required
from sqlalchemy import text

from app import app, super_admin_required
from models import db, ProjectSheet, QBOConnection


# ── Tool registry ──────────────────────────────────────────────────────────
# Each entry is one card. Keys:
#   id        stable slug (also the card's DOM id)
#   title / desc
#   group     section heading on the page
#   method    GET | POST   (the HTTP method the Run button uses)
#   url       the tool's route; "{pid}" / "{bid}" / "{q}" placeholders are
#             filled from the card's inputs
#   params    inputs the card shows: (name, label, kind) with kind in
#             project | budget | text | int
#   open      "inline" (fetch, show the response under the card) or
#             "tab" (open the URL in a new browser tab — HTML / download)
#   preview   optional {method, query} for a dry-run button shown first
#   apply     optional query string added on the confirmed run (e.g. apply=1)
#   confirm   confirmation text → the run is destructive; shown red
#   task      system_task_log task_name whose last_run_at is shown
#   note      small grey line under the description
TOOLS = [
    # ── Documents & Dropbox ────────────────────────────────────────────
    {"id": "dropbox-status", "group": "Documents & Dropbox",
     "title": "Dropbox connection", "method": "GET", "url": "/admin/dropbox/status",
     "open": "inline",
     "desc": "Checks the Dropbox credentials and that the project template folder exists."},
    {"id": "dbx-ls", "group": "Documents & Dropbox",
     "title": "List the Dropbox operations root", "method": "GET", "url": "/admin/dbx-ls",
     "open": "inline",
     "desc": "Shows the folders at the operations root and the namespace ids the app resolved."},
    {"id": "drop-sweep-status", "group": "Documents & Dropbox",
     "title": "Drop-folder sweep status", "method": "GET", "url": "/admin/drop-sweep-status",
     "open": "tab", "task": "drop_folder_sweep",
     "desc": "When the background sweeper last ran and, per active project, whether its _INBOX exists. Add ?provision=1 on that page to create missing folders now."},
    {"id": "import-dropbox", "group": "Documents & Dropbox",
     "title": "Scan Dropbox for project folders", "method": "FORM", "url": "/admin/import-dropbox",
     "confirm": "Scan the Dropbox operations root and import any project folders the app does not know yet?",
     "desc": "Imports a project for every folder at the operations root that has no project yet. Existing projects are left alone."},
    {"id": "docs-find", "group": "Documents & Dropbox",
     "title": "Find a document", "method": "GET", "url": "/admin/docs/find",
     "open": "inline", "params": [("q", "Filename or vendor fragment", "text"), ("pid", "Project (optional)", "project_opt")],
     "desc": "Where did a file land? Searches every project by filename or vendor and shows status, duplicates and links."},
    {"id": "docs-cleanup-errors", "group": "Documents & Dropbox",
     "title": "Clean up errored document rows", "method": "POST", "url": "/admin/docs/cleanup-errors",
     "open": "inline", "params": [("pid", "Project (optional)", "project_opt")],
     "preview": {"method": "GET", "label": "Preview"}, "apply": "apply=1",
     "confirm": "Move the retry-spam error rows to Trash? Rows are soft-deleted and restorable.",
     "desc": "Trashes failed-OCR rows that also have a healthy copy, and keeps only the newest of repeated failures for the same file."},
    {"id": "reprocess-unpaired", "group": "Documents & Dropbox",
     "title": "Reprocess unpaired receipts", "method": "POST",
     "url": "/admin/docs/project/{pid}/reprocess-unpaired",
     "open": "inline", "params": [("pid", "Project", "project")],
     "confirm": "Re-OCR every receipt on this project that is not paired to a bank charge? This uses Veryfi credits.",
     "desc": "The same job the 4 pm cron kicks off (/cron/reprocess-unpaired with the X-Cron-Token header): re-OCR, then AI clean-up and the double-coded scan. Runs in the background."},
    {"id": "docs-wipe", "group": "Documents & Dropbox",
     "title": "Wipe all receipts on a project (test)", "method": "POST",
     "url": "/admin/docs/project/{pid}/wipe",
     "open": "inline", "params": [("pid", "Project", "project")],
     "confirm": "Wipe EVERY document on this project?\n\nDocument rows and their own receipt-born ledger rows are deleted. Bank / QuickBooks / manual charges are kept and unlinked. Filed Dropbox files move to /_TRASH/.\n\nThis is a testing tool.",
     "confirm2": "Really sure? Only THIS project's documents are affected.",
     "desc": "Clean-start testing tool. Deletes a project's document rows, keeps and unlinks its bank charges, moves the files to Dropbox trash."},
    {"id": "trash-purge", "group": "Documents & Dropbox",
     "title": "Purge Dropbox trash", "method": "POST", "url": "/admin/trash/purge",
     "open": "inline", "task": "trash_purge",
     "confirm": "Permanently delete /_TRASH/ folders older than 30 days? This cannot be undone.",
     "desc": "Hard-deletes /_TRASH/ day folders older than 30 days. Also runs daily on its own."},

    # ── Ledger & QuickBooks ────────────────────────────────────────────
    {"id": "backup-transactions", "group": "Ledger & QuickBooks",
     "title": "Download a transactions backup", "method": "GET",
     "url": "/admin/backup/transactions.json", "open": "tab",
     "desc": "Every transaction's coding fields as JSON. Take one before a repair so coding can be diffed and restored."},
    {"id": "coa-audit", "group": "Ledger & QuickBooks",
     "title": "Chart-of-accounts audit", "method": "POST", "url": "/admin/coa-audit",
     "open": "inline", "params": [("project", "Project (optional)", "project_opt")],
     "preview": {"method": "GET", "label": "Report"}, "apply": "fix=txn_resync&apply=1",
     "confirm": "Re-sync every coded transaction's account code and name from its budget line? Take a transactions backup first.",
     "desc": "Finds transactions whose account code or name drifted from their budget line after the 2026-04 renumber; the fix re-syncs them."},
    {"id": "qbo-repair-refunds", "group": "Ledger & QuickBooks",
     "title": "Repair QuickBooks refunds", "method": "POST",
     "url": "/admin/qbo-imports/repair-refunds",
     "open": "inline", "params": [("project", "Project", "project")],
     "preview": {"method": "POST", "label": "Preview"}, "apply": "apply=1",
     "confirm": "Flip is_expense on the refunds and bill payments this preview listed?",
     "desc": "Pre-June-2026 imports that stored refunds as positive expenses. Only the expense flag is touched."},
    {"id": "qbo-purge", "group": "Ledger & QuickBooks",
     "title": "Purge QuickBooks imports for a project", "method": "POST",
     "url": "/admin/qbo-imports/purge",
     "open": "inline", "params": [("project", "Project", "project")],
     "preview": {"method": "POST", "label": "Preview"}, "apply": "apply=1",
     "confirm": "Delete every QuickBooks-imported transaction on this project? Receipts, typed rows and splits are kept. This erases the bank ledger for the project.",
     "desc": "Undo electronic imports for one project so the next sync starts clean. One project at a time."},
    {"id": "cleanup-estimate-txns", "group": "Ledger & QuickBooks",
     "title": "Non-ledger document rows on the ledger", "method": "GET",
     "url": "/admin/cleanup-estimate-txns", "open": "tab",
     "desc": "Lists transactions born from estimates, POs and other non-ledger documents, with a delete step on that page."},
    {"id": "claim-backfill", "group": "Ledger & QuickBooks",
     "title": "Cross-project claim backfill", "method": "POST",
     "url": "/admin/cross-project-claim-backfill",
     "open": "inline",
     "preview": {"method": "GET", "label": "Preview"},
     "confirm": "Mark every sibling charge the fuzzy matcher finds as claimed by its coded project? Individual rows can be unclaimed by re-saving them.",
     "desc": "Walks every coded transaction through the cross-project claim rule so the same expense stops showing in two projects."},
    {"id": "vendors-audit", "group": "Ledger & QuickBooks",
     "title": "Ghost vendor audit", "method": "GET", "url": "/vendors/audit",
     "open": "inline",
     "desc": "Read-only: vendor payees nothing references — safe-to-delete candidates. Nothing is deleted here."},

    # ── Budgets & catalog ──────────────────────────────────────────────
    {"id": "catalog-stats", "group": "Budgets & catalog",
     "title": "Catalog statistics", "method": "GET", "url": "/admin/catalog/stats",
     "open": "inline",
     "desc": "Counts per account code in the Quick Entry catalog and flags any legacy codes."},
    {"id": "sub-budget-status", "group": "Budgets & catalog",
     "title": "Sub-budget status", "method": "GET", "url": "/admin/sub-budget-status",
     "open": "tab", "params": [("pid", "Project (optional)", "project_opt")],
     "desc": "Per project, every sub-budget with its memberships and which budget each member line lives on."},
    {"id": "meal-status", "group": "Budgets & catalog",
     "title": "Meal lines status", "method": "GET", "url": "/admin/meal-status",
     "open": "inline", "params": [("project", "Project", "project")],
     "desc": "Per date: production-day flags, headcounts and what the meal auto-lines currently hold."},
    {"id": "crew-lookup", "group": "Budgets & catalog",
     "title": "Crew lookup", "method": "GET", "url": "/admin/crew-lookup",
     "open": "tab", "params": [("q", "Name", "text")],
     "desc": "Why a person is missing from the crew pickers: every matching crew and support row with the fields that gate visibility."},
    {"id": "debug-line", "group": "Budgets & catalog",
     "title": "Line math trace", "method": "GET", "url": "/admin/debug/line/{bid}",
     "open": "inline", "params": [("bid", "Budget id", "int"), ("description", "Description contains (optional)", "text_opt")],
     "desc": "Re-runs the calc engine on a budget's lines and shows each input and result."},
    {"id": "debug-budget", "group": "Budgets & catalog",
     "title": "Budget calc trace", "method": "GET", "url": "/admin/debug/budget/{bid}",
     "open": "tab", "params": [("bid", "Budget id", "int")],
     "desc": "The whole budget's calculation, line by line, as a page."},
    {"id": "split-labor", "group": "Budgets & catalog",
     "title": "Split multi-quantity labor lines", "method": "POST",
     "url": "/admin/migrate/split-labor", "open": "inline",
     "preview": {"method": "GET", "label": "Preview", "url": "/admin/migrate/split-labor/preview"},
     "confirm": "Split every labor line with quantity above 1 into named lines (A / B / C)? Already-split lines are skipped.",
     "desc": "Finds labor lines with quantity above 1 and splits them into independent named lines. Safe to re-run."},
    {"id": "resync-all", "group": "Budgets & catalog",
     "title": "Resync schedule-driven lines everywhere", "method": "POST",
     "url": "/admin/migrate/resync-all", "open": "inline",
     "confirm": "Rebuild the meals, flights, hotel, mileage, per diem, working meals and craft services lines on EVERY budget from its schedule? Safe to run; idempotent.",
     "desc": "Re-runs the schedule sync for every budget in the system."},
]


def _last_runs():
    """{task_name: (last_run_at, last_result)} from system_task_log — the
    table is created lazily by the boot tasks, so a missing table is just
    'no runs yet'."""
    out = {}
    try:
        rows = db.session.execute(text(
            "SELECT task_name, last_run_at, last_result FROM system_task_log")).fetchall()
        for name, at, res in rows:
            out[name] = (at, res)
    except Exception:
        db.session.rollback()
    return out


def _fmt_dt(d):
    if not d:
        return ""
    if isinstance(d, str):
        return d[:16].replace("T", " ")
    return f"{d.strftime('%b')} {d.day}, {d.year} {d.strftime('%H:%M')} UTC"


def company_status():
    """Status chips for the Admin hub's Company section. Cheap: reads env
    and the DB only — no network call (the Test buttons do those)."""
    conn = None
    try:
        conn = QBOConnection.query.first()
    except Exception:
        db.session.rollback()
    last_sync = None
    try:
        last_sync = (db.session.query(db.func.max(ProjectSheet.last_synced)).scalar())
    except Exception:
        db.session.rollback()
    qbo = {
        "connected": bool(conn and conn.refresh_token),
        "realm_id": (conn.realm_id if conn else None),
        "token_expiry": _fmt_dt(conn.token_expiry) if conn and conn.token_expiry else "",
        "token_expired": bool(conn and conn.token_expiry and conn.token_expiry < datetime.utcnow()),
        "last_sync": _fmt_dt(last_sync),
        "updated_at": _fmt_dt(conn.updated_at) if conn and conn.updated_at else "",
        "env_ok": bool(os.getenv("QBO_CLIENT_ID") and os.getenv("QBO_CLIENT_SECRET")),
    }
    dropbox = {
        "configured": bool((os.getenv("DROPBOX_REFRESH_TOKEN") and os.getenv("DROPBOX_APP_KEY"))
                           or os.getenv("DROPBOX_ACCESS_TOKEN")),
        "ops_path": os.getenv("DROPBOX_OPERATIONS_PATH") or "",
        "namespace": bool(os.getenv("DROPBOX_NAMESPACE_ID")),
    }
    veryfi = {
        "configured": bool(os.getenv("VERYFI_CLIENT_ID") and os.getenv("VERYFI_CLIENT_SECRET")
                           and os.getenv("VERYFI_USERNAME") and os.getenv("VERYFI_API_KEY")),
    }
    ai = {"configured": bool(os.getenv("ANTHROPIC_API_KEY"))}
    cron = {"configured": bool(os.getenv("CRON_TOKEN"))}
    return {"qbo": qbo, "dropbox": dropbox, "veryfi": veryfi, "ai": ai, "cron": cron}


def _tools_payload():
    runs = _last_runs()
    out = []
    for t in TOOLS:
        d = dict(t)
        d["params"] = [{"name": n, "label": l, "kind": k} for (n, l, k) in t.get("params", [])]
        d["destructive"] = bool(t.get("confirm"))
        lr = runs.get(t.get("task") or "")
        d["last_run"] = _fmt_dt(lr[0]) if lr else ""
        d["last_result"] = (lr[1] or "") if lr else ""
        out.append(d)
    return out


@app.route("/admin/maintenance", methods=["GET"])
@login_required
@super_admin_required
def admin_maintenance():
    projects = (ProjectSheet.query.filter(ProjectSheet.status != 'archived')
                .order_by(ProjectSheet.name).all())
    tools = _tools_payload()
    groups = []
    for t in tools:
        if not groups or groups[-1][0] != t["group"]:
            groups.append((t["group"], []))
        groups[-1][1].append(t)
    return render_template("admin_maintenance.html", groups=groups, tools=tools,
                           projects=projects, status=company_status())


@app.route("/admin/maintenance.json", methods=["GET"])
@login_required
@super_admin_required
def admin_maintenance_json():
    return jsonify({"tools": _tools_payload(), "status": company_status(),
                    "generated_at": datetime.utcnow().isoformat() + "Z"})
