"""Company ledger admin routes (2026-11-03).

The migration preview / run, the merge-log undo, the "claimed in two
projects" review page with its actions, the release-to-unclaimed tool and
the company QuickBooks account set. Every one of these is a card on
Admin → Maintenance (routes/admin_maintenance.py TOOLS) or a link from
Admin → QuickBooks — nobody types a URL. The data rules live in ledger.py.

Routes:
  GET  /admin/ledger/preview            pre-flight report (read-only JSON)
  POST /admin/ledger/run                the collapse (confirmed on the card)
  GET  /admin/ledger/merge-log.json     the merge log, newest first
  POST /admin/ledger/undo-merge         {log_id}  restore a removed row
  GET  /admin/ledger/conflicts          the review page (admin+)
  GET  /admin/ledger/conflicts.json     its data (?pid= narrows)
  POST /admin/ledger/conflicts/keep     {qbo_txn_id, qbo_txn_type,
                                         keep_project_id, loser_mode}
  POST /admin/ledger/conflicts/unmatch  {txn_id}  payee differs → unmatch
  POST /admin/ledger/conflicts/clear    {txn_id}  it's fine
  POST /admin/ledger/release            {txn_id}  release to unclaimed +
                                        unmatch receipt (any claimed row)
  GET  /admin/qbo-accounts              the company account set page
  GET  /admin/qbo-accounts.json
  POST /admin/qbo-accounts              {account_ids:[...]}

See routes/__init__.py for the module pattern (plain @app.route on the
same app object, imported at the bottom of app.py).
"""
import json
import logging

from flask import jsonify, render_template, request
from flask_login import current_user, login_required

from app import app, admin_required, admin_required_403, super_admin_required, _log_activity
from models import db, ProjectSheet, QBOConnection, TransactionMergeLog
import ledger


def _body():
    return request.get_json(silent=True) or request.form or {}


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ── Migration: preview / run ───────────────────────────────────────────────

@app.route("/admin/ledger/preview", methods=["GET"])
@login_required
@super_admin_required
def admin_ledger_preview():
    """The pre-flight report: counts, then every group with more than one
    row and what the run would keep, delete or flag. Touches nothing."""
    try:
        return jsonify(ledger.preflight(db))
    except Exception as e:
        logging.exception("[ledger] preview failed")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@app.route("/admin/ledger/run", methods=["POST"])
@login_required
@super_admin_required
def admin_ledger_run():
    """Collapse the per-project copies into one row per QuickBooks
    transaction. One transaction, marker + advisory lock; re-running finds
    nothing to do. The owner confirms on the Maintenance card after the
    preview — this never runs on its own at boot."""
    try:
        result = ledger.run_migration(user_id=getattr(current_user, 'id', None))
    except Exception as e:
        logging.exception("[ledger] migration failed")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500
    try:
        _log_activity(action='update', entity_type='transaction', entity_id=0,
                      entity_label='Company ledger migration',
                      after={k: v for k, v in result.items() if k != 'accounts'},
                      note=f"Company ledger: {result.get('summary', '')}")
    except Exception:
        pass
    return jsonify(result)


# ── Merge log + undo ───────────────────────────────────────────────────────

@app.route("/admin/ledger/merge-log.json", methods=["GET"])
@login_required
@super_admin_required
def admin_ledger_merge_log():
    rows = (TransactionMergeLog.query.order_by(TransactionMergeLog.id.desc())
            .limit(500).all())
    names = {p.id: p.name for p in ProjectSheet.query.all()}
    out = []
    for e in rows:
        try:
            d = json.loads(e.removed_row_json or '{}')
        except Exception:
            d = {}
        out.append({"id": e.id, "qbo_txn_id": e.qbo_txn_id, "qbo_txn_type": e.qbo_txn_type,
                    "kept_txn_id": e.kept_txn_id, "removed_txn_id": e.removed_txn_id,
                    "removed_project_id": e.removed_project_id,
                    "removed_project": names.get(e.removed_project_id),
                    "vendor": d.get("vendor"), "amount": d.get("amount"), "date": d.get("txn_date"),
                    "reason": e.reason,
                    "created_at": e.created_at.isoformat(timespec='minutes') if e.created_at else None,
                    "restored_at": e.restored_at.isoformat(timespec='minutes') if e.restored_at else None,
                    "restored_txn_id": e.restored_txn_id})
    return jsonify({"ok": True, "entries": out, "total": TransactionMergeLog.query.count()})


@app.route("/admin/ledger/undo-merge", methods=["POST"])
@login_required
@super_admin_required
def admin_ledger_undo_merge():
    log_id = _int(_body().get("log_id") or request.args.get("log_id"))
    if not log_id:
        return jsonify({"error": "log_id (a merge log entry id) is required"}), 400
    try:
        return jsonify(ledger.undo_merge(log_id))
    except ValueError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.exception("[ledger] undo failed")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── Review: claimed in two projects / payee differs ───────────────────────

@app.route("/admin/ledger/conflicts", methods=["GET"])
@login_required
@admin_required
def admin_ledger_conflicts():
    pid = _int(request.args.get("pid"))
    cards = ledger.conflicts(project_id=pid)
    projects = {p.id: p.name for p in ProjectSheet.query.order_by(ProjectSheet.name).all()}
    return render_template("admin_ledger_conflicts.html", cards=cards, pid=pid,
                           project_name=projects.get(pid), projects=projects)


@app.route("/admin/ledger/conflicts.json", methods=["GET"])
@login_required
@admin_required_403
def admin_ledger_conflicts_json():
    pid = _int(request.args.get("pid"))
    return jsonify({"ok": True, "cards": ledger.conflicts(project_id=pid)})


@app.route("/admin/ledger/conflicts/keep", methods=["POST"])
@login_required
@admin_required_403
def admin_ledger_conflict_keep():
    b = _body()
    keep_pid = _int(b.get("keep_project_id"))
    qid, qtype = (b.get("qbo_txn_id") or '').strip(), (b.get("qbo_txn_type") or '').strip()
    mode = (b.get("loser_mode") or 'receipt_to_place').strip()
    if not (keep_pid and qid and qtype):
        return jsonify({"error": "qbo_txn_id, qbo_txn_type and keep_project_id are required"}), 400
    try:
        return jsonify(ledger.resolve_keep(qid, qtype, keep_pid,
                                           user_id=getattr(current_user, 'id', None),
                                           loser_mode=mode))
    except ValueError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.exception("[ledger] keep failed")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@app.route("/admin/ledger/conflicts/unmatch", methods=["POST"])
@login_required
@admin_required_403
def admin_ledger_conflict_unmatch():
    tid = _int(_body().get("txn_id"))
    if not tid:
        return jsonify({"error": "txn_id is required"}), 400
    try:
        return jsonify(ledger.resolve_unmatch_receipt(tid, user_id=getattr(current_user, 'id', None)))
    except ValueError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.exception("[ledger] unmatch failed")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@app.route("/admin/ledger/conflicts/clear", methods=["POST"])
@login_required
@admin_required_403
def admin_ledger_conflict_clear():
    tid = _int(_body().get("txn_id"))
    if not tid:
        return jsonify({"error": "txn_id is required"}), 400
    try:
        return jsonify(ledger.resolve_clear(tid, user_id=getattr(current_user, 'id', None)))
    except ValueError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.exception("[ledger] clear failed")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@app.route("/admin/ledger/release", methods=["POST"])
@login_required
@admin_required_403
def admin_ledger_release():
    """Release to unclaimed + unmatch receipt, by transaction id (the
    Maintenance card and the row menu both post here)."""
    tid = _int(_body().get("txn_id") or request.args.get("txn_id"))
    if not tid:
        return jsonify({"error": "txn_id is required"}), 400
    try:
        return jsonify(ledger.release_row(tid, user_id=getattr(current_user, 'id', None)))
    except ValueError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.exception("[ledger] release failed")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── Company QuickBooks account set ─────────────────────────────────────────

def _accounts_payload():
    conn = QBOConnection.query.first()
    if not conn or not conn.refresh_token:
        return {"connected": False, "accounts": [], "enabled": [],
                "error": "QuickBooks not connected."}
    enabled = set()
    try:
        enabled = {str(x) for x in (json.loads(conn.enabled_account_ids or "[]") or [])}
    except Exception:
        enabled = set()
    from qbo_sync import list_qbo_accounts
    accts, err = [], None
    try:
        accts = list_qbo_accounts(conn, db)
        for a in accts:
            a["enabled"] = str(a["id"]) in enabled
        accts.sort(key=lambda a: (0 if a.get("type") == "Bank" else 1, (a.get("name") or "").lower()))
    except Exception as e:
        logging.exception("[qbo] list_qbo_accounts failed")
        err = f"Could not list accounts: {e}"
    return {"connected": True, "accounts": accts, "enabled": sorted(enabled), "error": err,
            "sync_through": conn.sync_through,
            "last_synced": conn.last_synced.isoformat(timespec='minutes') if conn.last_synced else None,
            "last_cdc_sync": conn.last_cdc_sync.isoformat(timespec='minutes') if conn.last_cdc_sync else None}


@app.route("/admin/qbo-accounts", methods=["GET"])
@login_required
@admin_required
def admin_qbo_accounts():
    return render_template("admin_qbo_accounts.html", d=_accounts_payload())


@app.route("/admin/qbo-accounts.json", methods=["GET"])
@login_required
@admin_required_403
def admin_qbo_accounts_json():
    return jsonify({"ok": True, **_accounts_payload()})


@app.route("/admin/qbo-accounts", methods=["POST"])
@login_required
@admin_required_403
def admin_qbo_accounts_save():
    """Save the company account set: {"account_ids": ["1","42",...]}. Every
    account checked here is pulled into the pool by the company sync."""
    conn = QBOConnection.query.first()
    if not conn:
        return jsonify({"error": "QuickBooks not connected."}), 400
    data = request.get_json(force=True) or {}
    ids = data.get("account_ids") or []
    if not isinstance(ids, list):
        return jsonify({"error": "account_ids must be a list"}), 400
    cleaned = [str(x) for x in ids if x not in (None, "", "0", 0)]
    before = conn.enabled_account_ids or '[]'
    conn.enabled_account_ids = json.dumps(cleaned)
    db.session.commit()
    try:
        _log_activity(action='update', entity_type='qbo_accounts', entity_id=0,
                      entity_label='Company QuickBooks accounts',
                      before={'enabled_account_ids': before},
                      after={'enabled_account_ids': conn.enabled_account_ids},
                      note=f'Company QuickBooks account set: {len(cleaned)} account'
                           f'{"s" if len(cleaned) != 1 else ""}')
    except Exception:
        pass
    return jsonify({"ok": True, "count": len(cleaned)})
