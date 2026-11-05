"""Admin → Maintenance → "Coded charges hidden under Not project" (2026-11-05).

The invariant: a row project P holds with a line, a section or a split is
never under P's Not project. Live on 2026-11-05 it was: GrubHub's $20,000
and $9,600 split BillPayments carried a transaction_pool_dismissal the
company-ledger migration had written for the project that KEPT them, so
they sat under Not project — off the Assigned list and off the line
rollups ($0 of Director fees). This tool is the company-wide check:

  GET  /admin/actuals/hidden-coded          the page (or ?format=json):
       • coded — rows coded by the holding project that also carry its
         dismissal or an exclusion flag. Fix (per row, or all) is
         actuals.unhide_coded — the dismissal goes, the flag clears, the
         row is back in Assigned; logged per row, undoable from the
         activity log (an undo that would hide a still-coded row is
         refused).
       • receipt_only — rows the holding project dismissed that carry
         only a receipt. Receipt + Not project is a legitimate state (a
         personal charge with its receipt, excluded), so these get no
         automatic action: listed per project with Restore per row (the
         project's own /recover route).
  POST /admin/actuals/hidden-coded/fix      {transaction_id} | {all: true,
                                            project?}

Module pattern: routes/__init__.py (plain @app.route on the same app,
imported at the bottom of app.py).
"""
import logging

from flask import jsonify, render_template, request
from flask_login import current_user, login_required

from app import app, maintenance_required, _log_activity
from models import db, Budget, BudgetLine, DocUpload, ProjectSheet, Transaction, TransactionSplit
from actuals import hidden_coded_rows, dismissed_receipt_only_rows, unhide_coded, claim_signals
from budget_calc import FP_COA_NAMES


def _undo_budget_id(pid):
    """activity_undo is budget-scoped in its URL; any budget of the project
    works (transaction entries carry project_id only)."""
    b = Budget.query.filter(Budget.project_id == pid).order_by(Budget.id.desc()).first()
    return b.id if b else None


def _line_label(lid):
    ln = BudgetLine.query.get(lid) if lid else None
    if ln is None:
        return f"line #{lid}" if lid else None
    return f"{ln.account_code} · {(ln.description or ln.account_name or 'untitled')[:60]}"


def _section_label(code):
    return f"{code} · {FP_COA_NAMES.get(code, '')}".strip(' ·') if code else None


def _coding_text(t):
    """What the project coded the row to — the line, the section, or
    the split pieces."""
    if t.budget_line_id:
        return _line_label(t.budget_line_id)
    if t.account_code:
        return _section_label(t.account_code)
    pieces = TransactionSplit.query.filter_by(transaction_id=t.id).order_by(TransactionSplit.id).all()
    if pieces:
        return 'Split: ' + ', '.join(
            f"{_line_label(sp.budget_line_id) if sp.budget_line_id else _section_label(sp.account_code)} "
            f"${float(sp.amount or 0):,.2f}" for sp in pieces)
    return None


def _row(t, d, names, kind):
    s = claim_signals(t)
    doc = DocUpload.query.get(t.doc_upload_id) if t.doc_upload_id else None
    return {
        "id": t.id, "project_id": t.project_id, "project": names.get(t.project_id, f"#{t.project_id}"),
        "vendor": t.vendor or '—', "amount": float(t.amount or 0), "date": (t.txn_date or '')[:10],
        "qbo": (f"{t.qbo_txn_type}/{t.qbo_txn_id}" if t.qbo_txn_id else None),
        "source": t.source,
        "signals": [k for k, v in s.items() if v],
        "coding": _coding_text(t) if kind == 'coded' else None,
        "receipt": ((doc.filed_filename or doc.original_filename) if doc is not None else None),
        "receipt_doc_id": (doc.id if doc is not None else None),
        "dismissed": d is not None,
        "dismissal_reason": (d.reason if d is not None else None),
        "dismissed_at": (d.created_at.isoformat(timespec='minutes') if (d is not None and d.created_at) else None),
        "flagged": bool(t.not_project_expense),
        "flag_reason": t.excluded_reason,
        "undo_bid": _undo_budget_id(t.project_id),
        "restore_url": f"/projects/{t.project_id}/actuals/transaction/{t.id}/recover",
        "actuals_url": f"/projects/{t.project_id}/actuals?txn={t.id}",
    }


def _preview(pid):
    names = {p.id: p.name for p in ProjectSheet.query.all()}
    coded = [_row(t, d, names, 'coded') for t, d in hidden_coded_rows(pid)]
    receipt_only = [_row(t, d, names, 'receipt') for t, d in dismissed_receipt_only_rows(pid)]
    return coded, receipt_only, names


@app.route("/admin/actuals/hidden-coded", methods=["GET"])
@login_required
@maintenance_required
def admin_hidden_coded():
    """Preview: the coded-and-hidden rows (fixable) and the receipt-only
    dismissed rows (review only). HTML, or JSON with ?format=json."""
    try:
        pid = int(request.args.get('project') or request.args.get('pid') or 0) or None
    except (TypeError, ValueError):
        pid = None
    coded, receipt_only, names = _preview(pid)
    if (request.args.get('format') or '') == 'json' or 'application/json' in (request.headers.get('Accept') or ''):
        return jsonify({"ok": True, "dry_run": True, "project_id": pid,
                        "count": len(coded), "coded": coded,
                        "receipt_only_count": len(receipt_only), "receipt_only": receipt_only,
                        "fix": "Fix deletes the project's dismissal and clears the exclusion flag on each "
                               "coded row (it returns to Assigned and counts on its line). Receipt-only rows "
                               "are listed for review only — Restore per row."})
    return render_template("admin_hidden_coded.html", coded=coded, receipt_only=receipt_only,
                           names=names, pid=pid)


@app.route("/admin/actuals/hidden-coded/fix", methods=["POST"])
@login_required
@maintenance_required
def admin_hidden_coded_fix():
    """Fix one coded-and-hidden row ({transaction_id}) or every one listed
    ({all: true, project?}). Each row: actuals.unhide_coded, committed,
    logged with its before-state (undoable; an undo that would hide a
    still-coded row is refused by the invariant)."""
    body = request.get_json(silent=True) or {}
    try:
        pid = int(body.get('project') or 0) or None
    except (TypeError, ValueError):
        pid = None
    targets = []
    if body.get('all'):
        targets = [t for t, _d in hidden_coded_rows(pid)]
    else:
        try:
            tid = int(body.get('transaction_id'))
        except (TypeError, ValueError):
            return jsonify({"error": "transaction_id required (or all: true)"}), 400
        t = Transaction.query.get_or_404(tid)
        live = [x for x, _d in hidden_coded_rows(t.project_id) if x.id == tid]
        if not live:
            return jsonify({"error": "This charge is no longer hidden (refresh the list)."}), 409
        targets = live
    fixed = []
    for t in targets:
        pid_t = t.project_id
        try:
            before = unhide_coded(t, pid_t)
            if before is None:
                continue
            db.session.commit()
        except Exception as ex:
            db.session.rollback()
            logging.warning(f"[hidden-coded] fix of txn {t.id} failed: {ex}")
            fixed.append({"id": t.id, "error": str(ex)})
            continue
        act = _log_activity(
            action='update', entity_type='transaction', entity_id=t.id,
            entity_label=(t.vendor or f'Txn #{t.id}')[:80], project_id=pid_t,
            before=before,
            after={'pool_dismissed': False, 'not_project_expense': False,
                   'excluded_at': None, 'excluded_by_user_id': None, 'excluded_reason': None},
            note=(f"Coded charge was hidden under Not project (Maintenance fix by "
                  f"{getattr(current_user, 'name', None) or getattr(current_user, 'email', 'admin')}) — "
                  f"back in Assigned; ${float(t.amount or 0):,.2f} counts on its line again")[:500])
        fixed.append({"id": t.id, "project_id": pid_t, "activity_id": (act.id if act else None),
                      "undo_bid": _undo_budget_id(pid_t)})
    return jsonify({"ok": True, "dry_run": False, "fixed": len([f for f in fixed if 'error' not in f]),
                    "rows": fixed})
