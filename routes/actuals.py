"""Project Actuals routes — M1b-2 slice of the app.py split (ACTUALS_HANDOFF
Phase 2, 2026-10-30).

Every /projects/<pid>/actuals/... route — the Actuals workspace page and its
workspace.json — plus the private helpers only they use
(_guard_doc_expense, _ledger_resolve_line, _txn_excluded_info, _txn_state_json).
See routes/__init__.py for the pattern: plain @app.route on the SAME app
object, imported at the bottom of app.py, so every endpoint name and URL is
byte-identical to the monolith version. Shared helpers (claim sync,
activity log, AI matchers, rollup clauses) stay in app.py and are imported
from there at module import time, the way routes/crew.py does.

Phase 2 additions (workspace.json, split, manual-expense) live at the bottom
of this file under their own banner.
"""
import csv
import io
import logging
import os
from budget_calc import (FP_COA_SECTIONS)
from datetime import (datetime)
from flask import (abort, jsonify, render_template, request, url_for)
from flask_login import (current_user, login_required)
from sqlalchemy import (func, text)

from app import (app, _absorb_receipt_as_backup, _actuals_vendor_suggestions, _ai_clean_transaction, _ai_coa_taxonomy, _ai_known_vendors, _ai_match_batch, _ai_past_corrections, _ai_suggest_code_for_txn, _bg_run_all_scans, _clear_ai_code_suggestion, _delete_evidence_for_txns, _exclude_estimate_linked_txns_clause, _line_picker_groups, _log_activity, _parse_bank_csv, _require_project_role, _run_budget_mismatch_scan, _run_double_coded_scan, _run_duplicate_receipt_scan, _run_people_line_scan, _section_name, _signed_amount, _split_parent_id_set, _txn_is_split_parent, _validate_upload_file)
from models import (Budget, BudgetLine, DocUpload, ProjectAccess, ProjectSheet, Transaction, TransactionDupDismissal, TransactionSplit, User, db)
from actuals import (ClaimError, claim_for, pool_filter, pool_txn, release, mark_not_project,
                     undismiss, pool_dismissal, dismissed_pool_rows, is_qbo_row)


def _pool_txn_or_404(pid, tid):
    """The row by id when it is in this project's pool (its own rows + the
    unclaimed QuickBooks rows — actuals.pool_filter), else 404."""
    txn = pool_txn(pid, tid)
    if txn is None:
        abort(404)
    return txn


def _split_parent_error(txn):
    """The 409 text for coding a container row: a transaction_split parent
    (Phase 2) is cleared with DELETE …/split; an itemized document from the
    doc modal."""
    if TransactionSplit.query.filter_by(transaction_id=txn.id).first() is not None:
        return ("This transaction is split across lines — the splits carry the "
                "coding. Remove the split to code it as one line.")
    return ("This document is itemized into line items — the sublines carry "
            "the coding. Open the doc and clear the itemization to code it as "
            "one line.")


def _guard_doc_expense(txn, data):
    """Actualizing 2.0 A1 (2026-07-20): a doc-born row is a DOCUMENT, not an
    expense until an explicit user action makes it one.

    REWORKED 2026-08-19 (owner: "AI suggested codes… doesn't create an
    expense… when I try to assign it to a line or take the AI suggestion it
    gives me an error… the whole creating-an-expense process is pretty
    muddy"): originally only the row dropdown's Create-expense option sent
    create_expense=true, and EVERY other explicit coding path — the doc
    editor's assign dropdown, drag-and-drop, the ✨ AI-suggestion accept,
    bulk coding — bounced with a 400. Coding a document to a budget line or
    section IS the create-expense intent, so all of these now activate the
    document as an expense (stamping activated_at + the primary evidence
    link) instead of erroring. These code paths are all user-driven clicks;
    nothing automated codes doc-born rows, so upload + AI suggestion alone
    still never creates spend — the doc stays $0 until a person codes it,
    matches it to an imported charge, or marks it as backup.
    RE-REWORKED 2026-10-30 (owner: "only transactions that have come
    through QuickBooks can be actualized … there is a pathway where you
    upload a receipt and say this receipt does not have an electronic
    charge — make it an expense, flagged, and you should have to give a
    reason"): silent activation-on-coding is OFF again. The ONLY way a
    receipt becomes spend is the explicit Create-expense action
    (doc_log_expense), which now REQUIRES a stated reason — or matching
    it to a real charge. Coding attempts on a not-yet-activated
    receipt row get a clear 400 pointing at those two paths."""
    if txn.source != 'doc_upload' or txn.activated_at is not None:
        return None
    return jsonify({"error": "This is a receipt, not a charge — it doesn't "
                             "count as spend yet. Match it to a QuickBooks "
                             "transaction in the Actuals register, or use "
                             "💰 Create expense (and give the reason: cash, "
                             "check, another card…) if no electronic charge "
                             "will ever arrive."}), 400


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/set-line", methods=["POST"])
@login_required
def actuals_set_line(pid, tid):
    """Click-to-code: set the budget_line_id on a transaction (or clear
    it). Auto-clones Working → Actual on the first link via
    actuals.link_transaction_to_line. Returns JSON."""
    txn = _pool_txn_or_404(pid, tid)
    data = request.get_json(force=True) or {}
    if data.get("budget_line_id"):
        _ge = _guard_doc_expense(txn, data)
        if _ge is not None:
            return _ge
    raw  = data.get("budget_line_id")

    # Itemized container: its coding lives on the sublines — re-coding it would
    # double-count the document. (Review fix 2026-07.)
    if raw and _txn_is_split_parent(txn):
        return jsonify({"error": _split_parent_error(txn)}), 409

    # Empty string / None / 0 = clear the link.
    if not raw:
        from actuals import unlink_transaction
        try:
            _before_clear = {'budget_line_id': txn.budget_line_id,
                             'account_code': txn.account_code,
                             'account_code_name': txn.account_code_name}
            unlink_transaction(tid)
            db.session.refresh(txn)
            _act = None
            try:
                _act = _log_activity(
                    action='update', entity_type='transaction',
                    entity_id=txn.id,
                    entity_label=(txn.vendor or txn.note or f'Txn #{txn.id}')[:80],
                    project_id=pid,
                    before=_before_clear,
                    after={'budget_line_id': None, 'account_code': None,
                           'account_code_name': None},
                    note='Cleared coding')
            except Exception: pass
            return jsonify({"ok": True, "cleared": True,
                            "activity_id": (_act.id if _act else None)})
        except Exception as e:
            logging.exception(f"[actuals] unlink failed: {e}")
            return jsonify({"error": str(e)}), 500

    try:
        line_id = int(raw)
    except (TypeError, ValueError):
        return jsonify({"error": "budget_line_id must be an integer"}), 400

    from actuals import link_transaction_to_line
    try:
        # Snapshot before-state for the activity log.
        _before = {'budget_line_id': txn.budget_line_id,
                   'account_code': txn.account_code,
                   'account_code_name': txn.account_code_name}
        result = link_transaction_to_line(
            tid, line_id, user_id=current_user.id
        )
        # Refresh after commit so UI gets the freshly-set fields.
        db.session.refresh(txn)
        # Coding it manually clears any AI code suggestion so it stops showing
        # in the popup / row chip / 'Codes to confirm' queue. (User 2026-06-22.)
        _clear_ai_code_suggestion(txn)
        db.session.commit()
        _act = None
        try:
            _label = (txn.vendor or txn.note or f'Txn #{txn.id}')[:80]
            _amt = float(txn.amount or 0)
            _coa_name = txn.account_code_name or ''
            _act = _log_activity(
                action='update', entity_type='transaction',
                entity_id=txn.id, entity_label=_label,
                project_id=pid,
                before=_before,
                after={'budget_line_id': txn.budget_line_id,
                       'account_code': txn.account_code,
                       'account_code_name': _coa_name},
                dollar_delta=0,
                note=f'Coded ${_amt:,.2f} → {txn.account_code} {_coa_name}'.strip())
        except Exception: pass
        return jsonify({
            "ok":            True,
            "cleared":       False,
            "activity_id":   (_act.id if _act else None),
            "transaction": {
                "id":              txn.id,
                "budget_line_id":  txn.budget_line_id,
                "account_code":    txn.account_code,
                "account_code_name": txn.account_code_name,
                "match_status":    txn.match_status,
            },
            "actual_was_just_created":  result.get("actual_was_just_created"),
            "working_was_just_created": result.get("working_was_just_created"),
            "actual_budget_id":         result.get("actual_budget_id"),
        })
    except ClaimError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 409
    except Exception as e:
        logging.exception(f"[actuals] link failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/projects/<int:pid>/actuals/suggestions", methods=["GET"])
@login_required
def actuals_suggestions(pid):
    """On-demand recompute of the vendor→line smart suggestions, so the
    Actuals tab can refresh its 💡 chips when reopened — picking up doc
    corrections and new line assignments without a full page reload.
    (User 2026-05-29.) Cheap token-overlap matching."""
    Budget.query.filter_by(project_id=pid).first_or_404()   # 404 on bad project
    return jsonify({"suggestions": _actuals_vendor_suggestions(pid)})


@app.route("/projects/<int:pid>/actuals/match-candidates", methods=["POST"])
@login_required
def actuals_match_candidates(pid):
    """Configurable candidate finder for the Review-matches screen. Body:
    {amount_tol (number $), date_window (int days | 'off'), use_vendor (bool)}.
    Returns per-charge shortlists with confidence. (User 2026-06-16.)"""
    ProjectSheet.query.get_or_404(pid)
    body = request.get_json(silent=True) or {}
    try:
        amount_tol = max(0.0, float(body.get('amount_tol', 0) or 0))
    except (TypeError, ValueError):
        amount_tol = 0.0
    _dw = body.get('date_window', 3)
    if _dw in (None, '', 'off', 'any', 'Any'):
        date_window = None
    else:
        try:
            date_window = max(0, int(_dw))
        except (TypeError, ValueError):
            date_window = 3
    use_vendor = bool(body.get('use_vendor'))
    card = body.get('card') or None
    from actuals import find_match_candidates
    try:
        return jsonify(find_match_candidates(pid, amount_tol=amount_tol,
                                             date_window=date_window, use_vendor=use_vendor,
                                             card=card))
    except Exception as e:
        logging.exception("[actuals] match-candidates failed")
        return jsonify({"error": str(e)}), 500


@app.route("/projects/<int:pid>/actuals/split-candidates", methods=["POST"])
@login_required
def actuals_split_candidates(pid):
    """Detect split receipts (one receipt = sum of several charges). Body:
    {date_window (default 5), card}. (User 2026-06-16.)"""
    ProjectSheet.query.get_or_404(pid)
    body = request.get_json(silent=True) or {}
    try:
        date_window = max(0, int(body.get('date_window') or 5))
    except (TypeError, ValueError):
        date_window = 5
    card = body.get('card') or None
    from actuals import find_split_candidates
    try:
        return jsonify(find_split_candidates(pid, date_window=date_window, card=card))
    except Exception as e:
        logging.exception("[actuals] split-candidates failed")
        return jsonify({"error": str(e)}), 500


@app.route("/projects/<int:pid>/actuals/link-split", methods=["POST"])
@login_required
def actuals_link_split(pid):
    """Link ONE receipt to MULTIPLE charges as an intentional split. Body:
    {keeper_doc_id, charge_tids:[...], remove_doc_ids:[...]}. Each charge gets
    doc_upload_id=keeper, match_status='confirmed', and a shared split_group;
    the keeper's own doc-source row is removed (it's now backing real charges),
    and any redundant duplicate receipts (the same file uploaded once per line)
    are folded into the keeper. (User 2026-06-16.)"""
    if getattr(current_user, 'role', None) == 'viewer':
        return jsonify({"error": "Forbidden"}), 403
    ProjectSheet.query.get_or_404(pid)
    data = request.get_json(force=True) or {}
    try:
        keeper = int(data.get('keeper_doc_id'))
        tids = [int(t) for t in (data.get('charge_tids') or [])]
    except (TypeError, ValueError):
        return jsonify({"error": "keeper_doc_id and charge_tids required"}), 400
    remove = []
    for x in (data.get('remove_doc_ids') or []):
        try:
            if int(x) != keeper:
                remove.append(int(x))
        except (TypeError, ValueError):
            pass
    if not keeper or len(tids) < 2:
        return jsonify({"error": "Need a receipt and at least two charges to split."}), 400
    doc = DocUpload.query.filter_by(id=keeper, project_id=pid).first()
    if not doc:
        return jsonify({"error": "Receipt not on this project"}), 404
    sg = f"SG-{keeper}"
    linked = 0
    for tid in tids:
        txn = pool_txn(pid, tid)
        if not txn:
            continue
        try:
            claim_for(txn, pid, why='split receipt linked')
        except ClaimError as e:
            db.session.rollback()
            return jsonify({"error": str(e)}), 409
        txn.doc_upload_id = keeper
        txn.match_status = 'confirmed'
        txn.split_group = sg
        if not txn.vendor and doc.vendor:
            txn.vendor = doc.vendor
        if txn.source in ('qbo_sync', 'csv_import'):
            txn.source = 'reconciled'
        txn.updated_at = datetime.utcnow()
        linked += 1
    # The keeper's OWN auto-created doc_upload row is now redundant (the receipt
    # is backing real charges) — drop it so it isn't double-counted.
    _delete_evidence_for_txns([r[0] for r in db.session.query(Transaction.id)
                               .filter_by(doc_upload_id=keeper, source='doc_upload').all()])
    Transaction.query.filter_by(doc_upload_id=keeper, source='doc_upload').delete(synchronize_session=False)
    # Fold redundant duplicate receipts into the keeper.
    folded = 0
    for rid in remove:
        rdoc = DocUpload.query.filter_by(id=rid, project_id=pid).first()
        if not rdoc:
            continue
        # Any charge still pointing at the dup → repoint to the keeper.
        Transaction.query.filter(pool_filter(pid),
                                 Transaction.doc_upload_id == rid,
                                 Transaction.source != 'doc_upload').update(
            {"doc_upload_id": keeper}, synchronize_session=False)
        _delete_evidence_for_txns([r[0] for r in db.session.query(Transaction.id)
                                   .filter_by(doc_upload_id=rid, source='doc_upload').all()])
        Transaction.query.filter_by(doc_upload_id=rid, source='doc_upload').delete(synchronize_session=False)
        rdoc.is_duplicate = True
        rdoc.duplicate_of_id = keeper
        rdoc.status = 'duplicate'
        folded += 1
    db.session.commit()
    try:
        _log_activity(action='update', entity_type='transaction_match', entity_id=keeper,
                      entity_label=f'Split receipt #{keeper}', project_id=pid,
                      note=f'Linked receipt #{keeper} to {linked} charges as a split'
                           + (f'; folded {folded} duplicate receipt(s)' if folded else ''))
    except Exception:
        pass
    return jsonify({"ok": True, "linked": linked, "folded": folded, "split_group": sg})


def _dismissal_info(d):
    """The who / when / why of a pool dismissal (Not project for this
    project only), in _txn_excluded_info's shape."""
    _by = None
    if d.created_by:
        try:
            _u = User.query.get(d.created_by)
            _by = (_u.name or _u.email) if _u else None
        except Exception:
            _by = None
    return {"at": d.created_at.isoformat(timespec='minutes') if d.created_at else None,
            "by_user_id": d.created_by, "by": _by, "reason": d.reason,
            "pool": True}


def _txn_excluded_info(txn):
    """The who / when / why of a Not-project exclusion, for JSON payloads.
    None when the row is not excluded."""
    _d = getattr(txn, '_pool_dismissal', None)
    if _d is not None:
        return _dismissal_info(_d)
    if not getattr(txn, 'not_project_expense', False):
        return None
    _by = None
    _uid = getattr(txn, 'excluded_by_user_id', None)
    if _uid:
        try:
            _u = User.query.get(_uid)
            _by = (_u.name or _u.email) if _u else None
        except Exception:
            _by = None
    _at = getattr(txn, 'excluded_at', None)
    return {"at": _at.isoformat(timespec='minutes') if _at else None,
            "by_user_id": _uid, "by": _by,
            "reason": getattr(txn, 'excluded_reason', None)}


def _txn_state_json(txn):
    """The coding / exclusion state of one row, returned by the Phase 1
    mutation routes so the UI can repaint without a reload."""
    return {"id": txn.id, "project_id": txn.project_id,
            "vendor": txn.vendor, "amount": float(txn.amount or 0),
            "is_expense": bool(txn.is_expense),
            "date": (txn.txn_date or '')[:10],
            "budget_line_id": txn.budget_line_id,
            "account_code": txn.account_code,
            "account_code_name": txn.account_code_name,
            "match_status": txn.match_status,
            "not_project_expense": bool(txn.not_project_expense),
            "excluded": _txn_excluded_info(txn)}


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/mark-not-project", methods=["POST"])
@login_required
def actuals_mark_not_project(pid, tid):
    """Mark a transaction as 'not a project expense' — drops it from every
    rollup and from the Unclaimed list, but keeps the row so it can be
    RECOVERED (Phase 1, 2026-10-30): excluded_at / excluded_by_user_id /
    excluded_reason record who, when and why; /recover clears them.

    Works on any pool row — a QBO / CSV charge, a manual expense or a
    receipt-born row (the old UI only offered the button on receipts; the
    route never had a guard, so none is added — the one thing it refuses
    is an itemized split CHILD, whose coding belongs to its parent).
    Body (optional JSON): {reason: str}. Any coding and any transaction
    splits are cleared; the before-state goes into the ActivityLog so the
    action can be undone."""
    txn = _pool_txn_or_404(pid, tid)
    _require_project_role(pid, 'editor')
    if txn.source == 'invoice_split' and txn.parent_transaction_id:
        return jsonify({"error": "This is one line of an itemized document — "
                                 "mark the parent document not-project instead, "
                                 "or clear the itemization first."}), 409
    _body = request.get_json(silent=True) or {}
    _reason = (_body.get('reason') or '').strip()[:300] or None
    _old_splits = [{"budget_line_id": sp.budget_line_id,
                    "amount": float(sp.amount or 0), "note": sp.note}
                   for sp in TransactionSplit.query.filter_by(transaction_id=txn.id).all()]
    _before_npe = {'not_project_expense': bool(txn.not_project_expense),
                   'account_code': txn.account_code,
                   'account_code_name': txn.account_code_name,
                   'budget_line_id': txn.budget_line_id,
                   'match_status': txn.match_status,
                   'excluded_at': None, 'excluded_by_user_id': None,
                   'excluded_reason': None,
                   'splits': _old_splits}
    if is_qbo_row(txn):
        _before_npe['doc_upload_id'] = txn.doc_upload_id
        _before_npe['pool_dismissed'] = False
    _now = datetime.utcnow()
    # THE rule (actuals.mark_not_project, 2026-11-04): a QuickBooks row is
    # dismissed for THIS project only (released first when this project
    # held it) — it stays in the pool for every other project; a row born
    # here is excluded as before.
    try:
        _mode = mark_not_project(txn, pid, _reason, current_user.id)
    except ClaimError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 409
    if _mode == 'excluded':
        txn.budget_line_id      = None
        txn.account_code        = None
        txn.account_code_name   = None
        txn.match_status        = 'unmatched'
        if _old_splits:
            TransactionSplit.query.filter_by(transaction_id=txn.id).delete(synchronize_session=False)
    txn.updated_at = _now
    db.session.commit()
    _act = None
    try:
        _amt = float(txn.amount or 0)
        _act = _log_activity(
            action='update', entity_type='transaction',
            entity_id=txn.id,
            entity_label=(txn.vendor or txn.note or f'Txn #{txn.id}')[:80],
            project_id=pid,
            before=_before_npe,
            after=({'pool_dismissed': True, 'account_code': None,
                    'account_code_name': None, 'budget_line_id': None, 'doc_upload_id': None,
                    'match_status': 'unmatched', 'excluded_reason': _reason, 'splits': []}
                   if _mode == 'dismissed' else
                   {'not_project_expense': True, 'account_code': None,
                    'account_code_name': None, 'budget_line_id': None,
                    'match_status': 'unmatched',
                    'excluded_at': _now.isoformat(timespec='minutes'),
                    'excluded_by_user_id': current_user.id,
                    'excluded_reason': _reason, 'splits': []}),
            note=f'Marked ${_amt:,.2f} as not a project expense'
                 + (' for this project (stays in the company pool for the others)' if _mode == 'dismissed' else '')
                 + (f' — {_reason}' if _reason else ''))
    except Exception: pass
    return jsonify({"ok": True, "transaction_id": tid, "mode": _mode,
                    "activity_id": (_act.id if _act else None),
                    "transaction": _txn_state_json(txn)})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/recover", methods=["POST"])
@login_required
def actuals_recover_not_project(pid, tid):
    """Undo a Not-project exclusion (Phase 1, 2026-10-30): clears
    not_project_expense and the three excluded_* columns so the row returns
    to the Unclaimed list, logs with an undo payload and returns the row's
    new state. A QuickBooks row with no other claim signal goes back to
    the company pool (actuals.release) — it reappears in Unclaimed here
    and in every other project. A pool row this project dismissed is
    restored by deleting the dismissal (TransactionPoolDismissal)."""
    txn = pool_txn(pid, tid, include_dismissed=True)
    if txn is None:
        abort(404)
    _require_project_role(pid, 'editor')
    _dis = pool_dismissal(txn.id, pid) if txn.project_id is None else None
    if _dis is not None:
        _before_d = {'pool_dismissed': True, 'excluded_reason': _dis.reason}
        undismiss(txn, pid)
        db.session.commit()
        _act = None
        try:
            _act = _log_activity(
                action='update', entity_type='transaction', entity_id=txn.id,
                entity_label=(txn.vendor or txn.note or f'Txn #{txn.id}')[:80],
                project_id=pid, before=_before_d, after={'pool_dismissed': False},
                note=f'Restored ${float(txn.amount or 0):,.2f} — back in Unclaimed for this project')
        except Exception: pass
        return jsonify({"ok": True, "transaction_id": tid, "restored_dismissal": True,
                        "activity_id": (_act.id if _act else None),
                        "transaction": _txn_state_json(txn)})
    if not txn.not_project_expense:
        return jsonify({"ok": True, "already": True,
                        "transaction": _txn_state_json(txn)})
    _before = {'not_project_expense': True,
               'excluded_at': (txn.excluded_at.isoformat(timespec='minutes')
                               if txn.excluded_at else None),
               'excluded_by_user_id': txn.excluded_by_user_id,
               'excluded_reason': txn.excluded_reason}
    # Phase 5: recovering a row QuickBooks voided means "keep it anyway" —
    # the red Voided flag (carried by no_charge_reason) comes off with it.
    from actuals import VOIDED_IN_QBO_REASON as _VOID
    if getattr(txn, 'no_charge_reason', None) == _VOID:
        _before['no_charge_reason'] = _VOID
        txn.no_charge_reason = None
    txn.not_project_expense = False
    txn.excluded_at         = None
    txn.excluded_by_user_id = None
    txn.excluded_reason     = None
    txn.updated_at          = datetime.utcnow()
    release(txn, why='recovered from Not project')
    db.session.commit()
    _act = None
    try:
        _amt = float(txn.amount or 0)
        _act = _log_activity(
            action='update', entity_type='transaction',
            entity_id=txn.id,
            entity_label=(txn.vendor or txn.note or f'Txn #{txn.id}')[:80],
            project_id=pid,
            before=_before,
            after={'not_project_expense': False, 'excluded_at': None,
                   'excluded_by_user_id': None, 'excluded_reason': None},
            note=f'Recovered ${_amt:,.2f} from Not project — back in Unclaimed')
    except Exception: pass
    return jsonify({"ok": True, "transaction_id": tid,
                    "activity_id": (_act.id if _act else None),
                    "transaction": _txn_state_json(txn)})


@app.route("/projects/<int:pid>/actuals/not-project.json", methods=["GET"])
@login_required
def actuals_not_project_json(pid):
    """The recoverable Not-project list (Phase 1): rows this project excluded,
    newest exclusion first, each with who / when / reason. (claimed_elsewhere
    is always empty since the company ledger, 2026-11-03: a charge another
    project claimed is simply not in this project's pool.)"""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'viewer')
    rows = (Transaction.query
            .filter(Transaction.project_id == pid,
                    Transaction.not_project_expense == True,      # noqa: E712
                    Transaction.backup_of_txn_id.is_(None))
            .all())
    _proj_names = {}
    def _pname(_p):
        if _p not in _proj_names:
            _pp = ProjectSheet.query.get(_p)
            _proj_names[_p] = _pp.name if _pp else f'project #{_p}'
        return _proj_names[_p]
    for _t, _d in dismissed_pool_rows(pid):
        _t._pool_dismissal = _d
        rows.append(_t)
    from actuals import signed_amount_value as _sav_np
    not_project, claimed_elsewhere = [], []
    for t in rows:
        not_project.append({
            "id": t.id, "date": (t.txn_date or '')[:10],
            "vendor": t.vendor or '—',
            "amount": _sav_np(t.amount, t.is_expense),
            "card_last4": getattr(t, 'card_last4', None),
            "source": t.source,
            "memo": (t.note or '')[:140] or (t.qbo_category or ''),
            "excluded": _txn_excluded_info(t)})
    not_project.sort(key=lambda r: ((r["excluded"] or {}).get("at") or '',
                                    r["date"] or '', r["id"]), reverse=True)
    claimed_elsewhere.sort(key=lambda r: (r["date"] or '', r["id"]), reverse=True)
    return jsonify({"ok": True, "not_project": not_project,
                    "claimed_elsewhere": claimed_elsewhere,
                    "counts": {"not_project": len(not_project),
                               "claimed_elsewhere": len(claimed_elsewhere)}})


@app.route("/projects/<int:pid>/actuals/line/<int:lid>/transactions.json", methods=["GET"])
@login_required
def actuals_line_transactions(pid, lid):
    """Every transaction coded to a single budget line, for the per-line
    Reconcile panel (large thumbnail tiles + merge / unlink / unmatch / uncode).
    (User 2026-06-26 — "see a single budget line and all things coded to it".)"""
    ProjectSheet.query.get_or_404(pid)
    line = (BudgetLine.query
            .join(Budget, Budget.id == BudgetLine.budget_id)
            .filter(BudgetLine.id == lid, Budget.project_id == pid)
            .first_or_404())
    # Charges are coded to whichever budget's line id (Working vs Actual), so
    # resolve sibling lines across this project that represent the SAME line
    # (same section + description). Lets the panel open from any budget view.
    _sib_q = (BudgetLine.query
              .join(Budget, Budget.id == BudgetLine.budget_id)
              .filter(Budget.project_id == pid,
                      BudgetLine.account_code == line.account_code))
    if line.description:
        _sib_q = _sib_q.filter(BudgetLine.description == line.description)
    else:
        _sib_q = _sib_q.filter(BudgetLine.id == line.id)
    _sib_ids = [l.id for l in _sib_q.all()] or [line.id]
    txns = (Transaction.query
            .filter(Transaction.project_id == pid,
                    Transaction.budget_line_id.in_(_sib_ids))
            .order_by(Transaction.txn_date.desc().nullslast(), Transaction.id.desc())
            .all())
    out = []
    for t in txns:
        doc = DocUpload.query.get(t.doc_upload_id) if t.doc_upload_id else None
        _dt = t.txn_date
        out.append({
            "id": t.id,
            "vendor": t.vendor,
            "amount": float(t.amount) if t.amount is not None else None,
            "date": (_dt.isoformat() if hasattr(_dt, 'isoformat') else _dt),
            "source": t.source,
            "match_status": t.match_status,
            "is_expense": bool(t.is_expense),
            "doc_upload_id": t.doc_upload_id,
            "has_image": bool(doc and doc.content_type and doc.content_type.startswith('image/')),
            "doc_category": (doc.category if doc else None),
            "doc_filename": (doc.filed_filename or doc.original_filename) if doc else None,
            "qbo_txn_id": t.qbo_txn_id,
            "account_code": t.account_code,
            "note": t.note,
        })
    # Split pieces landing on this line (parent has budget_line_id NULL).
    try:
        from actuals import split_pieces_for_lines
        for sp, pt in split_pieces_for_lines(pid, _sib_ids):
            doc = DocUpload.query.get(pt.doc_upload_id) if pt.doc_upload_id else None
            _dt = pt.txn_date
            out.append({
                "id": pt.id,
                "split_id": sp.id,
                "vendor": pt.vendor,
                "amount": float(sp.amount or 0),
                "split_of": float(pt.amount) if pt.amount is not None else None,
                "is_split": True,
                "date": (_dt.isoformat() if hasattr(_dt, 'isoformat') else _dt),
                "source": pt.source,
                "match_status": pt.match_status,
                "is_expense": bool(pt.is_expense),
                "doc_upload_id": pt.doc_upload_id,
                "has_image": bool(doc and doc.content_type and doc.content_type.startswith('image/')),
                "doc_category": (doc.category if doc else None),
                "doc_filename": (doc.filed_filename or doc.original_filename) if doc else None,
                "qbo_txn_id": pt.qbo_txn_id,
                "account_code": line.account_code,
                "note": sp.note or pt.note,
            })
        out.sort(key=lambda r: ((r.get("date") or ""), -r["id"]), reverse=True)
    except Exception as _spe:
        logging.warning(f"[line-txns] split pieces skipped for line {lid}: {_spe}")
    return jsonify({
        "ok": True,
        "line": {"id": line.id, "code": line.account_code,
                 "name": (line.description or line.account_name or ''),
                 "section_name": (_section_name(line.account_code) or line.account_name or '')},
        "transactions": out,
    })


def _ledger_resolve_line(pid, lid):
    """Resolve a budget-line id (from EITHER the Estimated or Working view) to
    the canonical line + the full project-wide SIBLING id set representing the
    SAME line — the exact same (account_code + description) resolution the
    per-line transactions.json endpoint uses. Also returns the WORKING sister
    line (for the line's forecast total) when one exists.

    Returns (line, sib_ids, working_line) or (None, None, None) if not found.
    """
    line = (BudgetLine.query
            .join(Budget, Budget.id == BudgetLine.budget_id)
            .filter(BudgetLine.id == lid, Budget.project_id == pid)
            .first())
    if line is None:
        return None, None, None
    _sib_q = (BudgetLine.query
              .join(Budget, Budget.id == BudgetLine.budget_id)
              .filter(Budget.project_id == pid,
                      BudgetLine.account_code == line.account_code))
    if line.description:
        _sib_q = _sib_q.filter(BudgetLine.description == line.description)
    else:
        _sib_q = _sib_q.filter(BudgetLine.id == line.id)
    sib_rows = _sib_q.all() or [line]
    sib_ids = [l.id for l in sib_rows] or [line.id]
    # Prefer the Working (budget_mode working/actual) sister for the forecast
    # total — its working_total is the evolving Working forecast.
    working_line = None
    for l in sib_rows:
        b = Budget.query.get(l.budget_id)
        if b and (b.budget_mode or '') == 'working':
            working_line = l
            break
    return line, sib_ids, working_line


@app.route("/projects/<int:pid>/actuals/line/<int:lid>/ledger.json", methods=["GET"])
@login_required
def actuals_line_ledger(pid, lid):
    """Line Ledger side panel data: the budget line's forecast total, coded
    spend total, and a CHRONOLOGICAL (oldest-first) list of every transaction
    coded to the line — with doc/thumbnail, split state, PO, review state, and
    anomaly-flag state per row. Sibling-aware (resolves an Estimated-view line
    to its Working sister so the panel opens from any view). Read-only."""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'viewer')
    line, sib_ids, working_line = _ledger_resolve_line(pid, lid)
    if line is None:
        abort(404)

    txns = (Transaction.query
            .filter(Transaction.project_id == pid,
                    Transaction.budget_line_id.in_(sib_ids))
            .order_by(Transaction.txn_date.asc().nullsfirst(),
                      Transaction.id.asc())
            .all())

    # Unresolved anomaly flags referencing any of these transactions.
    tids = [t.id for t in txns]
    flagged_tids = set()
    if tids:
        from models import AnomalyFlag
        for (ftid,) in (db.session.query(AnomalyFlag.transaction_id)
                        .filter(AnomalyFlag.transaction_id.in_(tids),
                                AnomalyFlag.resolved == False)  # noqa: E712
                        .distinct().all()):
            if ftid is not None:
                flagged_tids.add(ftid)

    # Resolve parent docs for split children whose own doc_upload_id is null.
    parent_ids = {t.parent_transaction_id for t in txns
                  if t.parent_transaction_id and not t.doc_upload_id}
    parent_docs = {}   # parent_txn_id -> doc_upload_id
    if parent_ids:
        for (p_id, p_doc) in (db.session.query(
                    Transaction.id, Transaction.doc_upload_id)
                .filter(Transaction.id.in_(parent_ids)).all()):
            parent_docs[p_id] = p_doc

    # 📎 Backup receipts pointing at these rows (v2): map target txn id →
    # [{doc_id, filename, amount}] in one batched pass.
    backups_by_target = {}
    _bk_docs = {}
    if tids:
        _bk_rows = (Transaction.query
                    .filter(Transaction.backup_of_txn_id.in_(tids)).all())
        _bk_doc_ids = {b.doc_upload_id for b in _bk_rows if b.doc_upload_id}
        _bk_docs = {}
        if _bk_doc_ids:
            for _bd in DocUpload.query.filter(DocUpload.id.in_(_bk_doc_ids)).all():
                _bk_docs[_bd.id] = (_bd.filed_filename or _bd.original_filename
                                    or f'Upload #{_bd.id}')
        for b in _bk_rows:
            backups_by_target.setdefault(b.backup_of_txn_id, []).append({
                "doc_id": b.doc_upload_id,
                "filename": _bk_docs.get(b.doc_upload_id, 'receipt'),
                "amount": float(b.amount) if b.amount is not None else None,
            })

    # A2: unified evidence — merge expense_evidence rows into the same map.
    # Skip pairs already present from the legacy backup shadow rows, and skip
    # each row's own primary doc (rendered as the row's doc cell already).
    try:
        from models import ExpenseEvidence as _EEv
        _seen_pairs = {(tgt, b["doc_id"]) for tgt, lst in backups_by_target.items()
                       for b in lst if b.get("doc_id")}
        _ev_rows = (_EEv.query.filter(_EEv.transaction_id.in_(tids)).all()
                    if tids else [])
        _ev_doc_ids = {e.doc_upload_id for e in _ev_rows} - set(_bk_docs or {})
        _ev_docs = dict(_bk_docs or {})
        if _ev_doc_ids:
            for _bd in DocUpload.query.filter(DocUpload.id.in_(_ev_doc_ids)).all():
                _ev_docs[_bd.id] = (_bd.filed_filename or _bd.original_filename
                                    or f'Upload #{_bd.id}')
        _own_doc = {t.id: t.doc_upload_id for t in txns}
        for e in _ev_rows:
            if e.kind == 'primary' and _own_doc.get(e.transaction_id) == e.doc_upload_id:
                continue   # the row's doc cell already shows it
            if (e.transaction_id, e.doc_upload_id) in _seen_pairs:
                continue
            backups_by_target.setdefault(e.transaction_id, []).append({
                "doc_id": e.doc_upload_id,
                "filename": _ev_docs.get(e.doc_upload_id, 'document'),
                "amount": None,
                "kind": e.kind,
            })
    except Exception as _eve:
        logging.warning(f"[ledger] evidence merge failed: {_eve}")

    out = []
    coded_total = 0.0
    # Coded total = the per-line Actual cell's rule: the ONE spend predicate
    # (row_counts_as_spend) and the ONE signed-amount rule (a refund nets
    # against the line). It used to add every row's expense amount and
    # ignore credits (Cleanup D).
    from actuals import (row_counts_as_spend as _rcas_lg,
                         signed_amount_value as _sav_lg,
                         split_sums_by_line as _ssbl_lg)
    _parents_lg = _split_parent_id_set(pid)
    # Split pieces coded to these lines (their parent has no line of its own).
    try:
        _split_total_lg = sum(_ssbl_lg(pid, sib_ids).values())
    except Exception as _sle:
        logging.warning(f"[ledger] split sums failed: {_sle}")
        _split_total_lg = 0.0
    for t in txns:
        # Effective doc: own doc, else (for a split child) its parent's doc.
        eff_doc_id = t.doc_upload_id
        if eff_doc_id is None and t.parent_transaction_id:
            eff_doc_id = parent_docs.get(t.parent_transaction_id)
        doc = DocUpload.query.get(eff_doc_id) if eff_doc_id else None
        _dt = t.txn_date
        amt = float(t.amount) if t.amount is not None else None
        if amt is not None and _rcas_lg(t, _parents_lg):
            coded_total += _sav_lg(amt, t.is_expense)
        out.append({
            "id": t.id,
            "date": (_dt.isoformat() if hasattr(_dt, 'isoformat') else _dt),
            "vendor": t.vendor,
            "amount": amt,
            "note": t.note,
            "source": t.source,
            "is_split": (t.source == 'invoice_split'),
            "doc": ({"id": doc.id,
                     "filename": (doc.filed_filename or doc.original_filename
                                  or f'Upload #{doc.id}'),
                     "has_thumb": True} if doc else None),
            "po_number": None,
            "reviewed": bool(t.reviewed_at),
            "reviewed_at": (t.reviewed_at.isoformat() if t.reviewed_at else None),
            "reviewed_by": t.reviewed_by,
            "flagged": (t.id in flagged_tids),
            "matched": (eff_doc_id is not None),
            # A3: accrual state — an invoice/receipt-born expense with no bank
            # charge reconciled to it yet ("awaiting payment"). QBO reconcile
            # flips source to 'reconciled', which clears this naturally.
            "awaiting": (t.source in ('doc_upload', 'invoice_split')
                         and not t.not_project_expense
                         and getattr(t, 'backup_of_txn_id', None) is None),
            "backups": backups_by_target.get(t.id, []),
        })

    # Split pieces (Phase 2 splits): the parent charge has budget_line_id
    # NULL, so the query above never sees it — list each piece that lands on
    # this line with ITS amount, marked as a split of the whole charge.
    try:
        from actuals import split_pieces_for_lines
        for sp, pt in split_pieces_for_lines(pid, sib_ids):
            doc = DocUpload.query.get(pt.doc_upload_id) if pt.doc_upload_id else None
            _dt = pt.txn_date
            amt = float(sp.amount or 0)
            if pt.is_expense:
                coded_total += amt
            out.append({
                "id": pt.id,
                "split_id": sp.id,
                "date": (_dt.isoformat() if hasattr(_dt, 'isoformat') else _dt),
                "vendor": pt.vendor,
                "amount": amt,
                "split_of": float(pt.amount) if pt.amount is not None else None,
                "note": sp.note or pt.note,
                "source": pt.source,
                "is_split": True,
                "doc": ({"id": doc.id,
                         "filename": (doc.filed_filename or doc.original_filename
                                      or f'Upload #{doc.id}'),
                         "has_thumb": True} if doc else None),
                "po_number": None,
                "reviewed": bool(pt.reviewed_at),
                "reviewed_at": (pt.reviewed_at.isoformat() if pt.reviewed_at else None),
                "reviewed_by": pt.reviewed_by,
                "flagged": False,
                "matched": pt.doc_upload_id is not None,
                "awaiting": False,
                "backups": backups_by_target.get(pt.id, []),
            })
        out.sort(key=lambda r: ((r.get("date") or ""), r["id"]))
    except Exception as _spe:
        logging.warning(f"[ledger] split pieces skipped for line {lid}: {_spe}")

    # Forecast total: the Working sister's working_total (the evolving forecast),
    # falling back to the resolved line's own working_total / estimated_total.
    _bl = working_line or line
    budget_total = None
    for _cand in (getattr(_bl, 'working_total', None),
                  getattr(_bl, 'estimated_total', None)):
        if _cand is not None:
            budget_total = float(_cand)
            break

    return jsonify({
        "ok": True,
        "line": {
            "id": line.id,
            "account_code": line.account_code,
            "description": (line.description or line.account_name or ''),
            "budget_total": budget_total,
            "coded_total": round(coded_total + _split_total_lg, 2),
            "txn_count": len(out),
        },
        "section": {
            "code": line.account_code,
            "name": (_section_name(line.account_code) or line.account_name or ''),
        },
        "txns": out,
    })


@app.route("/projects/<int:pid>/actuals/txn/<int:tid>/review", methods=["POST"])
@login_required
def actuals_txn_review(pid, tid):
    """Toggle a transaction's Line-Ledger reviewed state. Body {reviewed: bool}.
    Sets/clears reviewed_at (utcnow) + reviewed_by (current user). Advisory —
    does not touch totals or matching."""
    _require_project_role(pid, 'viewer')
    txn = _pool_txn_or_404(pid, tid)
    data = request.get_json(silent=True) or {}
    want = bool(data.get("reviewed"))
    if want:
        who = (current_user.name or (current_user.email.split('@')[0]
               if current_user.email else 'user'))
        txn.reviewed_at = datetime.utcnow()
        txn.reviewed_by = who
    else:
        txn.reviewed_at = None
        txn.reviewed_by = None
    db.session.commit()
    return jsonify({
        "ok": True,
        "reviewed": bool(txn.reviewed_at),
        "reviewed_at": (txn.reviewed_at.isoformat() if txn.reviewed_at else None),
        "reviewed_by": txn.reviewed_by,
    })


@app.route("/projects/<int:pid>/actuals/line/<int:lid>/review-all", methods=["POST"])
@login_required
def actuals_line_review_all(pid, lid):
    """Mark every currently-unreviewed transaction on this (sibling-resolved)
    line as reviewed. Returns the count newly marked."""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'viewer')
    line, sib_ids, _wl = _ledger_resolve_line(pid, lid)
    if line is None:
        abort(404)
    who = (current_user.name or (current_user.email.split('@')[0]
           if current_user.email else 'user'))
    now = datetime.utcnow()
    rows = (Transaction.query
            .filter(Transaction.project_id == pid,
                    Transaction.budget_line_id.in_(sib_ids),
                    Transaction.reviewed_at.is_(None))
            .all())
    for t in rows:
        t.reviewed_at = now
        t.reviewed_by = who
    db.session.commit()
    return jsonify({"ok": True, "count": len(rows)})


@app.route("/projects/<int:pid>/actuals/line-totals.json", methods=["POST"])
@login_required
def actuals_line_totals_batch(pid):
    """Live per-line actual totals for the budget page's Actual cells.

    Body {ids:[line ids from EITHER the Estimated or Working view]} → for each
    id, resolve the project-wide SIBLING set (same account_code + description —
    the same resolution the per-line transactions.json uses, so cell and
    detail always agree) and return the signed txn sum. Also returns per-code
    SECTION-ONLY totals (line-less coded spend). Powers refresh-without-reload
    after recoding (user 2026-07: "can a line automatically move if I recode it
    without refreshing?"). Read-only, fail-open."""
    _require_project_role(pid, 'viewer')
    try:
        ids = [int(x) for x in ((request.get_json(silent=True) or {}).get('ids') or [])][:2000]
    except Exception:
        ids = []
    out_lines, sections = {}, {}
    try:
        req_lines = (BudgetLine.query.filter(BudgetLine.id.in_(ids)).all()
                     if ids else [])
        # Sibling resolution in bulk: one pass over the project's lines.
        proj_lines = (BudgetLine.query
                      .join(Budget, Budget.id == BudgetLine.budget_id)
                      .filter(Budget.project_id == pid).all())
        sib_by_key = {}
        for pl in proj_lines:
            sib_by_key.setdefault(
                (pl.account_code, (pl.description or '').strip().lower()),
                set()).add(pl.id)
        # One signed-sum query over ALL sibling ids, distributed after.
        all_sib_ids = set()
        key_by_req = {}
        for rl in req_lines:
            k = (rl.account_code, (rl.description or '').strip().lower())
            key_by_req[rl.id] = k
            all_sib_ids |= sib_by_key.get(k, {rl.id})
        # ONE spend predicate (actuals.counts_as_spend, 2026-10-30) so the
        # live cell refresh agrees with the page-load rollup in budget_view —
        # this endpoint used to count un-activated receipt rows, backup rows
        # and itemized parents that budget_view excluded, so a cell could
        # change value on refresh.
        from actuals import counts_as_spend as _cas_lt
        _cas_clause = _cas_lt(_split_parent_id_set(pid))
        sums_by_lid = {}
        if all_sib_ids:
            for lid, amt in (db.session.query(
                        Transaction.budget_line_id, func.sum(_signed_amount()))
                    .filter(Transaction.project_id == pid,
                            Transaction.budget_line_id.in_(list(all_sib_ids)),
                            _cas_clause,
                            _exclude_estimate_linked_txns_clause())
                    .group_by(Transaction.budget_line_id).all()):
                sums_by_lid[lid] = float(amt or 0)
            # Transaction splits (Phase 2, 2026-10-30): same shared helper as
            # budget_view / _actuals_by_section_code, so a refreshed cell
            # equals the page-load value.
            from actuals import split_sums_by_line as _split_by_line_lt
            for lid, amt in _split_by_line_lt(pid, list(all_sib_ids)).items():
                sums_by_lid[lid] = sums_by_lid.get(lid, 0.0) + amt
        for rl in req_lines:
            sibs = sib_by_key.get(key_by_req[rl.id], {rl.id})
            out_lines[str(rl.id)] = round(sum(sums_by_lid.get(s, 0.0) for s in sibs), 2)
        # Section-only totals (line-less coded spend), keyed by account code.
        for code, amt in (db.session.query(
                    Transaction.account_code, func.sum(_signed_amount()))
                .filter(Transaction.project_id == pid,
                        Transaction.budget_line_id.is_(None),
                        Transaction.account_code.isnot(None),
                        _cas_clause,
                        _exclude_estimate_linked_txns_clause())
                .group_by(Transaction.account_code).all()):
            sections[str(code)] = round(float(amt or 0), 2)
    except Exception as _lte:
        logging.warning(f"[actuals/line-totals] failed: {_lte}")
    return jsonify({"ok": True, "lines": out_lines, "sections": sections})


@app.route("/projects/<int:pid>/actuals/section/<int:code>/transactions.json", methods=["GET"])
@login_required
def actuals_section_transactions(pid, code):
    """Every transaction coded to a COA SECTION but no specific budget line
    (account_code set, budget_line_id NULL). Backs the Working view's
    '📂 Section-coded spend' expandable row. Mirrors the per-line endpoint's
    response shape. Filtered to match the actual_section_only_by_code rollup:
    project-scoped, not_project_expense == False, and itemized-container
    parents (source='invoice_split' children point back at them) excluded so a
    split doc doesn't surface its uncoded parent here. (Migration P3, User
    2026-07 — bring the Actual view's section-only detail into Working.)"""
    ProjectSheet.query.get_or_404(pid)
    from actuals import counts_as_spend as _cas_sec
    _split_parents = _split_parent_id_set(pid)
    # The ONE spend rule (Cleanup D): the same rows the section rollup sums,
    # so this list ties to the section's Actual cell.
    txns = (Transaction.query
            .filter(Transaction.project_id == pid,
                    Transaction.account_code == code,
                    Transaction.budget_line_id.is_(None),
                    _cas_sec(_split_parents),
                    Transaction.source != 'invoice_split')
            .order_by(Transaction.txn_date.desc().nullslast(), Transaction.id.desc())
            .all())
    out = []
    for t in txns:
        doc = DocUpload.query.get(t.doc_upload_id) if t.doc_upload_id else None
        _dt = t.txn_date
        out.append({
            "id": t.id,
            "vendor": t.vendor,
            "amount": float(t.amount) if t.amount is not None else None,
            "date": (_dt.isoformat() if hasattr(_dt, 'isoformat') else _dt),
            "source": t.source,
            "match_status": t.match_status,
            "is_expense": bool(t.is_expense),
            "doc_upload_id": t.doc_upload_id,
            "has_image": bool(doc and doc.content_type and doc.content_type.startswith('image/')),
            "doc_category": (doc.category if doc else None),
            "doc_filename": (doc.filed_filename or doc.original_filename) if doc else None,
            "qbo_txn_id": t.qbo_txn_id,
            "account_code": t.account_code,
            "note": t.note,
        })
    return jsonify({
        "ok": True,
        "section": {"code": code,
                    "name": (_section_name(code) or dict(FP_COA_SECTIONS).get(code, ''))},
        "transactions": out,
    })


@app.route("/projects/<int:pid>/actuals/transaction/merge", methods=["POST"])
@login_required
def actuals_merge_transactions(pid):
    """Manually reconcile a pair of transactions that are really the same
    expense. Most common case: a receipt was uploaded *after* the QBO
    charge synced, so the receipt-first reconciliation (qbo_sync.py's
    _find_unreconciled_doc_match — runs only during sync) didn't catch
    them, and the user ends up with two rows for the same $97.75 charge.

    Body: { "tids": [<id_a>, <id_b>] } — exactly two transaction ids.

    Canonical-row priority (winner keeps its id, the other is deleted):
      1. Has qbo_txn_id (QBO charge is the canonical "money out" record)
      2. Has account_code set (already user-coded)
      3. Has budget_line_id set
      4. Lower id (older)

    Then we transfer onto the canonical, only filling blanks (never
    overwriting work already there):
      - doc_upload_id   (the receipt — always wins from the other row
                         if canonical lacks one; this is the whole point)
      - budget_line_id  (only if canonical has none)
      - account_code / account_code_name (only if canonical has none)
      - note            (concatenated if both have notes)

    Result: canonical row gets source='reconciled', match_status='confirmed'.
    The losing row is deleted (its FK row count is small — no children).

    Both rows must belong to the same project (pid). User must have at
    least editor role on the project.
    """
    _require_project_role(pid, 'editor')
    ProjectSheet.query.get_or_404(pid)
    data = request.get_json(force=True) or {}
    tids = data.get("tids") or []
    if not isinstance(tids, list) or len(tids) != 2:
        return jsonify({"error": "tids must be a list of exactly 2 transaction ids"}), 400
    try:
        tid_a, tid_b = int(tids[0]), int(tids[1])
    except (TypeError, ValueError):
        return jsonify({"error": "tids must be integers"}), 400
    if tid_a == tid_b:
        return jsonify({"error": "Cannot merge a transaction with itself"}), 400

    a = pool_txn(pid, tid_a)
    b = pool_txn(pid, tid_b)
    if not a or not b:
        return jsonify({"error": "Both transactions must exist on this project"}), 404

    # Pick canonical — the one we keep.
    def _canon_priority(t):
        # Lower tuple wins.
        return (
            0 if t.qbo_txn_id else 1,
            0 if t.account_code else 1,
            0 if t.budget_line_id else 1,
            t.id,
        )
    if _canon_priority(a) <= _canon_priority(b):
        canon, loser = a, b
    else:
        canon, loser = b, a

    # Refuse to merge two DISTINCT QuickBooks charges — deleting the loser would
    # destroy a real bank record AND, since its qbo_txn_id would no longer exist
    # in any row, the next sync would re-import it as a fresh duplicate. (Code
    # review 2026-06-04.)
    if (a.qbo_txn_id and b.qbo_txn_id and a.qbo_txn_id != b.qbo_txn_id):
        return jsonify({"error": "Both rows are distinct QuickBooks charges "
                                 "(different qbo_txn_id). Merging would delete a "
                                 "real bank record — unmatch one first if it's wrong."}), 400

    _before = {
        'canon_id':           canon.id,
        'canon_source':       canon.source,
        'canon_doc_upload_id': canon.doc_upload_id,
        'canon_budget_line_id': canon.budget_line_id,
        'canon_account_code': canon.account_code,
        'canon_qbo_txn_id':   canon.qbo_txn_id,
        'loser_id':           loser.id,
        'loser_source':       loser.source,
        'loser_doc_upload_id': loser.doc_upload_id,
        'loser_budget_line_id': loser.budget_line_id,
        'loser_account_code': loser.account_code,
        'loser_qbo_txn_id':   loser.qbo_txn_id,
    }

    # ── Transfer fields onto canonical (only fill blanks) ──────────────
    # The receipt is the whole point of the manual merge, so doc_upload_id
    # always comes over if canonical lacks one. If both rows have a doc
    # (rare — would mean two receipts for the same charge) we keep
    # canonical's and note the conflict.
    transferred = []
    doc_conflict = False
    if loser.doc_upload_id and not canon.doc_upload_id:
        canon.doc_upload_id = loser.doc_upload_id
        transferred.append('doc_upload_id')
    elif loser.doc_upload_id and canon.doc_upload_id and loser.doc_upload_id != canon.doc_upload_id:
        doc_conflict = True
    if loser.budget_line_id and not canon.budget_line_id:
        canon.budget_line_id = loser.budget_line_id
        transferred.append('budget_line_id')
    if loser.account_code and not canon.account_code:
        canon.account_code      = loser.account_code
        canon.account_code_name = loser.account_code_name
        transferred.append('account_code')
    # Notes: concatenate distinct ones so user-typed context isn't lost.
    if loser.note and (loser.note or '').strip() != (canon.note or '').strip():
        if canon.note:
            canon.note = f"{canon.note}\n[merged from #{loser.id}] {loser.note}"
        else:
            canon.note = loser.note
        transferred.append('note')

    # Vendor: prefer the receipt-side vendor when one of the two rows is
    # doc-only (its OCR'd value is human-readable — "Amazon", "Lyft"
    # vs QBO's processor noise "AMAZON MKTPL*BJ3NU6KN0"). Only swap if
    # the canonical row currently holds the QBO-side string AND the
    # other row carries the doc-side string. Same rationale as the
    # reconciliation path in qbo_sync.py (2026-05-15).
    if (loser.doc_upload_id and not canon.doc_upload_id
            and loser.vendor and loser.vendor != canon.vendor):
        canon.vendor = loser.vendor
        transferred.append('vendor')

    # ── Promote canonical to 'reconciled' / 'confirmed' ────────────────
    # Keep qbo_txn_id intact (we only transfer onto canonical when it
    # lacks one — see below).
    if loser.qbo_txn_id and not canon.qbo_txn_id:
        canon.qbo_txn_id     = loser.qbo_txn_id
        canon.qbo_txn_type   = loser.qbo_txn_type
        canon.qbo_account_id = loser.qbo_account_id
        canon.qbo_category   = loser.qbo_category
        transferred.append('qbo_txn_id')

    canon.source       = 'reconciled'
    canon.match_status = 'confirmed' if canon.budget_line_id or canon.account_code else canon.match_status
    canon.updated_at   = datetime.utcnow()
    # The merged row is this project's (a pooled QuickBooks charge is claimed
    # by the merge — the receipt / coding came from here).
    try:
        claim_for(canon, pid, why='merged with a row of this project')
    except ClaimError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 409

    # Clear FK on loser BEFORE delete so cascade-on-delete behavior is
    # predictable (we don't want the loser's doc_upload_id row touched —
    # the DocUpload itself stays, just transferred to canonical).
    loser.doc_upload_id   = None
    loser.budget_line_id  = None

    # Reparent the loser's invoice-split children onto the canonical row before
    # deleting it — otherwise the parent_transaction_id FK blocks the delete on
    # Postgres (500 mid-merge) / orphans them on SQLite. (Code review 2026-06-04.)
    _reparented = (Transaction.query
                   .filter(Transaction.parent_transaction_id == loser.id)
                   .update({"parent_transaction_id": canon.id},
                           synchronize_session=False))
    if _reparented:
        transferred.append(f'{_reparented}_child_split(s)_reparented')

    _loser_amt    = float(loser.amount or 0)
    _loser_vendor = loser.vendor or ''
    _loser_id     = loser.id

    db.session.delete(loser)
    db.session.commit()

    # Activity log: one entry on the surviving row recording what was
    # merged in. Helps the audit trail explain "why is this txn marked
    # 'reconciled' even though we didn't run a QBO sync?".
    try:
        _log_activity(
            action='update', entity_type='transaction',
            entity_id=canon.id,
            entity_label=(canon.vendor or canon.note or f'Txn #{canon.id}')[:80],
            project_id=pid,
            before=_before,
            after={'canon_id': canon.id, 'source': canon.source,
                   'doc_upload_id': canon.doc_upload_id,
                   'budget_line_id': canon.budget_line_id,
                   'account_code': canon.account_code,
                   'qbo_txn_id': canon.qbo_txn_id,
                   'transferred': transferred,
                   'deleted_loser_id': _loser_id},
            note=f'Manually merged duplicate ${_loser_amt:,.2f} {_loser_vendor} '
                 f'(loser #{_loser_id}) into this row')
    except Exception: pass

    return jsonify({
        "ok": True,
        "canonical_id": canon.id,
        "deleted_id":   _loser_id,
        "transferred":  transferred,
        "doc_conflict": doc_conflict,
    })


@app.route("/projects/<int:pid>/actuals/ai-cleanup", methods=["POST"])
@login_required
def actuals_ai_cleanup(pid):
    """Batch vendor cleanup over charges with NO receipt (CSV/QBO/manual) — the
    Actuals-side counterpart to /docs/ai-cleanup. Small batch per call; returns
    `remaining` so the UI loops until dry. (User 2026-06-18.)"""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    body = request.get_json(silent=True) or {}
    try:
        limit = min(15, max(1, int(body.get('limit', 8))))
    except (TypeError, ValueError):
        limit = 8
    force = bool(body.get('force'))
    q = (Transaction.query
         .filter(pool_filter(pid),
                 Transaction.vendor.isnot(None),
                 Transaction.doc_upload_id.is_(None),   # doc-linked charges clean via the doc
                 Transaction.source.in_(('csv_import', 'qbo_sync', 'manual_entry', 'reconciled'))))
    if not force:
        q = q.filter(Transaction.ai_cleaned_at.is_(None))
    total_remaining = q.count()
    rows = q.order_by(Transaction.id.desc()).limit(limit).all()
    known = _ai_known_vendors(pid)
    applied = 0
    flagged = 0
    for t in rows:
        try:
            r = _ai_clean_transaction(t, known=known, force=force)
            if r and r.get('applied'):
                applied += 1
            if r and r.get('flagged'):
                flagged += 1
        except Exception as e:
            logging.warning(f"[actuals/ai-cleanup] txn #{getattr(t,'id','?')} failed: {e}")
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
    return jsonify({"ok": True, "processed": len(rows), "applied": applied,
                    "flagged": flagged, "remaining": max(0, total_remaining - len(rows))})


@app.route("/projects/<int:pid>/actuals/ai-suggest-codes", methods=["POST"])
@login_required
def actuals_ai_suggest_codes(pid):
    """Batch: run AI categorize over UNCODED charges and store advisory
    suggestions. Processes a small batch per call (memory hits are instant;
    LLM calls aren't) and returns `remaining` so the UI can loop until dry.
    Body: {limit?, force?}. (User 2026-06-18.)"""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    body = request.get_json(silent=True) or {}
    try:
        limit = min(20, max(1, int(body.get('limit', 10))))
    except (TypeError, ValueError):
        limit = 10
    force = bool(body.get('force'))
    q = (Transaction.query
         .filter(pool_filter(pid),
                 Transaction.account_code.is_(None),
                 Transaction.not_project_expense == False,   # noqa: E712
                 Transaction.vendor.isnot(None)))
    # Skip itemized containers (their sublines carry the coding) and the
    # sublines' own source. (Review fix 2026-07.)
    _split_parents_q = (db.session.query(Transaction.parent_transaction_id)
                        .filter(Transaction.project_id == pid,
                                Transaction.source == 'invoice_split',
                                Transaction.parent_transaction_id.isnot(None)))
    q = q.filter(Transaction.source != 'invoice_split',
                 ~Transaction.id.in_(_split_parents_q))
    if not force:
        q = q.filter(Transaction.ai_suggested_code.is_(None))
    total_remaining = q.count()
    rows = q.order_by(Transaction.id.desc()).limit(limit).all()
    taxonomy = _ai_coa_taxonomy()
    past = _ai_past_corrections(pid)
    suggested = 0
    no_match = 0
    for txn in rows:
        try:
            if _ai_suggest_code_for_txn(txn, taxonomy=taxonomy, past=past, force=force):
                suggested += 1
            else:
                no_match += 1
        except Exception as e:
            logging.warning(f"[ai-suggest] txn #{getattr(txn,'id','?')} failed: {e}")
            no_match += 1
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
    return jsonify({"ok": True, "processed": len(rows), "suggested": suggested,
                    "no_match": no_match, "remaining": max(0, total_remaining - len(rows))})


@app.route("/projects/<int:pid>/actuals/clear-suggestions", methods=["POST"])
@login_required
def actuals_clear_suggestions(pid):
    """Wipe all AI code suggestions on a project's charges so 'Suggest codes' can
    be re-run from scratch (handy for testing). Does NOT touch confirmed coding.
    (User 2026-06-18.)"""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    n = (Transaction.query
         .filter(pool_filter(pid),
                 Transaction.ai_suggested_code.isnot(None))
         .update({Transaction.ai_suggested_code: None,
                  Transaction.ai_suggested_code_name: None,
                  Transaction.ai_code_confidence: None,
                  Transaction.ai_code_reason: None,
                  # Also drop the deterministic person/PO line pointer so the
                  # inbox "✓ Code" chip clears with the rest. (2026-07.)
                  Transaction.suggested_budget_line_id: None},
                 synchronize_session=False))
    db.session.commit()
    return jsonify({"ok": True, "cleared": n})


@app.route("/projects/<int:pid>/actuals/ai-match", methods=["POST"])
@login_required
def actuals_ai_match(pid):
    """AI-driven matching: for each still-unmatched charge, feed its heuristic
    candidate receipts to the AI to pick the best (or none) and write a SUGGESTED
    match — surfaced in the gold 'Suggested match' banner for one-click Confirm.
    Batched (returns `remaining`); the per-charge watermark re-opens whenever a
    newer receipt arrives (2026-10-30). Core shared with the reconcile panel."""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    body = request.get_json(silent=True) or {}
    try:
        limit = min(12, max(1, int(body.get('limit', 6))))
    except (TypeError, ValueError):
        limit = 6
    force = bool(body.get('force'))
    try:
        return jsonify(_ai_match_batch(pid, limit=limit, force=force))
    except Exception as e:
        logging.exception("[ai-match] failed")
        return jsonify({"error": str(e)}), 500


@app.route("/projects/<int:pid>/actuals/clear-match-suggestions", methods=["POST"])
@login_required
def actuals_clear_match_suggestions(pid):
    """Undo all SUGGESTED (not confirmed) matches + reset the AI-match watermark,
    so the AI match pass can be re-run from scratch (testing). (User 2026-06-18.)"""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    n = 0
    for _t in (Transaction.query
               .filter(Transaction.project_id == pid,
                       Transaction.match_status == 'suggested').all()):
        _t.match_status = 'unmatched'
        _t.doc_upload_id = None
        _t.match_confidence = None
        _t.match_source = None
        release(_t, why='match suggestions cleared')   # a suggestion-only claim goes back to the pool
        n += 1
    (Transaction.query
     .filter(pool_filter(pid),
             Transaction.ai_match_checked_at.isnot(None))
     .update({Transaction.ai_match_checked_at: None}, synchronize_session=False))
    db.session.commit()
    return jsonify({"ok": True, "cleared": n})


@app.route("/projects/<int:pid>/actuals/unmatch-all", methods=["POST"])
@login_required
def actuals_unmatch_all(pid):
    """TEST RESET: unlink EVERY charge from its receipt and return all matched
    charges (suggested AND confirmed) to the unmatched pool, reverting
    reconciled→csv_import so they re-enter the candidate pool, and clearing the
    AI-match watermark. Receipts (DocUpload) become available candidates again.
    Body {confirm:true} required — intended for a sandbox project. (User 2026-06-18.)"""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    body = request.get_json(silent=True) or {}
    if not body.get('confirm'):
        return jsonify({"error": "pass {confirm: true} to confirm this reset"}), 400
    reverted = (Transaction.query
                .filter(Transaction.project_id == pid, Transaction.source == 'reconciled')
                .update({Transaction.source: 'csv_import'}, synchronize_session=False))
    unmatched = 0
    for _t in (Transaction.query
               .filter(Transaction.project_id == pid,
                       Transaction.source.in_(('qbo_sync', 'csv_import')),
                       db.or_(Transaction.doc_upload_id.isnot(None),
                              Transaction.match_status != 'unmatched',
                              Transaction.ai_match_checked_at.isnot(None))).all()):
        _t.doc_upload_id = None
        _t.match_status = 'unmatched'
        _t.match_confidence = None
        _t.match_source = None
        _t.ai_match_checked_at = None
        release(_t, why='unmatch-all reset')   # receipt-only claims return to the pool
        unmatched += 1
    # Also clear any 'remembered' rejections so nothing is suppressed in the re-test.
    try:
        from models import MatchRejection
        MatchRejection.query.filter_by(project_id=pid).delete(synchronize_session=False)
    except Exception:
        pass
    db.session.commit()
    return jsonify({"ok": True, "unmatched": unmatched, "reverted_reconciled": reverted})


@app.route("/projects/<int:pid>/actuals/scan-anomalies", methods=["POST"])
@login_required
def actuals_scan_anomalies(pid):
    """Scan for issues and upsert AnomalyFlag review-queue rows: double-coded
    duplicates, budget mismatches (POs over cap, lines over plan), and people→line
    coding suggestions (invoice person → their budget line). (User 2026-06-18.)"""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    clusters, flags = _run_double_coded_scan(pid)
    dup_receipt_flags = _run_duplicate_receipt_scan(pid)
    budget_flags = _run_budget_mismatch_scan(pid)
    people_flags = _run_people_line_scan(pid)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
    return jsonify({"ok": True, "clusters": clusters, "flags": flags,
                    "dup_receipt_flags": dup_receipt_flags,
                    "budget_flags": budget_flags, "people_flags": people_flags,
                    "total_flags": flags + dup_receipt_flags + budget_flags + people_flags})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/dismiss-code", methods=["POST"])
@login_required
def actuals_dismiss_code(pid, tid):
    """Dismiss an AI code SUGGESTION (from the 'Codes to confirm' queue) without
    coding the charge — clears the ai_suggested_* fields so it stops surfacing.
    (Phase 2, 2026-06-22.)"""
    _require_project_role(pid, 'editor')
    t = _pool_txn_or_404(pid, tid)
    t.ai_suggested_code = None
    t.ai_suggested_code_name = None
    t.ai_code_confidence = None
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/set-coa", methods=["POST"])
@login_required
def actuals_set_coa(pid, tid):
    """Drag-drop coding: set just the COA section code without picking
    a specific budget line. Used when the user drags a COA section from
    the sidebar onto a transaction row. The dropdown remains the way to
    pick a specific sub-line (which triggers Working→Actual auto-clone).
    """
    txn = _pool_txn_or_404(pid, tid)
    data = request.get_json(force=True) or {}
    if data.get("account_code") and data.get("account_code") != "not_project":
        # not_project is excluded: flagging a document as "not a project
        # expense" must not activate it as one.
        _ge = _guard_doc_expense(txn, data)
        if _ge is not None:
            return _ge
    code_raw = data.get("account_code")
    name_raw = (data.get("account_code_name") or "").strip()
    # Itemized container: sublines carry the coding — block re-coding (incl.
    # not_project) so the doc can't double-count. Clearing stays allowed.
    if code_raw and _txn_is_split_parent(txn):
        return jsonify({"error": _split_parent_error(txn)}), 409
    _coa_before = {'account_code': txn.account_code,
                   'account_code_name': txn.account_code_name,
                   'not_project_expense': bool(txn.not_project_expense)}

    _np_mode = None
    if code_raw == "not_project":
        try:
            _np_mode = mark_not_project(txn, pid, None, current_user.id)
        except ClaimError as e:
            db.session.rollback()
            return jsonify({"error": str(e)}), 409
        txn.budget_line_id      = None
        txn.account_code        = None
        txn.account_code_name   = None
        txn.match_status        = 'unmatched'
    elif not code_raw:
        # Clear coding
        txn.account_code        = None
        txn.account_code_name   = None
        txn.budget_line_id      = None
        txn.match_status        = 'unmatched'
    else:
        try:
            code = int(code_raw)
        except (TypeError, ValueError):
            return jsonify({"error": "account_code must be int or 'not_project'"}), 400
        # Resolve canonical name from FP_COA_SECTIONS if not provided.
        if not name_raw:
            name_raw = dict(FP_COA_SECTIONS).get(code, '')
        txn.account_code      = code
        txn.account_code_name = name_raw[:100]
        txn.not_project_expense = False
        # Drag-drop sets the COA section but no specific line. Clear any
        # existing budget_line_id so the row actually MOVES to the new
        # section — otherwise a txn dragged off a specific line keeps that
        # stale line_id and stays stuck under it in the Actual budget (the
        # per-line cell keys on budget_line_id). (User 2026-06-26 —
        # "Consulting Engineer items never move".) The user then picks a
        # sub-line via the dropdown, which triggers Working→Actual auto-clone.
        txn.budget_line_id = None
        # Auto-add a placeholder line under this section if none exists
        # (per user 2026-05-01: a section coded from Actuals should
        # appear on the Budget view, not vanish silently).
        try:
            from actuals import ensure_section_in_working_budget
            _ensure_result = ensure_section_in_working_budget(
                pid, code, name_raw)
        except Exception as _ee:
            logging.warning(f"[actuals/set-coa] ensure-section failed: {_ee}")
            _ensure_result = {'created': False, 'working_was_just_created': False}
    # Coding decided → drop any advisory AI suggestion on this row.
    txn.ai_suggested_code = None
    txn.ai_suggested_code_name = None
    txn.ai_code_confidence = None
    txn.ai_code_reason = None
    txn.suggested_budget_line_id = None
    txn.updated_at = datetime.utcnow()
    # Company ledger: a section or a not-project mark claims the charge for
    # this project; a clear with nothing else on the row releases it.
    try:
        if txn.account_code:
            claim_for(txn, pid, why='coded to a section')
        elif _np_mode != 'dismissed':
            release(txn, why='section coding cleared')
    except ClaimError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 409
    # Reinforce the learned vendor→category mapping when a real section was set
    # (so AI/memory can auto-suggest this vendor next time). (2026-06-18.)
    if txn.account_code and not txn.not_project_expense and txn.vendor:
        try:
            from actuals import record_vendor_category
            record_vendor_category(pid, txn.vendor, txn.account_code)
        except Exception as _ve:
            logging.warning(f"[set-coa] vendor-cat record failed: {_ve}")
    db.session.commit()
    _act = None
    try:
        _amt = float(txn.amount or 0)
        if txn.not_project_expense:
            _note = f'Marked ${_amt:,.2f} as not a project expense'
        elif txn.account_code:
            _note = f'Set section: ${_amt:,.2f} → {txn.account_code} {txn.account_code_name or ""}'.strip()
        else:
            _note = 'Cleared section coding'
        _act = _log_activity(
            action='update', entity_type='transaction',
            entity_id=txn.id,
            entity_label=(txn.vendor or txn.note or f'Txn #{txn.id}')[:80],
            project_id=pid,
            before=_coa_before,
            after={'account_code': txn.account_code,
                   'account_code_name': txn.account_code_name,
                   'not_project_expense': bool(txn.not_project_expense)},
            note=_note)
    except Exception: pass
    return jsonify({
        "ok":              True,
        "transaction_id":  tid,
        "activity_id":     (_act.id if _act else None),
        "account_code":    txn.account_code,
        "account_code_name": txn.account_code_name,
        "not_project_expense": txn.not_project_expense,
        "budget_line_auto_created":     bool(locals().get('_ensure_result', {}).get('created')),
        "working_was_just_created":     bool(locals().get('_ensure_result', {}).get('working_was_just_created')),
    })


@app.route("/projects/<int:pid>/actuals/transactions/set-line-bulk", methods=["POST"])
@login_required
def actuals_set_line_bulk(pid):
    """Assign MANY transactions to one budget line (or COA section) in a SINGLE
    request. Previously the UI fired one POST per row, which was slow for big
    batches. Reuses link_transaction_to_line so the Working→Actual clone + line
    translation are identical to single-row coding. (User 2026-06-03.)"""
    if current_user.role not in ('super_admin', 'admin'):
        access = ProjectAccess.query.filter_by(project_id=pid, user_id=current_user.id).first()
        if not access:
            return jsonify({"error": "Forbidden"}), 403
    data = request.get_json(silent=True) or {}
    tids = data.get("transaction_ids") or []
    line_id = data.get("budget_line_id")
    account_code = data.get("account_code")
    if not isinstance(tids, list) or not tids:
        return jsonify({"error": "no transaction_ids"}), 400
    if not line_id and not account_code:
        return jsonify({"error": "budget_line_id or account_code required"}), 400

    from actuals import link_transaction_to_line
    updated, failed, errors, line_label = 0, 0, [], None
    # Phase 3 (2026-11-01): one ActivityLog row PER transaction with its
    # before-state, so the workspace's undo toast can revert the whole drop
    # through activity/undo-batch. The summary row at the end stays.
    activity_ids = []

    # Section-only: ensure the section exists on the Working budget once (not
    # per row), mirroring set-coa.
    if account_code and not line_id:
        try:
            code_int = int(account_code)
            nm = dict(FP_COA_SECTIONS).get(code_int, '')
            from actuals import ensure_section_in_working_budget
            ensure_section_in_working_budget(pid, code_int, nm)
        except Exception as _e:
            logging.warning(f"[set-line-bulk] ensure-section failed: {_e}")

    for tid in tids:
        try:
            txn = pool_txn(pid, int(tid))
            if not txn:
                failed += 1
                continue
            # Receipts can't be bulk-coded into spend (2026-10-30 rule:
            # only charges actualize) — skip them, count as failed.
            _ge_bulk = _guard_doc_expense(txn, data)
            if _ge_bulk is not None:
                failed += 1
                continue
            if _txn_is_split_parent(txn):
                failed += 1
                if len(errors) < 10:
                    errors.append(f"#{tid}: split or itemized parent")
                continue
            _row_before = {'budget_line_id': txn.budget_line_id,
                           'account_code': txn.account_code,
                           'account_code_name': txn.account_code_name,
                           'not_project_expense': bool(txn.not_project_expense)}
            _row_label = (txn.vendor or txn.note or f'Txn #{txn.id}')[:80]
            _row_amt = float(txn.amount or 0)
            if line_id:
                link_transaction_to_line(int(tid), int(line_id), user_id=current_user.id)
                db.session.refresh(txn)     # it commits + claim-syncs itself
                if line_label is None:
                    line_label = txn.account_code_name or (str(txn.account_code) if txn.account_code else None)
            else:
                code_int = int(account_code)
                nm = dict(FP_COA_SECTIONS).get(code_int, '')
                txn.account_code = code_int
                txn.account_code_name = nm[:100]
                txn.budget_line_id = None
                txn.not_project_expense = False
                txn.match_status = 'confirmed'
                txn.updated_at = datetime.utcnow()
                claim_for(txn, pid, why='coded to a section')
                db.session.commit()
                if line_label is None:
                    line_label = (str(code_int) + ' ' + nm).strip()
            updated += 1
            try:
                _row_act = _log_activity(
                    action='update', entity_type='transaction', entity_id=txn.id,
                    entity_label=_row_label, project_id=pid,
                    before=_row_before,
                    after={'budget_line_id': txn.budget_line_id,
                           'account_code': txn.account_code,
                           'account_code_name': txn.account_code_name,
                           'not_project_expense': False},
                    dollar_delta=0,
                    note=f'Coded ${_row_amt:,.2f} → {txn.account_code or ""} {txn.account_code_name or ""}'.strip())
                if _row_act is not None:
                    activity_ids.append(_row_act.id)
            except Exception:
                pass
        except Exception as e:
            db.session.rollback()
            failed += 1
            if len(errors) < 10:
                errors.append(f"#{tid}: {type(e).__name__}")
    try:
        _log_activity(action='update', entity_type='transaction', entity_id=0,
                      entity_label=f'Bulk-coded {updated} txns',
                      project_id=pid, before=None,
                      after={'line_label': line_label, 'count': updated},
                      note=f'Bulk-assigned {updated} transaction(s) to {line_label or "line"}')
    except Exception:
        pass
    return jsonify({"ok": True, "updated": updated, "failed": failed,
                    "errors": errors, "line_label": line_label,
                    "activity_ids": activity_ids})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/edit", methods=["POST"])
@login_required
def actuals_edit_transaction(pid, tid):
    """Edit Transaction modal: vendor / amount / txn_date / note.
    Mirrors the Docs detail modal's update endpoint but for the
    Actuals-side row. Also propagates back to the linked DocUpload
    (when one exists) so the two views stay in lockstep."""
    txn = _pool_txn_or_404(pid, tid)
    data = request.get_json(force=True) or {}
    _edit_before = {'vendor': txn.vendor,
                    'amount': float(txn.amount) if txn.amount is not None else None,
                    'txn_date': txn.txn_date,
                    'note': txn.note,
                    'no_charge_reason': getattr(txn, 'no_charge_reason', None)}

    if "vendor" in data:
        v = (data.get("vendor") or "").strip()
        txn.vendor = v[:300] if v else None
    if "amount" in data:
        a = data.get("amount")
        if a in (None, "", "null"):
            _new_amt = None
        else:
            try:
                _new_amt = round(float(a), 2)
            except (TypeError, ValueError):
                return jsonify({"error": "amount must be a number"}), 400
        # Cleanup A #16 (2026-11-02): a bank-sourced amount is what the bank
        # says it is, and a split row's amount must keep matching its split
        # sum — the old code changed either silently. Rule in
        # actuals.amount_edit_allowed (unit-tested).
        from actuals import amount_edit_allowed as _amt_ok
        _split_amts = [sp.amount for sp in
                       TransactionSplit.query.filter_by(transaction_id=txn.id).all()]
        _ok, _why = _amt_ok(txn, _new_amt if _new_amt is not None else 0, _split_amts)
        if not _ok:
            return jsonify({"error": _why}), 409
        txn.amount = _new_amt
    if "txn_date" in data:
        d = (data.get("txn_date") or "").strip()
        if not d:
            txn.txn_date = None
        else:
            try:
                from datetime import datetime as _dt_mod
                _ = _dt_mod.strptime(d[:10], "%Y-%m-%d")  # validate
                txn.txn_date = d[:10]
            except Exception:
                return jsonify({"error": "txn_date must be YYYY-MM-DD"}), 400
    if "note" in data:
        n = (data.get("note") or "").strip()
        txn.note = n[:500] if n else None
    # Missing-backup reason (ACTUALS_HANDOFF Phase 3, 2026-11-01): Concur's
    # Missing Receipt Declaration. A short reason stands in for a receipt
    # the person cannot produce — it clears the red Needs-backup flag and
    # shows as a yellow chip. Empty string clears it.
    if "no_charge_reason" in data:
        _ncr = (data.get("no_charge_reason") or "").strip()
        txn.no_charge_reason = _ncr[:300] if _ncr else None
    txn.updated_at = datetime.utcnow()

    # Sync back to the linked DocUpload so the Docs tab reflects the
    # same vendor/amount/date. Symmetric to the /docs/upload/<uid>/update
    # path that pushes the other direction. NEVER from an invoice_split
    # child — a subline edit must not overwrite the DOCUMENT's total with
    # one line's amount. (2026-07-20, same family as the doc→txn guard.)
    if txn.doc_upload_id and txn.source != 'invoice_split':
        doc = DocUpload.query.get(txn.doc_upload_id)
        if doc:
            doc.vendor = txn.vendor
            doc.amount = txn.amount
            if txn.txn_date:
                try:
                    from datetime import datetime as _dt_mod
                    doc.doc_date = _dt_mod.strptime(txn.txn_date[:10], "%Y-%m-%d").date()
                except Exception:
                    pass

    # Smart vendor rename: when the vendor changes, propagate the new name to
    # every other item in this project that shares the old vendor's canonical key
    # and learn the alias for future imports. (User 2026-06-22.)
    _vendor_prop = {"count": 0, "items": [], "canon": ""}
    if "vendor" in data and txn.vendor:
        _old_v = _edit_before.get('vendor')
        # Only when the vendor genuinely changed — re-saving an unchanged row must
        # not sweep canonical siblings.
        if _old_v and (_old_v or '').strip() != (txn.vendor or '').strip():
            from actuals import propagate_vendor_rename
            _vendor_prop = propagate_vendor_rename(pid, _old_v, txn.vendor, exclude_tid=txn.id)

    db.session.commit()
    _act = None
    try:
        _edit_after = {'vendor': txn.vendor,
                       'amount': float(txn.amount) if txn.amount is not None else None,
                       'txn_date': txn.txn_date,
                       'note': txn.note,
                       'no_charge_reason': txn.no_charge_reason}
        _amt_b = _edit_before.get('amount') or 0
        _amt_a = _edit_after.get('amount') or 0
        _delta = float(_amt_a) - float(_amt_b)
        _act = _log_activity(
            action='update', entity_type='transaction',
            entity_id=txn.id,
            entity_label=(txn.vendor or txn.note or f'Txn #{txn.id}')[:80],
            project_id=pid,
            before=_edit_before, after=_edit_after,
            dollar_delta=_delta,
            note=('Added a missing-backup reason' if ("no_charge_reason" in data and len(data) == 1)
                  else 'Edited transaction details'))
    except Exception: pass
    return jsonify({
        "ok":     True,
        "activity_id": (_act.id if _act else None),
        "no_charge_reason": txn.no_charge_reason,
        "vendor": txn.vendor,
        "amount": float(txn.amount) if txn.amount is not None else None,
        "txn_date": txn.txn_date,
        "note":     txn.note,
        "vendor_propagation": _vendor_prop,
        "old_vendor": _edit_before.get('vendor'),
    })


@app.route("/projects/<int:pid>/actuals/vendor-rename/undo", methods=["POST"])
@login_required
def actuals_vendor_rename_undo(pid):
    """Revert a smart vendor-rename propagation: restore the prior vendor on each
    listed transaction/document and drop the learned alias for that canon."""
    data = request.get_json(force=True) or {}
    items = data.get("items") or []
    canon = (data.get("canon") or "").strip()
    n = 0
    for it in items:
        try:
            if it.get("type") == "txn":
                r = Transaction.query.filter_by(id=it.get("id"), project_id=pid).first()
            else:
                r = DocUpload.query.filter_by(id=it.get("id"), project_id=pid).first()
            if r is not None:
                r.vendor = (it.get("old") or None)
                n += 1
        except Exception:
            pass
    if canon:
        try:
            from models import VendorAlias
            VendorAlias.query.filter_by(project_id=pid, raw_canonical=canon).delete(
                synchronize_session=False)
        except Exception:
            pass
    db.session.commit()
    return jsonify({"ok": True, "reverted": n})


@app.route("/projects/<int:pid>/actuals/auto-match", methods=["POST"])
@login_required
def actuals_run_auto_match(pid):
    """Run the auto-matcher on every unmatched QBO transaction in the
    project. Pairs them with DocUpload-source transactions where amount,
    date, and vendor align. Writes match_status='suggested' — never
    silently 'confirmed'. User reviews via per-row Confirm/Override
    buttons."""
    project = ProjectSheet.query.get_or_404(pid)
    from actuals import run_auto_match
    try:
        result = run_auto_match(pid)
        try:
            _suggested = int(result.get('suggested') or 0)
            _scanned   = int(result.get('scanned') or 0)
            _log_activity(
                action='update', entity_type='qbo_sync',
                entity_id=pid, entity_label=project.name or f'Project #{pid}',
                project_id=pid,
                after={'suggested': _suggested, 'scanned': _scanned},
                note=f'Auto-match: {_suggested} suggestion{"s" if _suggested != 1 else ""} '
                     f'across {_scanned} unmatched txn{"s" if _scanned != 1 else ""}')
        except Exception: pass
        return jsonify({"ok": True, **result})
    except Exception as e:
        logging.exception(f"[actuals] auto-match failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/projects/<int:pid>/actuals/reconcile.json", methods=["GET"])
@login_required
def actuals_reconcile_json(pid):
    """Reconciliation triage (owner 2026-10-07: "upload all the receipts …
    and the software tell me: if you're missing this, find where they
    match up, find where they potentially match up, and find ones that
    are orphaned").

    Runs ensure_doc_txns + the auto-matcher, then returns three buckets:
      suggested         — charge ↔ receipt pairs awaiting one-click
                          Confirm / Not-a-match.
      charges_no_receipt — imported charges (bank/QBO/CSV) with no
                          receipt linked: the chase list.
      orphan_receipts   — receipts/invoices no charge claims (and not
                          logged as a cash expense): decide per receipt —
                          match by hand, log as cash/check expense, or
                          mark duplicate / not-project.
    """
    ProjectSheet.query.get_or_404(pid)
    from actuals import run_auto_match
    try:
        run_auto_match(pid)   # also ensures every receipt is matchable
    except Exception as _e:
        logging.warning(f"[reconcile] auto-match failed (continuing): {_e}")
    # Then a small AI batch over whatever the heuristics left (owner
    # 2026-10-30: matching should "just work" from this panel, not need a
    # separate button). The watermark keeps it incremental — once a charge
    # is checked it's skipped until a NEWER receipt arrives — so repeat
    # panel opens cost nothing when there's nothing new. Fail-open.
    try:
        _aim = _ai_match_batch(pid, limit=8)
        if _aim.get('suggested'):
            logging.info(f"[reconcile] AI batch suggested {_aim['suggested']} "
                         f"(ai_calls={_aim.get('ai_calls')})")
    except Exception as _ae:
        logging.warning(f"[reconcile] AI match batch failed (continuing): {_ae}")

    doc_name = {}
    doc_rows = DocUpload.query.filter(
        DocUpload.project_id == pid,
        DocUpload.category.in_(('receipt', 'invoice'))).all()
    live_docs = {d.id: d for d in doc_rows
                 if not getattr(d, 'is_duplicate', False)
                 and (d.status or '') not in ('duplicate', 'deleted')}
    for d in doc_rows:
        doc_name[d.id] = d.filed_filename or d.original_filename or f'Doc #{d.id}'

    txns = Transaction.query.filter(
        pool_filter(pid),
        Transaction.not_project_expense == False).all()

    suggested, charges_no_receipt = [], []
    claimed_docs = set()      # docs a charge points at (suggested or confirmed)
    cash_logged_docs = set()  # docs whose doc-born row is an activated expense
    for t in txns:
        if t.source == 'doc_upload':
            if t.doc_upload_id and getattr(t, 'activated_at', None):
                cash_logged_docs.add(t.doc_upload_id)
            continue
        if t.source in ('qbo_sync', 'csv_import', 'reconciled', 'manual_entry'):
            if t.doc_upload_id:
                claimed_docs.add(t.doc_upload_id)
            if t.match_status == 'suggested' and t.doc_upload_id:
                d = live_docs.get(t.doc_upload_id)
                suggested.append({
                    "tid": t.id, "vendor": t.vendor or '',
                    "amount": float(t.amount or 0), "date": t.txn_date or '',
                    "confidence": float(t.match_confidence or 0),
                    "doc_id": t.doc_upload_id,
                    "doc_name": doc_name.get(t.doc_upload_id, ''),
                    "doc_vendor": (d.vendor if d else '') or '',
                    "doc_amount": (float(d.amount) if d and d.amount is not None else None),
                })
            elif t.doc_upload_id is None and bool(t.is_expense):
                charges_no_receipt.append({
                    "tid": t.id, "vendor": t.vendor or '',
                    "amount": float(t.amount or 0), "date": t.txn_date or '',
                    "card": t.card_last4 or '', "source": t.source,
                    "coded": bool(t.budget_line_id or t.account_code),
                })

    orphan_receipts = []
    for did, d in live_docs.items():
        if did in claimed_docs or did in cash_logged_docs:
            continue
        orphan_receipts.append({
            "doc_id": did, "name": doc_name.get(did, ''),
            "vendor": d.vendor or '', "category": d.category or '',
            "amount": float(d.amount) if d.amount is not None else None,
            "date": d.doc_date.isoformat() if d.doc_date else '',
        })

    charges_no_receipt.sort(key=lambda r: (r["date"] or ''), reverse=True)
    orphan_receipts.sort(key=lambda r: (r["date"] or ''), reverse=True)
    suggested.sort(key=lambda r: -r["confidence"])
    return jsonify({
        "ok": True,
        "suggested": suggested[:200],
        "charges_no_receipt": charges_no_receipt[:300],
        "orphan_receipts": orphan_receipts[:300],
        "stats": {
            "suggested_n": len(suggested),
            "charges_no_receipt_n": len(charges_no_receipt),
            "charges_no_receipt_total": round(sum(abs(r["amount"]) for r in charges_no_receipt), 2),
            "orphan_receipts_n": len(orphan_receipts),
            "orphan_receipts_total": round(sum(abs(r["amount"] or 0) for r in orphan_receipts), 2),
        },
    })


def _ws_crew_options(pid):
    """Crew picker for Create expense / manual expense: active crew members,
    the project's own roster first (those with a CrewAssignment on the
    current Working budget), then everyone else alphabetically."""
    from models import CrewMember, CrewAssignment
    budget, lines = _ws_working_budget_and_lines(pid)
    on_project = set()
    if lines:
        try:
            on_project = {r[0] for r in db.session.query(CrewAssignment.crew_member_id)
                          .filter(CrewAssignment.budget_line_id.in_(list(lines.keys()))).all()
                          if r[0]}
        except Exception:
            on_project = set()
    crew = CrewMember.query.filter(db.or_(CrewMember.active.is_(True),
                                          CrewMember.active.is_(None))).all()
    crew.sort(key=lambda c: (0 if c.id in on_project else 1, (c.name or '').lower()))
    return [{"id": c.id, "name": c.name,
             "department": c.department or '',
             "on_project": c.id in on_project} for c in crew]


@app.route("/projects/<int:pid>/actuals")
@login_required
def actuals_workspace_page(pid):
    """THE Actuals workspace (ACTUALS_HANDOFF Phase 3, 2026-11-01): one
    screen per project, two panes — the transaction pool on the left in
    six views (Unclaimed, Assigned, Needs backup, Receipts, Not project,
    Sync), the current Working budget on the right as drop targets.
    Replaces the /register page and the legacy #tab-actuals on the budget
    page. Data comes from actuals_workspace_json; the page only carries
    what the JS needs before the first fetch (line-picker options, crew
    picker, the budget id the undo route wants, and ?txn= to focus).

    Editor or better: every action here is a mutation, and a viewer has
    the budget page's read-only rollup."""
    from actuals import build_line_picker_options
    project = ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    _pick_budget, _pick_groups = _line_picker_groups(pid)
    line_groups = build_line_picker_options(_pick_groups)
    try:
        focus_txn = int(request.args.get('txn') or 0) or None
    except (TypeError, ValueError):
        focus_txn = None
    view = (request.args.get('view') or '').strip().lower()
    if view not in ('unclaimed', 'assigned', 'needs_backup', 'receipts', 'duplicates', 'not_project', 'sync'):
        view = ''
    return render_template("actuals.html", project=project,
                           budget=_pick_budget,
                           budget_id=(_pick_budget.id if _pick_budget else None),
                           line_groups=line_groups,
                           crew=_ws_crew_options(pid),
                           focus_txn=focus_txn, initial_view=view,
                           can_sync=bool(getattr(current_user, 'role', None) in ('super_admin', 'admin')))


@app.route("/projects/<int:pid>/actuals/scan-dup-transactions", methods=["GET", "POST"])
@login_required
def actuals_scan_dup_transactions(pid):
    """READ-ONLY: cluster likely-duplicate transactions so the user can confirm
    they're separate (or remove an accidental re-import). Conservative signals:
      • same qbo_txn_id (a re-imported QBO record — near-certain), OR
      • same |amount| + same date + same vendor + same card_last4.
    Skips intentional split_group sets and clusters already dismissed as
    separate. A cluster mixing a doc_upload row with an electronic charge is
    'matchable' (link, don't delete). (User 2026-06-17.)"""
    if not current_user.is_authenticated:
        return jsonify({"error": "Not authenticated"}), 401
    ProjectSheet.query.get_or_404(pid)
    if current_user.role not in ('super_admin', 'admin'):
        access = ProjectAccess.query.filter_by(project_id=pid, user_id=current_user.id).first()
        if not access or access.role not in ('owner', 'collaborator', 'editor'):
            return jsonify({"error": "Forbidden"}), 403
    import re as _re_dup
    from collections import defaultdict as _dd_dup
    txns = (Transaction.query.filter_by(project_id=pid)
            .filter(Transaction.not_project_expense == False,   # noqa: E712
                    Transaction.source != 'invoice_split').all())   # itemized sublines aren't dups (2026-07)
    dismissed = {d.dup_key for d in TransactionDupDismissal.query.filter_by(project_id=pid).all()}
    # Receipts already carried by an electronic charge — a doc_upload row whose
    # receipt is in here is a redundant PHANTOM (the charge is the real ledger
    # row), so it's safe to remove and de-double-count. (User 2026-06-17.)
    elec_doc_ids = {t.doc_upload_id for t in txns
                    if t.source in ('qbo_sync', 'csv_import', 'reconciled') and t.doc_upload_id}

    def _vnorm(v):
        return _re_dup.sub(r'[^a-z0-9]', '', (v or '').lower())[:18]

    def _row(t):
        phantom = (t.source == 'doc_upload' and t.doc_upload_id in elec_doc_ids)
        return {"tid": t.id, "source": t.source, "vendor": t.vendor,
                "amount": float(t.amount) if t.amount is not None else None,
                "date": (t.txn_date[:10] if t.txn_date else None), "card": t.card_last4,
                "coded": t.account_code_name or (str(t.account_code) if t.account_code else None),
                "doc_upload_id": t.doc_upload_id, "match_status": t.match_status,
                "phantom": phantom,
                # deletable: an electronic re-import with no receipt, OR a
                # redundant doc_upload phantom (receipt represented elsewhere).
                "deletable": ((t.source in ('qbo_sync', 'csv_import', 'manual_entry') and not t.doc_upload_id)
                              or phantom)}

    clusters, seen = [], set()
    # Tier 1 — same qbo_txn_id (definite re-import; confirmed rows flip to
    # source='reconciled', so only still-qbo_sync re-imports cluster here).
    by_qid = _dd_dup(list)
    for t in txns:
        if t.source == 'qbo_sync' and t.qbo_txn_id:
            by_qid[t.qbo_txn_id].append(t)
    for qid, members in by_qid.items():
        if len(members) < 2:
            continue
        key = 'qbo:' + str(qid)
        if key in dismissed:
            continue
        clusters.append({"key": key, "kind": "reimport",
                         "amount": abs(float(members[0].amount or 0)),
                         "rows": [_row(t) for t in members]})
        seen.update(t.id for t in members)
    # Tier 2 — same |amount| + date + vendor + card.
    by_attr = _dd_dup(list)
    for t in txns:
        if t.id in seen or t.amount is None or not t.txn_date or not t.vendor or not t.card_last4:
            continue
        akey = '%.2f|%s|%s|%s' % (abs(float(t.amount)), t.txn_date[:10], _vnorm(t.vendor), t.card_last4)
        by_attr[akey].append(t)
    for akey, members in by_attr.items():
        if len(members) < 2:
            continue
        sgs = {m.split_group for m in members if m.split_group}
        if len(sgs) == 1 and all(m.split_group for m in members):
            continue   # intentional split set
        key = 'attr:' + akey
        if key in dismissed:
            continue
        has_doc  = any(m.source == 'doc_upload' for m in members)
        has_phantom = any(m.source == 'doc_upload' and m.doc_upload_id in elec_doc_ids for m in members)
        has_open_elec = any(m.source in ('qbo_sync', 'csv_import')
                            and m.match_status == 'unmatched' and not m.doc_upload_id
                            for m in members)
        if has_phantom:
            kind = 'phantom'        # redundant doc_upload row → safe to remove
        elif has_doc and has_open_elec:
            kind = 'matchable'      # unmatched charge + a receipt → link them
        elif not has_doc:
            kind = 'duplicate'      # electronic re-imports
        else:
            kind = 'review'         # mixed/already-matched states → user decides
        clusters.append({"key": key, "kind": kind,
                         "amount": abs(float(members[0].amount or 0)),
                         "rows": [_row(t) for t in members]})
    overcount = sum((c["amount"] * (len(c["rows"]) - 1)) for c in clusters
                    if c["kind"] in ("duplicate", "reimport", "phantom"))
    clusters.sort(key=lambda c: -(c["amount"] * (len(c["rows"]) - 1)))
    return jsonify({"ok": True, "project_id": pid, "cluster_count": len(clusters),
                    "potential_overcount": round(overcount, 2), "clusters": clusters})


@app.route("/projects/<int:pid>/actuals/dup-transactions/dismiss", methods=["POST"])
@login_required
def actuals_dismiss_dup_transactions(pid):
    """Mark a duplicate-transaction cluster as genuinely SEPARATE so the scan
    won't flag it again. Body: {"dup_key": "..."}. (User 2026-06-17.)"""
    ProjectSheet.query.get_or_404(pid)
    if current_user.role not in ('super_admin', 'admin'):
        if not ProjectAccess.query.filter_by(project_id=pid, user_id=current_user.id).first():
            return jsonify({"error": "Forbidden"}), 403
    key = ((request.get_json(silent=True) or {}).get("dup_key") or "").strip()
    if not key:
        return jsonify({"error": "dup_key required"}), 400
    if not TransactionDupDismissal.query.filter_by(project_id=pid, dup_key=key).first():
        db.session.add(TransactionDupDismissal(
            project_id=pid, dup_key=key,
            created_by=(current_user.id if current_user.is_authenticated else None)))
        db.session.commit()
    return jsonify({"ok": True})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/delete-duplicate", methods=["POST"])
@login_required
def actuals_delete_duplicate_txn(pid, tid):
    """Delete ONE transaction the user confirmed is a duplicate re-import.
    Refuses a row that carries a receipt link (use Match / the receipt dup flow
    instead, so a receipt's only ledger row is never lost). Detaches children
    and logs a full before-snapshot. (User 2026-06-17.)"""
    if getattr(current_user, 'role', None) == 'viewer':
        return jsonify({"error": "Forbidden"}), 403
    t = _pool_txn_or_404(pid, tid)
    if t.source == 'doc_upload':
        # Only deletable if the receipt is ALSO carried by an electronic charge
        # (then this doc_upload row is a redundant phantom). Otherwise deleting
        # it would orphan the receipt — refuse.
        rep = (Transaction.query
               .filter(Transaction.project_id == pid,
                       Transaction.doc_upload_id == t.doc_upload_id,
                       Transaction.source.in_(('qbo_sync', 'csv_import', 'reconciled')),
                       Transaction.id != tid).first())
        if not rep:
            return jsonify({"error": "Deleting this would orphan the receipt — match it to a "
                            "charge or use the receipt duplicate flow instead."}), 400
        # Preserve coding: if this leftover row is coded but the surviving bank
        # charge isn't, copy the code over so we don't lose it. (User 2026-06-17.)
        if (t.account_code or t.budget_line_id) and not (rep.account_code or rep.budget_line_id):
            rep.account_code      = t.account_code
            rep.account_code_name = t.account_code_name
            rep.budget_line_id    = t.budget_line_id
            try:
                claim_for(rep, pid, why='coding carried from a duplicate')
            except ClaimError as e:
                db.session.rollback()
                return jsonify({"error": str(e)}), 409
    elif t.doc_upload_id:
        return jsonify({"error": "This charge is linked to a receipt — unmatch it first "
                        "instead of deleting it here."}), 400
    else:
        # Cleanup A #7 (2026-11-02): an unlinked bank / manual row is only a
        # "duplicate" when a twin SURVIVES — same |amount|, same date, same
        # vendor or card, different id (actuals.is_duplicate_twin). Without
        # one this click simply erased a charge from the ledger.
        from actuals import is_duplicate_twin as _is_twin
        _cands = (Transaction.query
                  .filter(pool_filter(pid),
                          Transaction.id != tid,
                          Transaction.txn_date == t.txn_date,
                          Transaction.source != 'invoice_split')
                  .all())
        _twin = next((c for c in _cands if _is_twin(t, c)), None)
        if _twin is None:
            return jsonify({"error": "No matching charge found to keep — this row has no "
                                     "twin (same amount, date and vendor or card), so it "
                                     "isn't a duplicate. If it isn't a project expense, "
                                     "mark it Not project instead."}), 409
        # Keep the coding on the survivor if only the doomed row carried it.
        if (t.account_code or t.budget_line_id) and not (_twin.account_code or _twin.budget_line_id):
            _twin.account_code      = t.account_code
            _twin.account_code_name = t.account_code_name
            _twin.budget_line_id    = t.budget_line_id
            try:
                claim_for(_twin, pid, why='coding carried from a duplicate')
            except ClaimError as e:
                db.session.rollback()
                return jsonify({"error": str(e)}), 409
    before = {"source": t.source, "vendor": t.vendor,
              "amount": float(t.amount) if t.amount is not None else None,
              "txn_date": t.txn_date, "card_last4": t.card_last4,
              "account_code": t.account_code, "account_code_name": t.account_code_name,
              "budget_line_id": t.budget_line_id, "qbo_txn_id": t.qbo_txn_id,
              "match_status": t.match_status}
    Transaction.query.filter_by(parent_transaction_id=tid).update(
        {"parent_transaction_id": None}, synchronize_session=False)
    Transaction.query.filter_by(backup_of_txn_id=tid).update(
        {"backup_of_txn_id": None}, synchronize_session=False)
    _delete_evidence_for_txns([tid])
    # Splits point at the row with no cascade (#7).
    before["splits"] = [{"budget_line_id": sp.budget_line_id, "amount": float(sp.amount or 0)}
                        for sp in TransactionSplit.query.filter_by(transaction_id=tid).all()]
    TransactionSplit.query.filter_by(transaction_id=tid).delete(synchronize_session=False)
    _label = (t.vendor or f'Txn #{tid}')[:80]
    _amt = float(t.amount or 0)
    db.session.delete(t)
    try:
        _log_activity(action='delete', entity_type='transaction', entity_id=tid,
                      entity_label=_label, project_id=pid, before=before, dollar_delta=-_amt,
                      note=f'Removed duplicate transaction: {_label} ${_amt:,.2f}')
    except Exception:
        pass
    db.session.commit()
    return jsonify({"ok": True, "deleted": tid})


@app.route("/projects/<int:pid>/actuals/reconcile-scan", methods=["GET"])
@login_required
def actuals_reconcile_scan(pid):
    """READ-ONLY foundation for the unified Reconcile review (Phase 1, User
    2026-06-17): group RECEIPTS and TRANSACTIONS that refer to the same spend
    (|amount| + date + vendor + card) into ONE cluster with two lanes, and
    pre-compute the duplicate flags (byte-identical receipts, re-imported /
    phantom charges, unmatched pairs). Returns only clusters needing attention."""
    if not current_user.is_authenticated:
        return jsonify({"error": "Not authenticated"}), 401
    ProjectSheet.query.get_or_404(pid)
    if current_user.role not in ('super_admin', 'admin'):
        access = ProjectAccess.query.filter_by(project_id=pid, user_id=current_user.id).first()
        if not access or access.role not in ('owner', 'collaborator', 'editor'):
            return jsonify({"error": "Forbidden"}), 403
    import re as _re_r
    from collections import defaultdict as _dd_r
    txns = (Transaction.query.filter_by(project_id=pid)
            .filter(Transaction.not_project_expense == False,   # noqa: E712
                    Transaction.source != 'invoice_split').all())   # itemized sublines aren't dups (2026-07)
    docs = (DocUpload.query.filter_by(project_id=pid)
            .filter(DocUpload.category.in_(('receipt', 'invoice')))
            .filter(DocUpload.status != 'deleted').all())
    dismissed = {d.dup_key for d in TransactionDupDismissal.query.filter_by(project_id=pid).all()}
    elec_doc_ids = {t.doc_upload_id for t in txns
                    if t.source in ('qbo_sync', 'csv_import', 'reconciled') and t.doc_upload_id}

    def _vnorm(v):
        return _re_r.sub(r'[^a-z0-9]', '', (v or '').lower())[:18]

    def _sig(amount, date, vendor, card):
        if amount is None or not date or not vendor or not card:
            return None
        return '%.2f|%s|%s|%s' % (abs(float(amount)), date[:10], _vnorm(vendor), card)

    groups = _dd_r(lambda: {"txns": [], "docs": []})
    for t in txns:
        s = _sig(t.amount, t.txn_date, t.vendor, t.card_last4)
        if s:
            groups[s]["txns"].append(t)
    for d in docs:
        s = _sig(d.amount, (d.doc_date.isoformat() if d.doc_date else None), d.vendor, d.card_last4)
        if s:
            groups[s]["docs"].append(d)

    clusters = []
    for sig, g in groups.items():
        gt, gd = g["txns"], g["docs"]
        if (len(gt) + len(gd)) < 2:
            continue
        key = 'sig:' + sig
        if key in dismissed:
            continue
        hash_counts = _dd_r(int)
        for d in gd:
            if d.file_hash:
                hash_counts[d.file_hash] += 1
        receipts = [{"doc_id": d.id, "file": (d.filed_filename or d.original_filename),
                     "amount": (float(d.amount) if d.amount is not None else None),
                     "status": d.status, "on_charge": (d.id in elec_doc_ids),
                     "is_image": bool(d.content_type and d.content_type.startswith('image/')),
                     "dup": bool(d.file_hash and hash_counts[d.file_hash] > 1)} for d in gd]
        qid_counts = _dd_r(int)
        for t in gt:
            if t.source == 'qbo_sync' and t.qbo_txn_id:
                qid_counts[t.qbo_txn_id] += 1
        charges = []
        for t in gt:
            phantom = (t.source == 'doc_upload' and t.doc_upload_id in elec_doc_ids)
            charges.append({"tid": t.id, "source": t.source,
                            "amount": (float(t.amount) if t.amount is not None else None),
                            "match_status": t.match_status, "doc_upload_id": t.doc_upload_id,
                            "coded": t.account_code_name or (str(t.account_code) if t.account_code else None),
                            "phantom": phantom,
                            "reimport": bool(t.source == 'qbo_sync' and t.qbo_txn_id and qid_counts[t.qbo_txn_id] > 1),
                            "deletable": ((t.source in ('qbo_sync', 'csv_import', 'manual_entry') and not t.doc_upload_id) or phantom)})
        # Sign-aware duplicate flags: a charge (+) and its refund (−) share a
        # magnitude (kept grouped so they can be matched) but are NOT duplicates —
        # only 2+ of the SAME sign are. (User 2026-06-18.)
        def _sgn(a):
            try:
                a = float(a)
            except (TypeError, ValueError):
                return 0
            return 1 if a > 0 else (-1 if a < 0 else 0)
        _elec_signs = [_sgn(c["amount"]) for c in charges
                       if c["source"] in ('qbo_sync', 'csv_import', 'reconciled')]
        _rec_signs = [_sgn(r["amount"]) for r in receipts]
        flags = {
            "dup_receipts": (_rec_signs.count(1) > 1 or _rec_signs.count(-1) > 1),
            "dup_charges": (any(c["reimport"] for c in charges)
                            or _elec_signs.count(1) > 1 or _elec_signs.count(-1) > 1),
            "phantom": any(c["phantom"] for c in charges),
            "needs_match": (any(c["source"] in ('qbo_sync', 'csv_import') and c["match_status"] == 'unmatched'
                                and not c["doc_upload_id"] for c in charges)
                            and any(not r["on_charge"] for r in receipts)),
        }
        if not any(flags.values()):
            continue   # already reconciled (1 charge + 1 receipt, matched) — skip
        clusters.append({
            "key": key, "amount": abs(float((gt[0].amount if gt else gd[0].amount) or 0)),
            "date": sig.split('|')[1], "vendor": (gt[0].vendor if gt else gd[0].vendor),
            "card": sig.split('|')[3],
            "receipts": receipts, "charges": charges, "flags": flags,
        })
    clusters.sort(key=lambda c: -c["amount"])
    return jsonify({"ok": True, "project_id": pid, "cluster_count": len(clusters),
                    "clusters": clusters[:300]})


@app.route("/projects/<int:pid>/actuals/reconcile-ai-judge", methods=["POST"])
@login_required
def actuals_reconcile_ai_judge(pid):
    """AI-layer §A2: ask Claude to judge an ambiguous reconcile cluster — are
    these the SAME spend (duplicate) or genuinely SEPARATE (e.g. two hotel
    nights for different people, per-passenger airfare)? Reasons over the JSON
    only; logs to ai_event; mutates nothing. Fail-open. (User 2026-06-17.)"""
    ProjectSheet.query.get_or_404(pid)
    if current_user.role not in ('super_admin', 'admin'):
        if not ProjectAccess.query.filter_by(project_id=pid, user_id=current_user.id).first():
            return jsonify({"error": "Forbidden"}), 403
    import ai_layer, time as _t, json as _j
    body = request.get_json(silent=True) or {}
    items = body.get("items") or []
    if len(items) < 2:
        return jsonify({"error": "Need at least two items to compare."}), 400
    payload = {
        "instruction": ("These records share an amount/date/vendor/card and may be the SAME "
                        "spend (duplicates to merge/remove) OR genuinely SEPARATE charges "
                        "(e.g. two hotel nights for different guests, per-passenger airfare on "
                        "one receipt). Decide which. If the data can't distinguish them, say so "
                        "and recommend review rather than guessing."),
        "new_document": items[0],
        "possible_matches": items[1:],
    }
    t0 = _t.time()
    verdict = ai_layer.detect_anomalies(payload)
    latency = int((_t.time() - t0) * 1000)
    try:
        from models import AiEvent
        db.session.add(AiEvent(
            project_id=pid, doc_upload_id=(body.get("doc_id") or None),
            feature='anomaly', provider=verdict.get('_provider'), model=verdict.get('_model'),
            request_json=_j.dumps(payload, default=str)[:8000],
            response_json=_j.dumps(verdict, default=str)[:8000], latency_ms=latency))
        db.session.commit()
    except Exception:
        db.session.rollback()
    return jsonify({"ok": True, "verdict": verdict, "latency_ms": latency})


@app.route("/projects/<int:pid>/actuals/automatch-trace")
@login_required
def actuals_automatch_trace(pid):
    """READ-ONLY: trace WHY auto-match does/doesn't pair a given vendor's rows.
    For ?vendor=<substr> (default 'airbnb'), shows the electronic (csv/qbo)
    rows, whether each is in the eligible unmatched pool (and if not, why), and
    for the first eligible one the top doc candidates with each gate's value
    (amount diff / day gap / vendor score). Admin only. (User 2026-06-02.)"""
    if current_user.role not in ('super_admin', 'admin'):
        return jsonify({"error": "Forbidden — admin only"}), 403
    import datetime as _dt
    from actuals import _vendor_similarity
    sub = (request.args.get('vendor') or 'airbnb').strip().lower()

    # ?analyze=1 — window-sensitivity: how many MORE unmatched electronic rows
    # would match (exact amount + vendor>=0.45) if the ±3-day window widened.
    if request.args.get('analyze') == '1':
        elec = (Transaction.query
                .filter(pool_filter(pid),
                        Transaction.source.in_(('qbo_sync', 'csv_import')),
                        Transaction.match_status == 'unmatched',
                        Transaction.doc_upload_id.is_(None),
                        Transaction.not_project_expense == False).all())
        docs = (Transaction.query
                .filter_by(project_id=pid, source='doc_upload',
                           not_project_expense=False).all())
        doc_by_amt = {}
        for d in docs:
            if d.amount is None:
                continue
            doc_by_amt.setdefault(round(float(d.amount), 2), []).append(d)
        buckets = {"<=3": 0, "4-7": 0, "8-14": 0, "15-30": 0, ">30": 0, "no_amt_doc": 0, "amt_doc_low_vendor": 0}
        examples = []
        for q in elec:
            if q.amount is None or not q.txn_date:
                continue
            try:
                q_dt = _dt.date.fromisoformat(q.txn_date[:10])
            except (TypeError, ValueError):
                continue
            cands = doc_by_amt.get(round(float(q.amount), 2), [])
            if not cands:
                buckets["no_amt_doc"] += 1
                continue
            best_gap, best_d = None, None
            for d in cands:
                if _vendor_similarity(q.vendor, d.vendor) < 0.45:
                    continue
                try:
                    g = abs((_dt.date.fromisoformat((d.txn_date or '')[:10]) - q_dt).days)
                except (TypeError, ValueError):
                    continue
                if best_gap is None or g < best_gap:
                    best_gap, best_d = g, d
            if best_gap is None:
                buckets["amt_doc_low_vendor"] += 1
                continue
            b = ("<=3" if best_gap <= 3 else "4-7" if best_gap <= 7
                 else "8-14" if best_gap <= 14 else "15-30" if best_gap <= 30 else ">30")
            buckets[b] += 1
            if b in ("4-7", "8-14") and len(examples) < 12:
                examples.append({"q": f"{q.txn_date} {q.vendor} ${float(q.amount)}",
                                 "doc": f"{best_d.txn_date} {best_d.vendor}", "gap": best_gap})
        return jsonify({"unmatched_electronic": len(elec), "open_docs": len(docs),
                        "would_match_within": buckets,
                        "note": "buckets count rows whose nearest exact-amount, vendor>=0.45 receipt falls in that day-gap range",
                        "examples_4_to_14_days": examples})

    def _match(t):
        return sub in (t.vendor or '').lower()

    elec = (Transaction.query
            .filter(pool_filter(pid),
                    Transaction.source.in_(('qbo_sync', 'csv_import')))
            .all())
    docs = (Transaction.query
            .filter_by(project_id=pid, source='doc_upload')
            .all())
    elec_hit = [t for t in elec if _match(t)]
    docs_hit = [t for t in docs if _match(t)]

    def _why_excluded(t):
        reasons = []
        if t.match_status != 'unmatched':
            reasons.append(f"match_status={t.match_status}")
        if t.doc_upload_id is not None:
            reasons.append(f"doc_upload_id={t.doc_upload_id}")
        if t.not_project_expense:
            reasons.append("not_project_expense")
        return reasons

    def _row(t):
        return {"id": t.id, "vendor": t.vendor, "amount": float(t.amount or 0),
                "date": t.txn_date, "match_status": t.match_status,
                "doc_upload_id": t.doc_upload_id,
                "not_project_expense": t.not_project_expense,
                "eligible": (not _why_excluded(t)) if t.source in ('qbo_sync', 'csv_import') else None,
                "excluded_because": _why_excluded(t)}

    out = {"vendor_filter": sub,
           "electronic_rows": [_row(t) for t in elec_hit[:8]],
           "doc_rows": [_row(t) for t in docs_hit[:8]],
           "candidate_trace": []}

    # For the first eligible electronic row, score the doc candidates.
    elig = next((t for t in elec_hit if not _why_excluded(t)), None)
    if elig and elig.txn_date and elig.amount is not None:
        try:
            q_dt = _dt.date.fromisoformat(elig.txn_date[:10])
        except (TypeError, ValueError):
            q_dt = None
        out["traced_electronic_row"] = _row(elig)
        out["traced_q_date_parsed"] = (q_dt.isoformat() if q_dt else "PARSE_FAILED")
        cands = []
        for d in docs:                       # scan ALL docs, not just vendor hits
            if d.amount is None or abs(float(d.amount) - float(elig.amount)) > 0.01:
                continue
            try:
                d_dt = _dt.date.fromisoformat((d.txn_date or '')[:10])
                gap = abs((d_dt - q_dt).days) if q_dt else None
            except (TypeError, ValueError):
                d_dt, gap = None, "DATE_PARSE_FAILED"
            cands.append({"doc_id": d.id, "vendor": d.vendor,
                          "amount": float(d.amount or 0), "date": d.txn_date,
                          "raw_date_repr": repr(d.txn_date),
                          "day_gap": gap,
                          "vendor_score": round(_vendor_similarity(elig.vendor, d.vendor), 3),
                          "passes": (gap is not None and gap != "DATE_PARSE_FAILED"
                                     and isinstance(gap, int) and gap <= 3
                                     and _vendor_similarity(elig.vendor, d.vendor) >= 0.45)})
        out["candidate_trace"] = cands[:10]
        out["amount_matched_doc_count"] = len(cands)
    return jsonify(out)


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/confirm-match", methods=["POST"])
@login_required
def actuals_confirm_match(pid, tid):
    """User accepts the auto-matcher's suggestion for this txn."""
    txn = _pool_txn_or_404(pid, tid)
    from actuals import confirm_match
    try:
        result = confirm_match(tid)
        _act = None
        try:
            _label = (txn.vendor or f'Txn #{txn.id}')[:80]
            _amt = float(txn.amount or 0)
            _act = _log_activity(
                action='update', entity_type='transaction_match',
                entity_id=txn.id, entity_label=_label,
                project_id=pid,
                after={'match_status': 'confirmed',
                       'doc_upload_id': txn.doc_upload_id},
                note=f'Confirmed match: {_label} ${_amt:,.2f}')
        except Exception: pass
        return jsonify({"ok": True, "activity_id": (_act.id if _act else None), **result})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.exception(f"[actuals] confirm-match failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/dismiss-suggestion", methods=["POST"])
@login_required
def actuals_dismiss_suggestion(pid, tid):
    """User marks the auto-matcher's suggestion as 'Not a match'. Separates the
    pair AND remembers the rejection so it's never re-suggested. (Pass
    remember=0 to just unlink without remembering.)"""
    txn = _pool_txn_or_404(pid, tid)
    body = request.get_json(silent=True) or {}
    remember = str(body.get('remember', '1')).lower() not in ('0', 'false', 'no')
    from actuals import dismiss_suggestion
    try:
        result = dismiss_suggestion(tid, remember=remember, user_id=current_user.id)
        _act = None
        try:
            _label = (txn.vendor or f'Txn #{txn.id}')[:80]
            _act = _log_activity(
                action='update', entity_type='transaction_match',
                entity_id=txn.id, entity_label=_label,
                project_id=pid,
                after={'match_status': 'unmatched'},
                note=(f'Not a match: {_label} — won\'t be re-suggested' if remember
                      else f'Dismissed suggested match for {_label}'))
        except Exception: pass
        return jsonify({"ok": True, "activity_id": (_act.id if _act else None), **result})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/unmatch", methods=["POST"])
@login_required
def actuals_unmatch(pid, tid):
    """Unlink a receipt from a charge (suggested OR confirmed) and return the
    receipt to the unlinked pool. The charge survives. (User 2026-06-02.)"""
    if getattr(current_user, 'role', None) == 'viewer':
        return jsonify({"error": "Forbidden"}), 403
    txn = _pool_txn_or_404(pid, tid)
    from actuals import unmatch_receipt
    try:
        info = unmatch_receipt(tid)
        _act = None
        try:
            _act = _log_activity(action='update', entity_type='transaction_match',
                                 entity_id=tid, entity_label=(txn.vendor or f'Txn #{tid}')[:80],
                                 project_id=pid, after={'match_status': 'unmatched'},
                                 note=f'Unmatched receipt from {(txn.vendor or "charge")[:60]}')
        except Exception:
            pass
        return jsonify({"ok": True, "activity_id": (_act.id if _act else None), **info})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@app.route("/projects/<int:pid>/actuals/undo-last-match", methods=["POST"])
@login_required
def actuals_undo_last_match(pid):
    """Undo the most recent confirmed match in this project (fat-finger
    insurance). Unlinks it and restores the receipt. (User 2026-06-02.)"""
    if getattr(current_user, 'role', None) == 'viewer':
        return jsonify({"error": "Forbidden"}), 403
    ProjectSheet.query.get_or_404(pid)
    from actuals import last_match_for_project, unmatch_receipt
    last = last_match_for_project(pid)
    if not last:
        return jsonify({"ok": True, "undone": False, "message": "No recent match to undo."})
    label = (last.vendor or f'Txn #{last.id}')
    info = unmatch_receipt(last.id)
    try:
        _log_activity(action='update', entity_type='transaction_match',
                      entity_id=last.id, entity_label=label[:80], project_id=pid,
                      after={'match_status': 'unmatched'},
                      note=f'Undid last match: {label[:60]}')
    except Exception:
        pass
    return jsonify({"ok": True, "undone": True, "transaction_id": last.id,
                    "vendor": last.vendor,
                    "amount": float(last.amount) if last.amount is not None else None,
                    **info})


@app.route("/projects/<int:pid>/actuals/code-suggested-bulk", methods=["POST"])
@login_required
def actuals_code_suggested_bulk(pid):
    """Bulk auto-code (cross-point 3): apply the vendor→line suggestions to
    uncoded transactions. Dry-run by default (?apply=1 to write). min_score
    (default 2 shared tokens) keeps it high-confidence. (User 2026-06-02.)"""
    if getattr(current_user, 'role', None) == 'viewer':
        return jsonify({"error": "Forbidden — viewers cannot code"}), 403
    ProjectSheet.query.get_or_404(pid)
    body = request.get_json(silent=True) or {}
    apply = (request.args.get('apply') == '1') or bool(body.get('apply'))
    try:
        min_score = int(body.get('min_score') or 1)
    except (TypeError, ValueError):
        min_score = 1
    sugg = _actuals_vendor_suggestions(pid)
    targets = [(tid, s) for tid, s in sugg.items() if int(s.get('score', 0)) >= min_score]
    if not apply:
        return jsonify({"dry": True, "would_code": len(targets), "min_score": min_score,
                        "total_suggested": len(sugg),
                        "sample": [{"label": s["label"], "code": s["code"], "score": s["score"]}
                                   for _, s in targets[:15]]})
    from actuals import link_transaction_to_line
    coded, failed = 0, 0
    for tid, s in targets:
        try:
            link_transaction_to_line(tid, s["line_id"], user_id=getattr(current_user, 'id', None))
            db.session.commit()
            coded += 1
        except Exception as e:
            db.session.rollback()
            failed += 1
            logging.warning(f"[bulk-code] txn {tid}: {e}")
    logging.info(f"[bulk-code] pid={pid} min_score={min_score}: {coded} coded, {failed} failed")
    return jsonify({"ok": True, "coded": coded, "failed": failed})


@app.route("/projects/<int:pid>/actuals/matches/confirm-bulk", methods=["POST"])
@login_required
def actuals_confirm_matches_bulk(pid):
    """Confirm many suggested matches in one click. Body:
      {min_confidence: 0-1 (default 0), tids: [optional explicit id list]}.
    Confirms every 'suggested' txn that has a doc and meets the filter.
    (User 2026-06-02.)"""
    if getattr(current_user, 'role', None) == 'viewer':
        return jsonify({"error": "Forbidden — viewers cannot confirm"}), 403
    ProjectSheet.query.get_or_404(pid)
    body = request.get_json(silent=True) or {}
    try:
        minc = float(body.get('min_confidence') or 0)
    except (TypeError, ValueError):
        minc = 0.0
    tids = body.get('tids')
    tidset = set(tids) if isinstance(tids, list) else None
    rows = (Transaction.query
            .filter_by(project_id=pid, match_status='suggested')
            .filter(Transaction.doc_upload_id.isnot(None)).all())
    from actuals import confirm_match
    confirmed, failed = 0, 0
    for t in rows:
        if tidset is not None and t.id not in tidset:
            continue
        if float(t.match_confidence or 0) < minc:
            continue
        try:
            confirm_match(t.id)
            confirmed += 1
        except Exception as e:
            failed += 1
            db.session.rollback()
            logging.warning(f"[bulk-confirm] txn {t.id}: {e}")
    logging.info(f"[bulk-confirm] pid={pid} min_conf={minc}: {confirmed} confirmed, {failed} failed")
    try:
        _log_activity(action='update', entity_type='transaction_match',
                      entity_id=pid, entity_label=f'Project #{pid}', project_id=pid,
                      after={'confirmed': confirmed},
                      note=f'Bulk-confirmed {confirmed} match{"es" if confirmed != 1 else ""}'
                           + (f' (>= {int(minc*100)}% confidence)' if minc else ''))
    except Exception:
        pass
    return jsonify({"ok": True, "confirmed": confirmed, "failed": failed})


@app.route("/projects/<int:pid>/actuals/matches/dismiss-bulk", methods=["POST"])
@login_required
def actuals_dismiss_matches_bulk(pid):
    """Dismiss many suggested matches at once. Body:
      {max_confidence: 0-1 (optional — only dismiss BELOW this),
       tids: [optional explicit id list]}. (User 2026-06-02.)"""
    if getattr(current_user, 'role', None) == 'viewer':
        return jsonify({"error": "Forbidden — viewers cannot dismiss"}), 403
    ProjectSheet.query.get_or_404(pid)
    body = request.get_json(silent=True) or {}
    maxc = body.get('max_confidence')
    try:
        maxc = float(maxc) if maxc is not None else None
    except (TypeError, ValueError):
        maxc = None
    tids = body.get('tids')
    tidset = set(tids) if isinstance(tids, list) else None
    rows = (Transaction.query
            .filter_by(project_id=pid, match_status='suggested')
            .filter(Transaction.doc_upload_id.isnot(None)).all())
    from actuals import dismiss_suggestion
    dismissed = 0
    for t in rows:
        if tidset is not None and t.id not in tidset:
            continue
        if maxc is not None and float(t.match_confidence or 0) >= maxc:
            continue
        try:
            dismiss_suggestion(t.id)
            dismissed += 1
        except Exception as e:
            db.session.rollback()
            logging.warning(f"[bulk-dismiss] txn {t.id}: {e}")
    logging.info(f"[bulk-dismiss] pid={pid}: {dismissed} dismissed")
    return jsonify({"ok": True, "dismissed": dismissed})


@app.route("/projects/<int:pid>/actuals/docs.json", methods=["GET"])
@login_required
def actuals_docs_list_for_picker(pid):
    """Return the list of DocUploads on this project that the user can
    link to a transaction. Used by the 'Link existing receipt' picker
    when one receipt should back multiple transactions (e.g. a Lyft
    week-rollup receipt backing 5 individual ride transactions on the
    bank statement).

    Returns thin metadata only (id, label, vendor, amount, date,
    category, has_image flag). The picker fetches the full preview
    on hover/click via the existing /docs/upload/<uid>/raw endpoint.
    """
    ProjectSheet.query.get_or_404(pid)
    rows = (DocUpload.query
            .filter_by(project_id=pid)
            .order_by(DocUpload.uploaded_at.desc())
            .all())
    out = []
    for d in rows:
        # Filter out error / corrupt rows — only show usable docs.
        if d.status == 'error':
            continue
        # Skip flagged duplicates — they're parked until pulled back.
        if d.is_duplicate or d.status == 'duplicate':
            continue
        if not d.filed_dropbox_path and not d.source_archive_path:
            continue
        out.append({
            "id":             d.id,
            "label":          d.filed_filename or d.original_filename or f"Upload #{d.id}",
            "crew_member_id": d.crew_member_id,
            "vendor":         d.vendor,
            "amount":         float(d.amount) if d.amount is not None else None,
            "doc_date":       d.doc_date.isoformat() if d.doc_date else None,
            "category":       d.category,
            "has_image":      bool(d.content_type and d.content_type.startswith('image/')),
        })
    return jsonify({"ok": True, "docs": out})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/link-doc", methods=["POST"])
@login_required
def actuals_link_existing_doc(pid, tid):
    """Link an existing DocUpload to a Transaction WITHOUT re-uploading.

    Use case: one receipt (e.g. Lyft week rollup) backs multiple
    smaller bank-side transactions. The user uploads the consolidated
    receipt once, then opens each individual transaction and links
    the same DocUpload via this endpoint.

    Body: {doc_upload_id: int}. Server: validates that the doc lives
    on the same project, sets transaction.doc_upload_id, backfills
    blank vendor / amount / date from the doc's OCR data (won't
    override existing values), flips match_status to 'confirmed'.
    """
    txn = _pool_txn_or_404(pid, tid)
    data = request.get_json(force=True) or {}
    raw  = data.get("doc_upload_id")
    if not raw:
        return jsonify({"error": "doc_upload_id required"}), 400
    try:
        doc_id = int(raw)
    except (TypeError, ValueError):
        return jsonify({"error": "doc_upload_id must be int"}), 400

    doc = DocUpload.query.filter_by(id=doc_id, project_id=pid).first()
    if not doc:
        return jsonify({"error": "DocUpload not found in this project"}), 404

    # Attaching a receipt claims the charge for this project (company ledger).
    try:
        claim_for(txn, pid, why='receipt attached')
    except ClaimError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 409
    txn.doc_upload_id = doc.id
    if not txn.vendor and doc.vendor:                txn.vendor   = doc.vendor
    if txn.amount is None and doc.amount is not None: txn.amount  = doc.amount
    if not txn.txn_date and doc.doc_date:            txn.txn_date = doc.doc_date.isoformat()
    txn.match_status = 'confirmed'
    txn.updated_at   = datetime.utcnow()
    db.session.commit()
    # Absorb the receipt's own placeholder row (source='doc_upload') exactly
    # like the suggested-match Confirm path does — otherwise it lingers in
    # "Receipts to place" even though the doc now rides on this charge.
    # (User 2026-07-08: handwritten tip → $2 amount mismatch → manual link
    # left the placeholder behind.)
    if txn.source != 'doc_upload':
        try:
            from actuals import confirm_match
            confirm_match(tid)
        except Exception as _e:
            # Roll back FIRST: after a failed flush the session is poisoned
            # and even reading txn.id re-raises — which is exactly how this
            # warning itself became the 500 the user saw (ERR-D62E8221,
            # 2026-10-05). Use the route arg, never the ORM object, here.
            try:
                db.session.rollback()
            except Exception:
                pass
            logging.warning(f"[link-doc] sister absorb failed txn {tid}: {_e}")
    _act = None
    try:
        _label = (txn.vendor or f'Txn #{txn.id}')[:80]
        _amt = float(txn.amount or 0)
        _doc_label = doc.filed_filename or doc.original_filename or f'Doc #{doc.id}'
        _act = _log_activity(
            action='update', entity_type='transaction_match',
            entity_id=txn.id, entity_label=_label,
            project_id=pid,
            after={'doc_upload_id': doc.id, 'match_status': 'confirmed'},
            note=f'Linked existing receipt "{_doc_label}" → ${_amt:,.2f} {_label}')
    except Exception: pass
    return jsonify({
        "ok":              True,
        "transaction_id":  txn.id,
        "doc_upload_id":   doc.id,
        "filed_filename":  doc.filed_filename or doc.original_filename,
        "activity_id":     (_act.id if _act else None),
    })


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/receipt-candidates", methods=["GET"])
@login_required
def actuals_receipt_candidates(pid, tid):
    """Ranked inbox receipts for ONE charge — the "Find backup" panel
    (Phase 5, 2026-11-03). ONE rule: actuals.match_candidates (score_match
    on amount / date / vendor / card), top `limit` (default 8). ?q= adds a
    plain search over every open receipt (vendor, amount, date, filename)
    under `pool`, so a receipt the engine would not rank can still be
    linked by hand. Read-only."""
    txn = _pool_txn_or_404(pid, tid)
    from actuals import match_candidates, open_receipts, score_match
    try:
        limit = max(1, min(50, int(request.args.get('limit') or 8)))
    except (TypeError, ValueError):
        limit = 8
    q = (request.args.get('q') or '').strip().lower()

    def _brief(d, s=None):
        return {"doc_id": d.id, "vendor": d.vendor or '', "amount": (float(d.amount) if d.amount is not None else None),
                "date": d.doc_date.isoformat() if d.doc_date else None,
                "name": (d.filed_filename or d.original_filename or f'Upload #{d.id}')[:120],
                "category": d.category or '', "card_last4": d.card_last4,
                "thumb": f"/docs/upload/{d.id}/thumb",
                "is_image": bool(d.content_type and d.content_type.startswith('image/')),
                "confidence": (s or {}).get("confidence"), "reasons": (s or {}).get("reasons") or [],
                "strong": bool((s or {}).get("strong"))}
    try:
        ranked = match_candidates(pid, txn=txn, limit=limit)
        cands = [_brief(c["item"], c) for c in ranked]
        pool = []
        if q:
            for d in open_receipts(pid):
                hay = ' '.join([d.vendor or '', (d.filed_filename or d.original_filename or ''),
                                (d.doc_date.isoformat() if d.doc_date else ''),
                                ('%.2f' % float(d.amount)) if d.amount is not None else '']).lower()
                if q in hay:
                    pool.append(_brief(d, score_match(txn, d)))
            pool.sort(key=lambda r: (-(r["confidence"] or 0), r["date"] or ''), reverse=False)
    except Exception as e:
        logging.exception("[actuals] receipt-candidates failed")
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True,
                    "txn": {"id": txn.id, "vendor": txn.vendor, "date": (txn.txn_date or '')[:10],
                            "amount": (float(txn.amount) if txn.amount is not None else None),
                            "card_last4": txn.card_last4},
                    "candidates": cands, "pool": pool[:60]})


@app.route("/projects/<int:pid>/actuals/doc/<int:did>/charge-candidates", methods=["GET"])
@login_required
def actuals_charge_candidates(pid, did):
    """Ranked open charges for ONE inbox receipt — the candidate panel the
    Receipts view opens when a receipt is selected (Phase 5, 2026-11-03).
    Same engine as receipt-candidates, from the other side; ?q= searches
    every unmatched charge (vendor / amount / date / last four) under
    `pool`. Read-only."""
    doc = DocUpload.query.filter_by(id=did, project_id=pid).first_or_404()
    from actuals import match_candidates, open_charges, score_match, signed_amount_value
    try:
        limit = max(1, min(50, int(request.args.get('limit') or 8)))
    except (TypeError, ValueError):
        limit = 8
    q = (request.args.get('q') or '').strip().lower()

    def _brief(t, s=None):
        return {"id": t.id, "vendor": t.vendor or '—', "date": (t.txn_date or '')[:10],
                "amount": signed_amount_value(t.amount, t.is_expense),
                "card_last4": t.card_last4, "source": t.source,
                "qbo_txn_type": t.qbo_txn_type,
                "assigned": bool(t.budget_line_id or t.account_code or getattr(t, 'has_splits', False)),
                "confidence": (s or {}).get("confidence"), "reasons": (s or {}).get("reasons") or [],
                "strong": bool((s or {}).get("strong"))}
    try:
        ranked = match_candidates(pid, doc=doc, limit=limit)
        cands = [_brief(c["item"], c) for c in ranked]
        pool = []
        if q:
            for t in open_charges(pid):
                hay = ' '.join([t.vendor or '', t.txn_date or '', t.card_last4 or '',
                                ('%.2f' % float(t.amount)) if t.amount is not None else '',
                                t.note or '']).lower()
                if q in hay:
                    pool.append(_brief(t, score_match(t, doc)))
            pool.sort(key=lambda r: (-(r["confidence"] or 0), r["date"] or ''))
    except Exception as e:
        logging.exception("[actuals] charge-candidates failed")
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True,
                    "doc": {"doc_id": doc.id, "vendor": doc.vendor, "amount": (float(doc.amount) if doc.amount is not None else None),
                            "date": doc.doc_date.isoformat() if doc.doc_date else None,
                            "name": (doc.filed_filename or doc.original_filename or f'Upload #{doc.id}')[:120],
                            "thumb": f"/docs/upload/{doc.id}/thumb"},
                    "candidates": cands, "pool": pool[:60]})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/merge-duplicate", methods=["POST"])
@login_required
def actuals_merge_duplicate(pid, tid):
    """Duplicates view → "Delete the extra" (Phase 5, 2026-11-03). <tid> is
    the EXTRA; body {keeper_id}. The keeper must be a twin of the extra
    (actuals.is_duplicate_twin — one rule). Then, by
    actuals.duplicate_delete_mode:
      delete — a qbo_sync / csv_import row with no coding and no receipt is
               simply deleted;
      merge  — everything else first moves its line / section, splits,
               receipt + evidence, reason, payer and note onto the keeper
               (actuals.merge_duplicate_into), then is deleted.
    Both steps are logged (update on the keeper, delete on the extra with a
    full snapshot) and undoable through the activity log: Undo recreates
    the extra from the snapshot and puts the keeper's coding back."""
    if getattr(current_user, 'role', None) == 'viewer':
        return jsonify({"error": "Forbidden"}), 403
    _require_project_role(pid, 'editor')
    from actuals import (is_duplicate_twin, duplicate_delete_mode, merge_duplicate_into,
                         txn_snapshot)
    from models import ExpenseEvidence
    extra = _pool_txn_or_404(pid, tid)
    body = request.get_json(silent=True) or {}
    try:
        keeper_id = int(body.get('keeper_id'))
    except (TypeError, ValueError):
        return jsonify({"error": "keeper_id required"}), 400
    if keeper_id == tid:
        return jsonify({"error": "The keeper and the extra must be different rows."}), 400
    keeper = pool_txn(pid, keeper_id)
    if keeper is None:
        return jsonify({"error": "Keeper not found on this project."}), 404
    if extra.source in ('doc_upload', 'invoice_split'):
        return jsonify({"error": "A receipt's own row or an itemized subline is not a charge — "
                                 "match or itemize it instead."}), 400
    extra.has_splits = TransactionSplit.query.filter_by(transaction_id=extra.id).first() is not None
    keeper.has_splits = TransactionSplit.query.filter_by(transaction_id=keeper.id).first() is not None
    extra.has_evidence = ExpenseEvidence.query.filter_by(transaction_id=extra.id).first() is not None
    if not is_duplicate_twin(extra, keeper):
        return jsonify({"error": "These two rows are not twins (same amount, same vendor or card, "
                                 "dates within the window, different QuickBooks ids), so the extra "
                                 "cannot be folded into the keeper."}), 409
    mode = duplicate_delete_mode(extra)
    k_before = txn_snapshot(keeper)
    x_before = txn_snapshot(extra)
    x_before["splits"] = [{"budget_line_id": sp.budget_line_id, "amount": float(sp.amount or 0),
                           "note": sp.note}
                          for sp in TransactionSplit.query.filter_by(transaction_id=tid).all()]
    moved = {"moved": [], "splits": 0, "evidence": 0}
    acts = []
    if mode == 'merge':
        _k_splits_before = [{"budget_line_id": sp.budget_line_id, "amount": float(sp.amount or 0),
                             "note": sp.note}
                            for sp in TransactionSplit.query.filter_by(transaction_id=keeper.id).all()]
        _splits = TransactionSplit.query.filter_by(transaction_id=tid).all()
        _evid = ExpenseEvidence.query.filter_by(transaction_id=tid).all()
        _k_doc_ids = {e.doc_upload_id for e in ExpenseEvidence.query.filter_by(transaction_id=keeper.id).all()}
        _evid_move, _evid_drop = [], []
        for e in _evid:
            (_evid_drop if e.doc_upload_id in _k_doc_ids else _evid_move).append(e)
        if keeper.has_splits and _splits:
            # Two split sets cannot both carry the amount — the keeper's win.
            for sp in _splits:
                db.session.delete(sp)
            _splits = []
        moved = merge_duplicate_into(keeper, extra, splits=_splits, evidence=_evid_move)
        for e in _evid_drop:
            db.session.delete(e)
        keeper.updated_at = datetime.utcnow()
        try:
            _ka = _log_activity(action='update', entity_type='transaction', entity_id=keeper.id,
                                entity_label=(keeper.vendor or f'Txn #{keeper.id}')[:80], project_id=pid,
                                before={**{k: k_before[k] for k in ('budget_line_id', 'account_code',
                                                                     'account_code_name', 'doc_upload_id',
                                                                     'match_status', 'match_confidence',
                                                                     'no_charge_reason', 'payment_method',
                                                                     'paid_by_crew_member_id')},
                                        "splits": _k_splits_before},
                                after={k: txn_snapshot(keeper)[k] for k in ('budget_line_id', 'account_code',
                                                                             'doc_upload_id', 'match_status')},
                                note=f'Duplicate: moved {", ".join(moved["moved"]) or "nothing"} from #{tid} '
                                     f'onto {(keeper.vendor or "keeper")[:50]}')
            if _ka:
                acts.append(_ka.id)
        except Exception:
            pass
    else:
        _delete_evidence_for_txns([tid])
        TransactionSplit.query.filter_by(transaction_id=tid).delete(synchronize_session=False)
    Transaction.query.filter_by(parent_transaction_id=tid).update(
        {"parent_transaction_id": keeper.id}, synchronize_session=False)
    Transaction.query.filter_by(backup_of_txn_id=tid).update(
        {"backup_of_txn_id": keeper.id}, synchronize_session=False)
    _label = (extra.vendor or f'Txn #{tid}')[:80]
    _amt = float(extra.amount or 0)
    db.session.delete(extra)
    db.session.flush()
    try:
        _xa = _log_activity(action='delete', entity_type='transaction', entity_id=tid,
                            entity_label=_label, project_id=pid, before=x_before, dollar_delta=-_amt,
                            note=f'Removed duplicate {_label} ${_amt:,.2f} (kept #{keeper.id}'
                                 + (f'; moved {", ".join(moved["moved"])}' if moved["moved"] else '') + ')')
        if _xa:
            acts.append(_xa.id)
    except Exception:
        pass
    db.session.commit()
    return jsonify({"ok": True, "deleted": tid, "keeper_id": keeper.id, "mode": mode,
                    "moved": moved, "activity_ids": acts})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/upload-receipt", methods=["POST"])
@login_required
def actuals_upload_receipt_to_transaction(pid, tid):
    """Direct receipt upload bound to an existing Transaction.

    Mirrors docs_upload_post's pipeline (Veryfi OCR + Dropbox source-
    archive + processed copy via fp_analyzer.analyze_and_file_single)
    but DOES NOT create a duplicate Transaction. Instead it:

      1. Files the doc through the analyzer normally.
      2. Creates a DocUpload row.
      3. Sets the EXISTING transaction's doc_upload_id to point at it.
      4. Backfills the transaction's vendor / amount / txn_date from
         OCR ONLY where the row currently has NULL — never overrides
         existing values.
      5. Flips match_status='confirmed' so the row reads as audit-
         clean.

    Use case: user has a paper receipt or a manually-found PDF and
    wants to attach it to a specific QBO transaction (or manual
    entry). Drag the file onto the row in the Actuals tab → this
    endpoint files everything correctly without polluting the
    ledger with a parallel doc-source row.
    """
    project = ProjectSheet.query.get_or_404(pid)
    txn     = _pool_txn_or_404(pid, tid)

    # Reject if the transaction already has a receipt linked. The user
    # should clear or replace the existing one explicitly rather than
    # silently swap.
    if txn.doc_upload_id:
        return jsonify({
            "error": "Transaction already has a receipt linked. "
                     "Open the receipt and re-upload from the Docs tab to replace.",
        }), 409
    # Attaching a receipt claims the charge for this project (company
    # ledger) — refused before any file is touched when another project
    # holds it.
    try:
        claim_for(txn, pid, why='receipt uploaded')
    except ClaimError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 409

    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "No file provided"}), 400
    _upl_err = _validate_upload_file(f, 'document') if f else None
    if _upl_err:
        return jsonify({"error": _upl_err}), 400

    if not project.dropbox_folder:
        return jsonify({"error": "Project has no Dropbox folder configured"}), 500

    # Read bytes once; pass to the analyzer.
    import hashlib, uuid as _uuid, json as _json
    data       = f.read()
    file_hash  = hashlib.sha256(data).hexdigest()
    _data_size = len(data)

    # Cross-doc dedupe: same file hash, real Dropbox path, not error.
    duplicate_of = (
        DocUpload.query
        .filter_by(project_id=pid, file_hash=file_hash)
        .filter(DocUpload.filed_dropbox_path.isnot(None))
        .filter(DocUpload.status != 'review')
               .filter(DocUpload.status != 'deleted')
        .first()
    )

    content_type = f.content_type or "application/octet-stream"
    ext     = os.path.splitext(f.filename)[1].lower() if f.filename else ""
    r2_key  = f"docs/{pid}/{_uuid.uuid4().hex}{ext}"
    user_name = (current_user.name or current_user.email.split('@')[0]
                 if current_user.email else 'user')

    from fp_analyzer import analyze_and_file_single
    try:
        result = analyze_and_file_single(
            file_bytes    = data,
            filename      = f.filename,
            project_name  = project.dropbox_folder,
            user_name     = user_name,
        )
    except Exception as _ae:
        logging.exception(f"[actuals/upload-to-txn] analyzer failed: {_ae}")
        return jsonify({"error": f"Analyzer failed: {_ae}"}), 500
    try:
        del data
    except Exception:
        pass

    status_map = {"filed": "done", "needs_review": "review", "error": "error"}
    upload_status = status_map.get(result.get("status"), "error")
    vr            = result.get("vr") or {}

    vendor_name = None
    amount      = None
    doc_date    = None
    doc_number  = None
    if vr:
        v = vr.get("vendor") or {}
        vendor_name = v.get("name") or v.get("raw_name")
        try:
            amount = float(vr.get("total")) if vr.get("total") is not None else None
        except Exception:
            amount = None
        try:
            from datetime import datetime as _dt_mod
            _d = vr.get("date") or ""
            doc_date = _dt_mod.strptime(_d[:10], "%Y-%m-%d").date() if _d else None
        except Exception:
            doc_date = None
        doc_number = (vr.get("invoice_number") or vr.get("purchase_order_number")
                      or None)   # never a tax id (no TIN in FPBudget)
        if doc_number:
            doc_number = str(doc_number)[:100]
    from fp_analyzer import extract_card_last4 as _extract_card4
    card_last4 = _extract_card4(vr) if vr else None

    upload = DocUpload(
        project_id          = pid,
        uploader_id         = current_user.id,
        r2_key              = r2_key,
        original_filename   = f.filename,
        file_size           = _data_size,
        content_type        = content_type,
        file_hash           = file_hash,
        status              = upload_status,
        veryfi_data         = _json.dumps(vr) if vr else None,
        vendor              = vendor_name,
        amount              = amount,
        doc_date            = doc_date,
        doc_number          = doc_number,
        card_last4          = card_last4,
        confidence          = round(float(result.get("confidence") or 0) * 100, 2),
        category            = result.get("doc_type"),
        veryfi_category     = (vr.get("category") if vr else None),
        filed_filename      = result.get("new_filename") or None,
        filed_dropbox_path  = result.get("filed_path") or result.get("staged_path"),
        filed_at            = datetime.utcnow() if result.get("filed_path") else None,
        source_archive_path = result.get("staged_path"),
        is_duplicate        = bool(duplicate_of),  # byte-hash only; autorename ≠ duplicate (CR 2026-06-04)
        duplicate_of_id     = duplicate_of.id if duplicate_of else None,
    )
    db.session.add(upload)
    db.session.commit()

    # Link to the existing transaction. Backfill blank fields only;
    # don't override values the user may have already set.
    txn.doc_upload_id = upload.id
    if not txn.vendor and vendor_name:    txn.vendor   = vendor_name
    if txn.amount is None and amount is not None: txn.amount = amount
    if not txn.txn_date and doc_date:     txn.txn_date = doc_date.isoformat()
    if not txn.card_last4 and card_last4: txn.card_last4 = card_last4
    txn.match_status = 'confirmed'
    txn.updated_at   = datetime.utcnow()
    db.session.commit()

    try:
        _log_activity(
            action='create', entity_type='doc_upload',
            entity_id=upload.id,
            entity_label=(upload.filed_filename or upload.original_filename or f'Upload #{upload.id}'),
            project_id=pid,
            before=None,
            after={'status': upload.status, 'vendor': upload.vendor,
                   'amount': float(upload.amount or 0),
                   'category': upload.category,
                   'linked_transaction_id': txn.id},
            note=f'Uploaded "{upload.original_filename}" → linked to transaction #{txn.id}'
        )
    except Exception: pass

    return jsonify({
        "ok":              True,
        "upload_id":       upload.id,
        "transaction_id":  txn.id,
        "filed_filename":  upload.filed_filename or upload.original_filename,
        "doc_type":        upload.category,
        "status":          upload_status,
    })


@app.route("/projects/<int:pid>/actuals/sync-now", methods=["POST"])
@login_required
def actuals_sync_now(pid):
    """Run the COMPANY-WIDE QuickBooks sync (qbo_sync.sync_company) from a
    project's Sync button. Since the company ledger (2026-11-03) every
    account in Admin → QuickBooks is pulled into one pool, so a sync from
    any project refreshes the pool for all of them. The per-project
    account picker is retired: the account set lives on the connection."""
    project = ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    from models import QBOConnection
    conn = QBOConnection.query.first()
    if not conn or not conn.refresh_token:
        return jsonify({
            "error": "QuickBooks not connected. Go to Admin → QuickBooks to authorize.",
            "needs_oauth": True,
        }), 400

    import json as _json
    if not _json.loads(conn.enabled_account_ids or "[]"):
        return jsonify({
            "error": "No QuickBooks accounts selected for the company. Pick them under Admin → QuickBooks.",
        }), 400

    # Optional date overrides from the panel form. When omitted,
    # sync_company uses its watermark logic (sync_through if set,
    # else 90 days back).
    body = request.get_json(silent=True) or {}
    start_date = (body.get("start_date") or "").strip() or None
    end_date   = (body.get("end_date")   or "").strip() or None
    # Light validation — must look like YYYY-MM-DD if provided.
    import re as _re
    _date_re = _re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if start_date and not _date_re.match(start_date):
        return jsonify({"error": "start_date must be YYYY-MM-DD"}), 400
    if end_date and not _date_re.match(end_date):
        return jsonify({"error": "end_date must be YYYY-MM-DD"}), 400

    from qbo_sync import sync_company, SYNC_LOCK_KEY
    # Audit H4 (2026-07-20): single-flight. A double-click or an overlapping
    # run duplicated qbo_txn_ids → IntegrityError at commit → the ENTIRE
    # batch rolled back. Non-blocking try-lock (Postgres); sqlite dev runs
    # unguarded. One lock for the whole company now.
    _is_pg = 'postgresql' in str(db.engine.url).lower()
    if _is_pg:
        _got = db.session.execute(text("SELECT pg_try_advisory_lock(:k)"),
                                  {"k": SYNC_LOCK_KEY}).scalar()
        if not _got:
            return jsonify({"error": "A QuickBooks sync is already running — "
                                     "wait for it to finish."}), 409
    try:
        result = sync_company(conn, db, start_date=start_date, end_date=end_date)
        try:
            _imported   = int(result.get('imported') or 0)
            _enriched   = int(result.get('enriched') or 0)
            _reconciled = int(result.get('reconciled') or 0)
            _cdc        = int(result.get('cdc_additions') or 0)
            _qdel       = int(result.get('qbo_deleted') or 0)
            _qvoid      = int(result.get('qbo_voided') or 0)
            _range   = ''
            if start_date or end_date:
                _range = f' ({start_date or "…"} → {end_date or "…"})'
            _log_activity(
                action='update', entity_type='qbo_sync',
                entity_id=pid, entity_label=project.name or f'Project #{pid}',
                project_id=pid,
                after={'imported': _imported, 'enriched': _enriched,
                       'reconciled': _reconciled, 'cdc_additions': _cdc,
                       'qbo_deleted': _qdel, 'qbo_voided': _qvoid,
                       'sync_through': str(result.get('sync_through') or '')},
                note=f'QuickBooks company sync{_range}: +{_imported} new, {_enriched} enriched, '
                     f'{_reconciled} reconciled to receipts, {_cdc} via change-capture'
                     + (f', {_qdel} removed (deleted in QuickBooks)' if _qdel else '')
                     + (f', {_qvoid} flagged voided' if _qvoid else ''))
        except Exception: pass
        # Phase 5: every sync re-runs THE matching engine so new charges
        # meet the receipts already waiting (suggested, never confirmed).
        try:
            from actuals import refresh_match_suggestions
            result['match_suggestions'] = refresh_match_suggestions(pid).get('suggestions', 0)
        except Exception as _me:
            logging.warning(f"[actuals] post-sync matching failed (continuing): {_me}")
        # Phase 3: auto-refresh the review queue after ingestion (background).
        _bg_run_all_scans(pid)
        return jsonify({"ok": True, **result})
    except Exception as e:
        logging.exception(f"[actuals] sync_now failed: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if _is_pg:
            try:
                db.session.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": SYNC_LOCK_KEY})
                db.session.commit()
            except Exception:
                pass


@app.route("/projects/<int:pid>/actuals/absorb-linked-receipts", methods=["GET", "POST"])
@login_required
def actuals_absorb_linked_receipts(pid):
    """Repair sweep (2026-07-08): absorb doc-born placeholder rows whose doc
    ALREADY rides on a charge. The manual /link-doc path historically set
    charge.doc_upload_id without merging the receipt's own placeholder txn the
    way the suggested-match Confirm path (actuals.confirm_match) does — so the
    receipt kept showing under "Receipts to place" after being linked.
    /link-doc now absorbs on the spot; this crawls the legacy leftovers.

    ?dry=1 (default) reports; ?dry=0 runs confirm_match per pair (which merges
    coding/card/note off the placeholder before deleting it).
    """
    if getattr(current_user, 'role', None) != 'super_admin':
        return jsonify({"error": "Forbidden"}), 403
    dry = (request.values.get("dry", "1").strip().lower() not in ("0", "false", "no"))
    ProjectSheet.query.get_or_404(pid)
    from actuals import confirm_match
    pairs, healed, skipped, errors = [], [], [], []
    charges = (Transaction.query
               .filter(Transaction.project_id == pid,
                       Transaction.doc_upload_id.isnot(None),
                       Transaction.source.notin_(('doc_upload', 'invoice_split')))
               .all())
    for t in charges:
        sister = (Transaction.query
                  .filter_by(doc_upload_id=t.doc_upload_id, source='doc_upload')
                  .first())
        if not sister or sister.id == t.id:
            continue
        entry = {"charge_id": t.id, "placeholder_id": sister.id,
                 "vendor": t.vendor, "doc_upload_id": t.doc_upload_id,
                 "charge_amount": float(t.amount) if t.amount is not None else None,
                 "placeholder_amount": float(sister.amount) if sister.amount is not None else None,
                 "match_status": t.match_status}
        pairs.append(entry)
        if dry:
            continue
        if t.match_status not in ('suggested', 'confirmed'):
            skipped.append(entry)     # ambiguous state — leave for a human
            continue
        try:
            confirm_match(t.id)
            healed.append(t.id)
        except Exception as e:
            db.session.rollback()
            errors.append({"charge_id": t.id, "error": f"{type(e).__name__}: {e}"})
    return jsonify({"ok": True, "dry": dry, "pairs": pairs,
                    "healed": healed, "skipped": skipped, "errors": errors})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/backup-candidates", methods=["GET"])
@login_required
def actuals_backup_candidates(pid, tid):
    """Candidate targets a backup receipt might document: itemized invoice
    sublines first (they ARE the line items), then other doc-carrying charges.
    Ranked by amount closeness then date closeness to the receipt. (v2.)"""
    _require_project_role(pid, 'viewer')
    txn = _pool_txn_or_404(pid, tid)
    _amt = abs(float(txn.amount or 0))
    cands = (Transaction.query
             .filter(Transaction.project_id == pid,
                     Transaction.id != tid,
                     Transaction.not_project_expense == False,  # noqa: E712
                     Transaction.backup_of_txn_id.is_(None))
             .filter((Transaction.source == 'invoice_split')
                     | (Transaction.doc_upload_id.isnot(None)))
             .all())
    def _rank(t):
        a = abs(abs(float(t.amount or 0)) - _amt)
        split_first = 0 if t.source == 'invoice_split' else 1
        return (split_first, a)
    out = []
    for t in sorted(cands, key=_rank)[:12]:
        out.append({
            "id": t.id,
            "vendor": t.vendor or '',
            "amount": float(t.amount) if t.amount is not None else None,
            "date": t.txn_date or '',
            "is_split": t.source == 'invoice_split',
            "note": (t.note or '')[:80],
        })
    return jsonify({"ok": True, "candidates": out})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/mark-backup", methods=["POST"])
@login_required
def actuals_mark_backup(pid, tid):
    """Mark a receipt's charge as BACKUP DOCUMENTATION for an invoice already
    on the ledger — the receipt file stays filed, but its own charge is
    excluded from every rollup so the invoice's itemized lines aren't double-
    counted. (User 2026-07-20: vendor sends an invoice PLUS the underlying
    parking receipts → both were counting.) v1 rides not_project_expense (the
    established exclusion flag) with a 📎 note naming what it backs up."""
    _require_project_role(pid, 'editor')
    txn = _pool_txn_or_404(pid, tid)
    data = request.get_json(force=True) or {}
    ref = (data.get("backs_up") or '').strip()[:200]
    # v2: hard linkage to the specific transaction (usually an invoice_split
    # subline) this receipt documents — powers the Line Ledger 📎 attachment.
    _target = None
    _target_id = data.get("target_txn_id")
    if _target_id:
        _target = Transaction.query.filter_by(id=int(_target_id),
                                              project_id=pid).first()
        if not _target:
            return jsonify({"error": "target transaction not found"}), 404
        if _target.id == txn.id:
            return jsonify({"error": "a charge cannot back itself up"}), 400
    _absorb_receipt_as_backup(txn, _target, ref)
    db.session.commit()
    return jsonify({"ok": True, "note": txn.note})


@app.route("/projects/<int:pid>/actuals/budget-line/new", methods=["POST"])
@login_required
def actuals_new_budget_line(pid):
    """Create a budget line in the Working budget straight from a line picker
    (Actuals rows, doc coding, invoice itemization) — including under a COA
    section that isn't in the budget yet. Body: {account_code, description}.
    (User 2026-07-20: "billing a line that doesn't exist in the budget… I need
    to be able to add a section that is not currently included.")

    Reuses ensure_section_in_working_budget (auto-inits Working, seeds the
    section + Estimated peer). If that call just CREATED the section's
    placeholder line, we rename the placeholder to the requested description
    instead of leaving '(Auto-added — no estimate)' noise plus a second line;
    otherwise we append a new $0 line at the bottom of the section."""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    from actuals import ensure_section_in_working_budget, get_current_working_budget
    data = request.get_json(force=True) or {}
    desc = (data.get("description") or '').strip()[:200]
    if not desc:
        return jsonify({"error": "description required"}), 400
    try:
        code = int(data.get("account_code"))
    except (TypeError, ValueError):
        return jsonify({"error": "account_code must be a COA section code"}), 400
    sec_name = dict(FP_COA_SECTIONS).get(code)
    if not sec_name:
        return jsonify({"error": f"unknown COA section {code}"}), 400

    res = ensure_section_in_working_budget(pid, code, sec_name)
    # Pre-Working projects (no auto-init on coding, owner 2026-09-01): the
    # section/line lands on the current Estimated instead.
    from actuals import get_current_estimated_budget as _gceb
    working = get_current_working_budget(pid) or _gceb(pid)
    if not working:
        return jsonify({"error": "This project has no budget yet — create an "
                                 "Estimated or Working budget first."}), 400
    line = None
    if res.get('created') and res.get('working_line_id'):
        # Brand-new section → its fresh placeholder becomes the named line.
        _ph = BudgetLine.query.get(res['working_line_id'])
        if _ph and (_ph.description or '').startswith('(Auto-added'):
            _ph.description = desc
            line = _ph
    if line is None:
        line = BudgetLine(
            budget_id=working.id, account_code=code, account_name=sec_name[:100],
            description=desc, is_labor=False, quantity=1, days=1, rate=0,
            fringe_type='N', agent_pct=0, rate_type='flat_day',
            estimated_total=0, working_total=0, sort_order=99999)
        db.session.add(line)
    db.session.commit()
    try:
        _log_activity(action='create', entity_type='budget_line', entity_id=line.id,
                      entity_label=desc, project_id=pid,
                      note=f'Added "{desc}" under {code} · {sec_name} from a line picker')
    except Exception:
        pass
    return jsonify({"ok": True, "line_id": line.id, "account_code": code,
                    "section_name": sec_name, "label": desc})


@app.route("/projects/<int:pid>/actuals/import-bank-csv", methods=["POST"])
@login_required
def actuals_import_bank_csv(pid):
    """Import monthly bank / credit-card CSV (Chase etc.) into project actuals as
    source='csv_import' Transactions. Dry-run preview by default; ?apply=1 to
    commit. Charges (negative amounts) import as expenses; refunds/returns as
    credits (is_expense=False); card-payment rows are SKIPPED (not project
    expenses). De-dupes against rows already imported with the same
    (date, vendor, |amount|, card) so overlapping months don't double up.
    (User 2026-06-02.)"""
    if current_user.role == 'viewer':
        return jsonify({"error": "Forbidden — viewers cannot import"}), 403
    ProjectSheet.query.get_or_404(pid)
    apply = (request.args.get('apply') == '1')
    include_payments = (request.args.get('include_payments') == '1')
    f = request.files.get('file')
    if not f:
        return jsonify({"error": "No file uploaded"}), 400
    _upl_err = _validate_upload_file(f, 'document') if f else None
    if _upl_err:
        return jsonify({"error": _upl_err}), 400
    try:
        content = f.read().decode('utf-8-sig')
    except UnicodeDecodeError:
        return jsonify({"error": "File must be UTF-8 encoded CSV"}), 400

    charge_sign = (request.args.get('charge_sign') or 'auto').lower()
    if charge_sign not in ('auto', 'pos', 'neg'):
        charge_sign = 'auto'
    rows, stats = _parse_bank_csv(content, charge_sign=charge_sign)
    if not stats["detected_columns"].get('date') or not (
            stats["detected_columns"].get('amount')
            or stats["detected_columns"].get('debit')
            or stats["detected_columns"].get('credit')):
        return jsonify({"error": "Could not find a date column and an amount "
                                 "(or debit/credit) column in this CSV.",
                        "detected_columns": stats["detected_columns"],
                        "header": list((csv.reader(io.StringIO(content)).__next__()
                                        if content else []))}), 400

    # De-dup by COUNT, not presence, so legitimately-identical same-day charges
    # (two $6.75 coffees on the same card) both import, while overlapping months
    # still don't double up. Skip a row only while we haven't yet matched the
    # number already in the DB for that fingerprint. (Code review 2026-06-04.)
    from collections import Counter as _Counter
    existing_counts = _Counter()
    for t in (Transaction.query
              .filter(Transaction.project_id == pid,
                      Transaction.source == 'csv_import').all()):
        existing_counts[(t.txn_date, (t.vendor or '').strip().lower(),
                         float(t.amount or 0), t.card_last4 or '')] += 1

    importable, dups, payments, transfers, zeros = [], 0, 0, 0, 0
    batch_counts = _Counter()
    for r in rows:
        if r["kind"] == 'zero':
            zeros += 1
            continue
        if r["kind"] == 'payment' and not include_payments:
            payments += 1
            continue
        if r["kind"] == 'transfer' and not include_payments:
            transfers += 1
            continue
        fp = (r["txn_date"], r["vendor"].strip().lower(), r["amount"], r["card_last4"] or '')
        batch_counts[fp] += 1
        # first existing_counts[fp] occurrences in this file are already in the DB
        if batch_counts[fp] <= existing_counts.get(fp, 0):
            dups += 1
            continue
        importable.append(r)

    dates = [r["txn_date"] for r in importable]
    by_card = {}
    for r in importable:
        by_card[r["card_last4"] or '—'] = by_card.get(r["card_last4"] or '—', 0) + 1
    report = {
        "dry": (not apply), "applied": apply, "project": pid,
        "detected_columns": stats["detected_columns"],
        "charge_polarity": stats.get("charge_polarity"),
        "polarity_source": stats.get("polarity_source"),
        "spend_sign_counts": stats.get("spend_sign_counts"),
        "rows_in_file": len(rows),
        "to_import": len(importable),
        "charges": sum(1 for r in importable if r["kind"] == 'charge'),
        "credits": sum(1 for r in importable if r["kind"] == 'credit'),
        "skipped_card_payments": payments,
        "skipped_transfers": transfers,
        "skipped_duplicates": dups,
        "skipped_zero": zeros,
        "parse_errors": stats["parse_errors"][:25],
        "parse_error_count": len(stats["parse_errors"]),
        "date_range": ([min(dates), max(dates)] if dates else None),
        "by_card": by_card,
        "sample": [{"date": r["txn_date"], "vendor": r["vendor"][:40],
                    "amount": r["amount"], "expense": r["is_expense"],
                    "card": r["card_last4"], "note": r["note"]}
                   for r in importable[:15]],
        "errors": [],
    }
    if apply and importable:
        try:
            from actuals import payee_for_vendor_string as _pfv
            _payee_cache = {}
            for r in importable:
                # Attribution (Documents Phase 3): a vendor string linked to
                # a Payee (VendorAlias.payee_id, or an exact name match)
                # attributes the row on arrival — source 'alias'.
                _vk = (r["vendor"] or '').strip().lower()
                if _vk not in _payee_cache:
                    _payee_cache[_vk] = _pfv(pid, r["vendor"])
                _pid_ = _payee_cache[_vk]
                db.session.add(Transaction(
                    project_id=pid, amount=r["amount"], is_expense=r["is_expense"],
                    vendor=r["vendor"], txn_date=r["txn_date"], note=r["note"],
                    card_last4=r["card_last4"], source='csv_import',
                    match_status='unmatched',
                    payee_id=_pid_, payee_source=('alias' if _pid_ else None),
                    created_via_user_id=getattr(current_user, 'id', None)))
            db.session.commit()
            report["created"] = len(importable)
            # Auto-run the matcher so the freshly-imported charges get paired
            # with open receipts immediately — previously suggestions only
            # appeared after a manual ⚡ Auto-Match click. (User 2026-06-11.)
            try:
                from actuals import run_auto_match
                _am = run_auto_match(pid)
                report["auto_match"] = {"suggestions": _am.get("suggestions"),
                                        "qbo_unmatched": _am.get("qbo_unmatched"),
                                        "doc_open": _am.get("doc_open")}
            except Exception as _ame:
                report["auto_match_error"] = type(_ame).__name__
        except Exception as e:
            db.session.rollback()
            report["errors"].append(str(e))
            return jsonify(report), 500
    # Phase 3: auto-refresh the review queue after ingestion (background).
    _bg_run_all_scans(pid)
    return jsonify(report)


# ═══════════════════════════════════════════════════════════════════════════
# ACTUALS_HANDOFF Phase 2 (2026-10-30): the new workspace payload, splits and
# the hand-typed manual expense. Pure rules (split validation, manual-expense
# validation, row flags, qbo_txn_id grouping) live in actuals.py and are
# unit-tested; this file only wires them to the request and the database.
# ═══════════════════════════════════════════════════════════════════════════

def _ws_line_label(ln):
    return f"{ln.account_code} · {(ln.description or ln.account_name or '')[:50]}"


def _ws_working_budget_and_lines(pid):
    """The current Working budget (falling back to the current Estimated,
    like _line_picker_groups) and its lines keyed by id."""
    budget, _groups = _line_picker_groups(pid)
    if not budget:
        return None, {}
    return budget, {l.id: l for l in budget.lines}


def _ws_line_estimates(budget, lines):
    """Live estimate per line, same calc the budget grid uses (via
    app._po_live_line_totals). Falls back to the stored column."""
    try:
        from app import _po_live_line_totals
        return _po_live_line_totals(budget, list(lines), budget.project_id)
    except Exception as _e:
        logging.warning(f"[workspace] live estimates failed, using stored: {_e}")
        return {l.id: float(l.estimated_total or 0) for l in lines}


def actuals_counts(pid):
    """The Actuals workspace's "Needs you" numbers for one project, computed
    by the SAME row-flag logic the workspace itself uses (no second copy of
    the rules — Cleanup C, 2026-11-02). Skips the matchers and the budget
    rollup, which the counts do not need. Fail-open: {} on any error.

      unclaimed / unclaimed_amount — charges with no line
      needs_backup                 — assigned charges with no receipt / reason
      receipts                     — receipts and invoices with no charge
      matches_to_review            — charges with a suggested receipt match
    """
    try:
        ws = _build_workspace(pid, run_matchers=False, with_budget=False)
    except Exception as _e:
        logging.warning(f"[actuals_counts] failed for project {pid}: {_e}")
        return {}
    rows = ws.get("transactions") or []
    out = dict(ws.get("counts") or {})
    out["unclaimed_amount"] = round(sum(
        float(r.get("amount") or 0) for r in rows if 'no_line' in r["flags"]["red"]), 2)
    # Both directions are the same pairs (a suggestion lives on the charge
    # and shows on its receipt); the workspace already counted them.
    out.setdefault("matches_to_review", sum(1 for r in rows if r.get("receipt_suggestion")))
    return out


@app.route("/projects/<int:pid>/actuals/workspace.json", methods=["GET"])
@login_required
def actuals_workspace_json(pid):
    """ONE payload for the new Actuals workspace (Phase 2, 2026-10-30), built
    from the same reads the retired /register payload (reconcile_workspace_json,
    removed in Cleanup B 2026-11-02) used.

      transactions      — every pool row of this project, one per QBO charge
                          (sibling rows of the same qbo_txn_id collapse to
                          one; see actuals.group_pool_rows), each with its
                          suggested line, assigned line, splits, payee, the
                          excluded info and every red / yellow flag computed
                          server-side (actuals.compute_row_flags).
      receipts          — DocUpload receipts / invoices with no transaction
                          (the Available Receipts inbox).
      not_project       — this project's excluded rows, newest first.
      claimed_elsewhere — rows another project claimed (project name only).
      sync              — QBO connection / window / last-sync status.
      duplicates        — groups of charges that look like one event, each
                          with its suggested keeper (Phase 5; one rule:
                          actuals.find_duplicate_groups + pick_keeper).
      voided            — excluded rows QuickBooks voided after import.
      counts            — the views: unclaimed, assigned, needs_backup,
                          receipts, duplicates, not_project, sync, plus
                          matches_to_review / strong_matches.
      budget            — sections and lines of the current Working budget
                          with estimate, actual (counts_as_spend + splits),
                          variance and is_drop_target.
    Runs the heuristic + incremental AI matchers first, fail-open, like the
    register does — suggestions are shown, never applied."""
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'viewer')
    return jsonify(_build_workspace(pid))


def _build_workspace(pid, run_matchers=True, with_budget=True):
    """The workspace payload as a plain dict (see actuals_workspace_json).
    run_matchers=False skips the heuristic / AI matchers; with_budget=False
    skips the budget-side rollup — both for actuals_counts, which only needs
    the rows and their flags."""
    from actuals import (run_auto_match, counts_as_spend, signed_amount_value,
                         split_sums_by_line, group_pool_rows, compute_row_flags,
                         find_duplicate_groups, score_match, rank_candidates,
                         open_receipts, VOIDED_IN_QBO_REASON)
    from models import ExpenseEvidence
    from budget_calc import FP_COA_NAMES as _WS_COA
    if run_matchers:
        try:
            run_auto_match(pid)
        except Exception as _e:
            logging.warning(f"[workspace.json] auto-match failed (continuing): {_e}")
        try:
            _ai_match_batch(pid, limit=8)
        except Exception as _ae:
            logging.warning(f"[workspace.json] AI batch failed (continuing): {_ae}")

    budget, lines_by_id = _ws_working_budget_and_lines(pid)
    _any_line = {}
    def _line_label_any(lid):
        """Label for a line that may sit on an older budget."""
        if lid in lines_by_id:
            return _ws_line_label(lines_by_id[lid])
        if lid not in _any_line:
            _l = BudgetLine.query.get(lid)
            _any_line[lid] = _ws_line_label(_l) if _l else f"line #{lid}"
        return _any_line[lid]

    docs = {d.id: d for d in DocUpload.query.filter(
        DocUpload.project_id == pid,
        DocUpload.status.notin_(('deleted',))).all()}
    def _doc_brief(d):
        return {"doc_id": d.id,
                "name": (d.filed_filename or d.original_filename or f"Doc #{d.id}")[:120],
                "vendor": d.vendor,
                "amount": float(d.amount) if d.amount is not None else None,
                "date": d.doc_date.isoformat() if d.doc_date else None,
                "category": d.category,
                "dup_pending": bool(d.is_duplicate and (d.status or '') != 'duplicate'),
                "thumb": f"/docs/upload/{d.id}/thumb"}

    # Every top-level money row (same selection the retired reconcile_workspace_json used):
    # electronic + hand-typed; doc-born rows only once activated; never
    # split children or backup rows. The project's POOL (company ledger,
    # 2026-11-03): its own rows + every unclaimed QuickBooks row.
    txns = (Transaction.query
            .filter(pool_filter(pid),
                    db.or_(Transaction.source.is_(None),
                           Transaction.source.notin_(('doc_upload', 'invoice_split'))),
                    Transaction.backup_of_txn_id.is_(None))
            .order_by(Transaction.txn_date.desc().nullslast(), Transaction.id.desc())
            .all())
    _activated_docs = (Transaction.query
                       .filter(Transaction.project_id == pid,
                               Transaction.source == 'doc_upload',
                               Transaction.activated_at.isnot(None),
                               Transaction.backup_of_txn_id.is_(None))
                       .all())
    txns = txns + _activated_docs
    # Pool rows this project marked Not project (company ledger dismissals,
    # 2026-11-04): they stay in the pool for everyone else and show here
    # under Not project with Restore.
    _dismissed_ids = set()
    for _t, _d in dismissed_pool_rows(pid):
        _t._pool_dismissal = _d
        txns.append(_t)
        _dismissed_ids.add(_t.id)
    txn_ids = [t.id for t in txns]
    # Splits, evidence, itemized children, crew names — bulk reads.
    splits_by_txn = {}
    if txn_ids:
        for sp in TransactionSplit.query.filter(
                TransactionSplit.transaction_id.in_(txn_ids)).all():
            splits_by_txn.setdefault(sp.transaction_id, []).append(sp)
    evidence_txn_ids = set()
    if txn_ids:
        evidence_txn_ids = {r[0] for r in db.session.query(ExpenseEvidence.transaction_id)
                            .filter(ExpenseEvidence.transaction_id.in_(txn_ids)).all()}
    kids_by_parent = {}
    for k in Transaction.query.filter(Transaction.project_id == pid,
                                      Transaction.source == 'invoice_split').all():
        if k.parent_transaction_id:
            kids_by_parent.setdefault(k.parent_transaction_id, []).append(k)
    crew_names = {}
    _crew_ids = {t.paid_by_crew_member_id for t in txns if getattr(t, 'paid_by_crew_member_id', None)}
    if _crew_ids:
        from models import CrewMember
        crew_names = {c.id: c.name for c in CrewMember.query.filter(CrewMember.id.in_(list(_crew_ids))).all()}
    for t in txns:
        t.has_splits = bool(splits_by_txn.get(t.id))
        t.has_evidence = t.id in evidence_txn_ids
    # Possible duplicates — ONE rule (Phase 5, 2026-11-03):
    # actuals.find_duplicate_groups / is_duplicate_twin (same |amount|, same
    # vendor or card, within 3 days for two QuickBooks records, same day
    # otherwise, DIFFERENT QuickBooks ids) with the keeper rule. "Keep both"
    # is remembered in transaction_dup_dismissal under the group key.
    dismissed = {d.dup_key for d in TransactionDupDismissal.query.filter_by(project_id=pid).all()}
    _dup_groups = find_duplicate_groups([t for t in txns if t.id not in _dismissed_ids], dismissed)
    dup_group_of = {}
    for _g in _dup_groups:
        for _m in _g["members"]:
            dup_group_of[_m.id] = _g["key"]
    dup_ids = set(dup_group_of)
    grouped = group_pool_rows(txns)
    # A charge whose attached receipt/invoice already has its own coded row
    # (source='doc_upload' on a line) should be told so: that line is the
    # strongest suggestion there is, and accepting it absorbs the receipt row
    # (link_transaction_to_line → confirm_match). Keyed by doc id. (Nov 2 2026)
    _sister_line_by_doc = {}
    for _s in Transaction.query.filter(Transaction.project_id == pid,
                                       Transaction.source == 'doc_upload',
                                       Transaction.doc_upload_id.isnot(None),
                                       Transaction.budget_line_id.isnot(None)).all():
        _sister_line_by_doc.setdefault(_s.doc_upload_id, _s.budget_line_id)

    transactions, not_project, claimed_elsewhere = [], [], []
    matched_doc_ids = set()
    try:
        _inbox_pool = open_receipts(pid)
    except Exception as _ope:
        logging.warning(f"[workspace.json] open_receipts failed: {_ope}")
        _inbox_pool = []
    for rep, members in grouped:
        t = rep
        _sps = splits_by_txn.get(t.id, [])
        splits = [{"id": sp.id, "budget_line_id": sp.budget_line_id,
                   "label": _line_label_any(sp.budget_line_id) if sp.budget_line_id else None,
                   "amount": float(sp.amount or 0), "note": sp.note} for sp in _sps]
        _kids = kids_by_parent.get(t.id)
        itemized = [{"id": k.id, "desc": (k.note or '')[:90],
                     "amount": float(k.amount or 0),
                     "budget_line_id": k.budget_line_id,
                     "label": (_line_label_any(k.budget_line_id) if k.budget_line_id else None)}
                    for k in _kids] if _kids else []
        doc = docs.get(t.doc_upload_id) if t.doc_upload_id else None
        receipt = None
        suggestion_doc = None
        if doc is not None:
            if t.match_status == 'confirmed':
                receipt = _doc_brief(doc)
                matched_doc_ids.add(doc.id)
            elif t.match_status == 'suggested':
                # Confidence + the engine's reasons for THIS pair, and
                # whether it is strong (no other inbox receipt within 0.2):
                # the Review-matches dialog and the receipt row show them.
                _sc = score_match(t, doc) or {}
                _ranked = rank_candidates(t, _inbox_pool, key=lambda d: d.id, limit=2)
                _strong = bool(_ranked and _ranked[0]["key"] == doc.id and _ranked[0]["strong"])
                suggestion_doc = {**_doc_brief(doc),
                                  "confidence": float(t.match_confidence or _sc.get("confidence") or 0),
                                  "reasons": _sc.get("reasons") or [],
                                  "strong": _strong,
                                  "source": t.match_source or 'heuristic'}
        suggested_line = None
        if getattr(t, 'suggested_budget_line_id', None):
            suggested_line = {"id": t.suggested_budget_line_id,
                              "label": _line_label_any(t.suggested_budget_line_id),
                              "confidence": float(t.ai_code_confidence or t.match_confidence or 0),
                              "reason": getattr(t, 'ai_code_reason', None)}
        elif getattr(t, 'ai_suggested_code', None):
            suggested_line = {"id": None, "section": t.ai_suggested_code,
                              "label": f"{t.ai_suggested_code} · {t.ai_suggested_code_name or _WS_COA.get(t.ai_suggested_code, '')}",
                              "confidence": float(t.ai_code_confidence or 0),
                              "reason": getattr(t, 'ai_code_reason', None)}
        # The attached receipt's own row is already coded → its line wins
        # over any AI guess; accepting it folds that row into this charge.
        if (t.doc_upload_id and t.source != 'doc_upload' and not t.budget_line_id
                and _sister_line_by_doc.get(t.doc_upload_id)):
            _sl = _sister_line_by_doc[t.doc_upload_id]
            suggested_line = {"id": _sl, "label": _line_label_any(_sl),
                              "confidence": 1.0,
                              "reason": "Its receipt is already on this line — accept to fold them together"}
        assigned_line = None
        if t.budget_line_id:
            assigned_line = {"id": t.budget_line_id, "label": _line_label_any(t.budget_line_id)}
        section_only = None
        if not t.budget_line_id and t.account_code:
            section_only = {"code": t.account_code,
                            "label": f"{t.account_code} · {_WS_COA.get(t.account_code, '')}"}
        has_backup = (t.id in evidence_txn_ids
                      or bool(getattr(t, 'no_charge_reason', None))
                      or (receipt is not None))
        flag_input = {
            "budget_line_id": t.budget_line_id,
            # Section-only coding and an itemized parent count as assigned.
            "assigned": bool(t.budget_line_id or splits or section_only or itemized),
            "splits": splits, "split_sum_ok": None,
            "amount": float(t.amount or 0), "is_expense": bool(t.is_expense),
            "has_backup": has_backup,
            "receipt_amount": (receipt or {}).get("amount"),
            "receipt_date": (receipt or {}).get("date"),
            "txn_date": (t.txn_date or '')[:10] or None,
            "possible_duplicate": t.id in dup_ids,
            "voided": (getattr(t, 'no_charge_reason', None) == VOIDED_IN_QBO_REASON),
            "payment_method": getattr(t, 'payment_method', None),
            "not_project_expense": bool(t.not_project_expense or t.id in _dismissed_ids),
        }
        flags = compute_row_flags(flag_input)
        excluded = _txn_excluded_info(t)
        row = {
            "id": t.id,
            "qbo_txn_id": t.qbo_txn_id, "qbo_txn_type": t.qbo_txn_type,
            # Company ledger: an unclaimed QuickBooks row is in the pool
            # (every project sees it until one claims it); a flagged row
            # is claimed by two projects — see the banner.
            "pooled": t.project_id is None,
            "claim_conflict": bool(getattr(t, 'claim_conflict', False)),
            "date": (t.txn_date or '')[:10],
            "vendor": t.vendor or '—',
            "amount": signed_amount_value(t.amount, t.is_expense),
            "is_expense": bool(t.is_expense),
            "card_last4": getattr(t, 'card_last4', None),
            "source": ('receipt' if t.source == 'doc_upload' else (t.source or 'manual_entry')),
            "memo": (t.note or '')[:140] or (t.qbo_category or ''),
            "qbo_category": t.qbo_category,
            "match_status": t.match_status,
            "suggested_line": suggested_line,
            "assigned_line": assigned_line,
            "section_only": section_only,
            "splits": splits,
            "itemized": itemized,
            "receipt": receipt,
            "receipt_suggestion": suggestion_doc,
            "has_backup": has_backup,
            "no_charge_reason": getattr(t, 'no_charge_reason', None),
            "payment_method": getattr(t, 'payment_method', None),
            "payee": (crew_names.get(t.paid_by_crew_member_id)
                      if getattr(t, 'paid_by_crew_member_id', None) else None),
            "paid_by_crew_member_id": getattr(t, 'paid_by_crew_member_id', None),
            "flags": flags,
            "excluded": excluded,
            "reviewed": bool(getattr(t, 'reviewed_at', None)),
            "dup_group": dup_group_of.get(t.id),
            "voided": bool(flag_input["voided"]),
        }
        if t.not_project_expense or t.id in _dismissed_ids:
            not_project.append(row)
        else:
            transactions.append(row)

    # Receipts inbox: proof docs with NO transaction behind them — not
    # matched to a charge and not activated into an expense. A doc whose
    # row is claimed elsewhere is a conflict, shown with the project name.
    _doc_txns = {}
    for dt in Transaction.query.filter(Transaction.project_id == pid,
                                       Transaction.source == 'doc_upload',
                                       Transaction.doc_upload_id.isnot(None)).all():
        _doc_txns.setdefault(dt.doc_upload_id, dt)
    receipts = []
    for d in docs.values():
        if (d.category or '') not in ('receipt', 'invoice'):
            continue
        if (d.status or '') == 'duplicate' or d.id in matched_doc_ids:
            continue
        dt = _doc_txns.get(d.id)
        if dt is not None and (dt.activated_at is not None or dt.not_project_expense):
            continue
        r = {**_doc_brief(d), "txn_id": (dt.id if dt is not None else None),
             "status": d.status, "suggested_charge_id": None, "suggested_charge": None}
        # The charge this receipt is suggested for (Phase 5: the suggestion
        # shows on BOTH sides — "Matches charge · Lyft · Sep 28 · $88.44 ·
        # 88%" with Confirm / Not a match on the receipt row too).
        for row in transactions:
            _rs = row.get("receipt_suggestion")
            if _rs and _rs["doc_id"] == d.id:
                r["suggested_charge_id"] = row["id"]
                r["suggested_charge"] = {"id": row["id"], "vendor": row["vendor"],
                                         "amount": row["amount"], "date": row["date"],
                                         "card_last4": row.get("card_last4"),
                                         "confidence": _rs["confidence"],
                                         "reasons": _rs.get("reasons") or [],
                                         "strong": bool(_rs.get("strong"))}
                break
        receipts.append(r)
    receipts.sort(key=lambda r: (r.get("date") or ''), reverse=True)
    not_project.sort(key=lambda r: ((r["excluded"] or {}).get("at") or '',
                                    r["date"] or '', r["id"]), reverse=True)
    transactions.sort(key=lambda r: (r["date"] or '', r["id"]), reverse=True)

    # Budget side — sections and lines of the current Working with estimate,
    # actual (counts_as_spend direct sums + splits), variance.
    sections = []
    if budget and with_budget:
        _lines = list(lines_by_id.values())
        _ests = _ws_line_estimates(budget, _lines)
        _parents = _split_parent_id_set(pid)
        direct = {}
        try:
            for lid, amt in (db.session.query(Transaction.budget_line_id, func.sum(_signed_amount()))
                             .filter(Transaction.project_id == pid,
                                     Transaction.budget_line_id.in_(list(lines_by_id.keys()) or [-1]),
                                     counts_as_spend(_parents),
                                     _exclude_estimate_linked_txns_clause())
                             .group_by(Transaction.budget_line_id).all()):
                direct[lid] = float(amt or 0)
            for lid, amt in split_sums_by_line(pid, list(lines_by_id.keys())).items():
                direct[lid] = direct.get(lid, 0.0) + amt
        except Exception as _re:
            logging.warning(f"[workspace.json] budget rollup failed: {_re}")
        from app import _section_for_code
        by_sec = {}
        for ln in sorted(_lines, key=lambda l: (l.account_code or 0, l.sort_order or 0, l.id)):
            try:
                sec = _section_for_code(int(ln.account_code))
            except (TypeError, ValueError):
                sec = None
            est = round(float(_ests.get(ln.id, 0) or 0), 2)
            act = round(direct.get(ln.id, 0.0), 2)
            by_sec.setdefault(sec, []).append({
                "id": ln.id, "account_code": ln.account_code,
                "label": _ws_line_label(ln),
                "description": ln.description or ln.account_name or '',
                "is_labor": bool(ln.is_labor),
                "line_tag": ln.line_tag,
                "parent_line_id": ln.parent_line_id,
                "estimate": est, "actual": act,
                "variance": round(est - act, 2),
                # Phase 4: the per-department Payroll Fringes lines (in 6500)
                # and the hidden auto-calc lines are real coding targets;
                # fringe_for_section / auto_kind let the screen label them
                # ("Payroll Fringes — Production Staff"). Header / spacer
                # rows are never targets; a labor section has no fringe row.
                "fringe_for_section": getattr(ln, 'fringe_for_section', None),
                "auto_kind": getattr(ln, 'auto_kind', None),
                "is_drop_target": (((ln.line_tag or '') in ('fringe_rollup', 'auto_calc'))
                                   or (not bool(ln.sync_omit)
                                       and (ln.line_tag or '') not in ('header', 'spacer'))),
            })
        _order = {code: i for i, (code, _n) in enumerate(FP_COA_SECTIONS)}
        for sec in sorted(by_sec, key=lambda c: (_order.get(c, 9999), c or 0)):
            rows = by_sec[sec]
            sections.append({
                "code": sec, "name": (_section_name(sec) if sec is not None else 'Other'),
                "estimate": round(sum(r["estimate"] for r in rows), 2),
                "actual": round(sum(r["actual"] for r in rows), 2),
                "variance": round(sum(r["variance"] for r in rows), 2),
                "is_drop_target": sec is not None,
                "lines": rows})

    # Sync is company-wide (2026-11-03): the account set and the watermarks
    # live on the QuickBooks connection (Admin → QuickBooks), not the project.
    import json as _json_ws
    from models import QBOConnection as _WSQC
    try:
        _qc = _WSQC.query.first()
    except Exception:
        _qc = None
    try:
        _acct_n = len(_json_ws.loads((_qc.enabled_account_ids if _qc else None) or '[]'))
    except Exception:
        _acct_n = 0
    sync = {"connected": bool(_qc and _qc.refresh_token),
            "accounts": _acct_n,
            "last_synced": (_qc.last_synced.isoformat(timespec='minutes')
                            if _qc is not None and _qc.last_synced else None),
            "last_cdc_sync": (_qc.last_cdc_sync.isoformat(timespec='minutes')
                              if _qc is not None and _qc.last_cdc_sync else None),
            "sync_through": (_qc.sync_through if _qc is not None else None),
            "company_wide": True,
            "auto": "twice a day"}
    # Duplicates view (Phase 5): one entry per group with the suggested
    # keeper and how each extra would be removed (hard delete, or move its
    # coding to the keeper first), plus the rows QuickBooks voided.
    from actuals import duplicate_delete_mode as _ddm
    _row_by_id = {r["id"]: r for r in transactions}
    duplicates = []
    for _g in _dup_groups:
        _members = [_row_by_id[m.id] for m in _g["members"] if m.id in _row_by_id]
        if len(_members) < 2:
            continue
        duplicates.append({
            "key": _g["key"],
            "keeper_id": _g["keeper"].id,
            "keeper_reason": _g["keeper_reason"],
            "vendor": _g["keeper"].vendor or '—',
            "amount": abs(float(_g["keeper"].amount or 0)),
            "members": [{"id": r["id"], "date": r["date"], "vendor": r["vendor"],
                         "amount": r["amount"], "source": r["source"],
                         "qbo_txn_id": r.get("qbo_txn_id"), "qbo_txn_type": r.get("qbo_txn_type"),
                         "card_last4": r.get("card_last4"),
                         "assigned_line": r.get("assigned_line"), "splits": r.get("splits") or [],
                         "receipt": r.get("receipt"), "has_backup": r.get("has_backup"),
                         "delete_mode": _ddm(next(m for m in _g["members"] if m.id == r["id"]))}
                        for r in _members],
        })
    voided = [r for r in not_project if r.get("voided")]
    n_unclaimed = sum(1 for r in transactions if 'no_line' in r["flags"]["red"])
    n_assigned = len(transactions) - n_unclaimed
    n_needs_backup = sum(1 for r in transactions if 'needs_backup' in r["flags"]["red"])
    from actuals import claim_conflicts_for
    try:
        n_conflicts = claim_conflicts_for(pid)
    except Exception:
        n_conflicts = 0
    counts = {"unclaimed": n_unclaimed, "assigned": n_assigned,
              "needs_backup": n_needs_backup, "receipts": len(receipts),
              "not_project": len(not_project),
              "duplicates": len(duplicates) + len(voided),
              "matches_to_review": sum(1 for r in transactions if r.get("receipt_suggestion")),
              "strong_matches": sum(1 for r in transactions
                                    if (r.get("receipt_suggestion") or {}).get("strong")),
              "sync": (0 if sync["connected"] else 1),
              "claimed_elsewhere": 0,
              "claim_conflicts": n_conflicts,
              "pooled": sum(1 for r in transactions if r.get("pooled"))}
    return {"ok": True, "project_id": pid,
            "budget_id": (budget.id if budget else None),
            "transactions": transactions, "receipts": receipts,
            "not_project": not_project,
            "duplicates": duplicates, "voided": voided,
            "claimed_elsewhere": claimed_elsewhere,
            "conflicts_url": (url_for('admin_ledger_conflicts', pid=pid) if n_conflicts else None),
            "sync": sync, "counts": counts,
            "budget": {"sections": sections,
                       "name": (budget.name if budget else None),
                       "mode": (getattr(budget, 'budget_mode', None) if budget else None)}}


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/split", methods=["POST", "DELETE"])
@login_required
def actuals_split_transaction(pid, tid):
    """Allocate one transaction across several lines of this project's
    current Working budget (Concur's Allocate; Phase 2, 2026-10-30).

    POST body {splits: [{budget_line_id, amount, note?}, ...]}: every line
    must be on the current Working budget, the amounts must sum to the
    transaction amount to the cent (else 400 with the delta), and the save
    REPLACES any existing splits. The transaction's own budget_line_id /
    account_code go NULL — it is now a split parent that reaches rollups
    only through its splits (actuals.split_sums_by_line). Claims the charge
    for this project and is logged with an undo payload.

    DELETE removes the splits; the transaction returns to Unclaimed."""
    from actuals import validate_splits
    txn = _pool_txn_or_404(pid, tid)
    _require_project_role(pid, 'editor')
    _old = [{"budget_line_id": sp.budget_line_id, "amount": float(sp.amount or 0),
             "note": sp.note} for sp in TransactionSplit.query.filter_by(transaction_id=tid).all()]
    _before = {'budget_line_id': txn.budget_line_id, 'account_code': txn.account_code,
               'account_code_name': txn.account_code_name,
               'not_project_expense': bool(txn.not_project_expense), 'splits': _old}
    _label = (txn.vendor or txn.note or f'Txn #{txn.id}')[:80]
    _amt = float(txn.amount or 0)

    if request.method == "DELETE":
        if not _old:
            return jsonify({"ok": True, "already": True, "transaction": _txn_state_json(txn)})
        TransactionSplit.query.filter_by(transaction_id=tid).delete(synchronize_session=False)
        txn.updated_at = datetime.utcnow()
        # The split was the claim → a QuickBooks row returns to the pool.
        release(txn, why='split removed', has_splits=False)
        db.session.commit()
        _act = None
        try:
            _act = _log_activity(action='update', entity_type='transaction', entity_id=txn.id,
                                 entity_label=_label, project_id=pid,
                                 before=_before, after={**_before, 'splits': []},
                                 note=f'Removed the split on ${_amt:,.2f} ({len(_old)} lines)')
        except Exception: pass
        return jsonify({"ok": True, "transaction": _txn_state_json(txn), "splits": [],
                        "activity_id": (_act.id if _act else None)})

    _ge = _guard_doc_expense(txn, {})
    if _ge is not None:
        return _ge
    if txn.source == 'invoice_split' and txn.parent_transaction_id:
        return jsonify({"error": "This is one line of an itemized document — "
                                 "split the parent document instead."}), 409
    if db.session.query(Transaction.id).filter_by(
            parent_transaction_id=txn.id, source='invoice_split').first() is not None:
        return jsonify({"error": "This document is itemized into line items — "
                                 "clear the itemization before splitting it."}), 409
    body = request.get_json(silent=True) or {}
    budget, lines_by_id = _ws_working_budget_and_lines(pid)
    if not budget:
        return jsonify({"error": "This project has no current budget to split against."}), 400
    clean, err = validate_splits(_amt, body.get('splits'), set(lines_by_id.keys()))
    if err:
        return jsonify({"error": err["error"], "delta": err["delta"],
                        "transaction_amount": _amt}), 400
    TransactionSplit.query.filter_by(transaction_id=tid).delete(synchronize_session=False)
    _now = datetime.utcnow()
    for row in clean:
        db.session.add(TransactionSplit(
            transaction_id=tid, budget_line_id=row["budget_line_id"],
            project_id=pid, amount=row["amount"], note=row["note"],
            created_at=_now, created_by_user_id=current_user.id))
    txn.budget_line_id    = None
    txn.account_code      = None
    txn.account_code_name = None
    txn.not_project_expense = False
    txn.excluded_at = txn.excluded_by_user_id = txn.excluded_reason = None
    txn.match_status = 'confirmed' if txn.doc_upload_id else txn.match_status
    txn.updated_at = _now
    _clear_ai_code_suggestion(txn)
    # A split is a claim (company ledger).
    try:
        claim_for(txn, pid, why='split across lines')
    except ClaimError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 409
    db.session.commit()
    _act = None
    try:
        _act = _log_activity(action='update', entity_type='transaction', entity_id=txn.id,
                             entity_label=_label, project_id=pid,
                             before=_before,
                             after={'budget_line_id': None, 'account_code': None,
                                    'account_code_name': None, 'not_project_expense': False,
                                    'splits': clean},
                      note=f'Split ${_amt:,.2f} across {len(clean)} lines: '
                           + ', '.join(f'{_ws_line_label(lines_by_id[r["budget_line_id"]])} ${r["amount"]:,.2f}'
                                       for r in clean)[:400])
    except Exception: pass
    out = [{"id": sp.id, "budget_line_id": sp.budget_line_id,
            "label": _ws_line_label(lines_by_id[sp.budget_line_id]),
            "amount": float(sp.amount or 0), "note": sp.note}
           for sp in TransactionSplit.query.filter_by(transaction_id=tid)
                                           .order_by(TransactionSplit.id).all()]
    return jsonify({"ok": True, "transaction": _txn_state_json(txn), "splits": out,
                    "activity_id": (_act.id if _act else None)})


@app.route("/projects/<int:pid>/actuals/manual-expense", methods=["POST"])
@login_required
def actuals_manual_expense(pid):
    """Hand-typed manual expense with no receipt and no QBO charge (Phase 2,
    2026-10-30 — the rare path). Body {vendor, amount, txn_date,
    budget_line_id?, payment_method (required), paid_by_crew_member_id?,
    note?, reason?}; reason is required when payment_method is cash
    (actuals.validate_manual_expense holds the rules). Creates
    source='manual_entry', activated_at=now, match_status='unmatched'
    (a receipt may still arrive), assigns the line through
    link_transaction_to_line when given, and returns the row. It lands in
    Needs backup until a receipt is linked or the reason stands in."""
    from actuals import validate_manual_expense, link_transaction_to_line
    ProjectSheet.query.get_or_404(pid)
    _require_project_role(pid, 'editor')
    clean, err = validate_manual_expense(request.get_json(silent=True) or {})
    if err:
        return jsonify({"error": err}), 400
    if clean["paid_by_crew_member_id"]:
        from models import CrewMember
        if CrewMember.query.get(clean["paid_by_crew_member_id"]) is None:
            return jsonify({"error": "paid_by_crew_member_id: no such crew member."}), 400
    if clean["budget_line_id"]:
        _b, _lines = _ws_working_budget_and_lines(pid)
        if clean["budget_line_id"] not in _lines:
            return jsonify({"error": "budget_line_id is not on this project's "
                                     "current Working budget."}), 400
    _now = datetime.utcnow()
    # Attribution (Documents Phase 3): an explicit payee_id in the body is
    # a person's pick ('manual'); else the vendor string's linked Payee
    # ('alias'); else nobody — a line picked below may name them.
    _body = request.get_json(silent=True) or {}
    _payee_id, _payee_src = None, None
    if _body.get("payee_id") not in (None, '', 0, '0'):
        from models import CrewMember as _CM
        try:
            _payee_id = int(_body.get("payee_id"))
        except (TypeError, ValueError):
            return jsonify({"error": "payee_id must be an integer."}), 400
        if _CM.query.get(_payee_id) is None:
            return jsonify({"error": "payee_id: no such payee."}), 400
        _payee_src = 'manual'
    else:
        from actuals import payee_for_vendor_string as _pfv
        _payee_id = _pfv(pid, clean["vendor"])
        _payee_src = 'alias' if _payee_id else None
    txn = Transaction(
        project_id=pid, source='manual_entry',
        vendor=clean["vendor"], amount=clean["amount"], is_expense=clean["is_expense"],
        txn_date=clean["txn_date"], note=clean["note"],
        payment_method=clean["payment_method"],
        paid_by_crew_member_id=clean["paid_by_crew_member_id"],
        no_charge_reason=clean["reason"],
        payee_id=_payee_id, payee_source=_payee_src,
        activated_at=_now, match_status='unmatched',
        created_via_user_id=current_user.id, created_at=_now, updated_at=_now)
    db.session.add(txn)
    db.session.commit()
    try:
        _log_activity(action='create', entity_type='transaction', entity_id=txn.id,
                      entity_label=(txn.vendor or f'Txn #{txn.id}')[:80], project_id=pid,
                      after={'vendor': txn.vendor, 'amount': float(txn.amount or 0),
                             'txn_date': txn.txn_date, 'payment_method': txn.payment_method,
                             'paid_by_crew_member_id': txn.paid_by_crew_member_id,
                             'no_charge_reason': txn.no_charge_reason},
                      note=f'Manual expense ${float(txn.amount or 0):,.2f} {txn.vendor} '
                           f'({txn.payment_method}' + (f' — {txn.no_charge_reason}' if txn.no_charge_reason else '') + ')')
    except Exception: pass
    if clean["budget_line_id"]:
        try:
            link_transaction_to_line(txn.id, clean["budget_line_id"], user_id=current_user.id)
            db.session.refresh(txn)
        except Exception as e:
            logging.exception(f"[manual-expense] link failed: {e}")
            return jsonify({"ok": True, "created": True, "transaction": _txn_state_json(txn),
                            "warning": f"Expense saved but the line could not be assigned: {e}"}), 201
    out = _txn_state_json(txn)
    out.update({"source": txn.source, "txn_date": txn.txn_date, "note": txn.note,
                "payment_method": txn.payment_method,
                "paid_by_crew_member_id": txn.paid_by_crew_member_id,
                "no_charge_reason": txn.no_charge_reason})
    return jsonify({"ok": True, "created": True, "transaction": out}), 201
