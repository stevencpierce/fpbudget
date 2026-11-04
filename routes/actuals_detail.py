"""The charge / receipt detail window (2026-11-04).

Owner, Nov 4: "I should be able to open these up and see what they're
linked to … chain of custody … remove it, interact with it, change it.
What if the amount is wrong?"

ONE JSON payload for ONE component (static/txn-detail.js), opened from every
row on Actuals (Unclaimed / Assigned / Needs backup / Receipts / Duplicates /
Not project) and from every transaction row in the budget page's Actual
drill-down:

  GET  /projects/<pid>/actuals/transaction/<tid>/detail.json
  GET  /projects/<pid>/actuals/doc/<did>/detail.json      (a receipt with no
                                                           transaction row)
  POST /projects/<pid>/actuals/transaction/<tid>/detach-doc
  POST /projects/<pid>/actuals/transaction/<tid>/fold-itemization

Everything the window changes goes through the routes that already exist
(set-line, set-coa, split, edit, link-doc, unmatch, upload-receipt,
activity undo); the two POSTs here are the only new mutations, both logged
with an undo payload. Module pattern: routes/__init__.py.
"""
import json
import logging
import re
from datetime import datetime

from flask import abort, jsonify, request
from flask_login import current_user, login_required

from app import (app, _log_activity, _require_project_role, _line_picker_groups,
                 _section_for_code, _section_name)
from budget_calc import FP_COA_SECTIONS, FP_COA_NAMES
from models import (ActivityLog, Budget, BudgetLine, CrewMember, DocUpload, ExpenseEvidence,
                    Transaction, TransactionSplit, User, db)
from actuals import (build_line_picker_options, claim_for, ClaimError,
                     fold_invoice_children_onto_charge, invoice_children_of, is_bank_sourced,
                     pool_txn, split_snapshot, txn_snapshot, counted_twice_for_project,
                     unmatch_receipt, PAYMENT_METHOD_LABELS, loanout_via, payee_display)


SOURCE_LABELS = {'qbo_sync': 'QuickBooks', 'reconciled': 'QuickBooks', 'csv_import': 'CSV',
                 'manual_entry': 'Manual', 'invoice_split': 'Invoice'}
UNDOABLE_TYPES = ('transaction', 'transaction_match')


def _source_label(t, doc=None):
    s = getattr(t, 'source', None) or 'manual_entry'
    if s == 'doc_upload':
        return 'Invoice' if (doc is not None and (doc.category or '') == 'invoice') else 'Receipt'
    return SOURCE_LABELS.get(s, 'Manual')


def _line_label(ln):
    if ln is None:
        return None
    return f"{ln.account_code} · {(ln.description or ln.account_name or 'untitled')[:60]}"


def _section_label(code):
    try:
        code = int(code)
    except (TypeError, ValueError):
        return None
    return f"{code} · {FP_COA_NAMES.get(code) or _section_name(code) or ''}".rstrip(' ·')


def _doc_json(d, kind=None):
    if d is None:
        return None
    name = d.filed_filename or d.original_filename or f"Doc #{d.id}"
    ct = (d.content_type or '').lower()
    line_items = []
    try:
        vd = d.veryfi_data if isinstance(d.veryfi_data, dict) else (json.loads(d.veryfi_data) if d.veryfi_data else {})
    except Exception:
        vd = {}
    for li in (vd.get("line_items") or []):
        if not isinstance(li, dict):
            continue
        amt = li.get("total") if li.get("total") is not None else li.get("price")
        try:
            amt = round(abs(float(amt)), 2) if amt is not None else None
        except (TypeError, ValueError):
            amt = None
        desc = (li.get("description") or li.get("text") or '').strip()[:200]
        if amt:
            line_items.append({"description": desc, "amount": amt})
    return {"doc_id": d.id, "name": name[:160], "vendor": d.vendor, "category": d.category,
            "amount": float(d.amount) if d.amount is not None else None,
            "date": d.doc_date.isoformat() if d.doc_date else None,
            "kind": kind, "status": d.status,
            "is_pdf": bool('pdf' in ct or name.lower().endswith('.pdf')),
            "is_image": bool(ct.startswith('image/')),
            "raw_url": f"/docs/upload/{d.id}/raw", "thumb_url": f"/docs/upload/{d.id}/thumb",
            "preview_url": f"/docs/upload/{d.id}/preview-link",
            "editor_url": f"/projects/{d.project_id}/docs/{d.id}/editor" if d.project_id else None,
            "line_items": line_items}


def _who(u):
    if u is None:
        return {"name": "System", "ai": False}
    return {"name": u.actor_name, "ai": bool(u.is_ai_agent)}


def _history(pid, tid):
    """The row's activity entries, newest first, and the latest one the
    viewer may undo (activity_undo's own rules: supported type, not undone,
    not a restore, no newer entry on the same row, own entry unless admin)."""
    rows = (ActivityLog.query
            .filter(ActivityLog.project_id == pid,
                    ActivityLog.entity_type.in_(UNDOABLE_TYPES),
                    ActivityLog.entity_id == tid)
            .order_by(ActivityLog.id.desc()).limit(40).all())
    users = {u.id: u for u in User.query.filter(User.id.in_({r.user_id for r in rows if r.user_id})).all()} if rows else {}
    out, undo_id = [], None
    admin = bool(getattr(current_user, 'has_all_project_access', False))
    newest_live_by_type = {}
    for r in rows:   # newest first
        if r.undone_at is None and r.entity_type not in newest_live_by_type:
            newest_live_by_type[r.entity_type] = r.id
    for r in rows:
        w = _who(users.get(r.user_id))
        undoable = (r.undone_at is None and r.action != 'restore'
                    and newest_live_by_type.get(r.entity_type) == r.id
                    and (r.before_json or r.entity_type == 'transaction_match')
                    and (admin or r.user_id == current_user.id))
        if undoable and undo_id is None:
            undo_id = r.id
        out.append({"id": r.id, "at": r.created_at.isoformat(timespec='minutes') if r.created_at else None,
                    "who": w["name"], "by_ai_agent": w["ai"], "action": r.action,
                    "kind": r.entity_type, "note": r.note or r.action,
                    "undone": r.undone_at is not None})
    for h in out:
        h["undoable"] = (h["id"] == undo_id)
    return out, undo_id


def _undo_budget_id(pid):
    """activity_undo is budget-scoped in its URL; any budget of the project
    works (transaction entries carry project_id only)."""
    b = (Budget.query.filter(Budget.project_id == pid)
         .order_by(Budget.id.desc()).first())
    return b.id if b else None


def _picker(pid):
    _b, groups = _line_picker_groups(pid)
    return build_line_picker_options(groups), (_b.id if _b else None)


def _target_json(pid, line_id=None, code=None):
    if line_id:
        ln = BudgetLine.query.get(line_id)
        sec = None
        try:
            sec = _section_for_code(int(ln.account_code)) if ln and ln.account_code is not None else None
        except (TypeError, ValueError):
            sec = None
        return {"kind": "line", "line_id": line_id, "label": _line_label(ln) or f"line #{line_id}",
                "section": sec, "section_label": _section_label(sec) if sec else None,
                "picker_value": str(line_id)}
    if code:
        return {"kind": "section", "code": int(code), "label": _section_label(code),
                "picker_value": f"section:{int(code)}"}
    return None


def _qbo_payee_name(t):
    """The payee QuickBooks recorded: the mirrored vendor behind the
    EntityRef, else the row's vendor text (a receipt may have renamed a
    reconciled row — 2026-11-04)."""
    if getattr(t, 'qbo_entity_ref', None):
        try:
            from models import QBOVendor
            v = QBOVendor.query.get(str(t.qbo_entity_ref))
            if v is not None and v.display_name:
                return v.display_name
        except Exception:
            pass
    return t.vendor


def _detail_for_txn(pid, t):
    docs_seen = set()
    primary = DocUpload.query.get(t.doc_upload_id) if t.doc_upload_id else None
    backup = []
    if primary is not None:
        kind = 'primary' if t.match_status == 'confirmed' or t.source == 'doc_upload' else 'suggested'
        backup.append(_doc_json(primary, kind))
        docs_seen.add(primary.id)
    for ev in ExpenseEvidence.query.filter_by(transaction_id=t.id).all():
        if ev.doc_upload_id in docs_seen:
            continue
        d = DocUpload.query.get(ev.doc_upload_id)
        if d is not None and (d.status or '') != 'deleted':
            backup.append(_doc_json(d, ev.kind or 'backup'))
            docs_seen.add(d.id)
    for shadow in Transaction.query.filter(Transaction.backup_of_txn_id == t.id).all():
        if shadow.doc_upload_id and shadow.doc_upload_id not in docs_seen:
            d = DocUpload.query.get(shadow.doc_upload_id)
            if d is not None:
                backup.append(_doc_json(d, 'backup'))
                docs_seen.add(d.id)
    confirmed_backup = [b for b in backup if b["kind"] != 'suggested']
    has_backup = bool(confirmed_backup or t.no_charge_reason)

    # Coding chain: Charge → line / section (each split piece its own row)
    # → the document behind it.
    sps = TransactionSplit.query.filter_by(transaction_id=t.id).order_by(TransactionSplit.id).all()
    kids = invoice_children_of(t) if t.source != 'invoice_split' else []
    back_docs = [b["doc_id"] for b in confirmed_backup]
    chain, mode = [], 'none'
    if sps:
        mode = 'split'
        for sp in sps:
            chain.append({**(_target_json(pid, sp.budget_line_id, None if sp.budget_line_id else sp.account_code) or {}),
                          "split_id": sp.id, "amount": float(sp.amount or 0), "note": sp.note,
                          "docs": back_docs})
    elif kids:
        mode = 'itemized'
        for k in kids:
            tj = _target_json(pid, k.budget_line_id, None if k.budget_line_id else k.account_code) or {"kind": "none", "label": "No line"}
            chain.append({**tj, "child_id": k.id, "amount": float(k.amount or 0), "note": k.note,
                          "docs": back_docs})
    elif t.budget_line_id or t.account_code:
        mode = 'line' if t.budget_line_id else 'section'
        chain.append({**_target_json(pid, t.budget_line_id, None if t.budget_line_id else t.account_code),
                      "amount": float(t.amount or 0), "docs": back_docs})
    # A split parent must be uncoded — surface the bug if it is not.
    parent_still_coded = bool(sps and (t.budget_line_id or t.account_code))

    # "Split this charge by the invoice": an attached document with line
    # items (Veryfi) or an itemization on the document's own row.
    split_offer = None
    if primary is not None and is_bank_sourced(t) and t.match_status == 'confirmed' and not sps:
        dj = _doc_json(primary)
        items = list(dj["line_items"])
        src = 'ocr'
        if kids:
            items = [{"description": k.note or '', "amount": float(k.amount or 0),
                      "picker_value": (str(k.budget_line_id) if k.budget_line_id
                                       else (f"section:{k.account_code}" if k.account_code else None))}
                     for k in kids]
            src = 'itemization'
        if len(items) >= 2:
            split_offer = {"doc_id": primary.id, "doc_name": dj["name"], "items": items, "source": src}

    payee = None
    if getattr(t, 'payee_id', None):
        cm = CrewMember.query.get(t.payee_id)
        if cm is not None:
            # Loan-out (2026-11-04): "Danielle Miller · via Two Foot Shuffle LLC".
            via = loanout_via(cm.id, (t.vendor, t.qbo_entity_ref),
                              primary.vendor if primary is not None else None)
            payee = {"id": cm.id, "name": cm.name, "source": t.payee_source,
                     "via": via, "label": payee_display(cm.name, via),
                     "url": f"/payees/{cm.id}"}
    # "Receipt payee differs from QuickBooks" (company ledger) — with the
    # "Same person — X is Y's loan-out" offer (ledger.payee_differs_info).
    payee_differs = None
    try:
        import ledger as _ledger
        payee_differs = _ledger.payee_differs_info(t)
        if payee_differs is not None:
            payee_differs["can_resolve"] = bool(getattr(current_user, 'has_all_project_access', False))
    except Exception as _pde:
        logging.warning(f"[detail] payee-differs info failed: {_pde}")
    paid_by = None
    if getattr(t, 'paid_by_crew_member_id', None):
        cm = CrewMember.query.get(t.paid_by_crew_member_id)
        paid_by = cm.name if cm else None

    bank = is_bank_sourced(t)
    if bank:
        amount_edit = {"allowed": False,
                       "reason": "This is the bank's figure — change the receipt or split instead."}
    else:
        amount_edit = {"allowed": not sps, "reason": (None if not sps else
                       "This row is split across lines — remove the split, then change the amount.")}

    twice = None
    try:
        for e in counted_twice_for_project(pid):
            r = e.get("doc_row")
            if e["charge"].id == t.id or (r is not None and r.id == t.id):
                d = DocUpload.query.get(r.doc_upload_id) if (r is not None and r.doc_upload_id) else None
                twice = {"kind": e["kind"], "charge_id": e["charge"].id,
                         "doc_row_id": (r.id if r is not None else None),
                         "doc_id": (r.doc_upload_id if r is not None else None),
                         "doc_name": ((d.filed_filename or d.original_filename) if d else None),
                         "charge_vendor": e["charge"].vendor,
                         "charge_amount": float(e["charge"].amount or 0),
                         "charge_date": (e["charge"].txn_date or '')[:10]}
                break
    except Exception as _cte:
        logging.warning(f"[detail] counted-twice check failed: {_cte}")

    history, undo_id = _history(pid, t.id)
    picker, picker_bid = _picker(pid)
    excluded = None
    if t.not_project_expense or t.excluded_at:
        excluded = {"reason": t.excluded_reason, "at": t.excluded_at.isoformat(timespec='minutes') if t.excluded_at else None}
    signed = float(t.amount or 0) * (1 if t.is_expense else -1)
    return {
        "ok": True, "kind": "transaction", "pid": pid,
        "txn": {"id": t.id, "source": t.source, "source_label": _source_label(t, primary),
                "amount": signed, "raw_amount": float(t.amount or 0), "is_expense": bool(t.is_expense),
                "date": (t.txn_date or '')[:10] or None, "vendor": t.vendor,
                "memo": t.note, "card_last4": t.card_last4,
                "account": t.qbo_account_id, "payment_method": t.payment_method,
                "payment_method_label": PAYMENT_METHOD_LABELS.get(t.payment_method or '', None),
                "paid_by": paid_by,
                "qbo": ({"id": t.qbo_txn_id, "type": t.qbo_txn_type, "payee": _qbo_payee_name(t),
                         "category": t.qbo_category, "entity_ref": t.qbo_entity_ref}
                        if t.qbo_txn_id else None),
                "payee": payee, "match_status": t.match_status,
                "no_charge_reason": t.no_charge_reason,
                "pooled": t.project_id is None, "claim_conflict": bool(t.claim_conflict),
                "excluded": excluded,
                "activated": bool(t.activated_at) or t.source != 'doc_upload'},
        "amount_edit": amount_edit,
        "backup": backup, "has_backup": has_backup,
        "needs_backup": bool(chain) and not has_backup,
        "coding": {"mode": mode, "chain": chain, "parent_still_coded": parent_still_coded,
                   "splits": [{"budget_line_id": sp.budget_line_id, "account_code": sp.account_code,
                               "picker_value": (str(sp.budget_line_id) if sp.budget_line_id else f"section:{sp.account_code}"),
                               "label": ((_line_label(BudgetLine.query.get(sp.budget_line_id)) if sp.budget_line_id
                                          else _section_label(sp.account_code)) or ''),
                               "amount": float(sp.amount or 0), "note": sp.note} for sp in sps]},
        "split_offer": split_offer,
        "counted_twice": twice,
        "payee_differs": payee_differs,
        "history": history, "undo_activity_id": undo_id, "undo_bid": _undo_budget_id(pid),
        "picker": picker, "picker_bid": picker_bid,
        "sections": [{"code": c, "name": n} for c, n in FP_COA_SECTIONS],
        "can_edit": True,
    }


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/detail.json", methods=["GET"])
@login_required
def actuals_transaction_detail(pid, tid):
    """Everything about one row for the detail window: header facts, backup
    (documents with preview), the coding chain (each link a row; split
    pieces each their own), the amount rule, the split-by-invoice offer, the
    counted-twice warning and the row's history with its latest Undo."""
    _require_project_role(pid, 'viewer')
    t = pool_txn(pid, tid, include_dismissed=True)
    if t is None:
        # A receipt row / an itemized child is project-owned, not a pool row.
        t = Transaction.query.filter_by(id=tid, project_id=pid).first()
    if t is None:
        abort(404)
    if t.source == 'invoice_split' and t.parent_transaction_id:
        parent = Transaction.query.get(t.parent_transaction_id)
        if parent is not None:
            t = parent
    return jsonify(_detail_for_txn(pid, t))


@app.route("/projects/<int:pid>/actuals/doc/<int:did>/detail.json", methods=["GET"])
@login_required
def actuals_doc_detail(pid, did):
    """The detail window for a RECEIPT row. When the document has its own
    ledger row (or rides on a charge) this is that row's detail; a receipt
    with no transaction at all gets a document-only payload."""
    _require_project_role(pid, 'viewer')
    d = DocUpload.query.filter_by(id=did, project_id=pid).first_or_404()
    t = (Transaction.query.filter(Transaction.doc_upload_id == did,
                                  Transaction.source.in_(('qbo_sync', 'reconciled', 'csv_import')),
                                  Transaction.match_status == 'confirmed').first()
         or Transaction.query.filter(Transaction.doc_upload_id == did,
                                     Transaction.source == 'doc_upload',
                                     Transaction.project_id == pid).first())
    if t is not None:
        out = _detail_for_txn(pid, t)
        out["opened_from_doc"] = did
        return jsonify(out)
    picker, picker_bid = _picker(pid)
    return jsonify({"ok": True, "kind": "doc", "pid": pid, "doc": _doc_json(d, 'primary'),
                    "backup": [_doc_json(d, 'primary')], "coding": {"mode": "none", "chain": []},
                    "history": [], "undo_activity_id": None, "undo_bid": _undo_budget_id(pid),
                    "picker": picker, "picker_bid": picker_bid,
                    "sections": [{"code": c, "name": n} for c, n in FP_COA_SECTIONS]})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/detach-doc", methods=["POST"])
@login_required
def actuals_detach_doc(pid, tid):
    """Detach ONE document from a row (the detail window's Detach). The
    charge's own receipt goes back to Receipts to place as a document-only
    row (unmatch); a backup document's evidence link is removed (and its
    absorbed row restored). The charge keeps its coding and shows Needs
    backup when nothing else backs it. Body {doc_id}. Logged; undoable."""
    _require_project_role(pid, 'editor')
    t = pool_txn(pid, tid, include_dismissed=True) or Transaction.query.filter_by(id=tid, project_id=pid).first()
    if t is None:
        abort(404)
    body = request.get_json(silent=True) or {}
    try:
        doc_id = int(body.get('doc_id'))
    except (TypeError, ValueError):
        return jsonify({"error": "doc_id required"}), 400
    label = (t.vendor or f'Txn #{t.id}')[:80]
    if t.doc_upload_id == doc_id and t.source != 'doc_upload':
        ExpenseEvidence.query.filter_by(transaction_id=t.id, doc_upload_id=doc_id).delete(
            synchronize_session=False)
        db.session.commit()
        info = unmatch_receipt(t.id)
        act = None
        try:
            act = _log_activity(action='update', entity_type='transaction_match', entity_id=t.id,
                                entity_label=label, project_id=pid,
                                after={'doc_upload_id': None, 'match_status': 'unmatched'},
                                note=f'Detached the receipt from {label} — back in Receipts to place')
        except Exception:
            pass
        return jsonify({"ok": True, "mode": "unmatched", "activity_id": (act.id if act else None), **info})
    ev = ExpenseEvidence.query.filter_by(transaction_id=t.id, doc_upload_id=doc_id).all()
    shadow = Transaction.query.filter(Transaction.backup_of_txn_id == t.id,
                                      Transaction.doc_upload_id == doc_id).first()
    if not ev and shadow is None:
        return jsonify({"error": "That document is not attached to this row."}), 404
    before = {'evidence': [{'doc_upload_id': e.doc_upload_id, 'kind': e.kind}
                           for e in ExpenseEvidence.query.filter_by(transaction_id=t.id).all()]}
    for e in ev:
        db.session.delete(e)
    if shadow is not None:
        shadow.backup_of_txn_id = None
        shadow.not_project_expense = False
        shadow.match_status = 'unmatched'
        shadow.note = (re.sub(r'^\U0001F4CE Backup.*?\.(\s+|$)', '', shadow.note or '').strip() or None)
        shadow.updated_at = datetime.utcnow()
    db.session.commit()
    act = None
    try:
        act = _log_activity(action='update', entity_type='transaction', entity_id=t.id,
                            entity_label=label, project_id=pid, before=before,
                            after={'evidence': [b for b in before['evidence'] if b['doc_upload_id'] != doc_id]},
                            note=f'Detached a backup document from {label}')
    except Exception:
        pass
    return jsonify({"ok": True, "mode": "evidence", "activity_id": (act.id if act else None)})


@app.route("/projects/<int:pid>/actuals/transaction/<int:tid>/fold-itemization", methods=["POST"])
@login_required
def actuals_fold_itemization(pid, tid):
    """Turn a charge's legacy itemization (invoice_split children) into its
    transaction_split — the ONE mechanism — and delete the children. Body
    {keep: 'charge' | 'invoice'} when the charge is already split (default:
    keep the charge's split). Logged with the children's snapshots; Undo
    puts them back."""
    _require_project_role(pid, 'editor')
    t = pool_txn(pid, tid) or abort(404)
    kids = invoice_children_of(t, t.doc_upload_id)
    if not kids:
        return jsonify({"error": "This charge has no itemized invoice lines to fold."}), 400
    body = request.get_json(silent=True) or {}
    keep = (body.get('keep') or 'charge').lower()
    before = {'budget_line_id': t.budget_line_id, 'account_code': t.account_code,
              'account_code_name': t.account_code_name, 'splits': split_snapshot(t.id),
              'recreate_rows': [{**txn_snapshot(k), 'id': k.id} for k in kids],
              'recreate_evidence': [{'transaction_id': e.transaction_id, 'doc_upload_id': e.doc_upload_id,
                                     'kind': e.kind}
                                    for e in ExpenseEvidence.query.filter(
                                        ExpenseEvidence.transaction_id.in_([k.id for k in kids])).all()]}
    info = fold_invoice_children_onto_charge(t, kids, pid, current_user.id,
                                             keep_charge_split=(keep != 'invoice'))
    if info.get('mode') == 'refused':
        db.session.rollback()
        return jsonify({"error": info.get('error')}), 400
    try:
        claim_for(t, pid, why='itemization folded into the split')
    except ClaimError as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 409
    db.session.commit()
    act = None
    try:
        act = _log_activity(action='update', entity_type='transaction', entity_id=t.id,
                            entity_label=(t.vendor or f'Txn #{t.id}')[:80], project_id=pid,
                            before=before, after={'splits': split_snapshot(t.id)},
                            note=f'Folded {len(kids)} itemized invoice lines into the split ({info["mode"]})')
    except Exception:
        pass
    return jsonify({"ok": True, "mode": info["mode"], "activity_id": (act.id if act else None)})


# ── Maintenance: invoices counted twice / split charges still coded ───────
# Admin → Maintenance cards (routes/admin_maintenance.py TOOLS). The preview
# never writes; every Fix is one click per row, logged with the rows it
# removes so Undo (the activity log, or the toast) puts them back.
from flask import render_template                       # noqa: E402
from app import maintenance_required                     # noqa: E402
from models import ProjectSheet                          # noqa: E402
from actuals import (coding_contribution, target_actuals, invoice_children_to_split_rows,  # noqa: E402
                     split_parents_still_coded, confirm_match, _child_dicts, _txn_target)


def _tgt_label(k):
    kind, val = k
    if kind == 'line':
        return _line_label(BudgetLine.query.get(val)) or f"line #{val}"
    return (_section_label(val) or str(val)) + ' · section'


def _counted_twice_entry(pid, e):
    """One preview card: what counts today, what the Fix leaves, and every
    touched line's total before / after."""
    c, r = e["charge"], e.get("doc_row")
    charge_c = coding_contribution(c)
    if e["kind"] == 'split_and_itemized':
        kids = invoice_children_of(c, c.doc_upload_id)
        removed = {}
        for k in kids:
            tg = _txn_target(k)
            if tg:
                removed[tg] = round(removed.get(tg, 0.0) + float(k.amount or 0) * (1 if c.is_expense else -1), 2)
        doc_c, after_c = removed, charge_c
        fix = "Keep the charge's split; remove the invoice's duplicate itemization"
        doc = DocUpload.query.get(c.doc_upload_id) if c.doc_upload_id else None
    else:
        doc_c = coding_contribution(r)
        kids = invoice_children_of(r, r.doc_upload_id)
        doc = DocUpload.query.get(r.doc_upload_id) if r.doc_upload_id else None
        has_split = TransactionSplit.query.filter_by(transaction_id=c.id).first() is not None
        if kids and not has_split:
            rows, err = invoice_children_to_split_rows(c.amount, _child_dicts(kids), _txn_target(c))
            after_c = {}
            for row in rows or []:
                tg = ('line', row["budget_line_id"]) if row["budget_line_id"] else ('section', row["account_code"])
                after_c[tg] = round(after_c.get(tg, 0.0) + row["amount"] * (1 if c.is_expense else -1), 2)
            fix = ("Attach the invoice to the charge; its lines become the charge's split"
                   if not err else f"Cannot convert: {err}")
        else:
            after_c = charge_c
            fix = ("Attach the invoice to the charge; the charge keeps its "
                   + ("split" if has_split else "coding") + " and the invoice's own row stops counting")
    targets = sorted(set(charge_c) | set(doc_c) | set(after_c), key=lambda k: (k[0], k[1] or 0))
    now = target_actuals(pid, targets)
    lines = []
    for k in targets:
        b = now.get(k, 0.0)
        a = round(b - charge_c.get(k, 0.0) - doc_c.get(k, 0.0) + after_c.get(k, 0.0), 2)
        lines.append({"label": _tgt_label(k), "before": round(b, 2), "after": a})
    return {"kind": e["kind"], "project_id": pid, "undo_bid": _undo_budget_id(pid),
            "charge": {"id": c.id, "vendor": c.vendor, "amount": float(c.amount or 0),
                       "date": (c.txn_date or '')[:10], "source": c.source},
            "doc_row": ({"id": r.id, "vendor": r.vendor, "amount": float(r.amount or 0),
                         "date": (r.txn_date or '')[:10], "itemized": len(kids)} if r is not None else None),
            "doc": ({"id": doc.id, "name": doc.filed_filename or doc.original_filename} if doc else None),
            "itemized_rows": len(kids), "fix": fix, "lines": lines,
            "counted_now": round(sum(charge_c.values()) + sum(doc_c.values()), 2),
            "counted_after": round(sum(after_c.values()), 2)}


def _counted_twice_preview(project_id=None):
    pids = ([project_id] if project_id else
            [p.id for p in ProjectSheet.query.filter(ProjectSheet.status != 'archived').all()])
    out = []
    for pid in pids:
        try:
            for e in counted_twice_for_project(pid):
                out.append(_counted_twice_entry(pid, e))
        except Exception as _e:
            logging.warning(f"[counted-twice] project {pid} skipped: {_e}")
            db.session.rollback()
    return out


@app.route("/admin/actuals/counted-twice", methods=["GET"])
@login_required
@maintenance_required
def admin_counted_twice():
    """Find invoices counted twice — preview (HTML, or JSON with
    ?format=json). (a) a charge split AND itemized by its invoice; (b) a
    coded / itemized invoice row with no charge + a counted charge with the
    same payee and amount (±$0.01) within 30 days in the same project."""
    try:
        pid = int(request.args.get('project') or request.args.get('pid') or 0) or None
    except (TypeError, ValueError):
        pid = None
    rows = _counted_twice_preview(pid)
    if (request.args.get('format') or '') == 'json' or 'application/json' in (request.headers.get('Accept') or ''):
        return jsonify({"ok": True, "count": len(rows), "rows": rows})
    names = {p.id: p.name for p in ProjectSheet.query.all()}
    return render_template("admin_counted_twice.html", rows=rows, names=names, pid=pid)


@app.route("/admin/actuals/counted-twice/fix", methods=["POST"])
@login_required
@maintenance_required
def admin_counted_twice_fix():
    """Fix ONE counted-twice row (click-to-confirm on the preview page).
    Body {charge_id, doc_row_id?}. (b): attach the invoice to the charge —
    the receipt confirm absorbs the invoice's own row and folds its
    itemization into the charge's split (actuals.confirm_match). (a): keep
    the charge's split, fold the duplicate itemization away. Logged with
    every removed row; Undo restores them."""
    from routes.actuals import _link_undo_before
    body = request.get_json(silent=True) or {}
    try:
        cid = int(body.get('charge_id'))
        rid = int(body['doc_row_id']) if body.get('doc_row_id') else None
    except (TypeError, ValueError):
        return jsonify({"error": "charge_id required"}), 400
    c = Transaction.query.get_or_404(cid)
    pid = c.project_id
    if not pid:
        return jsonify({"error": "That charge is in the unclaimed pool — nothing counts twice."}), 400
    live = [e for e in counted_twice_for_project(pid) if e["charge"].id == cid
            and (rid is None or (e.get("doc_row") is not None and e["doc_row"].id == rid))]
    if not live:
        return jsonify({"error": "This row is no longer counted twice (refresh the preview)."}), 409
    e = live[0]
    label = (c.vendor or f'Txn #{c.id}')[:80]
    if e["kind"] == 'doc_and_charge':
        r = e["doc_row"]
        before = _link_undo_before(c, r.doc_upload_id)
        c.doc_upload_id = r.doc_upload_id
        c.match_status = 'confirmed'
        c.updated_at = datetime.utcnow()
        db.session.commit()
        try:
            res = confirm_match(c.id)
        except Exception as ex:
            db.session.rollback()
            return jsonify({"error": f"Could not attach: {ex}"}), 400
        act = _log_activity(action='update', entity_type='transaction_match', entity_id=c.id,
                            entity_label=label, project_id=pid, before=before,
                            after={'doc_upload_id': r.doc_upload_id, 'match_status': 'confirmed'},
                            note=(f'Counted twice → attached the invoice to {label} ${float(c.amount or 0):,.2f}'
                                  + (f' ({(res.get("itemization") or {}).get("mode")})' if res.get("itemization") else ''))[:500])
        return jsonify({"ok": True, "mode": "attached", "itemization": res.get("itemization"),
                        "activity_id": (act.id if act else None)})
    kids = invoice_children_of(c, c.doc_upload_id)
    before = {'budget_line_id': c.budget_line_id, 'account_code': c.account_code,
              'account_code_name': c.account_code_name, 'splits': split_snapshot(c.id),
              'recreate_rows': [{**txn_snapshot(k), 'id': k.id} for k in kids],
              'recreate_evidence': [{'transaction_id': x.transaction_id, 'doc_upload_id': x.doc_upload_id,
                                     'kind': x.kind}
                                    for x in ExpenseEvidence.query.filter(
                                        ExpenseEvidence.transaction_id.in_([k.id for k in kids])).all()]}
    info = fold_invoice_children_onto_charge(c, kids, pid, current_user.id, keep_charge_split=True)
    c.budget_line_id = None
    c.account_code = None
    c.account_code_name = None
    db.session.commit()
    act = _log_activity(action='update', entity_type='transaction', entity_id=c.id,
                        entity_label=label, project_id=pid, before=before,
                        after={'splits': split_snapshot(c.id)},
                        note=f'Counted twice → kept the split on {label}, removed {len(kids)} duplicate itemized rows')
    return jsonify({"ok": True, "mode": info.get("mode"), "activity_id": (act.id if act else None)})


@app.route("/admin/actuals/split-still-coded", methods=["GET", "POST"])
@login_required
@maintenance_required
def admin_split_still_coded():
    """Split charges still coded — a split parent must carry no line and no
    section of its own (its pieces carry the coding). GET lists them; POST
    with apply=1 uncodes each parent (logged per row, undoable)."""
    try:
        pid = int(request.args.get('project') or 0) or None
    except (TypeError, ValueError):
        pid = None
    rows = split_parents_still_coded(pid)
    listing = [{"id": t.id, "project_id": t.project_id, "vendor": t.vendor,
                "amount": float(t.amount or 0), "date": (t.txn_date or '')[:10],
                "line": _line_label(BudgetLine.query.get(t.budget_line_id)) if t.budget_line_id else None,
                "section": t.account_code,
                "pieces": [{"label": (_line_label(BudgetLine.query.get(sp.budget_line_id)) if sp.budget_line_id
                                      else _section_label(sp.account_code)), "amount": float(sp.amount or 0)}
                           for sp in TransactionSplit.query.filter_by(transaction_id=t.id).all()]}
               for t in rows]
    if request.method == 'GET' or str(request.args.get('apply') or '') != '1':
        return jsonify({"ok": True, "dry_run": True, "count": len(listing), "rows": listing,
                        "fix": "Run uncodes each parent: its split pieces keep the coding."})
    fixed = []
    for t in rows:
        before = {'budget_line_id': t.budget_line_id, 'account_code': t.account_code,
                  'account_code_name': t.account_code_name}
        t.budget_line_id = None
        t.account_code = None
        t.account_code_name = None
        t.updated_at = datetime.utcnow()
        db.session.commit()
        act = _log_activity(action='update', entity_type='transaction', entity_id=t.id,
                            entity_label=(t.vendor or f'Txn #{t.id}')[:80], project_id=t.project_id,
                            before=before, after={'budget_line_id': None, 'account_code': None,
                                                  'account_code_name': None},
                            note='Split charge was still coded — uncoded the parent (its pieces carry the coding)')
        fixed.append({"id": t.id, "activity_id": (act.id if act else None)})
    return jsonify({"ok": True, "dry_run": False, "fixed": len(fixed), "rows": fixed})
