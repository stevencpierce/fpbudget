"""Super-admin transaction-pool routes — M1b-2 slice of the app.py split
(ACTUALS_HANDOFF Phase 2, 2026-10-30).

The coding backup JSON, the QBO import repair / purge tools and the
estimate-txn cleanup page. Same pattern as routes/actuals.py: @app.route
on the same app object, endpoint names and URLs unchanged.
/admin/transactions is the COMPANY LEDGER (2026-11-03): every QuickBooks
transaction once, with who claimed it. The cross-project claim backfill
and the per-project QBO coverage matrix were retired with the company
ledger (one row per transaction — there are no siblings to claim and no
per-project account sets).
"""
import logging
from datetime import (datetime)
from flask import (jsonify, redirect, render_template, request, url_for)
from flask_login import (current_user, login_required)

from app import (app, _NON_LEDGER_DOC_CATEGORIES, _delete_evidence_for_txns, _log_activity, maintenance_required)
from models import (DocUpload, ProjectSheet, Transaction, TransactionSplit, db)


@app.route("/admin/backup/transactions.json", methods=["GET"])
@login_required
@maintenance_required
def admin_backup_transactions():
    """READ-ONLY archive: every transaction's coding-relevant fields as JSON.
    Pulled before/after risky fixes so coding can be diffed and restored
    independent of Render PITR. (Code review remediation, 2026-06-04.)"""
    rows = (Transaction.query
            .with_entities(Transaction.id, Transaction.project_id,
                           Transaction.source, Transaction.vendor,
                           Transaction.amount, Transaction.txn_date,
                           Transaction.is_expense, Transaction.account_code,
                           Transaction.account_code_name,
                           Transaction.budget_line_id, Transaction.match_status,
                           Transaction.doc_upload_id,
                           Transaction.not_project_expense,
                           Transaction.qbo_txn_id, Transaction.qbo_txn_type)
            .order_by(Transaction.id).all())
    out = [{"id": r.id, "project_id": r.project_id, "source": r.source,
            "vendor": r.vendor,
            "amount": float(r.amount) if r.amount is not None else None,
            "txn_date": r.txn_date, "is_expense": bool(r.is_expense),
            "account_code": r.account_code,
            "account_code_name": r.account_code_name,
            "budget_line_id": r.budget_line_id, "match_status": r.match_status,
            "doc_upload_id": r.doc_upload_id,
            "not_project_expense": bool(r.not_project_expense),
            "qbo_txn_id": r.qbo_txn_id, "qbo_txn_type": r.qbo_txn_type}
           for r in rows]
    return jsonify({"generated_at": datetime.utcnow().isoformat() + 'Z',
                    "count": len(out), "transactions": out})


@app.route("/admin/qbo-imports/repair-refunds", methods=["POST"])
@login_required
def qbo_repair_refunds():
    """Repair pre-2026-06-04 QBO rows where refunds imported as POSITIVE
    expenses (CC refunds / Purchase-with-Credit) and BillPayments imported as
    credits. Re-queries QBO over a date window and corrects is_expense on
    existing local rows (only that boolean is touched). Reports QBO refunds with
    no local row too. DRY-RUN by default; ?apply=1 to write. Admin only.
    Body/params: project=<pid> (required), start_date, end_date (YYYY-MM-DD;
    default = last 365 days → today). (Code review 2026-06-04.)"""
    if not getattr(current_user, 'can_open_admin', False):
        return jsonify({"error": "Forbidden — admin only"}), 403
    from models import QBOConnection
    conn = QBOConnection.query.first()
    if not conn or not conn.refresh_token:
        return jsonify({"error": "QuickBooks not connected. Go to Admin → QBO to authorize.",
                        "needs_oauth": True}), 400
    body = request.get_json(silent=True) or {}
    apply = (request.args.get('apply') == '1') or bool(body.get('apply'))
    pid_raw = request.args.get('project') or body.get('project')
    try:
        pid = int(pid_raw)
    except (TypeError, ValueError):
        return jsonify({"error": "project (pid) required"}), 400
    project = ProjectSheet.query.get_or_404(pid)
    import re as _re, datetime as _dt
    _date_re = _re.compile(r"^\d{4}-\d{2}-\d{2}$")
    start_date = (body.get('start_date') or request.args.get('start_date') or '').strip() \
        or (_dt.date.today() - _dt.timedelta(days=365)).isoformat()
    end_date = (body.get('end_date') or request.args.get('end_date') or '').strip() \
        or _dt.date.today().isoformat()
    if not _date_re.match(start_date) or not _date_re.match(end_date):
        return jsonify({"error": "start_date/end_date must be YYYY-MM-DD"}), 400
    # Which corrections to apply: credit_only (safe — removes wrongly-counted
    # spend) | expense_only (ADDS spend; double-count risk vs CSV) | both.
    direction = (body.get('direction') or request.args.get('direction') or 'both').lower()
    if direction not in ('both', 'credit_only', 'expense_only'):
        return jsonify({"error": "direction must be both|credit_only|expense_only"}), 400
    from qbo_sync import repair_is_expense
    try:
        result = repair_is_expense(project, conn, db, start_date, end_date,
                                   apply=apply, direction=direction)
    except Exception as e:
        logging.exception("qbo repair-refunds failed")
        return jsonify({"error": "Repair failed", "detail": type(e).__name__}), 500
    if apply and result.get('fixed'):
        try:
            _log_activity(action='update', entity_type='transaction', entity_id=0,
                          entity_label=f"QBO refund repair ({result['fixed']} fixed)",
                          project_id=pid, before=None, after={'fixed': result['fixed']},
                          note=(f"Repaired is_expense on {result['fixed']} QBO rows "
                                f"({result['flip_to_credit_count']} refunds→credit, "
                                f"{result['flip_to_expense_count']} bill payments→expense)"))
        except Exception:
            pass
    return jsonify(result)


@app.route("/admin/qbo-imports/purge", methods=["POST"])
@login_required
def qbo_imports_purge(pid=None):
    """Undo electronic (QuickBooks) imports. Since the company ledger
    (2026-11-03) a QuickBooks transaction has ONE row for the whole
    company, so "purge for a project" means: RELEASE that project's
    source='qbo_sync' rows back to the pool (coding, exclusion and splits
    cleared; the rows stay — they are the company's record), while
    project=pool DELETES the unclaimed pool rows so the next sync starts
    clean. Receipts (doc_upload), typed rows (manual_entry), reconciled
    rows and rows carrying a receipt are NEVER touched. Dry-run by
    default; ?apply=1 to write. Super-admin only. (User 2026-06-01;
    Cleanup A #8, 2026-11-02; company ledger 2026-11-03.)"""
    if not getattr(current_user, 'can_run_maintenance', False):
        return jsonify({"error": "Forbidden — super admin only"}), 403
    apply = (request.args.get('apply') == '1')
    scope = request.args.get('project')
    # project=pool permanently DELETES the unclaimed pool rows — owner only.
    # The per-project scope only releases rows (they stay), so the AI Agent
    # may run that one.
    if scope == 'pool' and not getattr(current_user, 'is_super_admin', False):
        return jsonify({"error": "Forbidden — purging the pool is super admin only"}), 403
    if scope in (None, '', 'all'):
        return jsonify({"error": "project=<id> (release that project's imports to the pool) "
                                 "or project=pool (delete the unclaimed pool rows) is required."}), 400
    scope_pid = None
    if scope != 'pool':
        try:
            scope_pid = int(scope)
        except (TypeError, ValueError):
            return jsonify({"error": "project must be an integer or 'pool'"}), 400
        if ProjectSheet.query.get(scope_pid) is None:
            return jsonify({"error": f"project {scope_pid} not found"}), 404

    q = (Transaction.query
         .filter(Transaction.source == 'qbo_sync')
         .filter(Transaction.doc_upload_id.is_(None)))
    if scope_pid is not None:
        q = q.filter(Transaction.project_id == scope_pid)
    else:
        q = q.filter(Transaction.project_id.is_(None))
    rows = q.all()
    dates = [t.txn_date for t in rows if t.txn_date]
    ids = [t.id for t in rows]
    coded = sum(1 for t in rows if t.budget_line_id or t.account_code)
    excluded = sum(1 for t in rows if t.not_project_expense)
    amount = round(sum(float(t.amount or 0) for t in rows), 2)
    report = {"dry": (not apply), "applied": apply,
              "scope": ("pool (unclaimed rows — deleted)" if scope_pid is None
                        else f"project {scope_pid} (released to the pool)"),
              "qbo_sync_total": len(rows), "coded": coded, "excluded": excluded,
              "amount": amount,
              "date_range": ([min(dates), max(dates)] if dates else None),
              "errors": []}
    if apply and ids:
        try:
            idset = set(ids)
            TransactionSplit.query.filter(TransactionSplit.transaction_id.in_(idset))\
                .delete(synchronize_session=False)
            _delete_evidence_for_txns(list(idset))
            Transaction.query.filter(Transaction.backup_of_txn_id.in_(idset)).update(
                {"backup_of_txn_id": None}, synchronize_session=False)
            if scope_pid is not None:
                from actuals import release
                for t in rows:
                    t.budget_line_id = t.account_code = t.account_code_name = None
                    t.suggested_budget_line_id = None
                    t.not_project_expense = False
                    t.excluded_at = t.excluded_by_user_id = t.excluded_reason = None
                    t.match_status = 'unmatched'
                    release(t, why='QuickBooks imports purged for the project', has_splits=False)
                db.session.commit()
                report["released"] = len(rows)
                _note = f'Released {len(rows)} QuickBooks-imported row(s) to the company pool'
            else:
                Transaction.query.filter(Transaction.parent_transaction_id.in_(idset)).update(
                    {"parent_transaction_id": None}, synchronize_session=False)
                deleted = (Transaction.query
                           .filter(Transaction.id.in_(idset))
                           .delete(synchronize_session=False))
                db.session.commit()
                report["deleted"] = int(deleted)
                _note = f'Purged {int(deleted)} unclaimed QuickBooks row(s) from the pool'
            try:
                _log_activity(action='delete' if scope_pid is None else 'update',
                              entity_type='transaction', entity_id=0,
                              entity_label=f'QBO purge ({len(rows)} rows)',
                              project_id=scope_pid, before={'ids': sorted(idset)[:500]},
                              note=_note)
            except Exception:
                pass
        except Exception as e:
            db.session.rollback()
            report["errors"].append(str(e))
            return jsonify(report), 500
    return jsonify(report)


# ── Admin: cleanup of estimate-linked Transactions ──────────────────────────
# Both the upload and metadata-update paths already prevent estimate-type
# docs from creating Transaction rows. But pre-guard legacy data and the
# QBO-sync-then-manual-match pathway can leave stray estimate-linked
# Transactions in the DB, polluting Actual rollups. This route gives the
# super admin a way to list them (GET) and delete them (POST).
# 2026-05-14 per user report.
@app.route("/admin/cleanup-estimate-txns", methods=["GET", "POST"])
@maintenance_required
def admin_cleanup_estimate_txns():
    from flask import request, render_template_string
    # Find every Transaction whose linked doc has a non-ledger category.
    # Join via doc_upload_id so a Transaction with no doc attached is
    # never picked up — manual-entry / pure-QBO rows are always kept.
    q = (db.session.query(Transaction, DocUpload)
         .join(DocUpload, DocUpload.id == Transaction.doc_upload_id)
         .filter(DocUpload.category.in_(_NON_LEDGER_DOC_CATEGORIES))
         .order_by(Transaction.txn_date.desc().nullslast(),
                   Transaction.id.desc()))
    rows = q.all()
    if request.method == "POST" and request.form.get("confirm") == "delete":
        # Cleanup A #3 (2026-11-02): this used to hard-delete every row in the
        # list, bank charges included — a QBO charge matched to a doc that was
        # later retyped as a PO vanished from the ledger. Only the documents'
        # OWN rows (source='doc_upload') are deleted; bank / manual rows are
        # UNLINKED from the paperwork and keep their coding. Evidence + splits
        # are cleared first (no FK cascade on either).
        _doomed = [t.id for (t, _d) in rows if t.source == 'doc_upload']
        _unlink = [t.id for (t, _d) in rows if t.source != 'doc_upload']
        _unlinked = 0
        if _unlink:
            _unlinked = (Transaction.query
                         .filter(Transaction.id.in_(_unlink))
                         .update({"doc_upload_id": None,
                                  "match_status": "unmatched",
                                  "match_confidence": None},
                                 synchronize_session=False))
        if _doomed:
            Transaction.query.filter(Transaction.parent_transaction_id.in_(_doomed)).update(
                {"parent_transaction_id": None}, synchronize_session=False)
            Transaction.query.filter(Transaction.backup_of_txn_id.in_(_doomed)).update(
                {"backup_of_txn_id": None}, synchronize_session=False)
            _delete_evidence_for_txns(_doomed)
            TransactionSplit.query.filter(TransactionSplit.transaction_id.in_(_doomed))\
                .delete(synchronize_session=False)
            db.session.flush()
            (Transaction.query
             .filter(Transaction.id.in_(_doomed))
             .delete(synchronize_session=False))
        if _doomed or _unlink:
            db.session.commit()
            logging.warning(f"[admin/cleanup] deleted {len(_doomed)} doc-born estimate-linked "
                            f"Transaction rows, unlinked {_unlinked} bank/manual rows "
                            f"by user={current_user.id}")
            try:
                _log_activity(action='delete', entity_type='transaction', entity_id=0,
                              entity_label=f'Estimate-linked cleanup ({len(_doomed)} deleted)',
                              before={'deleted_ids': _doomed[:200], 'unlinked_ids': _unlink[:200]},
                              note=f'Admin cleanup: removed {len(_doomed)} doc-born row(s) '
                                   f'behind non-ledger paperwork; unlinked {_unlinked} bank/manual row(s)')
            except Exception:
                pass
            return render_template_string(
                "<h2>Deleted {{ n }} doc-born estimate-linked Transaction rows; "
                "unlinked {{ u }} bank / manual charge(s) (kept, coding intact).</h2>"
                "<p><a href=\"{{ url_for('admin_cleanup_estimate_txns') }}\">Refresh list</a> · "
                "<a href=\"{{ url_for('dashboard') }}\">Dashboard</a></p>",
                n=len(_doomed), u=_unlinked)
        return redirect(url_for('admin_cleanup_estimate_txns'))

    HTML = """
<!doctype html><html><head><title>Cleanup estimate-linked Transactions</title>
<style>
 body{font-family:system-ui,sans-serif;max-width:1100px;margin:24px auto;padding:0 18px;color:#222}
 h1{font-size:18px;margin:0 0 6px} .sub{color:#6b7280;font-size:13px;margin-bottom:16px}
 table{border-collapse:collapse;width:100%;font-size:12px}
 th{background:#f3f4f6;text-align:left;padding:6px 8px;border-bottom:1px solid #e5e7eb}
 td{padding:5px 8px;border-bottom:1px solid #f3f4f6;vertical-align:top}
 .amt{text-align:right;font-variant-numeric:tabular-nums}
 .cat{background:#fef3c7;padding:1px 6px;border-radius:3px;font-size:11px}
 .empty{padding:24px;text-align:center;color:#6b7280;background:#f9fafb;border-radius:6px}
 .danger{background:#dc2626;color:#fff;border:none;padding:10px 16px;border-radius:5px;font-weight:600;cursor:pointer;font-size:14px}
 .danger:hover{background:#b91c1c}
</style></head><body>
<h1>Cleanup: estimate-linked Transactions</h1>
<div class="sub">
  Transactions whose linked DocUpload has a non-ledger category
  (estimate / quote / purchase_order / contract / tax_form / release / legal /
  insurance / misc). These shouldn't exist — they're paperwork, not spend —
  but legacy data + post-upload category edits can leave them. Cleaning up
  here deletes only the documents' <b>own</b> rows (source = doc_upload);
  bank / QuickBooks / CSV / manual charges in the list are <b>unlinked</b>
  from the paperwork and kept with their coding.
</div>
{% if rows %}
<form method="post" style="margin:18px 0">
  <input type="hidden" name="confirm" value="delete">
  <button class="danger" type="submit"
    onclick="return confirm('Clean up {{ rows|length }} rows? Doc-born rows are deleted (cannot be undone); bank / manual charges are unlinked and kept.')">
    🗑 Clean up all {{ rows|length }} rows
  </button>
</form>
<table>
  <thead><tr>
    <th>txn id</th><th>source</th><th>doc id</th><th>category</th>
    <th>txn date</th><th>vendor</th><th class="amt">amount</th>
    <th>doc filename</th>
  </tr></thead>
  <tbody>
  {% for t, d in rows %}
  <tr>
    <td>{{ t.id }}</td>
    <td>{{ t.source }}{% if t.source != 'doc_upload' %} <i>(unlink)</i>{% else %} <i>(delete)</i>{% endif %}</td>
    <td>{{ d.id }}</td>
    <td><span class="cat">{{ d.category }}</span></td>
    <td>{{ t.txn_date or '—' }}</td>
    <td>{{ t.vendor or '—' }}</td>
    <td class="amt">{{ '${:,.2f}'.format(t.amount|float) if t.amount is not none else '—' }}</td>
    <td>{{ d.filed_filename or d.original_filename or '' }}</td>
  </tr>
  {% endfor %}
  </tbody>
</table>
{% else %}
<div class="empty">✓ No estimate-linked Transactions found. All clean.</div>
{% endif %}
</body></html>
"""
    return render_template_string(HTML, rows=rows)


# ═══════════════════════════════════════════════════════════════════════════
# The COMPANY LEDGER (2026-11-03): every QuickBooks transaction once, plus
# the manual / CSV / activated-receipt rows, with who claimed it.
# ═══════════════════════════════════════════════════════════════════════════

LEDGER_STATUSES = ('all', 'claimed', 'unclaimed', 'not_project', 'conflict', 'qbo')


def _admin_pool_query_params():
    """Parse the /admin/transactions filters once for both the page and the
    JSON. Returns a dict; bad numbers fall back to defaults."""
    a = request.args
    def _int(k, default, lo=None, hi=None):
        try:
            v = int(a.get(k, default))
        except (TypeError, ValueError):
            v = default
        if lo is not None: v = max(lo, v)
        if hi is not None: v = min(hi, v)
        return v
    def _flag(k, default=False):
        v = (a.get(k) or '').strip().lower()
        if v == '':
            return default
        return v in ('1', 'true', 'yes', 'on')
    status = (a.get('status') or '').strip().lower()
    # The pre-ledger checkbox names still work.
    if status not in LEDGER_STATUSES:
        status = 'unclaimed' if _flag('hide_claimed') else 'all'
    return {"q": (a.get('q') or '').strip()[:120],
            "project_id": _int('project_id', 0) or None,
            "card": (a.get('card') or '').strip()[:8] or None,
            "from": (a.get('from') or '').strip()[:10] or None,
            "to": (a.get('to') or '').strip()[:10] or None,
            "status": status,
            "hide_not_project": _flag('hide_not_project', False),
            "limit": _int('limit', 200, 1, 1000),
            "offset": _int('offset', 0, 0)}


def _admin_pool_rows(p):
    """The company ledger, filtered and paged: one row per transaction
    (there is one row per QuickBooks transaction company-wide now).
    Returns (rows, total, project_names)."""
    from sqlalchemy import or_, and_
    from actuals import signed_amount_value
    from models import ExpenseEvidence
    # Every top-level money row: electronic + hand-typed, plus receipt-born
    # rows once activated into an expense; never split children or backup rows.
    q = (Transaction.query
         .filter(Transaction.backup_of_txn_id.is_(None),
                 or_(Transaction.source.is_(None),
                     Transaction.source.notin_(('doc_upload', 'invoice_split')),
                     and_(Transaction.source == 'doc_upload',
                          Transaction.activated_at.isnot(None)))))
    if p["project_id"]:
        q = q.filter(Transaction.project_id == p["project_id"])
    if p["status"] == 'claimed':
        q = q.filter(Transaction.project_id.isnot(None), Transaction.qbo_txn_id.isnot(None))
    elif p["status"] == 'unclaimed':
        q = q.filter(Transaction.project_id.is_(None))
    elif p["status"] == 'not_project':
        q = q.filter(Transaction.not_project_expense == True)          # noqa: E712
    elif p["status"] == 'conflict':
        q = q.filter(Transaction.claim_conflict == True)               # noqa: E712
    elif p["status"] == 'qbo':
        q = q.filter(Transaction.qbo_txn_id.isnot(None))
    if p["hide_not_project"]:
        q = q.filter(or_(Transaction.not_project_expense.is_(None),
                         Transaction.not_project_expense == False))    # noqa: E712
    if p["card"]:
        q = q.filter(Transaction.card_last4 == p["card"])
    if p["from"]:
        q = q.filter(Transaction.txn_date >= p["from"])
    if p["to"]:
        q = q.filter(Transaction.txn_date <= p["to"])
    if p["q"]:
        like = f"%{p['q']}%"
        conds = [Transaction.vendor.ilike(like), Transaction.note.ilike(like),
                 Transaction.card_last4 == p["q"], Transaction.qbo_txn_id == p["q"]]
        try:
            from decimal import Decimal as _D
            _amt = _D(p["q"].replace('$', '').replace(',', ''))
            conds.append(Transaction.amount == _amt)
        except Exception:
            pass
        if p["q"].isdigit():
            conds.append(Transaction.id == int(p["q"]))
        q = q.filter(or_(*conds))
    total = q.count()
    txns = (q.order_by(Transaction.txn_date.desc().nullslast(), Transaction.id.desc())
            .offset(p["offset"]).limit(p["limit"]).all())
    ids = [t.id for t in txns]
    split_parent_ids = set()
    if ids:
        split_parent_ids = {r[0] for r in db.session.query(TransactionSplit.transaction_id)
                            .filter(TransactionSplit.transaction_id.in_(ids)).distinct().all()}
    evidence_ids = set()
    if ids:
        evidence_ids = {r[0] for r in db.session.query(ExpenseEvidence.transaction_id)
                        .filter(ExpenseEvidence.transaction_id.in_(ids)).distinct().all()}
    line_ids = {t.budget_line_id for t in txns if t.budget_line_id}
    lines = {}
    if line_ids:
        from models import BudgetLine
        lines = {l.id: l for l in BudgetLine.query.filter(BudgetLine.id.in_(list(line_ids))).all()}
    project_names = {pp.id: pp.name for pp in ProjectSheet.query.all()}
    from budget_calc import FP_COA_NAMES as _COA
    rows = []
    for t in txns:
        has_splits = t.id in split_parent_ids
        line_label = None
        if t.budget_line_id and t.budget_line_id in lines:
            l = lines[t.budget_line_id]
            line_label = f"{l.account_code} · {(l.description or l.account_name or '')[:50]}"
        elif has_splits:
            line_label = "split"
        elif t.account_code:
            line_label = f"{t.account_code} · {_COA.get(t.account_code, '')} (section)"
        backup = ('backup' if (t.id in evidence_ids or (t.doc_upload_id and t.match_status == 'confirmed'))
                  else 'reason' if getattr(t, 'no_charge_reason', None)
                  else 'none')
        # The bank / card account the money moved on: the card's last four,
        # else the QuickBooks account display string the sync stored on an
        # uncoded row ("CC: 1234"), else the payment method / entity type.
        if t.card_last4:
            account = f"••{t.card_last4}"
        elif t.qbo_txn_id and not (t.budget_line_id or t.account_code) and (t.account_code_name or '').strip():
            account = (t.account_code_name or '')[:40]
        elif getattr(t, 'payment_method', None):
            account = (t.payment_method or '').replace('_', ' ').capitalize()
        elif t.qbo_txn_type == 'BillPayment':
            account = 'Bank · bill payment'
        elif t.qbo_txn_type:
            account = t.qbo_txn_type
        else:
            account = ''
        claimer = t.project_id
        rows.append({
            "id": t.id,
            "qbo_txn_id": t.qbo_txn_id, "qbo_txn_type": t.qbo_txn_type,
            "date": (t.txn_date or '')[:10], "vendor": t.vendor or '—',
            "amount": signed_amount_value(t.amount, t.is_expense),
            "card_last4": t.card_last4, "account": account,
            "source": ('receipt' if t.source == 'doc_upload' else (t.source or 'manual_entry')),
            "project_id": claimer, "project": (project_names.get(claimer) if claimer else None),
            "row_project_id": t.project_id,
            "line": line_label, "backup": backup,
            "excluded": bool(t.not_project_expense),
            "claim_conflict": bool(getattr(t, 'claim_conflict', False)),
            "claim_conflict_reason": getattr(t, 'claim_conflict_reason', None),
            "payment_method": getattr(t, 'payment_method', None),
        })
    return rows, total, project_names


@app.route("/admin/transactions.json", methods=["GET"])
@login_required
@maintenance_required
def admin_transactions_json():
    """The company ledger as JSON (2026-11-03): every transaction once with
    the claiming project ("Unclaimed" when none), line label, backup state,
    account and source. Query params: q (vendor / memo / amount / last four
    / QBO id / row id), project_id, status (all | claimed | unclaimed |
    not_project | conflict | qbo), card, from, to (YYYY-MM-DD),
    hide_not_project, limit (≤1000), offset."""
    p = _admin_pool_query_params()
    rows, total, _names = _admin_pool_rows(p)
    return jsonify({"ok": True, "rows": rows, "total": total,
                    "limit": p["limit"], "offset": p["offset"], "filters": p})


@app.route("/admin/transactions.csv", methods=["GET"])
@login_required
@maintenance_required
def admin_transactions_csv():
    """The ledger as a CSV download with the SAME filters as the page and the
    JSON (Phase 3, 2026-11-01). Ignores limit / offset: an export is the
    whole filtered set (capped at 20,000 rows). Columns in
    actuals.ADMIN_POOL_CSV_COLUMNS."""
    from actuals import admin_pool_csv
    from flask import Response
    p = _admin_pool_query_params()
    p["offset"], p["limit"] = 0, 20000
    rows, total, _names = _admin_pool_rows(p)
    body = admin_pool_csv(rows)
    _stamp = datetime.utcnow().strftime('%Y-%m-%d')
    return Response(body, mimetype="text/csv",
                    headers={"Content-Disposition":
                             f'attachment; filename="fpbudget-ledger-{_stamp}.csv"'})


@app.route("/admin/transactions", methods=["GET"])
@login_required
@maintenance_required
def admin_transactions_page():
    """The company ledger page (2026-11-03): search, project / status /
    card / date filters, one row per transaction with Date · Payee ·
    Amount · Account · Claimed by · Line · Backup. A row click opens the
    charge in the claiming project's workspace (?txn= selects and scrolls
    to it); an unclaimed row opens in the first active project's pool.
    The JSON above is the contract; this page is a server-rendered read
    of the same rows."""
    from models import QBOConnection
    import json as _json_adm
    p = _admin_pool_query_params()
    rows, total, names = _admin_pool_rows(p)
    projects = sorted(names.items(), key=lambda kv: (kv[1] or '').lower())
    try:
        _qc = QBOConnection.query.first()
        accounts = len(_json_adm.loads((_qc.enabled_account_ids if _qc else None) or '[]'))
    except Exception:
        accounts = 0
    unclaimed = Transaction.query.filter(Transaction.project_id.is_(None),
                                         Transaction.qbo_txn_id.isnot(None)).count()
    conflicts = Transaction.query.filter(Transaction.claim_conflict == True).count()   # noqa: E712
    _first = (ProjectSheet.query.filter(ProjectSheet.status == 'active')
              .order_by(ProjectSheet.name).first())
    net = round(sum(float(r.get("amount") or 0) for r in rows), 2)
    cards = sorted({r["card_last4"] for r in rows if r.get("card_last4")})
    _mon = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
    _this_year = str(datetime.utcnow().year)
    for r in rows:
        d = r.get("date") or ''
        try:
            r["date_short"] = (f"{_mon[int(d[5:7]) - 1]} {int(d[8:10])}"
                               + ('' if d[:4] == _this_year else f" {d[2:4]}")) if len(d) >= 10 else d
        except (ValueError, IndexError):
            r["date_short"] = d
    return render_template("admin_transactions.html", rows=rows, total=total,
                           filters=p, projects=projects, accounts=accounts,
                           unclaimed=unclaimed, conflicts=conflicts,
                           pool_pid=(_first.id if _first else None),
                           statuses=LEDGER_STATUSES, net=net, cards=cards)


# ═══ Phase 5 — Reconcile with QuickBooks (Maintenance tool, 2026-11-03) ═══

@app.route("/admin/qbo-reconcile-deletions", methods=["POST"])
@login_required
@maintenance_required
def admin_qbo_reconcile_deletions():
    """Maintenance → "Reconcile with QuickBooks". Company-wide by default
    (?project=all — every QuickBooks row once, claimed or in the pool,
    since the company ledger), or the rows one project claimed
    (?project=<pid>): re-fetch by id the QuickBooks records FPBudget holds
    from the last 120 days (?days=) and report the ones QuickBooks no
    longer returns — voided or deleted after import. Dry run by default;
    ?apply=1 applies THE deletion rule (qbo_sync.apply_qbo_deletions /
    actuals.qbo_deletion_action): uncoded + no receipt → deleted (logged,
    undoable); coded or with a receipt → kept, excluded, flagged
    'Voided in QuickBooks'. Fail-open: an API error on one project is
    reported, the others still run."""
    from models import QBOConnection
    from qbo_sync import reconcile_qbo_deletions
    conn = QBOConnection.query.first()
    if not conn or not conn.refresh_token:
        return jsonify({"error": "QuickBooks not connected. Connect it on the Admin hub first.",
                        "needs_oauth": True}), 400
    apply = (request.args.get('apply') == '1')
    scope = (request.args.get('project') or request.args.get('pid') or 'all').strip()
    try:
        days = max(7, min(400, int(request.args.get('days') or 120)))
    except (TypeError, ValueError):
        days = 120
    if scope in ('', 'all'):
        projects = [None]        # the whole company ledger in one pass
    else:
        try:
            p = ProjectSheet.query.get(int(scope))
        except (TypeError, ValueError):
            p = None
        if p is None:
            return jsonify({"error": f"project {scope} not found"}), 404
        projects = [p]
    report, errors = [], []
    for p in projects:
        try:
            report.append(reconcile_qbo_deletions(p, conn, db, days=days, apply=apply))
        except Exception as e:
            db.session.rollback()
            logging.warning(f"[qbo-reconcile] {'company' if p is None else 'project %s' % p.id} failed: {e}")
            errors.append({"project_id": (p.id if p is not None else None),
                           "project": (p.name if p is not None else 'whole company'),
                           "error": str(e)[:300]})
    n_missing = sum(len(r["missing"]) for r in report)
    n_deleted = sum(len((r.get("applied") or {}).get("deleted") or []) for r in report)
    n_voided = sum(len((r.get("applied") or {}).get("voided") or []) for r in report)
    if apply and (n_deleted or n_voided):
        try:
            _log_activity(action='update', entity_type='qbo_sync', entity_id=0,
                          entity_label='Reconcile with QuickBooks',
                          after={"deleted": n_deleted, "voided": n_voided, "days": days},
                          note=f'Reconcile with QuickBooks: {n_deleted} removed, {n_voided} flagged voided '
                               f'({'whole company' if scope in ('', 'all') else 'project ' + scope})')
        except Exception:
            pass
    return jsonify({"ok": True, "dry": not apply, "days": days,
                    "projects": len(report), "missing": n_missing,
                    "deleted": n_deleted, "voided": n_voided,
                    "report": report, "errors": errors,
                    "note": ("Dry run — nothing changed. Apply removes uncoded rows with no receipt "
                             "and flags the rest 'Voided in QuickBooks' (both undoable)." if not apply
                             else "Applied.")})
