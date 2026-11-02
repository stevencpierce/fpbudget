"""Super-admin transaction-pool routes — M1b-2 slice of the app.py split
(ACTUALS_HANDOFF Phase 2, 2026-10-30).

The cross-project claim backfill, the coding backup JSON, the QBO import
repair / purge tools, the QBO coverage matrix and the estimate-txn cleanup
page. Same pattern as routes/actuals.py: @app.route on the same app object,
endpoint names and URLs unchanged. /admin/transactions (the whole pool,
one row per QBO charge) is added at the bottom under its own banner.
"""
import logging
from datetime import (datetime)
from flask import (jsonify, redirect, render_template, request, url_for)
from flask_login import (current_user, login_required)

from app import (app, _NON_LEDGER_DOC_CATEGORIES, _delete_evidence_for_txns, _log_activity, _sync_claim_state, admin_required, super_admin_required)
from models import (DocUpload, ProjectSheet, Transaction, TransactionSplit, db)


@app.route("/admin/cross-project-claim-backfill", methods=["GET", "POST"])
@login_required
@super_admin_required
def admin_cross_project_claim_backfill():
    """One-shot: run _sync_claim_state across every already-coded
    transaction in the database so the new fuzzy-match channel
    retroactively claims siblings in other projects.

    User report 2026-05-15: a Lyft $37.93 was coded in Project A but
    still showing in Project B because the pre-fuzzy claim logic only
    matched on qbo_txn_id and Project A's row was doc-only. After
    deploying the fuzzy match, NEW coding actions claim correctly, but
    historical ones don't until we walk the table once.

    GET shows a dry-run preview (how many siblings would get claimed
    without writing anything). POST runs it for real and commits.
    """
    from sqlalchemy import or_ as _or
    # "Coded" for this backfill = any claim signal recognized by
    # _sync_claim_state. As of 2026-05-15 that includes doc_upload_id
    # too (receipt attachment → claim).
    coded_q = (Transaction.query
               .filter(_or(Transaction.budget_line_id.isnot(None),
                           Transaction.account_code.isnot(None),
                           Transaction.not_project_expense == True,
                           Transaction.doc_upload_id.isnot(None)))
               .filter(Transaction.amount.isnot(None),
                       Transaction.txn_date.isnot(None),
                       Transaction.vendor.isnot(None)))
    coded_count = coded_q.count()

    if request.method == "GET":
        # Dry-run preview: iterate but rollback at the end.
        marked = 0
        preview = []
        for t in coded_q.limit(2000).all():
            _before = {
                str(s.id): (s.claimed_by_project_id, bool(s.not_project_expense))
                for s in Transaction.query
                              .filter(Transaction.project_id != t.project_id,
                                      Transaction.amount == t.amount,
                                      Transaction.txn_date == t.txn_date)
                              .limit(20).all()
            }
            _sync_claim_state(t)
            for s in db.session.dirty:
                if not isinstance(s, Transaction):
                    continue
                _b = _before.get(str(s.id))
                if _b is None:
                    continue
                if _b != (s.claimed_by_project_id, bool(s.not_project_expense)):
                    marked += 1
                    if len(preview) < 25:
                        preview.append({
                            'sibling_id':   s.id,
                            'sibling_proj': s.project_id,
                            'vendor':       s.vendor,
                            'amount':       float(s.amount or 0),
                            'date':         s.txn_date,
                            'claimed_by':   s.claimed_by_project_id,
                            'not_project':  bool(s.not_project_expense),
                        })
        db.session.rollback()
        return jsonify({
            'ok': True,
            'mode': 'dry-run',
            'coded_transactions': coded_count,
            'siblings_would_claim': marked,
            'sample': preview,
            'next': 'POST to this URL to apply.',
        })

    # POST: actually run + commit.
    marked = 0
    errors = 0
    for t in coded_q.all():
        try:
            _sync_claim_state(t)
            for s in list(db.session.dirty):
                if isinstance(s, Transaction):
                    marked += 1
        except Exception as e:
            errors += 1
            logging.warning("[claim-backfill] failed on txn #%s: %s", t.id, e)
    db.session.commit()
    return jsonify({
        'ok': True,
        'mode': 'committed',
        'coded_transactions': coded_count,
        'siblings_marked_dirty': marked,
        'errors': errors,
    })


@app.route("/admin/backup/transactions.json", methods=["GET"])
@login_required
@super_admin_required
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


@app.route("/admin/qbo-coverage", methods=["GET"])
@login_required
@admin_required
def admin_qbo_coverage():
    """Per-project QBO sync diagnostics.

    Shows a matrix of every active project × every Bank/Credit-Card
    account on the connected QBO realm, with a ✓ wherever the project
    has that account in its qbo_account_ids. Also surfaces each
    project's sync_through watermark + last_synced + last_cdc_sync, so
    you can pinpoint "why didn't this project see those 5/12 Amazon
    charges?" — almost always: the relevant card isn't selected, or
    the watermark is parked too far in the future.

    Built 2026-05-14 in response to user report that two projects
    "synced" minutes apart saw different transaction sets.
    """
    from models import QBOConnection
    import json as _json

    projects = (ProjectSheet.query
                .filter(ProjectSheet.status != 'archived')
                .order_by(ProjectSheet.name).all())

    conn = QBOConnection.query.first()
    qbo_connected = bool(conn and conn.refresh_token)
    accounts = []
    qbo_error = None
    if qbo_connected:
        try:
            from qbo_sync import list_qbo_accounts
            accounts = list_qbo_accounts(conn, db)
            # Stable sort: Bank first, then Credit Card; then by name.
            accounts.sort(key=lambda a: (
                0 if a.get("type") == "Bank" else 1,
                (a.get("name") or "").lower(),
            ))
        except Exception as e:
            qbo_error = str(e)
            logging.exception("[admin qbo-coverage] list_qbo_accounts failed")

    # Build matrix: each row = project with its enabled-set + watermark.
    rows = []
    for p in projects:
        try:
            enabled_ids = set(_json.loads(p.qbo_account_ids or "[]") or [])
        except Exception:
            enabled_ids = set()
        rows.append({
            "project":      p,
            "enabled_ids":  enabled_ids,
            "enabled_count": len(enabled_ids),
            # The 4 timestamps that govern *what* a sync pulls.
            "sync_through": p.sync_through,
            "last_synced":  p.last_synced,
            "last_cdc_sync": p.last_cdc_sync,
        })

    return render_template("admin_qbo_coverage.html",
                           projects=rows,
                           accounts=accounts,
                           qbo_connected=qbo_connected,
                           qbo_error=qbo_error,
                           now=datetime.utcnow())


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
    if current_user.role not in ('super_admin', 'admin'):
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
    """Undo electronic (QuickBooks) imports — delete Transactions with
    source='qbo_sync'. Receipts (doc_upload), typed rows (manual_entry) and
    invoice splits are NEVER touched. Dry-run by default; ?apply=1 to delete.
    Scope: ?project=<pid> limits to one project; default = all projects.

    Before deleting we:
      • release cross-project claims that point at a project whose qbo rows
        are being removed (clear claimed_by_project_id on surviving rows),
      • detach any child transaction whose parent is being deleted
        (parent_transaction_id → NULL) so no split is orphaned.
    Linked DocUploads are left intact (the link lives on the txn side).
    Admin only. (User 2026-06-01.)"""
    if current_user.role not in ('super_admin', 'admin'):
        return jsonify({"error": "Forbidden — admin only"}), 403
    apply = (request.args.get('apply') == '1')
    scope_pid = request.args.get('project')
    try:
        scope_pid = int(scope_pid) if scope_pid not in (None, '', 'all') else None
    except (TypeError, ValueError):
        return jsonify({"error": "project must be an integer or 'all'"}), 400

    # Defense-in-depth: never delete a qbo_sync row that is matched to a receipt
    # (doc_upload_id set). Confirming a match now flips source→'reconciled', but
    # rows confirmed before that fix are still source='qbo_sync' with a doc link;
    # deleting them would lose the receipt's only ledger row. (Code review
    # 2026-06-04.)
    q = (Transaction.query
         .filter(Transaction.source == 'qbo_sync')
         .filter(Transaction.doc_upload_id.is_(None)))
    if scope_pid is not None:
        q = q.filter(Transaction.project_id == scope_pid)
    rows = q.all()

    # Per-project breakdown so the user can confirm scope before deleting.
    by_proj = {}
    dates = []
    ids = []
    for t in rows:
        ids.append(t.id)
        if t.txn_date:
            dates.append(t.txn_date)
        p = by_proj.setdefault(t.project_id, {"total": 0, "coded": 0, "uncoded": 0,
                                              "claimed": 0, "with_doc": 0,
                                              "amount": 0.0})
        p["total"] += 1
        if t.budget_line_id:
            p["coded"] += 1
        else:
            p["uncoded"] += 1
        if t.claimed_by_project_id:
            p["claimed"] += 1
        if t.doc_upload_id:
            p["with_doc"] += 1
        p["amount"] += float(t.amount or 0)

    # Context: what is being PRESERVED (so the user sees the blast radius).
    preserved = {}
    for src in ('manual_entry', 'doc_upload', 'invoice_split', 'reconciled'):
        pq = Transaction.query.filter(Transaction.source == src)
        if scope_pid is not None:
            pq = pq.filter(Transaction.project_id == scope_pid)
        preserved[src] = pq.count()
    # qbo_sync rows kept because they're matched to a receipt (excluded above).
    _mq = (Transaction.query.filter(Transaction.source == 'qbo_sync')
           .filter(Transaction.doc_upload_id.isnot(None)))
    if scope_pid is not None:
        _mq = _mq.filter(Transaction.project_id == scope_pid)
    preserved['qbo_sync_matched_kept'] = _mq.count()

    # Project names for readability
    pnames = {p.id: p.name for p in ProjectSheet.query.all()}
    report = {"dry": (not apply), "applied": apply,
              "scope": ("all projects" if scope_pid is None else f"project {scope_pid}"),
              "qbo_sync_total": len(rows),
              "date_range": ([min(dates), max(dates)] if dates else None),
              "by_project": {f"{pid_}{(' · '+pnames[pid_]) if pnames.get(pid_) else ''}": v
                             for pid_, v in sorted(by_proj.items())},
              "preserved_counts": preserved,
              "errors": []}

    if apply and ids:
        try:
            idset = set(ids)
            # 1) detach children whose parent is being deleted
            child_q = Transaction.query.filter(
                Transaction.parent_transaction_id.in_(idset))
            detached = 0
            for c in child_q.all():
                if c.id not in idset:        # don't bother for rows also dying
                    c.parent_transaction_id = None
                    detached += 1
            # 2) release cross-project claims pointing at affected projects
            affected_projects = set(by_proj.keys())
            released = (Transaction.query
                        .filter(Transaction.claimed_by_project_id.in_(affected_projects))
                        .filter(~Transaction.id.in_(idset))
                        .update({"claimed_by_project_id": None},
                                synchronize_session=False))
            db.session.flush()
            # 3) delete the qbo_sync rows
            deleted = (Transaction.query
                       .filter(Transaction.id.in_(idset))
                       .delete(synchronize_session=False))
            db.session.commit()
            report["deleted"] = int(deleted)
            report["children_detached"] = detached
            report["claims_released"] = int(released or 0)
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
@super_admin_required
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
# ACTUALS_HANDOFF Phase 2 (2026-10-30): the whole pool across projects.
# ═══════════════════════════════════════════════════════════════════════════

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
    return {"q": (a.get('q') or '').strip()[:120],
            "project_id": _int('project_id', 0) or None,
            "card": (a.get('card') or '').strip()[:8] or None,
            "from": (a.get('from') or '').strip()[:10] or None,
            "to": (a.get('to') or '').strip()[:10] or None,
            "hide_claimed": _flag('hide_claimed', False),
            "hide_not_project": _flag('hide_not_project', False),
            "limit": _int('limit', 200, 1, 1000),
            "offset": _int('offset', 0, 0)}


def _admin_pool_rows(p):
    """The pool, one row per QBO charge (manual / CSV rows by id), filtered
    and paged. Returns (rows, total_groups, project_names)."""
    from sqlalchemy import or_, and_
    from actuals import group_pool_rows, signed_amount_value
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
    txns = q.order_by(Transaction.txn_date.desc().nullslast(), Transaction.id.desc()).all()
    ids = [t.id for t in txns]
    split_parent_ids = set()
    if ids:
        split_parent_ids = {r[0] for r in db.session.query(TransactionSplit.transaction_id)
                            .filter(TransactionSplit.transaction_id.in_(ids)).distinct().all()}
    for t in txns:
        t.has_splits = t.id in split_parent_ids
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
    for rep, members in group_pool_rows(txns):
        # The claiming project: the member that carries coding / splits, else
        # the claimed_by_project_id any member points at, else none.
        claimer = None
        for m in members:
            if m.budget_line_id or m.account_code or getattr(m, 'has_splits', False):
                claimer = m.project_id
                break
        if claimer is None:
            for m in members:
                if getattr(m, 'claimed_by_project_id', None):
                    claimer = m.claimed_by_project_id
                    break
        # Excluded: every member of the group is not-project and none is
        # claimed by a project (an unclaimed sibling hidden by a claim is
        # "claimed", not "excluded").
        all_np = all(m.not_project_expense for m in members)
        excluded = bool(all_np and claimer is None)
        if p["hide_claimed"] and claimer is not None:
            continue
        if p["hide_not_project"] and excluded:
            continue
        t = rep
        for m in members:
            if m.project_id == claimer:
                t = m
                break
        else:
            # Unclaimed: show the sibling that carries the most detail.
            t = max(members, key=lambda m: (bool(m.card_last4), bool(m.note), -m.id))
        line_label = None
        if t.budget_line_id and t.budget_line_id in lines:
            l = lines[t.budget_line_id]
            line_label = f"{l.account_code} · {(l.description or l.account_name or '')[:50]}"
        elif getattr(t, 'has_splits', False):
            line_label = "split"
        elif t.account_code:
            line_label = f"{t.account_code} · {_COA.get(t.account_code, '')} (section)"
        backup = ('backup' if (t.id in evidence_ids or (t.doc_upload_id and t.match_status == 'confirmed'))
                  else 'reason' if getattr(t, 'no_charge_reason', None)
                  else 'none')
        rows.append({
            "id": t.id, "sibling_ids": [m.id for m in members if m.id != t.id],
            "qbo_txn_id": t.qbo_txn_id, "qbo_txn_type": t.qbo_txn_type,
            "date": (t.txn_date or '')[:10], "vendor": t.vendor or '—',
            "amount": signed_amount_value(t.amount, t.is_expense),
            "card_last4": t.card_last4,
            "source": ('receipt' if t.source == 'doc_upload' else (t.source or 'manual_entry')),
            "project_id": claimer, "project": (project_names.get(claimer) if claimer else None),
            # The row's own project (the claimer's when claimed): where a
            # click opens it, even when nobody has claimed it yet.
            "row_project_id": t.project_id,
            "seen_in": sorted({project_names.get(m.project_id, f'#{m.project_id}') for m in members}),
            "line": line_label, "backup": backup, "excluded": excluded,
            "payment_method": getattr(t, 'payment_method', None),
        })
    total = len(rows)
    return rows[p["offset"]:p["offset"] + p["limit"]], total, project_names


@app.route("/admin/transactions.json", methods=["GET"])
@login_required
@super_admin_required
def admin_transactions_json():
    """The whole transaction pool across projects (Phase 2, 2026-10-30): one
    row per QBO charge (sibling rows of the same qbo_txn_id collapse; manual /
    CSV rows by id) with the claiming project, line label, backup state,
    excluded flag and source. Query params: q (vendor / memo / amount / last
    four / id), project_id, card, from, to (YYYY-MM-DD), hide_claimed,
    hide_not_project, limit (≤1000), offset."""
    p = _admin_pool_query_params()
    rows, total, _names = _admin_pool_rows(p)
    return jsonify({"ok": True, "rows": rows, "total": total,
                    "limit": p["limit"], "offset": p["offset"], "filters": p})


@app.route("/admin/transactions.csv", methods=["GET"])
@login_required
@super_admin_required
def admin_transactions_csv():
    """The pool as a CSV download with the SAME filters as the page and the
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
                             f'attachment; filename="fpbudget-transactions-{_stamp}.csv"'})


@app.route("/admin/transactions", methods=["GET"])
@login_required
@super_admin_required
def admin_transactions_page():
    """The super-admin pool page (Phase 3 design, 2026-11-01): search,
    project / card / date filters, Hide claimed and Hide not-project, one
    row per charge with the claiming project, line, backup state and
    source. A row click opens the charge in the claiming project's
    workspace (?txn= selects and scrolls to it). The JSON above is the
    contract; this page is a server-rendered read of the same rows."""
    p = _admin_pool_query_params()
    rows, total, names = _admin_pool_rows(p)
    projects = sorted(names.items(), key=lambda kv: (kv[1] or '').lower())
    # Header line: how many projects sync QBO accounts, and the pool size
    # (unfiltered) so the filtered count has a denominator.
    try:
        import json as _json_adm
        syncing = sum(1 for pp in ProjectSheet.query.all()
                      if pp.qbo_account_ids and _json_adm.loads(pp.qbo_account_ids or '[]'))
    except Exception:
        syncing = 0
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
                           filters=p, projects=projects, syncing=syncing,
                           net=net, cards=cards)
