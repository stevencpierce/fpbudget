"""The project cost ledger as a spreadsheet (INTAKE_WRAP_HANDOFF Phase 1,
2026-11-23) — available at any time, and written into the wrap package in
Phase 2. Plain values, no formulas or macros, so it works without the app.

Sheets
  Ledger     one row per expense or split piece: reference number, date,
             payee (and the loan-out it was paid via), amount (credits
             negative), account code, section, budget line, payment
             method, QuickBooks type and id, file name(s), backup status,
             and a blank Qualified column (no tax-credit tagging yet)
  By line    the live budget's lines: working, actual, variance and the
             reference numbers behind each actual
  Payees     total paid per payee, W-9 status, QuickBooks vendor id
  Documents  every live document of the project and the references it backs
  Waivers    wrap-gate waivers (empty until Phase 2)
  Read me    what the sheets are and as-of time

Every number goes through the existing one rules: row_counts_as_spend,
signed_amount_value, section_for_code, payee_display_for_txn,
payee_w9_status, _po_live_line_totals (the budget grid's live total).
"""
import csv
import io
from datetime import datetime

from budget_calc import FP_COA_NAMES, section_for_code
from models import (db, BudgetLine, CrewMember, DocUpload, ExpenseEvidence, ProjectSheet,
                    Transaction, TransactionSplit)

LEDGER_COLUMNS = ["Reference", "Date", "Payee", "Paid via", "Vendor (as charged)", "Amount",
                  "Account code", "Section", "Budget line", "Payment method",
                  "QuickBooks type", "QuickBooks id", "File name(s)", "Backup", "Qualified"]


def _section_text(code):
    if not code:
        return ''
    sec = section_for_code(int(code))
    return f"{sec} {FP_COA_NAMES.get(sec, '')}".strip() if sec else ''


def _line_text(ln):
    if ln is None:
        return ''
    return (ln.description or ln.account_name or '').strip()


def build_ledger(pid):
    """{sheet name: [header, *rows]} for project `pid`."""
    import actuals
    from actuals import PAYMENT_METHOD_LABELS
    from app import _split_parent_id_set, _po_live_line_totals
    import project_codes as pc

    p = db.session.get(ProjectSheet, pid)
    containers = set(_split_parent_id_set(pid))
    txns = (Transaction.query.filter(Transaction.project_id == pid)
            .order_by(Transaction.txn_date, Transaction.id).all())
    # Containers count only through their pieces; the predicate is told
    # to skip the container rule so a split parent qualifies here.
    spend = [t for t in txns if actuals.row_counts_as_spend(t, split_parent_ids=set())]
    by_id = {t.id: t for t in txns}
    refs = pc.ref_labels(txns)

    pieces = {}
    if spend:
        for sp in TransactionSplit.query.filter(
                TransactionSplit.transaction_id.in_([t.id for t in spend])).all():
            pieces.setdefault(sp.transaction_id, []).append(sp)

    tids = [t.id for t in spend]
    ev = {}
    for e in (ExpenseEvidence.query.filter(ExpenseEvidence.transaction_id.in_(tids)).all() if tids else []):
        ev.setdefault(e.transaction_id, set()).add(e.doc_upload_id)
    doc_ids = {t.doc_upload_id for t in spend if t.doc_upload_id} | {d for s in ev.values() for d in s}
    docs = {d.id: d for d in DocUpload.query.filter(DocUpload.id.in_(doc_ids)).all()} if doc_ids else {}
    payee_ids = {t.payee_id for t in spend if getattr(t, 'payee_id', None)}
    payees = {c.id: c for c in CrewMember.query.filter(CrewMember.id.in_(payee_ids)).all()} if payee_ids else {}
    line_ids = {t.budget_line_id for t in spend if t.budget_line_id} | \
               {sp.budget_line_id for s in pieces.values() for sp in s if sp.budget_line_id}
    lines = {ln.id: ln for ln in BudgetLine.query.filter(BudgetLine.id.in_(line_ids)).all()} if line_ids else {}
    try:
        lo_index = actuals.loanout_index()
    except Exception:
        lo_index = None

    def _files(t):
        ids = ([t.doc_upload_id] if t.doc_upload_id else []) + sorted(ev.get(t.id, ()))
        names = []
        for i in ids:
            d = docs.get(i)
            if d is not None:
                n = d.filed_filename or d.original_filename or f"Doc #{d.id}"
                if n not in names:
                    names.append(n)
        return names

    def _backup(t, files):
        if files:
            return "Backed up"
        if t.no_charge_reason:
            return f"No receipt: {t.no_charge_reason}"
        return "Missing"

    def _payee(t, files):
        c = payees.get(getattr(t, 'payee_id', None))
        if c is None:
            return '', ''
        doc_vendor = None
        if t.doc_upload_id and docs.get(t.doc_upload_id) is not None:
            doc_vendor = docs[t.doc_upload_id].vendor
        info = actuals.payee_display_for_txn(t, c.name, doc_vendor, index=lo_index)
        return c.name, info.get('via') or ''

    rows, by_line_actual, by_line_refs, by_payee = [], {}, {}, {}
    for t in spend:
        if t.id in containers and not pieces.get(t.id):
            continue   # an itemized parent: its children are the rows
        parent = by_id.get(t.parent_transaction_id) if t.parent_transaction_id else None
        ref = refs.get(t.id) or (refs.get(parent.id) if parent is not None else None) or ''
        src = parent if parent is not None else t
        files = _files(src) if parent is not None else _files(t)
        payee, via = _payee(t, files)
        if not payee and parent is not None:
            payee, via = _payee(parent, files)
        method = (PAYMENT_METHOD_LABELS.get(t.payment_method or '', '') if t.payment_method
                  else (f"Card ••{t.card_last4}" if t.card_last4 else ''))
        base = {"ref": ref, "date": (t.txn_date or '')[:10], "payee": payee, "via": via,
                "vendor": t.vendor or '', "method": method,
                "qbo_type": src.qbo_txn_type or '', "qbo_id": src.qbo_txn_id or '',
                "files": "; ".join(files), "backup": _backup(src, files)}
        parts = pieces.get(t.id) or [None]
        for sp in parts:
            amount = actuals.signed_amount_value(sp.amount if sp else t.amount, t.is_expense)
            line_id = (sp.budget_line_id if sp else t.budget_line_id)
            ln = lines.get(line_id) if line_id else None
            code = (ln.account_code if ln else (sp.account_code if sp else t.account_code))
            rows.append([base["ref"], base["date"], base["payee"], base["via"], base["vendor"],
                         round(float(amount or 0), 2), code or '', _section_text(code),
                         _line_text(ln), base["method"], base["qbo_type"], base["qbo_id"],
                         base["files"], base["backup"], ''])
            if line_id:
                by_line_actual[line_id] = by_line_actual.get(line_id, 0.0) + float(amount or 0)
                if base["ref"]:
                    by_line_refs.setdefault(line_id, [])
                    if base["ref"] not in by_line_refs[line_id]:
                        by_line_refs[line_id].append(base["ref"])
            key = getattr(t, 'payee_id', None) or (parent.payee_id if parent is not None else None)
            if key:
                by_payee[key] = by_payee.get(key, 0.0) + float(amount or 0)

    # By line — the live budget the budget page shows.
    by_line = [["Account code", "Section", "Budget line", "Working", "Actual", "Variance",
                "References"]]
    budget = actuals.get_live_budget(pid)
    if budget is not None:
        blines = (BudgetLine.query.filter_by(budget_id=budget.id)
                  .order_by(BudgetLine.account_code, BudgetLine.sort_order, BudgetLine.id).all())
        live = _po_live_line_totals(budget, blines, pid)
        for ln in blines:
            if getattr(ln, 'line_tag', None) in ('header', 'spacer'):
                continue
            w = round(float(live.get(ln.id, 0) or 0), 2)
            a = round(by_line_actual.get(ln.id, 0.0), 2)
            if abs(w) < 0.005 and abs(a) < 0.005:
                continue
            by_line.append([ln.account_code, _section_text(ln.account_code), _line_text(ln),
                            w, a, round(w - a, 2), ", ".join(by_line_refs.get(ln.id, []))])

    payees_sheet = [["Payee", "Total paid", "W-9", "QuickBooks vendor id"]]
    for cid, total in sorted(by_payee.items(), key=lambda kv: -kv[1]):
        c = payees.get(cid)
        if c is None:
            continue
        try:
            w9 = (actuals.payee_w9_status(c) or {}).get('status') or ''
        except Exception:
            w9 = ''
        payees_sheet.append([c.name, round(total, 2), w9.replace('_', ' '),
                             getattr(c, 'qbo_vendor_id', None) or ''])

    refs_by_doc = {}
    for t in spend:
        for did in ([t.doc_upload_id] if t.doc_upload_id else []) + sorted(ev.get(t.id, ())):
            r = refs.get(t.id)
            if r:
                refs_by_doc.setdefault(did, [])
                if r not in refs_by_doc[did]:
                    refs_by_doc[did].append(r)
    docs_sheet = [["File name", "Type", "Vendor", "Amount", "Date", "References", "Dropbox path"]]
    for d in (DocUpload.query.filter(DocUpload.project_id == pid,
                                     db.or_(DocUpload.status.is_(None), DocUpload.status != 'deleted'))
              .order_by(DocUpload.doc_date, DocUpload.id).all()):
        docs_sheet.append([d.filed_filename or d.original_filename or f"Doc #{d.id}", d.category or '',
                           d.vendor or '', float(d.amount) if d.amount is not None else '',
                           d.doc_date.isoformat() if d.doc_date else '',
                           ", ".join(refs_by_doc.get(d.id, [])), d.filed_dropbox_path or ''])

    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    readme = [["FPBudget cost ledger"],
              [f"Project: {p.name if p else pid}"],
              [f"Project code: {(p.project_code if p else '') or 'not awarded yet'}"],
              [f"As of: {now}"],
              [""],
              ["Ledger: one row per expense, or per piece of a split expense (a split keeps one reference number)."],
              ["Amounts: expenses positive, credits and refunds negative."],
              ["By line: the live budget (Working when one exists). Working is the budget grid's live total."],
              ["Payees: totals paid on this project, with the W-9 state FPBudget knows and the QuickBooks vendor id."],
              ["Documents: every live document filed to the project and the reference numbers it backs."],
              ["Waivers: wrap-gate waivers, written when the project wraps."],
              ["Qualified: left blank for tax-credit tagging; not filled by FPBudget yet."]]

    return {"Ledger": [LEDGER_COLUMNS] + rows, "By line": by_line, "Payees": payees_sheet,
            "Documents": docs_sheet, "Waivers": [["Flag", "Item", "Reason", "By", "When"]],
            "Read me": readme}


def ledger_csv(sheets):
    buf = io.StringIO()
    w = csv.writer(buf)
    for row in sheets["Ledger"]:
        w.writerow(row)
    return buf.getvalue().encode('utf-8-sig')


def ledger_xlsx(sheets):
    from openpyxl import Workbook
    from openpyxl.styles import Font
    wb = Workbook()
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(title=name[:31])
        for r in rows:
            ws.append(r)
            # Plain values only: text that starts with '=' stays text, never
            # a formula (a vendor name or memo must not execute).
            for cell in ws[ws.max_row]:
                if isinstance(cell.value, str) and cell.value.startswith('='):
                    cell.data_type = 's'
        if name != 'Read me' and rows:
            for cell in ws[1]:
                cell.font = Font(bold=True)
            ws.freeze_panes = "A2"
        for col in ws.columns:
            width = max((len(str(c.value)) for c in col if c.value is not None), default=8)
            ws.column_dimensions[col[0].column_letter].width = min(max(10, width + 2), 60)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def ledger_filename(pid, ext):
    p = db.session.get(ProjectSheet, pid)
    stem = (p.project_code if p and p.project_code else f"project-{pid}")
    return f"{stem}_cost-ledger_{datetime.utcnow().strftime('%Y-%m-%d')}.{ext}"
