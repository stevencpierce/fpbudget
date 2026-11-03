"""Payee directory + Payee page (DOCUMENTS_HANDOFF Phase 1, 2026-11-01).

A Payee is anyone the company pays, person or vendor — the CrewMember
record promoted to a page. `/crew` now redirects here; the crew edit /
delete / support-contact / docs routes in routes/crew.py keep working
unchanged (the Payee page's Edit modal posts to /crew/<cid>/edit).

Routes (all admin + super-admin only, per the Oct 30 decision):
  GET  /payees                 directory (kind filter, search, tax year,
                               threshold in force, needs-attention toggle)
  GET  /payees.json            the directory's data
  GET  /payees/report.csv      the 1099 prep sheet for ?year=
  GET  /payees/<id>            the Payee page
  GET  /payees/<id>.json       the page's data
  POST /payees/<id>/update     legal name, address, kind, W-2 flag, notes,
                               Wrapbook id, W-9 received date. The qbo_*
                               fields are mirror-only and never accepted.
  POST /payees/<id>/docs/upload  (Phase 2) a document with NO project, owned
                               by the payee: runs the one ingest pipeline
                               (OCR, type, W-9 kind + received date, the
                               global duplicate rule) and files to
                               <OPS>/Crew Database/<Payee>/<Year>/.

Numbers come from ONE helper, actuals.paid_by_payee(); the W-9 answer from
ONE rule, actuals.payee_w9_status(); the 1099 chip from actuals.flag_1099().
See routes/__init__.py for the module pattern (plain @app.route on the same
app object, imported at the bottom of app.py).
"""
import csv
import io
import json
from datetime import date, datetime

from flask import (render_template, request, jsonify, flash, redirect,
                   url_for, abort, Response)
from flask_login import login_required, current_user
from sqlalchemy import func, or_

from app import (app, admin_hub_required, _log_activity, _line_budget_total, _RATE_TYPE_LABELS,
                 _ingest_document_bytes, _validate_upload_file)
from models import (db, CrewMember, CrewAssignment, Budget, BudgetLine, ProjectSheet,
                    DocUpload, Transaction, VendorAlias, ScheduleDay, ProjectCrewMember,
                    QBOVendor, QBOConnection)
import actuals
from actuals import (paid_by_payee, w9_status_for, w9_fallback_dates, flag_1099,
                     qbo_matches_w9, threshold_for_year, reporting_threshold_rows,
                     canon_vendor, doc_class, doc_owner_id, expiry_state,
                     payee_w9_status, link_vendor_alias_to_payee, unlink_vendor_alias,
                     TAX_FORM_KIND_LABELS, PAYEE_KINDS, SHARED_LINE,
                     W9_ON_FILE, W9_MISSING, W9_NOT_REQUIRED, W9_UNDER_THRESHOLD)
import qbo_sync


KIND_LABELS = {'person': 'Person', 'loan_out': 'Loan-out', 'vendor': 'Vendor'}

# Paperwork groups on the Payee page, in display order. 'employee_vendor_doc'
# holds IDs and onboarding forms; its sub-type lives in DocUpload.doc_subtype
# (Phase 2), tax forms carry tax_form_kind + received_on, COIs expires_on.
PAPERWORK_GROUPS = (
    ('tax_form',            'Tax forms'),
    ('insurance',           'Insurance'),
    ('contract',            'Contracts'),
    ('employee_vendor_doc', 'Identification'),
    ('release',             'Releases'),
    ('legal',               'Legal'),
    ('misc',                'Other'),
)

COI_WARN_DAYS = actuals.COI_WARN_DAYS


def _doc_owner_clause(cm_id):
    """SQL for "this document belongs to payee cm_id": the Phase 2 owner
    column OR the alias column (rows the 0023 data step has not touched)."""
    return or_(DocUpload.owner_payee_id == cm_id, DocUpload.crew_member_id == cm_id)

# Fields POST /payees/<id>/update (and the crew edit form) may write. The
# QuickBooks mirror fields are deliberately absent: QuickBooks → FPBudget only.
PAYEE_TEXT_FIELDS = ('legal_name', 'address_line1', 'address_line2', 'city',
                     'state', 'postal_code', 'wrapbook_worker_id', 'notes')
PAYEE_FIELDS = PAYEE_TEXT_FIELDS + ('kind', 'is_w2_employee', 'w9_received_on')
# Once a Payee is linked to a QuickBooks vendor these are QuickBooks's
# (Phase 3): the edit form shows them read-only and a stale form cannot
# write them here either (the mirror would overwrite them on the next sync).
QBO_OWNED_FIELDS = frozenset(qbo_sync.MIRRORED_PAYEE_FIELDS)


# ── small formatting helpers ───────────────────────────────────────────────

@app.template_filter("money0")
def _money0_filter(v):
    """$14,300 — whole dollars, used by the payee templates."""
    return _fmt_money0(v)


@app.template_filter("fmtdate")
def _fmtdate_filter(v):
    """'Jan 12, 2026' from a date or ISO string; '' when empty."""
    return _fmt_date(v)


def _iso(d):
    return d.isoformat() if d else None


def _fmt_date(d):
    """'Jan 12, 2026' for a date / ISO string / None."""
    if not d:
        return ''
    if isinstance(d, str):
        try:
            d = date.fromisoformat(d[:10])
        except ValueError:
            return d
    if isinstance(d, datetime):
        d = d.date()
    return f"{d.strftime('%b')} {d.day}, {d.year}"


def _fmt_money0(v):
    try:
        v = float(v or 0)
    except (TypeError, ValueError):
        v = 0.0
    sign = '-' if v < 0 else ''
    return f"{sign}${abs(v):,.0f}"


def _kind_of(cm):
    k = getattr(cm, 'kind', None) or ('vendor' if getattr(cm, 'is_vendor', False) else 'person')
    return k if k in PAYEE_KINDS else 'person'


def _address(cm):
    parts = [cm.address_line1, cm.address_line2]
    city_line = " ".join(x for x in (
        f"{cm.city}," if cm.city and (cm.state or cm.postal_code) else (cm.city or ''),
        cm.state or '', cm.postal_code or '') if x).strip()
    parts.append(city_line)
    return ", ".join(p.strip() for p in parts if p and p.strip())


def _w9_cell(status):
    """Directory W-9 column: {state, text} from a w9_status_for() result."""
    s = status["status"]
    if s == W9_ON_FILE:
        return {"state": "ok", "text": _fmt_date(status["date"]) or "On file"}
    if s == W9_NOT_REQUIRED:
        return {"state": "muted", "text": "Not required"}
    if s == W9_UNDER_THRESHOLD:
        return {"state": "muted", "text": "Under threshold"}
    return {"state": "bad", "text": "Missing"}


def _qbo_cell(cm, w9, reportable):
    """Directory QuickBooks column: {state, text}."""
    if getattr(cm, 'is_w2_employee', False):
        return {"state": "muted", "text": "Wrapbook"}
    if not cm.qbo_vendor_id:
        return ({"state": "warn", "text": "Not in QuickBooks"} if reportable
                else {"state": "muted", "text": "Not linked"})
    if cm.qbo_vendor_1099 is True:
        return {"state": "ok", "text": "1099 on"}
    if reportable:
        return {"state": "warn", "text": "1099 off · mismatch"}
    return {"state": "ok", "text": "1099 off"}


def _coi_state(doc, today=None):
    """COI expiry chip from DocUpload.expires_on (Phase 2): red when past,
    amber within COI_WARN_DAYS, green otherwise; a COI with no expiry date
    shows the date it was issued, muted, and the inbox asks for the date."""
    today = today or date.today()
    exp = getattr(doc, 'expires_on', None)
    st = expiry_state(exp, today, COI_WARN_DAYS)
    if st == 'bad':
        return {"state": "bad", "text": f"expired {exp.strftime('%b')} {exp.day}, {exp.year}",
                "date": _iso(exp)}
    if st == 'warn':
        return {"state": "warn", "text": f"expires {exp.strftime('%b')} {exp.day}",
                "date": _iso(exp)}
    if st == 'ok':
        return {"state": "ok", "text": f"expires {exp.strftime('%b')} {exp.year}", "date": _iso(exp)}
    d = doc.doc_date or (doc.uploaded_at.date() if doc.uploaded_at else None)
    return {"state": "muted", "text": (f"no expiry · on file {_fmt_date(d)}" if d else "no expiry"),
            "date": _iso(d)}


# ── bulk lookups ───────────────────────────────────────────────────────────

def _projects_by_payee(payee_ids=None):
    """{crew_member_id: {project_id: project_name}} from CrewAssignment,
    BudgetLine.assigned_crew_id and DocUpload.crew_member_id — the same
    three sources crew_profile_json unions. Three bulk queries."""
    out = {}

    def _add(cid, pid, pname):
        if cid and pid:
            out.setdefault(cid, {})[pid] = pname

    q1 = (db.session.query(CrewAssignment.crew_member_id, ProjectSheet.id, ProjectSheet.name)
          .join(BudgetLine, BudgetLine.id == CrewAssignment.budget_line_id)
          .join(Budget, Budget.id == BudgetLine.budget_id)
          .join(ProjectSheet, ProjectSheet.id == Budget.project_id)
          .filter(CrewAssignment.crew_member_id.isnot(None)))
    q2 = (db.session.query(BudgetLine.assigned_crew_id, ProjectSheet.id, ProjectSheet.name)
          .join(Budget, Budget.id == BudgetLine.budget_id)
          .join(ProjectSheet, ProjectSheet.id == Budget.project_id)
          .filter(BudgetLine.assigned_crew_id.isnot(None)))
    _owner_col = func.coalesce(DocUpload.owner_payee_id, DocUpload.crew_member_id)
    q3 = (db.session.query(_owner_col, ProjectSheet.id, ProjectSheet.name)
          .join(ProjectSheet, ProjectSheet.id == DocUpload.project_id)
          .filter(or_(DocUpload.owner_payee_id.isnot(None), DocUpload.crew_member_id.isnot(None)),
                  DocUpload.status.notin_(('deleted', 'duplicate'))))
    if payee_ids is not None:
        ids = list(payee_ids) or [0]
        q1 = q1.filter(CrewAssignment.crew_member_id.in_(ids))
        q2 = q2.filter(BudgetLine.assigned_crew_id.in_(ids))
        q3 = q3.filter(or_(DocUpload.owner_payee_id.in_(ids), DocUpload.crew_member_id.in_(ids)))
    for q in (q1, q2, q3):
        for cid, pid, pname in q.distinct().all():
            _add(cid, pid, pname)
    return out


def _years_available(threshold_rows):
    """Tax-year picker: years present in transactions ∪ seeded threshold
    years ∪ the current year, newest first."""
    years = {date.today().year} | set(threshold_rows.keys())
    try:
        for (y,) in (db.session.query(func.substr(Transaction.txn_date, 1, 4))
                     .filter(Transaction.txn_date.isnot(None)).distinct().all()):
            if y and str(y).isdigit():
                years.add(int(y))
    except Exception:
        pass
    return sorted(years, reverse=True)


def _year_arg(default=None):
    y = request.args.get('year', '')
    try:
        return int(y) if y else (default or date.today().year)
    except ValueError:
        return default or date.today().year


# ── directory ──────────────────────────────────────────────────────────────

def _directory_data(year):
    thr_rows = reporting_threshold_rows()
    threshold = threshold_for_year(year, thr_rows)
    members = CrewMember.query.order_by(func.lower(CrewMember.name)).all()
    name_by_id = {m.id: m.name for m in members}
    paid = paid_by_payee()                       # every payee, every year
    projects = _projects_by_payee()
    fallback_w9 = w9_fallback_dates()
    coi_by_payee = {}
    for d in (DocUpload.query
              .filter(or_(DocUpload.owner_payee_id.isnot(None), DocUpload.crew_member_id.isnot(None)),
                      DocUpload.category == 'insurance',
                      DocUpload.status.notin_(('deleted', 'duplicate')))
              .order_by(DocUpload.expires_on.desc().nullslast(),
                        DocUpload.doc_date.desc().nullslast(), DocUpload.id.desc()).all()):
        coi_by_payee.setdefault(doc_owner_id(d), d)

    rows, counts = [], {"all": 0, "person": 0, "loan_out": 0, "vendor": 0,
                        "needs_attention": 0}
    for m in members:
        kind = _kind_of(m)
        by_year = paid.get(m.id, {})
        this = by_year.get(year, {"card": 0.0, "non_card": 0.0, "total": 0.0, "n": 0})
        all_time = round(sum(b["total"] for b in by_year.values()), 2)
        w9 = w9_status_for(m, non_card=this["non_card"], threshold=threshold,
                           fallback_date=fallback_w9.get(m.id))
        f = flag_1099(this["non_card"], threshold, w9["status"],
                      m.qbo_vendor_id, m.qbo_vendor_1099, card=this["card"])
        w9_cell = _w9_cell(w9)
        if w9["status"] in (W9_MISSING, W9_UNDER_THRESHOLD):
            if f["label"] == "Card only":
                w9_cell = {"state": "muted", "text": "Card only"}
            elif not this["total"] and not all_time:
                w9_cell = {"state": "muted", "text": "—"}
        coi = coi_by_payee.get(m.id)
        lov = name_by_id.get(m.loan_out_vendor_id) if m.loan_out_vendor_id else None
        row = {
            "id": m.id, "name": m.name, "legal_name": m.legal_name,
            "company": m.company, "email": m.email,
            "kind": kind, "kind_label": KIND_LABELS[kind],
            "is_w2_employee": bool(m.is_w2_employee), "active": bool(m.active),
            "department": m.department or '',
            "pays_as": lov, "loan_out_vendor_id": m.loan_out_vendor_id,
            "projects": len(projects.get(m.id, {})),
            "paid_year": this["total"], "paid_year_card": this["card"],
            "paid_year_non_card": this["non_card"],
            "paid_all_time": all_time,
            "w9": w9, "w9_cell": w9_cell,
            "qbo": _qbo_cell(m, w9, f["reportable"]),
            "qbo_vendor_id": m.qbo_vendor_id, "qbo_vendor_1099": m.qbo_vendor_1099,
            "coi": _coi_state(coi) if coi else None,
            "flag": f, "needs_attention": f["needs_attention"],
            "url": url_for('payee_page', payee_id=m.id),
        }
        rows.append(row)
        counts["all"] += 1
        counts[kind] += 1
        if f["needs_attention"]:
            counts["needs_attention"] += 1

    shared = paid.get(SHARED_LINE, {}).get(year)
    # QuickBooks vendor mirror state for the header chip (super-admin sees
    # the Link screen; admins see the state).
    qbo = None
    try:
        conn = QBOConnection.query.first()
        linked_ids = {m.qbo_vendor_id for m in members if m.qbo_vendor_id}
        n_vendors = QBOVendor.query.count()
        n_unlinked = sum(1 for (vid,) in db.session.query(QBOVendor.qbo_vendor_id).all()
                         if vid not in linked_ids)
        qbo = {"connected": bool(conn and conn.refresh_token),
               "last_sync": (conn.vendor_cdc_since.isoformat() if conn and conn.vendor_cdc_since else None),
               "vendors": n_vendors, "unlinked": n_unlinked}
    except Exception:
        qbo = None
    return {
        "year": year, "threshold": threshold,
        "years": _years_available(thr_rows),
        "rows": rows, "counts": counts,
        "shared_line_year": shared,
        "qbo": qbo,
    }


@app.route("/payees")
@login_required
@admin_hub_required
def payees_directory():
    data = _directory_data(_year_arg())
    return render_template("payees.html", d=data, KIND_LABELS=KIND_LABELS)


@app.route("/payees.json")
@login_required
@admin_hub_required
def payees_directory_json():
    return jsonify({"ok": True, **_directory_data(_year_arg())})


@app.route("/payees/report.csv")
@login_required
@admin_hub_required
def payees_report_csv():
    """The 1099 prep sheet for one tax year: every payee paid that year,
    non-card first. Same rows and rules as the directory."""
    year = _year_arg()
    data = _directory_data(year)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Name", "Legal name", "Kind", "W-2 employee", "Department", "Email",
                "Address", f"Paid {year} card", f"Paid {year} non-card",
                f"Paid {year} total", "Threshold", "W-9 status", "W-9 received",
                "QuickBooks vendor id", "QuickBooks 1099", "1099 status",
                "Needs attention"])
    members = {m.id: m for m in CrewMember.query.all()}
    rows = [r for r in data["rows"] if abs(r["paid_year"]) > 0.004]
    rows.sort(key=lambda r: (-r["paid_year_non_card"], r["name"].lower()))
    for r in rows:
        m = members.get(r["id"])
        qbo1099 = '' if m.qbo_vendor_1099 is None else ('yes' if m.qbo_vendor_1099 else 'no')
        w.writerow([r["name"], r["legal_name"] or '', r["kind_label"],
                    'yes' if r["is_w2_employee"] else 'no', r["department"],
                    r["email"] or '', _address(m) if m else '',
                    f"{r['paid_year_card']:.2f}", f"{r['paid_year_non_card']:.2f}",
                    f"{r['paid_year']:.2f}", f"{data['threshold']:.2f}",
                    r["w9"]["status"], r["w9"]["date"] or '',
                    m.qbo_vendor_id or '', qbo1099, r["flag"]["label"],
                    'yes' if r["needs_attention"] else 'no'])
    out = buf.getvalue()
    return Response(out, mimetype="text/csv",
                    headers={"Content-Disposition":
                             f'attachment; filename="payees_1099_prep_{year}.csv"'})


# ── Payee page ─────────────────────────────────────────────────────────────
# URL parameter is <int:payee_id>, never <int:pid>: app.enforce_project_access
# treats every `pid` view arg as a project id and would 403 the page.

def _payee_page_data(cm, year):
    thr_rows = reporting_threshold_rows()
    threshold = threshold_for_year(year, thr_rows)
    kind = _kind_of(cm)

    # Money: one helper, two groupings.
    by_year = paid_by_payee(payee_id=cm.id).get(cm.id, {})
    by_project = paid_by_payee(payee_id=cm.id, by='project').get(cm.id, {})
    this = by_year.get(year, {"card": 0.0, "non_card": 0.0, "total": 0.0, "n": 0})
    all_time = round(sum(b["total"] for b in by_year.values()), 2)
    dated_years = sorted(y for y in by_year if y is not None)
    fallback = (w9_fallback_dates([cm.id]).get(cm.id)
                if not cm.w9_received_on else None)
    w9 = w9_status_for(cm, non_card=this["non_card"], threshold=threshold,
                       fallback_date=fallback)

    paid_rows = []
    for y in sorted((y for y in by_year if y is not None), reverse=True):
        b = by_year[y]
        th = threshold_for_year(y, thr_rows)
        wy = w9_status_for(cm, non_card=b["non_card"], threshold=th, fallback_date=fallback)
        paid_rows.append({"year": y, **b, "threshold": th,
                          "flag": flag_1099(b["non_card"], th, wy["status"],
                                            cm.qbo_vendor_id, cm.qbo_vendor_1099,
                                            card=b["card"])})
    if None in by_year:
        b = by_year[None]
        paid_rows.append({"year": None, **b, "threshold": None,
                          "flag": {"level": "grey", "label": "Undated", "reportable": False,
                                   "needs_attention": False}})

    # Projects: assignments + line-level assignment, per project, with role
    # names, schedule dates, budgeted (current working-else-estimated
    # budget) and paid (attributed through the same helper).
    projects = {}
    for pid_, pname in _projects_by_payee([cm.id]).get(cm.id, {}).items():
        projects[pid_] = {"project_id": pid_, "project_name": pname}
    ps_by_id = {p.id: p for p in ProjectSheet.query.filter(
        ProjectSheet.id.in_(list(projects) or [0])).all()}
    roles_by_proj, budgeted_by_proj = {}, {}
    for pid_ in projects:
        try:
            b = (actuals.get_current_working_budget(pid_)
                 or actuals.get_current_estimated_budget(pid_))
        except Exception:
            b = None
        if not b:
            continue
        lines = BudgetLine.query.filter_by(budget_id=b.id).all()
        line_by_id = {ln.id: ln for ln in lines}
        theirs = set()
        if line_by_id:
            for ca in CrewAssignment.query.filter(
                    CrewAssignment.crew_member_id == cm.id,
                    CrewAssignment.budget_line_id.in_(list(line_by_id))).all():
                theirs.add(ca.budget_line_id)
        for ln in lines:
            if ln.assigned_crew_id == cm.id:
                theirs.add(ln.id)
        roles, seen, tot = [], set(), 0.0
        for lid in theirs:
            ln = line_by_id.get(lid)
            if not ln:
                continue
            tot += _line_budget_total(ln)
            r = (ln.description or ln.account_name or '').strip()
            if r and r not in seen:
                seen.add(r); roles.append(r)
        roles_by_proj[pid_] = roles
        budgeted_by_proj[pid_] = round(tot, 2)
    dates_by_proj = {}
    for pid_, dmin, dmax in (db.session.query(Budget.project_id,
                                              func.min(ScheduleDay.date),
                                              func.max(ScheduleDay.date))
                             .join(Budget, Budget.id == ScheduleDay.budget_id)
                             .filter(ScheduleDay.crew_member_id == cm.id)
                             .group_by(Budget.project_id).all()):
        dates_by_proj[pid_] = (dmin, dmax)
    pcm_by_proj = {p.project_id: p for p in
                   ProjectCrewMember.query.filter_by(crew_member_id=cm.id).all()}
    project_rows = []
    for pid_, p in projects.items():
        ps = ps_by_id.get(pid_)
        dmin, dmax = dates_by_proj.get(pid_, (None, None))
        if isinstance(dmin, str):
            dmin = date.fromisoformat(dmin[:10])
        if isinstance(dmax, str):
            dmax = date.fromisoformat(dmax[:10])
        if dmin and dmax:
            if dmin == dmax:
                dates = f"{dmin.strftime('%b')} {dmin.day}, {dmin.year}"
            elif dmin.year == dmax.year and dmin.month == dmax.month:
                dates = f"{dmin.strftime('%b')} {dmin.day}–{dmax.day}, {dmin.year}"
            else:
                dates = f"{dmin.strftime('%b')} {dmin.day} – {dmax.strftime('%b')} {dmax.day}, {dmax.year}"
        else:
            dates = ''
        pcm = pcm_by_proj.get(pid_)
        paid_p = by_project.get(pid_, {"total": 0.0, "card": 0.0, "non_card": 0.0, "n": 0})
        project_rows.append({
            **p, "status": (ps.status if ps else ''),
            "roles": roles_by_proj.get(pid_, []),
            "dates": dates, "date_from": _iso(dmin), "date_to": _iso(dmax),
            "budgeted": budgeted_by_proj.get(pid_, 0.0),
            "paid": paid_p["total"],
            "employment_type": pcm.employment_type if pcm else None,
            "union_status": pcm.union_status if pcm else None,
            "url": url_for('project_budget_redirect', pid=pid_) + "#people",
            "_sort": (dmax or date.min, pid_),
        })
    project_rows.sort(key=lambda r: r["_sort"], reverse=True)
    for r in project_rows:
        r.pop("_sort", None)
    active_count = sum(1 for r in project_rows if r["status"] == 'active')

    # Documents: every non-deleted doc this payee owns, split by class. A
    # pending duplicate (a second W-9 awaiting Replace / Keep both) is
    # listed with its flag so the page can offer the resolution.
    docs = (DocUpload.query.filter(_doc_owner_clause(cm.id),
                                   DocUpload.status.notin_(('deleted', 'duplicate')))
            .order_by(DocUpload.doc_date.desc().nullslast(),
                      DocUpload.uploaded_at.desc().nullslast(), DocUpload.id.desc()).all())
    pname_by_id = {p.id: p.name for p in ProjectSheet.query.filter(
        ProjectSheet.id.in_({d.project_id for d in docs if d.project_id} or {0})).all()}
    doc_ids = [d.id for d in docs]
    # The charge an evidence doc matched: a non-doc-born transaction that
    # points at it (bank / QBO / CSV row), else its own activated row.
    charge_by_doc = {}
    if doc_ids:
        for t in (Transaction.query.filter(Transaction.doc_upload_id.in_(doc_ids),
                                           Transaction.not_project_expense == False)   # noqa: E712
                  .order_by(Transaction.id).all()):
            cur = charge_by_doc.get(t.doc_upload_id)
            if cur is None or (cur.source == 'doc_upload' and t.source != 'doc_upload'):
                charge_by_doc[t.doc_upload_id] = t

    def _charge_cell(t):
        if t is None:
            return {"state": "muted", "text": "Unmatched"}
        if t.source == 'doc_upload':
            if getattr(t, 'activated_at', None):
                return {"state": "ok", "text": (t.no_charge_reason or "Logged")[:40]}
            return {"state": "muted", "text": "Awaiting charge"}
        how = (t.payment_method or '').replace('_', ' ').title() if t.payment_method else \
              (f"Card •{t.card_last4}" if t.card_last4 else (t.source or 'Charge').replace('_', ' ').title())
        when = _fmt_date(t.txn_date)
        return {"state": "ok", "text": f"{how} {when}".strip(), "txn_id": t.id}

    evidence, paperwork = [], {k: [] for k, _ in PAPERWORK_GROUPS}
    for d in docs:
        cat = (d.category or '').lower()
        _shown_date = (d.received_on if cat == 'tax_form' else None) or d.doc_date \
            or (d.uploaded_at.date() if d.uploaded_at else None)
        _subtype = d.doc_subtype
        if cat == 'tax_form':
            _subtype = TAX_FORM_KIND_LABELS.get(d.tax_form_kind or '', d.doc_subtype)
        entry = {
            "id": d.id, "category": cat,
            "name": d.filed_filename or d.original_filename or f"Doc #{d.id}",
            "subtype": _subtype,
            "tax_form_kind": d.tax_form_kind,
            "received_on": _iso(d.received_on),
            "expires_on": _iso(d.expires_on),
            "expiry_state": expiry_state(d.expires_on),
            "date": _iso(_shown_date),
            "date_label": _fmt_date(_shown_date),
            "project_id": d.project_id, "project_name": pname_by_id.get(d.project_id),
            "amount": float(d.amount) if d.amount is not None else None,
            "synced": bool(getattr(d, 'crew_dbx_path', None)),
            "status": d.status,
            "dup_pending": bool(d.is_duplicate) and d.status != 'duplicate',
            "duplicate_of_id": d.duplicate_of_id,
            "url": f"/docs/upload/{d.id}/raw",
            "editor_url": (url_for('doc_editor_page', pid=d.project_id, uid=d.id)
                           if d.project_id else url_for('doc_editor_page_global', uid=d.id)),
        }
        if doc_class(cat) == 'evidence' and cat:
            entry["charge"] = _charge_cell(charge_by_doc.get(d.id))
            evidence.append(entry)
        elif cat in paperwork:
            if cat == 'insurance':
                entry["coi"] = _coi_state(d)
            paperwork[cat].append(entry)
        else:
            paperwork['misc'].append(entry)
    paperwork_groups = [{"key": k, "label": lbl, "docs": paperwork[k]}
                        for k, lbl in PAPERWORK_GROUPS if paperwork[k]]
    unsynced = sum(1 for g in paperwork_groups for e in g["docs"] if not e["synced"])

    # Trash: this payee's soft-deleted documents (a replaced W-9 lands here),
    # newest first, restorable from the page — a project-less document is
    # in no project's Trash view, so this is its only way back.
    trashed = []
    for d in (DocUpload.query.filter(_doc_owner_clause(cm.id), DocUpload.status == 'deleted')
              .order_by(DocUpload.deleted_at.desc().nullslast(), DocUpload.id.desc()).limit(25).all()):
        try:
            _meta = json.loads(d.trash_meta) if d.trash_meta else {}
        except Exception:
            _meta = {}
        trashed.append({
            "id": d.id, "category": (d.category or '').lower(),
            "name": d.filed_filename or d.original_filename or f"Doc #{d.id}",
            "subtype": TAX_FORM_KIND_LABELS.get(d.tax_form_kind or '', d.doc_subtype)
                       if (d.category or '') == 'tax_form' else d.doc_subtype,
            "deleted_at": d.deleted_at.isoformat() if d.deleted_at else None,
            "deleted_label": _fmt_date(d.deleted_at) if d.deleted_at else '',
            "superseded_by_id": _meta.get('superseded_by_id'),
            "project_name": pname_by_id.get(d.project_id) if d.project_id else None,
        })

    # Vendor names: alias rows LINKED to this payee (VendorAlias.payee_id,
    # Phase 3 — every charge from that string attributes here), then rows
    # whose clean or raw name merely matches by text (link them to make it
    # stick).
    keys = {canon_vendor(x) for x in (cm.name, cm.legal_name, cm.company) if x}
    keys.discard('')
    aliases, seen_alias = [], set()
    for a in VendorAlias.query.filter(VendorAlias.payee_id == cm.id).order_by(VendorAlias.clean_name).all():
        seen_alias.add(a.id)
        aliases.append({"id": a.id, "raw": a.raw_canonical, "clean": a.clean_name,
                        "project_id": a.project_id, "confirm_count": a.confirm_count,
                        "linked": True})
    if keys:
        for a in VendorAlias.query.filter(VendorAlias.payee_id.is_(None)).all():
            if a.id in seen_alias:
                continue
            if canon_vendor(a.clean_name) in keys or (a.raw_canonical or '') in keys:
                aliases.append({"id": a.id, "raw": a.raw_canonical, "clean": a.clean_name,
                                "project_id": a.project_id,
                                "confirm_count": a.confirm_count, "linked": False})
    # Raw vendor strings on this payee's transactions that no alias covers
    # yet (so they can be linked in one click).
    raw_strings = {}
    try:
        for (v_,) in (db.session.query(Transaction.vendor)
                      .filter(Transaction.payee_id == cm.id, Transaction.vendor.isnot(None))
                      .distinct().all()):
            cv_ = canon_vendor(v_)
            if cv_ and cv_ not in {a["raw"] for a in aliases}:
                raw_strings.setdefault(cv_, (v_ or '').strip())
    except Exception:
        pass
    qbo_row = QBOVendor.query.get(cm.qbo_vendor_id) if cm.qbo_vendor_id else None

    lov = CrewMember.query.get(cm.loan_out_vendor_id) if cm.loan_out_vendor_id else None
    loan_outs_through = ([{"id": p.id, "name": p.name} for p in
                          CrewMember.query.filter_by(loan_out_vendor_id=cm.id)
                          .order_by(CrewMember.name).all()]
                         if kind in ('vendor', 'loan_out') else [])

    return {
        "year": year, "threshold": threshold, "years": _years_available(thr_rows),
        "identity": {
            "id": cm.id, "name": cm.name, "kind": kind, "kind_label": KIND_LABELS[kind],
            "department": cm.department, "company": cm.company,
            "legal_name": cm.legal_name, "address": _address(cm),
            "address_line1": cm.address_line1, "address_line2": cm.address_line2,
            "city": cm.city, "state": cm.state, "postal_code": cm.postal_code,
            "email": cm.email, "phone": cm.phone, "active": bool(cm.active),
            "default_rate": float(cm.default_rate) if cm.default_rate is not None else None,
            "default_rate_type": cm.default_rate_type,
            "default_rate_label": _RATE_TYPE_LABELS.get(cm.default_rate_type, cm.default_rate_type or ''),
            "default_fringe": cm.default_fringe,
            "default_agent_pct": float(cm.default_agent_pct or 0) * 100,
            "is_w2_employee": bool(cm.is_w2_employee),
            "wrapbook_worker_id": cm.wrapbook_worker_id,
            "notes": cm.notes,
            "loan_out_vendor": ({"id": lov.id, "name": lov.name} if lov else None),
            "loan_outs_through": loan_outs_through,
            "required_docs": cm.required_docs or '',
        },
        "stats": {
            "paid_year": this["total"], "paid_year_card": this["card"],
            "paid_year_non_card": this["non_card"],
            "all_time": all_time,
            "since": (dated_years[0] if dated_years else None),
            "projects": len(project_rows), "projects_active": active_count,
            "w9": w9,
        },
        "paid_by_year": paid_rows,
        "projects": project_rows,
        "evidence": evidence,
        "paperwork": paperwork_groups,
        "paperwork_unsynced": unsynced,
        "trashed": trashed,
        "quickbooks": {
            "linked": bool(cm.qbo_vendor_id),
            "vendor_id": cm.qbo_vendor_id,
            "vendor_name": (qbo_row.display_name if qbo_row and qbo_row.display_name
                            else (cm.legal_name or cm.name)),
            "vendor_1099": cm.qbo_vendor_1099,
            "vendor_active": (qbo_row.active if qbo_row else None),
            "matches_w9": qbo_matches_w9(w9["status"], cm.qbo_vendor_id, cm.qbo_vendor_1099),
            "synced_at": cm.qbo_synced_at.isoformat() if cm.qbo_synced_at else None,
            "synced_label": _fmt_date(cm.qbo_synced_at) if cm.qbo_synced_at else '',
            "origin": cm.qbo_origin,
            "can_create": (not cm.qbo_vendor_id and not cm.is_w2_employee
                           and bool(QBOConnection.query.first())),
            "owned_fields": sorted(QBO_OWNED_FIELDS) if cm.qbo_vendor_id else [],
        },
        "vendor_names": aliases,
        "vendor_strings_unlinked": [{"canon": k, "raw": v} for k, v in sorted(raw_strings.items())],
        "edit_url": url_for('crew_edit', cid=cm.id),
        "qbo_create_url": url_for('payee_qbo_create', payee_id=cm.id),
        "upload_url": url_for('payee_docs_upload', payee_id=cm.id),
        "pending_duplicates": [e for g in paperwork_groups for e in g["docs"] if e["dup_pending"]],
    }


@app.route("/payees/<int:payee_id>")
@login_required
@admin_hub_required
def payee_page(payee_id):
    cm = CrewMember.query.get_or_404(payee_id)
    data = _payee_page_data(cm, _year_arg())
    return render_template("payee.html", d=data, cm=cm, KIND_LABELS=KIND_LABELS)


@app.route("/payees/<int:payee_id>.json")
@login_required
@admin_hub_required
def payee_page_json(payee_id):
    cm = CrewMember.query.get_or_404(payee_id)
    return jsonify({"ok": True, **_payee_page_data(cm, _year_arg())})


# ── update ─────────────────────────────────────────────────────────────────

def _parse_bool(v):
    if isinstance(v, bool):
        return v
    return str(v or '').strip().lower() in ('1', 'true', 'yes', 'on')


def apply_payee_fields(cm, src):
    """Write the Payee fields present in `src` (a form or JSON dict) onto a
    CrewMember. Shared by POST /payees/<id>/update and the crew edit form.
    Returns {field: [before, after]} for the fields that changed, or raises
    ValueError with a user-facing message. Never touches the qbo_* mirror."""
    changes = {}

    def _set(field, value):
        before = getattr(cm, field, None)
        if isinstance(before, date):
            before = before.isoformat()
        after = value.isoformat() if isinstance(value, date) else value
        if before != after:
            changes[field] = [before, after]
            setattr(cm, field, value)

    _qbo_owned = bool(getattr(cm, 'qbo_vendor_id', None))
    for f in PAYEE_TEXT_FIELDS:
        if f in src:
            if _qbo_owned and f in QBO_OWNED_FIELDS:
                continue        # QuickBooks owns it (Phase 3)
            v = (src.get(f) or '')
            v = str(v).strip() or None
            _set(f, v)
    if 'kind' in src:
        k = str(src.get('kind') or '').strip().lower()
        if k not in PAYEE_KINDS:
            raise ValueError(f"Unknown payee kind '{k}'. Use person, loan_out or vendor.")
        _set('kind', k)
        _set('is_vendor', k in ('vendor', 'loan_out'))
    if 'is_w2_employee' in src:
        # A form sends hidden "0" then the checkbox "1" when ticked: the
        # LAST value wins (JSON bodies have a single value).
        vals = src.getlist('is_w2_employee') if hasattr(src, 'getlist') else [src.get('is_w2_employee')]
        _set('is_w2_employee', _parse_bool(vals[-1] if vals else None))
    if 'w9_received_on' in src:
        raw = str(src.get('w9_received_on') or '').strip()
        if raw:
            try:
                _set('w9_received_on', date.fromisoformat(raw[:10]))
            except ValueError:
                raise ValueError("W-9 received date must be YYYY-MM-DD.")
        else:
            _set('w9_received_on', None)
            _set('w9_doc_upload_id', None)
    return changes


@app.route("/payees/<int:payee_id>/update", methods=["POST"])
@login_required
@admin_hub_required
def payee_update(payee_id):
    cm = CrewMember.query.get_or_404(payee_id)
    src = request.get_json(silent=True) if request.is_json else request.form
    src = src or {}
    try:
        changes = apply_payee_fields(cm, src)
    except ValueError as e:
        if request.is_json:
            return jsonify({"ok": False, "error": str(e)}), 400
        flash(str(e), "error")
        return redirect(url_for('payee_page', payee_id=payee_id))
    if changes:
        db.session.commit()
        _log_activity(action='update', entity_type='payee', entity_id=cm.id,
                      entity_label=cm.name, field_changes=changes,
                      note="Payee record updated")
    if request.is_json or request.args.get('fmt') == 'json':
        return jsonify({"ok": True, "id": cm.id, "changed": sorted(changes)})
    flash(f"Updated {cm.name}.", "success")
    return redirect(url_for('payee_page', payee_id=payee_id))


# ── Upload from the Payee page (DOCUMENTS_HANDOFF Phase 2) ─────────────────

@app.route("/payees/<int:payee_id>/docs/upload", methods=["POST"])
@login_required
@admin_hub_required
def payee_docs_upload(payee_id):
    """Upload a document that belongs to this payee and to NO project.

    Runs the one ingest pipeline (app._ingest_document_bytes) in payee mode:
    OCR, type detection (a W-9 / W-8 becomes tax_form with its kind and the
    received date; anything else an employee/vendor document unless the
    analyzer is sure it is a COI / contract / release), the global
    per-payee duplicate rule (a second W-9 is flagged for Replace / Keep
    both), the W-9 date rule, and filing straight into
    <OPS>/Crew Database/<Payee>/<Year>/. No Transaction is ever created.
    Returns the same JSON shape as the project upload (201 filed, 202 needs
    review, 4xx/5xx {error})."""
    cm = CrewMember.query.get_or_404(payee_id)
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "No file provided"}), 400
    _err = _validate_upload_file(f, 'document')
    if _err:
        return jsonify({"error": _err}), 400
    data = f.read()
    payload, code = _ingest_document_bytes(None, None, f.filename, f.content_type, data,
                                           uploader=current_user, owner_payee=cm)
    if code in (201, 202):
        try:
            _log_activity(action='create', entity_type='payee_doc', entity_id=payload.get('upload_id'),
                          entity_label=(payload.get('new_filename') or f.filename)[:300],
                          project_id=None, before=None,
                          after={'owner_payee_id': cm.id, 'category': payload.get('category'),
                                 'tax_form_kind': payload.get('tax_form_kind'),
                                 'status': payload.get('status')},
                          note=f'Uploaded "{f.filename}" to {cm.name} (no project)')
        except Exception:
            pass
    payload = dict(payload)
    payload["payee_id"] = cm.id
    return jsonify(payload), code


# ── Create in QuickBooks (DOCUMENTS_HANDOFF Phase 3, Steven Nov 1) ─────────

def _payee_to_quickbooks(cm):
    """The one write FPBudget makes to QuickBooks, for one Payee: link to an
    exact DisplayName match, else create a Vendor. Raises ValueError (→ 409)
    when the Payee is already linked or is a W-2 employee, RuntimeError
    when QuickBooks is unreachable. Commits + logs on success. Returns the
    result dict of qbo_sync.ensure_vendor_for_payee."""
    conn = QBOConnection.query.first()
    if not conn or not conn.refresh_token:
        raise RuntimeError("QuickBooks is not connected.")
    w9 = payee_w9_status(cm)
    r = qbo_sync.ensure_vendor_for_payee(conn, db, cm, w9_on_file=(w9["status"] == W9_ON_FILE))
    n = actuals.attribute_txns_for_qbo_vendor(r["vendor_id"], cm.id)
    db.session.commit()
    _log_activity(action='update', entity_type='payee', entity_id=cm.id, entity_label=cm.name,
                  field_changes={"qbo_vendor_id": [None, r["vendor_id"]],
                                 "qbo_origin": [None, cm.qbo_origin]},
                  after={"action": r["action"], "display_name": r["display_name"],
                         "vendor_1099": bool(w9["status"] == W9_ON_FILE),
                         "created_payload": r.get("payload"), "charges_attributed": n},
                  note=(f"Created QuickBooks vendor “{r['display_name']}” (#{r['vendor_id']})"
                        if r["action"] == 'created' else
                        f"Linked to existing QuickBooks vendor “{r['display_name']}” (#{r['vendor_id']}) — not duplicated"))
    r["charges_attributed"] = n
    return r


@app.route("/payees/<int:payee_id>/qbo-create", methods=["POST"])
@login_required
@admin_hub_required
def payee_qbo_create(payee_id):
    """Create this Payee in QuickBooks (or link an exact DisplayName match).
    409 when already linked or a W-2 employee. FPBudget may CREATE in
    QuickBooks; it never updates, sparse-updates or overwrites a record
    that exists there — after this the record is QuickBooks's."""
    cm = CrewMember.query.get_or_404(payee_id)
    want_json = request.is_json or request.args.get('fmt') == 'json'
    try:
        r = _payee_to_quickbooks(cm)
    except ValueError as e:
        db.session.rollback()
        if want_json:
            return jsonify({"ok": False, "error": str(e)}), 409
        flash(str(e), "error")
        return redirect(url_for('payee_page', payee_id=payee_id))
    except Exception as e:
        db.session.rollback()
        if want_json:
            return jsonify({"ok": False, "error": f"QuickBooks: {e}"}), 502
        flash(f"QuickBooks: {e}", "error")
        return redirect(url_for('payee_page', payee_id=payee_id))
    if want_json:
        return jsonify({"ok": True, "action": r["action"], "vendor_id": r["vendor_id"],
                        "display_name": r["display_name"],
                        "charges_attributed": r["charges_attributed"]})
    flash((f"Created “{r['display_name']}” in QuickBooks." if r["action"] == 'created'
           else f"Linked to the existing QuickBooks vendor “{r['display_name']}”."), "success")
    return redirect(url_for('payee_page', payee_id=payee_id))


# ── Vendor names ↔ Payee (DOCUMENTS_HANDOFF Phase 3) ───────────────────────

@app.route("/payees/<int:payee_id>/vendor-names", methods=["POST"])
@login_required
@admin_hub_required
def payee_vendor_name_link(payee_id):
    """Link a raw vendor string to this Payee (VendorAlias.payee_id). Body
    {raw: str, project_id?: int, alias_id?: int}: an existing alias row by
    id, or a new / global row from the string. Every transaction carrying
    that string with no payee (or an earlier alias attribution) attributes
    here."""
    cm = CrewMember.query.get_or_404(payee_id)
    src = request.get_json(silent=True) if request.is_json else request.form
    src = src or {}
    try:
        alias_id = int(src.get("alias_id") or 0)
    except (TypeError, ValueError):
        alias_id = 0
    try:
        if alias_id:
            a = VendorAlias.query.get_or_404(alias_id)
            if a.payee_id and a.payee_id != cm.id:
                return jsonify({"ok": False, "error": "That vendor name is linked to another payee — unlink it there first."}), 409
            n = actuals.link_alias_row_to_payee(a, cm.id)
        else:
            raw = (src.get("raw") or '').strip()
            if not raw:
                return jsonify({"ok": False, "error": "Type a vendor name to link."}), 400
            try:
                pid_ = int(src.get("project_id")) if src.get("project_id") not in (None, '', 'null') else None
            except (TypeError, ValueError):
                pid_ = None
            a, n = link_vendor_alias_to_payee(pid_, raw, cm.id, clean_name=cm.name)
    except ValueError as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 400
    db.session.commit()
    _log_activity(action='update', entity_type='payee', entity_id=cm.id, entity_label=cm.name,
                  after={"vendor_alias_id": a.id, "raw": a.raw_canonical, "charges_attributed": n},
                  note=f"Vendor name “{a.clean_name}” linked to {cm.name}; {n} charge{'s' if n != 1 else ''} attributed")
    return jsonify({"ok": True, "alias_id": a.id, "raw": a.raw_canonical, "clean": a.clean_name,
                    "charges_attributed": n})


@app.route("/payees/<int:payee_id>/vendor-names/<int:alias_id>/unlink", methods=["POST"])
@login_required
@admin_hub_required
def payee_vendor_name_unlink(payee_id, alias_id):
    cm = CrewMember.query.get_or_404(payee_id)
    a = VendorAlias.query.get_or_404(alias_id)
    if a.payee_id != cm.id:
        return jsonify({"ok": False, "error": "That vendor name is not linked to this payee."}), 409
    n = unlink_vendor_alias(a)
    db.session.commit()
    _log_activity(action='update', entity_type='payee', entity_id=cm.id, entity_label=cm.name,
                  after={"vendor_alias_id": a.id, "raw": a.raw_canonical, "charges_cleared": n},
                  note=f"Vendor name “{a.clean_name}” unlinked from {cm.name}")
    return jsonify({"ok": True, "alias_id": a.id, "charges_cleared": n})
