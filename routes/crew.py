"""Crew & support-contact routes — M1b slice of the app.py split.

See routes/__init__.py for the pattern. Endpoint names and URLs are
byte-identical to the monolith version; only the file moved. All mutating
routes remain gated by _require_global_editor (audit C1).
"""
from flask import (render_template, request, jsonify, flash, redirect,
                   url_for, abort)
from flask_login import login_required, current_user
from sqlalchemy import func

from app import app, _require_global_editor, _normalize_phone, _validate_email
from models import (db, CrewMember, SupportContact, CrewAssignment, Budget,
                    BudgetLine, BudgetDirectContact, ProjectSheet)

@app.route("/crew")
@login_required
def crew_list():
    members = CrewMember.query.order_by(CrewMember.department, CrewMember.name).all()
    # Build {crew_member_id: [project_name, ...]} via CrewAssignment and assigned_crew_id
    crew_projects = {}
    rows = (db.session.query(CrewAssignment.crew_member_id, ProjectSheet.name)
            .join(BudgetLine, BudgetLine.id == CrewAssignment.budget_line_id)
            .join(Budget, Budget.id == BudgetLine.budget_id)
            .join(ProjectSheet, ProjectSheet.id == Budget.project_id)
            .filter(CrewAssignment.crew_member_id.isnot(None))
            .distinct().all())
    for crew_id, proj_name in rows:
        crew_projects.setdefault(crew_id, set()).add(proj_name)
    rows2 = (db.session.query(BudgetLine.assigned_crew_id, ProjectSheet.name)
             .join(Budget, Budget.id == BudgetLine.budget_id)
             .join(ProjectSheet, ProjectSheet.id == Budget.project_id)
             .filter(BudgetLine.assigned_crew_id.isnot(None))
             .distinct().all())
    for crew_id, proj_name in rows2:
        crew_projects.setdefault(crew_id, set()).add(proj_name)
    crew_projects = {k: sorted(v) for k, v in crew_projects.items()}
    # Primary agent per crew member (role_type='agent', active)
    agent_rows = SupportContact.query.filter_by(role_type='agent', active=True).all()
    agent_map = {}  # crew_member_id → first agent SupportContact
    for ag in agent_rows:
        if ag.crew_member_id not in agent_map:
            agent_map[ag.crew_member_id] = ag
    return render_template("crew.html", members=members, crew_projects=crew_projects,
                           agent_map=agent_map)



@app.route("/crew/new", methods=["POST"])
@login_required
def crew_new():
    _require_global_editor()
    want_json = request.is_json or request.args.get("fmt") == "json"
    name = (request.json or request.form).get("name", "").strip() if want_json else request.form.get("name", "").strip()
    if not name:
        if want_json:
            return jsonify({"error": "Name is required"}), 400
        flash("Name is required.", "error")
        return redirect(url_for("crew_list"))

    def _get(field, default=""):
        src = request.json if want_json else request.form
        return (src.get(field) or default)

    _email = _get("email").strip() or None
    if _email and not _validate_email(_email):
        if want_json:
            return jsonify({"error": f"Invalid email address: {_email}"}), 400
        flash(f"Invalid email address: {_email}", "error")
        return redirect(url_for("crew_list"))

    # Dedup guard (user 2026-05-29): don't create a second record for
    # someone already in the database. Match on email first (the strongest
    # key), then fall back to a case-insensitive name match. On a hit,
    # return the EXISTING member instead of a copy — so the budget-tab
    # "add new person" flow transparently assigns the existing person, and
    # bulk re-submits don't pile up duplicates.
    _dupe = None
    if _email:
        _dupe = CrewMember.query.filter(func.lower(CrewMember.email) == _email.lower()).first()
    if not _dupe:
        _dupe = CrewMember.query.filter(func.lower(CrewMember.name) == name.lower()).first()
    if _dupe:
        if want_json:
            return jsonify({"ok": True, "id": _dupe.id, "name": _dupe.name,
                            "department": _dupe.department or "", "company": _dupe.company or "",
                            "duplicate": True}), 200
        flash(f"“{_dupe.name}” is already in the crew database — not added again.", "error")
        return redirect(url_for("crew_list"))

    m = CrewMember(
        name=name,
        department=_get("department") or None,
        default_rate=_get("default_rate") or None,
        default_rate_type=_get("default_rate_type", "day_10"),
        default_fringe=_get("default_fringe", "N"),
        default_agent_pct=float(_get("default_agent_pct", 0) or 0) / 100,
        email=_email,
        phone=_normalize_phone(_get("phone")),
        company=_get("company") or None,
        is_vendor=bool(_get("is_vendor")),
        loan_out_vendor_id=(int(_get("loan_out_vendor_id")) if str(_get("loan_out_vendor_id")).strip().isdigit() else None),
    )
    _rd_new = (request.json if want_json else request.form)
    _rd = _rd_new.get("required_docs") if _rd_new else None
    if isinstance(_rd, (list, tuple)):
        m.required_docs = ",".join(str(x).strip() for x in _rd if str(x).strip()) or None
    elif _rd:
        m.required_docs = str(_rd).strip() or None
    db.session.add(m)
    db.session.commit()

    if want_json:
        return jsonify({"ok": True, "id": m.id, "name": m.name,
                        "department": m.department or "", "company": m.company or ""})
    flash(f"Added {m.name}.", "success")
    return redirect(url_for("crew_list"))


@app.route("/crew/<int:cid>/edit", methods=["POST"])
@login_required
def crew_edit(cid):
    _require_global_editor()
    m = CrewMember.query.get_or_404(cid)
    want_json = request.is_json or request.args.get("fmt") == "json"

    def _get(field, default=""):
        src = request.json if want_json else request.form
        return (src or {}).get(field) or default

    m.name             = _get("name", m.name).strip() or m.name
    m.department       = _get("department", "").strip() or None
    m.default_rate     = _get("default_rate") or None
    m.default_rate_type= _get("default_rate_type", m.default_rate_type)
    m.default_fringe   = _get("default_fringe", m.default_fringe)
    m.default_agent_pct= float(_get("default_agent_pct", 0) or 0) / 100
    _email = _get("email", "").strip() or None
    if _email and not _validate_email(_email):
        if want_json:
            return jsonify({"error": f"Invalid email: {_email}"}), 400
        flash(f"Invalid email address: {_email}", "error")
        return redirect(url_for("crew_list"))
    m.email = _email
    m.phone = _normalize_phone(_get("phone", "").strip())
    m.company = _get("company", "").strip() or None
    # Vendor / loan-out + per-vendor required docs.
    _src = request.json if want_json else request.form
    if _src is not None and ("is_vendor" in _src):
        m.is_vendor = bool(_src.get("is_vendor"))
    if _src is not None and ("loan_out_vendor_id" in _src):
        _lo = _src.get("loan_out_vendor_id")
        m.loan_out_vendor_id = int(_lo) if str(_lo).strip().isdigit() else None
    if _src is not None and ("required_docs" in _src):
        _rd = _src.get("required_docs")
        if isinstance(_rd, (list, tuple)):
            m.required_docs = ",".join(str(x).strip() for x in _rd if str(x).strip()) or None
        else:
            m.required_docs = (str(_rd).strip() or None) if _rd else None
    if not want_json:
        m.active = request.form.get("active") == "1"
    db.session.commit()
    if want_json:
        return jsonify({"ok": True, "id": m.id, "name": m.name})
    flash(f"Updated {m.name}.", "success")
    return redirect(url_for("crew_list"))


@app.route("/crew/<int:cid>/json", methods=["GET"])
@login_required
def crew_get_json(cid):
    _require_global_editor()
    m = CrewMember.query.get_or_404(cid)
    return jsonify({
        "id": m.id, "name": m.name, "department": m.department or "",
        "email": m.email or "", "phone": m.phone or "", "company": m.company or "",
        "default_rate": float(m.default_rate) if m.default_rate else "",
        "default_rate_type": m.default_rate_type or "day_10",
        "default_fringe": m.default_fringe or "N",
        "default_agent_pct": float(m.default_agent_pct or 0) * 100,
        "is_vendor": bool(getattr(m, 'is_vendor', False)),
        "loan_out_vendor_id": getattr(m, 'loan_out_vendor_id', None),
        "required_docs": getattr(m, 'required_docs', None) or "",
    })


@app.route("/crew/<int:cid>/delete", methods=["POST"])
@login_required
def crew_delete(cid):
    _require_global_editor()
    m = CrewMember.query.get_or_404(cid)
    # Null out FK references so the delete doesn't fail on constraint violations
    BudgetLine.query.filter_by(assigned_crew_id=cid).update({"assigned_crew_id": None},
                                                             synchronize_session=False)
    CrewAssignment.query.filter_by(crew_member_id=cid).update({"crew_member_id": None},
                                                               synchronize_session=False)
    BudgetDirectContact.query.filter_by(crew_member_id=cid).delete(synchronize_session=False)
    db.session.delete(m)
    db.session.commit()
    flash("Crew member deleted.", "success")
    return redirect(url_for("crew_list"))


# ── Support Contacts (reps/agents/managers) ────────────────────────────────

@app.route("/crew/<int:cid>/support", methods=["GET"])
@login_required
def support_contacts_list(cid):
    _require_global_editor()
    CrewMember.query.get_or_404(cid)
    contacts = SupportContact.query.filter_by(crew_member_id=cid, active=True).all()
    return jsonify([{
        "id": s.id, "role_type": s.role_type, "name": s.name,
        "email": s.email or "", "phone": s.phone or "", "company": s.company or "",
        "notify_callsheet": bool(s.notify_callsheet),
        "cc_by_default": bool(s.cc_by_default),
        "fee_pct": float(s.fee_pct) * 100 if s.fee_pct else None,
        "fee_type": s.fee_type or None,
    } for s in contacts])


@app.route("/crew/<int:cid>/support/save", methods=["POST"])
@login_required
def support_contact_save(cid):
    _require_global_editor()
    CrewMember.query.get_or_404(cid)
    data = request.get_json(force=True)
    sid = data.get("id")
    if sid:
        s = SupportContact.query.filter_by(id=sid, crew_member_id=cid).first_or_404()
    else:
        s = SupportContact(crew_member_id=cid)
        db.session.add(s)
    # (x or "") not .get(x, ""): the JS sends explicit nulls for blank fields,
    # and dict.get's default only applies when the KEY is missing — a null
    # company/email 500'd here ('NoneType' has no .strip, 2026-07-13).
    s.role_type   = data.get("role_type") or "other"
    s.name        = (data.get("name") or "").strip()
    s.email       = (data.get("email") or "").strip() or None
    s.phone       = _normalize_phone(data.get("phone") or "")
    s.company     = (data.get("company") or "").strip() or None
    s.notify_callsheet = bool(data.get("notify_callsheet", False))
    s.cc_by_default    = bool(data.get("cc_by_default", False))
    raw_fee = data.get("fee_pct")
    s.fee_pct  = float(raw_fee) / 100 if raw_fee is not None and raw_fee != '' else None
    s.fee_type = data.get("fee_type") or None
    if not s.name:
        return jsonify({"error": "Name required"}), 400
    db.session.commit()
    # When the primary agent's fee is set, keep CrewMember.default_agent_pct in sync
    if s.role_type == 'agent' and s.fee_pct is not None:
        cm = CrewMember.query.get(cid)
        if cm:
            # Only update if this is the only/first active agent or matches current default
            other_agents = SupportContact.query.filter(
                SupportContact.crew_member_id == cid,
                SupportContact.role_type == 'agent',
                SupportContact.active == True,
                SupportContact.id != s.id,
            ).first()
            if not other_agents:
                cm.default_agent_pct = s.fee_pct
                db.session.commit()
    return jsonify({"ok": True, "id": s.id})


@app.route("/crew/<int:cid>/support/<int:sid>/delete", methods=["POST"])
@login_required
def support_contact_delete(cid, sid):
    _require_global_editor()
    s = SupportContact.query.filter_by(id=sid, crew_member_id=cid).first_or_404()
    s.active = False
    db.session.commit()
    return jsonify({"ok": True})
