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

# Payee fields the crew forms may carry (DOCUMENTS_HANDOFF Phase 1). Kept
# as a literal here so this module never imports routes.payees at load
# time (that module imports app, which imports this one last).
_PAYEE_FIELDS = ('kind', 'is_w2_employee', 'w9_received_on', 'legal_name',
                 'address_line1', 'address_line2', 'city', 'state',
                 'postal_code', 'wrapbook_worker_id', 'notes')

def _payee_next():
    """Where a crew edit returns to: a /payees/... path when the Payee page's
    modal sent one (never an off-site URL), else the directory."""
    nxt = (request.form.get("next") or request.args.get("next") or "").strip()
    if nxt.startswith("/payees") and "//" not in nxt:
        return nxt
    return url_for("crew_list")


@app.route("/crew")
@login_required
def crew_list():
    """The Crew Database is now the Payee directory (DOCUMENTS_HANDOFF
    Phase 1, 2026-11-01): /crew is a redirect so every old link and the
    nav keep working. The endpoint name stays `crew_list` for url_for."""
    return redirect(url_for("payees_directory"), code=302)


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
    # Payee fields (kind, legal name, address, W-2 flag…) when the form
    # carries them — the Payee directory's New payee modal does.
    if any(k in _rd_new for k in _PAYEE_FIELDS) if _rd_new else False:
        from routes.payees import apply_payee_fields
        try:
            apply_payee_fields(m, _rd_new)
        except ValueError as _pe:
            if want_json:
                return jsonify({"error": str(_pe)}), 400
            flash(str(_pe), "error")
            return redirect(_payee_next())
    db.session.add(m)
    db.session.commit()

    if want_json:
        return jsonify({"ok": True, "id": m.id, "name": m.name,
                        "department": m.department or "", "company": m.company or ""})
    flash(f"Added {m.name}.", "success")
    return redirect(_payee_next())


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
    # Payee fields (DOCUMENTS_HANDOFF Phase 1): the Payee page's Edit modal
    # posts them in the same form. The QuickBooks mirror fields are never
    # accepted here (apply_payee_fields ignores them).
    _changes = {}
    if _src is not None and any(k in _src for k in _PAYEE_FIELDS):
        from routes.payees import apply_payee_fields
        try:
            _changes = apply_payee_fields(m, _src)
        except ValueError as _pe:
            if want_json:
                return jsonify({"error": str(_pe)}), 400
            flash(str(_pe), "error")
            return redirect(_payee_next())
    db.session.commit()
    if _changes:
        from app import _log_activity
        _log_activity(action='update', entity_type='payee', entity_id=m.id,
                      entity_label=m.name, field_changes=_changes,
                      note="Payee record updated (crew edit)")
    if want_json:
        return jsonify({"ok": True, "id": m.id, "name": m.name})
    flash(f"Updated {m.name}.", "success")
    return redirect(_payee_next())


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
        "active": bool(m.active),
        # Payee fields (Phase 1). qbo_* are read-only mirror values.
        "kind": getattr(m, 'kind', None) or ('vendor' if m.is_vendor else 'person'),
        "legal_name": m.legal_name or "", "address_line1": m.address_line1 or "",
        "address_line2": m.address_line2 or "", "city": m.city or "",
        "state": m.state or "", "postal_code": m.postal_code or "",
        "w9_received_on": m.w9_received_on.isoformat() if m.w9_received_on else "",
        "is_w2_employee": bool(m.is_w2_employee),
        "wrapbook_worker_id": m.wrapbook_worker_id or "", "notes": m.notes or "",
        "qbo_vendor_id": m.qbo_vendor_id, "qbo_vendor_1099": m.qbo_vendor_1099,
        "qbo_origin": m.qbo_origin,
    })


def _crew_delete_blockers(cid):
    """Everything that still references a CrewMember and would either FK-fail
    the delete (Timecard, ProjectCrewMember) or silently orphan records
    (DocUpload, ScheduleDay, loan-out links). Returns {label: count} for the
    non-zero ones. (DOCUMENTS_HANDOFF Phase 0, 2026-10-30.)"""
    from models import DocUpload, Timecard, ScheduleDay, ProjectCrewMember
    checks = {
        "document":           DocUpload.query.filter(
                                  DocUpload.crew_member_id == cid,
                                  DocUpload.status != 'deleted').count(),
        "timecard":           Timecard.query.filter_by(crew_member_id=cid).count(),
        "schedule day":       ScheduleDay.query.filter_by(crew_member_id=cid).count(),
        "project roster row": ProjectCrewMember.query.filter_by(crew_member_id=cid).count(),
        "loan-out link":      (CrewMember.query.filter_by(loan_out_vendor_id=cid).count()
                               + ProjectCrewMember.query.filter_by(loan_out_vendor_id=cid).count()),
    }
    return {k: v for k, v in checks.items() if v}


@app.route("/crew/<int:cid>/delete", methods=["POST"])
@login_required
def crew_delete(cid):
    _require_global_editor()
    m = CrewMember.query.get_or_404(cid)
    # Refuse while anything still points at this person: documents, timecards,
    # schedule days, per-project roster rows or loan-out links. The old code
    # nulled BudgetLine / CrewAssignment only, so anyone with paperwork either
    # FK-errored (500) or lost their document history. Deactivate instead.
    blockers = _crew_delete_blockers(cid)
    if blockers:
        parts = [f"{n} {lbl}{'s' if n != 1 else ''}" for lbl, n in blockers.items()]
        msg = (f"Can't delete {m.name}: still referenced by "
               + ", ".join(parts)
               + ". Deactivate the person instead (Edit → Active off) so the "
                 "history stays intact.")
        wants_json = (request.is_json
                      or 'application/json' in (request.headers.get('Accept') or ''))
        if wants_json:
            return jsonify({"error": msg, "blockers": blockers,
                            "suggest": "deactivate"}), 400
        # The crew.html form posts here as a plain form submit: a 400 body
        # would strand the browser on a blank page, so refuse with a flash
        # and bounce back to the list (the JSON branch above is the 400).
        flash(msg, "error")
        return redirect(url_for("crew_list"))
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


# ── Per-person document vault (owner 2026-10-08) ──────────────────────────
# "I want to be able to access that through the crew database … super admin
# only for now … let those documents live with them forever." Lists every
# DocUpload linked to this person across ALL projects, grouped by year, and
# syncs any missing Crew Database (Dropbox) duplicates.

def _require_super_admin():
    if getattr(current_user, 'role', None) != 'super_admin':
        abort(403)


@app.route("/crew/<int:cid>/docs.json", methods=["GET"])
@login_required
def crew_docs_json(cid):
    _require_super_admin()
    from models import DocUpload
    cm = CrewMember.query.get_or_404(cid)
    docs = (DocUpload.query
            .filter(DocUpload.crew_member_id == cid,
                    DocUpload.status.notin_(('deleted', 'duplicate')))
            .all())
    proj_names = {p.id: p.name for p in ProjectSheet.query.filter(
        ProjectSheet.id.in_({d.project_id for d in docs if d.project_id})).all()} if docs else {}
    by_year = {}
    for d in docs:
        # DocUpload's timestamp column is uploaded_at (no created_at exists);
        # the old getattr never matched, so undated docs bucketed under "—".
        yr = (d.doc_date.year if d.doc_date
              else (d.uploaded_at.year if getattr(d, 'uploaded_at', None) else 0))
        by_year.setdefault(str(yr or '—'), []).append({
            "id": d.id,
            "name": d.filed_filename or d.original_filename or f"Doc #{d.id}",
            "category": d.category or '',
            "project": proj_names.get(d.project_id, ''),
            "project_id": d.project_id,
            "amount": float(d.amount) if d.amount is not None else None,
            "date": d.doc_date.isoformat() if d.doc_date else '',
            "crew_dbx_path": getattr(d, 'crew_dbx_path', None),
        })
    years = sorted(by_year.keys(), reverse=True)
    for y in years:
        by_year[y].sort(key=lambda r: r["date"] or '', reverse=True)
    return jsonify({"ok": True, "crew": {"id": cm.id, "name": cm.name},
                    "years": years, "docs_by_year": by_year,
                    "total": len(docs),
                    "unsynced": sum(1 for y in years for r in by_year[y]
                                    if not r["crew_dbx_path"])})


@app.route("/crew/<int:cid>/docs/sync", methods=["POST"])
@login_required
def crew_docs_sync(cid):
    """Copy every not-yet-duplicated doc for this person into their Crew
    Database Dropbox folder (<OPS_ROOT>/Crew Database/<Name>/<Year>[/<Project>])."""
    _require_super_admin()
    from models import DocUpload
    from app import _copy_doc_to_crew_folder
    CrewMember.query.get_or_404(cid)
    docs = (DocUpload.query
            .filter(DocUpload.crew_member_id == cid,
                    DocUpload.crew_dbx_path.is_(None),
                    DocUpload.status.notin_(('deleted', 'duplicate')))
            .all())
    copied, skipped = 0, 0
    for d in docs:
        if _copy_doc_to_crew_folder(d):
            copied += 1
        else:
            skipped += 1
    if copied:
        db.session.commit()
    return jsonify({"ok": True, "copied": copied, "skipped": skipped,
                    "note": ("skipped docs have no Dropbox file yet or "
                             "Dropbox is unreachable" if skipped else "")})
