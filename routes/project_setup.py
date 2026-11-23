"""Project setup: stage, code, dates, Award (INTAKE_WRAP_HANDOFF Phase 1,
2026-11-23). The rules live in project_codes.py; this is the screen.

  GET  /projects/<pid>/setup                 the page
  GET  /projects/<pid>/setup/preview.json    ?initials=&start= → the code
                                             Award would give + how many
                                             expenses it would number
  POST /projects/<pid>/setup/award           Estimate → Awarded (owner)
  POST /projects/<pid>/setup/stage           estimate | awarded | wrapping
  POST /projects/<pid>/setup/dates           start / end
  POST /projects/<pid>/setup/code            super-admin correction while
                                             no reference number exists
  GET  /admin/projects/estimate-holdings     Maintenance: Estimate projects
                                             that already hold charges or
                                             documents

Module pattern: routes/__init__.py (plain @app.route on the same app,
imported at the bottom of app.py).
"""
from datetime import datetime

from flask import abort, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app import app, maintenance_required, _log_activity, _require_project_role, _user_project_role
from models import db, Budget, ProjectSheet
import project_codes as pc


def _date(s):
    s = (s or '').strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], '%Y-%m-%d').date()
    except ValueError:
        raise pc.StageError(f"Not a date: {s}")


def _snap(p):
    return {'stage': p.stage, 'project_code': p.project_code,
            'client_initials': p.client_initials,
            'start_date': p.start_date.isoformat() if p.start_date else None,
            'end_date': p.end_date.isoformat() if p.end_date else None}


def _budget_dates(p):
    """The live budget's dates, offered when the project has none yet."""
    b = (Budget.query.filter(Budget.project_id == p.id)
         .order_by(Budget.id.desc()).first())
    return (getattr(b, 'start_date', None), getattr(b, 'end_date', None)) if b else (None, None)


def _log(p, before, note):
    try:
        _log_activity(action='update', entity_type='project', entity_id=p.id,
                      entity_label=p.name, project_id=p.id,
                      before=before, after=_snap(p), note=note[:500])
    except Exception:
        pass


@app.route("/projects/<int:pid>/setup")
@login_required
def project_setup(pid):
    _require_project_role(pid, 'viewer')
    p = ProjectSheet.query.get_or_404(pid)
    b_start, b_end = _budget_dates(p)
    start = p.start_date or b_start
    initials = p.client_initials or pc.suggest_initials(p.client_name)
    suggested = None
    if not p.project_code and initials and start:
        try:
            suggested = pc.next_free_code(pc.code_base(initials, start), p.id)
        except pc.StageError:
            suggested = None
    return render_template(
        "project_setup.html", project=p, stage=pc.project_stage(p),
        stage_label=pc.stage_label(p), stage_labels=pc.STAGE_LABELS,
        can_edit=_user_project_role(pid) == 'owner',
        is_super_admin=getattr(current_user, 'role', None) == 'super_admin',
        start=start, end=p.end_date or b_end, initials=initials,
        suggested_code=suggested, to_number=pc.eligible_count(pid),
        holdings=pc.estimate_holdings(pid))


@app.route("/projects/<int:pid>/setup/preview.json")
@login_required
def project_setup_preview(pid):
    _require_project_role(pid, 'viewer')
    p = ProjectSheet.query.get_or_404(pid)
    try:
        code = p.project_code or pc.next_free_code(
            pc.code_base(request.args.get('initials'), _date(request.args.get('start'))), p.id)
    except pc.StageError as e:
        return jsonify({'ok': False, 'error': str(e)})
    return jsonify({'ok': True, 'code': code, 'to_number': pc.eligible_count(pid)})


def _redirect(pid):
    return redirect(url_for('project_setup', pid=pid))


@app.route("/projects/<int:pid>/setup/award", methods=["POST"])
@login_required
def project_setup_award(pid):
    _require_project_role(pid, 'owner')
    p = ProjectSheet.query.get_or_404(pid)
    before = _snap(p)
    try:
        n = pc.award(p, request.form.get('initials'), _date(request.form.get('start_date')),
                     _date(request.form.get('end_date')),
                     code=(request.form.get('code') or '').strip() or None)
        db.session.commit()
    except pc.StageError as e:
        db.session.rollback()
        flash(str(e), 'error')
        return _redirect(pid)
    _log(p, before, f'Awarded "{p.name}" as {p.project_code}'
         + (f'; numbered {n} expense{"s" if n != 1 else ""}' if n else ''))
    db.session.commit()
    flash(f"Awarded as {p.project_code}."
          + (f" {n} expense{'s' if n != 1 else ''} numbered." if n else ''), 'success')
    return _redirect(pid)


@app.route("/projects/<int:pid>/setup/stage", methods=["POST"])
@login_required
def project_setup_stage(pid):
    _require_project_role(pid, 'owner')
    p = ProjectSheet.query.get_or_404(pid)
    before = _snap(p)
    try:
        pc.set_stage(p, request.form.get('stage'))
        db.session.commit()
    except pc.StageError as e:
        db.session.rollback()
        flash(str(e), 'error')
        return _redirect(pid)
    _log(p, before, f'Stage of "{p.name}" set to {pc.stage_label(p)}')
    db.session.commit()
    flash(f"Stage set to {pc.stage_label(p)}.", 'success')
    return _redirect(pid)


@app.route("/projects/<int:pid>/setup/dates", methods=["POST"])
@login_required
def project_setup_dates(pid):
    _require_project_role(pid, 'owner')
    p = ProjectSheet.query.get_or_404(pid)
    before = _snap(p)
    try:
        start, end = _date(request.form.get('start_date')), _date(request.form.get('end_date'))
        if start and end and end < start:
            raise pc.StageError("The end date is before the start date.")
    except pc.StageError as e:
        flash(str(e), 'error')
        return _redirect(pid)
    p.start_date, p.end_date = start, end
    db.session.commit()
    _log(p, before, f'Dates of "{p.name}" updated (the project code does not change)')
    db.session.commit()
    flash("Dates saved. The project code stays as it is.", 'success')
    return _redirect(pid)


@app.route("/projects/<int:pid>/setup/code", methods=["POST"])
@login_required
def project_setup_code(pid):
    if getattr(current_user, 'role', None) != 'super_admin':
        abort(403)
    p = ProjectSheet.query.get_or_404(pid)
    before = _snap(p)
    try:
        pc.change_code(p, request.form.get('code'))
        db.session.commit()
    except pc.StageError as e:
        db.session.rollback()
        flash(str(e), 'error')
        return _redirect(pid)
    _log(p, before, f'Project code of "{p.name}" corrected to {p.project_code}')
    db.session.commit()
    flash(f"Code corrected to {p.project_code}.", 'success')
    return _redirect(pid)


@app.route("/admin/projects/estimate-holdings")
@login_required
@maintenance_required
def admin_estimate_holdings():
    rows = pc.estimate_projects_holding_money()
    if request.args.get('format') == 'json':
        return jsonify({'projects': [{'id': r['project'].id, 'name': r['project'].name,
                                      'transactions': r['transactions'],
                                      'documents': r['documents']} for r in rows]})
    return render_template("admin_estimate_holdings.html", rows=rows)
