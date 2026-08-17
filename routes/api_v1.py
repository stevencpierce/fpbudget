"""Mobile app API v1 — Phase 0 slice (2026-07-23). See MOBILE_APP_PLAN.md.

Auth model: POST /api/v1/auth/login exchanges email+password for a bearer
token (ApiToken row, hash-stored). Every other /api/v1 route requires
`Authorization: Bearer fpb_...`, resolved to current_user by the
request_loader in app.py. api_auth_required additionally insists the request
authenticated via that header — a browser session cookie is NOT accepted on
/api/ routes, which is what makes the /api/ CSRF exemption safe.

Versioning contract: once the app ships, /api/v1 responses may gain fields
but never lose or rename them — breaking changes go to /api/v2.

Same M1 pattern as the other slices: @app.route on the shared app object,
imported at app.py's bottom.
"""
from datetime import datetime, timedelta
from functools import wraps

from flask import request, jsonify, g
from flask_login import current_user

from app import (app, docs_upload_post, docs_upload_status,
                 _docs_accessible_projects, _actuals_by_section_code,
                 upsert_line, delete_line)
from models import db, User, ApiToken, ProjectAccess, Budget, BudgetLine
from budget_calc import get_fringe_configs, calc_top_sheet
from api_auth import generate_token, hash_token, parse_bearer


def api_auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not (getattr(g, "api_bearer_auth", False)
                and current_user.is_authenticated):
            return jsonify({"error": "Authentication required — send "
                                     "Authorization: Bearer <token>."}), 401
        return f(*args, **kwargs)
    return decorated


# ── Login throttle ────────────────────────────────────────────────────────
# In-process only (per gunicorn worker), so it's a speed bump rather than a
# hard guarantee — but it turns an online brute-force from thousands of
# guesses/min into a handful. Fail-open by design: throttle bugs must never
# lock real users out.
_LOGIN_FAILS = {}          # key → [datetime, ...] of recent failures
_LOGIN_WINDOW = timedelta(minutes=15)
_LOGIN_MAX_FAILS = 10


def _throttle_key():
    email = ((request.get_json(silent=True) or {}).get("email") or "").strip().lower()
    return f"{email}|{request.remote_addr or ''}"


def _login_throttled():
    try:
        now = datetime.utcnow()
        fails = [t for t in _LOGIN_FAILS.get(_throttle_key(), [])
                 if now - t < _LOGIN_WINDOW]
        _LOGIN_FAILS[_throttle_key()] = fails
        return len(fails) >= _LOGIN_MAX_FAILS
    except Exception:
        return False


def _login_failed():
    try:
        _LOGIN_FAILS.setdefault(_throttle_key(), []).append(datetime.utcnow())
        if len(_LOGIN_FAILS) > 5000:   # bound worker memory
            _LOGIN_FAILS.clear()
    except Exception:
        pass


def _user_payload(user):
    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "role": user.role,
        "display_role": user.display_role,
        "is_docs_only": user.is_docs_only,
    }


@app.route("/api/v1/auth/login", methods=["POST"])
def api_auth_login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    device_name = (data.get("device_name") or "").strip()[:120]
    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400
    if _login_throttled():
        return jsonify({"error": "Too many failed attempts — wait a few "
                                 "minutes and try again."}), 429
    user = User.query.filter_by(email=email).first()
    if not (user and user.is_active and user.check_password(password)):
        _login_failed()
        return jsonify({"error": "Invalid credentials."}), 401
    if user.must_change_password:
        return jsonify({"error": "Your account requires a new password — "
                                 "set one on the FPBudget website, then log "
                                 "in here again."}), 403
    raw = generate_token()
    db.session.add(ApiToken(user_id=user.id, token_hash=hash_token(raw),
                            device_name=device_name or None,
                            last_used_at=datetime.utcnow()))
    db.session.commit()
    return jsonify({"token": raw, "user": _user_payload(user)})


@app.route("/api/v1/auth/logout", methods=["POST"])
@api_auth_required
def api_auth_logout():
    tok = ApiToken.query.get(getattr(g, "api_token_id", None) or 0)
    if tok and not tok.revoked_at:
        tok.revoked_at = datetime.utcnow()
        db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/v1/me", methods=["GET"])
@api_auth_required
def api_me():
    """Who am I + which projects can I touch. The app builds its whole
    home screen (project picker, role-based tabs) from this one call."""
    projects = _docs_accessible_projects(current_user)
    if current_user.is_admin:
        roles = {p.id: "owner" for p in projects}
    else:
        rows = ProjectAccess.query.filter_by(user_id=current_user.id).all()
        roles = {pa.project_id: ("editor" if pa.role == "collaborator"
                                 else (pa.role or "viewer"))
                 for pa in rows}
    return jsonify({
        "user": _user_payload(current_user),
        "projects": [{
            "id": p.id,
            "name": p.name,
            "client_name": p.client_name,
            "status": p.status,
            "role": roles.get(p.id, "viewer"),
            # False → uploads will fail until Dropbox is provisioned; the
            # app shows a friendly "ask your line producer" message.
            "can_upload_docs": bool(p.dropbox_folder),
        } for p in projects],
    })


# ── Docs upload (thin aliases over the existing web endpoints) ────────────
# The heavy lifting (validation, dedup, Veryfi OCR, Dropbox filing) lives in
# app.py's handlers; these aliases just give the mobile app stable /api/v1
# paths. Endpoint names start with "api_docs" so enforce_project_access
# keeps allowing uploads from viewer/docs_only project roles.

@app.route("/api/v1/projects/<int:pid>/docs/upload", methods=["POST"])
@api_auth_required
def api_docs_upload(pid):
    return docs_upload_post(pid)


@app.route("/api/v1/docs/<int:uid>/status", methods=["GET"])
@api_auth_required
def api_docs_upload_status(uid):
    return docs_upload_status(uid)


@app.route("/api/v1/projects/<int:pid>/docs/recent", methods=["GET"])
@api_auth_required
def api_docs_recent(pid):
    """The current user's own recent uploads in this project — powers the
    app's 'Recent uploads' list (and survives app reinstall, unlike a
    device-local history). Project access is enforced by the central
    enforce_project_access gate on <pid>."""
    from models import DocUpload
    rows = (DocUpload.query
            .filter_by(project_id=pid, uploader_id=current_user.id)
            .filter(DocUpload.status != 'deleted')
            .order_by(DocUpload.uploaded_at.desc())
            .limit(25).all())
    return jsonify({"uploads": [{
        "id": u.id,
        "status": u.status,
        "original_filename": u.original_filename,
        "filed_filename": u.filed_filename,
        "vendor": u.vendor,
        "amount": float(u.amount) if u.amount is not None else None,
        "doc_date": u.doc_date.isoformat() if u.doc_date else None,
        "is_duplicate": bool(u.is_duplicate),
        "uploaded_at": u.uploaded_at.isoformat() if u.uploaded_at else None,
    } for u in rows]})


# ── Budgets (Phase 2) ─────────────────────────────────────────────────────
# Read endpoints serve the app's budget list + a fully-computed summary so
# the phone NEVER does budget math — calc_top_sheet/calc_line on the server
# remain the single source of truth. Mutations are thin wrappers over the
# battle-tested web handlers (upsert_line / delete_line), so activity
# logging, estimated-edit protection, and schedule guards all apply
# unchanged. enforce_project_access gates <pid> centrally: any project
# member may GET; POST/DELETE require editor or better.

_MODE_LABELS = {"estimated": "Estimated", "working": "Working",
                "schedule": "Working", "hybrid": "Hybrid",
                "actual": "Actual"}


@app.route("/api/v1/projects/<int:pid>/budgets", methods=["GET"])
@api_auth_required
def api_budgets_list(pid):
    rows = (Budget.query.filter_by(project_id=pid)
            .filter(Budget.version_status != 'archived')
            .order_by(Budget.updated_at.desc().nullslast(), Budget.id.desc())
            .all())
    return jsonify({"budgets": [{
        "id": b.id,
        "name": b.name,
        "budget_mode": b.budget_mode,
        "mode_label": ("Actual" if b.is_actual
                       else _MODE_LABELS.get(b.budget_mode, b.budget_mode)),
        "is_actual": bool(b.is_actual),
        "version_status": b.version_status,
        "version_number": b.version_number,
        "start_date": b.start_date.isoformat() if b.start_date else None,
        "end_date": b.end_date.isoformat() if b.end_date else None,
        "target_budget": (float(b.target_budget)
                          if b.target_budget is not None else None),
        "updated_at": b.updated_at.isoformat() if b.updated_at else None,
    } for b in rows]})


@app.route("/api/v1/projects/<int:pid>/budgets/<int:bid>/summary",
           methods=["GET"])
@api_auth_required
def api_budget_summary(pid, bid):
    """Whole-budget view for the app: section roll-up (top sheet), grand
    totals incl. auto lines (Workers' Comp / Payroll Fee / Insurance /
    Company Fee), and every line with its server-computed total."""
    budget = Budget.query.filter_by(id=bid, project_id=pid).first_or_404()
    lines = (BudgetLine.query.filter_by(budget_id=bid)
             .order_by(BudgetLine.account_code, BudgetLine.sort_order,
                       BudgetLine.id).all())
    fringe_cfgs = get_fringe_configs(db.session, pid)
    profile = budget.payroll_profile
    pw_start = (budget.payroll_week_start
                if budget.payroll_week_start is not None
                else (profile.payroll_week_start if profile else 6))
    actuals_by_code = _actuals_by_section_code(pid)
    ts = calc_top_sheet(budget, lines, fringe_cfgs, actuals_by_code,
                        profile, pw_start)
    lt = ts.get("line_totals") or {}

    def _num(x):
        return float(x) if x is not None else None

    return jsonify({
        "budget": {
            "id": budget.id,
            "name": budget.name,
            "budget_mode": budget.budget_mode,
            "mode_label": ("Actual" if budget.is_actual
                           else _MODE_LABELS.get(budget.budget_mode,
                                                 budget.budget_mode)),
            "is_actual": bool(budget.is_actual),
            "version_status": budget.version_status,
            "version_number": budget.version_number,
            "target_budget": _num(budget.target_budget),
            "start_date": (budget.start_date.isoformat()
                           if budget.start_date else None),
            "end_date": (budget.end_date.isoformat()
                         if budget.end_date else None),
        },
        "totals": {
            "subtotal_estimated": ts["subtotal_estimated"],
            "subtotal_actual": ts["subtotal_actual"],
            "company_fee": ts["company_fee"],
            "company_fee_dispersed": ts["company_fee_dispersed"],
            "workers_comp_amount": ts["workers_comp_amount"],
            "payroll_fee_amount": ts["payroll_fee_amount"],
            "production_insurance_amount": ts["production_insurance_amount"],
            "grand_total_estimated": ts["grand_total_estimated"],
            "grand_total_actual": ts["grand_total_actual"],
            "grand_variance": ts["grand_variance"],
        },
        "sections": [{
            "code": r["code"],
            "account": r["account"],
            "estimated": r["estimated"],
            "actual": r["actual"],
            "variance": r["variance"],
        } for r in ts["rows"]],
        "lines": [{
            "id": ln.id,
            "account_code": ln.account_code,
            "account_name": ln.account_name,
            "description": ln.description or "",
            "is_labor": bool(ln.is_labor),
            "use_schedule": bool(ln.use_schedule),
            "line_tag": ln.line_tag,
            "quantity": _num(ln.quantity),
            "days": _num(ln.days),
            "rate": _num(ln.rate),
            "rate_type": ln.rate_type,
            "est_ot": _num(ln.est_ot),
            "fringe_type": ln.fringe_type,
            "agent_pct": _num(ln.agent_pct),
            "note": ln.note,
            "sort_order": ln.sort_order or 0,
            "parent_line_id": ln.parent_line_id,
            "subtotal": (lt.get(ln.id) or {}).get("subtotal", 0),
            "fringe_amount": (lt.get(ln.id) or {}).get("fringe_amount", 0),
            "agent_amount": (lt.get(ln.id) or {}).get("agent_amount", 0),
            "total": (lt.get(ln.id) or {}).get("total", 0),
        } for ln in lines],
    })


@app.route("/api/v1/projects/<int:pid>/budgets/<int:bid>/line",
           methods=["POST"])
@api_auth_required
def api_budget_line_save(pid, bid):
    # Same body/semantics as the web endpoint, including the 409 responses
    # (estimated_protected → resend with override_estimated: true;
    # schedule_conflict → resolve on the website).
    return upsert_line(pid, bid)


@app.route("/api/v1/projects/<int:pid>/budgets/<int:bid>/line/<int:lid>",
           methods=["DELETE"])
@api_auth_required
def api_budget_line_delete(pid, bid, lid):
    return delete_line(pid, bid, lid)
