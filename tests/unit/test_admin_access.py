"""Cleanup E — Admin access, end to end on sqlite with the Flask test client
(2026-11-02).

Boots the real app against a throw-away sqlite file (or reuses the one
another app-booting module in this process already opened) and checks:

  1. a project viewer gets 403 on /settings/company (GET and POST) and on
     the fringe-library writes (save / new / delete); an admin may write
     fringes; the company profile is super-admin only
  2. a non-super-admin gets 403 on /admin/maintenance (+ .json); the
     super admin gets the page with every registered tool card on it
  3. the one-shot repair routes removed in Cleanup E are gone (404)
  4. /vendors/audit is admin-only

Skipped automatically where app.py's runtime deps are not installed.
Run: python -m pytest tests/unit/test_admin_access.py -q --confcutdir=tests/unit -o addopts=""
"""
import json
import os
import sys
import tempfile

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-admin-")
_PRESET = os.environ.get("DATABASE_URL", "")
if _PRESET and not _PRESET.startswith("sqlite:"):
    pytest.skip("DATABASE_URL is not sqlite; refusing to boot", allow_module_level=True)
os.environ.setdefault("DATABASE_URL", "sqlite:///" + os.path.join(_DB_DIR, "t.db"))
os.environ.setdefault("SECRET_KEY", "test")

try:
    _cwd = os.getcwd()
    os.chdir(ROOT)
    import app as fpapp                       # noqa: E402
    from models import (db, User, ProjectSheet, ProjectAccess, FringeConfig)   # noqa: E402
    import routes.admin_maintenance as maint  # noqa: F401,E402
    _IMPORT_ERR = None
except Exception as _e:                       # pragma: no cover
    _IMPORT_ERR = _e
finally:
    try:
        os.chdir(_cwd)
    except Exception:
        pass

pytestmark = pytest.mark.skipif(_IMPORT_ERR is not None,
                                reason=f"app.py not importable here: {_IMPORT_ERR}")

S = {}


def _user(email, role):
    u = User.query.filter_by(email=email).first()
    if not u:
        u = User(email=email, name=email.split("@")[0], role=role)
        db.session.add(u)
    u.role = role
    u.set_password("pw")
    db.session.flush()
    return u


@pytest.fixture(scope="module")
def ctx():
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        su = _user("ce-super@thefp.tv", "super_admin")
        admin = _user("ce-admin@thefp.tv", "admin")
        viewer = _user("ce-viewer@thefp.tv", "line_producer")
        p = ProjectSheet(name="Cleanup E access", status="active")
        db.session.add(p)
        db.session.flush()
        db.session.add(ProjectAccess(project_id=p.id, user_id=viewer.id, role="viewer"))
        fr = FringeConfig.query.filter_by(project_id=None, fringe_type="ZZ").first()
        if not fr:
            fr = FringeConfig(project_id=None, fringe_type="ZZ", label="Test fringe", rate=0.1)
            db.session.add(fr)
            db.session.flush()
        db.session.commit()
        S.update(su=su.id, admin=admin.id, viewer=viewer.id, pid=p.id, fid=fr.id)
    return {"app": app, "client": app.test_client()}


def _login(c, uid):
    with c.session_transaction() as s:
        s["_user_id"] = str(uid)
        s["_fresh"] = True
        s["_csrf"] = "t"


def _post_json(c, url, payload=None):
    return c.post(url, data=json.dumps(payload or {}), content_type="application/json",
                  headers={"X-CSRFToken": "t"})


def _post_form(c, url, data=None):
    return c.post(url, data=dict(data or {}, csrf_token="t"), headers={"X-CSRFToken": "t"})


# ── 1. company settings + fringe writes ──────────────────────────────────
def test_viewer_403_on_company_settings(ctx):
    c = ctx["client"]
    _login(c, S["viewer"])
    assert c.get("/settings/company").status_code == 403
    assert _post_json(c, "/settings/company", {"company_name": "x"}).status_code == 403
    # Company profile is super-admin only — an admin is refused too.
    _login(c, S["admin"])
    assert c.get("/settings/company").status_code == 403
    _login(c, S["su"])
    r = c.get("/settings/company")
    assert r.status_code == 200 and "company_name" in r.get_json()


def test_viewer_403_on_fringe_writes_admin_ok(ctx):
    c = ctx["client"]
    _login(c, S["viewer"])
    assert _post_form(c, "/fringe-config", {"fringe_id": S["fid"], "label": "nope", "rate": "0.2"}).status_code == 403
    assert _post_form(c, "/fringe-config/new", {"fringe_type": "QQ", "label": "nope", "rate": "0.1"}).status_code == 403
    assert _post_form(c, f"/fringe-config/{S['fid']}/delete").status_code == 403
    # Viewing stays open to every logged-in user, without the edit controls.
    r = c.get("/fringe-config")
    assert r.status_code == 200
    assert b"+ Add Fringe" not in r.data and b"openFringeEdit(" not in r.data.split(b"<script")[0]
    _login(c, S["admin"])
    r = _post_form(c, "/fringe-config", {"fringe_id": S["fid"], "label": "Edited", "rate": "0.2"})
    assert r.status_code in (302, 200)
    with ctx["app"].app_context():
        assert db.session.get(FringeConfig, S["fid"]).label == "Edited"


# ── 2. maintenance page ──────────────────────────────────────────────────
def test_maintenance_is_super_admin_only(ctx):
    c = ctx["client"]
    _login(c, S["viewer"])
    assert c.get("/admin/maintenance").status_code == 403
    assert c.get("/admin/maintenance.json").status_code == 403
    _login(c, S["admin"])
    assert c.get("/admin/maintenance").status_code == 403
    _login(c, S["su"])
    r = c.get("/admin/maintenance")
    assert r.status_code == 200
    html = r.data.decode()
    for t in maint.TOOLS:
        assert f'id="tool-{t["id"]}"' in html, t["id"]
    j = c.get("/admin/maintenance.json").get_json()
    assert len(j["tools"]) == len(maint.TOOLS)
    assert "qbo" in j["status"]
    # The registry points only at routes that exist.
    rules = {r.rule for r in ctx["app"].url_map.iter_rules()}
    for t in maint.TOOLS:
        rule = (t.get("preview") or {}).get("url") or t["url"]
        rule = rule.replace("{pid}", "<int:pid>").replace("{bid}", "<int:bid>")
        assert rule in rules, (t["id"], rule)
        if t.get("preview", {}).get("url"):
            assert t["url"] in rules, t["id"]


def test_admin_hub_renders_for_both_admin_roles(ctx):
    c = ctx["client"]
    _login(c, S["admin"])
    r = c.get("/admin")
    assert r.status_code == 200 and b'id="libraries"' in r.data and b'id="company"' not in r.data
    _login(c, S["su"])
    r = c.get("/admin")
    assert r.status_code == 200 and b'id="company"' in r.data and b'id="maintenance"' in r.data


# ── 3. removed routes ────────────────────────────────────────────────────
REMOVED = [
    ("POST", "/admin/catalog/reseed"),
    ("POST", "/admin/catalog/purge-legacy-duplicates"),
    ("POST", "/admin/catalog/rehouse-staff-from/2600"),
    ("POST", "/admin/catalog/wipe-and-reseed"),
    ("POST", "/admin/catalog/repair-codes"),
    ("GET", "/admin/repair-date-years"),
    ("GET", "/docs/repair-extensions"),
    ("POST", "/docs/1/backfill-card4"),
    ("POST", "/docs/1/relabel-subtype"),
    ("POST", "/docs/1/reconcile-filing"),
    ("GET", "/projects/1/docs/reset-person-invoices"),
    ("GET", "/admin/veryfi/classify/1"),
    ("POST", "/projects/1/budget/1/from-template"),
    ("POST", "/projects/1/budget/1/set-active"),
    ("GET", "/projects/1/budget/1/estimate/shares"),
    ("GET", "/projects/1/notify-preview"),
    ("POST", "/projects/1/budget/1/gantt/expand"),
    ("GET", "/projects/1/budget/1/callsheet/contacts"),
    ("GET", "/api/v1/docs/1/status"),
]


def test_removed_one_shot_routes_404(ctx):
    c = ctx["client"]
    _login(c, S["su"])
    for method, url in REMOVED:
        r = c.open(url, method=method, headers={"X-CSRFToken": "t"})
        assert r.status_code == 404, (method, url, r.status_code)


# ── 4. vendors audit ─────────────────────────────────────────────────────
def test_vendors_audit_admin_only(ctx):
    c = ctx["client"]
    _login(c, S["viewer"])
    assert c.get("/vendors/audit").status_code == 403
    _login(c, S["admin"])
    assert c.get("/vendors/audit").status_code == 200
