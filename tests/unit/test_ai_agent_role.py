"""AI Agent role (`ai_agent`, displayed "AI Agent") — end to end on sqlite
with the Flask test client (2026-11-03).

The AI assistant's account (claude@thefp.tv) may see and edit every project
and run every maintenance / diagnostic tool, but never the owner powers:
users, roles, the QuickBooks connection and company account set, the
company profile, project deletes, mass-delete tools, the Sentry test.

Checks:
  1. helper semantics (is_admin excludes ai_agent; one helper per decision)
  2. AI Agent MAY: /admin, /admin/maintenance (+ .json), the ledger
     preview, /admin/errors, /admin/transactions, /admin/qbo-accounts (read),
     any project's budget + actuals (no ProjectAccess row needed)
  3. AI Agent MAY NOT: every /admin/users/* POST, QuickBooks connect /
     disconnect, POST /admin/qbo-accounts, project delete (+ bulk), Sentry
     test, company profile, docs wipe, Dropbox trash purge, project ACLs
  4. super_admin and admin unchanged
  5. the badge: activity log JSON + the rendered budget overview carry the
     "AI Agent" badge for an ai_agent actor; the header shows the session
     pill; the maintenance run is audited in activity_log

Run: python -m pytest tests/unit/test_ai_agent_role.py -q --confcutdir=tests/unit -o addopts=""
"""
import json
import os
import sys
import tempfile

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-aiagent-")
_PRESET = os.environ.get("DATABASE_URL", "")
if _PRESET and not _PRESET.startswith("sqlite:"):
    pytest.skip("DATABASE_URL is not sqlite; refusing to boot", allow_module_level=True)
os.environ.setdefault("DATABASE_URL", "sqlite:///" + os.path.join(_DB_DIR, "t.db"))
os.environ.setdefault("SECRET_KEY", "test")

try:
    _cwd = os.getcwd()
    os.chdir(ROOT)
    import app as fpapp                       # noqa: E402
    from models import (db, User, ProjectSheet, ProjectAccess, Budget,   # noqa: E402
                        ActivityLog)
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


def _user(email, role, name=None):
    u = User.query.filter_by(email=email).first()
    if not u:
        u = User(email=email, name=name or email.split("@")[0], role=role)
        db.session.add(u)
    u.role = role
    u.is_active = True
    u.must_change_password = False
    u.set_password("pw")
    db.session.flush()
    return u


@pytest.fixture(scope="module")
def ctx():
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        su = _user("ai-super@thefp.tv", "super_admin")
        admin = _user("ai-admin@thefp.tv", "admin")
        lp = _user("ai-lp@thefp.tv", "line_producer")
        agent = _user("ai-claude@thefp.tv", "ai_agent", name="Claude")
        victim = _user("ai-victim@thefp.tv", "line_producer")
        p = ProjectSheet(name="AI Agent access", status="active")
        db.session.add(p)
        db.session.flush()
        # NO ProjectAccess row for the agent: it must reach the project anyway.
        db.session.add(ProjectAccess(project_id=p.id, user_id=lp.id, role="editor"))
        b = Budget(project_id=p.id, name="AI Working", budget_mode="working",
                   version_status="current", version_number=1)
        db.session.add(b)
        db.session.flush()
        db.session.add(ActivityLog(project_id=p.id, budget_id=b.id, user_id=agent.id,
                                   action="update", entity_type="budget_line",
                                   entity_label="Camera package", note="AI test edit"))
        db.session.commit()
        S.update(su=su.id, admin=admin.id, lp=lp.id, agent=agent.id, victim=victim.id,
                 pid=p.id, bid=b.id)
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


# ── 1. helpers ───────────────────────────────────────────────────────────
def test_helpers_one_decision_each():
    a = User(email="x@x", role="ai_agent")
    assert a.is_ai_agent and not a.is_admin and not a.is_super_admin
    assert a.has_all_project_access and a.can_run_maintenance and a.can_open_admin
    assert a.display_role == "AI Agent"
    a.name = "Claude"
    assert a.actor_label == "Claude · AI Agent"
    ad = User(email="y@y", role="admin", name="Ann")
    assert ad.is_admin and ad.has_all_project_access and ad.can_open_admin
    assert not ad.can_run_maintenance and not ad.is_ai_agent and ad.actor_label == "Ann"
    su = User(email="z@z", role="super_admin")
    assert su.is_super_admin and su.is_admin and su.can_run_maintenance
    lp = User(email="w@w", role="line_producer")
    assert not (lp.has_all_project_access or lp.can_run_maintenance or lp.can_open_admin)


# ── 2. what the AI Agent may do ──────────────────────────────────────────
def test_agent_may_open_admin_and_maintenance(ctx):
    c = ctx["client"]
    _login(c, S["agent"])
    r = c.get("/admin")
    assert r.status_code == 200
    html = r.data.decode()
    assert 'id="maintenance"' in html and 'id="company"' not in html
    assert "/admin/sentry-test" not in html          # no Sentry test button
    assert "Create New User" not in html             # no user management
    r = c.get("/admin/maintenance")
    assert r.status_code == 200
    html = r.data.decode()
    for t in maint.TOOLS:
        assert f'id="tool-{t["id"]}"' in html, t["id"]
    assert "Super Admin only" in html               # wipe / trash purge cards locked
    assert c.get("/admin/maintenance.json").status_code == 200


def test_agent_may_run_ledger_preview_errors_transactions(ctx):
    c = ctx["client"]
    _login(c, S["agent"])
    assert c.get("/admin/ledger/preview").status_code == 200
    assert c.get("/admin/ledger/merge-log.json").status_code == 200
    assert c.get("/admin/ledger/conflicts.json").status_code == 200
    assert c.get("/admin/errors").status_code == 200
    assert c.get("/admin/transactions.json").status_code == 200
    assert c.get("/admin/qbo-accounts").status_code == 200
    assert c.get("/admin/qbo-accounts.json").status_code == 200


def test_agent_may_open_any_project_budget_and_actuals(ctx):
    c = ctx["client"]
    _login(c, S["agent"])
    assert c.get(f"/projects/{S['pid']}/budget/{S['bid']}").status_code == 200
    assert c.get(f"/projects/{S['pid']}/actuals").status_code == 200
    assert c.get(f"/projects/{S['pid']}/actuals/workspace.json").status_code == 200
    j = c.get(f"/projects/{S['pid']}/budget/{S['bid']}/activity").get_json()
    assert j is not None


def test_agent_maintenance_run_is_audited(ctx):
    c = ctx["client"]
    _login(c, S["agent"])
    with ctx["app"].app_context():
        before = ActivityLog.query.filter_by(user_id=S["agent"],
                                             entity_type="maintenance_tool").count()
    assert c.get("/admin/ledger/preview").status_code == 200
    with ctx["app"].app_context():
        rows = (ActivityLog.query.filter_by(user_id=S["agent"], entity_type="maintenance_tool")
                .order_by(ActivityLog.id.desc()).all())
        assert len(rows) == before + 1
        assert "AI Agent ran maintenance tool" in rows[0].note
        assert "/admin/ledger/preview" in rows[0].note


# ── 3. what the AI Agent may NOT do ──────────────────────────────────────
def test_agent_403_on_user_admin(ctx):
    c = ctx["client"]
    _login(c, S["agent"])
    v = S["victim"]
    assert _post_form(c, "/admin/users/create", {"email": "new@x.tv", "role": "super_admin"}).status_code == 403
    assert _post_form(c, f"/admin/users/{v}/edit", {"role": "super_admin"}).status_code == 403
    assert _post_form(c, f"/admin/users/{S['agent']}/edit", {"role": "super_admin"}).status_code == 403
    assert _post_form(c, f"/admin/users/{v}/toggle-active").status_code == 403
    assert _post_form(c, f"/admin/users/{v}/reset-password").status_code == 403
    assert _post_form(c, f"/admin/users/{S['su']}/delete").status_code == 403
    assert _post_form(c, f"/admin/projects/{S['pid']}/access", {"user_id": v, "role": "owner"}).status_code in (302, 403)
    with ctx["app"].app_context():
        assert db.session.get(User, S["agent"]).role == "ai_agent"
        assert db.session.get(User, S["su"]) is not None
        assert db.session.get(User, v).role == "line_producer" and db.session.get(User, v).is_active
        assert User.query.filter_by(email="new@x.tv").first() is None
        assert ProjectAccess.query.filter_by(project_id=S["pid"], user_id=v).first() is None


def test_agent_403_on_project_acl(ctx):
    c = ctx["client"]
    _login(c, S["agent"])
    assert _post_form(c, f"/projects/{S['pid']}/share", {"email": "ai-victim@thefp.tv"}).status_code == 403
    assert _post_form(c, f"/projects/{S['pid']}/share/role", {"user_id": S["lp"], "role": "owner"}).status_code == 403
    assert _post_form(c, f"/projects/{S['pid']}/share/remove", {"user_id": S["lp"]}).status_code == 403


def test_agent_403_on_quickbooks_connection_and_account_set(ctx):
    c = ctx["client"]
    _login(c, S["agent"])
    assert c.get("/qbo/oauth/start").status_code == 403
    assert _post_json(c, "/qbo/oauth/disconnect").status_code == 403
    assert _post_json(c, "/admin/qbo-accounts", {"account_ids": []}).status_code == 403


def test_agent_403_on_destructive_and_owner_only(ctx):
    c = ctx["client"]
    _login(c, S["agent"])
    assert _post_form(c, f"/projects/{S['pid']}/delete").status_code == 403
    assert _post_form(c, "/projects/bulk-delete", {"project_ids": S["pid"]}).status_code == 403
    assert c.get("/admin/sentry-test").status_code == 403
    assert c.get("/settings/company").status_code == 403
    assert _post_json(c, "/settings/company", {"company_name": "x"}).status_code == 403
    assert _post_json(c, f"/admin/docs/project/{S['pid']}/wipe").status_code == 403
    assert _post_json(c, "/admin/trash/purge").status_code == 403
    assert _post_json(c, "/admin/qbo-imports/purge?project=pool&apply=1").status_code == 403
    with ctx["app"].app_context():
        assert db.session.get(ProjectSheet, S["pid"]) is not None


# ── 4. super_admin / admin unchanged ─────────────────────────────────────
def test_super_admin_unchanged(ctx):
    c = ctx["client"]
    _login(c, S["su"])
    assert c.get("/admin/maintenance").status_code == 200
    assert c.get("/admin/ledger/preview").status_code == 200
    assert c.get("/admin/errors").status_code == 200
    assert c.get("/settings/company").status_code == 200
    r = c.get("/admin")
    assert r.status_code == 200 and b'id="company"' in r.data
    # Only the super admin is offered the AI Agent role.
    assert "AI Agent — Claude (limited admin)" in r.data.decode()


def test_admin_unchanged(ctx):
    c = ctx["client"]
    _login(c, S["admin"])
    assert c.get("/admin/maintenance").status_code == 403
    assert c.get("/admin/ledger/preview").status_code == 403
    assert c.get("/admin/errors").status_code == 403
    r = c.get("/admin")
    assert r.status_code == 200 and b'id="company"' not in r.data
    assert "AI Agent — Claude (limited admin)" not in r.data.decode()
    assert c.get(f"/projects/{S['pid']}/budget/{S['bid']}").status_code == 200
    assert c.get("/admin/ledger/conflicts.json").status_code == 200
    # An admin cannot mint or take over the AI Agent account.
    assert _post_form(c, "/admin/users/create", {"email": "agent2@x.tv", "role": "ai_agent"}).status_code == 302
    assert _post_form(c, f"/admin/users/{S['agent']}/reset-password").status_code == 302
    with ctx["app"].app_context():
        assert User.query.filter_by(email="agent2@x.tv").first() is None


def test_line_producer_still_refused(ctx):
    c = ctx["client"]
    _login(c, S["lp"])
    assert c.get("/admin/maintenance").status_code == 403
    assert _post_form(c, f"/admin/users/{S['victim']}/toggle-active").status_code == 403


# ── 5. the AI Agent badge ────────────────────────────────────────────────
def test_activity_log_renders_ai_agent_badge(ctx):
    c = ctx["client"]
    _login(c, S["su"])
    j = c.get(f"/projects/{S['pid']}/budget/{S['bid']}/activity").get_json()
    items = j.get("items") or []
    mine = [it for it in items if it.get("who_id") == S["agent"]]
    assert mine, items
    assert all(it["who_ai_agent"] is True for it in mine)
    assert all(it["who_ai_agent"] is False for it in items if it.get("who_id") != S["agent"])
    # The Activity tab renderer appends the shared badge after the name.
    js = open(os.path.join(ROOT, "static", "budget-js", "b14.js")).read()
    assert "${whoEsc}${FP.aiBadge(it.who_ai_agent)}" in js
    utils = open(os.path.join(ROOT, "static", "fp-utils.js")).read()
    assert "window.FP.aiBadge = aiBadge" in utils and "AI Agent</span>" in utils
    # Server-rendered "recent activity" on the budget overview shows it too.
    html = c.get(f"/projects/{S['pid']}/budget/{S['bid']}").data.decode()
    assert 'class="ai-agent-badge"' in html


def test_ai_badge_global_and_session_pill(ctx):
    with ctx["app"].app_context():
        assert "AI Agent" in str(fpapp._ai_badge(db.session.get(User, S["agent"])))
        assert str(fpapp._ai_badge(db.session.get(User, S["su"]))) == ""
    c = ctx["client"]
    _login(c, S["agent"])
    html = c.get("/").data.decode()
    assert "AI Agent session" in html
    _login(c, S["su"])
    assert "AI Agent session" not in c.get("/").data.decode()
    # The user list shows the distinct badge on the agent's row.
    r = c.get("/admin").data.decode()
    assert "role-ai_agent" in r
