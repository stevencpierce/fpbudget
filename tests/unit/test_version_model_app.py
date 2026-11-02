"""Cleanup C — the version model, end to end on sqlite with the Flask test
client (2026-11-02). Boots the real app against a throw-away sqlite file and
checks the pieces that need a database:

  1. get_current_working_budget / get_current_estimated_budget /
     get_live_budget follow the "newest of its kind" rule, and a project
     with Estimated v2 + Working v1 reports (v2, v1)
  2. _run_budget_mismatch_scan flags a line on the LIVE Working (it used
     to look for a current Actual budget and never fired)
  3. the budget page renders the three version-strip states and the
     Viewing toggle enables Working only for a version that has one
  4. a legacy is_actual budget URL redirects to the live budget
  5. the sub-budget save ignores a pinned budget_id

Skipped automatically where app.py's runtime deps are not installed.
Run: python -m pytest tests/unit/test_version_model_app.py -q --confcutdir=tests/unit -o addopts=""
"""
import json
import os
import sys
import tempfile

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-version-")
_PRESET = os.environ.get("DATABASE_URL", "")
if _PRESET and not _PRESET.startswith("sqlite:"):
    pytest.skip("DATABASE_URL is not sqlite; refusing to boot", allow_module_level=True)
os.environ.setdefault("DATABASE_URL", "sqlite:///" + os.path.join(_DB_DIR, "t.db"))
os.environ.setdefault("SECRET_KEY", "test")

try:
    _cwd = os.getcwd()
    os.chdir(ROOT)
    import app as fpapp                       # noqa: E402
    from models import (db, User, ProjectSheet, ProjectAccess, Budget, BudgetLine,   # noqa: E402
                        Transaction, AnomalyFlag, SubBudget)
    import actuals                            # noqa: E402
    import routes.actuals                     # noqa: F401,E402
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
def vctx():
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        su = _user("steven@thefp.tv", "super_admin")
        # Project A: Estimated v1 (superseded) + Working v1 (current) + Estimated v2 (current) → state B
        p = ProjectSheet(name="Cleanup C versions", status="active")
        db.session.add(p)
        db.session.flush()
        db.session.add(ProjectAccess(project_id=p.id, user_id=su.id, role="owner"))
        e1 = Budget(project_id=p.id, name="Est v1", budget_mode="estimated", version_status="superseded", version_number=1)
        w1 = Budget(project_id=p.id, name="Work v1", budget_mode="working", version_status="current", version_number=1)
        e2 = Budget(project_id=p.id, name="Est v2", budget_mode="estimated", version_status="current", version_number=2)
        legacy_actual = Budget(project_id=p.id, name="Actual (legacy)", budget_mode="working",
                               version_status="archived", version_number=1, is_actual=True)
        db.session.add_all([e1, w1, e2, legacy_actual])
        db.session.flush()
        ln = BudgetLine(budget_id=w1.id, account_code=2601, account_name="Camera Equipment",
                        description="Lens", is_labor=False, quantity=1, days=1, rate=100,
                        rate_type="flat_day", estimated_total=100, sort_order=1)
        db.session.add(ln)
        db.session.flush()
        # $400 coded to a $100 line → over by $300 and 300%
        db.session.add(Transaction(project_id=p.id, source='qbo_sync', amount=400, is_expense=True,
                                   txn_date="2026-10-01", vendor="Lensrentals", match_status='confirmed',
                                   budget_line_id=ln.id, account_code=2601))
        # Project C: Estimated v1 only → state C
        p3 = ProjectSheet(name="Cleanup C no working", status="active")
        db.session.add(p3)
        db.session.flush()
        db.session.add(ProjectAccess(project_id=p3.id, user_id=su.id, role="owner"))
        e3 = Budget(project_id=p3.id, name="Est only", budget_mode="estimated", version_status="current", version_number=1)
        db.session.add(e3)
        db.session.commit()
        S.update(su=su.id, pid=p.id, e1=e1.id, w1=w1.id, e2=e2.id, actual=legacy_actual.id, lid=ln.id,
                 pid3=p3.id, e3=e3.id)
    c = app.test_client()
    return {"app": app, "client": c}


def _login(c, uid):
    with c.session_transaction() as s:
        s["_user_id"] = str(uid)
        s["_fresh"] = True
        s["_csrf"] = "t"


def _post(c, url, payload=None):
    return c.post(url, data=json.dumps(payload or {}), content_type="application/json",
                  headers={"X-CSRFToken": "t"})


# ── 1. the one "current" rule ────────────────────────────────────────────
def test_current_helpers_follow_newest_of_its_kind(vctx):
    with vctx["app"].app_context():
        w = actuals.get_current_working_budget(S["pid"])
        e = actuals.get_current_estimated_budget(S["pid"])
        assert w.id == S["w1"] and e.id == S["e2"]
        assert actuals.get_live_budget(S["pid"]).id == S["w1"]
        # the legacy Actual (is_actual) never wins even though it is "working"
        assert actuals.get_current_working_budget(S["pid"]).is_actual is False
        # a project with no Working: the Estimated is the live budget
        assert actuals.get_current_working_budget(S["pid3"]) is None
        assert actuals.get_live_budget(S["pid3"]).id == S["e3"]
        assert actuals.version_strip_state(e, w) == 'ahead'


# ── 2. the mismatch scan targets the live Working ────────────────────────
def test_mismatch_scan_flags_a_line_on_the_live_working(vctx):
    with vctx["app"].app_context():
        n = fpapp._run_budget_mismatch_scan(S["pid"])
        db.session.commit()
        assert n >= 1
        flags = AnomalyFlag.query.filter_by(project_id=S["pid"], type='budget_mismatch').all()
        payloads = [json.loads(f.payload_json or '{}') for f in flags]
        over = [p for p in payloads if p.get("kind") == "line_over_budget" and p.get("line_id") == S["lid"]]
        assert over and round(over[0]["over"], 2) == 300.0


# ── 3. the header: three strip states + the Viewing rule ─────────────────
def test_budget_page_renders_state_b_and_the_viewing_rule(vctx):
    c = vctx["client"]
    _login(c, S["su"])
    r = c.get(f"/projects/{S['pid']}/budget/{S['e2']}")
    assert r.status_code == 200
    html = r.data.decode()
    assert 'data-state="ahead"' in html
    assert "Spend is tracked on Working v1." in html
    assert "Estimated v2 has no Working yet" in html
    assert "Start Working v2" in html
    # Estimated v2 has no Working → the Working segment is disabled
    assert 'No Working budget for v2 yet' in html
    # viewing Working v1 → same strip (built from the LIVE pair), Working segment live
    r = c.get(f"/projects/{S['pid']}/budget/{S['w1']}")
    html = r.data.decode()
    assert 'data-state="ahead"' in html
    assert 'No Working budget for v1 yet' not in html
    # the version dropdown no longer offers "+ Initialize Working"
    assert "Initialize Working" not in html


def test_budget_page_renders_state_c(vctx):
    c = vctx["client"]
    _login(c, S["su"])
    r = c.get(f"/projects/{S['pid3']}/budget/{S['e3']}")
    assert r.status_code == 200
    html = r.data.decode()
    assert 'data-state="none"' in html
    assert "Nothing is tracked yet" in html and "Start Working v1" in html
    assert 'No Working budget for v1 yet' in html


# ── 4. a legacy Actual URL redirects to the live budget ──────────────────
def test_legacy_actual_url_redirects(vctx):
    c = vctx["client"]
    _login(c, S["su"])
    r = c.get(f"/projects/{S['pid']}/budget/{S['actual']}")
    assert r.status_code == 302
    assert r.headers["Location"].endswith(f"/projects/{S['pid']}/budget/{S['w1']}")


# ── 5. sub-budgets are always on the current Working ─────────────────────
def test_sub_budget_save_ignores_a_pinned_budget_id(vctx):
    c = vctx["client"]
    _login(c, S["su"])
    r = _post(c, f"/projects/{S['pid']}/sub-budgets/save", {"name": "Day 2", "budget_id": S["e1"]})
    assert r.status_code == 200, r.data
    with vctx["app"].app_context():
        sb = SubBudget.query.filter_by(project_id=S["pid"], name="Day 2").first()
        assert sb is not None and sb.budget_id is None
