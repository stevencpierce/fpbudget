"""Budget row menu — duplicate a sub-header block / a whole section
(2026-11-03), end to end on sqlite with the Flask test client.

One endpoint, /line/<lid>/duplicate with body scope = line | block | section:

  1. block: a sub-header duplicates itself AND the lines under it (down to
     the next header / spacer), the copies land directly below the block,
     in order, names kept; internal notes copy, the discussion flag does not
  2. section: every row of the section (sub-headers, lines, spacers, kit-fee
     children) is appended at the end of that section; children follow the
     copied parent; computed rows are never copied
  3. block on a plain line → 400; an unknown scope → 400
  4. a viewer cannot duplicate (the write gate)

Run: python -m pytest tests/unit/test_row_menu_duplicate.py -q --confcutdir=tests/unit -o addopts=""
"""
import json
import os
import sys
import tempfile

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-rowmenu-")
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
                        BudgetComment)
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


def _line(bid, code, desc, order, **kw):
    base = dict(budget_id=bid, account_code=code, account_name="Camera", description=desc,
                is_labor=False, quantity=1, days=1, rate=0, rate_type="flat_day",
                sort_order=order, estimated_total=0)
    base.update(kw)
    ln = BudgetLine(**base)
    db.session.add(ln)
    db.session.flush()
    return ln


@pytest.fixture(scope="module")
def ctx():
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        owner = _user("rowmenu-owner@thefp.tv", "line_producer")
        viewer = _user("rowmenu-viewer@thefp.tv", "line_producer")
        p = ProjectSheet(name="Row menu duplicate", status="active")
        db.session.add(p)
        db.session.flush()
        db.session.add_all([
            ProjectAccess(project_id=p.id, user_id=owner.id, role="owner"),
            ProjectAccess(project_id=p.id, user_id=viewer.id, role="viewer"),
        ])
        b = Budget(project_id=p.id, name="RM Working", budget_mode="working",
                   version_status="current", version_number=1)
        db.session.add(b)
        db.session.flush()
        # Section 2601: H1 / A / B / C / spacer / D — then a managed fringe row
        h1 = _line(b.id, 2601, "CAMERA PACKAGE", 0, line_tag="header",
                   note=json.dumps({"bold": True, "color": "#5b8af9"}))
        a = _line(b.id, 2601, "Alexa body", 1, estimated_total=1000, discussion_note="talk")
        bb = _line(b.id, 2601, "Lens set", 2, estimated_total=2000)
        c = _line(b.id, 2601, "Media", 3, estimated_total=300)
        sp = _line(b.id, 2601, "", 4, line_tag="spacer")
        d = _line(b.id, 2601, "Monitor", 5, estimated_total=400)
        kit = _line(b.id, 2601, "Kit Fee", 6, estimated_total=50, line_tag="kit_fee",
                    parent_line_id=d.id)
        managed = _line(b.id, 2601, "Payroll Fringes", 7, line_tag="fringe_rollup",
                        fringe_for_section=2000)
        db.session.add(BudgetComment(project_id=p.id, budget_id=b.id, budget_line_id=bb.id,
                                     author_id=owner.id, body="ask about the rental rate"))
        db.session.commit()
        S.update(owner=owner.id, viewer=viewer.id, pid=p.id, bid=b.id,
                 h1=h1.id, a=a.id, b=bb.id, c=c.id, sp=sp.id, d=d.id, kit=kit.id,
                 managed=managed.id)
    c = app.test_client()
    return {"app": app, "client": c}


def _login(c, uid):
    with c.session_transaction() as s:
        s["_user_id"] = str(uid)
        s["_fresh"] = True
        s["_csrf"] = "t"


def _dup(c, lid, **body):
    return c.post(f"/projects/{S['pid']}/budget/{S['bid']}/line/{lid}/duplicate",
                  data=json.dumps(body), content_type="application/json",
                  headers={"X-CSRFToken": "t"})


def _section_rows(code=2601):
    return (BudgetLine.query.filter_by(budget_id=S["bid"], account_code=code)
            .order_by(BudgetLine.sort_order, BudgetLine.id).all())


# ── 1. block ─────────────────────────────────────────────────────────────
def test_block_duplicates_header_and_its_lines_below_the_block(ctx):
    c = ctx["client"]
    _login(c, S["owner"])
    r = _dup(c, S["h1"], scope="block")
    assert r.status_code == 200, r.data
    j = r.get_json()
    assert j["ok"] and j["scope"] == "block" and j["count"] == 4 and len(j["ids"]) == 4
    with ctx["app"].app_context():
        rows = _section_rows()
        descs = [(ln.description, ln.line_tag) for ln in rows]
        # H1 A B C | H1' A' B' C' | spacer D kit managed — the copies sit
        # directly below the block, in order, names kept.
        assert descs[:8] == [("CAMERA PACKAGE", "header"), ("Alexa body", None),
                             ("Lens set", None), ("Media", None),
                             ("CAMERA PACKAGE", "header"), ("Alexa body", None),
                             ("Lens set", None), ("Media", None)]
        assert descs[8] == ("", "spacer")
        assert [ln.sort_order for ln in rows] == list(range(len(rows)))
        new = rows[4:8]
        assert [ln.id for ln in new] == j["ids"]
        assert new[0].note == json.dumps({"bold": True, "color": "#5b8af9"})   # formatting kept
        assert [float(ln.estimated_total or 0) for ln in new] == [0, 1000, 2000, 300]
        assert new[1].discussion_note is None          # the flag does not copy
        # The internal note on "Lens set" copied to its clone.
        copied_notes = BudgetComment.query.filter_by(budget_line_id=new[2].id).all()
        assert [n.body for n in copied_notes] == ["ask about the rental rate"]
        assert BudgetComment.query.filter_by(budget_line_id=S["b"]).count() == 1
        S["block_ids"] = j["ids"]


# ── 2. section ───────────────────────────────────────────────────────────
def test_section_appends_every_row_and_remaps_children(ctx):
    c = ctx["client"]
    _login(c, S["owner"])
    with ctx["app"].app_context():
        before = _section_rows()
        n_before = len(before)
        copyable = [ln for ln in before if ln.line_tag not in ("fringe_rollup", "auto_calc")]
    r = _dup(c, S["d"], scope="section")      # any line of the section works
    assert r.status_code == 200, r.data
    j = r.get_json()
    assert j["count"] == len(copyable) == n_before - 1
    with ctx["app"].app_context():
        rows = _section_rows()
        assert len(rows) == n_before + len(copyable)
        assert [ln.sort_order for ln in rows] == list(range(len(rows)))
        tail = rows[n_before:]
        assert [ln.id for ln in tail] == j["ids"]
        assert [ln.description for ln in tail] == [ln.description for ln in copyable]
        assert [ln.line_tag for ln in tail] == [ln.line_tag for ln in copyable]
        # Exactly one managed row — never copied.
        assert sum(1 for ln in rows if ln.line_tag == "fringe_rollup") == 1
        # The kit-fee child follows the COPIED "Monitor", not the original.
        new_monitor = next(ln for ln in tail if ln.description == "Monitor")
        new_kit = next(ln for ln in tail if ln.line_tag == "kit_fee")
        assert new_kit.parent_line_id == new_monitor.id
        assert db.session.get(BudgetLine, S["kit"]).parent_line_id == S["d"]


# ── 3. argument checks ───────────────────────────────────────────────────
def test_block_needs_a_sub_header_and_scope_is_validated(ctx):
    c = ctx["client"]
    _login(c, S["owner"])
    assert _dup(c, S["a"], scope="block").status_code == 400
    assert _dup(c, S["a"], scope="department").status_code == 400
    # Plain line duplicate still lands directly below its source.
    r = _dup(c, S["a"], scope="line", duplicate_schedule=False)
    assert r.status_code == 200, r.data
    with ctx["app"].app_context():
        rows = _section_rows()
        i = next(k for k, ln in enumerate(rows) if ln.id == S["a"])
        assert rows[i + 1].id == r.get_json()["id"]
        assert rows[i + 1].description == "Alexa body"


# ── 4. viewer ────────────────────────────────────────────────────────────
def test_viewer_cannot_duplicate(ctx):
    c = ctx["client"]
    _login(c, S["viewer"])
    assert _dup(c, S["h1"], scope="block").status_code == 403
