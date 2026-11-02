"""Cleanup A — safety, end to end on sqlite with the Flask test client
(2026-11-02).

Boots the real app against a throw-away sqlite file (or reuses the one
another app-booting module in this process already opened) and walks the
data-loss / access fixes through their routes:

  1. docs wipe keeps bank charges (unlinked, coding intact) and deletes
     only the documents' own rows
  2. a receipt edit writes through to the doc's own row, never the bank row
  3. a project viewer gets 403 on /docs/upload/<uid>/update
  4. delete_budget: editor 403; coded spend → 409 needs_force; force=1
     deletes and the transactions survive uncoded
  5. review-queue resolve: delete_txn on a bank row → 400; a transaction
     the flag isn't about → 400
  6. amount edit on a bank row → 409
  7. delete-duplicate without a twin → 409; with a twin → deleted
  8. cron: ?token= refused (401), X-Cron-Token accepted
  9. a password change revokes the user's API tokens
 10. QBO purge: admin 403, super_admin without a project scope 400
 11. a locked version refuses delete_line through the mobile API too

Skipped automatically where app.py's runtime deps are not installed.
Run: python -m pytest tests/unit/test_safety_app.py -q --confcutdir=tests/unit -o addopts=""
"""
import json
import os
import sys
import tempfile
from datetime import datetime

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-safety-")
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
                        DocUpload, Transaction, TransactionSplit, ApiToken, AnomalyFlag)
    import api_auth                           # noqa: E402
    import routes.actuals                     # noqa: F401,E402
    import routes.actuals_admin               # noqa: F401,E402
    import routes.api_v1                      # noqa: F401,E402
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


@pytest.fixture(scope="module")
def safety_mp():
    from _pytest.monkeypatch import MonkeyPatch
    m = MonkeyPatch()
    yield m
    m.undo()


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
def safety_ctx(safety_mp):
    app = fpapp.app
    app.config["TESTING"] = True
    # No Dropbox in tests: the wipe / delete paths must not need it.
    for k in ("DROPBOX_REFRESH_TOKEN", "DROPBOX_APP_KEY", "DROPBOX_ACCESS_TOKEN"):
        safety_mp.delenv(k, raising=False)
    with app.app_context():
        db.create_all()
        su = _user("steven@thefp.tv", "super_admin")
        admin = _user("safety-admin@thefp.tv", "admin")
        owner = _user("safety-owner@thefp.tv", "line_producer")
        editor = _user("safety-editor@thefp.tv", "line_producer")
        viewer = _user("safety-viewer@thefp.tv", "line_producer")
        p = ProjectSheet(name="Cleanup A safety", status="active")
        db.session.add(p)
        db.session.flush()
        db.session.add_all([
            ProjectAccess(project_id=p.id, user_id=owner.id, role="owner"),
            ProjectAccess(project_id=p.id, user_id=editor.id, role="editor"),
            ProjectAccess(project_id=p.id, user_id=viewer.id, role="viewer"),
        ])
        est = Budget(project_id=p.id, name="CA Estimated", budget_mode="estimated",
                     version_status="current", version_number=1)
        b = Budget(project_id=p.id, name="CA Working", budget_mode="working",
                   version_status="current", version_number=1)
        db.session.add_all([est, b])
        db.session.flush()
        ln = BudgetLine(budget_id=b.id, account_code=2601, account_name="Camera",
                        description="Lens", is_labor=False, quantity=1, days=1, rate=100,
                        rate_type="flat_day", sort_order=1)
        db.session.add(ln)
        db.session.commit()
        S.update(su=su.id, admin=admin.id, owner=owner.id, editor=editor.id, viewer=viewer.id,
                 pid=p.id, bid=b.id, est=est.id, lid=ln.id)
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


def _mk_txn(**kw):
    base = dict(project_id=S["pid"], source='qbo_sync', amount=100, is_expense=True,
                txn_date="2026-03-01", vendor="Lyft", match_status='unmatched')
    base.update(kw)
    t = Transaction(**base)
    db.session.add(t)
    db.session.flush()
    return t


def _mk_doc(**kw):
    base = dict(project_id=S["pid"], uploader_id=S["owner"], status="done", category="receipt",
                vendor="Lyft", amount=100, original_filename="r.pdf")
    base.update(kw)
    d = DocUpload(**base)
    db.session.add(d)
    db.session.flush()
    return d


# ── 1. docs wipe ─────────────────────────────────────────────────────────
def test_docs_wipe_keeps_bank_charges(safety_ctx):
    c = safety_ctx["client"]
    with safety_ctx["app"].app_context():
        d = _mk_doc()
        own = _mk_txn(source='doc_upload', doc_upload_id=d.id)
        bank = _mk_txn(source='qbo_sync', doc_upload_id=d.id, qbo_txn_id="q1",
                       qbo_txn_type="Purchase", budget_line_id=S["lid"], account_code=2601)
        manual = _mk_txn(source='manual_entry', vendor="Cash lunch")
        db.session.commit()
        ids = dict(doc=d.id, own=own.id, bank=bank.id, manual=manual.id)
    _login(c, S["su"])
    r = _post(c, f"/admin/docs/project/{S['pid']}/wipe")
    assert r.status_code == 200, r.data
    body = r.get_json()
    assert body["deleted_count"] == 1 and body["txn_deleted"] == 1 and body["txn_unlinked"] == 1
    with safety_ctx["app"].app_context():
        assert db.session.get(DocUpload, ids["doc"]) is None
        assert db.session.get(Transaction, ids["own"]) is None
        bank = db.session.get(Transaction, ids["bank"])
        assert bank is not None and bank.doc_upload_id is None
        assert bank.budget_line_id == S["lid"] and bank.account_code == 2601
        assert db.session.get(Transaction, ids["manual"]) is not None


def test_docs_wipe_is_super_admin_only(safety_ctx):
    c = safety_ctx["client"]
    _login(c, S["admin"])
    assert _post(c, f"/admin/docs/project/{S['pid']}/wipe").status_code == 403


# ── 2 / 3. receipt edit write-through + viewer access ───────────────────
def test_doc_update_writes_only_to_own_row(safety_ctx):
    c = safety_ctx["client"]
    with safety_ctx["app"].app_context():
        d = _mk_doc(amount=50, vendor="Shop")
        own = _mk_txn(source='doc_upload', doc_upload_id=d.id, amount=50, vendor="Shop")
        bank = _mk_txn(source='qbo_sync', doc_upload_id=d.id, amount=50, vendor="SHOP INC",
                       txn_date="2026-03-02")
        db.session.commit()
        ids = dict(doc=d.id, own=own.id, bank=bank.id)
    _login(c, S["owner"])
    r = _post(c, f"/docs/upload/{ids['doc']}/update", {"amount": 55, "vendor": "Shop Co", "doc_date": ""})
    assert r.status_code == 200, r.data
    with safety_ctx["app"].app_context():
        own = db.session.get(Transaction, ids["own"])
        bank = db.session.get(Transaction, ids["bank"])
        assert float(own.amount) == 55.0 and own.vendor == "Shop Co" and own.txn_date is None
        assert float(bank.amount) == 50.0 and bank.vendor == "SHOP INC" and bank.txn_date == "2026-03-02"


def test_viewer_cannot_edit_documents(safety_ctx):
    c = safety_ctx["client"]
    with safety_ctx["app"].app_context():
        d = _mk_doc()
        db.session.commit()
        did = d.id
    _login(c, S["viewer"])
    assert _post(c, f"/docs/upload/{did}/update", {"vendor": "x"}).status_code == 403
    assert _post(c, f"/docs/upload/{did}/reocr").status_code == 403
    assert _post(c, f"/docs/upload/{did}/parse-people").status_code == 403
    _login(c, S["editor"])
    assert _post(c, f"/docs/upload/{did}/update", {"vendor": "x"}).status_code == 200


# ── 4. delete_budget ─────────────────────────────────────────────────────
def test_delete_budget_role_and_force(safety_ctx):
    c = safety_ctx["client"]
    with safety_ctx["app"].app_context():
        extra = Budget(project_id=S["pid"], name="CA Working v2", budget_mode="working",
                       version_status="superseded", version_number=2)
        db.session.add(extra)
        db.session.flush()
        ln2 = BudgetLine(budget_id=extra.id, account_code=2601, account_name="Camera",
                         description="Body", is_labor=False, quantity=1, days=1, rate=100,
                         rate_type="flat_day", sort_order=1)
        db.session.add(ln2)
        db.session.flush()
        t = _mk_txn(budget_line_id=ln2.id, account_code=2601)
        db.session.commit()
        xb, tid = extra.id, t.id
    _login(c, S["editor"])
    assert _post(c, f"/projects/{S['pid']}/budget/{xb}/delete").status_code == 403
    _login(c, S["owner"])
    r = _post(c, f"/projects/{S['pid']}/budget/{xb}/delete")
    assert r.status_code == 409 and r.get_json()["needs_force"] and r.get_json()["coded_count"] == 1
    r = _post(c, f"/projects/{S['pid']}/budget/{xb}/delete?force=1")
    assert r.status_code == 200, r.data
    with safety_ctx["app"].app_context():
        assert db.session.get(Budget, xb) is None
        t = db.session.get(Transaction, tid)
        assert t is not None and t.budget_line_id is None


# ── 5. review-queue resolve ──────────────────────────────────────────────
def test_anomaly_resolve_refuses_bank_delete_and_foreign_txn(safety_ctx):
    c = safety_ctx["client"]
    with safety_ctx["app"].app_context():
        bank = _mk_txn(source='qbo_sync')
        other = _mk_txn(source='doc_upload')
        f = AnomalyFlag(project_id=S["pid"], type='double_coded', severity='high',
                        title='t', transaction_id=bank.id, resolved=False,
                        payload_json=json.dumps({"members": [{"tid": bank.id}]}))
        db.session.add(f)
        db.session.commit()
        fid, bid_, oid = f.id, bank.id, other.id
    _login(c, S["owner"])
    r = _post(c, f"/projects/{S['pid']}/anomalies/{fid}/resolve", {"action": "delete_txn", "transaction_id": bid_})
    assert r.status_code == 400 and "bank" in r.get_json()["error"].lower()
    r = _post(c, f"/projects/{S['pid']}/anomalies/{fid}/resolve", {"action": "uncode", "transaction_id": oid})
    assert r.status_code == 400 and "review item" in r.get_json()["error"]
    r = _post(c, f"/projects/{S['pid']}/anomalies/{fid}/resolve", {"action": "mark_not_project", "transaction_id": bid_})
    assert r.status_code == 200, r.data
    with safety_ctx["app"].app_context():
        assert db.session.get(Transaction, bid_).not_project_expense is True
        assert db.session.get(Transaction, oid) is not None


# ── 6. amount edit ───────────────────────────────────────────────────────
def test_amount_edit_refused_on_bank_row(safety_ctx):
    c = safety_ctx["client"]
    with safety_ctx["app"].app_context():
        bank = _mk_txn(source='qbo_sync', amount=80)
        man = _mk_txn(source='manual_entry', amount=80)
        db.session.commit()
        bid_, mid = bank.id, man.id
    _login(c, S["owner"])
    r = _post(c, f"/projects/{S['pid']}/actuals/transaction/{bid_}/edit", {"amount": 81})
    assert r.status_code == 409
    r = _post(c, f"/projects/{S['pid']}/actuals/transaction/{bid_}/edit", {"amount": 80, "note": "ok"})
    assert r.status_code == 200, r.data
    r = _post(c, f"/projects/{S['pid']}/actuals/transaction/{mid}/edit", {"amount": 81})
    assert r.status_code == 200, r.data


# ── 7. delete-duplicate ──────────────────────────────────────────────────
def test_delete_duplicate_needs_a_twin(safety_ctx):
    c = safety_ctx["client"]
    with safety_ctx["app"].app_context():
        lone = _mk_txn(source='csv_import', amount=12.34, vendor="Solo Vendor", txn_date="2026-04-04")
        a = _mk_txn(source='csv_import', amount=56.78, vendor="Twin Vendor", txn_date="2026-04-05")
        b = _mk_txn(source='csv_import', amount=56.78, vendor="Twin Vendor", txn_date="2026-04-05")
        db.session.commit()
        lone_id, a_id, b_id = lone.id, a.id, b.id
    _login(c, S["owner"])
    r = _post(c, f"/projects/{S['pid']}/actuals/transaction/{lone_id}/delete-duplicate")
    assert r.status_code == 409
    r = _post(c, f"/projects/{S['pid']}/actuals/transaction/{b_id}/delete-duplicate")
    assert r.status_code == 200, r.data
    with safety_ctx["app"].app_context():
        assert db.session.get(Transaction, lone_id) is not None
        assert db.session.get(Transaction, a_id) is not None
        assert db.session.get(Transaction, b_id) is None


# ── 8. cron token ────────────────────────────────────────────────────────
def test_cron_token_header_only(safety_ctx, safety_mp):
    c = safety_ctx["client"]
    safety_mp.setenv("CRON_TOKEN", "sekrit")
    r = c.post("/cron/reprocess-unpaired?token=sekrit")
    assert r.status_code == 401
    r = c.post("/cron/reprocess-unpaired", headers={"X-Cron-Token": "wrong"})
    assert r.status_code == 401
    r = c.post("/cron/reprocess-unpaired?pid=999999", headers={"X-Cron-Token": "sekrit"})
    assert r.status_code != 401


# ── 9. API tokens revoked on password change ─────────────────────────────
def test_password_change_revokes_api_tokens(safety_ctx):
    c = safety_ctx["client"]
    with safety_ctx["app"].app_context():
        raw = api_auth.generate_token()
        db.session.add(ApiToken(user_id=S["editor"], token_hash=api_auth.hash_token(raw),
                                device_name="phone"))
        db.session.commit()
    _login(c, S["editor"])
    r = c.post("/profile", data={"action": "password", "current_password": "pw",
                                 "new_password": "newpassword1", "confirm_password": "newpassword1",
                                 "csrf_token": "t"})
    assert r.status_code in (200, 302), r.data
    with safety_ctx["app"].app_context():
        toks = ApiToken.query.filter_by(user_id=S["editor"]).all()
        assert toks and all(t.revoked_at is not None for t in toks)
        _user("safety-editor@thefp.tv", "line_producer")   # password back to 'pw'
        db.session.commit()


# ── 10. QBO purge scope ──────────────────────────────────────────────────
def test_qbo_purge_scope_and_role(safety_ctx):
    c = safety_ctx["client"]
    _login(c, S["admin"])
    assert _post(c, "/admin/qbo-imports/purge?project=" + str(S["pid"])).status_code == 403
    _login(c, S["su"])
    assert _post(c, "/admin/qbo-imports/purge").status_code == 400
    assert _post(c, "/admin/qbo-imports/purge?project=all").status_code == 400
    r = _post(c, "/admin/qbo-imports/purge?project=" + str(S["pid"]))
    assert r.status_code == 200 and r.get_json()["dry"] is True


# ── 11. lock through the mobile API ──────────────────────────────────────
def test_locked_version_blocks_api_delete_line(safety_ctx):
    c = safety_ctx["client"]
    with safety_ctx["app"].app_context():
        est = db.session.get(Budget, S["est"])
        est.locked = True
        raw = api_auth.generate_token()
        db.session.add(ApiToken(user_id=S["owner"], token_hash=api_auth.hash_token(raw),
                                device_name="phone"))
        db.session.commit()
    with c.session_transaction() as s:
        s.clear()
    r = c.delete(f"/api/v1/projects/{S['pid']}/budgets/{S['bid']}/line/{S['lid']}",
                 headers={"Authorization": f"Bearer {raw}"})
    assert r.status_code == 403 and "locked" in r.get_json()["error"].lower()
    with safety_ctx["app"].app_context():
        assert db.session.get(BudgetLine, S["lid"]) is not None
        db.session.get(Budget, S["est"]).locked = False
        db.session.commit()
