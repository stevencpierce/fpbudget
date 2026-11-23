"""Project stage, project code and reference numbers (INTAKE_WRAP_HANDOFF
Phase 1, 2026-11-23) — end to end on sqlite through the real app.

  1. stage: NULL reads as awarded, status wrapped / archived win, a new
     project starts as an Estimate
  2. code: initials suggested from the client (GrubHub → GH), GH-2609,
     then GH-2609B for a second project that month; codes are found in
     text only from the real list (GrubHub's PO WG-5465 is not one)
  3. Award numbers the project's coded expenses oldest first, skipping
     uncoded rows, Not project, absorbed receipts and split children
  4. the flush hook numbers a row the moment any path codes it; a number
     is kept through uncode / recode, and a row re-coded by ANOTHER
     project gets that project's next number
  5. an Estimate never numbers; going back to Estimate is refused once
     the project holds money; the code is frozen once numbers exist
  6. routes: setup page, Award, stage, the Maintenance list

Run: python -m pytest tests/unit/test_project_codes_app.py -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
import tempfile
from datetime import date

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-codes-")
_PRESET = os.environ.get("DATABASE_URL", "")
if _PRESET and not _PRESET.startswith("sqlite:"):
    pytest.skip("DATABASE_URL is not sqlite; refusing to boot", allow_module_level=True)
os.environ.setdefault("DATABASE_URL", "sqlite:///" + os.path.join(_DB_DIR, "t.db"))
os.environ.setdefault("SECRET_KEY", "test")

try:
    _cwd = os.getcwd()
    os.chdir(ROOT)
    import app as fpapp                       # noqa: E402
    from models import (db, User, ProjectSheet, ProjectAccess, Budget, BudgetLine,  # noqa: E402
                        Transaction, TransactionSplit)
    import project_codes as pc                # noqa: E402
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


def _project(name, client, stage=None, status='active'):
    p = ProjectSheet(name=name, client_name=client, stage=stage, status=status)
    db.session.add(p)
    db.session.flush()
    db.session.add(ProjectAccess(project_id=p.id, user_id=S['uid'], role='owner'))
    b = Budget(project_id=p.id, name=f"{name} Budget")
    db.session.add(b)
    db.session.flush()
    ln = BudgetLine(budget_id=b.id, account_code=1100, account_name="Director",
                    description="Director fee")
    db.session.add(ln)
    db.session.commit()
    return p.id, ln.id


def _txn(pid, d, amount, **kw):
    t = Transaction(project_id=pid, txn_date=d, amount=amount, vendor=kw.pop('vendor', 'Vendor'), **kw)
    db.session.add(t)
    db.session.commit()
    return t.id


def _ref(tid):
    t = db.session.get(Transaction, tid)
    db.session.refresh(t)
    return pc.ref_for(t)


@pytest.fixture(scope="module")
def ctx():
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        u = User.query.filter_by(email="codes-admin@thefp.tv").first()
        if not u:
            u = User(email="codes-admin@thefp.tv", name="Codes Admin", role="super_admin")
            db.session.add(u)
        u.role = "super_admin"
        u.set_password("pw")
        db.session.commit()
        S["uid"] = u.id
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["_user_id"] = str(S["uid"])
        sess["_fresh"] = True
        sess["_csrf"] = "t"
    c.environ_base["HTTP_X_CSRFTOKEN"] = "t"
    return {"app": app, "client": c}


def test_stage_rule(ctx):
    with ctx["app"].app_context():
        assert pc.project_stage(ProjectSheet(name="x", status='active')) == 'awarded'
        assert pc.project_stage(ProjectSheet(name="x", status='active', stage='estimate')) == 'estimate'
        assert pc.project_stage(ProjectSheet(name="x", status='wrapped', stage='wrapping')) == 'wrapped'
        assert pc.project_stage(ProjectSheet(name="x", status='archived')) == 'archived'
        assert not pc.accepts_spend(ProjectSheet(name="x", status='active', stage='estimate'))
        assert pc.accepts_spend(ProjectSheet(name="x", status='active'))


def test_initials_and_codes(ctx):
    with ctx["app"].app_context():
        assert pc.suggest_initials("GrubHub") == "GH"
        assert pc.suggest_initials("Global Industrial Run Strong") == "GIRS"
        assert pc.suggest_initials("Ventana Productions LLC") == "VE"
        assert pc.code_base("gh", date(2026, 9, 14)) == "GH-2609"
        with pytest.raises(pc.StageError):
            pc.validate_code("GH2609")


def test_award_numbers_oldest_first(ctx):
    with ctx["app"].app_context():
        pid, lid = _project("GrubHub Fee Breakdown", "GrubHub", stage='estimate')
        S['gh'], S['gh_line'] = pid, lid
        late = _txn(pid, "2026-10-30", 20000, budget_line_id=lid, vendor="Steven Pierce")
        early = _txn(pid, "2026-10-01", 6400, account_code=1100, vendor="Igor Kropotov")
        split_parent = _txn(pid, "2026-10-15", 9600, vendor="GrubHub split")
        db.session.add(TransactionSplit(transaction_id=split_parent, budget_line_id=lid,
                                        project_id=pid, amount=9600))
        db.session.commit()
        uncoded = _txn(pid, "2026-09-20", 50)
        not_proj = _txn(pid, "2026-09-21", 70, budget_line_id=lid, not_project_expense=True)
        backup = _txn(pid, "2026-09-22", 6400, budget_line_id=lid, backup_of_txn_id=early)
        # An Estimate never numbers, whatever is coded.
        assert _ref(late) is None
        p = db.session.get(ProjectSheet, pid)
        n = pc.award(p, "gh", date(2026, 9, 14), date(2026, 11, 30))
        db.session.commit()
        assert n == 3 and p.project_code == "GH-2609" and pc.project_stage(p) == 'awarded'
        assert _ref(early) == "GH-2609-0001"
        assert _ref(split_parent) == "GH-2609-0002"
        assert _ref(late) == "GH-2609-0003"
        for tid in (uncoded, not_proj, backup):
            assert _ref(tid) is None
        S['uncoded'], S['early'] = uncoded, early


def test_second_project_same_month_gets_letter(ctx):
    with ctx["app"].app_context():
        pid, lid = _project("GrubHub Social", "GrubHub", stage='estimate')
        S['gh2'], S['gh2_line'] = pid, lid
        p = db.session.get(ProjectSheet, pid)
        assert pc.suggest_initials("GrubHub") == "GH"   # reused from the first project
        pc.award(p, "GH", date(2026, 9, 2))
        db.session.commit()
        assert p.project_code == "GH-2609B"
        assert pc.find_codes_in("Re: GH-2609 invoice, PO WG-5465") == ["GH-2609"]
        assert pc.find_codes_in("Fwd: GH-2609B call sheet") == ["GH-2609B"]
        assert pc.find_codes_in("WG-5465") == []


def test_flush_hook_numbers_on_first_coding(ctx):
    with ctx["app"].app_context():
        t = db.session.get(Transaction, S['uncoded'])
        t.budget_line_id = S['gh_line']
        db.session.commit()
        assert _ref(S['uncoded']) == "GH-2609-0004"
        # Uncode and recode in the same project: the number stays.
        t = db.session.get(Transaction, S['uncoded'])
        t.budget_line_id = None
        db.session.commit()
        t = db.session.get(Transaction, S['uncoded'])
        t.account_code = 1100
        db.session.commit()
        assert _ref(S['uncoded']) == "GH-2609-0004"
        # Released and coded by another project: that project's next number.
        t = db.session.get(Transaction, S['uncoded'])
        t.account_code = None
        t.project_id = None
        db.session.commit()
        t = db.session.get(Transaction, S['uncoded'])
        t.project_id = S['gh2']
        t.budget_line_id = S['gh2_line']
        db.session.commit()
        assert _ref(S['uncoded']) == "GH-2609B-0001"
        assert db.session.get(ProjectSheet, S['gh']).ref_seq_last == 4   # never reused


def test_frozen_code_and_estimate_guard(ctx):
    with ctx["app"].app_context():
        p = db.session.get(ProjectSheet, S['gh'])
        with pytest.raises(pc.StageError):
            pc.change_code(p, "GH-2610")
        with pytest.raises(pc.StageError):
            pc.set_stage(p, 'estimate')
        pc.set_stage(p, 'wrapping')
        assert pc.project_stage(p) == 'wrapping'
        db.session.rollback()


def test_routes(ctx):
    c = ctx["client"]
    with ctx["app"].app_context():
        pid, _ = _project("Etoro Press", "Ventana", stage='estimate')
        _txn(pid, "2026-10-02", 10)
    r = c.get(f"/projects/{pid}/setup")
    assert r.status_code == 200 and b"Award" in r.data
    r = c.get(f"/admin/projects/estimate-holdings?format=json")
    assert any(x['id'] == pid for x in r.get_json()['projects'])
    r = c.get(f"/projects/{pid}/setup/preview.json?initials=VE&start=2026-10-05")
    assert r.get_json()['code'] == "VE-2610"
    r = c.post(f"/projects/{pid}/setup/award",
               data={"initials": "VE", "start_date": "2026-10-05", "csrf_token": "t"})
    assert r.status_code == 302
    r = c.post(f"/projects/{pid}/setup/stage", data={"stage": "estimate", "csrf_token": "t"})
    with ctx["app"].app_context():
        p = db.session.get(ProjectSheet, pid)
        assert p.project_code == "VE-2610" and pc.project_stage(p) == 'awarded'
    r = c.get(f"/admin/projects/estimate-holdings?format=json")
    assert not any(x['id'] == pid for x in r.get_json()['projects'])


def test_estimate_takes_no_charges_or_documents(ctx, monkeypatch):
    import actuals
    c = ctx["client"]
    with ctx["app"].app_context():
        pid, lid = _project("Pitch Deck Estimate", "Ventana", stage='estimate')
        p = db.session.get(ProjectSheet, pid)
        p.dropbox_folder = "2026-12_Ventana_Pitch"
        db.session.commit()
        pool = Transaction(project_id=None, txn_date="2026-12-01", amount=99, vendor="Pool row",
                           qbo_txn_id="Q-EST-1", qbo_txn_type="Purchase", source="qbo_sync")
        db.session.add(pool)
        db.session.commit()
        pool_id = pool.id
        # The company pool is not offered to an Estimate …
        ids = {t.id for t in Transaction.query.filter(actuals.pool_filter(pid)).all()}
        assert pool_id not in ids
        assert pool_id in {t.id for t in Transaction.query.filter(actuals.pool_filter(S['gh'])).all()}
        assert not actuals.in_pool(db.session.get(Transaction, pool_id), pid)
        # … and cannot be claimed by it.
        with pytest.raises(actuals.ClaimError):
            actuals.claim_for(db.session.get(Transaction, pool_id), pid)
        db.session.rollback()
        # Documents are refused before anything is saved.
        payload, code = fpapp._ingest_document_bytes(pid, p, "r.pdf", "application/pdf", b"%PDF-1.4",
                                                     uploader=db.session.get(User, S['uid']))
        assert code == 409 and payload.get("estimate")
        # The _INBOX sweep leaves the files where they are.
        monkeypatch.setattr(fpapp, "_dbx_client", lambda: (_ for _ in ()).throw(AssertionError("no Dropbox")))
        res = fpapp._sweep_project_drop_folder(p)
        assert res["ok"] and res["estimate"] and res["imported"] == 0
    r = c.post(f"/projects/{pid}/actuals/manual-expense",
               json={"vendor": "X", "amount": 10, "txn_date": "2026-12-02", "payment_method": "company_card"})
    assert r.status_code == 409
    r = c.get(f"/projects/{pid}/setup")
    assert b"Estimate" in r.data


def test_reference_numbers_reach_the_screens(ctx):
    c = ctx["client"]
    pid, lid, early = S['gh'], S['gh_line'], S['early']
    with ctx["app"].app_context():
        t = db.session.get(Transaction, early)
        t.account_code = None
        t.budget_line_id = lid
        db.session.commit()
    j = c.get(f"/projects/{pid}/actuals/transaction/{early}/detail.json").get_json()
    assert j["txn"]["ref"] == "GH-2609-0001"
    j = c.get(f"/projects/{pid}/actuals/line/{lid}/transactions.json").get_json()
    refs = {r["id"]: r.get("ref") for r in j["transactions"]}
    assert refs[early] == "GH-2609-0001"


def test_ledger_export(ctx):
    import io
    from openpyxl import load_workbook
    c = ctx["client"]
    pid = S['gh']
    r = c.get(f"/projects/{pid}/ledger.csv")
    assert r.status_code == 200 and "GH-2609_cost-ledger_" in r.headers["Content-Disposition"]
    text = r.data.decode("utf-8-sig")
    assert text.splitlines()[0].startswith("Reference,Date,Payee")
    assert "GH-2609-0001" in text and "GH-2609-0003" in text
    # The split charge is one row per piece and keeps its one number.
    assert text.count("GH-2609-0002") == 1
    # Not project / absorbed receipts never appear.
    assert ",70.0," not in text
    r = c.get(f"/projects/{pid}/ledger.xlsx")
    wb = load_workbook(io.BytesIO(r.data))
    assert wb.sheetnames == ["Ledger", "By line", "Payees", "Documents", "Waivers", "Read me"]
    ws = wb["Ledger"]
    refs = [row[0] for row in ws.iter_rows(min_row=2, values_only=True)]
    assert "GH-2609-0001" in refs
    by_line = list(wb["By line"].iter_rows(min_row=2, values_only=True))
    assert any("GH-2609-0001" in (row[6] or "") for row in by_line)
    assert c.get(f"/projects/{pid}/ledger.pdf").status_code == 404


def test_first_working_budget_awards_the_project(ctx):
    with ctx["app"].app_context():
        # A new project starts as an Estimate …
        pid, _ = _project("Cliburn Recital", "Cliburn Foundation", stage='estimate')
        assert pc.project_stage(db.session.get(ProjectSheet, pid)) == 'estimate'
        # … and its first Working budget awards it: code from the client's
        # initials + the budget's start month.
        db.session.add(Budget(project_id=pid, name="Cliburn Working v1", budget_mode='working',
                              start_date=date(2027, 3, 10)))
        db.session.commit()
        p = db.session.get(ProjectSheet, pid)
        db.session.refresh(p)
        assert pc.project_stage(p) == 'awarded' and p.project_code == "CF-2703"
        assert not pc.needs_code(p)
        # A budget switched to Working awards too; no client and no dates →
        # awarded (it takes charges) but Project setup asks for the code.
        pid2, _ = _project("Untitled Pitch", None, stage='estimate')
        b = Budget.query.filter_by(project_id=pid2).first()
        b.budget_mode = 'working'
        db.session.commit()
        p2 = db.session.get(ProjectSheet, pid2)
        db.session.refresh(p2)
        assert pc.project_stage(p2) == 'awarded' and p2.project_code is None
        assert pc.needs_code(p2)
        # Saving the Working budget again changes nothing.
        b = Budget.query.filter_by(project_id=pid).filter(Budget.budget_mode == 'working').first()
        b.name = "Cliburn Working v1 (renamed)"
        db.session.commit()
        assert db.session.get(ProjectSheet, pid).project_code == "CF-2703"


def test_backfill_existing_projects(ctx):
    c = ctx["client"]
    with ctx["app"].app_context():
        # Projects from before stages (stage NULL) with Working budgets.
        later, _ = _project("Veolia Spring", "Veolia")
        db.session.add(Budget(project_id=later, name="W", budget_mode='working', start_date=date(2026, 5, 20)))
        earlier, lid = _project("Veolia Providence", "Veolia")
        db.session.add(Budget(project_id=earlier, name="W", budget_mode='working', start_date=date(2026, 5, 2)))
        db.session.commit()
        # The hook awards new Working budgets; undo that to model the
        # projects that existed before this release.
        for x in (later, earlier):
            p = db.session.get(ProjectSheet, x)
            p.project_code = p.client_initials = p.stage = p.awarded_at = p.start_date = None
            p.ref_seq_last = None
        db.session.commit()
        coded = _txn(earlier, "2026-05-05", 1200, budget_line_id=lid)
        noclient, _ = _project("Mystery Job", None)
        db.session.add(Budget(project_id=noclient, name="W", budget_mode='working'))
        wrapped, _ = _project("Old Wrapped Show", "Ventana", status='wrapped')
        db.session.add(Budget(project_id=wrapped, name="W", budget_mode='working', start_date=date(2025, 1, 4)))
        idle, _ = _project("Idle Estimate", "Nobody")
        db.session.commit()
        for x in (noclient, wrapped):
            p = db.session.get(ProjectSheet, x)
            p.project_code = p.stage = p.client_initials = p.awarded_at = None
        db.session.commit()
    j = c.get("/admin/projects/codes?format=json").get_json()
    codes = {it['id']: it['code'] for it in j['award']}
    assert codes[earlier] == "VE-2605" and codes[later] == "VE-2605B"
    assert codes[wrapped] == "VE-2501"
    assert noclient in {it['id'] for it in j['manual']}
    assert idle in {it['id'] for it in j['estimate']}
    with ctx["app"].app_context():
        assert db.session.get(ProjectSheet, earlier).project_code is None   # preview wrote nothing
    r = c.post("/admin/projects/codes", data={"csrf_token": "t"})
    assert r.status_code == 200
    with ctx["app"].app_context():
        assert db.session.get(ProjectSheet, earlier).project_code == "VE-2605"
        assert _ref(coded) == "VE-2605-0001"
        w = db.session.get(ProjectSheet, wrapped)
        assert w.project_code == "VE-2501" and pc.project_stage(w) == 'wrapped'
        assert pc.project_stage(db.session.get(ProjectSheet, idle)) == 'estimate'
        assert pc.needs_code(db.session.get(ProjectSheet, noclient))
    # Run again: nothing left to do.
    j = c.get("/admin/projects/codes?format=json").get_json()
    assert not any(it['id'] in (earlier, later, wrapped) for it in j['award'])
