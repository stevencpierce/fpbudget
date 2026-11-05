"""Company ledger end to end on sqlite (2026-11-03).

Boots the real app against a throw-away sqlite file (or reuses the one
another app-booting module in this process already opened) and walks:

  1. the migration on the live shapes of 2026-11-03 (0 / 1 / 2+ claimed,
     a "—" vendor twin, a split-only claim, a receipt-only claim, the
     Joel / Ryan payee-differs case): expected keeps, deletes, conflicts,
     merge-log rows, re-pointed evidence, the seeded company account set,
     the company unique index, and an idempotent second run
  2. pool_filter: project A sees its rows + the unclaimed pool, never B's
     claimed rows
  3. claim / release transitions for line, section, split, receipt and
     exclude / restore, through the real routes, and the 409 when another
     project holds the row
  4. sync_company dedupes company-wide (no second copy, even while two
     pre-migration copies exist) and enriches blanks
  5. receipt-first reconciliation across projects: a unique match claims
     the charge for the receipt's project as a SUGGESTION; receipts in two
     projects leave it unclaimed with a note on each
  6. the review actions: keep (both loser modes), unmatch receipt, release
     to unclaimed, and undo through the merge log

Run: python -m pytest tests/unit/test_company_ledger.py -q --confcutdir=tests/unit -o addopts=""
"""
import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-ledger-")
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
                        DocUpload, Transaction, TransactionSplit, ExpenseEvidence,
                        TransactionMergeLog, QBOConnection, MatchRejection)
    import actuals                            # noqa: E402
    import ledger                             # noqa: E402
    import qbo_sync                           # noqa: E402
    import routes.actuals                     # noqa: F401,E402
    import routes.ledger_admin                # noqa: F401,E402
    from sqlalchemy import text               # noqa: E402
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
NOW = datetime(2026, 11, 3, 12, 0, 0)


@pytest.fixture(scope="module")
def mp():
    from _pytest.monkeypatch import MonkeyPatch
    m = MonkeyPatch()
    yield m
    m.undo()


def _txn(pid, vendor, amount, d, **kw):
    t = Transaction(project_id=pid, vendor=vendor, amount=Decimal(str(amount)), is_expense=True,
                    txn_date=d, created_at=NOW, updated_at=NOW, **kw)
    db.session.add(t)
    db.session.flush()
    return t


def _doc(pid, uid, vendor, amount, d, name):
    doc = DocUpload(project_id=pid, uploader_id=uid, original_filename=name, filed_filename=name,
                    status="filed", vendor=vendor, amount=Decimal(str(amount)), doc_date=d,
                    category="receipt", content_type="application/pdf")
    db.session.add(doc)
    db.session.flush()
    return doc


@pytest.fixture(scope="module")
def ctx(mp):
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        u = User.query.filter_by(email="ledger-admin@thefp.tv").first()
        if not u:
            u = User(email="ledger-admin@thefp.tv", name="Ledger Admin", role="super_admin")
            db.session.add(u)
        u.role = "super_admin"
        u.set_password("pw")
        grub = ProjectSheet(name="GrubHub Fee Breakdown", status="active", qbo_account_ids='["41","42"]',
                            sync_through="2026-10-20")
        gints = ProjectSheet(name="GINTS 2026", status="active", qbo_account_ids='["42","43"]',
                             sync_through="2026-10-28")
        db.session.add_all([grub, gints])
        db.session.flush()
        for p in (grub, gints):
            db.session.add(ProjectAccess(project_id=p.id, user_id=u.id, role="owner"))
            b = Budget(project_id=p.id, name=f"{p.name} Working", budget_mode="working",
                       version_status="current")
            db.session.add(b)
            db.session.flush()
            lines = []
            for code, desc in ((2700, "Post"), (3401, "Rideshare"), (2101, "Talent")):
                ln = BudgetLine(budget_id=b.id, account_code=code, account_name=desc, description=desc,
                                quantity=1, rate_type="flat_day", days=1, rate=100, sort_order=code)
                db.session.add(ln)
                lines.append(ln)
            db.session.flush()
            S[f"b_{p.id}"] = b.id
            S[f"lines_{p.id}"] = [l.id for l in lines]
        conn = QBOConnection.query.first()
        if conn is None:
            conn = QBOConnection(realm_id="123", access_token="a", refresh_token="r",
                                 token_expiry=NOW + timedelta(days=30), enabled_account_ids='["41"]')
            db.session.add(conn)
        db.session.commit()
        S.update(uid=u.id, grub=grub.id, gints=gints.id)
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["_user_id"] = str(S["uid"])
        sess["_fresh"] = True
        sess["_csrf"] = "t"
    c.environ_base["HTTP_X_CSRFTOKEN"] = "t"
    return {"app": app, "client": c}


def _seed_live_shapes():
    """The groups verified on live data on 2026-11-03."""
    G, N, uid = S["grub"], S["gints"], S["uid"]
    gl, nl = S[f"lines_{G}"], S[f"lines_{N}"]
    rows = {}
    # a. BillPayment/1300000312 About Artists $3,080: GINTS unclaimed / GrubHub reconciled + receipt
    d_a = _doc(G, uid, "About Artists", 3080, date(2026, 10, 20), "about-artists.pdf")
    rows["a_n"] = _txn(N, "About Artists", 3080, "2026-10-21", source="qbo_sync",
                       qbo_txn_id="1300000312", qbo_txn_type="BillPayment")
    rows["a_g"] = _txn(G, "About Artists", 3080, "2026-10-21", source="reconciled",
                       qbo_txn_id="1300000312", qbo_txn_type="BillPayment", doc_upload_id=d_a.id,
                       budget_line_id=gl[2], account_code=2101, match_status="confirmed")
    db.session.add(ExpenseEvidence(transaction_id=rows["a_g"].id, doc_upload_id=d_a.id, kind="primary"))
    # the GINTS twin carries a stray evidence row that must survive on the kept row
    d_a2 = _doc(N, uid, "About Artists", 3080, date(2026, 10, 20), "about-artists-copy.pdf")
    db.session.add(ExpenseEvidence(transaction_id=rows["a_n"].id, doc_upload_id=d_a2.id, kind="backup"))
    S["evidence_doc_from_twin"] = d_a2.id
    # b. BillPayment/1300000324 Nahuel Attar $9,600: GINTS unclaimed / GrubHub split 9000+600, no line
    rows["b_n"] = _txn(N, "Nahuel Attar", 9600, "2026-10-22", source="qbo_sync",
                       qbo_txn_id="1300000324", qbo_txn_type="BillPayment")
    rows["b_g"] = _txn(G, "Nahuel Attar", 9600, "2026-10-22", source="qbo_sync",
                       qbo_txn_id="1300000324", qbo_txn_type="BillPayment")
    db.session.add_all([
        TransactionSplit(transaction_id=rows["b_g"].id, budget_line_id=gl[0], project_id=G, amount=Decimal("9000")),
        TransactionSplit(transaction_id=rows["b_g"].id, budget_line_id=gl[2], project_id=G, amount=Decimal("600"))])
    # c. Purchase/28267 Wrapbook: GINTS unclaimed / GrubHub reconciled
    rows["c_n"] = _txn(N, "Wrapbook US", 1200, "2026-10-19", source="qbo_sync",
                       qbo_txn_id="28267", qbo_txn_type="Purchase")
    rows["c_g"] = _txn(G, "Wrapbook", 1200, "2026-10-19", source="reconciled",
                       qbo_txn_id="28267", qbo_txn_type="Purchase", budget_line_id=gl[2], account_code=2101)
    # d. BillPayment/1300000319 $4,500: GINTS "Joel Knutson" (VendorRef) / GrubHub reconciled "Ryan Mauskopf"
    d_d = _doc(G, uid, "Ryan Mauskopf", 4500, date(2026, 10, 15), "mauskopf-invoice.pdf")
    rows["d_n"] = _txn(N, "Joel Knutson", 4500, "2026-10-16", source="qbo_sync",
                       qbo_txn_id="1300000319", qbo_txn_type="BillPayment", qbo_entity_ref="311")
    rows["d_g"] = _txn(G, "Ryan Mauskopf", 4500, "2026-10-16", source="reconciled",
                       qbo_txn_id="1300000319", qbo_txn_type="BillPayment", doc_upload_id=d_d.id,
                       budget_line_id=gl[2], account_code=2101, match_status="confirmed")
    S["doc_d"] = d_d.id
    # e. Purchase/28032 Lyft $26.31: coded in BOTH (line in both) — plus a GINTS receipt
    d_e = _doc(N, uid, "Lyft", 26.31, date(2026, 10, 10), "lyft-26.pdf")
    rows["e_g"] = _txn(G, "Lyft", 26.31, "2026-10-10", source="qbo_sync", qbo_txn_id="28032",
                       qbo_txn_type="Purchase", budget_line_id=gl[1], account_code=3401)
    rows["e_n"] = _txn(N, "Lyft", 26.31, "2026-10-10", source="reconciled", qbo_txn_id="28032",
                       qbo_txn_type="Purchase", budget_line_id=nl[1], account_code=3401,
                       doc_upload_id=d_e.id, match_status="confirmed")
    S["doc_e"] = d_e.id
    # f. Purchase/28088 Lyft $11.98: GINTS line / GrubHub section-only
    rows["f_n"] = _txn(N, "Lyft", 11.98, "2026-10-11", source="qbo_sync", qbo_txn_id="28088",
                       qbo_txn_type="Purchase", budget_line_id=nl[1], account_code=3401)
    rows["f_g"] = _txn(G, "Lyft", 11.98, "2026-10-11", source="qbo_sync", qbo_txn_id="28088",
                       qbo_txn_type="Purchase", account_code=3401, account_code_name="Transportation")
    # g. BillPayment/27909 Wrapbook: unclaimed in both, "—" on one copy
    rows["g_n"] = _txn(N, "—", 500, "2026-10-01", source="qbo_sync", qbo_txn_id="27909",
                       qbo_txn_type="BillPayment")
    rows["g_g"] = _txn(G, "Wrapbook US", 500, "2026-10-01", source="qbo_sync", qbo_txn_id="27909",
                       qbo_txn_type="BillPayment")
    # h. Purchase/28264: unclaimed in both, both named → newest id kept
    rows["h_g"] = _txn(G, "Amazon", 42, "2026-10-02", source="qbo_sync", qbo_txn_id="28264",
                       qbo_txn_type="Purchase")
    rows["h_n"] = _txn(N, "Amazon", 42, "2026-10-02", source="qbo_sync", qbo_txn_id="28264",
                       qbo_txn_type="Purchase")
    # i. a lone claimed row — untouched
    rows["i_g"] = _txn(G, "Sweetgreen", 88, "2026-10-03", source="qbo_sync", qbo_txn_id="28999",
                       qbo_txn_type="Purchase", budget_line_id=gl[2], account_code=2101)
    db.session.commit()
    return {k: v.id for k, v in rows.items()}


# ── 1. the migration ──────────────────────────────────────────────────────

def test_01_preflight_and_migration_on_the_live_shapes(ctx):
    with ctx["app"].app_context():
        ids = _seed_live_shapes()
        S["ids"] = ids
        pre = ledger.preflight(db)
        c = pre["counts"]
        assert c["groups"] == 9 and c["groups_with_copies"] == 8
        assert c["to_delete"] == 6, c
        assert c["to_release_to_pool"] == 2
        assert c["conflicts_two_projects"] == 4
        assert c["payee_differs"] == 1
        assert pre["already_run"] is False
        # nothing changed by the preview
        assert Transaction.query.filter(Transaction.qbo_txn_id.isnot(None)).count() == 17

        r = ledger.run_migration(user_id=S["uid"])
        assert (r["deleted"], r["released"], r["flagged"]) == (6, 2, 5), r
        assert r["index_created"] is False and r["remaining_duplicate_groups"] == 0 or r["index_created"]
        G, N = S["grub"], S["gints"]
        gone = {"a_n", "b_n", "c_n", "d_n", "g_n", "h_g"}
        for k in gone:
            assert Transaction.query.get(ids[k]) is None, k
        for k in set(ids) - gone:
            assert Transaction.query.get(ids[k]) is not None, k
        # kept rows keep their project; unclaimed survivors are in the pool
        assert Transaction.query.get(ids["a_g"]).project_id == G
        assert Transaction.query.get(ids["b_g"]).project_id == G
        assert Transaction.query.get(ids["g_g"]).project_id is None and Transaction.query.get(ids["g_g"]).vendor == "Wrapbook US"
        assert Transaction.query.get(ids["h_n"]).project_id is None
        # the twin's evidence moved onto the kept row
        assert ExpenseEvidence.query.filter_by(transaction_id=ids["a_g"],
                                               doc_upload_id=S["evidence_doc_from_twin"]).first() is not None
        # conflicts: both rows kept and flagged
        for k in ("e_g", "e_n", "f_n", "f_g"):
            t = Transaction.query.get(ids[k])
            assert t.claim_conflict and t.claim_conflict_reason == ledger.TWO_PROJECTS, k
        d = Transaction.query.get(ids["d_g"])
        assert d.claim_conflict and d.claim_conflict_reason.startswith(ledger.PAYEE_DIFFERS)
        assert "Joel Knutson" in d.claim_conflict_reason
        assert not Transaction.query.get(ids["i_g"]).claim_conflict
        # merge log: one row per deletion, restorable
        logs = TransactionMergeLog.query.all()
        assert len(logs) == 6
        assert {l.removed_txn_id for l in logs} == {ids[k] for k in gone}
        assert all(json.loads(l.removed_row_json)["qbo_txn_id"] for l in logs)
        # company account set = union of every project's retired sets
        conn = QBOConnection.query.first()
        assert set(json.loads(conn.enabled_account_ids)) == {"41", "42", "43"}
        assert conn.sync_through == "2026-10-20"      # the oldest watermark
        # the company unique index exists on the collapsed table
        names = {r[0] for r in db.session.execute(text(
            "SELECT name FROM sqlite_master WHERE type='index'")).fetchall()}
        assert "uq_transaction_qbo_company" in names
        assert ledger.marker_done()
        # second run: a no-op
        r2 = ledger.run_migration(user_id=S["uid"])
        assert (r2["deleted"], r2["released"]) == (0, 0)
        assert TransactionMergeLog.query.count() == 6
        assert ledger.preflight(db)["already_run"] is True


def test_02_the_company_index_refuses_a_second_copy(ctx):
    with ctx["app"].app_context():
        from sqlalchemy.exc import IntegrityError
        db.session.add(Transaction(project_id=S["gints"], vendor="x", amount=Decimal("1"), is_expense=True,
                                   txn_date="2026-10-03", source="qbo_sync", qbo_txn_id="28999",
                                   qbo_txn_type="Purchase"))
        with pytest.raises(IntegrityError):
            db.session.commit()
        db.session.rollback()


# ── 2. pool_filter ────────────────────────────────────────────────────────

def test_03_pool_filter_shows_own_rows_plus_the_pool_never_the_other_projects(ctx):
    with ctx["app"].app_context():
        G, N, ids = S["grub"], S["gints"], S["ids"]
        seen_g = {t.id for t in Transaction.query.filter(actuals.pool_filter(G)).all()}
        seen_n = {t.id for t in Transaction.query.filter(actuals.pool_filter(N)).all()}
        pool = {ids["g_g"], ids["h_n"]}
        assert pool <= seen_g and pool <= seen_n
        assert ids["a_g"] in seen_g and ids["a_g"] not in seen_n
        assert ids["f_n"] in seen_n and ids["f_n"] not in seen_g
        assert actuals.in_pool(Transaction.query.get(ids["g_g"]), N)
        assert not actuals.in_pool(Transaction.query.get(ids["a_g"]), N)
        assert actuals.pool_txn(N, ids["a_g"]) is None
        # the workspace payload agrees and carries the banner count
        r = ctx["client"].get(f"/projects/{N}/actuals/workspace.json")
        assert r.status_code == 200
        d = r.get_json()
        got = {row["id"] for row in d["transactions"]}
        assert pool <= got and ids["a_g"] not in got
        assert d["counts"]["claim_conflicts"] == 2 and d["conflicts_url"]
        assert all(row["pooled"] for row in d["transactions"] if row["id"] in pool)


# ── 3. claim / release transitions ────────────────────────────────────────

def _pid_of(tid):
    db.session.expire_all()
    return Transaction.query.get(tid).project_id


def test_04_line_claims_and_unassign_releases(ctx):
    with ctx["app"].app_context():
        G, N, ids = S["grub"], S["gints"], S["ids"]
        tid = ids["g_g"]
        r = ctx["client"].post(f"/projects/{N}/actuals/transaction/{tid}/set-line",
                               json={"budget_line_id": S[f"lines_{N}"][0]})
        assert r.status_code == 200, r.get_json()
        assert _pid_of(tid) == N
        # GrubHub can no longer see or touch it
        assert ctx["client"].get(f"/projects/{G}/actuals/workspace.json").get_json()["transactions"]
        assert tid not in {x["id"] for x in ctx["client"].get(f"/projects/{G}/actuals/workspace.json").get_json()["transactions"]}
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{tid}/set-line",
                               json={"budget_line_id": S[f"lines_{G}"][0]})
        assert r.status_code == 404
        # coding it to another project's line through the helper is refused
        with pytest.raises(ValueError):
            actuals.link_transaction_to_line(tid, S[f"lines_{G}"][0])
        r = ctx["client"].post(f"/projects/{N}/actuals/transaction/{tid}/set-line", json={"budget_line_id": None})
        assert r.status_code == 200 and _pid_of(tid) is None


def test_05_section_claims_and_clear_releases(ctx):
    with ctx["app"].app_context():
        G, N, ids = S["grub"], S["gints"], S["ids"]
        tid = ids["h_n"]
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{tid}/set-coa", json={"account_code": 3401})
        assert r.status_code == 200, r.get_json()
        assert _pid_of(tid) == G
        r = ctx["client"].post(f"/projects/{N}/actuals/transaction/{tid}/set-coa", json={"account_code": 2700})
        assert r.status_code == 404
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{tid}/set-coa", json={"account_code": None})
        assert r.status_code == 200 and _pid_of(tid) is None


def test_06_split_claims_and_removing_it_releases(ctx):
    with ctx["app"].app_context():
        G, ids = S["grub"], S["ids"]
        tid = ids["g_g"]
        l0, l1 = S[f"lines_{G}"][:2]
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{tid}/split",
                               json={"splits": [{"budget_line_id": l0, "amount": 300},
                                                {"budget_line_id": l1, "amount": 200}]})
        assert r.status_code == 200, r.get_json()
        assert _pid_of(tid) == G
        assert TransactionSplit.query.filter_by(transaction_id=tid).count() == 2
        r = ctx["client"].delete(f"/projects/{G}/actuals/transaction/{tid}/split")
        assert r.status_code == 200 and _pid_of(tid) is None


def test_07_receipt_claims_and_unmatch_releases(ctx):
    with ctx["app"].app_context():
        N, ids, uid = S["gints"], S["ids"], S["uid"]
        tid = ids["h_n"]
        doc = _doc(N, uid, "Amazon", 42, date(2026, 10, 2), "amazon-42.pdf")
        db.session.commit()
        r = ctx["client"].post(f"/projects/{N}/actuals/transaction/{tid}/link-doc", json={"doc_upload_id": doc.id})
        assert r.status_code == 200, r.get_json()
        assert _pid_of(tid) == N
        r = ctx["client"].post(f"/projects/{N}/actuals/transaction/{tid}/unmatch", json={})
        assert r.status_code == 200, r.get_json()
        assert _pid_of(tid) is None
        # the receipt is back as its own row in GINTS
        assert Transaction.query.filter_by(doc_upload_id=doc.id, source="doc_upload").first().project_id == N


def test_08_not_project_on_a_pool_row_is_a_dismissal_for_that_project_only(ctx):
    """Marking a pool charge Not project in GrubHub hides it from GrubHub's
    Unclaimed only; GINTS still sees it; Restore brings it back. On a row
    GrubHub had claimed it releases the row to the pool AND dismisses it."""
    with ctx["app"].app_context():
        G, N, ids = S["grub"], S["gints"], S["ids"]
        tid = ids["g_g"]
        ws = lambda p: ctx["client"].get(f"/projects/{p}/actuals/workspace.json").get_json()
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{tid}/mark-not-project", json={"reason": "personal"})
        assert r.status_code == 200 and r.get_json()["mode"] == "dismissed", r.get_json()
        t = Transaction.query.get(tid)
        assert t.project_id is None and not t.not_project_expense
        g = ws(G)
        assert tid not in {x["id"] for x in g["transactions"]}
        np_row = next(x for x in g["not_project"] if x["id"] == tid)
        assert np_row["excluded"]["reason"] == "personal" and np_row["excluded"]["pool"] is True
        assert tid in {x["id"] for x in ws(N)["transactions"]}, "still in GINTS's pool"
        assert tid not in {x.id for x in actuals.open_charges(G)}
        nj = ctx["client"].get(f"/projects/{G}/actuals/not-project.json").get_json()
        assert tid in {x["id"] for x in nj["not_project"]}
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{tid}/recover", json={})
        assert r.status_code == 200 and r.get_json()["restored_dismissal"] is True
        assert tid in {x["id"] for x in ws(G)["transactions"]}
        # claimed by GrubHub, then Not project → released + dismissed
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{tid}/set-line",
                               json={"budget_line_id": S[f"lines_{G}"][0]})
        assert r.status_code == 200 and _pid_of(tid) == G
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{tid}/mark-not-project", json={})
        assert r.status_code == 200 and r.get_json()["mode"] == "dismissed"
        t = Transaction.query.get(tid)
        assert t.project_id is None and t.budget_line_id is None
        assert tid in {x["id"] for x in ws(N)["transactions"]}
        assert tid not in {x["id"] for x in ws(G)["transactions"]}
        # undo of that mark: coding back, claimed again, dismissal gone
        act = r.get_json()["activity_id"]
        r = ctx["client"].post(f"/projects/{G}/budget/{S[f'b_{G}']}/activity/{act}/undo", json={})
        assert r.status_code == 200, r.get_json()
        db.session.expire_all()
        t = Transaction.query.get(tid)
        assert t.project_id == G and t.budget_line_id == S[f"lines_{G}"][0]
        assert actuals.pool_dismissal(tid, G) is None
        # leave it in the pool for the later tests
        ctx["client"].post(f"/projects/{G}/actuals/transaction/{tid}/set-line", json={"budget_line_id": None})
        actuals.undismiss(Transaction.query.get(tid), G)
        db.session.commit()
        assert _pid_of(tid) is None


def test_09_claim_for_refuses_a_row_another_project_holds(ctx):
    with ctx["app"].app_context():
        G, N, ids = S["grub"], S["gints"], S["ids"]
        t = Transaction.query.get(ids["a_g"])
        with pytest.raises(actuals.ClaimError):
            actuals.claim_for(t, N)
        assert actuals.claim_for(t, G) is False        # already this project's: no change
        assert actuals.release(t) is False             # still claimed (line + receipt): stays
        db.session.rollback()


# ── 4. sync_company ───────────────────────────────────────────────────────

def _stub_pull(mp, rows):
    mp.setattr(qbo_sync, "fetch_transactions", lambda *a, **k: list(rows))
    mp.setattr(qbo_sync, "fetch_transactions_cdc", lambda *a, **k: ([], None))
    mp.setattr(qbo_sync, "list_qbo_accounts", lambda *a, **k: [])


def _row(qid, vendor, amount, d, qtype="Purchase", ref=None, note=""):
    return {"qbo_txn_id": qid, "qbo_txn_type": qtype, "qbo_account_id": "41", "qbo_category": None,
            "txn_date": d, "vendor": vendor, "amount": amount, "is_expense": True,
            "account_name": "CC: 4417", "note": note, "qbo_entity_ref": ref}


def test_10_sync_company_dedupes_company_wide_and_enriches_blanks(ctx, mp):
    with ctx["app"].app_context():
        G, N, ids = S["grub"], S["gints"], S["ids"]
        conn = QBOConnection.query.first()
        # a blank-vendor pooled row to enrich, an existing claimed row, a brand-new charge
        blank = _txn(None, "—", 77, "2026-10-25", source="qbo_sync", qbo_txn_id="30001", qbo_txn_type="Purchase")
        db.session.commit()
        _stub_pull(mp, [_row("30001", "Delta Air Lines", 77, "2026-10-25", ref="9"),
                        _row("28999", "SWEETGREEN NYC", 88, "2026-10-03", ref="5"),
                        _row("30002", "Home Depot", 12.5, "2026-10-26")])
        r = qbo_sync.sync_company(conn, db)
        assert r["imported"] == 1 and r["enriched"] >= 1, r
        assert Transaction.query.filter_by(qbo_txn_id="30001").count() == 1
        assert Transaction.query.get(blank.id).vendor == "Delta Air Lines"
        kept = Transaction.query.get(ids["i_g"])
        assert kept.project_id == G, "a claimed row is never duplicated"
        assert kept.vendor == "SWEETGREEN NYC", "QuickBooks names the payee on a plain QuickBooks row"
        kept.vendor = "Sweetgreen"
        kept.ai_cleaned_at = NOW          # a cleaned / person-set name is never overwritten
        db.session.commit()
        qbo_sync.sync_company(conn, db)
        assert Transaction.query.get(ids["i_g"]).vendor == "Sweetgreen"
        new = Transaction.query.filter_by(qbo_txn_id="30002").one()
        assert new.project_id is None and new.source == "qbo_sync"
        assert conn.last_synced is not None and conn.sync_through
        # a second sync adds nothing
        r = qbo_sync.sync_company(conn, db)
        assert r["imported"] == 0
        assert Transaction.query.filter(Transaction.qbo_txn_id.in_(["30001", "30002", "28999"])).count() == 3


def test_11_sync_company_tolerates_pre_migration_copies(ctx, mp):
    """Before the Maintenance run has collapsed the copies, a sync must not
    add a third one (the index is partial on claim_conflict, so the two
    flagged conflict rows stand in for a pre-migration pair)."""
    with ctx["app"].app_context():
        conn = QBOConnection.query.first()
        assert Transaction.query.filter_by(qbo_txn_id="28032").count() == 2
        _stub_pull(mp, [_row("28032", "LYFT *RIDE", 26.31, "2026-10-10")])
        r = qbo_sync.sync_company(conn, db)
        assert r["imported"] == 0
        assert Transaction.query.filter_by(qbo_txn_id="28032").count() == 2


# ── 5. receipt-first reconciliation across projects ───────────────────────

def test_12_unique_receipt_match_claims_as_a_suggestion(ctx, mp):
    with ctx["app"].app_context():
        G, uid = S["grub"], S["uid"]
        conn = QBOConnection.query.first()
        doc = _doc(G, uid, "Blick Art Materials", 61.4, date(2026, 10, 27), "blick.pdf")
        drow = _txn(G, "Blick Art Materials", 61.4, "2026-10-27", source="doc_upload", doc_upload_id=doc.id)
        db.session.commit()
        _stub_pull(mp, [_row("30010", "BLICK ART MATERIALS", 61.4, "2026-10-28")])
        r = qbo_sync.sync_company(conn, db)
        assert r["reconciled"] == 1 and r["imported"] == 0 and r["ambiguous"] == 0
        t = Transaction.query.get(drow.id)
        assert (t.qbo_txn_id, t.source, t.match_status, t.project_id) == ("30010", "reconciled", "suggested", G)
        assert Transaction.query.filter_by(qbo_txn_id="30010").count() == 1


def test_13_receipts_in_two_projects_leave_the_charge_unclaimed(ctx, mp):
    with ctx["app"].app_context():
        G, N, uid = S["grub"], S["gints"], S["uid"]
        conn = QBOConnection.query.first()
        d1 = _doc(G, uid, "Uber", 33.0, date(2026, 10, 29), "uber-g.pdf")
        d2 = _doc(N, uid, "Uber", 33.0, date(2026, 10, 29), "uber-n.pdf")
        r1 = _txn(G, "Uber", 33.0, "2026-10-29", source="doc_upload", doc_upload_id=d1.id)
        r2 = _txn(N, "Uber", 33.0, "2026-10-29", source="doc_upload", doc_upload_id=d2.id)
        db.session.commit()
        _stub_pull(mp, [_row("30011", "UBER *TRIP", 33.0, "2026-10-29")])
        r = qbo_sync.sync_company(conn, db)
        assert r["imported"] == 1 and r["reconciled"] == 0 and r["ambiguous"] == 1
        t = Transaction.query.filter_by(qbo_txn_id="30011").one()
        assert t.project_id is None and t.source == "qbo_sync"
        for rr in (r1, r2):
            rr = Transaction.query.get(rr.id)
            assert rr.qbo_txn_id is None and "30011" in (rr.note or ""), "both receipts carry the suggestion"


# ── 6. the review actions and undo ────────────────────────────────────────

def test_14_keep_in_a_project_receipt_back_to_place_and_undo(ctx):
    with ctx["app"].app_context():
        G, N, ids = S["grub"], S["gints"], S["ids"]
        # Purchase/28032: keep in GrubHub; GINTS's receipt goes back to Receipts to place
        r = ctx["client"].post("/admin/ledger/conflicts/keep",
                               json={"qbo_txn_id": "28032", "qbo_txn_type": "Purchase", "keep_project_id": G})
        assert r.status_code == 200, r.get_json()
        assert Transaction.query.get(ids["e_n"]) is None
        kept = Transaction.query.get(ids["e_g"])
        assert kept.project_id == G and not kept.claim_conflict
        back = Transaction.query.filter_by(doc_upload_id=S["doc_e"], source="doc_upload").one()
        assert back.project_id == N and back.budget_line_id is None
        log_id = r.get_json()["removed"][0]["merge_log_id"]
        # undo puts the GINTS row back, flagged next to the kept one
        r = ctx["client"].post("/admin/ledger/undo-merge", json={"log_id": log_id})
        assert r.status_code == 200, r.get_json()
        restored = Transaction.query.get(r.get_json()["transaction_id"])
        assert restored.project_id == N and restored.qbo_txn_id == "28032" and restored.claim_conflict
        assert Transaction.query.get(ids["e_g"]).claim_conflict
        assert TransactionMergeLog.query.get(log_id).restored_at is not None
        r = ctx["client"].post("/admin/ledger/undo-merge", json={"log_id": log_id})
        assert r.status_code == 400
        # and keep again, so the group is settled for the tests below
        r = ctx["client"].post("/admin/ledger/conflicts/keep",
                               json={"qbo_txn_id": "28032", "qbo_txn_type": "Purchase", "keep_project_id": G})
        assert r.status_code == 200


def test_15_keep_with_the_losers_receipt_staying_coded_as_its_own_row(ctx):
    """Purchase/28026-shaped: the loser was a WRONG reconcile — its receipt is
    a different charge of the same amount and keeps its line in its project."""
    with ctx["app"].app_context():
        G, N, uid = S["grub"], S["gints"], S["uid"]
        d = _doc(N, uid, "SiegeWorks", 200, date(2026, 10, 14), "siegeworks.pdf")
        loser = _txn(N, "SiegeWorks", 200, "2026-10-14", source="reconciled", qbo_txn_id="28026",
                     qbo_txn_type="Purchase", doc_upload_id=d.id, budget_line_id=S[f"lines_{N}"][0],
                     account_code=2700, match_status="confirmed", claim_conflict=True,
                     claim_conflict_reason=ledger.TWO_PROJECTS)
        winner = _txn(G, "ALLWAYS ON THE GO", 200, "2026-10-14", source="qbo_sync", qbo_txn_id="28026",
                      qbo_txn_type="Purchase", account_code=3400, claim_conflict=True,
                      claim_conflict_reason=ledger.TWO_PROJECTS)
        db.session.commit()
        r = ctx["client"].post("/admin/ledger/conflicts/keep",
                               json={"qbo_txn_id": "28026", "qbo_txn_type": "Purchase", "keep_project_id": G,
                                     "loser_mode": "receipt_stays_coded"})
        assert r.status_code == 200, r.get_json()
        l = Transaction.query.get(loser.id)
        assert l is not None and l.project_id == N and l.budget_line_id == S[f"lines_{N}"][0]
        assert l.qbo_txn_id is None and l.source == "doc_upload" and l.doc_upload_id == d.id
        assert not l.claim_conflict
        w = Transaction.query.get(winner.id)
        assert w.project_id == G and w.qbo_txn_id == "28026" and not w.claim_conflict
        log_id = r.get_json()["removed"][0]["merge_log_id"]
        assert r.get_json()["removed"][0]["demoted"] is True
        # undo restores the QuickBooks identity in place and flags both
        r = ctx["client"].post("/admin/ledger/undo-merge", json={"log_id": log_id})
        assert r.status_code == 200 and r.get_json()["in_place"] is True
        l = Transaction.query.get(loser.id)
        assert l.qbo_txn_id == "28026" and l.source == "reconciled" and l.claim_conflict
        assert Transaction.query.get(winner.id).claim_conflict


def test_16_release_to_unclaimed_unmatches_the_receipt_and_undo(ctx):
    """BillPayment/1300000319-shaped (owner 2026-11-03): the charge goes back
    to unclaimed, the invoice back to Receipts to place, the pair is not
    re-suggested; undo restores the row in place."""
    with ctx["app"].app_context():
        G, ids = S["grub"], S["ids"]
        tid = ids["d_g"]
        r = ctx["client"].post("/admin/ledger/release", json={"txn_id": tid})
        assert r.status_code == 200, r.get_json()
        t = Transaction.query.get(tid)
        assert t.project_id is None and t.source == "qbo_sync" and t.doc_upload_id is None
        assert t.budget_line_id is None and t.account_code is None and not t.claim_conflict
        back = Transaction.query.filter_by(doc_upload_id=S["doc_d"], source="doc_upload").one()
        assert back.project_id == G and back.budget_line_id is None
        assert MatchRejection.query.filter_by(transaction_id=tid, doc_upload_id=S["doc_d"]).first() is not None
        # the auto-matcher leaves the pair alone
        actuals.run_auto_match(G)
        assert Transaction.query.get(tid).doc_upload_id is None
        log_id = r.get_json()["merge_log_id"]
        r = ctx["client"].post("/admin/ledger/undo-merge", json={"log_id": log_id})
        assert r.status_code == 200 and r.get_json()["in_place"] is True
        t = Transaction.query.get(tid)
        assert t.project_id == G and t.source == "reconciled" and t.doc_upload_id == S["doc_d"]
        assert Transaction.query.filter_by(doc_upload_id=S["doc_d"], source="doc_upload").count() == 0
        assert MatchRejection.query.filter_by(transaction_id=tid).count() == 0


def test_17_payee_differs_unmatch_keeps_the_quickbooks_payee(ctx):
    with ctx["app"].app_context():
        G, ids = S["grub"], S["ids"]
        tid = ids["d_g"]
        t = Transaction.query.get(tid)
        t.claim_conflict = True
        t.claim_conflict_reason = f"{ledger.PAYEE_DIFFERS} (Joel Knutson)"
        db.session.commit()
        cards = ledger.conflicts(project_id=G)
        card = next(c for c in cards if c["qbo_txn_id"] == "1300000319")
        assert card["kind"] == "payee" and card["qbo_payee"] == "Joel Knutson"
        r = ctx["client"].post("/admin/ledger/conflicts/unmatch", json={"txn_id": tid})
        assert r.status_code == 200, r.get_json()
        t = Transaction.query.get(tid)
        assert t.vendor == "Joel Knutson" and t.doc_upload_id is None and not t.claim_conflict
        assert t.project_id == G, "still coded to a line → stays claimed"
        assert Transaction.query.filter_by(doc_upload_id=S["doc_d"], source="doc_upload").count() == 1


def test_18_its_fine_clears_a_single_flag_and_refuses_a_pair(ctx):
    with ctx["app"].app_context():
        ids = S["ids"]
        r = ctx["client"].post("/admin/ledger/conflicts/clear", json={"txn_id": ids["f_n"]})
        assert r.status_code == 400 and "another project" in r.get_json()["error"]
        t = Transaction.query.get(ids["i_g"])
        t.claim_conflict = True
        t.claim_conflict_reason = "test"
        db.session.commit()
        r = ctx["client"].post("/admin/ledger/conflicts/clear", json={"txn_id": ids["i_g"]})
        assert r.status_code == 200 and not Transaction.query.get(ids["i_g"]).claim_conflict


def test_19_ledger_page_and_json_show_claimed_by_and_unclaimed(ctx):
    with ctx["app"].app_context():
        r = ctx["client"].get("/admin/transactions.json?status=unclaimed")
        assert r.status_code == 200
        rows = r.get_json()["rows"]
        assert rows and all(row["project"] is None for row in rows)
        r = ctx["client"].get("/admin/transactions.json?status=conflict")
        assert {row["qbo_txn_id"] for row in r.get_json()["rows"]} >= {"28088"}
        r = ctx["client"].get("/admin/transactions?status=claimed")
        assert r.status_code == 200 and b"Company ledger" in r.data
        r = ctx["client"].get(f"/admin/ledger/conflicts?pid={S['gints']}")
        assert r.status_code == 200 and b"28088" in r.data
        r = ctx["client"].get("/admin/ledger/preview")
        assert r.status_code == 200 and r.get_json()["already_run"] is True
        r = ctx["client"].get("/admin/ledger/merge-log.json")
        assert r.status_code == 200 and r.get_json()["total"] >= 8


# ── 7. after the merge with Phase 5 (matching engine, duplicates) ─────────

class _Row:
    def __init__(self, **kw):
        self.__dict__.update(dict(id=None, amount=None, txn_date=None, vendor=None, card_last4=None,
                                  qbo_txn_id=None, qbo_txn_type=None, qbo_entity_ref=None), **kw)


def test_20_duplicates_use_the_quickbooks_payee():
    """Joel Knutson vs Ryan Mauskopf: two $4,500 bill payments a day apart.
    A receipt renamed the first one "Ryan Mauskopf", so vendor text alone
    calls them twins — the QuickBooks payee (EntityRef) says they are two
    different people. The same payee still pairs a real double-post."""
    joel = _Row(id=1, amount=Decimal("4500"), txn_date="2026-10-19", vendor="Ryan Mauskopf",
                qbo_txn_id="1300000319", qbo_txn_type="BillPayment", qbo_entity_ref="311")
    ryan = _Row(id=2, amount=Decimal("4500"), txn_date="2026-10-20", vendor="Ryan Mauskopf",
                qbo_txn_id="1300000322", qbo_txn_type="BillPayment", qbo_entity_ref="402")
    assert not actuals.is_duplicate_twin(joel, ryan)
    assert actuals.find_duplicate_groups([joel, ryan]) == []
    again = _Row(id=3, amount=Decimal("4500"), txn_date="2026-10-21", vendor="R. MAUSKOPF ACH",
                 qbo_txn_id="1300000330", qbo_txn_type="BillPayment", qbo_entity_ref="402")
    assert actuals.is_duplicate_twin(ryan, again), "same QuickBooks payee = same vendor"
    groups = actuals.find_duplicate_groups([joel, ryan, again])
    assert len(groups) == 1 and {m.id for m in groups[0]["members"]} == {2, 3}
    # without payees on both sides the vendor / card rule still applies
    plain = _Row(id=4, amount=Decimal("4500"), txn_date="2026-10-20", vendor="Ryan Mauskopf",
                 qbo_txn_id="1300000340", qbo_txn_type="BillPayment")
    assert actuals.is_duplicate_twin(ryan, plain)


def test_21_matching_engine_claims_pooled_charges_and_releases_stale_ones(ctx):
    with ctx["app"].app_context():
        G, N, uid = S["grub"], S["gints"], S["uid"]
        today = date.today()
        charge = _txn(None, "Paper Source", 57.25, today.isoformat(), source="qbo_sync",
                      qbo_txn_id="31001", qbo_txn_type="Purchase", card_last4="4417")
        doc = _doc(N, uid, "Paper Source", 57.25, today, "paper-source.pdf")
        db.session.commit()
        assert charge.id in {t.id for t in actuals.open_charges(G)}, "a pooled charge is open in every project"
        actuals.refresh_match_suggestions(N)
        t = Transaction.query.get(charge.id)
        assert (t.project_id, t.match_status, t.doc_upload_id) == (N, 'suggested', doc.id)
        assert charge.id not in {x.id for x in actuals.open_charges(G)}
        # the receipt goes away → the stale suggestion clears and the charge returns to the pool
        d = DocUpload.query.get(doc.id)
        d.status = 'deleted'
        db.session.commit()
        actuals.refresh_match_suggestions(N)
        t = Transaction.query.get(charge.id)
        assert t.project_id is None and t.doc_upload_id is None and t.match_status == 'unmatched'


def test_22_quickbooks_deletion_reaches_the_pooled_row(ctx):
    with ctx["app"].app_context():
        t = _txn(None, "Voided Co", 19.0, "2026-10-30", source="qbo_sync",
                 qbo_txn_id="31002", qbo_txn_type="Purchase")
        db.session.commit()
        out = qbo_sync.apply_qbo_deletions(db, None, [{"qbo_txn_id": "31002", "qbo_txn_type": "Purchase"}])
        assert out["deleted"] == [t.id]
        assert Transaction.query.filter_by(qbo_txn_id="31002").count() == 0


# ── 8. the corrected claim rule on the live cases of 2026-11-04 ───────────
# Production's preview (2026-11-04) showed "Not project" counted as a claim:
# 577 fake conflicts and real rows deleted. Claim signals are line,
# section, split, receipt only; every Not project mark is a per-project
# dismissal of the one pool row.

def _proj(name):
    p = ProjectSheet(name=name, status="active")
    db.session.add(p)
    db.session.flush()
    return p.id


def _live_rule_fixture():
    db.session.execute(text("DROP INDEX IF EXISTS uq_transaction_qbo_company"))
    uid = S["uid"]
    P = {n: _proj(n) for n in ("CLIBURN 25", "CLIBURN 25 _MUNDY TEST", "2026_FP GENERAL OPERATIONS",
                               "Global Run Strong 2026", "Veolia Providence Event", "Etoro", "Bystander")}
    P["GRUB"], P["GINTS"] = S["grub"], S["gints"]
    np_kw = dict(not_project_expense=True)
    r = {}
    # Branden Fedde — BillPayment 1300000143 $5,600
    r["bf_test"] = _txn(P["CLIBURN 25 _MUNDY TEST"], "", 5600, "2025-05-29", source="qbo_sync",
                        qbo_txn_id="1300000143", qbo_txn_type="BillPayment", **np_kw)
    r["bf_c25"] = _txn(P["CLIBURN 25"], "Branden Fedde", 5600, "2025-05-29", source="reconciled",
                       qbo_txn_id="1300000143", qbo_txn_type="BillPayment", note="May invoice")
    # Ryan — BillPayment 1300000322 $4,500
    r["ry_grub"] = _txn(P["GRUB"], "Ryan Mauskopf", 4500, "2026-10-20", source="reconciled",
                        qbo_txn_id="1300000322", qbo_txn_type="BillPayment")
    r["ry_ops"] = _txn(P["2026_FP GENERAL OPERATIONS"], "RYAN MAUSKOPF", 4500, "2026-10-20", source="qbo_sync",
                       qbo_txn_id="1300000322", qbo_txn_type="BillPayment", qbo_entity_ref="402",
                       excluded_reason="GrubHub job", **np_kw)
    r["ry_rs"] = _txn(P["Global Run Strong 2026"], "RYAN MAUSKOPF", 4500, "2026-10-20", source="qbo_sync",
                      qbo_txn_id="1300000322", qbo_txn_type="BillPayment", qbo_entity_ref="402", **np_kw)
    r["ry_gints"] = _txn(P["GINTS"], "RYAN MAUSKOPF", 4500, "2026-10-20", source="qbo_sync",
                         qbo_txn_id="1300000322", qbo_txn_type="BillPayment", qbo_entity_ref="402")
    # Netflix — Not project everywhere
    r["nf_ops"] = _txn(P["2026_FP GENERAL OPERATIONS"], "NETFLIX.COM", 15.49, "2026-10-01", source="qbo_sync",
                       qbo_txn_id="28501", qbo_txn_type="Purchase", **np_kw)
    r["nf_et"] = _txn(P["Etoro"], "NETFLIX.COM", 15.49, "2026-10-01", source="qbo_sync",
                      qbo_txn_id="28501", qbo_txn_type="Purchase", excluded_reason="personal", **np_kw)
    r["nf_gints"] = _txn(P["GINTS"], "NETFLIX.COM", 15.49, "2026-10-01", source="qbo_sync",
                         qbo_txn_id="28501", qbo_txn_type="Purchase", **np_kw)
    # Jacks Gas — Purchase 27800 $55.03, a REAL two-project claim
    d_jg = _doc(P["2026_FP GENERAL OPERATIONS"], uid, "Jacks Gas and Service", 55.03, date(2026, 6, 16), "jacks.pdf")
    r["jg_ops"] = _txn(P["2026_FP GENERAL OPERATIONS"], "Jacks Gas and Service", 55.03, "2026-06-16",
                       source="reconciled", qbo_txn_id="27800", qbo_txn_type="Purchase", doc_upload_id=d_jg.id,
                       match_status="confirmed")
    r["jg_veo"] = _txn(P["Veolia Providence Event"], "Jack's Gas and Service", 55.03, "2026-06-16",
                       source="qbo_sync", qbo_txn_id="27800", qbo_txn_type="Purchase", qbo_entity_ref="77",
                       account_code=3400, account_code_name="Transportation")
    # River Bar — Purchase 27985: a person-typed payee vs raw processor text
    d_rb = _doc(P["2026_FP GENERAL OPERATIONS"], uid, "Coco Lee", 46.52, date(2026, 8, 31), "coco.pdf")
    r["rb_ops"] = _txn(P["2026_FP GENERAL OPERATIONS"], "Coco Lee", 46.52, "2026-08-31", source="reconciled",
                       qbo_txn_id="27985", qbo_txn_type="Purchase", doc_upload_id=d_rb.id, match_status="confirmed")
    r["rb_rs"] = _txn(P["Global Run Strong 2026"], "TST*RIVER BAR- SOMERVILL", 46.52, "2026-08-31",
                      source="qbo_sync", qbo_txn_id="27985", qbo_txn_type="Purchase", qbo_entity_ref="88")
    # Vendor carry — the claimed keeper holds processor noise, a copy holds a person-set name
    r["vc_keep"] = _txn(P["Etoro"], "SQ *CASA VIDEO", 75, "2026-09-03", source="qbo_sync",
                        qbo_txn_id="28777", qbo_txn_type="Purchase", account_code=2600)
    r["vc_copy"] = _txn(P["Bystander"], "Casa Video", 75, "2026-09-03", source="manual_entry",
                        qbo_txn_id="28777", qbo_txn_type="Purchase", note="lens rental return")
    db.session.commit()
    keys = {("1300000143", "BillPayment"), ("1300000322", "BillPayment"), ("28501", "Purchase"),
            ("27800", "Purchase"), ("27985", "Purchase"), ("28777", "Purchase")}
    return P, {k: v.id for k, v in r.items()}, keys


def test_23_preview_on_the_live_cases_counts_only_real_claims(ctx):
    with ctx["app"].app_context():
        P, R, keys = _live_rule_fixture()
        S.update(live_P=P, live_R=R, live_keys=keys)
        pre = ledger.preflight(db, keys=keys)
        c = pre["counts"]
        assert c["groups"] == 6
        assert c["conflicts_two_projects"] == 2, c              # only Jacks Gas
        assert c["payee_differs"] == 0, "River Bar's QuickBooks name is processor text"
        assert c["to_delete"] == 8, c        # 1 + 3 + 2 + 0 + 1 + 1
        assert c["to_release_to_pool"] == 3, c                  # Branden, Ryan, Netflix keepers
        assert c["dismissals_to_create"] == 6, c                # Mundy, Ops+RunStrong, Ops+Etoro+GINTS
        assert c["vendor_or_note_carried"] >= 1
        assert pre["by_project"]["CLIBURN 25 _MUNDY TEST"]["dismissals"] == 1
        assert pre["by_project"]["Veolia Providence Event"]["conflicts"] == 1
        dec = {d["qbo_txn_id"]: d for d in pre["decisions"]}
        bf = {row["id"]: row for row in dec["1300000143"]["rows"]}
        assert bf[R["bf_c25"]]["action"] == "keep" and bf[R["bf_test"]]["action"] == "delete"
        assert "dismissal" in bf[R["bf_test"]]["why"]
        assert "delete #" in dec["1300000143"]["what"] and "CLIBURN 25" in dec["1300000143"]["what"]
        assert dec["27985"]["qbo_payee_from_quickbooks_vendor"] is True
        assert not any(row["payee_differs"] for row in dec["27985"]["rows"])


def test_24_the_run_keeps_the_real_rows_and_writes_dismissals(ctx):
    with ctx["app"].app_context():
        from models import TransactionPoolDismissal
        P, R, keys = S["live_P"], S["live_R"], S["live_keys"]
        r = ledger.run_migration(user_id=S["uid"], keys=keys)
        assert r["deleted"] == 8 and r["dismissals"] == 6, r
        g = Transaction.query.get
        # Branden Fedde: CLIBURN 25's reconciled row survives, in the pool, vendor + note intact
        bf = g(R["bf_c25"])
        assert bf is not None and bf.project_id is None and bf.vendor == "Branden Fedde" and bf.note == "May invoice"
        assert bf.source == "qbo_sync", "a reconciled row with no receipt goes back to qbo_sync"
        assert g(R["bf_test"]) is None
        assert TransactionPoolDismissal.query.filter_by(transaction_id=bf.id,
                                                        project_id=P["CLIBURN 25 _MUNDY TEST"]).count() == 1
        assert bf.id in {t.id for t in Transaction.query.filter(actuals.pool_filter(P["CLIBURN 25"])).all()}
        assert bf.id not in {t.id for t in Transaction.query.filter(actuals.pool_filter(P["CLIBURN 25 _MUNDY TEST"])).all()}
        # Ryan: GrubHub's reconciled row kept (not deleted for a Not project copy), released, two dismissals
        ry = g(R["ry_grub"])
        assert ry is not None and ry.project_id is None and ry.vendor == "Ryan Mauskopf" and not ry.claim_conflict
        for k in ("ry_ops", "ry_rs", "ry_gints"):
            assert g(R[k]) is None
        dis = {d.project_id: d.reason for d in TransactionPoolDismissal.query.filter_by(transaction_id=ry.id)}
        assert set(dis) == {P["2026_FP GENERAL OPERATIONS"], P["Global Run Strong 2026"]}
        assert dis[P["2026_FP GENERAL OPERATIONS"]] == "GrubHub job"
        # Netflix: one pool row, hidden from the three that said not ours, visible to the rest
        nf = [t for t in Transaction.query.filter_by(qbo_txn_id="28501").all()]
        assert len(nf) == 1 and nf[0].project_id is None and not nf[0].not_project_expense
        for name in ("2026_FP GENERAL OPERATIONS", "Etoro"):
            assert nf[0].id not in {t.id for t in Transaction.query.filter(actuals.pool_filter(P[name])).all()}
        assert nf[0].id not in {t.id for t in Transaction.query.filter(actuals.pool_filter(P["GINTS"])).all()}
        assert nf[0].id in {t.id for t in Transaction.query.filter(actuals.pool_filter(P["Bystander"])).all()}
        # Jacks Gas: a real conflict — both kept and flagged
        for k in ("jg_ops", "jg_veo"):
            assert g(R[k]).claim_conflict and g(R[k]).claim_conflict_reason == ledger.TWO_PROJECTS
        # River Bar: kept in FP General Ops, NOT flagged
        rb = g(R["rb_ops"])
        assert rb.project_id == P["2026_FP GENERAL OPERATIONS"] and not rb.claim_conflict
        assert g(R["rb_rs"]) is None
        # Vendor carry: the keeper had processor noise, the copy a person-set name + note
        vc = g(R["vc_keep"])
        assert vc.vendor == "Casa Video" and vc.note == "lens rental return" and vc.project_id == P["Etoro"]
        # the deleted copies are all in the merge log, restorable
        assert TransactionMergeLog.query.filter(
            TransactionMergeLog.removed_txn_id.in_([R["bf_test"], R["ry_ops"], R["ry_rs"], R["ry_gints"]])).count() == 4
        # second run on the same keys: nothing to do
        r2 = ledger.run_migration(user_id=S["uid"], keys=keys)
        assert (r2["deleted"], r2["released"], r2["dismissals"]) == (0, 0, 0)


def test_25_payee_flag_only_against_a_real_quickbooks_vendor():
    assert not ledger.payee_flag_applies("Coco Lee", "TST*RIVER BAR- SOMERVILL", True)
    assert not ledger.payee_flag_applies("Coco Lee", "River Bar", False), "raw text is never enough"
    assert ledger.payee_flag_applies("Ryan Mauskopf", "Joel Knutson", True)
    assert not ledger.payee_flag_applies("Lyft", "LYFT", True)


# ── 9. a coded charge is never hidden under Not project (2026-11-05) ──────
# Live on GrubHub (project 309): the $20,000 Steven Pierce BillPayment
# (txn 7038, split 6,000 + 14,000 to the two 1100 Director lines) and the
# $9,600 Nahuel Attar one (7037) sat under Not project with a dismissal the
# ledger migration had written for the project that KEPT them (plus the
# legacy flag) — off the Assigned list, their lines at $0.

def _line_actual(ws, lid):
    for sec in ws["budget"]["sections"]:
        for ln in sec["lines"]:
            if ln["id"] == lid:
                return ln["actual"]
    return None


def _ws(ctx, p):
    r = ctx["client"].get(f"/projects/{p}/actuals/workspace.json")
    assert r.status_code == 200, r.get_json()
    return r.get_json()


def test_26_the_7038_shape_split_plus_dismissal_goes_to_assigned_and_counts(ctx):
    from models import TransactionPoolDismissal, ActivityLog
    with ctx["app"].app_context():
        G, uid = S["grub"], S["uid"]
        l0, l1 = S[f"lines_{G}"][:2]
        wsb = _ws(ctx, G)
        line0_before, line1_before = _line_actual(wsb, l0), _line_actual(wsb, l1)
        t = _txn(G, "Steven Pierce", 20000, "2026-10-28", source="qbo_sync",
                 qbo_txn_id="7038", qbo_txn_type="BillPayment",
                 not_project_expense=True)            # the legacy flag, excluded_at NULL
        db.session.add_all([
            TransactionSplit(transaction_id=t.id, budget_line_id=l0, project_id=G, amount=Decimal("6000")),
            TransactionSplit(transaction_id=t.id, budget_line_id=l1, project_id=G, amount=Decimal("14000"))])
        db.session.add(TransactionPoolDismissal(transaction_id=t.id, project_id=G,
                                                reason="Not project (marked before the company ledger)",
                                                created_at=NOW, created_by=uid))
        db.session.commit()
        tid = t.id
        S["t7038"] = tid
        # the check lists it, as coded-and-hidden, nowhere else
        hidden = actuals.hidden_coded_rows(G)
        assert [x.id for x, _d in hidden] == [tid] and hidden[0][1] is not None
        assert tid not in {x.id for x, _d in actuals.dismissed_receipt_only_rows(G)}
        base0 = actuals.split_sums_by_line(G).get(l0, 0.0)       # Nahuel's 9,000 is already there
        base1 = actuals.split_sums_by_line(G).get(l1, 0.0)
        ws0 = _ws(ctx, G)      # NB: this load repairs it; the sums before are the hidden ones
        # the workspace repairs it on read: Assigned, counted, dismissal gone, logged once
        ws = ws0
        assert tid in {x["id"] for x in ws["transactions"]}
        assert tid not in {x["id"] for x in ws["not_project"]}
        row = next(x for x in ws["transactions"] if x["id"] == tid)
        assert row["excluded"] is None and len(row["splits"]) == 2
        assert _line_actual(ws, l0) == round(line0_before + 6000.0, 2)
        assert _line_actual(ws, l1) == round(line1_before + 14000.0, 2)
        db.session.expire_all()
        t = Transaction.query.get(tid)
        assert t.project_id == G and not t.not_project_expense and t.excluded_at is None
        assert actuals.pool_dismissal(tid, G) is None
        sums = actuals.split_sums_by_line(G)
        assert sums.get(l0) == round(base0 + 6000.0, 2) and sums.get(l1) == round(base1 + 14000.0, 2)
        logs = ActivityLog.query.filter_by(entity_type='transaction', entity_id=tid).all()
        assert len([a for a in logs if 'hidden under Not project' in (a.note or '')]) == 1
        assert actuals.hidden_coded_rows(G) == []
        # a second load logs nothing more
        _ws(ctx, G)
        logs2 = ActivityLog.query.filter_by(entity_type='transaction', entity_id=tid).all()
        assert len([a for a in logs2 if 'hidden under Not project' in (a.note or '')]) == 1
        # the invariant at the write: a dismissal by GrubHub on it is refused
        with pytest.raises(actuals.DismissalError):
            actuals.dismiss_for(Transaction.query.get(tid), G, "no")
        db.session.rollback()
        # ...while GINTS (which does not hold it) could still not see it, and Not project
        # through the ONE write releases first, so it never trips
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{tid}/mark-not-project", json={"reason": "x"})
        assert r.status_code == 200 and r.get_json()["mode"] == "dismissed"
        db.session.expire_all()
        t = Transaction.query.get(tid)
        assert t.project_id is None and actuals.pool_dismissal(tid, G) is not None
        assert TransactionSplit.query.filter_by(transaction_id=tid).count() == 0
        # and the undo of that mark puts the split back, claimed, no dismissal
        act = r.get_json()["activity_id"]
        r = ctx["client"].post(f"/projects/{G}/budget/{S[f'b_{G}']}/activity/{act}/undo", json={})
        assert r.status_code == 200, r.get_json()
        db.session.expire_all()
        t = Transaction.query.get(tid)
        assert t.project_id == G and TransactionSplit.query.filter_by(transaction_id=tid).count() == 2
        assert actuals.pool_dismissal(tid, G) is None
        assert tid in {x["id"] for x in _ws(ctx, G)["transactions"]}


def test_27_coding_a_row_with_a_stale_dismissal_unhides_it(ctx):
    """A line, a section or a split written on a row that carries this
    project's dismissal drops the dismissal (claim_for, even when the row is
    already this project's). A receipt alone leaves it."""
    from models import TransactionPoolDismissal
    with ctx["app"].app_context():
        G, uid = S["grub"], S["uid"]
        l0, l1 = S[f"lines_{G}"][:2]
        made = []
        for n in range(4):
            t = _txn(G, f"Stale {n}", 100 + n, "2026-10-29", source="qbo_sync",
                     qbo_txn_id=f"7100{n}", qbo_txn_type="Purchase")
            db.session.add(TransactionPoolDismissal(transaction_id=t.id, project_id=G, reason="legacy",
                                                    created_at=NOW, created_by=uid))
            made.append(t.id)
        db.session.commit()
        a, b, c, d = made
        # a line (set-line → actuals.link_transaction_to_line)
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{a}/set-line", json={"budget_line_id": l0})
        assert r.status_code == 200, r.get_json()
        assert actuals.pool_dismissal(a, G) is None
        # a section (set-coa)
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{b}/set-coa", json={"account_code": 3401})
        assert r.status_code == 200, r.get_json()
        assert actuals.pool_dismissal(b, G) is None
        # a split
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{c}/split",
                               json={"splits": [{"budget_line_id": l0, "amount": 50},
                                                {"budget_line_id": l1, "amount": 52}]})
        assert r.status_code == 200, r.get_json()
        assert actuals.pool_dismissal(c, G) is None
        # a receipt only: the dismissal stays (receipt + Not project is a legitimate state)
        doc = _doc(G, uid, "Stale 3", 103, date(2026, 10, 29), "stale-3.pdf")
        db.session.commit()
        r = ctx["client"].post(f"/projects/{G}/actuals/transaction/{d}/link-doc", json={"doc_upload_id": doc.id})
        assert r.status_code == 200, r.get_json()
        assert actuals.pool_dismissal(d, G) is not None
        db.session.expire_all()
        assert Transaction.query.get(d).doc_upload_id == doc.id
        S["stale_receipt"] = d
        # the helper itself, on a held row: the dismissal goes with the line
        e = _txn(G, "Stale 4", 104, "2026-10-29", source="qbo_sync", qbo_txn_id="71004", qbo_txn_type="Purchase")
        db.session.add(TransactionPoolDismissal(transaction_id=e.id, project_id=G, reason="legacy", created_at=NOW))
        db.session.commit()
        actuals.link_transaction_to_line(e.id, l1, user_id=uid)
        assert actuals.pool_dismissal(e.id, G) is None
        for tid in (a, b, c, e.id):
            assert tid in {x["id"] for x in _ws(ctx, G)["transactions"]}


def test_28_receipt_only_and_dismissed_stays_under_not_project_and_is_listed_for_review(ctx):
    from models import TransactionPoolDismissal
    with ctx["app"].app_context():
        G, uid = S["grub"], S["uid"]
        # the personal Amazon charge with its receipt, excluded — the legacy
        # shape carries the flag AND a dismissal by the holding project
        doc = _doc(G, uid, "Amazon", 31.5, date(2026, 10, 30), "amazon-personal.pdf")
        t = _txn(G, "Amazon", 31.5, "2026-10-30", source="reconciled", qbo_txn_id="7200",
                 qbo_txn_type="Purchase", doc_upload_id=doc.id, match_status="confirmed",
                 not_project_expense=True, excluded_reason="personal", excluded_at=NOW,
                 excluded_by_user_id=uid)
        db.session.add(TransactionPoolDismissal(transaction_id=t.id, project_id=G, reason="personal",
                                                created_at=NOW, created_by=uid))
        db.session.commit()
        tid = t.id
        ws = _ws(ctx, G)
        assert tid in {x["id"] for x in ws["not_project"]}, "receipt + Not project stays excluded"
        assert tid not in {x["id"] for x in ws["transactions"]}
        assert actuals.pool_dismissal(tid, G) is not None, "nothing touched it"
        assert tid not in {x.id for x, _d in actuals.hidden_coded_rows(G)}
        listed = {x.id for x, _d in actuals.dismissed_receipt_only_rows(G)}
        assert tid in listed and S["stale_receipt"] in listed
        nj = ctx["client"].get(f"/projects/{G}/actuals/not-project.json").get_json()
        assert tid in {x["id"] for x in nj["not_project"]}
        # the Maintenance page: under receipt_only, not under coded
        r = ctx["client"].get(f"/admin/actuals/hidden-coded?project={G}&format=json")
        assert r.status_code == 200, r.get_data(as_text=True)[:300]
        d = r.get_json()
        assert tid not in {x["id"] for x in d["coded"]}
        ro = next(x for x in d["receipt_only"] if x["id"] == tid)
        assert ro["flagged"] and ro["dismissed"] and ro["signals"] == ["receipt"]
        assert ro["restore_url"].endswith(f"/transaction/{tid}/recover")
        r = ctx["client"].get(f"/admin/actuals/hidden-coded?project={G}")
        assert r.status_code == 200 and b"Receipt only, under Not project" in r.data
        # Restore (a person's call) clears the flag AND the stale dismissal; the receipt stays
        r = ctx["client"].post(ro["restore_url"], json={})
        assert r.status_code == 200, r.get_json()
        db.session.expire_all()
        t = Transaction.query.get(tid)
        assert not t.not_project_expense and t.doc_upload_id == doc.id and t.project_id == G
        assert actuals.pool_dismissal(tid, G) is None
        assert tid in {x["id"] for x in _ws(ctx, G)["transactions"]}
        # undo of the Restore: excluded again, dismissal back (a receipt is no coding)
        act = r.get_json()["activity_id"]
        r = ctx["client"].post(f"/projects/{G}/budget/{S[f'b_{G}']}/activity/{act}/undo", json={})
        assert r.status_code == 200, r.get_json()
        db.session.expire_all()
        t = Transaction.query.get(tid)
        assert t.not_project_expense and actuals.pool_dismissal(tid, G) is not None
        assert tid in {x["id"] for x in _ws(ctx, G)["not_project"]}


def test_29_the_maintenance_fix_and_its_undo_respect_the_invariant(ctx):
    from models import TransactionPoolDismissal
    with ctx["app"].app_context():
        G, N, uid = S["grub"], S["gints"], S["uid"]
        gl, nl = S[f"lines_{G}"], S[f"lines_{N}"]
        # three hidden coded rows across two projects: a line + flag, a
        # section + dismissal, a split + both
        a = _txn(G, "Line + flag", 10, "2026-10-31", source="qbo_sync", qbo_txn_id="7301",
                 qbo_txn_type="Purchase", budget_line_id=gl[0], account_code=2700, not_project_expense=True)
        b = _txn(N, "Section + dismissal", 20, "2026-10-31", source="qbo_sync", qbo_txn_id="7302",
                 qbo_txn_type="Purchase", account_code=3401)
        c = _txn(N, "Split + both", 30, "2026-10-31", source="qbo_sync", qbo_txn_id="7303",
                 qbo_txn_type="Purchase", not_project_expense=True)
        db.session.add_all([
            TransactionPoolDismissal(transaction_id=b.id, project_id=N, reason="legacy", created_at=NOW),
            TransactionPoolDismissal(transaction_id=c.id, project_id=N, reason="legacy", created_at=NOW),
            TransactionSplit(transaction_id=c.id, budget_line_id=nl[0], project_id=N, amount=Decimal("30"))])
        db.session.commit()
        ids = (a.id, b.id, c.id)
        r = ctx["client"].get("/admin/actuals/hidden-coded?format=json")
        assert r.status_code == 200
        d = r.get_json()
        assert {x["id"] for x in d["coded"]} >= set(ids)
        by = {x["id"]: x for x in d["coded"]}
        assert by[a.id]["flagged"] and not by[a.id]["dismissed"] and by[a.id]["signals"] == ["line"]
        assert by[b.id]["dismissed"] and by[b.id]["signals"] == ["section"]
        assert by[c.id]["dismissed"] and by[c.id]["flagged"] and by[c.id]["signals"] == ["split"]
        assert by[c.id]["coding"].startswith("Split:")
        # HTML renders
        r = ctx["client"].get("/admin/actuals/hidden-coded")
        assert r.status_code == 200 and b"Coded and hidden" in r.data and b"Split + both" in r.data
        # fix one
        r = ctx["client"].post("/admin/actuals/hidden-coded/fix", json={"transaction_id": a.id})
        assert r.status_code == 200 and r.get_json()["fixed"] == 1, r.get_json()
        act_a = r.get_json()["rows"][0]["activity_id"]
        db.session.expire_all()
        assert not Transaction.query.get(a.id).not_project_expense
        # fixing it again: no longer listed
        r = ctx["client"].post("/admin/actuals/hidden-coded/fix", json={"transaction_id": a.id})
        assert r.status_code == 409
        # undo of the fix while the line stands is refused — the row would be hidden again
        r = ctx["client"].post(f"/projects/{G}/budget/{S[f'b_{G}']}/activity/{act_a}/undo", json={})
        assert r.status_code == 409, r.get_json()
        assert "coded this charge" in r.get_json()["error"]
        db.session.expire_all()
        assert not Transaction.query.get(a.id).not_project_expense
        # fix all (scoped to GINTS)
        r = ctx["client"].post("/admin/actuals/hidden-coded/fix", json={"all": True, "project": N})
        assert r.status_code == 200 and r.get_json()["fixed"] == 2, r.get_json()
        db.session.expire_all()
        assert actuals.pool_dismissal(b.id, N) is None and actuals.pool_dismissal(c.id, N) is None
        assert not Transaction.query.get(c.id).not_project_expense
        assert actuals.hidden_coded_rows() == []
        # rollup: the split counts in GINTS now
        assert actuals.split_sums_by_line(N).get(nl[0]) == 30.0
        ws = _ws(ctx, N)
        assert {b.id, c.id} <= {x["id"] for x in ws["transactions"]}
        assert not ({b.id, c.id} & {x["id"] for x in ws["not_project"]})


def test_30_migration_never_dismisses_the_keepers_own_project_when_it_codes_the_row(ctx):
    """A re-run of the ledger migration on the 7038 shape: the keeper is
    split by GrubHub and carries the legacy flag; a GINTS copy carries the
    flag too. One dismissal — for GINTS — never one for GrubHub."""
    from models import TransactionPoolDismissal
    with ctx["app"].app_context():
        db.session.execute(text("DROP INDEX IF EXISTS uq_transaction_qbo_company"))
        G, N = S["grub"], S["gints"]
        gl = S[f"lines_{G}"]
        keep = _txn(G, "Steven Pierce", 20000, "2026-10-28", source="qbo_sync",
                    qbo_txn_id="7038m", qbo_txn_type="BillPayment", not_project_expense=True)
        copy = _txn(N, "STEVEN PIERCE", 20000, "2026-10-28", source="qbo_sync",
                    qbo_txn_id="7038m", qbo_txn_type="BillPayment", not_project_expense=True,
                    excluded_reason="GrubHub job")
        db.session.add_all([
            TransactionSplit(transaction_id=keep.id, budget_line_id=gl[0], project_id=G, amount=Decimal("6000")),
            TransactionSplit(transaction_id=keep.id, budget_line_id=gl[1], project_id=G, amount=Decimal("14000"))])
        # a section-coded keeper whose own copy in the other project is uncoded + flagged
        keep2 = _txn(N, "Lyft", 12, "2026-10-28", source="qbo_sync", qbo_txn_id="7039m",
                     qbo_txn_type="Purchase", account_code=3401, not_project_expense=True)
        copy2 = _txn(G, "LYFT", 12, "2026-10-28", source="qbo_sync", qbo_txn_id="7039m",
                     qbo_txn_type="Purchase", not_project_expense=True)
        db.session.commit()
        keys = {("7038m", "BillPayment"), ("7039m", "Purchase")}
        pre = ledger.preflight(db, keys=keys)
        assert pre["counts"]["dismissals_to_create"] == 2, pre["counts"]      # GINTS on 7038m, GrubHub on 7039m
        a = ledger.analyze(db, keys=keys)
        assert {(k, p) for k, p, *_ in a["dismissals"]} == {(keep.id, N), (keep2.id, G)}
        r = ledger.run_migration(user_id=S["uid"], keys=keys)
        assert r["deleted"] == 2 and r["dismissals"] == 2, r
        assert Transaction.query.get(copy.id) is None and Transaction.query.get(copy2.id) is None
        dis = {(d.transaction_id, d.project_id) for d in TransactionPoolDismissal.query.filter(
            TransactionPoolDismissal.transaction_id.in_([keep.id, keep2.id])).all()}
        assert dis == {(keep.id, N), (keep2.id, G)}
        assert actuals.pool_dismissal(keep.id, G) is None and actuals.pool_dismissal(keep2.id, N) is None
        # ...and the legacy flag on the coded keepers is cleared by the run itself
        assert r["unhidden"] == 2, r
        assert not Transaction.query.get(keep.id).not_project_expense
        assert not Transaction.query.get(keep2.id).not_project_expense
        assert not any(x.id in (keep.id, keep2.id) for x, _d in actuals.hidden_coded_rows())
        assert ledger.analyze(db, keys=keys)["dismissals"] == []
        ws = _ws(ctx, G)
        assert keep.id in {x["id"] for x in ws["transactions"]}
        assert keep.id not in {x["id"] for x in ws["not_project"]}
