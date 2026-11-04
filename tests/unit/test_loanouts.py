"""Loan-out companies + the ONE payee identity rule, and Compare duplicates
in place (2026-11-04).

Owner, Nov 4: "Danielle Miller is Two Foot Shuffle LLC … we want the
individuals, but then we still need the company attached somehow." and
"You Choose, LLC is not Sarah Caye Productions — those are different."
And: "when I go to Actuals → Unclaimed I see 'possible duplicate' … Can I
compare them? … compare and settle this right in this view."

Part 1 is pure (actuals.same_payee over an explicit LoanoutIndex, the
ledger's payee-differs rule, the matching engine's vendor score, the
duplicate twin rule, actuals.loanout_proposal, actuals.compare_columns).
Part 2 boots the real app on sqlite (or reuses the one another app-booting
module opened) and walks the routes: Payee page loan-outs, doc / QuickBooks
attribution, the payee-differs recheck and "Same person" / "Different",
the Compare payload, Keep this one + Undo, Keep both remembered.

Run: python -m pytest tests/unit/test_loanouts.py -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
import tempfile
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace as NS

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

import actuals  # noqa: E402
from actuals import (LoanoutIndex, build_loanout_index, same_payee, score_match,  # noqa: E402
                     vendor_score, is_duplicate_twin, use_loanout_index, loanout_proposal,
                     compare_columns, payee_key, loanout_via, payee_display)


# ── Part 1: pure ───────────────────────────────────────────────────────────

DANIELLE = NS(id=501, name="Danielle Miller", legal_name=None, qbo_vendor_id="LO-301")
SARAH = NS(id=502, name="Sarah Caye", legal_name=None, qbo_vendor_id="LO-311")


def _idx():
    return build_loanout_index(
        [DANIELLE, SARAH],
        [NS(id=1, payee_id=501, company_name="Two Foot Shuffle LLC", qbo_vendor_id="LO-302"),
         NS(id=2, payee_id=501, company_name="Danielle Miller Edit, Inc.", qbo_vendor_id=None),
         NS(id=3, payee_id=502, company_name="Sarah Caye Productions", qbo_vendor_id="LO-312")])


def test_same_payee_direct_names_and_refs():
    idx = LoanoutIndex()
    assert same_payee("Lyft", "LYFT *RIDE SUN 9PM", index=idx)
    assert same_payee("Two Foot Shuffle, LLC", "Two Foot Shuffle LLC", index=idx)
    assert same_payee("Two Foot Shuffle LLC", "Two Foot Shuffle", index=idx), "entity suffix is not identity"
    assert same_payee(("", "77"), ("Somebody", "77"), index=idx), "same QuickBooks vendor id"
    assert not same_payee("Danielle Miller", "Two Foot Shuffle LLC", index=idx), "no link → different"
    assert not same_payee("", "", index=idx)


def test_same_payee_through_a_loan_out_and_a_shared_person():
    idx = _idx()
    assert same_payee("Danielle Miller", "Two Foot Shuffle LLC", index=idx)
    assert same_payee("Two Foot Shuffle, L.L.C.", "danielle miller", index=idx)
    # both are loan-outs of the same person
    assert same_payee("Two Foot Shuffle LLC", "Danielle Miller Edit Inc", index=idx)
    # QuickBooks refs: the person's vendor and the loan-out's vendor
    assert same_payee(("", "LO-301"), ("", "LO-302"), index=idx)
    assert same_payee(("Danielle Miller", "LO-301"), "Two Foot Shuffle LLC", index=idx)


def test_you_choose_is_not_sarah_caye_productions():
    idx = _idx()
    assert not same_payee("You Choose, LLC", "Sarah Caye Productions", index=idx)
    assert not same_payee("You Choose, LLC", "Two Foot Shuffle LLC", index=idx), "two LLCs are not one payee"
    assert same_payee("Sarah Caye", "Sarah Caye Productions", index=idx)


def test_ambiguous_company_maps_to_nobody():
    idx = build_loanout_index([DANIELLE, SARAH],
                              [NS(id=1, payee_id=501, company_name="Shared Co LLC", qbo_vendor_id=None),
                               NS(id=2, payee_id=502, company_name="Shared Co", qbo_vendor_id=None)])
    assert idx.person_for("Shared Co LLC") is None
    assert not same_payee("Danielle Miller", "Sarah Caye", index=idx)


def test_ledger_payees_differ_uses_same_payee_and_ignores_llc():
    import ledger
    with use_loanout_index(_idx()):
        assert not ledger.payees_differ("Two Foot Shuffle LLC", "Danielle Miller")
        assert not ledger.payee_flag_applies("Two Foot Shuffle LLC", "Danielle Miller", True)
        assert ledger.payees_differ("You Choose, LLC", "Sarah Caye Productions")
        assert ledger.payee_flag_applies("You Choose, LLC", "Sarah Caye Productions", True)
        assert ledger.payees_differ("You Choose LLC", "Two Foot Shuffle LLC"), "LLC is not a shared word"
    with use_loanout_index(LoanoutIndex()):
        assert ledger.payees_differ("Two Foot Shuffle LLC", "Danielle Miller"), "no link → still flagged"
        assert ledger.payee_flag_applies("Ryan Mauskopf", "Joel Knutson", True)
        assert not ledger.payee_flag_applies("Lyft", "LYFT", True)


def test_score_match_credits_the_vendor_through_a_loan_out():
    charge = {"id": 1, "amount": 4800.00, "txn_date": "2026-09-21", "vendor": "Danielle Miller",
              "qbo_entity_ref": "LO-301"}
    invoice = {"id": 9, "amount": 4800.00, "doc_date": date(2026, 9, 19), "vendor": "Two Foot Shuffle LLC"}
    with use_loanout_index(LoanoutIndex()):
        before = score_match(charge, invoice)
    with use_loanout_index(_idx()):
        after = score_match(charge, invoice)
        assert vendor_score("Danielle Miller", "Two Foot Shuffle LLC") == (1.0, 'same payee (loan-out)')
        assert vendor_score("You Choose LLC", "Two Foot Shuffle LLC")[0] == 0.2
    assert before["vendor_score"] == 0.2 and after["vendor_score"] == 1.0
    assert 'same payee (loan-out)' in after["reasons"]
    assert after["confidence"] > before["confidence"]


def _row(**kw):
    base = dict(id=None, amount=None, txn_date=None, vendor=None, card_last4=None,
                qbo_txn_id=None, qbo_txn_type=None, qbo_entity_ref=None)
    base.update(kw)
    return NS(**base)


def test_duplicate_twin_sees_a_person_and_their_loan_out():
    a = _row(id=1, amount=Decimal("4800"), txn_date="2026-09-21", vendor="Danielle Miller",
             qbo_txn_id="A1", qbo_entity_ref="LO-301")
    b = _row(id=2, amount=Decimal("4800"), txn_date="2026-09-22", vendor="Two Foot Shuffle LLC",
             qbo_txn_id="A2", qbo_entity_ref="LO-302")
    c = _row(id=3, amount=Decimal("4800"), txn_date="2026-09-22", vendor="Two Foot Shuffle LLC", qbo_txn_id="A3")
    with use_loanout_index(LoanoutIndex()):
        assert not is_duplicate_twin(a, b), "two QuickBooks payees, no link"
        assert not is_duplicate_twin(a, c)
    with use_loanout_index(_idx()):
        assert is_duplicate_twin(a, b), "the person's vendor and the loan-out's vendor are one payee"
        assert is_duplicate_twin(a, c), "vendor text through the loan-out"
        joel = _row(id=4, amount=Decimal("4800"), txn_date="2026-09-21", vendor="X", qbo_txn_id="A4",
                    qbo_entity_ref="LO-999")
        assert not is_duplicate_twin(a, joel)


def test_loanout_proposal_picks_the_person_and_the_company():
    people = [NS(id=501, name="Danielle Miller", legal_name=None, kind="person", is_vendor=False,
                 qbo_vendor_id="LO-301"),
              NS(id=600, name="Two Foot Shuffle LLC", legal_name=None, kind="loan_out", is_vendor=True,
                 qbo_vendor_id=None)]
    vendors = [NS(qbo_vendor_id="LO-301", display_name="Danielle Miller", company_name=None),
               NS(qbo_vendor_id="LO-302", display_name="Two Foot Shuffle LLC", company_name="Two Foot Shuffle LLC")]
    p = loanout_proposal("Danielle Miller", "LO-301", "Two Foot Shuffle LLC", people=people, qbo_vendors=vendors)
    assert p == {"person_id": 501, "person_name": "Danielle Miller",
                 "company_name": "Two Foot Shuffle LLC", "qbo_vendor_id": "LO-302"}
    # QuickBooks paid the company; the receipt names the person
    p2 = loanout_proposal("Two Foot Shuffle LLC", "LO-302", "Danielle Miller", people=people, qbo_vendors=vendors)
    assert p2["person_id"] == 501 and p2["company_name"] == "Two Foot Shuffle LLC" and p2["qbo_vendor_id"] == "LO-302"
    assert loanout_proposal("You Choose, LLC", None, "Sarah Caye Productions", people=people) is None


def test_loanout_via_and_display():
    idx = _idx()
    assert loanout_via(501, ("Danielle Miller", "LO-302"), index=idx) == "Two Foot Shuffle LLC"
    assert loanout_via(501, "Two Foot Shuffle, LLC", index=idx) == "Two Foot Shuffle LLC"
    assert loanout_via(501, "Danielle Miller", index=idx) is None, "paid directly"
    assert payee_display("Danielle Miller", "Two Foot Shuffle LLC") == "Danielle Miller · via Two Foot Shuffle LLC"
    assert payee_key("Two Foot Shuffle, L.L.C.") == "two foot shuffle"


def test_compare_columns_marks_the_keeper_and_the_differences():
    rows = [
        {"id": 12, "source": "QuickBooks", "date": "2026-10-20", "amount": 4500.0, "vendor": "Ryan Mauskopf",
         "payee_ref": ["Ryan Mauskopf", None], "card": "ACH", "qbo": {"type": "BillPayment", "id": "322"},
         "memo": "ACH", "coding": "", "backup": None, "match": "No receipt",
         "imported": {"at": "2026-10-21T09:00", "by": "QuickBooks sync"}, "delete_mode": "delete"},
        {"id": 11, "source": "QuickBooks", "date": "2026-10-19", "amount": 4500.0, "vendor": "Ryan Mauskopf",
         "payee_ref": ["Ryan Mauskopf", None], "card": "ACH", "qbo": {"type": "BillPayment", "id": "319"},
         "memo": "ACH", "coding": "2101 · Talent", "backup": {"doc_id": 5}, "match": "Receipt confirmed",
         "imported": {"at": "2026-10-20T09:00", "by": "QuickBooks sync"}, "delete_mode": "merge"},
    ]
    out = compare_columns(rows, 11, "has the receipt", index=LoanoutIndex())
    assert [c["id"] for c in out["columns"]] == [11, 12], "keeper first"
    assert out["columns"][0]["is_keeper"] and not out["columns"][1]["is_keeper"]
    assert set(out["diff"]) == {"date", "coding", "backup", "match", "imported"}
    assert "qbo" not in out["diff"], "QuickBooks ids always differ; the type is compared"
    assert out["same_payee"] and out["payee_note"] is None
    assert "#12 is deleted" in out["columns"][0]["keep_effect"]
    assert "#11 is removed after its coding" in out["columns"][1]["keep_effect"]
    assert [f["key"] for f in out["fields"]][:3] == ["source", "date", "amount"]
    # a person and their loan-out: same payee, noted not highlighted
    rows[1]["payee_ref"] = ["Two Foot Shuffle LLC", None]
    out = compare_columns(rows, 11, "has the receipt", index=build_loanout_index(
        [NS(id=1, name="Ryan Mauskopf", legal_name=None, qbo_vendor_id=None)],
        [NS(id=1, payee_id=1, company_name="Two Foot Shuffle LLC", qbo_vendor_id=None)]))
    assert out["same_payee"] and "payee" not in out["diff"] and out["payee_note"]
    out = compare_columns(rows, 11, "has the receipt", index=LoanoutIndex())
    assert not out["same_payee"] and "payee" in out["diff"]


# ── Part 2: app-booting ────────────────────────────────────────────────────

_DB_DIR = tempfile.mkdtemp(prefix="fpb-loanout-")
_PRESET = os.environ.get("DATABASE_URL", "")
if _PRESET and not _PRESET.startswith("sqlite:"):
    _IMPORT_ERR = RuntimeError("DATABASE_URL is not sqlite; refusing to boot")
else:
    os.environ.setdefault("DATABASE_URL", "sqlite:///" + os.path.join(_DB_DIR, "t.db"))
    os.environ.setdefault("SECRET_KEY", "test")
    try:
        _cwd = os.getcwd()
        os.chdir(ROOT)
        import app as fpapp                       # noqa: E402
        from models import (db, User, ProjectSheet, ProjectAccess, Budget, BudgetLine,  # noqa: E402
                            DocUpload, Transaction, CrewMember, QBOVendor, PayeeLoanout,
                            ExpenseEvidence, TransactionDupDismissal, ActivityLog)
        import ledger                             # noqa: E402
        import qbo_sync                           # noqa: E402
        import routes.actuals                     # noqa: F401,E402
        import routes.actuals_detail              # noqa: F401,E402
        import routes.payees                      # noqa: F401,E402
        import routes.ledger_admin                # noqa: F401,E402
        _IMPORT_ERR = None
    except Exception as _e:                       # pragma: no cover
        _IMPORT_ERR = _e
    finally:
        try:
            os.chdir(_cwd)
        except Exception:
            pass

needs_app = pytest.mark.skipif(_IMPORT_ERR is not None, reason=f"app.py not importable here: {_IMPORT_ERR}")
S = {}
NOW = datetime(2026, 11, 4, 12, 0, 0)


@pytest.fixture(scope="module")
def ctx():
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        u = User.query.filter_by(email="loanout-admin@thefp.tv").first()
        if not u:
            u = User(email="loanout-admin@thefp.tv", name="Loanout Admin", role="super_admin")
            db.session.add(u)
        u.role = "super_admin"
        u.set_password("pw")
        p = ProjectSheet(name="Global Run Strong 2026 (loan-outs)", status="active")
        p2 = ProjectSheet(name="Other show (loan-outs)", status="active")
        db.session.add_all([p, p2])
        db.session.flush()
        for pr in (p, p2):
            db.session.add(ProjectAccess(project_id=pr.id, user_id=u.id, role="owner"))
        b = Budget(project_id=p.id, name="Working", budget_mode="working", version_status="current")
        db.session.add(b)
        db.session.flush()
        ln = BudgetLine(budget_id=b.id, account_code=4701, account_name="Editor", description="Editor",
                        quantity=1, rate_type="flat_day", days=1, rate=100, sort_order=1)
        db.session.add(ln)
        dm = CrewMember(name="Danielle Miller", kind="person", active=True, qbo_vendor_id="LO-301")
        tfs_rec = CrewMember(name="Two Foot Shuffle LLC", kind="loan_out", is_vendor=True, active=True,
                             qbo_vendor_id="LO-302")
        sc = CrewMember(name="Sarah Caye", kind="person", active=True, qbo_vendor_id="LO-311")
        db.session.add_all([dm, tfs_rec, sc])
        for vid, nm in (("LO-301", "Danielle Miller"), ("LO-302", "Two Foot Shuffle LLC"),
                        ("LO-311", "Sarah Caye Productions"), ("LO-312", "You Choose, LLC")):
            if QBOVendor.query.get(vid) is None:
                db.session.add(QBOVendor(qbo_vendor_id=vid, display_name=nm, active=True))
        db.session.flush()
        S.update(uid=u.id, pid=p.id, pid2=p2.id, bid=b.id, line=ln.id, dm=dm.id, tfs_rec=tfs_rec.id, sc=sc.id)
        db.session.commit()
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["_user_id"] = str(S["uid"])
        sess["_fresh"] = True
        sess["_csrf"] = "t"
    c.environ_base["HTTP_X_CSRFTOKEN"] = "t"
    yield {"app": app, "client": c}
    # Other app-booting modules share this database: leave no mirrored
    # QuickBooks vendors or loan-out links behind (they count the mirror).
    with app.app_context():
        PayeeLoanout.query.delete(synchronize_session=False)
        QBOVendor.query.filter(QBOVendor.qbo_vendor_id.like("LO-%")).delete(synchronize_session=False)
        db.session.commit()


def _txn(pid, vendor, amount, d, **kw):
    t = Transaction(project_id=pid, vendor=vendor, amount=Decimal(str(amount)), is_expense=True,
                    txn_date=d, created_at=NOW, updated_at=NOW, **kw)
    db.session.add(t)
    db.session.flush()
    return t


def _doc(pid, vendor, amount, d, name, category="invoice"):
    doc = DocUpload(project_id=pid, uploader_id=S["uid"], original_filename=name, filed_filename=name,
                    status="filed", vendor=vendor, amount=Decimal(str(amount)), doc_date=d,
                    category=category, content_type="application/pdf")
    db.session.add(doc)
    db.session.flush()
    return doc


@needs_app
def test_10_payee_differs_flags_until_the_loan_out_is_linked_then_same_person_clears(ctx):
    c = ctx["client"]
    with ctx["app"].app_context():
        pid = S["pid"]
        inv = _doc(pid, "Two Foot Shuffle LLC", 4800, date(2026, 9, 19), "TFS-0921.pdf")
        t = _txn(pid, "Two Foot Shuffle LLC", 4800, "2026-09-21", source="reconciled",
                 qbo_txn_id="1300000304", qbo_txn_type="BillPayment", qbo_entity_ref="LO-301",
                 doc_upload_id=inv.id, match_status="confirmed", budget_line_id=S["line"],
                 claim_conflict=True, claim_conflict_reason=f"{ledger.PAYEE_DIFFERS} (Danielle Miller)")
        db.session.add(ExpenseEvidence(transaction_id=t.id, doc_upload_id=inv.id, kind="primary"))
        yc_inv = _doc(pid, "You Choose, LLC", 1250, date(2026, 9, 24), "you-choose-7.pdf")
        y = _txn(pid, "You Choose, LLC", 1250, "2026-09-26", source="reconciled",
                 qbo_txn_id="1300000307", qbo_txn_type="BillPayment", qbo_entity_ref="LO-311",
                 doc_upload_id=yc_inv.id, match_status="confirmed", budget_line_id=S["line"],
                 claim_conflict=True, claim_conflict_reason=f"{ledger.PAYEE_DIFFERS} (Sarah Caye Productions)")
        db.session.commit()
        S.update(danielle_txn=t.id, yc_txn=y.id, tfs_doc=inv.id)
        assert ledger.recheck_payee_flags() == [], "nothing linked yet → both stay flagged"
        cards = {cd["qbo_txn_id"]: cd for cd in ledger.conflicts(project_id=pid)}
        pd = cards["1300000304"]["payee_differs"]
        assert pd["proposal"]["person_id"] == S["dm"] and pd["proposal"]["company_name"] == "Two Foot Shuffle LLC"
        assert pd["proposal"]["qbo_vendor_id"] == "LO-302"
    # the detail window offers it too
    r = c.get(f"/projects/{S['pid']}/actuals/transaction/{S['danielle_txn']}/detail.json")
    assert r.status_code == 200
    pdd = r.get_json()["payee_differs"]
    assert pdd and pdd["can_resolve"] and pdd["proposal"]["person_name"] == "Danielle Miller"
    # Different keeps the flag and stops asking (You Choose ≠ Sarah Caye)
    r = c.post("/admin/ledger/conflicts/different", json={"txn_id": S["yc_txn"]})
    assert r.status_code == 200 and r.get_json()["flagged"]
    with ctx["app"].app_context():
        y = Transaction.query.get(S["yc_txn"])
        assert y.claim_conflict and ledger.DIFFERENT_CONFIRMED in y.claim_conflict_reason
        assert ledger.qbo_payee_from_reason(y.claim_conflict_reason) == "Sarah Caye Productions"
        assert ledger.payee_differs_info(y)["different_confirmed"]
    # Same person: links the loan-out, clears the flag, attributes the charge
    r = c.post("/admin/ledger/conflicts/same-person", json={
        "txn_id": S["danielle_txn"], "person_id": S["dm"], "company_name": "Two Foot Shuffle LLC",
        "qbo_vendor_id": "LO-302"})
    assert r.status_code == 200, r.get_json()
    j = r.get_json()
    assert j["person_name"] == "Danielle Miller" and j["company_name"] == "Two Foot Shuffle LLC"
    with ctx["app"].app_context():
        t = Transaction.query.get(S["danielle_txn"])
        assert not t.claim_conflict and t.payee_id == S["dm"]
        lo = PayeeLoanout.query.filter_by(payee_id=S["dm"]).one()
        assert lo.company_name == "Two Foot Shuffle LLC" and lo.qbo_vendor_id == "LO-302"
        assert lo.vendor_match_key == actuals.vendor_match_key("Two Foot Shuffle LLC")
        assert Transaction.query.get(S["yc_txn"]).claim_conflict, "You Choose stays flagged"
        assert ActivityLog.query.filter(ActivityLog.entity_type == 'payee_loanout').count() >= 1
    # the detail window header now reads "Danielle Miller · via Two Foot Shuffle LLC"
    r = c.get(f"/projects/{S['pid']}/actuals/transaction/{S['danielle_txn']}/detail.json")
    pay = r.get_json()["txn"]["payee"]
    assert pay["label"] == "Danielle Miller · via Two Foot Shuffle LLC"
    assert r.get_json()["payee_differs"] is None


@needs_app
def test_11_recheck_clears_another_flag_the_link_explains(ctx):
    with ctx["app"].app_context():
        pid = S["pid"]
        inv = _doc(pid, "Two Foot Shuffle, LLC", 900, date(2026, 10, 2), "TFS-1002.pdf")
        t = _txn(pid, "Two Foot Shuffle, LLC", 900, "2026-10-03", source="reconciled",
                 qbo_txn_id="1300000390", qbo_txn_type="BillPayment", qbo_entity_ref="LO-301",
                 doc_upload_id=inv.id, match_status="confirmed",
                 claim_conflict=True, claim_conflict_reason=f"{ledger.PAYEE_DIFFERS} (Danielle Miller)")
        db.session.commit()
        cleared = ledger.recheck_payee_flags()
        db.session.commit()
        assert t.id in cleared and S["yc_txn"] not in cleared
        assert not Transaction.query.get(t.id).claim_conflict


@needs_app
def test_12_payee_page_adds_and_lists_loan_outs_and_refuses_bad_ones(ctx):
    c = ctx["client"]
    r = c.post(f"/payees/{S['sc']}/loanouts", json={"company_name": "Sarah Caye Productions"})
    assert r.status_code == 200 and r.get_json()["created"]
    # a company already Danielle's cannot be Sarah's
    r = c.post(f"/payees/{S['sc']}/loanouts", json={"company_name": "Two Foot Shuffle LLC"})
    assert r.status_code == 400 and "already Danielle Miller" in r.get_json()["error"]
    # the person's own name is not a loan-out; a vendor record is not a person
    r = c.post(f"/payees/{S['sc']}/loanouts", json={"company_name": "Sarah Caye"})
    assert r.status_code == 400
    r = c.post(f"/payees/{S['tfs_rec']}/loanouts", json={"company_name": "Anything LLC"})
    assert r.status_code == 400
    r = c.post(f"/payees/{S['sc']}/loanouts", json={"company_name": "Caye Co", "qbo_vendor_id": "NOPE"})
    assert r.status_code == 400 and "mirror" in r.get_json()["error"]
    # still You Choose ≠ Sarah Caye Productions after Sarah's loan-out exists
    with ctx["app"].app_context():
        assert not same_payee("You Choose, LLC", "Sarah Caye Productions")
        assert same_payee("Sarah Caye", "Sarah Caye Productions")
    r = c.get(f"/payees/{S['dm']}.json")
    d = r.get_json()
    assert [lo["company_name"] for lo in d["loanouts"]] == ["Two Foot Shuffle LLC"]
    assert d["loanouts"][0]["paid_all_time"] >= 4800.0
    assert c.get(f"/payees/{S['dm']}.json?year=2025").get_json()["loanouts"][0]["paid_year"] == 0.0
    r = c.get(f"/payees/{S['dm']}.json?year=2026")
    assert r.get_json()["loanouts"][0]["paid_year"] >= 4800.0, "the 1099 view: paid through the company"
    r = c.get(f"/payees/{S['tfs_rec']}.json")
    assert r.get_json()["loanout_of"]["name"] == "Danielle Miller"
    r = c.get(f"/payees/{S['dm']}")
    assert r.status_code == 200 and b"Loan-out companies" in r.data and b"Two Foot Shuffle LLC" in r.data
    r = c.get("/payees.json?year=2026")
    row = next(x for x in r.get_json()["rows"] if x["id"] == S["dm"])
    assert row["loanouts"] == ["Two Foot Shuffle LLC"]
    r = c.get("/payees?year=2026")
    assert b"via Two Foot Shuffle LLC" in r.data
    r = c.get("/payees/report.csv?year=2026")
    assert b"via loan-out" in r.data and b"Two Foot Shuffle LLC 4" in r.data


@needs_app
def test_13_documents_and_quickbooks_refs_attribute_to_the_person(ctx):
    with ctx["app"].app_context():
        assert actuals.payee_for_vendor_string(S["pid"], "Two Foot Shuffle, LLC") == S["dm"]
        assert actuals.payee_for_vendor_string(S["pid"], "You Choose, LLC") is None
        m = qbo_sync._payee_by_qbo_vendor(db)
        assert m["LO-302"] == S["dm"], "a VendorRef to the loan-out's vendor attributes the person"
        assert m["LO-301"] == S["dm"]
        doc = _doc(S["pid"], "Two Foot Shuffle LLC", 300, date(2026, 10, 9), "TFS-1009.pdf")
        db.session.commit()
        all_cm = CrewMember.query.all()
        sug = fpapp._crew_suggest_for(doc, all_cm, set(), ocr_text="")
        assert sug["crew_member_id"] == S["dm"] and sug["via"] == "Two Foot Shuffle LLC"
        S["tfs_doc2"] = doc.id
        # a synced charge whose EntityRef is the loan-out's vendor
        t = Transaction(project_id=None, vendor="Two Foot Shuffle LLC", amount=Decimal("120"), is_expense=True,
                        txn_date="2026-10-12", source="qbo_sync", qbo_txn_id="LO-SYNC-1",
                        qbo_txn_type="BillPayment", qbo_entity_ref="LO-302")
        assert qbo_sync._attribute_from_entity_ref(t, "LO-302", m) and t.payee_id == S["dm"]
    r = ctx["client"].get(f"/docs/upload/{S['tfs_doc2']}/crew-suggest")
    assert r.get_json()["suggestion"]["name"] == "Danielle Miller"
    r = ctx["client"].get(f"/projects/{S['pid']}/docs/inbox.json")
    if r.status_code == 200:
        rows = {x["id"]: x for x in r.get_json().get("rows", [])}
        if S["tfs_doc2"] in rows:
            assert rows[S["tfs_doc2"]]["payee_suggestion"]["via"] == "Two Foot Shuffle LLC"


@needs_app
def test_14_workspace_rows_show_the_person_via_the_company(ctx):
    r = ctx["client"].get(f"/projects/{S['pid']}/actuals/workspace.json")
    assert r.status_code == 200
    row = next(x for x in r.get_json()["transactions"] if x["id"] == S["danielle_txn"])
    assert row["payee_name"] == "Danielle Miller" and row["payee_via"] == "Two Foot Shuffle LLC"


@needs_app
def test_20_compare_payload_keep_this_one_and_undo(ctx):
    c = ctx["client"]
    with ctx["app"].app_context():
        pid = S["pid"]
        rec = _doc(pid, "Gaffer Supply", 333.33, date(2026, 10, 27), "gaffer-333.pdf", category="receipt")
        a = _txn(pid, "Gaffer Supply", 333.33, "2026-10-27", source="manual_entry", payment_method="company_card",
                 activated_at=NOW, card_last4="4417", note="first entry")
        b = _txn(pid, "GAFFER SUPPLY", 333.33, "2026-10-27", source="manual_entry", payment_method="company_card",
                 activated_at=NOW, card_last4="4417", budget_line_id=S["line"], doc_upload_id=rec.id,
                 match_status="confirmed", note="second entry")
        db.session.add(ExpenseEvidence(transaction_id=b.id, doc_upload_id=rec.id, kind="primary"))
        db.session.commit()
        S.update(cmp_a=a.id, cmp_b=b.id)
    r = c.get(f"/projects/{S['pid']}/actuals/duplicates/compare.json?tid={S['cmp_a']}")
    assert r.status_code == 200, r.get_json()
    d = r.get_json()
    assert d["keeper_id"] == S["cmp_b"] and d["keeper_reason"] == "has the receipt"
    assert [col["id"] for col in d["columns"]] == [S["cmp_b"], S["cmp_a"]]
    assert {"coding", "backup", "match", "memo"} <= set(d["diff"]) and "amount" not in d["diff"]
    assert d["columns"][0]["backup"]["doc_id"] and d["columns"][0]["coded"]
    assert d["key"] == f"dup:{min(S['cmp_a'], S['cmp_b'])}-{max(S['cmp_a'], S['cmp_b'])}"
    S["cmp_key"] = d["key"]
    # the workspace chip names the same group
    ws = c.get(f"/projects/{S['pid']}/actuals/workspace.json").get_json()
    assert {t["dup_group"] for t in ws["transactions"] if t["id"] in (S["cmp_a"], S["cmp_b"])} == {d["key"]}
    n_dup = ws["counts"]["duplicates"]
    # a stale or wrong keeper is refused
    r = c.post(f"/projects/{S['pid']}/actuals/duplicates/settle", json={"key": "dup:1-2", "keeper_id": S["cmp_a"]})
    assert r.status_code == 409
    # Keep this one = the OTHER column (the extra holds the receipt and line → they move)
    r = c.post(f"/projects/{S['pid']}/actuals/duplicates/settle", json={"key": d["key"], "keeper_id": S["cmp_a"]})
    assert r.status_code == 200, r.get_json()
    j = r.get_json()
    assert j["deleted"] == [S["cmp_b"]] and j["modes"][str(S["cmp_b"])] == "merge" and len(j["activity_ids"]) == 2
    with ctx["app"].app_context():
        assert Transaction.query.get(S["cmp_b"]) is None
        k = Transaction.query.get(S["cmp_a"])
        assert k.budget_line_id == S["line"] and k.doc_upload_id is not None and "second entry" in k.note
    ws = c.get(f"/projects/{S['pid']}/actuals/workspace.json").get_json()
    assert ws["counts"]["duplicates"] == n_dup - 1
    assert not any(t.get("dup_group") == S["cmp_key"] for t in ws["transactions"])
    assert c.get(f"/projects/{S['pid']}/actuals/duplicates/compare.json?key={S['cmp_key']}").status_code == 404
    # Undo (the toast's undo-batch) brings the extra back with its coding
    r = c.post(f"/projects/{S['pid']}/budget/{S['bid']}/activity/undo-batch", json={"ids": j["activity_ids"][::-1]})
    assert r.status_code == 200 and r.get_json()["undone"] == 2, r.get_json()
    with ctx["app"].app_context():
        back = Transaction.query.get(S["cmp_b"])
        assert back is not None and back.budget_line_id == S["line"] and back.doc_upload_id is not None
        assert Transaction.query.get(S["cmp_a"]).budget_line_id is None


@needs_app
def test_21_keep_both_is_remembered_in_every_project(ctx):
    c = ctx["client"]
    r = c.get(f"/projects/{S['pid']}/actuals/duplicates/compare.json?tid={S['cmp_a']}")
    assert r.status_code == 200
    key = r.get_json()["key"]
    r = c.post(f"/projects/{S['pid']}/actuals/dup-transactions/dismiss", json={"dup_key": key})
    assert r.status_code == 200
    assert c.get(f"/projects/{S['pid']}/actuals/duplicates/compare.json?key={key}").status_code == 404
    ws = c.get(f"/projects/{S['pid']}/actuals/workspace.json").get_json()
    assert not any(t.get("dup_group") == key for t in ws["transactions"])
    with ctx["app"].app_context():
        assert TransactionDupDismissal.query.filter_by(dup_key=key).count() == 1
        # a pool pair settled in one project is settled for the other too
        x = _txn(None, "Pool Twin Co", 61.10, "2026-10-28", source="qbo_sync", qbo_txn_id="PT-1",
                 qbo_txn_type="Purchase", card_last4="1111")
        y = _txn(None, "Pool Twin Co", 61.10, "2026-10-28", source="qbo_sync", qbo_txn_id="PT-2",
                 qbo_txn_type="Purchase", card_last4="1111")
        db.session.commit()
        pool_key = f"dup:{x.id}-{y.id}"
    ws2 = c.get(f"/projects/{S['pid2']}/actuals/workspace.json").get_json()
    assert any(t.get("dup_group") == pool_key for t in ws2["transactions"])
    c.post(f"/projects/{S['pid']}/actuals/dup-transactions/dismiss", json={"dup_key": pool_key})
    ws2 = c.get(f"/projects/{S['pid2']}/actuals/workspace.json").get_json()
    assert not any(t.get("dup_group") == pool_key for t in ws2["transactions"])


@needs_app
def test_22_removing_a_loan_out_reverts_the_quickbooks_attribution(ctx):
    c = ctx["client"]
    with ctx["app"].app_context():
        t = _txn(None, "Two Foot Shuffle LLC", 77, "2026-10-30", source="qbo_sync", qbo_txn_id="LO-SYNC-2",
                 qbo_txn_type="BillPayment", qbo_entity_ref="LO-302", payee_id=S["dm"], payee_source="qbo")
        db.session.commit()
        lo = PayeeLoanout.query.filter_by(payee_id=S["dm"]).one()
        tid, lid = t.id, lo.id
    r = c.post(f"/payees/{S['dm']}/loanouts/{lid}/remove", json={})
    assert r.status_code == 200 and r.get_json()["charges_reattributed"] >= 1
    with ctx["app"].app_context():
        assert Transaction.query.get(tid).payee_id == S["tfs_rec"], "back to the vendor's own payee"
        assert not same_payee("Danielle Miller", "Two Foot Shuffle LLC")
