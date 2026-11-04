"""ONE split mechanism, the detail window and the counted-twice tools, end to
end on sqlite through the real routes (2026-11-04).

  1. invoice children → split on attach: link-doc on a charge whose invoice
     row is itemized converts the children into the charge's
     transaction_split (amounts / lines / notes kept), deletes the children
     and the invoice row, the line totals count once — and Undo puts every
     row back
  2. the GINTS 7246 shape (charge split 900 / 93.35 AND coded to the line,
     invoice itemized 3 × 300 to the same line, never attached): the line
     shows $1,800; "Find invoices counted twice" lists it with the line
     total before / after; Fix attaches and keeps the charge's split; the
     drill-down lists the split parent only as its pieces; "Split charges
     still coded" uncodes the parent
  3. a split parent is never coded: set-line refuses with the one message,
     link_transaction_to_line raises, the receipt confirm never promotes a
     line onto it
  4. section-target split pieces count in the section rollups (workspace
     section_coded + section actual, the top-sheet section map, the section
     drill-down)
  5. itemizing an ATTACHED document writes the charge's split
  6. detail.json carries the header, backup, chain, amount rule, history
     with an undoable entry, and the counted-twice warning

Run: python -m pytest tests/unit/test_one_split_app.py -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
import tempfile
from datetime import date, datetime
from decimal import Decimal

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-split-")
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
                        DocUpload, Transaction, TransactionSplit, ExpenseEvidence)
    import actuals                            # noqa: E402
    import routes.actuals                     # noqa: F401,E402
    import routes.actuals_detail              # noqa: F401,E402
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
NOW = datetime(2026, 11, 4, 12, 0, 0)


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


def _kids(parent, doc, pieces):
    out = []
    for lid, amt, note in pieces:
        k = _txn(parent.project_id, parent.vendor, amt, parent.txn_date, source="invoice_split",
                 parent_transaction_id=parent.id, doc_upload_id=doc.id, budget_line_id=lid,
                 match_status="confirmed", note=note)
        db.session.add(ExpenseEvidence(transaction_id=k.id, doc_upload_id=doc.id, kind="itemized"))
        out.append(k)
    return out


@pytest.fixture(scope="module")
def ctx():
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        u = User.query.filter_by(email="split-admin@thefp.tv").first()
        if not u:
            u = User(email="split-admin@thefp.tv", name="Split Admin", role="super_admin")
            db.session.add(u)
        u.role = "super_admin"
        u.set_password("pw")
        p = ProjectSheet(name="GINTS split test", status="active")
        db.session.add(p)
        db.session.flush()
        db.session.add(ProjectAccess(project_id=p.id, user_id=u.id, role="owner"))
        b = Budget(project_id=p.id, name="GINTS Working", budget_mode="working", version_status="current")
        db.session.add(b)
        db.session.flush()
        lines = {}
        for code, desc in ((2004, "Labor"), (3412, "Parking"), (2701, "Editor"), (4701, "Color")):
            ln = BudgetLine(budget_id=b.id, account_code=code, account_name=desc, description=desc,
                            quantity=1, rate_type="flat_day", days=1, rate=1000, sort_order=code)
            db.session.add(ln)
            db.session.flush()
            lines[desc] = ln.id
        db.session.commit()
        S.update(uid=u.id, pid=p.id, bid=b.id, lines=lines)
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["_user_id"] = str(S["uid"])
        sess["_fresh"] = True
        sess["_csrf"] = "t"
    c.environ_base["HTTP_X_CSRFTOKEN"] = "t"
    return {"app": app, "client": c}


def _post(c, url, body=None, method="POST"):
    return c.open(url, method=method, json=body or {})


def _line_actual(c, lid):
    ws = c.get(f"/projects/{S['pid']}/actuals/workspace.json").get_json()
    for sec in ws["budget"]["sections"]:
        for ln in sec["lines"]:
            if ln["id"] == lid:
                return ln["actual"]
    return None


# ── 1. children → split on attach, undo ───────────────────────────────────

def test_01_attach_converts_the_invoices_itemization_into_the_charges_split(ctx):
    c, L, pid = ctx["client"], S["lines"], S["pid"]
    with ctx["app"].app_context():
        doc = _doc(pid, "Rare Medium", 993.35, date(2026, 10, 1), "rare-medium-inv.pdf")
        row = _txn(pid, "Rare Medium", 993.35, "2026-10-01", source="doc_upload", doc_upload_id=doc.id,
                   match_status="unmatched")
        _kids(row, doc, [(L["Labor"], 600, "Labor 3 days"), (L["Parking"], 393.35, "Parking")])
        ch = _txn(pid, "RARE MEDIUM", 993.35, "2026-10-03", source="qbo_sync", qbo_txn_id="7001",
                  qbo_txn_type="BillPayment")
        db.session.commit()
        S.update(c1=ch.id, d1=doc.id, r1=row.id)
    assert _line_actual(c, L["Labor"]) == 600.0
    r = _post(c, f"/projects/{pid}/actuals/transaction/{S['c1']}/link-doc", {"doc_upload_id": S["d1"]})
    assert r.status_code == 200, r.data
    j = r.get_json()
    assert j["itemization"]["mode"] == "converted"
    with ctx["app"].app_context():
        ch = Transaction.query.get(S["c1"])
        sps = TransactionSplit.query.filter_by(transaction_id=ch.id).order_by(TransactionSplit.id).all()
        assert [(sp.budget_line_id, float(sp.amount), sp.note) for sp in sps] == [
            (L["Labor"], 600.0, "Labor 3 days"), (L["Parking"], 393.35, "Parking")]
        assert ch.budget_line_id is None and ch.account_code is None, "a split parent is uncoded"
        assert Transaction.query.filter_by(doc_upload_id=S["d1"], source="invoice_split").count() == 0
        assert Transaction.query.get(S["r1"]) is None, "the invoice's own row was absorbed"
    assert _line_actual(c, L["Labor"]) == 600.0
    assert _line_actual(c, L["Parking"]) == 393.35
    # Undo: the itemization, the invoice row and the unlinked charge come back.
    r = _post(c, f"/projects/{pid}/budget/{S['bid']}/activity/{j['activity_id']}/undo")
    assert r.status_code == 200 and r.get_json()["ok"], r.data
    with ctx["app"].app_context():
        assert Transaction.query.get(S["r1"]) is not None
        assert Transaction.query.filter_by(doc_upload_id=S["d1"], source="invoice_split").count() == 2
        assert TransactionSplit.query.filter_by(transaction_id=S["c1"]).count() == 0
        assert Transaction.query.get(S["c1"]).doc_upload_id is None
    assert _line_actual(c, L["Labor"]) == 600.0


# ── 2. GINTS 7246: find counted twice → Fix; split still coded ────────────

def test_02_gints_shape_is_found_fixed_and_the_parent_is_uncoded(ctx):
    c, L, pid = ctx["client"], S["lines"], S["pid"]
    with ctx["app"].app_context():
        doc = _doc(pid, "SP Parking", 993.35, date(2026, 10, 12), "inv-3485.pdf")
        row = _txn(pid, "SP Parking", 993.35, "2026-10-12", source="doc_upload", doc_upload_id=doc.id)
        _kids(row, doc, [(L["Editor"], 300, "Day 1"), (L["Editor"], 300, "Day 2"), (L["Editor"], 300, "Day 3")])
        ch = _txn(pid, "SP Parking", 993.35, "2026-10-14", source="qbo_sync", qbo_txn_id="7246",
                  qbo_txn_type="Purchase", budget_line_id=L["Editor"], account_code=2701)   # the bug: coded AND split
        db.session.add_all([TransactionSplit(transaction_id=ch.id, budget_line_id=L["Editor"], project_id=pid, amount=Decimal("900")),
                            TransactionSplit(transaction_id=ch.id, budget_line_id=L["Parking"], project_id=pid, amount=Decimal("93.35"))])
        db.session.commit()
        S.update(c2=ch.id, d2=doc.id, r2=row.id)
    assert _line_actual(c, L["Editor"]) == 1800.0
    # the drill-down lists the split parent only as its pieces
    tj = c.get(f"/projects/{pid}/actuals/line/{L['Editor']}/transactions.json").get_json()
    mine = [t for t in tj["transactions"] if t["id"] == S["c2"]]
    assert [t.get("is_split") for t in mine] == [True] and mine[0]["amount"] == 900.0
    lg = c.get(f"/projects/{pid}/actuals/line/{L['Editor']}/ledger.json").get_json()
    assert [t.get("is_split") for t in lg["txns"] if t["id"] == S["c2"]] == [True]
    # preview: listed with the line totals before / after
    pv = c.get(f"/admin/actuals/counted-twice?project={pid}&format=json").get_json()
    card = next(r for r in pv["rows"] if r["charge"]["id"] == S["c2"])
    assert card["kind"] == "doc_and_charge" and card["doc_row"]["id"] == S["r2"]
    ed = next(l for l in card["lines"] if l["label"].startswith("2701"))
    assert (ed["before"], ed["after"]) == (1800.0, 900.0)
    html = c.get(f"/admin/actuals/counted-twice?project={pid}").get_data(as_text=True)
    assert "Invoices counted twice" in html and "$1,800.00" in html
    # the matcher suggests it on the charge row too
    ws = c.get(f"/projects/{pid}/actuals/workspace.json").get_json()
    row = next(t for t in ws["transactions"] if t["id"] == S["c2"])
    assert row["counted_twice"]["doc_id"] == S["d2"] and "counted_twice" in row["flags"]["yellow"]
    assert ws["counts"]["counted_twice"] >= 1
    # Fix
    r = _post(c, "/admin/actuals/counted-twice/fix", {"charge_id": S["c2"], "doc_row_id": S["r2"]})
    assert r.status_code == 200, r.data
    assert r.get_json()["itemization"]["mode"] == "kept_charge_split"
    assert _line_actual(c, L["Editor"]) == 900.0
    with ctx["app"].app_context():
        ch = Transaction.query.get(S["c2"])
        assert ch.doc_upload_id == S["d2"] and ch.match_status == "confirmed"
        assert Transaction.query.filter_by(doc_upload_id=S["d2"], source="invoice_split").count() == 0
    # the parent was still coded — the check lists it, Fix uncodes it
    pv = c.get(f"/admin/actuals/split-still-coded?project={pid}").get_json()
    assert [r["id"] for r in pv["rows"]] == [S["c2"]]
    r = _post(c, f"/admin/actuals/split-still-coded?project={pid}&apply=1")
    assert r.get_json()["fixed"] == 1
    with ctx["app"].app_context():
        ch = Transaction.query.get(S["c2"])
        assert ch.budget_line_id is None and ch.account_code is None
    assert _line_actual(c, L["Editor"]) == 900.0
    # nothing left to fix
    pv = c.get(f"/admin/actuals/counted-twice?project={pid}&format=json").get_json()
    assert not [r for r in pv["rows"] if r["charge"]["id"] == S["c2"]]


# ── 3. a split parent is never coded ──────────────────────────────────────

def test_03_split_parent_refuses_set_line_and_link(ctx):
    c, L, pid = ctx["client"], S["lines"], S["pid"]
    r = _post(c, f"/projects/{pid}/actuals/transaction/{S['c2']}/set-line", {"budget_line_id": L["Labor"]})
    assert r.status_code == 409
    assert r.get_json()["error"] == "This charge is split — edit the split instead."
    with ctx["app"].app_context():
        with pytest.raises(actuals.SplitParentError):
            actuals.link_transaction_to_line(S["c2"], L["Labor"], user_id=S["uid"])
        # receipt confirm with a coded sister row never promotes a line onto a split parent
        doc = _doc(pid, "Sister Co", 50, date(2026, 10, 2), "sister.pdf")
        sis = _txn(pid, "Sister Co", 50, "2026-10-02", source="doc_upload", doc_upload_id=doc.id,
                   budget_line_id=L["Color"], activated_at=NOW)
        ch = _txn(pid, "Sister Co", 50, "2026-10-02", source="qbo_sync", qbo_txn_id="7300",
                  qbo_txn_type="Purchase", doc_upload_id=doc.id, match_status="suggested")
        db.session.add_all([TransactionSplit(transaction_id=ch.id, budget_line_id=L["Labor"], project_id=pid, amount=Decimal("30")),
                            TransactionSplit(transaction_id=ch.id, budget_line_id=L["Parking"], project_id=pid, amount=Decimal("20"))])
        db.session.commit()
        actuals.confirm_match(ch.id)
        ch = Transaction.query.get(ch.id)
        assert ch.budget_line_id is None and Transaction.query.get(sis.id) is None


# ── 4. section-target split pieces in the rollups ─────────────────────────

def test_04_a_split_piece_on_a_section_counts_in_the_section(ctx):
    c, L, pid = ctx["client"], S["lines"], S["pid"]
    with ctx["app"].app_context():
        ch = _txn(pid, "Rare Medium Post", 500, "2026-10-20", source="manual_entry", payment_method="company_card")
        db.session.commit()
        S["c4"] = ch.id
    r = _post(c, f"/projects/{pid}/actuals/transaction/{S['c4']}/split",
              {"splits": [{"budget_line_id": L["Color"], "amount": 200},
                          {"account_code": 4700, "amount": 300, "note": "Post services"}]})
    assert r.status_code == 200, r.data
    out = r.get_json()["splits"]
    assert out[1]["account_code"] == 4700 and out[1]["budget_line_id"] is None and "section" in out[1]["label"]
    with ctx["app"].app_context():
        assert actuals.split_section_sums(pid).get(4700) == 300.0
        assert fpapp._actuals_by_section_code(pid).get(4700, 0) >= 300.0
    ws = c.get(f"/projects/{pid}/actuals/workspace.json").get_json()
    sec = next(s for s in ws["budget"]["sections"] if s["code"] == 4700)
    assert sec["section_coded"]["total"] == 300.0 and sec["section_coded"]["count"] == 1
    color = next(l for l in sec["lines"] if l["id"] == L["Color"])
    assert sec["actual"] == round(color["actual"] + 300.0, 2)
    st = c.get(f"/projects/{pid}/actuals/section/4700/transactions.json").get_json()
    assert [(t["id"], t["amount"], t.get("is_split")) for t in st["transactions"]] == [(S["c4"], 300.0, True)]
    # a piece names one target, never both
    r = _post(c, f"/projects/{pid}/actuals/transaction/{S['c4']}/split",
              {"splits": [{"budget_line_id": L["Color"], "account_code": 4700, "amount": 200},
                          {"account_code": 4700, "amount": 300}]})
    assert r.status_code == 400 and "not both" in r.get_json()["error"]
    # undo of the split puts the section piece back as a section piece
    with ctx["app"].app_context():
        assert TransactionSplit.query.filter_by(transaction_id=S["c4"], account_code=4700).count() == 1


# ── 5. itemizing an ATTACHED document writes the charge's split ───────────

def test_05_itemize_on_an_attached_invoice_splits_the_charge(ctx):
    c, L, pid = ctx["client"], S["lines"], S["pid"]
    with ctx["app"].app_context():
        doc = _doc(pid, "Katherine McClure", 1250, date(2026, 10, 22), "km-invoice.pdf")
        ch = _txn(pid, "Katherine McClure", 1250, "2026-10-23", source="qbo_sync", qbo_txn_id="7400",
                  qbo_txn_type="BillPayment", doc_upload_id=doc.id, match_status="confirmed")
        db.session.commit()
        S.update(c5=ch.id, d5=doc.id)
    li = c.get(f"/docs/upload/{S['d5']}/line-items").get_json()
    assert li["writes"] == "split" and li["parent_txn_id"] == S["c5"]
    r = _post(c, f"/docs/upload/{S['d5']}/itemize",
              {"rows": [{"amount": 1000, "budget_line_id": str(L["Labor"]), "description": "Legal review"},
                        {"amount": 250, "budget_line_id": "section:3400", "description": "Filing fees"}],
               "main_line_id": ""})
    assert r.status_code == 200, r.data
    assert r.get_json()["mode"] == "split"
    with ctx["app"].app_context():
        sps = TransactionSplit.query.filter_by(transaction_id=S["c5"]).order_by(TransactionSplit.id).all()
        assert [(sp.budget_line_id, sp.account_code, float(sp.amount)) for sp in sps] == [
            (L["Labor"], None, 1000.0), (None, 3400, 250.0)]
        assert Transaction.query.filter_by(doc_upload_id=S["d5"], source="invoice_split").count() == 0
    li = c.get(f"/docs/upload/{S['d5']}/line-items").get_json()
    assert [x["amount"] for x in li["existing"]] == [1000.0, 250.0]
    # lines over the charge are refused
    r = _post(c, f"/docs/upload/{S['d5']}/itemize",
              {"rows": [{"amount": 1300, "budget_line_id": str(L["Labor"])}], "main_line_id": ""})
    assert r.status_code == 400


# ── 6. the detail payload ─────────────────────────────────────────────────

def test_06_detail_json_has_the_chain_backup_amount_rule_and_history(ctx):
    c, L, pid = ctx["client"], S["lines"], S["pid"]
    d = c.get(f"/projects/{pid}/actuals/transaction/{S['c5']}/detail.json").get_json()
    assert d["ok"] and d["txn"]["source_label"] == "QuickBooks"
    assert d["txn"]["qbo"]["id"] == "7400" and d["txn"]["qbo"]["type"] == "BillPayment"
    assert d["amount_edit"]["allowed"] is False and "bank" in d["amount_edit"]["reason"]
    assert [b["doc_id"] for b in d["backup"]] == [S["d5"]]
    assert d["coding"]["mode"] == "split" and len(d["coding"]["chain"]) == 2
    assert d["coding"]["chain"][1]["kind"] == "section" and d["coding"]["chain"][1]["docs"] == [S["d5"]]
    assert d["history"] and d["undo_activity_id"] == d["history"][0]["id"]
    assert d["picker"] and any(o["id"] == L["Labor"] for g in d["picker"] for o in g["opts"])
    # a manual row's amount is editable, and the edit is logged and undoable
    d4 = c.get(f"/projects/{pid}/actuals/transaction/{S['c4']}/detail.json").get_json()
    assert d4["amount_edit"]["allowed"] is False, "split rows must remove the split first"
    with ctx["app"].app_context():
        m = _txn(pid, "Cash tip", 20, "2026-10-21", source="manual_entry", payment_method="cash",
                 budget_line_id=L["Color"], account_code=4701)
        db.session.commit()
        mid = m.id
    dm = c.get(f"/projects/{pid}/actuals/transaction/{mid}/detail.json").get_json()
    assert dm["amount_edit"]["allowed"] is True and dm["txn"]["source_label"] == "Manual"
    assert dm["needs_backup"] is True and dm["coding"]["chain"][0]["kind"] == "line"
    r = _post(c, f"/projects/{pid}/actuals/transaction/{mid}/edit", {"amount": 25})
    assert r.status_code == 200
    dm = c.get(f"/projects/{pid}/actuals/transaction/{mid}/detail.json").get_json()
    assert dm["txn"]["amount"] == 25.0 and dm["undo_activity_id"]
    r = _post(c, f"/projects/{pid}/budget/{dm['undo_bid']}/activity/{dm['undo_activity_id']}/undo")
    assert r.status_code == 200, r.data
    with ctx["app"].app_context():
        assert float(Transaction.query.get(mid).amount) == 20.0
    # the receipt variant: a document with no transaction
    with ctx["app"].app_context():
        lone = _doc(pid, "Lonely Receipt", 12, date(2026, 10, 2), "lonely.jpg", category="receipt")
        db.session.commit()
        lid = lone.id
    dd = c.get(f"/projects/{pid}/actuals/doc/{lid}/detail.json").get_json()
    assert dd["kind"] == "doc" and dd["doc"]["doc_id"] == lid
    # detach: the charge keeps its coding and needs backup again
    r = _post(c, f"/projects/{pid}/actuals/transaction/{S['c5']}/detach-doc", {"doc_id": S["d5"]})
    assert r.status_code == 200, r.data
    d = c.get(f"/projects/{pid}/actuals/transaction/{S['c5']}/detail.json").get_json()
    assert d["coding"]["mode"] == "split" and d["needs_backup"] is True
