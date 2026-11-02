"""Documents Phase 3 end to end on sqlite with the Flask test client
(DOCUMENTS_HANDOFF Phase 3, 2026-11-02).

Boots the real app against a throw-away sqlite file (or reuses the one
another app-booting module in this process already opened) and walks:

  1. the payee backfill body: doc owner / single line owner / linked alias /
     exact vendor name each attribute; disagreeing signals log a conflict
     to the ActivityLog and set nothing; a second run is a no-op
  2. link_transaction_to_line stamps 'line', a re-code moves it, a shared
     line clears it, a 'manual' value survives everything
  3. paid_by_payee prefers payee_id over the doc / line inference
  4. sync_project with a stubbed QuickBooks pull keeps EntityRef and
     attributes 'qbo' through CrewMember.qbo_vendor_id
  5. sync_vendors with a stubbed pull: refreshes a linked payee, links an
     exact DisplayName, leaves an ambiguous one, writes the mirror and the
     watermark, and the next run is CDC
  6. POST /payees/<id>/qbo-create: 409 for W-2 and already linked; an
     exact DisplayName match links with NO POST; otherwise exactly one
     POST to /vendor whose body has no Id / SyncToken, never an update
  7. the Link screen: GET renders the unlinked vendor, link and
     create-payee attribute the vendor's charges
  8. a manual expense with payee_id lands as 'manual'; one whose vendor
     string is a linked alias lands as 'alias'

Skipped automatically where app.py's runtime deps are not installed.
Run: python -m pytest tests/unit/test_txn_payee_app.py -q --confcutdir=tests/unit -o addopts=""
"""
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-docs3-")
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
                        CrewMember, CrewAssignment, DocUpload, Transaction,
                        VendorAlias, ActivityLog, QBOConnection, QBOVendor)
    import actuals                            # noqa: E402
    import qbo_sync                           # noqa: E402
    import routes.payees                      # noqa: F401,E402
    import routes.qbo_admin                   # noqa: F401,E402
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
def mp():
    from _pytest.monkeypatch import MonkeyPatch
    m = MonkeyPatch()
    yield m
    m.undo()


@pytest.fixture(scope="module")
def ctx(mp):
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        u = User.query.filter_by(email="steven@thefp.tv").first()
        if not u:
            u = User(email="steven@thefp.tv", name="Steven", role="super_admin")
            db.session.add(u)
        u.role = "super_admin"
        u.set_password("pw")
        p = ProjectSheet(name="Phase 3 attribution", status="active", qbo_account_ids='["41"]')
        db.session.add(p)
        db.session.flush()
        db.session.add(ProjectAccess(project_id=p.id, user_id=u.id, role="owner"))
        b = Budget(project_id=p.id, name="P3 Working", budget_mode="working", version_status="current")
        db.session.add(b)
        db.session.flush()
        theo = CrewMember(name="Ines Varga", kind="person", active=True)
        mara = CrewMember(name="Ruth Okafor", kind="person", active=True)
        vega = CrewMember(name="Harborlight Camera LLC", kind="vendor", is_vendor=True, active=True)
        w2 = CrewMember(name="Pat Payroll", kind="person", is_w2_employee=True, active=True)
        db.session.add_all([theo, mara, vega, w2])
        db.session.flush()
        L = lambda **kw: BudgetLine(budget_id=b.id, quantity=1, rate_type="flat_day", days=1, rate=100, **kw)
        l_theo = L(account_code=2001, account_name="Production Staff", description="Director", is_labor=True, sort_order=1)
        l_shared = L(account_code=2002, account_name="Production Staff", description="PAs", is_labor=True, sort_order=2)
        l_none = L(account_code=2601, account_name="Camera", description="Lens", is_labor=False, sort_order=3)
        db.session.add_all([l_theo, l_shared, l_none])
        db.session.flush()
        db.session.add_all([
            CrewAssignment(budget_line_id=l_theo.id, instance=1, crew_member_id=theo.id),
            CrewAssignment(budget_line_id=l_shared.id, instance=1, crew_member_id=theo.id),
            CrewAssignment(budget_line_id=l_shared.id, instance=2, crew_member_id=mara.id),
        ])
        db.session.commit()
        S.update(uid=u.id, pid=p.id, bid=b.id, theo=theo.id, mara=mara.id, vega=vega.id, w2=w2.id,
                 l_theo=l_theo.id, l_shared=l_shared.id, l_none=l_none.id)
    c = app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(S["uid"])
        s["_fresh"] = True
        s["_csrf"] = "t"
    c.environ_base["HTTP_X_CSRFTOKEN"] = "t"
    return {"app": app, "client": c}


def _post(c, url, payload=None):
    return c.post(url, data=json.dumps(payload or {}), content_type="application/json",
                  headers={"X-CSRFToken": "t"})


def _mk_txn(**kw):
    base = dict(project_id=S["pid"], source='csv_import', amount=100, is_expense=True,
                txn_date="2026-03-01", match_status='unmatched')
    base.update(kw)
    t = Transaction(**base)
    db.session.add(t)
    db.session.flush()
    return t


# ── 1. backfill ────────────────────────────────────────────────────────────

def test_01_backfill_attributes_unambiguous_rows_and_logs_conflicts(ctx):
    with ctx["app"].app_context():
        doc_theo = DocUpload(project_id=S["pid"], category='invoice', status='done',
                             original_filename="inv.pdf", amount=100)
        doc_theo.set_owner(S["theo"])
        db.session.add(doc_theo)
        db.session.flush()
        db.session.add(VendorAlias(project_id=None, raw_canonical=actuals.canon_vendor("LYFT *RIDE 8AM"),
                                   clean_name="Lyft", payee_id=S["mara"]))
        db.session.add(VendorAlias(project_id=None, raw_canonical="harborlight camera llc",
                                   clean_name="Harborlight Camera LLC"))     # unlinked; name → vega
        t_doc = _mk_txn(doc_upload_id=doc_theo.id, vendor="Some bank text")
        t_line = _mk_txn(budget_line_id=S["l_theo"], vendor="Bank text 2")
        t_alias = _mk_txn(vendor="LYFT *RIDE 8AM")
        t_name = _mk_txn(vendor="Harborlight Camera LLC")
        t_shared = _mk_txn(budget_line_id=S["l_shared"], vendor="x")
        t_conflict = _mk_txn(doc_upload_id=doc_theo.id, budget_line_id=S["l_shared"], vendor="Harborlight Camera LLC")
        t_nothing = _mk_txn(vendor="Unknown Vendor 99")
        t_manual = _mk_txn(vendor="Harborlight Camera LLC", payee_id=S["mara"], payee_source='manual')
        db.session.commit()
        ids = dict(doc=t_doc.id, line=t_line.id, alias=t_alias.id, name=t_name.id, shared=t_shared.id,
                   conflict=t_conflict.id, nothing=t_nothing.id, manual=t_manual.id)
        S["doc_theo"] = doc_theo.id
        S.update({f"t_{k}": v for k, v in ids.items()})
        summary = fpapp._backfill_txn_payee_body()
        print("\n[backfill]", summary)
        got = {k: (Transaction.query.get(v).payee_id, Transaction.query.get(v).payee_source)
               for k, v in ids.items()}
        assert got["doc"] == (S["theo"], 'doc')
        assert got["line"] == (S["theo"], 'line')
        assert got["alias"] == (S["mara"], 'alias')
        assert got["name"] == (S["vega"], 'alias')
        assert got["shared"] == (None, None)
        assert got["conflict"] == (None, None), "doc says Theo, vendor says Vega → logged, not guessed"
        assert got["nothing"] == (None, None)
        assert got["manual"] == (S["mara"], 'manual')
        # the alias whose clean name is exactly one payee's name got linked
        assert VendorAlias.query.filter_by(raw_canonical="harborlight camera llc").first().payee_id == S["vega"]
        rows = ActivityLog.query.filter_by(action='attribution_conflict', entity_id=ids["conflict"]).all()
        assert len(rows) == 1 and 'candidates' in (rows[0].after_json or '')
        # idempotent
        summary2 = fpapp._backfill_txn_payee_body()
        assert "attributed 0" in summary2
        assert ActivityLog.query.filter_by(action='attribution_conflict').count() == 1


# ── 2. link_transaction_to_line ───────────────────────────────────────────

def test_02_link_to_line_sets_moves_and_clears_line_attribution(ctx):
    with ctx["app"].app_context():
        t = _mk_txn(vendor="Bank text 3")
        db.session.commit()
        tid = t.id
        actuals.link_transaction_to_line(tid, S["l_theo"], user_id=S["uid"])
        t = Transaction.query.get(tid)
        assert (t.payee_id, t.payee_source) == (S["theo"], 'line')
        actuals.link_transaction_to_line(tid, S["l_shared"], user_id=S["uid"])
        t = Transaction.query.get(tid)
        assert t.payee_id is None, "a shared line names nobody; the stale line value goes"
        actuals.link_transaction_to_line(tid, S["l_theo"], user_id=S["uid"])
        actuals.unlink_transaction(tid)
        assert Transaction.query.get(tid).payee_id is None
        # a manual pick survives coding and un-coding
        t = Transaction.query.get(tid)
        t.payee_id, t.payee_source = S["mara"], 'manual'
        db.session.commit()
        actuals.link_transaction_to_line(tid, S["l_theo"], user_id=S["uid"])
        t = Transaction.query.get(tid)
        assert (t.payee_id, t.payee_source) == (S["mara"], 'manual')
        actuals.unlink_transaction(tid)
        assert Transaction.query.get(tid).payee_id == S["mara"]
        # a 'doc' value is not replaced by the line either
        t = Transaction.query.get(tid)
        t.payee_id, t.payee_source = S["vega"], 'doc'
        db.session.commit()
        actuals.link_transaction_to_line(tid, S["l_theo"], user_id=S["uid"])
        assert Transaction.query.get(tid).payee_id == S["vega"]


# ── 3. paid_by_payee prefers payee_id ──────────────────────────────────────

def test_03_paid_by_payee_prefers_explicit_attribution(ctx):
    with ctx["app"].app_context():
        def _tot(pid_):
            return (actuals.paid_by_payee(payee_id=pid_, year=2026).get(pid_, {})
                    .get(2026, {"total": 0.0}))["total"]
        before = {"ines": _tot(S["theo"]), "harbor": _tot(S["vega"])}
        # doc owned by Ines, but the row explicitly says Harborlight → its money
        t = _mk_txn(doc_upload_id=S["doc_theo"], vendor="z", amount=250, txn_date="2026-05-05",
                    payee_id=S["vega"], payee_source='manual', activated_at=datetime.utcnow(),
                    source='manual_entry', payment_method='check')
        # NULL payee_id, doc owned by Theo → still Theo (the fallback stays)
        t2 = _mk_txn(doc_upload_id=S["doc_theo"], vendor="z", amount=40, txn_date="2026-05-06",
                     activated_at=datetime.utcnow(), source='manual_entry', payment_method='check')
        db.session.commit()
        paid = actuals.paid_by_payee(year=2026)
        assert round(paid[S["vega"]][2026]["total"] - before["harbor"], 2) == 250.0
        assert round(paid[S["theo"]][2026]["total"] - before["ines"], 2) == 40.0, \
            "the explicit row counts for Harborlight only; the NULL row still falls back to the doc owner"
        assert round(_tot(S["vega"]) - before["harbor"], 2) == 250.0     # per-payee narrowing agrees
        assert round(_tot(S["theo"]) - before["ines"], 2) == 40.0
        db.session.delete(Transaction.query.get(t.id)); db.session.delete(Transaction.query.get(t2.id))
        db.session.commit()


# ── 4. sync_project keeps EntityRef and attributes 'qbo' ──────────────────

def _conn():
    conn = QBOConnection.query.first()
    if conn is None:
        conn = QBOConnection(realm_id="123", access_token="a", refresh_token="r",
                             token_expiry=datetime.utcnow() + timedelta(days=1), enabled_account_ids='["41"]')
        db.session.add(conn)
        db.session.commit()
    return conn


def test_04_sync_project_attributes_from_entity_ref(ctx, mp):
    rows = [
        {"qbo_txn_id": "9001", "qbo_txn_type": "Purchase", "qbo_account_id": "41", "qbo_category": None,
         "txn_date": "2026-06-01", "vendor": "Harborlight Camera LLC", "amount": 300.0, "is_expense": True,
         "account_name": "CC: 8432", "note": "", "qbo_entity_ref": "88"},
        {"qbo_txn_id": "9002", "qbo_txn_type": "Purchase", "qbo_account_id": "41", "qbo_category": None,
         "txn_date": "2026-06-02", "vendor": "Unknown Co", "amount": 20.0, "is_expense": True,
         "account_name": "CC: 8432", "note": "", "qbo_entity_ref": "77"},
    ]
    mp.setattr(qbo_sync, "fetch_transactions", lambda *a, **k: list(rows))
    mp.setattr(qbo_sync, "fetch_transactions_cdc", lambda *a, **k: ([], None))
    mp.setattr(qbo_sync, "list_qbo_accounts", lambda *a, **k: [])
    with ctx["app"].app_context():
        CrewMember.query.get(S["vega"]).qbo_vendor_id = "88"
        db.session.commit()
        r = qbo_sync.sync_project(ProjectSheet.query.get(S["pid"]), _conn(), db)
        assert r["imported"] == 2 and r["attributed"] == 1
        a = Transaction.query.filter_by(qbo_txn_id="9001").first()
        b = Transaction.query.filter_by(qbo_txn_id="9002").first()
        assert (a.payee_id, a.payee_source, a.qbo_entity_ref) == (S["vega"], 'qbo', "88")
        assert (b.payee_id, b.qbo_entity_ref) == (None, "77")
        S["t_9002"] = b.id
        # a person's pick survives the next sync; a 'qbo' value follows QuickBooks
        a.payee_id, a.payee_source = S["theo"], 'manual'
        db.session.commit()
        qbo_sync.sync_project(ProjectSheet.query.get(S["pid"]), _conn(), db)
        assert Transaction.query.filter_by(qbo_txn_id="9001").first().payee_id == S["theo"]


# ── 5. sync_vendors ───────────────────────────────────────────────────────

RAW = [
    {"Id": "88", "SyncToken": "4", "DisplayName": "Harborlight Camera LLC", "CompanyName": "Harborlight Camera, LLC",
     "PrimaryEmailAddr": {"Address": "ar@harborlight.com"}, "Vendor1099": True, "Active": True},
    {"Id": "77", "SyncToken": "1", "DisplayName": "Ruth Okafor", "Vendor1099": False, "Active": True,
     "BillAddr": {"Line1": "1 Main", "City": "Medford", "CountrySubDivisionCode": "MA", "PostalCode": "02155"}},
    {"Id": "66", "SyncToken": "1", "DisplayName": "Twin Name", "Active": True},
    {"Id": "55", "SyncToken": "1", "DisplayName": "Nobody Knows Inc", "Active": False},
    {"Id": "44", "SyncToken": "1", "DisplayName": "Pat Payroll", "Active": True},
]


def test_05_sync_vendors_mirrors_links_and_watermarks(ctx):
    calls = []

    def fake_fetch(conn, db_, since):
        calls.append(since)
        return list(RAW), datetime(2026, 11, 2, 12, 0, 0)

    with ctx["app"].app_context():
        db.session.add_all([CrewMember(name="Twin Name", kind="vendor", is_vendor=True, active=True),
                            CrewMember(name="Twin Name", kind="person", active=True)])
        db.session.commit()
        conn = _conn()
        conn.vendor_cdc_since = None
        db.session.commit()
        r = qbo_sync.sync_vendors(conn, db, fetch=fake_fetch)
        print("\n[sync_vendors]", r)
        assert r["mode"] == 'full' and calls[-1] is None
        assert r["upserted"] == 5 and r["refreshed"] == 1 and r["linked"] == 1
        assert r["ambiguous"] == 1 and r["unlinked"] == 3
        vega = CrewMember.query.get(S["vega"])
        assert vega.email == "ar@harborlight.com" and vega.qbo_vendor_1099 is True
        assert vega.qbo_origin == 'qbo' and vega.qbo_sync_token == "4"
        mara = CrewMember.query.get(S["mara"])
        assert mara.qbo_vendor_id == "77" and mara.city == "Medford" and mara.qbo_vendor_1099 is False
        assert mara.name == "Ruth Okafor"
        assert CrewMember.query.filter_by(qbo_vendor_id="66").count() == 0, "ambiguous name waits"
        assert CrewMember.query.filter_by(qbo_vendor_id="44").count() == 0, \
            "a W-2 employee is never auto-linked by name (Wrapbook's, not a vendor)"
        assert QBOVendor.query.count() == 5
        assert QBOVendor.query.get("55").active is False
        assert conn.vendor_cdc_since == datetime(2026, 11, 2, 11, 55, 0)
        # the charge that carried EntityRef 77 now attributes to Mara once linked
        # (attribution of already-synced rows happens on the next project sync
        # or on an explicit link; sync_vendors itself touches no transaction)
        assert Transaction.query.get(S["t_9002"]).payee_id is None
        r2 = qbo_sync.sync_vendors(conn, db, fetch=fake_fetch)
        assert r2["mode"] == 'cdc' and calls[-1] == datetime(2026, 11, 2, 11, 55, 0)
        assert r2["linked"] == 0 and r2["refreshed"] == 2


# ── 6. qbo-create: refuse / link / create ─────────────────────────────────

class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self._p = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._p


class _FakeRequests:
    """Records every call; `query_hits` decides what the DisplayName query
    returns; POST /vendor answers with a created Vendor."""
    def __init__(self, query_hits=None):
        self.gets, self.posts = [], []
        self.query_hits = query_hits or []

    def get(self, url, headers=None, params=None, timeout=None):
        self.gets.append((url, params))
        if url.endswith("/query"):
            return _Resp(200, {"QueryResponse": {"Vendor": list(self.query_hits)}, "time": "2026-11-02T12:00:00Z"})
        return _Resp(404, {})

    def post(self, url, headers=None, json=None, timeout=None):
        self.posts.append((url, json))
        if url.endswith("/vendor"):
            return _Resp(200, {"Vendor": dict(json, Id="501", SyncToken="0")})
        return _Resp(400, {"error": "unexpected"})


def test_06a_qbo_create_refuses_w2_and_linked(ctx, mp):
    c = ctx["client"]
    with ctx["app"].app_context():
        _conn()
    r = _post(c, f"/payees/{S['w2']}/qbo-create")
    assert r.status_code == 409, r.get_json()
    r = _post(c, f"/payees/{S['vega']}/qbo-create")        # linked to 88 in test 04
    assert r.status_code == 409 and "already linked" in r.get_json()["error"]


def test_06b_qbo_create_links_exact_match_without_posting(ctx, mp):
    c = ctx["client"]
    fake = _FakeRequests(query_hits=[{"Id": "301", "SyncToken": "2", "DisplayName": "Ines Varga",
                                      "Vendor1099": True, "Active": True,
                                      "PrimaryEmailAddr": {"Address": "ines@example.com"}}])
    mp.setattr(qbo_sync, "requests", fake)
    mp.setattr(qbo_sync, "get_valid_token", lambda conn, db_: "tok")
    r = _post(c, f"/payees/{S['theo']}/qbo-create")
    assert r.status_code == 200, r.get_json()
    j = r.get_json()
    assert j["action"] == 'linked' and j["vendor_id"] == "301"
    assert fake.posts == [], "an existing record is linked, never written"
    assert any("DisplayName = 'Ines Varga'" in (p or {}).get("query", "") for _u, p in fake.gets)
    with ctx["app"].app_context():
        theo = CrewMember.query.get(S["theo"])
        assert theo.qbo_vendor_id == "301" and theo.qbo_origin == 'qbo'
        assert theo.email == "ines@example.com" and theo.qbo_vendor_1099 is True
        assert QBOVendor.query.get("301").display_name == "Ines Varga"


def test_06c_qbo_create_creates_when_no_match(ctx, mp):
    c = ctx["client"]
    with ctx["app"].app_context():
        cm = CrewMember(name="Harbor Grip & Electric", kind="vendor", is_vendor=True, active=True,
                        legal_name="Harbor Grip and Electric LLC", email="ap@harborgrip.com",
                        phone="617-555-0199", address_line1="9 Pier Rd", city="Boston", state="MA",
                        postal_code="02210", w9_received_on=datetime(2026, 1, 15).date())
        db.session.add(cm)
        db.session.commit()
        S["harbor"] = cm.id
        _mk_txn(vendor="Harbor Grip", qbo_entity_ref="501", source='qbo_sync', qbo_txn_id="9100",
                qbo_txn_type="Purchase")
        db.session.commit()
    fake = _FakeRequests(query_hits=[])
    mp.setattr(qbo_sync, "requests", fake)
    mp.setattr(qbo_sync, "get_valid_token", lambda conn, db_: "tok")
    r = _post(c, f"/payees/{S['harbor']}/qbo-create")
    assert r.status_code == 200, r.get_json()
    j = r.get_json()
    assert j["action"] == 'created' and j["vendor_id"] == "501" and j["charges_attributed"] == 1
    assert len(fake.posts) == 1
    url, body = fake.posts[0]
    assert url.endswith("/123/vendor") and "?" not in url, "a plain create — no operation=update"
    assert body["DisplayName"] == "Harbor Grip and Electric LLC"
    assert body["CompanyName"] == "Harbor Grip and Electric LLC"
    assert body["PrimaryEmailAddr"] == {"Address": "ap@harborgrip.com"}
    assert body["BillAddr"]["Line1"] == "9 Pier Rd" and body["BillAddr"]["PostalCode"] == "02210"
    assert body["Vendor1099"] is True, "W-9 on file → 1099 tracking on"
    for k in ("Id", "SyncToken", "sparse"):
        assert k not in body
    with ctx["app"].app_context():
        cm = CrewMember.query.get(S["harbor"])
        assert cm.qbo_vendor_id == "501" and cm.qbo_origin == 'fpbudget'
        t = Transaction.query.filter_by(qbo_txn_id="9100").first()
        assert (t.payee_id, t.payee_source) == (S["harbor"], 'qbo')
        # the edit form refuses mirrored fields now
        from routes.payees import apply_payee_fields
        changes = apply_payee_fields(cm, {"legal_name": "Renamed", "notes": "ok"})
        assert "legal_name" not in changes and "notes" in changes
    # a second click is a 409
    r = _post(c, f"/payees/{S['harbor']}/qbo-create")
    assert r.status_code == 409


# ── 7. the Link screen ────────────────────────────────────────────────────

def test_07_link_screen_lists_links_and_creates(ctx):
    c = ctx["client"]
    page = c.get("/admin/qbo-vendors")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert "Nobody Knows Inc" in html and "Twin Name" in html
    j = c.get("/admin/qbo-vendors.json").get_json()
    ids = {v["id"]: v for v in j["unlinked"]}
    assert {"66", "55", "44"} <= set(ids) and "88" not in ids and "77" not in ids
    assert len(ids["66"]["suggestions"]) == 2, "both Twin Names offered, a person decides"
    assert ids["44"]["suggestions"] == [], "W-2 Pat is not a candidate"
    # link 66 to the vendor Twin Name
    with ctx["app"].app_context():
        twin_vendor = CrewMember.query.filter_by(name="Twin Name", kind="vendor").first().id
        _mk_txn(vendor="Twin Name", qbo_entity_ref="66", source='qbo_sync', qbo_txn_id="9200",
                qbo_txn_type="Purchase")
        db.session.commit()
    r = _post(c, "/admin/qbo-vendors/66/link", {"payee_id": twin_vendor})
    assert r.status_code == 200, r.get_json()
    assert r.get_json()["charges_attributed"] == 1
    r = _post(c, "/admin/qbo-vendors/66/link", {"payee_id": twin_vendor})
    assert r.status_code == 409
    r = _post(c, "/admin/qbo-vendors/44/link", {"payee_id": S["w2"]})
    assert r.status_code == 409, "a W-2 employee cannot be a QuickBooks vendor"
    # create a payee from 55
    r = _post(c, "/admin/qbo-vendors/55/create-payee")
    assert r.status_code == 200, r.get_json()
    with ctx["app"].app_context():
        cm = CrewMember.query.get(r.get_json()["payee_id"])
        assert cm.name == "Nobody Knows Inc" and cm.kind == 'vendor' and cm.qbo_vendor_id == "55"
        assert cm.qbo_origin == 'qbo' and cm.active is False
        assert Transaction.query.filter_by(qbo_txn_id="9200").first().payee_id == twin_vendor
    j = c.get("/admin/qbo-vendors.json").get_json()
    assert {v["id"] for v in j["unlinked"]} == {"44"}
    # a non-super-admin is refused
    with ctx["app"].app_context():
        u = User.query.get(S["uid"])
        u.role = "admin"
        db.session.commit()
    try:
        assert c.get("/admin/qbo-vendors").status_code == 403
    finally:
        with ctx["app"].app_context():
            User.query.get(S["uid"]).role = "super_admin"
            db.session.commit()


# ── 8. manual expense attribution ─────────────────────────────────────────

def test_08_manual_expense_manual_and_alias_sources(ctx):
    c = ctx["client"]
    r = _post(c, f"/projects/{S['pid']}/actuals/manual-expense",
              {"vendor": "Cash to Ines", "amount": 50, "txn_date": "2026-07-01",
               "payment_method": "cash", "reason": "petty cash", "payee_id": S["theo"]})
    assert r.status_code in (200, 201), r.get_json()
    with ctx["app"].app_context():
        t = Transaction.query.get(r.get_json()["transaction"]["id"])
        assert (t.payee_id, t.payee_source) == (S["theo"], 'manual')
    r = _post(c, f"/projects/{S['pid']}/actuals/manual-expense",
              {"vendor": "LYFT *RIDE 8AM", "amount": 12, "txn_date": "2026-07-02",
               "payment_method": "check"})
    assert r.status_code in (200, 201), r.get_json()
    with ctx["app"].app_context():
        t = Transaction.query.get(r.get_json()["transaction"]["id"])
        assert (t.payee_id, t.payee_source) == (S["mara"], 'alias')
    r = _post(c, f"/projects/{S['pid']}/actuals/manual-expense",
              {"vendor": "x", "amount": 1, "txn_date": "2026-07-02", "payment_method": "check",
               "payee_id": 999999})
    assert r.status_code == 400


# ── 9. vendor-name link / unlink on the Payee page ────────────────────────

def test_09_vendor_name_link_and_unlink(ctx):
    c = ctx["client"]
    with ctx["app"].app_context():
        _mk_txn(vendor="HARBOR GRIP #22", amount=70, txn_date="2026-08-01")
        db.session.commit()
    r = _post(c, f"/payees/{S['harbor']}/vendor-names", {"raw": "HARBOR GRIP #22"})
    assert r.status_code == 200, r.get_json()
    j = r.get_json()
    assert j["raw"] == "harbor grip" and j["charges_attributed"] == 1
    pj = c.get(f"/payees/{S['harbor']}.json").get_json()
    linked = [a for a in pj["vendor_names"] if a["linked"]]
    assert any(a["id"] == j["alias_id"] for a in linked)
    r = _post(c, f"/payees/{S['theo']}/vendor-names", {"alias_id": j["alias_id"]})
    assert r.status_code == 409, "linked to another payee — unlink first"
    r = _post(c, f"/payees/{S['harbor']}/vendor-names/{j['alias_id']}/unlink")
    assert r.status_code == 200 and r.get_json()["charges_cleared"] == 1
    with ctx["app"].app_context():
        assert VendorAlias.query.get(j["alias_id"]).payee_id is None


# ── 10. payroll parse → itemize carries the person; pages render ──────────

def test_10_parse_people_rows_carry_crew_member_id_and_itemize_writes_it(ctx, mp):
    import ai_layer
    c = ctx["client"]
    with ctx["app"].app_context():
        wrapbook = CrewMember(name="Wrapbook Payroll Co", kind="vendor", is_vendor=True, active=True)
        db.session.add(wrapbook)
        db.session.flush()
        up = DocUpload(project_id=S["pid"], vendor="Wrapbook", amount=1300.00, status='done',
                       original_filename="wrapbook.pdf", category="invoice",
                       veryfi_data=json.dumps({"text": "payroll", "line_items": []}))
        up.set_owner(wrapbook.id)
        db.session.add(up)
        db.session.flush()
        db.session.add(Transaction(project_id=S["pid"], doc_upload_id=up.id, amount=1300.00,
                                   vendor="Wrapbook", source="qbo_sync", is_expense=True,
                                   txn_date="2026-09-01"))
        db.session.commit()
        S["wrapbook"], S["uid_doc"] = wrapbook.id, up.id

    mp.setattr(ai_layer, "extract_people", lambda payload: {
        "rows": [{"name": "Varga, Ines", "amount": 1300.00, "wages": 1000.00, "fringes": 200.00,
                  "workers_comp": 60.00, "platform_fee": 40.00}],
        "confidence": 0.9, "_provider": "stub", "document_total": 1300.00})
    r = _post(c, f"/docs/upload/{S['uid_doc']}/parse-people", {})
    assert r.status_code == 200, r.data
    rows = {x["desc"]: x for x in r.get_json()["rows"]}
    assert rows["Varga, Ines — payroll"]["line_id"] == str(S["l_theo"])
    assert rows["Varga, Ines — payroll"]["crew_member_id"] == S["theo"], \
        "the parsed row names the PERSON behind the line"
    r = _post(c, f"/docs/upload/{S['uid_doc']}/itemize",
              {"rows": [{"amount": x["amount"], "budget_line_id": x["line_id"],
                         "description": x["desc"], "crew_member_id": x.get("crew_member_id")}
                        for x in rows.values()],
               "main_line_id": ""})
    assert r.status_code == 200, r.data
    with ctx["app"].app_context():
        kids = {k.note: k for k in Transaction.query.filter_by(doc_upload_id=S["uid_doc"],
                                                              source='invoice_split').all()}
        assert (kids["Varga, Ines — payroll"].payee_id, kids["Varga, Ines — payroll"].payee_source) == (S["theo"], 'doc')
        assert kids["Varga, Ines — employer fringes"].payee_id == S["wrapbook"], \
            "a row with no person inherits the document's owner"
    # the Payee pages render with the new cards
    html = c.get(f"/payees/{S['harbor']}").get_data(as_text=True)
    assert "QuickBooks owns the legal name" in html and "Harbor Grip" in html
    html = c.get(f"/payees/{S['theo']}").get_data(as_text=True)
    assert "Vendor names" in html
    html = c.get("/payees").get_data(as_text=True)
    assert "QuickBooks vendors" in html and "Create in QuickBooks" in html
