"""Documents Phase 2, end to end on sqlite with the Flask test client
(DOCUMENTS_HANDOFF Phase 2, 2026-11-01).

Boots the real app against a throwaway sqlite file with the analyzer and
Dropbox stubbed, then walks the acceptance list:

  • upload a W-9 from a Payee page with NO project → it appears in that
    person's Documents on file and w9_received_on is set
  • a second W-9 → flagged as a pending duplicate; Replace supersedes
  • inbox JSON carries why_here and the payee suggestion; bulk assign_payee
  • set type to insurance with expires_on in the past → red on the Payee page
  • changing a matched invoice to contract leaves the bank charge in place
    (the Phase 0 guarantee still holds)
  • the 0023 data step moves sub-types out of `vendor` and sets the W-9
    pointer (run against the same sqlite file)

Skipped automatically where app.py's runtime deps are not installed (CI's
unit step installs only the handful the pure tests need).

Run: python -m pytest tests/unit/test_doc_ownership_boot.py -q --confcutdir=tests/unit -o addopts="" -s
"""
import io
import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-docs2-")
os.environ.setdefault("DATABASE_URL", "sqlite:///" + os.path.join(_DB_DIR, "t.db"))
os.environ.pop("RUN_BOOT_TASKS", None)

try:
    import app as fpapp                       # noqa: E402
    from models import (db, User, ProjectSheet, CrewMember, DocUpload,   # noqa: E402
                        Transaction, ProjectAccess)
    import fp_analyzer                        # noqa: E402
    import routes.payees                      # noqa: F401,E402  (registers the routes)
except Exception as _e:                       # pragma: no cover — CI without runtime deps
    pytest.skip(f"app runtime deps not installed: {_e}", allow_module_level=True)


CREW_ROOT = fpapp._crew_db_root()
PDF = b"%PDF-1.4\n%fake\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"


class _FakeDbx:
    """Stub Dropbox client: every call succeeds and echoes the destination."""
    class _M:
        def __init__(self, p): self.path_display = p
    class _R:
        def __init__(self, p): self.metadata = _FakeDbx._M(p)
    def files_copy_v2(self, src, dest, **kw): return _FakeDbx._R(dest)
    def files_move_v2(self, src, dest, **kw): return _FakeDbx._R(dest)
    def files_get_metadata(self, p): return _FakeDbx._M(p)
    def files_delete_v2(self, p): return None


def _fake_analyzer(kind_text, doc_type, confident=True):
    """A stand-in for fp_analyzer.analyze_and_file_single that files under
    the dest_folder (payee mode) or the project (project mode)."""
    def run(file_bytes, filename, project_name, user_name, dest_folder=None, archive_base=""):
        year = "2026"
        if dest_folder:
            filed = f"{dest_folder.rstrip('/').replace('{year}', year)}/2026-10-01_TAX_{filename}"
            archive = f"{archive_base.rstrip('/')}/2026-10/batch/{filename}"
        else:
            filed = f"/ops/{project_name}/01_ADMIN/PROCESSED DOCUMENTS/X/{user_name}/{filename}"
            archive = f"/ops/{project_name}/01_ADMIN/PROCESSED DOCUMENTS/_SOURCE_ARCHIVE/2026-10/b/{filename}"
        return {
            "status": "filed" if confident else "needs_review",
            "doc_type": doc_type,
            "filed_path": filed if confident else None,
            "staged_path": archive,
            "confidence": 0.93 if confident else 0.4,
            "new_filename": os.path.basename(filed),
            "original_filename": filename,
            "duplicate": False, "error": None, "needs_review": not confident,
            "vr": {"vendor": {"name": "Theo Lindqvist"}, "date": "2026-10-01",
                   "total": None, "ocr_text": kind_text, "document_type": doc_type},
        }
    return run


@pytest.fixture(scope="module")
def ctx(monkeypatch_module):
    app = fpapp.app
    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    with app.app_context():
        db.create_all()
        u = User(email="steven@thefp.tv", name="Steven", role="super_admin")
        u.set_password("pw")
        db.session.add(u)
        p = ProjectSheet(name="GrubHub Fee Breakdown", dropbox_folder="GrubHub_Fee_Breakdown", status="active")
        db.session.add(p)
        theo = CrewMember(name="Theo Lindqvist", kind="person", active=True)
        vega = CrewMember(name="Vega Camera LLC", kind="vendor", is_vendor=True, active=True)
        db.session.add_all([theo, vega])
        db.session.commit()
        ids = {"user": u.id, "project": p.id, "theo": theo.id, "vega": vega.id}
    monkeypatch_module.setattr(fpapp, "_dbx_client", lambda: _FakeDbx())
    monkeypatch_module.setattr(fp_analyzer, "get_dropbox_client", lambda: _FakeDbx())
    monkeypatch_module.setenv("DROPBOX_ACCESS_TOKEN", "x")
    c = app.test_client()
    r = c.post("/login", data={"email": "steven@thefp.tv", "password": "pw"}, follow_redirects=False)
    assert r.status_code in (302, 303), r.status_code
    with c.session_transaction() as s:
        s["_csrf"] = "t0k3n"
    c.environ_base["HTTP_X_CSRFTOKEN"] = "t0k3n"
    return {"app": app, "client": c, **ids}


@pytest.fixture(scope="module")
def monkeypatch_module():
    from _pytest.monkeypatch import MonkeyPatch
    m = MonkeyPatch()
    yield m
    m.undo()


def _upload(client, url, name, kind_text, doc_type, monkeypatch, confident=True, extra=None):
    monkeypatch.setattr(fp_analyzer, "analyze_and_file_single", _fake_analyzer(kind_text, doc_type, confident))
    data = {"file": (io.BytesIO(PDF), name)}
    if extra:
        data.update(extra)
    return client.post(url, data=data, content_type="multipart/form-data")


# ── 1. W-9 from the Payee page, no project ─────────────────────────────────

def test_payee_page_w9_upload(ctx, monkeypatch_module):
    c = ctx["client"]
    r = _upload(c, f"/payees/{ctx['theo']}/docs/upload", "w9_lindqvist.pdf",
                "Form W-9 Request for Taxpayer Identification Number", "tax_form", monkeypatch_module)
    assert r.status_code == 201, r.get_json()
    j = r.get_json()
    print("\n[payee upload]", json.dumps(j, default=str)[:300])
    assert j["category"] == "tax_form" and j["tax_form_kind"] == "w9"
    with ctx["app"].app_context():
        d = DocUpload.query.get(j["upload_id"])
        assert d.project_id is None
        assert d.owner_payee_id == ctx["theo"] == d.crew_member_id
        assert d.received_on == date(2026, 10, 1)
        assert d.filed_dropbox_path.startswith(f"{CREW_ROOT}/Theo Lindqvist/2026/")
        assert d.crew_dbx_path == d.filed_dropbox_path          # filed copy IS the crew copy
        assert d.source_archive_path.startswith(f"{CREW_ROOT}/_SOURCE_ARCHIVE/")
        assert Transaction.query.filter_by(doc_upload_id=d.id).count() == 0
        cm = CrewMember.query.get(ctx["theo"])
        assert cm.w9_received_on == date(2026, 10, 1) and cm.w9_doc_upload_id == d.id
        ctx["w9_a"] = d.id
    pj = c.get(f"/payees/{ctx['theo']}.json").get_json()
    groups = {g["key"]: g for g in pj["paperwork"]}
    assert "tax_form" in groups and groups["tax_form"]["docs"][0]["id"] == ctx["w9_a"]
    assert groups["tax_form"]["docs"][0]["subtype"] == "W-9"
    assert pj["stats"]["w9"]["status"] == "on_file" and pj["stats"]["w9"]["source"] == "payee"
    page = c.get(f"/payees/{ctx['theo']}")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert "w9_lindqvist.pdf" in html and "Upload document" in html and "disabled" not in html.split("Upload document")[0][-200:]
    # the project-less editor renders
    ed = c.get(f"/docs/{ctx['w9_a']}/editor")
    assert ed.status_code == 200 and "Back to Theo Lindqvist" in ed.get_data(as_text=True)


# ── 2. Second W-9 → pending duplicate; Replace supersedes ──────────────────

def test_second_w9_flagged_then_replace(ctx, monkeypatch_module):
    c = ctx["client"]
    r = _upload(c, f"/payees/{ctx['theo']}/docs/upload", "w9_lindqvist_2026.pdf",
                "Form W-9 (Rev. 2024) Request for Taxpayer", "tax_form", monkeypatch_module)
    assert r.status_code == 201, r.get_json()
    j = r.get_json()
    print("[second W-9]", json.dumps({k: j[k] for k in ('status', 'is_duplicate', 'duplicate_of_id')}))
    assert j["is_duplicate"] is True and j["duplicate_of_id"] == ctx["w9_a"]
    ctx["w9_b"] = j["upload_id"]
    pj = c.get(f"/payees/{ctx['theo']}.json").get_json()
    assert [e["id"] for e in pj["pending_duplicates"]] == [ctx["w9_b"]]
    html = c.get(f"/payees/{ctx['theo']}").get_data(as_text=True)
    assert "Possible duplicate" in html and "payeeResolveDup(" in html
    # the payee's pointer did NOT move to a pending duplicate
    with ctx["app"].app_context():
        assert CrewMember.query.get(ctx["theo"]).w9_doc_upload_id == ctx["w9_a"]
    r = c.post(f"/docs/upload/{ctx['w9_b']}/resolve-duplicate", json={"action": "replace"})
    assert r.status_code == 200, r.get_json()
    print("[replace]", r.get_json())
    with ctx["app"].app_context():
        a = DocUpload.query.get(ctx["w9_a"]); b = DocUpload.query.get(ctx["w9_b"])
        assert a.status == "deleted" and json.loads(a.trash_meta)["superseded_by_id"] == b.id
        assert b.is_duplicate is False and b.duplicate_of_id is None
        cm = CrewMember.query.get(ctx["theo"])
        assert cm.w9_doc_upload_id == b.id and cm.w9_received_on == date(2026, 10, 1)
    pj = c.get(f"/payees/{ctx['theo']}.json").get_json()
    tax = [e["id"] for g in pj["paperwork"] if g["key"] == "tax_form" for e in g["docs"]]
    assert tax == [ctx["w9_b"]]
    assert [(t["id"], t["superseded_by_id"]) for t in pj["trashed"]] == [(ctx["w9_a"], ctx["w9_b"])]


# ── 3. Inbox JSON: why_here + payee suggestion; bulk assign_payee ──────────

def test_inbox_why_here_and_suggestion_and_bulk_assign(ctx):
    c = ctx["client"]
    pid = ctx["project"]
    with ctx["app"].app_context():
        inv = DocUpload(project_id=pid, uploader_id=ctx["user"], status="done", category="invoice",
                        vendor="Vega Camera LLC", amount=1950, doc_date=date(2026, 10, 29),
                        original_filename="IMG_4471.pdf", filed_dropbox_path="/ops/x/INVOICES/a.pdf",
                        confidence=92,
                        veryfi_data=json.dumps({"ocr_text": "Invoice from Vega Camera LLC to FP"}))
        coi = DocUpload(project_id=pid, uploader_id=ctx["user"], status="done", category="insurance",
                        vendor="Northshore Grip", original_filename="Northshore_COI_2026.pdf",
                        filed_dropbox_path="/ops/x/INSURANCE_COIs/c.pdf", confidence=90)
        unk = DocUpload(project_id=pid, uploader_id=ctx["user"], status="review", category=None,
                        original_filename="scan.pdf", filed_dropbox_path="/ops/x/_UPLOADS_PENDING/s.pdf",
                        confidence=40)
        db.session.add_all([inv, coi, unk]); db.session.commit()
        ctx["inv"], ctx["coi"], ctx["unk"] = inv.id, coi.id, unk.id
    j = c.get(f"/projects/{pid}/docs/inbox.json").get_json()
    rows = {r["id"]: r for r in j["docs"]}
    print("[inbox]", {k: (v["state"], v["why_here"], (v["payee_suggestion"] or {}).get("name")) for k, v in rows.items()})
    assert rows[ctx["inv"]]["payee_suggestion"]["crew_member_id"] == ctx["vega"]
    assert rows[ctx["inv"]]["why_here"] == "confirm_payee"
    assert rows[ctx["coi"]]["why_here"] == "expiry_missing"
    assert rows[ctx["unk"]]["state"] == "needs_review" and rows[ctx["unk"]]["why_here"] == "type_unclear"
    assert rows[ctx["inv"]]["doc_class"] == "evidence" and rows[ctx["coi"]]["doc_class"] == "paperwork"
    assert any(p["id"] == ctx["vega"] for p in j["payees"])
    assert "trash" in j["counts"]
    r = c.post(f"/projects/{pid}/docs/batch", json={"action": "assign_payee", "payee_id": ctx["vega"],
                                                    "ids": [ctx["inv"], ctx["coi"]]})
    assert r.status_code == 200 and r.get_json()["done"] == 2, r.get_json()
    with ctx["app"].app_context():
        for did in (ctx["inv"], ctx["coi"]):
            d = DocUpload.query.get(did)
            assert d.owner_payee_id == ctx["vega"] == d.crew_member_id
    j = c.get(f"/projects/{pid}/docs/inbox.json").get_json()
    rows = {r["id"]: r for r in j["docs"]}
    assert rows[ctx["inv"]]["payee_name"] == "Vega Camera LLC" and rows[ctx["inv"]]["why_here"] is None
    # bad payee → 400, unknown action → 400
    assert c.post(f"/projects/{pid}/docs/batch", json={"action": "assign_payee", "payee_id": 99999, "ids": [ctx["inv"]]}).status_code == 400
    # the pages that mount the one editor render
    assert c.get(f"/projects/{pid}/docs").status_code == 200
    ed = c.get(f"/projects/{pid}/docs/{ctx['inv']}/editor").get_data(as_text=True)
    assert 'id="docDetailLinePicker"' in ed and 'docDetailLineOptsTpl' in ed and 'doc-editor.js' in ed
    assert c.get(f"/docs/{ctx['inv']}/editor").status_code == 302      # project doc → project editor


# ── 4. COI with an expiry in the past → red on the Payee page ──────────────

def test_insurance_expiry_red(ctx):
    c = ctx["client"]
    past = (date.today() - timedelta(days=3)).isoformat()
    r = c.post(f"/docs/upload/{ctx['coi']}/update", json={"category": "insurance", "expires_on": past,
                                                          "owner_payee_id": ctx["vega"]})
    assert r.status_code == 200, r.get_json()
    assert r.get_json()["expires_on"] == past
    pj = c.get(f"/payees/{ctx['vega']}.json").get_json()
    ins = [e for g in pj["paperwork"] if g["key"] == "insurance" for e in g["docs"]]
    print("[coi]", ins[0]["coi"])
    assert ins[0]["coi"]["state"] == "bad" and ins[0]["expiry_state"] == "bad"
    html = c.get(f"/payees/{ctx['vega']}").get_data(as_text=True)
    assert 'pay-doc bad' in html
    soon = (date.today() + timedelta(days=20)).isoformat()
    c.post(f"/docs/upload/{ctx['coi']}/update", json={"expires_on": soon})
    pj = c.get(f"/payees/{ctx['vega']}.json").get_json()
    ins = [e for g in pj["paperwork"] if g["key"] == "insurance" for e in g["docs"]]
    assert ins[0]["coi"]["state"] == "warn"


# ── 5. Phase 0 guarantee: matched invoice → contract keeps the bank charge ─

def test_matched_invoice_to_contract_keeps_bank_charge(ctx):
    c = ctx["client"]
    pid = ctx["project"]
    with ctx["app"].app_context():
        own = Transaction(project_id=pid, source="doc_upload", doc_upload_id=ctx["inv"], vendor="Vega",
                          amount=1950, is_expense=True, match_status="unmatched")
        bank = Transaction(project_id=pid, source="qbo_sync", doc_upload_id=ctx["inv"], vendor="VEGA CAMERA",
                           amount=1950, is_expense=True, match_status="confirmed", account_code=2600)
        db.session.add_all([own, bank]); db.session.commit()
        own_id, bank_id = own.id, bank.id
    r = c.post(f"/docs/upload/{ctx['inv']}/update", json={"category": "contract"})
    assert r.status_code == 200, r.get_json()
    with ctx["app"].app_context():
        assert Transaction.query.get(own_id) is None
        b = Transaction.query.get(bank_id)
        assert b is not None and b.doc_upload_id is None and b.account_code == 2600
        print("[phase 0]", f"doc-born txn {own_id} removed; bank txn {bank_id} kept, unlinked, coding intact")


# ── 6. A project-less document refuses an evidence type ────────────────────

def test_project_less_doc_refuses_evidence_type(ctx):
    c = ctx["client"]
    r = c.post(f"/docs/upload/{ctx['w9_b']}/update", json={"category": "invoice"})
    assert r.status_code == 400 and "no project" in r.get_json()["error"].lower()


# ── 7. Mobile: payee_id on the project upload + GET /api/v1/payees ─────────

def test_mobile_payee_id_and_roster(ctx, monkeypatch_module):
    from api_auth import generate_token, hash_token
    from models import ApiToken
    with ctx["app"].app_context():
        raw = generate_token()
        db.session.add(ApiToken(user_id=ctx["user"], token_hash=hash_token(raw), device_name="test"))
        db.session.commit()
    c = ctx["app"].test_client()
    h = {"Authorization": f"Bearer {raw}"}
    r = c.get("/api/v1/payees", headers=h)
    assert r.status_code == 200
    names = [p["name"] for p in r.get_json()["payees"]]
    assert "Theo Lindqvist" in names and "Vega Camera LLC" in names
    monkeypatch_module.setattr(fp_analyzer, "analyze_and_file_single",
                               _fake_analyzer("Invoice 88213", "invoice"))
    r = c.post(f"/api/v1/projects/{ctx['project']}/docs/upload", headers=h,
               data={"file": (io.BytesIO(PDF), "inv.pdf"), "payee_id": str(ctx["vega"])},
               content_type="multipart/form-data")
    assert r.status_code == 201, r.get_json()
    with ctx["app"].app_context():
        d = DocUpload.query.get(r.get_json()["upload_id"])
        assert d.owner_payee_id == ctx["vega"] and d.project_id == ctx["project"]
        assert Transaction.query.filter_by(doc_upload_id=d.id, source="doc_upload").count() == 1


# ── 8. The 0023 data step on sqlite ────────────────────────────────────────

def test_migration_data_step(ctx):
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "m0023", os.path.join(ROOT, "alembic", "versions", "0023_doc_ownership.py"))
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    with ctx["app"].app_context():
        legacy_w9 = DocUpload(project_id=ctx["project"], uploader_id=ctx["user"], status="done",
                              category="employee_vendor_doc", vendor="W-9", crew_member_id=ctx["theo"],
                              doc_date=date(2025, 6, 1), original_filename="old_w9.pdf")
        legacy_id = DocUpload(project_id=ctx["project"], uploader_id=ctx["user"], status="done",
                              category="employee_vendor_doc", vendor="Driver's License",
                              crew_member_id=ctx["theo"], original_filename="dl.pdf")
        legacy_tax = DocUpload(project_id=ctx["project"], uploader_id=ctx["user"], status="done",
                               category="tax_form", vendor="Acme", crew_member_id=ctx["vega"],
                               filed_filename="2025_TAX_W-8BEN-E_Acme.pdf", doc_date=date(2025, 2, 2))
        db.session.add_all([legacy_w9, legacy_id, legacy_tax]); db.session.commit()
        ids = (legacy_w9.id, legacy_id.id, legacy_tax.id)
        # undo what the ORM default did so the rows look pre-0023
        db.session.execute(db.text("UPDATE doc_upload SET owner_payee_id = NULL WHERE id IN (:a,:b,:c)"),
                           {"a": ids[0], "b": ids[1], "c": ids[2]})
        db.session.execute(db.text("UPDATE crew_member SET w9_received_on = NULL, w9_doc_upload_id = NULL "
                                   "WHERE id = :v"), {"v": ctx["vega"]})
        db.session.commit()
        m._data_step(db.session.connection())
        db.session.commit()
        db.session.expire_all()
        w9, dl, tax = (DocUpload.query.get(i) for i in ids)
        print("[0023]", (w9.category, w9.tax_form_kind, w9.doc_subtype, w9.vendor, w9.received_on),
              (dl.category, dl.doc_subtype, dl.vendor), (tax.tax_form_kind, tax.received_on))
        assert (w9.category, w9.tax_form_kind, w9.doc_subtype, w9.vendor) == ("tax_form", "w9", "W-9", None)
        assert w9.owner_payee_id == ctx["theo"] and w9.received_on == date(2025, 6, 1)
        assert (dl.category, dl.doc_subtype, dl.vendor, dl.tax_form_kind) == ("employee_vendor_doc", "Driver's License", None, None)
        assert tax.tax_form_kind == "w8bene" and tax.received_on == date(2025, 2, 2) and tax.vendor == "Acme"
        vega = CrewMember.query.get(ctx["vega"])
        assert vega.w9_received_on == date(2025, 2, 2) and vega.w9_doc_upload_id == tax.id
        # Theo keeps the newer 2026 form from test 2, not the 2025 legacy one
        assert CrewMember.query.get(ctx["theo"]).w9_doc_upload_id == ctx["w9_b"]
