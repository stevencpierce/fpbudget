"""Actuals Phase 4 end to end on sqlite with the Flask test client
(ACTUALS_HANDOFF, Steven's decision of Oct 30 2026).

Boots app.py against a throw-away sqlite file (RUN_BOOT_TASKS=1 so
db.create_all + seeds run), then:

  1. a project with two labor departments renders its budget and gets one
     "Payroll Fringes — <Department>" line in 6500 per department, with the
     department's fringe pot as its estimate, plus the four hidden
     auto-calc lines carrying the calc's amounts;
  2. a parsed payroll package (extract_people stubbed) lands wages on each
     person's line, fringes on the department's 6500 line, workers' comp on
     the WC line and the platform fee on the Payroll Service Fee line, and
     the labor section's variance does not move when the fringes post;
  3. a row that does not tie out keeps its split and comes back with a
     delta;
  4. removing every labor line from a department removes its 6500 line
     (and keeps + flags one that still has actuals);
  5. the boot data migration is idempotent: a second run changes nothing.

Skipped when app.py cannot be imported (CI installs only the unit-test
deps; locally the full requirements are present).
Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import json
import os
import sys
import tempfile

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_TMP = tempfile.mkdtemp(prefix="fp4-")
# Never touch a real database: this module only runs against its own
# throw-away sqlite file. A DATABASE_URL already pointing elsewhere (a dev
# shell with production credentials, say) skips the whole module.
_PRESET_DB = os.environ.get("DATABASE_URL", "")
_SAFE = (not _PRESET_DB) or _PRESET_DB.startswith("sqlite:")
if _SAFE:
    os.environ["DATABASE_URL"] = "sqlite:///" + os.path.join(_TMP, "p4.db")
    os.environ.setdefault("RUN_BOOT_TASKS", "1")
    os.environ.setdefault("SECRET_KEY", "test")

try:
    if not _SAFE:
        raise RuntimeError(f"DATABASE_URL is not sqlite ({_PRESET_DB.split('@')[-1]}); refusing to boot")
    _cwd = os.getcwd()
    os.chdir(ROOT)
    import app as A                                    # noqa: E402
    from models import (db, User, ProjectSheet, ProjectAccess, Budget,  # noqa: E402
                        BudgetLine, Transaction, DocUpload)
    import ai_layer                                    # noqa: E402
    _IMPORT_ERR = None
except Exception as _e:                                # pragma: no cover
    _IMPORT_ERR = _e
finally:
    try:
        os.chdir(_cwd)
    except Exception:
        pass

pytestmark = pytest.mark.skipif(_IMPORT_ERR is not None,
                                reason=f"app.py not importable here: {_IMPORT_ERR}")

S = {}   # ids shared across the ordered tests below


def _client():
    c = A.app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(S["uid"])
        s["_fresh"] = True
        s["_csrf"] = "t"
    return c


def _post(c, url, payload):
    return c.post(url, data=json.dumps(payload), content_type="application/json",
                  headers={"X-CSRFToken": "t"})


def _lines(bid, tag):
    with A.app.app_context():
        return BudgetLine.query.filter_by(budget_id=bid, line_tag=tag).order_by(BudgetLine.id).all()


def _ws_sections(c, pid):
    j = _client().get(f"/projects/{pid}/actuals/workspace.json").get_json()
    return {s["code"]: s for s in j["budget"]["sections"]}


def test_01_two_departments_get_their_6500_lines_with_the_right_estimates():
    A.app.config["TESTING"] = True
    with A.app.app_context():
        # Another test module may have booted app.py first (same process,
        # its sqlite file, RUN_BOOT_TASKS unset) — make the pieces this
        # module needs exist regardless of who imported app first.
        from budget_calc import seed_fringes
        db.create_all()
        seed_fringes(db.session)
        db.session.commit()
        u = User.query.filter_by(email="p4@test.tv").first()
        if not u:
            u = User(email="p4@test.tv", name="P4", role="super_admin")
            u.set_password("pw")
            db.session.add(u)
            db.session.commit()
        p = ProjectSheet(name="Phase 4 fringes")
        db.session.add(p)
        db.session.flush()
        db.session.add(ProjectAccess(project_id=p.id, user_id=u.id, role="owner"))
        # Working budget straight away (the target budget for the lines).
        b = Budget(project_id=p.id, name="P4 Working", budget_mode="working",
                   version_status="current", workers_comp_pct=0.03, payroll_fee_pct=0.02,
                   production_insurance_mode="flat", production_insurance_flat=150,
                   company_fee_pct=0.10)
        db.session.add(b)
        db.session.flush()
        L = lambda **kw: BudgetLine(budget_id=b.id, quantity=1, rate_type="flat_day", **kw)
        rows = [
            L(account_code=2001, account_name="Production Staff", description="Director",
              is_labor=True, days=2, rate=1000, fringe_type="N", sort_order=1),
            L(account_code=2002, account_name="Production Staff", description="Producer",
              is_labor=True, days=1, rate=800, fringe_type="N", sort_order=2),
            L(account_code=2101, account_name="Talent", description="Host",
              is_labor=True, days=1, rate=500, fringe_type="N", sort_order=3),
            L(account_code=2601, account_name="Camera", description="Lens",
              is_labor=False, days=1, rate=300, sort_order=4),
        ]
        db.session.add_all(rows)
        db.session.commit()
        S.update(uid=u.id, pid=p.id, bid=b.id,
                 director=rows[0].id, producer=rows[1].id, host=rows[2].id)
        rate = float(A.get_fringe_configs(db.session, p.id)["N"].rate)
        S["rate"] = rate
    c = _client()
    r = c.get(f"/projects/{S['pid']}/budget/{S['bid']}")
    assert r.status_code == 200
    html = r.data.decode()
    fr = _lines(S["bid"], "fringe_rollup")
    by_sec = {l.fringe_for_section: l for l in fr}
    assert sorted(by_sec) == [2000, 2100]
    assert all(l.account_code == 6500 and l.sync_omit and not l.is_labor for l in fr)
    assert by_sec[2000].description == "Payroll Fringes — Production Staff"
    assert float(by_sec[2000].estimated_total) == round(2800 * S["rate"], 2)
    assert float(by_sec[2100].estimated_total) == round(500 * S["rate"], 2)
    autos = {l.auto_kind: l for l in _lines(S["bid"], "auto_calc")}
    assert sorted(autos) == ["fee", "ins", "pcf", "wc"]
    assert (autos["wc"].account_code, float(autos["wc"].estimated_total)) == (6000, round(3300 * 0.03, 2))
    assert (autos["ins"].account_code, float(autos["ins"].estimated_total)) == (6000, 150.0)
    assert (autos["fee"].account_code, float(autos["fee"].estimated_total)) == (6500, round(3300 * 0.02, 2))
    assert autos["pcf"].account_code == 6500
    # grid: labor sections carry the read-only summary, 6500 the real rows
    assert "2000-FR" in html and "2100-FR" in html and "in 6500 ↗" in html
    assert 'id="fr-6500-2000"' in html and 'id="fr-6500-2100"' in html
    assert "6000-WC" in html and "6000-PLI" in html and "6500-PF" in html
    S["fr2000"], S["fr2100"] = by_sec[2000].id, by_sec[2100].id
    S["wc"], S["fee"] = autos["wc"].id, autos["fee"].id
    # register: fringe lines under 6500 with the department name, as targets
    secs = _ws_sections(c, S["pid"])
    labels = [(l["label"], l["is_drop_target"]) for l in secs[6500]["lines"] if l["line_tag"]]
    assert ("6500 · Payroll Fringes — Production Staff", True) in labels
    assert ("6500 · Payroll Fringes — Talent", True) in labels
    assert not any(l["line_tag"] == "fringe_rollup" for l in secs[2000]["lines"])
    assert secs[2000]["estimate"] == 2800.0                        # wages only
    # 6500 in the register = fringes + payroll fee + the Production Company
    # Fee line (the register counts the fee line so its grand total equals
    # the Top Sheet grand total; the budget grid keeps the fee below the
    # subtotal). Fee = 10% × (3600 lines + 99 wc + 150 ins + 66 fee; the
    # fringes are fee-exempt by default) = 391.50.
    pcf = [l for l in secs[6500]["lines"] if l["auto_kind"] == "pcf"][0]
    assert pcf["estimate"] == round(0.10 * (3600 + 99 + 150 + 66), 2)
    assert secs[6500]["estimate"] == round(3300 * S["rate"] + 66.0 + pcf["estimate"], 2)


def test_02_parsed_package_lands_wages_on_people_and_fringes_in_6500(monkeypatch):
    pid, bid = S["pid"], S["bid"]
    rate = S["rate"]
    with A.app.app_context():
        up = DocUpload(project_id=pid, vendor="Wrapbook", amount=2500.00,
                       original_filename="wrapbook.pdf", category="invoice",
                       veryfi_data=json.dumps({"text": "payroll", "line_items": []}))
        db.session.add(up)
        db.session.flush()
        t = Transaction(project_id=pid, doc_upload_id=up.id, amount=2500.00,
                        vendor="Wrapbook", source="qbo_sync", is_expense=True)
        db.session.add(t)
        db.session.commit()
        S["uid_doc"] = up.id

    def fake_people(payload):
        return {"rows": [
            {"name": "Director", "amount": 1300.00, "wages": 1000.00, "fringes": 200.00,
             "workers_comp": 60.00, "platform_fee": 40.00},
            {"name": "Host", "amount": 650.00, "wages": 500.00, "fringes": 100.00,
             "workers_comp": 30.00, "platform_fee": 20.00},
            # does not tie out: 1000 + 200 + 60 + 40 = 1300 ≠ 550 → delta
            {"name": "Producer", "amount": 550.00, "wages": 400.00, "fringes": 80.00,
             "workers_comp": 24.00, "platform_fee": 16.00},
        ], "confidence": 0.9, "_provider": "claude", "document_total": 2500.00}
    monkeypatch.setattr(ai_layer, "extract_people", fake_people)

    c = _client()
    r = _post(c, f"/docs/upload/{S['uid_doc']}/parse-people", {})
    assert r.status_code == 200, r.data
    j = r.get_json()
    rows = {x["desc"]: x for x in j["rows"]}
    assert rows["Director — payroll"]["line_id"] == str(S["director"])
    assert rows["Host — payroll"]["line_id"] == str(S["host"])
    assert rows["Producer — payroll"]["amount"] == 430.00          # 400 + (550 − 520)
    assert rows["Producer — payroll"]["delta"] == 30.00
    assert j["deltas"] == [{"name": "Producer", "delta": 30.00}]
    assert rows["Director — employer fringes"]["line_id"] == str(S["fr2000"])
    assert rows["Producer — employer fringes"]["line_id"] == str(S["fr2000"])
    assert rows["Host — employer fringes"]["line_id"] == str(S["fr2100"])
    assert rows["Workers' compensation (employer)"]["line_id"] == str(S["wc"])
    assert rows["Workers' compensation (employer)"]["amount"] == 114.00
    assert rows["Payroll platform / handling fee"]["line_id"] == str(S["fee"])
    assert rows["Payroll platform / handling fee"]["amount"] == 76.00
    assert "Payroll Fringes — Production Staff" in " ".join(j["fringe_lines"])
    assert j["sum"] == 2500.00

    before = _ws_sections(c, pid)
    # Save the itemization exactly as the UI would (people first, then fringes).
    r = _post(c, f"/docs/upload/{S['uid_doc']}/itemize",
              {"rows": [{"amount": x["amount"], "budget_line_id": x["line_id"],
                         "description": x["desc"]} for x in j["rows"]],
               "main_line_id": ""})
    assert r.status_code == 200, r.data
    after = _ws_sections(c, pid)
    # wages on the people, in their own sections
    l2000 = {l["id"]: l for l in after[2000]["lines"]}
    assert l2000[S["director"]]["actual"] == 1000.00
    assert l2000[S["producer"]]["actual"] == 430.00
    assert after[2000]["actual"] == 1430.00
    assert after[2100]["actual"] == 500.00
    # fringes on the department lines in 6500; WC on the WC line; fee on the fee line
    l6500 = {l["id"]: l for l in after[6500]["lines"]}
    assert l6500[S["fr2000"]]["actual"] == 280.00
    assert l6500[S["fr2100"]]["actual"] == 100.00
    assert l6500[S["fee"]]["actual"] == 76.00
    l6000 = {l["id"]: l for l in after[6000]["lines"]}
    assert l6000[S["wc"]]["actual"] == 114.00
    # the labor section's variance moved by the WAGES only — not the fringes
    assert before[2000]["variance"] - after[2000]["variance"] == 1430.00
    assert before[2100]["variance"] - after[2100]["variance"] == 500.00
    # and the top sheet agrees: 2000's actual is wages only
    r = c.get(f"/projects/{pid}/budget/{bid}/audit.json")
    assert r.status_code == 200
    aud = r.get_json()
    assert aud["summary"]["section_errors"] == 0
    assert all(l["ok"] for l in aud["lines"])
    html = c.get(f"/projects/{pid}/budget/{bid}").data.decode()
    import re
    m = re.search(r'data-ts-code="2000".*?</tr>', html, re.S)
    cells = re.findall(r'<td class="col-num[^"]*"[^>]*>\s*([^<]*?)\s*<', m.group(0))
    assert cells[2] == "$1,430.00"
    m = re.search(r'data-ts-code="6500".*?</tr>', html, re.S)
    cells = re.findall(r'<td class="col-num[^"]*"[^>]*>\s*([^<]*?)\s*<', m.group(0))
    assert cells[2] == "$456.00"      # 280 + 100 fringes + 76 fee


def test_03_removing_all_labor_from_a_department_removes_its_line_unless_actuals():
    pid, bid = S["pid"], S["bid"]
    with A.app.app_context():
        # Talent (2100) has fringe actuals coded → line must stay, flagged.
        host = BudgetLine.query.get(S["host"])
        db.session.delete(host)
        db.session.commit()
    c = _client()
    r = c.get(f"/projects/{pid}/budget/{bid}")
    assert r.status_code == 200
    html = r.data.decode()
    fr = {l.fringe_for_section: l for l in _lines(bid, "fringe_rollup")}
    assert 2100 in fr and float(fr[2100].estimated_total) == 0.0
    assert "no labor left in this department" in html
    with A.app.app_context():
        # Now strip Talent's fringe actuals (re-point them to 2000's line) and
        # the empty department line goes away on the next render.
        Transaction.query.filter_by(budget_line_id=S["fr2100"]).update(
            {Transaction.budget_line_id: S["fr2000"]}, synchronize_session=False)
        db.session.commit()
    r = c.get(f"/projects/{pid}/budget/{bid}")
    assert r.status_code == 200
    fr = {l.fringe_for_section: l for l in _lines(bid, "fringe_rollup")}
    assert sorted(fr) == [2000]
    assert "2100-FR" not in r.data.decode()


def test_04_boot_migration_is_idempotent_and_adopts_legacy_lines():
    pid, bid = S["pid"], S["bid"]
    with A.app.app_context():
        # A pre-Phase-4 per-section fringe line with a coded actual, plus a
        # parked section:6500 fringe row whose person resolves.
        legacy = BudgetLine(budget_id=bid, account_code=2600, account_name="Camera",
                            description="Payroll Fringes — Camera", line_tag="fringe_rollup",
                            estimated_total=0, is_labor=False, sync_omit=True, sort_order=9999)
        db.session.add(legacy)
        db.session.flush()
        db.session.add(Transaction(project_id=pid, amount=12.00, vendor="Wrapbook",
                                   source="invoice_split", budget_line_id=legacy.id,
                                   account_code=2600, is_expense=True,
                                   note="Grip — employer fringes"))
        parked = Transaction(project_id=pid, amount=9.00, vendor="Wrapbook",
                             source="invoice_split", budget_line_id=None, account_code=6500,
                             is_expense=True, note="Director — employer fringes",
                             doc_upload_id=S["uid_doc"])
        db.session.add(parked)
        db.session.commit()
        legacy_id, parked_id = legacy.id, parked.id
        first = A._migrate_fringes_to_admin_body()
        adopted = BudgetLine.query.get(legacy_id)
        assert adopted is not None and adopted.account_code == 6500
        assert adopted.fringe_for_section == 2600
        assert Transaction.query.get(parked_id).budget_line_id == S["fr2000"]
        snapshot = sorted((l.id, l.account_code, l.line_tag, l.fringe_for_section, l.auto_kind,
                           float(l.estimated_total or 0))
                          for l in BudgetLine.query.filter_by(budget_id=bid).all())
        second = A._migrate_fringes_to_admin_body()
        again = sorted((l.id, l.account_code, l.line_tag, l.fringe_for_section, l.auto_kind,
                        float(l.estimated_total or 0))
                       for l in BudgetLine.query.filter_by(budget_id=bid).all())
        assert snapshot == again
        assert "fringe rows moved 0" in second
    assert "fringe rows moved 1" in first
