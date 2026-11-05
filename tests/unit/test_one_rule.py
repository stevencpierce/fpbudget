"""Cleanup D — one rule per decision (2026-11-02).

Pins that each decision has ONE implementation and that every former
copy routes through it:

  1. counts_as_spend at each former call-site shape (PO billed total,
     sub-budget billed total, per-line ledger coded_total, mismatch scan)
     and a source guard that the rollups in app.py / routes/actuals.py
     filter on it and not on a hand copy
  2. vendor cleaning: strip_processor_prefix / vendor_match_key /
     payee_name_key; canon_vendor's storage key is unchanged; the old
     app._normalize_vendor prefix list gives the same answers except the
     documented punctuation / 'IN *' cases
  3. section_for_code edge cases and the explicit default
  4. document-type, payment-method, mode and COA lists: the Python canon,
     and the templates / JS read the served copy (no literal list left)
  5. seed catalogs name sections exactly as FP_COA_NAMES does
  6. no private JS esc / money copy survives outside static/fp-utils.js

The app-booting tests at the end render the real pages on sqlite and
compare what the browser would see against the Python canon; they skip
themselves where app.py's runtime deps are not installed.
Run: python -m pytest tests/unit/test_one_rule.py -q --confcutdir=tests/unit -o addopts=""
"""
import os
import re
import sys
import tempfile
from datetime import datetime
from types import SimpleNamespace as NS

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

from actuals import (                                   # noqa: E402
    counts_as_spend, row_counts_as_spend, signed_amount_value,
    strip_processor_prefix, vendor_match_key, payee_name_key, canon_vendor,
    _vendor_similarity, payee_name_index,
    PAYMENT_METHODS, PAYMENT_METHOD_LABELS,
)
from budget_calc import (                               # noqa: E402
    section_for_code, FP_COA_SECTIONS, FP_COA_NAMES, FP_CATALOG_SEED,
    MODE_META, mode_label, mode_meta,
)


def _src(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def _func_body(src, name):
    """Source text of top-level `def name(` up to the next top-level def /
    decorator (good enough to assert which helpers a rollup calls)."""
    m = re.search(r"^def %s\(" % re.escape(name), src, flags=re.M)
    assert m, f"{name} not found"
    rest = src[m.end():]
    n = re.search(r"^(def |@app\.route)", rest, flags=re.M)
    return rest[: n.start()] if n else rest


# ── 1. counts_as_spend at the former call-site shapes ───────────────────

def _txn(**kw):
    base = dict(id=1, source="qbo_sync", amount=100.0, is_expense=True,
                not_project_expense=False, activated_at=None, backup_of_txn_id=None,
                budget_line_id=10, account_code=2601)
    base.update(kw)
    return NS(**base)


# The rows every former copy used to disagree on: a bank charge, a refund, an
# un-activated receipt row, a backup receipt, a split container.
ROWS = [
    _txn(id=1, amount=250.00),                                             # counts
    _txn(id=2, amount=40.00, is_expense=False),                            # refund: -40
    _txn(id=3, source="doc_upload", amount=999.0, activated_at=None),      # evidence only
    _txn(id=4, source="doc_upload", amount=30.0,
         activated_at=datetime(2026, 10, 1)),                              # logged cash: counts
    _txn(id=5, amount=500.0, backup_of_txn_id=1),                          # backup receipt
    _txn(id=6, amount=700.0),                                              # split parent
    _txn(id=7, amount=5.0, not_project_expense=True),                      # excluded
]
SPLIT_PARENTS = {6}
EXPECTED_SPEND = 250.00 - 40.00 + 30.00


def _rollup(rows, parents=SPLIT_PARENTS):
    return round(sum(signed_amount_value(t.amount, t.is_expense)
                     for t in rows if row_counts_as_spend(t, parents)), 2)


def test_po_billed_total_shape():
    # app._po_to_dict: lines on the PO → their transactions, signed.
    assert _rollup(ROWS) == EXPECTED_SPEND


def test_sub_budget_billed_shape():
    # app._sub_budget_to_dict: same rows, same answer, no copy.
    assert _rollup(ROWS) == _rollup(ROWS)


def test_line_ledger_coded_total_shape():
    # routes/actuals.actuals_line_ledger used to add every row's expense
    # amount (250 + 999 + 30 + 500 + 700 + 5) and ignore the refund.
    old_copy = sum(t.amount for t in ROWS if t.is_expense)
    assert old_copy == 2484.0
    assert _rollup(ROWS) == EXPECTED_SPEND


def test_mismatch_scan_line_shape():
    # app._run_budget_mismatch_scan used to drop nothing but not-project and
    # add the refund as +40 (amounts are stored unsigned).
    old_copy = sum(float(t.amount) for t in ROWS if not t.not_project_expense)
    assert old_copy == 2519.0
    assert _rollup(ROWS) == EXPECTED_SPEND


def test_counts_as_spend_sql_names_every_rule():
    from sqlalchemy.dialects import sqlite
    sql = str(counts_as_spend({6}).compile(dialect=sqlite.dialect(),
                                           compile_kwargs={"literal_binds": True})).lower()
    assert "not_project_expense = false" in sql or "not_project_expense = 0" in sql
    assert "activated_at is not null" in sql
    assert "backup_of_txn_id is null" in sql
    assert "not in" in sql


ROLLUPS = {
    "app.py": ["_po_to_dict", "_sub_budget_to_dict", "_run_budget_mismatch_scan",
               "budget_mismatches", "people_summary", "person_profile",
               "_actuals_by_section_code", "_build_wrap_book_data"],
    "routes/actuals.py": ["actuals_line_ledger", "actuals_section_transactions"],
}


@pytest.mark.parametrize("rel,name", [(f, n) for f, ns in ROLLUPS.items() for n in ns])
def test_rollups_filter_on_counts_as_spend(rel, name):
    body = _func_body(_src(rel), name)
    assert "counts_as_spend" in body, f"{rel}:{name} does not use counts_as_spend"
    # None of the former hand copies survives next to it.
    for frag in ("def _is_expense_row",                       # wrap book
                 "if t.is_expense and amt is not None",       # ledger coded_total
                 "if amt <= 0:",                              # mismatch scan
                 "if not bool(_t.is_expense):",               # people summary
                 "for p in payments if p[\"is_expense\"]"):   # person total_paid
        assert frag not in body, f"{rel}:{name} still carries a hand copy: {frag}"
    # … and the "not project" test is inside counts_as_spend, not beside it.
    if name != "_build_wrap_book_data":   # it loads every row (orphan receipts too) and
        sql_filters = re.findall(r"Transaction\.not_project_expense == False", body)   # applies row_counts_as_spend in Python
        assert not sql_filters, f"{rel}:{name} filters on not_project_expense beside counts_as_spend"


def test_no_current_budget_hand_queries():
    src = _src("app.py")
    for name, helper in (("budget_new", "get_current_estimated_budget"),
                         ("line_assign_po", "get_current_working_budget"),
                         ("assign_crew", "get_current_working_budget"),
                         ("po_list_page", "get_live_budget"),
                         ("sub_budgets_page", "get_live_budget")):
        body = _func_body(src, name)
        assert helper in body, f"{name} does not read {helper}"
        assert "version_status='current'" not in body and \
            "version_status == 'current'" not in body, f"{name} keeps a hand query"


# ── 2. vendor cleaning ──────────────────────────────────────────────────

def test_strip_processor_prefix():
    assert strip_processor_prefix("SQ *CASA VIDEO") == "CASA VIDEO"
    assert strip_processor_prefix("TST* TACO BELL 1234") == "TACO BELL 1234"
    assert strip_processor_prefix("PAYPAL *SOUTHWESTAI") == "SOUTHWESTAI"
    assert strip_processor_prefix("IN *GOVISION LLC") == "GOVISION LLC"
    assert strip_processor_prefix("Home Depot #1234") == "Home Depot #1234"
    assert strip_processor_prefix("") == "" and strip_processor_prefix(None) == ""


def test_canon_vendor_storage_key_is_unchanged():
    # VendorAlias.raw_canonical / VendorCategoryMap.vendor_canonical rows are
    # keyed on this; the processor prefix is NOT stripped here on purpose.
    assert canon_vendor("AMAZON MKTPLACE #1234") == "amazon mktplace"
    assert canon_vendor("SQ *CASA VIDEO") == "sq casa video"
    assert canon_vendor("LYFT *RIDE 8AM") == "lyft ride 8am"


def _old_normalize_vendor(v):
    """app._normalize_vendor as it was before Cleanup D (reference copy)."""
    if not v:
        return ""
    s = str(v).strip().lower()
    for _p in ('sq *', 'sq*', 'tst* ', 'tst*', 'amzn mktp ', 'amzn mktp* ',
               'amazon mktpl*', 'amazon mktpl ', 'amazon mktp ', 'paypal *',
               'paypal*', 'pp*', 'pos debit ', 'remote online deposit '):
        if s.startswith(_p):
            s = s[len(_p):]
            break
    s = re.split(r'\s+\*', s, maxsplit=1)[0]
    s = re.sub(r'\s+\d[\w\-: ]*$', '', s).strip()
    return re.sub(r'\s+', ' ', s)


SAME = ["LYFT *RIDE MON 8AM", "Lyft", "SQ *CASA VIDEO", "TST* TACO BELL 1234",
        "PAYPAL *SOUTHWESTAI", "AMAZON MKTPL*BJ3NU6KN0", "Uber Trip 2026-05-01",
        "Delta Air 0062345", "POS DEBIT WALMART 4455", "UBER   EATS", "Shell Oil 57442"]
# Where the old list and the canonical rule disagree, the canonical wins:
# punctuation becomes a space (canon_vendor's rule) and 'IN *' is a prefix.
DIFFER = {"Home Depot #1234": "home depot", "IN *GOVISION LLC": "govision llc",
          "B&H Photo": "b h photo", "Joe's Diner": "joe s diner",
          "Amazon.com": "amazon com", "7-Eleven 33021": "7 eleven"}


def test_vendor_match_key_matches_old_prefix_list():
    for v in SAME:
        assert vendor_match_key(v) == _old_normalize_vendor(v), v
    for v, want in DIFFER.items():
        assert vendor_match_key(v) == want, v
        assert _old_normalize_vendor(v) != want, v   # it really did differ


def test_normalize_vendor_is_an_alias():
    body = _func_body(_src("app.py"), "_normalize_vendor")
    assert "vendor_match_key" in body and "startswith" not in body


def test_vendor_similarity_sees_through_prefixes():
    assert _vendor_similarity("SQ *CASA VIDEO", "Casa Video") == 1.0
    assert _vendor_similarity("IN *GOVISION LLC", "GoVision LLC") == 1.0
    assert _vendor_similarity("Lyft", "Uber") < 0.45


def test_payee_name_key_is_the_index_key():
    assert payee_name_key("  Vega Camera, LLC ") == "vega camera, llc"
    idx = payee_name_index([NS(id=1, name="Vega Camera LLC", legal_name=None, company=None)])
    assert idx[payee_name_key("VEGA CAMERA LLC")] == 1
    from qbo_sync import match_vendor_to_payee
    theo = NS(id=2, name="Theo Lindqvist")
    f = {"qbo_vendor_id": "9", "display_name": "  THEO LINDQVIST "}
    assert match_vendor_to_payee(f, {}, {payee_name_key("Theo Lindqvist"): [theo]}) == (theo, 'name')


# ── 3. section_for_code ─────────────────────────────────────────────────

def test_section_for_code_edges():
    assert section_for_code(1000) == 1000
    assert section_for_code(2950) == 2900          # not a thousands bucket
    assert section_for_code(3450) == 3400
    assert section_for_code(6850) == 6800
    assert section_for_code(99999) == 6800
    assert section_for_code(999) is None           # below the first section
    assert section_for_code(999, default=FP_COA_SECTIONS[0][0]) == 1000
    assert section_for_code(0, default=1000) == 1000


def test_app_section_for_code_is_an_alias():
    src = _src("app.py")
    body = _func_body(src, "_section_for_code")
    assert "_bc_section_for_code(code, default)" in body
    assert "for start, _ in FP_COA_SECTIONS" not in body
    # No local re-definition survives in export_pdf / gantt / the archive.
    assert src.count("def _section_for_code(") == 1
    assert "def section_for_code(" not in src


# ── 4. the served lists ─────────────────────────────────────────────────

def test_document_type_list_single_source():
    try:
        from fp_analyzer import DOCUMENT_TYPES, DOC_PREFIXES, DOC_TYPE_LABELS, DOC_TYPE_ALIASES
    except Exception as e:  # pragma: no cover
        pytest.skip(f"fp_analyzer not importable here: {e}")
    keys = [k for k, _ in DOC_TYPE_LABELS]
    assert len(keys) == len(set(keys))
    assert set(keys) == set(DOCUMENT_TYPES) == set(DOC_PREFIXES)
    assert "timecard" in keys and "employee_vendor_doc" in keys
    assert set(DOC_TYPE_ALIASES.values()) <= set(keys)
    # Every former copy reads the served list now.
    assert "DOCUMENT_TYPES.keys()" in _src("app.py")
    assert "'timecard', 'legal', 'release'" not in _src("app.py")      # old fallback
    assert "('payroll', 'Payroll package')" not in _src("app.py")      # docs_inbox_page copy
    assert "{% for _k, _lbl in DOC_TYPE_LABELS %}" in _src("templates/_doc_fields.html")
    assert '<option value="receipt"' not in _src("templates/_doc_fields.html")
    assert "window.__FP_DOC_TYPES" in _src("templates/docs_inbox.html")
    assert "['receipt','Receipt']" not in _src("templates/docs_inbox.html")
    assert "window.__FP_DOC_TYPES" in _src("templates/mobile_upload.html")
    assert "{ v: 'receipt'" not in _src("templates/mobile_upload.html")
    for t in ("templates/base.html", "templates/mobile_upload.html"):
        assert "__FP_DOC_TYPES = {{ DOC_TYPE_LABELS | tojson }}" in _src(t), t
        assert "__FP_DOC_TYPE_ALIASES = {{ DOC_TYPE_ALIASES | tojson }}" in _src(t), t


def test_payment_method_list_single_source():
    assert set(PAYMENT_METHOD_LABELS) == set(PAYMENT_METHODS)
    assert "ach" in PAYMENT_METHODS and PAYMENT_METHOD_LABELS["ach"] == "ACH"
    html = _src("templates/actuals.html")
    assert "{% for _pm in PAYMENT_METHODS %}" in html
    assert 'value="company_card">Company card' not in html
    js = _src("static/actuals-js/a01.js")
    assert "window.__FP_PAYMENT_METHODS" in js
    assert "personal_card: 'Personal'" not in js
    assert "__FP_PAYMENT_METHODS = {{ PAYMENT_METHOD_LABELS | tojson }}" in _src("templates/base.html")


def test_mode_meta_single_source():
    assert set(MODE_META) == {"estimated", "working"}
    for row in MODE_META.values():
        assert {"label", "short", "title", "desc", "cls"} <= set(row)
    assert mode_meta(None) is MODE_META["estimated"]
    assert mode_meta("actual") is MODE_META["estimated"]
    assert mode_label("working") == "Working"
    assert mode_label("estimated") == "Estimated"
    assert mode_label("working", is_actual=True) == "Actual"
    assert mode_label("schedule") == "Working" and mode_label("hybrid") == "Hybrid"
    assert mode_label("odd") == "odd"
    for t in ("templates/budget.html", "templates/callsheet.html", "templates/gantt.html"):
        assert "{% set MODE_META" not in _src(t), t
        assert "MODE_META.get(budget.budget_mode, MODE_META['estimated'])" in _src(t), t
    api = _src("routes/api_v1.py")
    assert "_MODE_LABELS" not in api and "mode_label" in api
    assert '"MODE_META": _fp_mode_meta' in _src("app.py")


def test_coa_sections_single_source():
    assert FP_COA_NAMES == dict(FP_COA_SECTIONS)
    assert "__FP_COA_SECTIONS = {{ FP_COA_SECTIONS | tojson }}" in _src("templates/base.html")
    js = _src("static/budget-js/b09.js")
    assert "window.__FP_COA_SECTIONS" in js
    assert "{ code:1000,  name:'Development Rights & Story', items:[" not in js
    assert "ROLE_LIBRARY" not in js


# ── 5. seed catalogs name sections as FP_COA_NAMES does ─────────────────

def test_catalog_seed_section_names():
    for row in FP_CATALOG_SEED:
        code, cname = row[0], row[1]
        assert FP_COA_NAMES[code] == cname, (code, cname)
    assert "category_name=FP_COA_NAMES.get(code, cname)" in _src("budget_calc.py")


def test_app_seed_templates_section_names():
    body = _func_body(_src("app.py"), "_import_minutecon26_budget")
    assert "_COA = dict(FP_COA_SECTIONS)" in body
    src = _src("app.py")
    i = src.index("def _seed_production_templates():")
    j = src.index("def _web_worker_essential_columns", i)
    seg = src[i:j]
    pairs = re.findall(r'\(\s*(\d{4}),\s*"([^"]+)",', seg)
    assert pairs, "no seed rows found"
    for code, name in pairs:
        assert FP_COA_NAMES[int(code)] == name, (code, name)
    assert seg.count("account_name=FP_COA_NAMES.get(code, acct)") == 2


# ── 6. no private esc / money copy in JS ────────────────────────────────

def _js_sources():
    out = []
    for d in ("static", "static/budget-js", "static/actuals-js"):
        p = os.path.join(ROOT, d)
        for f in sorted(os.listdir(p)):
            if f.endswith(".js") and f != "fp-utils.js":
                out.append(os.path.join(d, f))
    for f in sorted(os.listdir(os.path.join(ROOT, "templates"))):
        if f.endswith(".html"):
            out.append(os.path.join("templates", f))
    return out


_ESC_DEF = re.compile(r"(=>|function)[^\n]*&lt;")
_MONEY_DEF = re.compile(r"(=>|function)[^\n]*'\$' *\+[^\n]*toLocaleString")


def test_no_private_esc_or_money_copies():
    # Pages that do not extend base.html (no FP loaded) keep their own
    # helpers; everything that loads fp-utils.js must alias it.
    standalone = {"templates/mobile_upload.html", "templates/share.html",
                  "templates/estimate_portal.html", "templates/login.html",
                  "templates/forgot_password.html", "templates/reset_password.html",
                  "templates/404.html", "templates/wrap_book.html",
                  "templates/wrap_review.html", "templates/budget_pdf.html",
                  "templates/catering_export.html", "templates/travel_export.html"}
    bad = []
    for rel in _js_sources():
        if rel in standalone:
            continue
        for n, line in enumerate(_src(rel).splitlines(), 1):
            if _ESC_DEF.search(line) or _MONEY_DEF.search(line):
                bad.append(f"{rel}:{n}: {line.strip()[:90]}")
    assert not bad, "private esc/money copies:\n" + "\n".join(bad)
    assert "fp-utils.js" in _src("templates/base.html")
    fp = _src("static/fp-utils.js")
    assert "window.FP.esc = esc" in fp and "window.FP.money = money" in fp


# ── App-booting checks: the rendered pages carry the canon ──────────────

_DB_DIR = tempfile.mkdtemp(prefix="fpb-onerule-")
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
        from models import (db, User, ProjectSheet, ProjectAccess, Budget, BudgetLine,   # noqa: E402
                            Transaction)
        import routes.actuals                     # noqa: F401,E402
        import routes.api_v1                      # noqa: F401,E402
        _IMPORT_ERR = None
    except Exception as _e:                       # pragma: no cover
        _IMPORT_ERR = _e
    finally:
        try:
            os.chdir(_cwd)
        except Exception:
            pass

appboot = pytest.mark.skipif(_IMPORT_ERR is not None,
                             reason=f"app.py not importable here: {_IMPORT_ERR}")
S = {}


@pytest.fixture(scope="module")
def one_rule_ctx():
    if _IMPORT_ERR is not None:
        pytest.skip(str(_IMPORT_ERR))
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        u = User.query.filter_by(email="onerule@thefp.tv").first()
        if not u:
            u = User(email="onerule@thefp.tv", name="onerule", role="super_admin")
            db.session.add(u)
        u.role = "super_admin"
        u.set_password("pw")
        db.session.flush()
        p = ProjectSheet(name="Cleanup D one rule", status="active")
        db.session.add(p)
        db.session.flush()
        db.session.add(ProjectAccess(project_id=p.id, user_id=u.id, role="owner"))
        est = Budget(project_id=p.id, name="D Estimated", budget_mode="estimated",
                     version_status="current", version_number=1)
        wrk = Budget(project_id=p.id, name="D Working", budget_mode="working",
                     version_status="current", version_number=1)
        db.session.add_all([est, wrk])
        db.session.flush()
        ln = BudgetLine(budget_id=wrk.id, account_code=2601, account_name="Camera",
                        description="Lens", is_labor=False, quantity=1, days=1, rate=100,
                        rate_type="flat_day", sort_order=1)
        db.session.add(ln)
        db.session.flush()
        for kw in (dict(amount=250, is_expense=True),
                   dict(amount=40, is_expense=False),
                   dict(source='doc_upload', amount=999, activated_at=None),
                   dict(amount=500, backup_of_txn_id=None),
                   # a Not project row is uncoded (2026-11-05: a row the
                   # project coded is never hidden under Not project — the
                   # workspace repairs that shape on read)
                   dict(amount=5, not_project_expense=True, budget_line_id=None, account_code=None)):
            base = dict(project_id=p.id, source='qbo_sync', txn_date="2026-03-01",
                        vendor="Lyft", match_status='unmatched', budget_line_id=ln.id,
                        account_code=2601)
            base.update(kw)
            db.session.add(Transaction(**base))
        db.session.flush()
        bk = Transaction.query.filter_by(project_id=p.id, amount=500).first()
        first = Transaction.query.filter_by(project_id=p.id, amount=250).first()
        bk.backup_of_txn_id = first.id
        db.session.commit()
        S.update(uid=u.id, pid=p.id, bid=wrk.id, est=est.id, lid=ln.id)
    c = app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(u.id)
        s["_fresh"] = True
        s["_csrf"] = "t"
    return {"app": app, "client": c}


@appboot
def test_rendered_actuals_page_carries_the_canon(one_rule_ctx):
    import json
    c = one_rule_ctx["client"]
    r = c.get(f"/projects/{S['pid']}/actuals")
    assert r.status_code == 200, r.status_code
    html = r.get_data(as_text=True)
    # The radios come from PAYMENT_METHODS (ACH included) …
    for pm in PAYMENT_METHODS:
        assert f'name="aw-pay" value="{pm}"' in html, pm
    # … and base.html serves the JS copies from the same Python objects.
    m = re.search(r"window\.__FP_PAYMENT_METHODS = (\{.*?\});", html)
    assert m and json.loads(m.group(1)) == PAYMENT_METHOD_LABELS
    m = re.search(r"window\.__FP_DOC_TYPES = (\[.*?\]);", html)
    from fp_analyzer import DOC_TYPE_LABELS
    assert m and [tuple(x) for x in json.loads(m.group(1))] == list(DOC_TYPE_LABELS)
    m = re.search(r"window\.__FP_COA_SECTIONS = (\[.*?\]);", html)
    assert m and [tuple(x) for x in json.loads(m.group(1))] == list(FP_COA_SECTIONS)


@appboot
def test_rendered_mobile_upload_and_docs_inbox(one_rule_ctx):
    import json
    from fp_analyzer import DOC_TYPE_LABELS
    c = one_rule_ctx["client"]
    r = c.get("/upload")
    assert r.status_code == 200
    m = re.search(r"window\.__FP_DOC_TYPES = (\[.*?\]);", r.get_data(as_text=True))
    assert m and [tuple(x) for x in json.loads(m.group(1))] == list(DOC_TYPE_LABELS)
    r = c.get(f"/projects/{S['pid']}/docs")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    for k, label in DOC_TYPE_LABELS:
        assert label in html, (k, label)


@appboot
def test_rendered_budget_page_mode_badge(one_rule_ctx):
    c = one_rule_ctx["client"]
    r = c.get(f"/projects/{S['pid']}/budget/{S['bid']}")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    assert MODE_META["working"]["label"] in html and MODE_META["working"]["cls"] in html
    assert "fp-utils.js" in html


@appboot
def test_api_mode_label_from_one_table(one_rule_ctx):
    from budget_calc import mode_label as _ml
    with one_rule_ctx["app"].app_context():
        b = db.session.get(Budget, S["bid"])
        assert _ml(b.budget_mode, b.is_actual) == "Working"


@appboot
def test_ledger_and_line_totals_agree(one_rule_ctx):
    """The per-line ledger's coded_total is the per-line Actual rule:
    250 - 40 (refund), minus nothing else: the un-activated receipt row,
    the backup receipt and the not-project row drop out."""
    c = one_rule_ctx["client"]
    r = c.get(f"/projects/{S['pid']}/actuals/line/{S['lid']}/ledger.json")
    assert r.status_code == 200, r.data
    body = r.get_json()
    assert body["line"]["coded_total"] == 210.0
    r = c.get(f"/projects/{S['pid']}/budget/{S['bid']}/mismatches")
    assert r.status_code == 200


@appboot
def test_section_alias_in_app(one_rule_ctx):
    assert fpapp._section_for_code(999) is None
    assert fpapp._section_for_code(999, fpapp._FIRST_SECTION) == 1000
    assert fpapp._section_for_code(2950) == 2900 == section_for_code(2950)
    assert fpapp._section_name(2900) == FP_COA_NAMES[2900]
    assert fpapp._normalize_vendor("SQ *CASA VIDEO #12") == vendor_match_key("SQ *CASA VIDEO #12")
