"""Actuals Phase 3 pure helpers (ACTUALS_HANDOFF, 2026-11-01).

  1. build_line_picker_options — the ordered line-picker structure the
     workspace, its Split selects and its Create-expense combobox share:
     headers / spacers dropped, children under their parent, orphans kept,
     the fringe summary last and marked kind='fringe'.
  2. admin_pool_csv — the company-ledger export: header row, the page's
     status labels (unclaimed / claimed / not project / conflict), the
     account column, amounts to the cent.

No Flask, no DB. Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import csv
import io
import os
import sys
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import build_line_picker_options, admin_pool_csv, ADMIN_POOL_CSV_COLUMNS  # noqa: E402


def _ln(id, code, desc, parent=None, tag=None, name="Section"):
    return NS(id=id, account_code=code, description=desc, account_name=name,
              parent_line_id=parent, line_tag=tag)


def test_picker_drops_headers_and_spacers_and_orders_children_under_parents():
    groups = [{"code": 2000, "name": "Production Staff", "lines": [
        _ln(1, 2000, "Department", tag="header"),
        _ln(2, 2001, "Director of Photography"),
        _ln(3, 2003, "Kit fee", parent=2),
        _ln(4, 2002, "Camera Operator"),
        _ln(5, 2000, None, tag="spacer"),
    ]}]
    out = build_line_picker_options(groups)
    assert [g["code"] for g in out] == [2000]
    opts = out[0]["opts"]
    assert [(o["id"], o["kind"]) for o in opts] == [(2, "top"), (3, "child"), (4, "top")]
    assert opts[0]["label"] == "2001 · Director of Photography"
    assert opts[1]["parent_id"] == 2


def test_picker_puts_fringe_last_as_read_only_and_keeps_orphans():
    groups = [{"code": 2000, "name": "Production Staff", "lines": [
        _ln(9, 2099, "Payroll Fringes", tag="fringe_rollup"),
        _ln(7, 2010, "Agent fee", parent=999),      # parent is on another section
        _ln(2, 2001, "Director of Photography"),
    ]}]
    opts = build_line_picker_options(groups)[0]["opts"]
    assert [(o["id"], o["kind"]) for o in opts] == [(2, "top"), (7, "child"), (9, "fringe")]
    assert opts[-1]["label"] == "Payroll Fringes"      # no code prefix on the fringe row


def test_picker_skips_empty_groups_and_accepts_namespace_groups():
    groups = [NS(code=2600, name="Camera", lines=[_ln(1, 2601, "x", tag="header")]),
              NS(code=3400, name="Transportation", lines=[_ln(2, 3401, "Rideshare")])]
    out = build_line_picker_options(groups)
    assert [g["code"] for g in out] == [3400]
    assert out[0]["name"] == "Transportation"
    assert build_line_picker_options([]) == []
    assert build_line_picker_options(None) == []


def test_picker_truncates_long_descriptions_and_falls_back_to_account_name():
    long = "x" * 80
    groups = [{"code": 2000, "name": "S", "lines": [_ln(1, 2001, long), _ln(2, 2002, None, name="Account Name")]}]
    opts = build_line_picker_options(groups)[0]["opts"]
    assert opts[0]["label"] == "2001 · " + "x" * 60
    assert opts[1]["label"] == "2002 · Account Name"


def _row(**kw):
    base = {"id": 7, "date": "2026-10-22", "vendor": "Lensrentals", "amount": 912.0,
            "card_last4": "4417", "account": "••4417", "source": "qbo_sync", "project": None,
            "project_id": None, "line": None, "backup": "none", "excluded": False,
            "claim_conflict": False,
            "qbo_txn_id": "18822", "qbo_txn_type": "Purchase", "payment_method": None}
    base.update(kw)
    return base


def test_csv_header_and_status_labels():
    text = admin_pool_csv([
        _row(),
        _row(id=8, project="Ventana", project_id=3, line="2601 · Camera package", backup="backup"),
        _row(id=9, excluded=True, amount=-120.0, source="qbo_sync"),
        _row(id=10, project="GINTS", project_id=4, claim_conflict=True),
    ])
    rows = list(csv.reader(io.StringIO(text)))
    assert rows[0] == list(ADMIN_POOL_CSV_COLUMNS)
    assert len(rows) == 5
    by_id = {r[-1]: r for r in rows[1:]}
    assert by_id["7"][9] == "unclaimed" and by_id["7"][4] == "••4417"
    assert by_id["8"][9] == "claimed" and by_id["8"][6] == "Ventana" and by_id["8"][7] == "2601 · Camera package"
    assert by_id["9"][9] == "not project" and by_id["9"][2] == "-120.00"
    assert by_id["10"][9] == "conflict"


def test_csv_amounts_to_the_cent_and_blanks_for_missing():
    text = admin_pool_csv([_row(amount=48.2, card_last4=None, account="", qbo_txn_id=None)])
    r = list(csv.reader(io.StringIO(text)))[1]
    assert r[2] == "48.20" and r[3] == "" and r[4] == "" and r[10] == ""
    assert admin_pool_csv([]).strip() == ",".join(ADMIN_POOL_CSV_COLUMNS)
    assert admin_pool_csv(None).count("\n") == 1


def test_csv_escapes_commas_and_quotes_in_vendor():
    text = admin_pool_csv([_row(vendor='Joe\'s "Diner", Inc.')])
    r = list(csv.reader(io.StringIO(text)))[1]
    assert r[1] == 'Joe\'s "Diner", Inc.'
