"""ONE split mechanism + invoices counted twice (2026-11-04) — pure rules.

  1. invoice_children_to_split_rows — a document's itemization becomes the
     charge's transaction_split pieces that sum to the CHARGE (the bank's
     figure): equal, a tip on top (remainder piece / the charge's own
     coding / the largest piece), a short charge, refused when it cannot.
  2. find_counted_twice — (a) a charge split AND itemized; (b) a coded /
     itemized invoice row with no charge + a counted charge with the same
     payee and amount (±$0.01) within 30 days in the same project, incl.
     the "Katherine shape" (one invoice itemized across labor + something
     else while the charge is coded whole) and the GINTS 7246 shape.
  3. validate_splits — a piece names a line OR a section, exactly one.
  4. amount_edit_allowed — the bank's figure is read-only.

Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import (  # noqa: E402
    invoice_children_to_split_rows, find_counted_twice, validate_splits,
    amount_edit_allowed, REMAINDER_NOTE, SPLIT_PARENT_MESSAGE,
)


def _kid(line=None, code=None, amount=0, note=None):
    return {"budget_line_id": line, "account_code": code, "amount": amount, "note": note}


# ── 1. invoice children → split rows ──────────────────────────────────────

def test_children_that_sum_to_the_charge_become_the_pieces():
    rows, err = invoice_children_to_split_rows(993.35, [
        _kid(1, amount=600, note="Labor"), _kid(2, amount=393.35, note="Parking")])
    assert err is None
    assert [(r["budget_line_id"], r["amount"], r["note"]) for r in rows] == [
        (1, 600.0, "Labor"), (2, 393.35, "Parking")]


def test_a_section_child_stays_a_section_piece():
    rows, err = invoice_children_to_split_rows(500, [_kid(1, amount=300), _kid(None, 4700, 200)])
    assert err is None
    assert rows[1]["budget_line_id"] is None and rows[1]["account_code"] == 4700


def test_tip_on_top_goes_to_the_remainder_piece():
    rows, err = invoice_children_to_split_rows(105, [
        _kid(1, amount=60), _kid(2, amount=40, note=REMAINDER_NOTE)])
    assert err is None
    assert [r["amount"] for r in rows] == [60.0, 45.0]


def test_tip_on_top_goes_to_the_charges_own_coding_when_no_remainder():
    rows, err = invoice_children_to_split_rows(105, [_kid(1, amount=60), _kid(2, amount=40)],
                                               charge_target=('line', 9))
    assert err is None
    assert rows[-1] == {"budget_line_id": 9, "account_code": None, "amount": 5.0,
                        "note": "Difference to the charge"}
    assert round(sum(r["amount"] for r in rows), 2) == 105.0


def test_tip_on_top_with_nothing_else_lands_on_the_largest_piece_noted():
    rows, err = invoice_children_to_split_rows(105, [_kid(1, amount=60), _kid(2, amount=40)])
    assert err is None
    assert rows[0]["amount"] == 65.0 and "+$5.00" in rows[0]["note"]


def test_charge_short_comes_off_the_largest_piece_or_is_refused():
    rows, err = invoice_children_to_split_rows(95, [_kid(1, amount=60), _kid(2, amount=40)])
    assert err is None and [r["amount"] for r in rows] == [55.0, 40.0]
    rows, err = invoice_children_to_split_rows(10, [_kid(1, amount=60), _kid(2, amount=40)])
    assert rows is None and "more than" in err


def test_pieces_on_one_line_with_no_notes_collapse_to_one_coding():
    rows, err = invoice_children_to_split_rows(900, [_kid(5, amount=300), _kid(5, amount=300), _kid(5, amount=300)])
    assert err is None and rows == [{"budget_line_id": 5, "account_code": None, "amount": 900.0, "note": None}]
    # with their own notes they stay three pieces (notes preserved)
    rows, err = invoice_children_to_split_rows(900, [_kid(5, amount=300, note=f"Day {i}") for i in (1, 2, 3)])
    assert err is None and len(rows) == 3


def test_children_without_a_target_are_refused():
    rows, err = invoice_children_to_split_rows(100, [_kid(amount=100)])
    assert rows is None and "no budget line" in err
    rows, err = invoice_children_to_split_rows(100, [])
    assert rows is None


# ── 2. find_counted_twice ─────────────────────────────────────────────────

def _charge(id, vendor="Gotham Sound", amount=217.75, date="2026-10-10", pid=309, **kw):
    base = dict(id=id, project_id=pid, vendor=vendor, amount=amount, txn_date=date,
                doc_upload_id=None, budget_line_id=11, account_code=2700,
                not_project_expense=False, backup_of_txn_id=None)
    base.update(kw)
    return NS(**base)


def _docrow(id, vendor="Gotham Sound", amount=217.75, date="2026-10-08", pid=309, **kw):
    base = dict(id=id, project_id=pid, vendor=vendor, amount=amount, txn_date=date,
                doc_upload_id=900 + id, budget_line_id=12, account_code=2700,
                not_project_expense=False, backup_of_txn_id=None, activated_at="2026-10-09")
    base.update(kw)
    return NS(**base)


def test_b_coded_invoice_row_and_coded_charge_same_payee_and_amount():
    out = find_counted_twice([_charge(4452)], [_docrow(4499)])
    assert [(e["kind"], e["charge"].id, e["doc_row"].id) for e in out] == [("doc_and_charge", 4452, 4499)]


def test_b_matches_the_live_grubhub_candidates():
    charges = [_charge(4452), _charge(4484, vendor="Lyft", amount=88.44, date="2026-09-28"),
               _charge(4443, vendor="Delta Air Lines", amount=668.79, date="2026-09-20")]
    docs = [_docrow(4499), _docrow(4492, vendor="LYFT *RIDE", amount=88.44, date="2026-09-28"),
            _docrow(4442, vendor="Delta", amount=668.79, date="2026-09-19")]
    pairs = {(e["charge"].id, e["doc_row"].id) for e in find_counted_twice(charges, docs)}
    assert (4452, 4499) in pairs and (4484, 4492) in pairs


def test_b_katherine_shape_itemized_invoice_beside_a_charge_coded_whole():
    # One invoice with labor + something else, itemized on its own row (not
    # activated, the children carry it); the charge is coded to one line.
    doc = _docrow(70, vendor="Katherine McClure", amount=1250.00, activated_at=None,
                  budget_line_id=None, account_code=None)
    kids = {70: [NS(id=71), NS(id=72)]}
    ch = _charge(60, vendor="Katherine McClure", amount=1250.00, date="2026-10-20")
    out = find_counted_twice([ch], [doc], kids_by_parent=kids)
    assert [(e["kind"], e["charge"].id) for e in out] == [("doc_and_charge", 60)]


def test_b_gints_7246_shape_split_charge_and_itemized_invoice():
    doc = _docrow(4544, vendor="Parking Pros", amount=993.35, activated_at=None,
                  budget_line_id=None, account_code=None, pid=300)
    ch = _charge(7246, vendor="Parking Pros", amount=993.35, pid=300, budget_line_id=None, account_code=None)
    out = find_counted_twice([ch], [doc], splits_by_txn={7246: [NS(), NS()]}, kids_by_parent={4544: [NS(), NS(), NS()]})
    assert [e["kind"] for e in out] == ["doc_and_charge"]


def test_b_is_not_triggered_by_near_misses():
    base = _charge(1)
    assert not find_counted_twice([base], [_docrow(2, vendor="Other Vendor")])
    assert not find_counted_twice([base], [_docrow(2, amount=217.77)])          # 2 cents off
    assert find_counted_twice([base], [_docrow(2, amount=217.76)])              # 1 cent is fine
    assert not find_counted_twice([base], [_docrow(2, date="2026-08-01")])      # > 30 days
    assert not find_counted_twice([base], [_docrow(2, pid=999)])               # other project
    assert not find_counted_twice([_charge(1, doc_upload_id=55)], [_docrow(2)])  # another receipt
    assert not find_counted_twice([_charge(1, budget_line_id=None, account_code=None)], [_docrow(2)])  # charge not counted
    assert not find_counted_twice([base], [_docrow(2, activated_at=None)])     # a plain receipt
    assert not find_counted_twice([base], [_docrow(2, not_project_expense=True)])


def test_b_one_invoice_pairs_with_one_charge_the_closest():
    out = find_counted_twice([_charge(1, date="2026-10-01"), _charge(2, date="2026-10-07")], [_docrow(3)])
    assert [(e["charge"].id, e["doc_row"].id) for e in out] == [(2, 3)]


def test_a_split_and_itemized_charge():
    ch = _charge(5, budget_line_id=None, account_code=None)
    out = find_counted_twice([ch], [], splits_by_txn={5: [NS()]}, kids_by_parent={5: [NS()]})
    assert [e["kind"] for e in out] == ["split_and_itemized"]
    assert not find_counted_twice([ch], [], splits_by_txn={5: [NS()]})


# ── 3. validate_splits with section pieces ────────────────────────────────

def test_a_split_piece_may_target_a_section():
    clean, err = validate_splits(100, [{"budget_line_id": 1, "amount": 60},
                                       {"account_code": 4700, "amount": 40}], {1}, {4700})
    assert err is None
    assert clean[1] == {"budget_line_id": None, "account_code": 4700, "amount": 40.0, "note": None}
    clean, err = validate_splits(100, [{"budget_line_id": 1, "amount": 60},
                                       {"budget_line_id": "section:4700", "amount": 40}], {1})
    assert err is None and clean[1]["account_code"] == 4700


def test_a_piece_names_exactly_one_target():
    _, err = validate_splits(100, [{"budget_line_id": 1, "account_code": 4700, "amount": 60},
                                   {"budget_line_id": 2, "amount": 40}])
    assert "not both" in err["error"]
    _, err = validate_splits(100, [{"amount": 60}, {"budget_line_id": 2, "amount": 40}])
    assert "no budget line" in err["error"]
    _, err = validate_splits(100, [{"account_code": 9999, "amount": 60}, {"budget_line_id": 2, "amount": 40}],
                             None, {4700})
    assert "section 9999" in err["error"]


# ── 4. amount edit guard + the one message ────────────────────────────────

def test_amount_edit_guard():
    ok, why = amount_edit_allowed(NS(source='qbo_sync', amount=50), 60, [])
    assert not ok and "bank" in why
    ok, why = amount_edit_allowed(NS(source='manual_entry', amount=50), 60, [])
    assert ok
    ok, why = amount_edit_allowed(NS(source='doc_upload', amount=50), 60, [25, 25])
    assert not ok and "split" in why
    assert amount_edit_allowed(NS(source='reconciled', amount=50), 50, [])[0]   # no-op edit


def test_split_parent_message():
    assert SPLIT_PARENT_MESSAGE == "This charge is split — edit the split instead."
