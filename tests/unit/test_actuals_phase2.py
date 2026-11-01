"""Actuals Phase 1-2 rules (ACTUALS_HANDOFF, 2026-10-30).

Pure unit tests — no Flask, no DB, no network. Four rules are pinned:

  1. validate_splits — a split must name ≥2 lines of the current Working,
     every amount non-zero with the transaction's sign, and the amounts must
     sum to the transaction amount TO THE CENT (else the delta is reported).
  2. validate_manual_expense — vendor / amount / date / payment_method
     rules, reason required for cash.
  3. compute_row_flags — the red / yellow flags the workspace computes
     server-side.
  4. group_pool_rows — one row per qbo_txn_id, the claiming sibling wins.

Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import (  # noqa: E402
    validate_splits, validate_manual_expense, compute_row_flags,
    group_pool_rows, PAYMENT_METHODS, REASON_KIND_TO_PAYMENT_METHOD, _cents,
)


# ── _cents: money arithmetic without float noise ───────────────────────────

def test_cents_rounds_half_up_and_keeps_sign():
    assert _cents(10) == 1000
    assert _cents("10.005") == 1001
    assert _cents(0.1 + 0.2) == 30          # 0.30000000000000004 → 30
    assert _cents(-37.93) == -3793
    assert _cents(None) == 0


# ── validate_splits ────────────────────────────────────────────────────────

def _splits(*pairs, notes=None):
    return [{"budget_line_id": lid, "amount": amt} for lid, amt in pairs]


def test_split_sums_to_the_cent():
    clean, err = validate_splits(100.00, _splits((1, 60), (2, 40)), {1, 2})
    assert err is None
    assert [(r["budget_line_id"], r["amount"]) for r in clean] == [(1, 60.0), (2, 40.0)]


def test_split_short_by_a_cent_is_refused_with_delta():
    clean, err = validate_splits(100.00, _splits((1, 60), (2, 39.99)), {1, 2})
    assert clean is None
    assert err["delta"] == -0.01
    assert "short by $0.01" in err["error"]


def test_split_over_is_refused_with_positive_delta():
    _, err = validate_splits(100.00, _splits((1, 60), (2, 40.50)))
    assert err["delta"] == 0.5
    assert "over by $0.50" in err["error"]


def test_split_float_noise_does_not_break_the_sum():
    # 33.33 + 33.33 + 33.34 in floats is 99.99999999999999 or 100.00000000000001
    clean, err = validate_splits(100.00, _splits((1, 33.33), (2, 33.33), (3, 33.34)))
    assert err is None and len(clean) == 3


def test_split_needs_two_lines():
    _, err = validate_splits(100.00, _splits((1, 100)))
    assert "at least two" in err["error"]
    _, err = validate_splits(100.00, [])
    assert err is not None
    _, err = validate_splits(100.00, None)
    assert err is not None


def test_split_lines_must_be_on_the_working_budget():
    _, err = validate_splits(100.00, _splits((1, 60), (9, 40)), allowed_line_ids={1, 2})
    assert "line #9" in err["error"]
    # No allowed set given → the route has not scoped it; the pure rule passes.
    clean, err = validate_splits(100.00, _splits((1, 60), (9, 40)))
    assert err is None


def test_split_rejects_zero_and_wrong_sign_pieces():
    _, err = validate_splits(100.00, _splits((1, 100), (2, 0)))
    assert "$0" in err["error"]
    _, err = validate_splits(100.00, _splits((1, 120), (2, -20)))
    assert "same sign" in err["error"]


def test_split_of_a_refund_is_negative_pieces():
    clean, err = validate_splits(-50.00, _splits((1, -30), (2, -20)))
    assert err is None
    assert sum(r["amount"] for r in clean) == -50.0


def test_split_rejects_bad_rows():
    _, err = validate_splits(100.00, [{"budget_line_id": "x", "amount": 50},
                                      {"budget_line_id": 2, "amount": 50}])
    assert "no budget line" in err["error"]
    _, err = validate_splits(100.00, [{"budget_line_id": 1, "amount": "fifty"},
                                      {"budget_line_id": 2, "amount": 50}])
    assert "not a number" in err["error"]
    _, err = validate_splits("abc", _splits((1, 60), (2, 40)))
    assert "Transaction amount" in err["error"]
    _, err = validate_splits(0, _splits((1, 0), (2, 0)))
    assert "$0 transaction" in err["error"]


def test_split_note_is_trimmed_and_capped():
    clean, _ = validate_splits(10, [{"budget_line_id": 1, "amount": 5, "note": "  x" * 200},
                                    {"budget_line_id": 2, "amount": 5, "note": "   "}])
    assert len(clean[0]["note"]) <= 300
    assert clean[1]["note"] is None


# ── validate_manual_expense ────────────────────────────────────────────────

def _body(**kw):
    base = dict(vendor="Taxi", amount=25, txn_date="2026-10-05",
                payment_method="personal_card")
    base.update(kw)
    return base


def test_manual_expense_happy_path():
    clean, err = validate_manual_expense(_body(paid_by_crew_member_id="7",
                                               budget_line_id=3, note=" late ride "))
    assert err is None
    assert clean["vendor"] == "Taxi" and clean["amount"] == 25.0
    assert clean["is_expense"] is True
    assert clean["paid_by_crew_member_id"] == 7 and clean["budget_line_id"] == 3
    assert clean["note"] == "late ride" and clean["reason"] is None


def test_manual_expense_requires_vendor_amount_date_method():
    assert "vendor" in validate_manual_expense(_body(vendor=" "))[1]
    assert "amount" in validate_manual_expense(_body(amount="lots"))[1]
    assert "$0" in validate_manual_expense(_body(amount=0))[1]
    assert "txn_date" in validate_manual_expense(_body(txn_date="10/05/2026"))[1]
    assert "real date" in validate_manual_expense(_body(txn_date="2026-13-40"))[1]
    assert "payment_method" in validate_manual_expense(_body(payment_method=""))[1]
    assert "payment_method" in validate_manual_expense(_body(payment_method="venmo"))[1]


def test_manual_expense_accepts_every_payment_method():
    assert set(PAYMENT_METHODS) == {"company_card", "cash", "personal_card",
                                    "check", "ach", "other"}
    for pm in PAYMENT_METHODS:
        clean, err = validate_manual_expense(_body(payment_method=pm, reason="petty cash"))
        assert err is None and clean["payment_method"] == pm, pm


def test_manual_expense_cash_requires_a_reason():
    _, err = validate_manual_expense(_body(payment_method="cash"))
    assert "reason" in err
    _, err = validate_manual_expense(_body(payment_method="cash", reason="  "))
    assert "reason" in err
    clean, err = validate_manual_expense(_body(payment_method="cash", reason="petty cash"))
    assert err is None and clean["reason"] == "petty cash"
    # Any other method: reason optional.
    assert validate_manual_expense(_body(payment_method="check"))[1] is None


def test_manual_expense_negative_amount_is_a_credit():
    clean, err = validate_manual_expense(_body(amount=-12.5))
    assert err is None
    assert clean["amount"] == 12.5 and clean["is_expense"] is False


def test_manual_expense_optional_ints_validate():
    assert validate_manual_expense(_body(budget_line_id="abc"))[1] == "budget_line_id must be an integer."
    assert validate_manual_expense(_body(paid_by_crew_member_id="x"))[1] == "paid_by_crew_member_id must be an integer."
    clean, _ = validate_manual_expense(_body(budget_line_id="", paid_by_crew_member_id=0))
    assert clean["budget_line_id"] is None and clean["paid_by_crew_member_id"] is None


def test_manual_expense_body_must_be_an_object():
    assert validate_manual_expense(None)[1]
    assert validate_manual_expense([1, 2])[1]


def test_reason_kind_maps_to_payment_method():
    # The older log-expense shape keeps working through this map.
    assert REASON_KIND_TO_PAYMENT_METHOD == {"cash": "cash", "check": "check",
                                            "other_card": "personal_card",
                                            "other": "other"}


# ── compute_row_flags ──────────────────────────────────────────────────────

def _row(**kw):
    base = dict(budget_line_id=None, splits=[], split_sum_ok=None, amount=100.0,
                is_expense=True, has_backup=False, receipt_amount=None,
                receipt_date=None, txn_date="2026-10-01", possible_duplicate=False,
                payment_method=None, not_project_expense=False)
    base.update(kw)
    return base


def test_unassigned_row_is_red_no_line_only():
    assert compute_row_flags(_row()) == {"red": ["no_line"], "yellow": []}


def test_assigned_without_backup_is_red_needs_backup():
    f = compute_row_flags(_row(budget_line_id=5))
    assert f["red"] == ["needs_backup"] and "no_line" not in f["red"]


def test_backup_or_reason_clears_needs_backup():
    assert compute_row_flags(_row(budget_line_id=5, has_backup=True))["red"] == []


def test_split_counts_as_assigned_and_must_sum():
    ok = compute_row_flags(_row(splits=[{"amount": 60}, {"amount": 40}], has_backup=True))
    assert ok["red"] == []
    bad = compute_row_flags(_row(splits=[{"amount": 60}, {"amount": 39.99}], has_backup=True))
    assert bad["red"] == ["split_mismatch"]
    # The caller may pass the verdict directly.
    assert "split_mismatch" in compute_row_flags(_row(splits=[{"amount": 100}],
                                                      split_sum_ok=False, has_backup=True))["red"]


def test_split_flags_work_on_objects_too():
    f = compute_row_flags(NS(**_row(splits=[NS(amount=60), NS(amount=40)], has_backup=True)))
    assert f["red"] == []


def test_assigned_override_counts_section_coding():
    # Section-only coding: no line id, but the caller says it is assigned.
    f = compute_row_flags(_row(assigned=True, has_backup=True))
    assert f["red"] == []
    f = compute_row_flags(_row(assigned=False, budget_line_id=5))
    assert "no_line" in f["red"]


def test_excluded_row_carries_no_red_flags():
    f = compute_row_flags(_row(not_project_expense=True, possible_duplicate=True))
    assert f["red"] == [] and f["yellow"] == ["possible_duplicate"]


def test_yellow_flags():
    f = compute_row_flags(_row(budget_line_id=5, has_backup=True,
                               receipt_amount=98.50, receipt_date="2026-10-20",
                               possible_duplicate=True, payment_method="personal_card"))
    assert f["red"] == []
    assert f["yellow"] == ["possible_duplicate", "amount_mismatch", "date_far", "paid_by_other"]


def test_receipt_within_seven_days_and_same_amount_is_quiet():
    f = compute_row_flags(_row(budget_line_id=5, has_backup=True,
                               receipt_amount=100.0, receipt_date="2026-10-08"))
    assert f["yellow"] == []
    f = compute_row_flags(_row(budget_line_id=5, has_backup=True,
                               receipt_amount=100.0, receipt_date="2026-10-09"))
    assert f["yellow"] == ["date_far"]


def test_paid_by_other_only_for_non_company_methods():
    for pm in ("company_card", "check", "ach", None):
        assert "paid_by_other" not in compute_row_flags(_row(payment_method=pm))["yellow"], pm
    for pm in ("cash", "personal_card", "other"):
        assert "paid_by_other" in compute_row_flags(_row(payment_method=pm))["yellow"], pm


def test_bad_dates_and_amounts_do_not_crash_flags():
    f = compute_row_flags(_row(receipt_date="soon", receipt_amount="n/a"))
    assert f["yellow"] == []


# ── group_pool_rows ────────────────────────────────────────────────────────

def _t(id, qid=None, qtype="Purchase", project_id=1, **kw):
    base = dict(id=id, qbo_txn_id=qid, qbo_txn_type=(qtype if qid else None),
                project_id=project_id, budget_line_id=None, account_code=None,
                has_splits=False)
    base.update(kw)
    return NS(**base)


def test_same_qbo_txn_id_collapses_to_one_row():
    rows = [_t(1, "Q1", project_id=1), _t(2, "Q1", project_id=2), _t(3, "Q2")]
    groups = group_pool_rows(rows)
    assert [(rep.id, [m.id for m in ms]) for rep, ms in groups] == [(1, [1, 2]), (3, [3])]


def test_claiming_sibling_is_the_representative():
    rows = [_t(1, "Q1", project_id=1), _t(2, "Q1", project_id=2, budget_line_id=9)]
    (rep, members), = group_pool_rows(rows)
    assert rep.id == 2 and [m.id for m in members] == [1, 2]
    rows = [_t(1, "Q1"), _t(2, "Q1", has_splits=True)]
    assert group_pool_rows(rows)[0][0].id == 2
    rows = [_t(1, "Q1"), _t(2, "Q1", account_code=4100)]
    assert group_pool_rows(rows)[0][0].id == 2


def test_rows_without_qbo_id_are_their_own_groups():
    rows = [_t(1), _t(2), _t(3, "Q1"), _t(4, "Q1")]
    groups = group_pool_rows(rows)
    assert [rep.id for rep, _ in groups] == [1, 2, 3]
    assert len(groups[2][1]) == 2


def test_qbo_txn_type_is_part_of_the_key():
    # A Purchase and a CreditCardCredit can share an id string in QBO.
    rows = [_t(1, "77", qtype="Purchase"), _t(2, "77", qtype="CreditCardCredit")]
    assert len(group_pool_rows(rows)) == 2


def test_group_order_is_first_seen():
    rows = [_t(5, "B"), _t(6, "A"), _t(7, "B")]
    assert [rep.id for rep, _ in group_pool_rows(rows)] == [5, 6]
