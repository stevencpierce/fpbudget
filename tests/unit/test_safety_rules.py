"""Cleanup A — safety rules (2026-11-02).

Pure unit tests — no Flask, no DB, no network. Pins the predicates behind
the data-loss fixes so the rule survives independently of the route that
applies it:

  • doc_edit_writes_through_to — a receipt edit only rewrites the doc's OWN
    ledger row, never a matched bank charge
  • amount_edit_allowed — bank-sourced amounts are read-only; a split row's
    amount must still equal its split sum
  • is_duplicate_twin — "delete duplicate" needs a surviving twin
  • review_resolve_allowed / anomaly_flag_txn_ids — the review queue can
    only touch the charge its flag is about, and never deletes a bank row
  • budget_lock_denial — the lock applies to delete as well as upsert
  • signed credits — the two totals that used to count credits wrong
    (wrap-book export: $0; "Code expenses" buckets: positive) now use
    signed_amount_value

Run: python -m pytest tests/unit/test_safety_rules.py -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import (  # noqa: E402
    BANK_SOURCES, amount_edit_allowed, anomaly_flag_txn_ids, budget_lock_denial,
    doc_edit_writes_through_to, is_bank_sourced, is_duplicate_twin,
    review_resolve_allowed, signed_amount_value,
)


def _txn(**kw):
    base = dict(id=1, source="qbo_sync", amount=100.0, is_expense=True,
                vendor="Lyft", txn_date="2026-10-01", card_last4="4821")
    base.update(kw)
    return NS(**base)


# ── 1. Receipt edit write-through ────────────────────────────────────────
def test_doc_edit_only_touches_the_docs_own_row():
    assert doc_edit_writes_through_to(_txn(source="doc_upload")) is True
    for src in BANK_SOURCES:
        assert doc_edit_writes_through_to(_txn(source=src)) is False, src
    assert doc_edit_writes_through_to(_txn(source="manual_entry")) is False
    assert doc_edit_writes_through_to(_txn(source="invoice_split")) is False


def test_bank_sources_are_the_three_import_paths():
    assert set(BANK_SOURCES) == {"qbo_sync", "csv_import", "reconciled"}
    assert is_bank_sourced(_txn(source="reconciled"))
    assert not is_bank_sourced(_txn(source="doc_upload"))


# ── 16. Amount edits ─────────────────────────────────────────────────────
def test_amount_edit_refused_on_bank_rows():
    ok, why = amount_edit_allowed(_txn(source="qbo_sync", amount=100), 101, [])
    assert ok is False and "bank" in why.lower()


def test_amount_edit_noop_always_allowed_even_on_bank_rows():
    ok, _ = amount_edit_allowed(_txn(source="qbo_sync", amount=100), "100.00", [])
    assert ok is True


def test_amount_edit_with_splits_must_match_split_sum():
    t = _txn(source="manual_entry", amount=100)
    ok, why = amount_edit_allowed(t, 90, [60, 40])
    assert ok is False and "split" in why.lower()
    ok, _ = amount_edit_allowed(t, 100.004, [60, 40])     # rounds to the cent
    assert ok is True
    ok, _ = amount_edit_allowed(t, 120, [])              # no splits → free
    assert ok is True


def test_amount_edit_rejects_non_numbers():
    ok, why = amount_edit_allowed(_txn(source="manual_entry"), "abc", [])
    assert ok is False and "number" in why


# ── 7. Delete-duplicate needs a twin ─────────────────────────────────────
def test_twin_requires_same_amount_date_and_vendor_or_card():
    a = _txn(id=1, amount=37.93, vendor="Lyft", txn_date="2026-10-01", card_last4="4821")
    # Phase 5: the vendor side is vendor_match_key, so "LYFT *ride" IS Lyft;
    # a different vendor on a different card is not a twin.
    assert is_duplicate_twin(a, _txn(id=2, amount=37.93, vendor="Uber", txn_date="2026-10-01", card_last4=None)) is False
    assert is_duplicate_twin(a, _txn(id=2, amount=37.93, vendor="LYFT *ride", txn_date="2026-10-01", card_last4=None)) is True
    assert is_duplicate_twin(a, _txn(id=2, amount=37.93, vendor="Lyft", txn_date="2026-10-01", card_last4=None)) is True
    assert is_duplicate_twin(a, _txn(id=2, amount=-37.93, vendor="Other", txn_date="2026-10-01", card_last4="4821")) is True
    assert is_duplicate_twin(a, _txn(id=2, amount=37.94, vendor="Lyft", txn_date="2026-10-01")) is False
    assert is_duplicate_twin(a, _txn(id=2, amount=37.93, vendor="Lyft", txn_date="2026-10-02")) is False
    assert is_duplicate_twin(a, _txn(id=1, amount=37.93, vendor="Lyft", txn_date="2026-10-01")) is False  # itself
    assert is_duplicate_twin(a, None) is False


def test_twin_never_matches_zero_amounts():
    a = _txn(id=1, amount=0, vendor="Lyft")
    assert is_duplicate_twin(a, _txn(id=2, amount=0, vendor="Lyft")) is False


# ── 4. Review-queue resolve ──────────────────────────────────────────────
def test_review_resolve_only_touches_the_flags_transaction():
    ok, why = review_resolve_allowed("uncode", _txn(id=9), {1, 2})
    assert ok is False and "review item" in why
    ok, _ = review_resolve_allowed("uncode", _txn(id=2), {1, 2})
    assert ok is True


def test_review_resolve_never_deletes_a_bank_row():
    ok, why = review_resolve_allowed("delete_txn", _txn(id=1, source="qbo_sync"), {1})
    assert ok is False and "bank" in why.lower()
    ok, _ = review_resolve_allowed("delete_txn", _txn(id=1, source="doc_upload"), {1})
    assert ok is True
    ok, _ = review_resolve_allowed("mark_not_project", _txn(id=1, source="qbo_sync"), {1})
    assert ok is True


def test_anomaly_flag_txn_ids_collects_column_and_payload():
    f = NS(transaction_id=5)
    assert anomaly_flag_txn_ids(f, {}) == {5}
    assert anomaly_flag_txn_ids(f, {"transaction_id": "6"}) == {5, 6}
    assert anomaly_flag_txn_ids(NS(transaction_id=None),
                                {"members": [{"tid": 7}, {"tid": "8"}, {"x": 1}]}) == {7, 8}
    assert anomaly_flag_txn_ids(NS(transaction_id=None), None) == set()


# ── 10. Lock applies to delete too ───────────────────────────────────────
def test_lock_denial_reads_budget_then_anchor():
    assert budget_lock_denial(NS(locked=True), None)
    assert budget_lock_denial(NS(locked=False), NS(locked=True))
    assert budget_lock_denial(NS(locked=False), NS(locked=False)) is None
    assert budget_lock_denial(NS(), None) is None
    assert budget_lock_denial(None, None) is None


# ── 17. Credits in the two totals ────────────────────────────────────────
def _wrapbook_amt(t):
    """The wrap-book export's per-row amount after Cleanup A."""
    return signed_amount_value(t.amount, t.is_expense)


def test_wrapbook_and_code_bucket_totals_net_credits():
    rows = [_txn(amount=100, is_expense=True), _txn(amount=25, is_expense=False)]
    # before: credits were $0 in the wrap-book (100) and +25 in the buckets (125)
    assert sum(_wrapbook_amt(t) for t in rows) == 75.0
    bucket_total = 0.0
    for t in rows:
        bucket_total += signed_amount_value(t.amount, t.is_expense)
    assert bucket_total == 75.0
