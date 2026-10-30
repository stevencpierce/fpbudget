"""Document-class rule (DOCUMENTS_HANDOFF Phase 0, 2026-10-30).

Pure unit tests — no Flask, no DB. `actuals.doc_class(category)` is the ONE
rule for which document types create a ledger Transaction; it replaced five
hand-copied non-ledger lists that disagreed with each other. These tests pin
every category to its class so a drift in any one list shows up here.

Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import (  # noqa: E402
    doc_class, is_ledger_category, is_non_ledger_category, non_ledger_categories,
    EVIDENCE_CATEGORIES, PAPERWORK_CATEGORIES, COMMITMENT_CATEGORIES,
    NON_LEDGER_CATEGORIES,
)


# ── The three classes ──────────────────────────────────────────────────────

def test_evidence_categories():
    for c in ("receipt", "invoice", "payroll", "timecard"):
        assert doc_class(c) == "evidence", c
        assert is_ledger_category(c), c
        assert not is_non_ledger_category(c), c


def test_paperwork_categories():
    for c in ("tax_form", "employee_vendor_doc", "contract", "insurance",
              "release", "legal", "misc"):
        assert doc_class(c) == "paperwork", c
        assert not is_ledger_category(c), c
        assert is_non_ledger_category(c), c


def test_commitment_categories():
    for c in ("estimate", "quote", "purchase_order"):
        assert doc_class(c) == "commitment", c
        assert not is_ledger_category(c), c
        assert is_non_ledger_category(c), c


def test_unknown_and_blank_are_evidence():
    # An unclassified upload behaves like a receipt until someone types it —
    # a receipt-shaped row is safer than a silently dropped one.
    for c in (None, "", "   ", "something_new"):
        assert doc_class(c) == "evidence", repr(c)
        assert is_ledger_category(c), repr(c)


def test_case_and_whitespace_tolerant():
    assert doc_class("  Tax_Form ") == "paperwork"
    assert doc_class("ESTIMATE") == "commitment"
    assert doc_class("Receipt") == "evidence"


# ── The sets agree with the function ───────────────────────────────────────

def test_sets_are_disjoint_and_complete():
    assert not (EVIDENCE_CATEGORIES & PAPERWORK_CATEGORIES)
    assert not (EVIDENCE_CATEGORIES & COMMITMENT_CATEGORIES)
    assert not (PAPERWORK_CATEGORIES & COMMITMENT_CATEGORIES)
    assert NON_LEDGER_CATEGORIES == PAPERWORK_CATEGORIES | COMMITMENT_CATEGORIES
    # Every known UI category is accounted for by exactly one class.
    all_ui = {"receipt", "invoice", "estimate", "contract", "purchase_order",
              "insurance", "tax_form", "payroll", "timecard", "legal",
              "release", "employee_vendor_doc", "misc", "quote"}
    assert all_ui == EVIDENCE_CATEGORIES | PAPERWORK_CATEGORIES | COMMITMENT_CATEGORIES


def test_non_ledger_categories_list_is_sorted_and_json_safe():
    lst = non_ledger_categories()
    assert lst == sorted(lst)
    assert set(lst) == NON_LEDGER_CATEGORIES
    assert all(isinstance(c, str) for c in lst)
    # The five old lists' worst omissions must be present.
    assert "employee_vendor_doc" in lst   # _NON_LEDGER_DOC_CATEGORIES omitted it
    assert "estimate" in lst and "purchase_order" in lst   # admin reconcile omitted these


# ── The decisions the old lists got wrong ──────────────────────────────────

def test_admin_reconcile_never_creates_txn_for_estimates_or_pos():
    # The admin Dropbox reconcile path used a list without estimate / quote /
    # purchase_order, so it created ledger rows for commitments. The one rule
    # must say "skip" for them.
    for u in (NS(category="estimate"), NS(category="quote"), NS(category="purchase_order")):
        assert is_non_ledger_category(u.category)


def test_timecard_is_evidence_and_restores_with_a_row():
    # Restore-from-Trash recreates a row for ledger types; it used to list
    # receipt + invoice only. Payroll and timecard are evidence too.
    for u in (NS(category="payroll"), NS(category="timecard")):
        assert is_ledger_category(u.category)


def test_fp_analyzer_knows_timecard():
    # Bulk Set type → Timecard returned 400 and filed into MISC/DOC because
    # fp_analyzer's tables lacked the key. Import lazily: fp_analyzer pulls in
    # optional SDKs that may be absent in a bare test env.
    try:
        from fp_analyzer import DOCUMENT_TYPES, DOC_PREFIXES
    except Exception as e:  # pragma: no cover — env without the SDKs
        import pytest
        pytest.skip(f"fp_analyzer not importable here: {e}")
    assert DOCUMENT_TYPES["timecard"].endswith("TIMECARDS")
    assert DOC_PREFIXES["timecard"] == "TIMECARD"
    # Every category the classifier knows is covered by the doc_class rule.
    for c in DOCUMENT_TYPES:
        assert doc_class(c) in ("evidence", "paperwork", "commitment"), c


# ── Delete / retype rules, as pure decisions on stub rows ──────────────────

def _retype_decision(txns):
    """What docs_upload_update does on a change to a non-ledger category:
    the doc's OWN rows (source='doc_upload') go, every other row is unlinked.
    Returns (ids_to_delete, ids_to_unlink)."""
    delete = [t.id for t in txns if t.source == "doc_upload"]
    unlink = [t.id for t in txns if t.source != "doc_upload"]
    return delete, unlink


def test_retype_to_non_ledger_keeps_bank_rows():
    txns = [NS(id=1, source="doc_upload"),
            NS(id=2, source="qbo_sync"),
            NS(id=3, source="csv_import"),
            NS(id=4, source="reconciled")]
    delete, unlink = _retype_decision(txns)
    assert delete == [1]
    assert unlink == [2, 3, 4]


def test_inbox_duplicate_state_split():
    """Pending duplicates (is_duplicate=True, status != 'duplicate') land in
    needs_review; only confirmed ones (status='duplicate') are buried."""
    def state(d):
        dup_pending = bool(d.is_duplicate) and d.status != "duplicate"
        if d.status == "duplicate":
            return "duplicate"
        if d.status == "review" or dup_pending:
            return "needs_review"
        return "done"
    assert state(NS(is_duplicate=True, status="duplicate")) == "duplicate"
    assert state(NS(is_duplicate=True, status="done")) == "needs_review"
    assert state(NS(is_duplicate=False, status="review")) == "needs_review"
    assert state(NS(is_duplicate=False, status="done")) == "done"
