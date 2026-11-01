"""Payee reporting rules (DOCUMENTS_HANDOFF Phase 1, 2026-11-01).

Pure unit tests — no Flask, no DB. Pins the threshold lookup, the one W-9
rule, the 1099 flag matrix, card vs non-card classification, payee
attribution and the shared-line exclusion in actuals.py.

Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from datetime import date
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import (  # noqa: E402
    threshold_for_year, is_card_payment, txn_year, attribute_payee,
    aggregate_paid, w9_status_for, flag_1099, qbo_matches_w9,
    W9_ON_FILE, W9_MISSING, W9_NOT_REQUIRED, W9_UNDER_THRESHOLD, SHARED_LINE,
)


# ── Threshold lookup with fallback ─────────────────────────────────────────

def test_threshold_from_table_rows():
    rows = {2025: 600, 2026: 2000, 2027: 2050}
    assert threshold_for_year(2025, rows) == 600
    assert threshold_for_year(2026, rows) == 2000
    assert threshold_for_year(2027, rows) == 2050


def test_threshold_fallback_when_year_missing():
    assert threshold_for_year(2024, {}) == 600
    assert threshold_for_year(2025, None) == 600
    assert threshold_for_year(2026, {}) == 2000
    assert threshold_for_year(2030, {2025: 600}) == 2000


def test_threshold_tolerates_bad_input():
    assert threshold_for_year("2025", {2025: 600}) == 600
    assert threshold_for_year(None, {}) == 2000
    assert threshold_for_year("abc", {}) == 2000


# ── Card vs non-card ───────────────────────────────────────────────────────

def test_card_classification():
    assert is_card_payment(NS(payment_method='company_card', card_last4=None))
    assert is_card_payment(NS(payment_method='personal_card', card_last4=None))
    assert not is_card_payment(NS(payment_method='check', card_last4='4821'))
    assert not is_card_payment(NS(payment_method='ach', card_last4=None))
    assert not is_card_payment(NS(payment_method='cash', card_last4=None))
    # payment_method unset: a card_last4 marks a synced card charge
    assert is_card_payment(NS(payment_method=None, card_last4='4821'))
    assert not is_card_payment(NS(payment_method=None, card_last4=None))
    assert not is_card_payment(NS(payment_method='', card_last4=''))


def test_txn_year():
    assert txn_year(NS(txn_date='2026-03-04')) == 2026
    assert txn_year(NS(txn_date=None)) is None
    assert txn_year(NS(txn_date='')) is None


# ── Attribution + shared-line exclusion ────────────────────────────────────

DOC_OWNER = {10: 7}                       # doc 10 belongs to payee 7
LINE_OWNERS = {100: {7}, 200: {7, 8}, 300: set()}


def test_doc_owner_wins_over_line():
    t = NS(doc_upload_id=10, budget_line_id=200)
    assert attribute_payee(t, DOC_OWNER, LINE_OWNERS) == 7


def test_single_crew_line_attributes():
    t = NS(doc_upload_id=None, budget_line_id=100)
    assert attribute_payee(t, DOC_OWNER, LINE_OWNERS) == 7


def test_shared_line_attributes_to_nobody():
    t = NS(doc_upload_id=None, budget_line_id=200)
    assert attribute_payee(t, DOC_OWNER, LINE_OWNERS) == SHARED_LINE


def test_unassigned_line_and_unowned_doc_attribute_to_none():
    assert attribute_payee(NS(doc_upload_id=None, budget_line_id=300), DOC_OWNER, LINE_OWNERS) is None
    assert attribute_payee(NS(doc_upload_id=99, budget_line_id=None), DOC_OWNER, LINE_OWNERS) is None
    assert attribute_payee(NS(doc_upload_id=None, budget_line_id=None), DOC_OWNER, LINE_OWNERS) is None


def test_aggregate_never_double_counts_shared_line():
    rows = [
        (7, 2026, 1000.0, False),
        (7, 2026, 250.0, True),
        (7, 2025, 300.0, False),
        (SHARED_LINE, 2026, 900.0, False),
        (None, 2026, 5.0, False),          # dropped
        (7, 2026, -100.0, False),          # a credit nets
    ]
    out = aggregate_paid(rows)
    assert out[7][2026] == {"card": 250.0, "non_card": 900.0, "total": 1150.0, "n": 3}
    assert out[7][2025] == {"card": 0.0, "non_card": 300.0, "total": 300.0, "n": 1}
    assert out[SHARED_LINE][2026]["total"] == 900.0
    assert sum(b["total"] for p in out.values() for b in p.values()) == 2350.0
    assert None not in out


# ── The one W-9 rule ───────────────────────────────────────────────────────

def test_w9_not_required_for_w2_employee():
    p = NS(is_w2_employee=True, w9_received_on=date(2026, 1, 1))
    assert w9_status_for(p)["status"] == W9_NOT_REQUIRED


def test_w9_on_file_from_payee_date():
    p = NS(is_w2_employee=False, w9_received_on=date(2026, 1, 12))
    s = w9_status_for(p, non_card=0, threshold=2000)
    assert s == {"status": W9_ON_FILE, "date": "2026-01-12", "source": "payee"}


def test_w9_on_file_from_doc_fallback():
    p = NS(is_w2_employee=False, w9_received_on=None)
    s = w9_status_for(p, fallback_date=date(2025, 4, 9))
    assert s["status"] == W9_ON_FILE and s["date"] == "2025-04-09" and s["source"] == "doc"


def test_w9_under_threshold_and_missing():
    p = NS(is_w2_employee=False, w9_received_on=None)
    assert w9_status_for(p, non_card=1300, threshold=2000)["status"] == W9_UNDER_THRESHOLD
    assert w9_status_for(p, non_card=2000, threshold=2000)["status"] == W9_MISSING
    assert w9_status_for(p)["status"] == W9_MISSING          # no amounts known
    assert w9_status_for(p, non_card=700, threshold=600)["status"] == W9_MISSING


# ── 1099 flag matrix ───────────────────────────────────────────────────────

def test_1099_grey_states():
    assert flag_1099(5000, 2000, W9_NOT_REQUIRED)["label"] == "Not required"
    r = flag_1099(1300, 2000, W9_MISSING)
    assert r["level"] == "grey" and r["label"] == "Under threshold" and not r["needs_attention"]
    r = flag_1099(0, 2000, W9_MISSING, card=5310)
    assert r["level"] == "grey" and r["label"] == "Card only"
    # a W-9 on file does not change an under-threshold year
    assert flag_1099(100, 600, W9_ON_FILE, "V1", True)["level"] == "grey"


def test_1099_red_when_reportable_and_w9_missing():
    r = flag_1099(6240, 2000, W9_MISSING)
    assert r["level"] == "red" and r["reportable"] and r["needs_attention"]
    assert flag_1099(600, 600, W9_UNDER_THRESHOLD)["level"] == "red"   # at the floor


def test_1099_amber_when_quickbooks_cannot_file():
    r = flag_1099(8400, 2000, W9_ON_FILE, qbo_vendor_id=None)
    assert r["level"] == "amber" and r["label"] == "Not in QuickBooks" and r["needs_attention"]
    r = flag_1099(11900, 2000, W9_ON_FILE, qbo_vendor_id="V9", qbo_vendor_1099=False)
    assert r["level"] == "amber" and r["label"] == "1099 off · mismatch"
    r = flag_1099(11900, 2000, W9_ON_FILE, qbo_vendor_id="V9", qbo_vendor_1099=None)
    assert r["level"] == "amber"


def test_1099_green_when_ready():
    r = flag_1099(14300, 2000, W9_ON_FILE, qbo_vendor_id="V2", qbo_vendor_1099=True)
    assert r["level"] == "green" and r["reportable"] and not r["needs_attention"]


def test_year_threshold_switch_changes_the_answer():
    # $1,300 non-card: reportable under 2025's $600, not under 2026's $2,000.
    assert flag_1099(1300, threshold_for_year(2025, {}), W9_MISSING)["level"] == "red"
    assert flag_1099(1300, threshold_for_year(2026, {}), W9_MISSING)["level"] == "grey"


def test_qbo_matches_w9():
    assert qbo_matches_w9(W9_ON_FILE, None, True) == "unlinked"
    assert qbo_matches_w9(W9_ON_FILE, "V1", True) == "yes"
    assert qbo_matches_w9(W9_MISSING, "V1", False) == "no"
    assert qbo_matches_w9(W9_ON_FILE, "V1", False) == "mismatch"
    assert qbo_matches_w9(W9_MISSING, "V1", True) == "mismatch"
