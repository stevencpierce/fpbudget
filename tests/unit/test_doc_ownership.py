"""Document ownership rules (DOCUMENTS_HANDOFF Phase 2, 2026-11-01).

Pure unit tests — no Flask, no DB. Pins the four rules app.py reads:
  1. the GLOBAL per-payee duplicate identity (actuals.doc_dup_key)
  2. the one W-9 date rule (actuals.w9_update_for)
  3. the Docs Inbox "Why here" derivation (actuals.doc_review_reason)
  4. the filing path rule: paperwork under the Payee, evidence under the
     project / uploader (actuals.doc_folder_segments)
plus tax_form_kind_from_text and the COI expiry state.

Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from datetime import date, datetime
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import (  # noqa: E402
    doc_dup_key, pick_dup_pair, w9_update_for, w9_date_from_doc, doc_review_reason,
    doc_folder_segments, tax_form_kind_from_text, expiry_state, doc_owner_id,
    REVIEW_TYPE_UNCLEAR, REVIEW_CONFIRM_PAYEE, REVIEW_POSSIBLE_DUPLICATE,
    REVIEW_EXPIRY_MISSING, REVIEW_NO_LINE, REVIEW_LOW_OCR, REVIEW_REASON_LABELS,
)


def _doc(**kw):
    base = dict(id=1, project_id=10, owner_payee_id=None, crew_member_id=None,
                category=None, tax_form_kind=None, doc_subtype=None, vendor=None,
                received_on=None, doc_date=None, expires_on=None,
                uploaded_at=datetime(2026, 10, 1, 12, 0), status='done',
                is_duplicate=False, confidence=90.0,
                filed_filename=None, original_filename=None)
    base.update(kw)
    return NS(**base)


# ── tax_form_kind_from_text ────────────────────────────────────────────────

def test_kind_from_text_table():
    assert tax_form_kind_from_text("W-9") == 'w9'
    assert tax_form_kind_from_text("w9_lindqvist.pdf") == 'w9'
    assert tax_form_kind_from_text("Form W-8BEN") == 'w8ben'
    assert tax_form_kind_from_text("W-8BEN-E (entity)") == 'w8bene'
    assert tax_form_kind_from_text("W-4") == 'w4'
    assert tax_form_kind_from_text("I-9 employment") == 'i9'
    assert tax_form_kind_from_text("Driver's License") is None
    assert tax_form_kind_from_text(None) is None


# ── 1. Duplicate rule: global per (owner, kind), never per project ─────────

def test_dup_key_is_owner_and_kind_not_project():
    a = _doc(id=1, project_id=10, owner_payee_id=7, category='tax_form', tax_form_kind='w9')
    b = _doc(id=2, project_id=99, owner_payee_id=7, category='tax_form', tax_form_kind='w9')
    assert doc_dup_key(a) == doc_dup_key(b) == (7, 'tax_form:w9')
    c = _doc(id=3, project_id=None, owner_payee_id=7, category='tax_form', tax_form_kind='w9')
    assert doc_dup_key(c) == doc_dup_key(a)          # a project-less W-9 counts too


def test_dup_key_differs_by_kind_and_owner():
    w9 = _doc(owner_payee_id=7, category='tax_form', tax_form_kind='w9')
    w4 = _doc(owner_payee_id=7, category='tax_form', tax_form_kind='w4')
    other = _doc(owner_payee_id=8, category='tax_form', tax_form_kind='w9')
    assert doc_dup_key(w9) != doc_dup_key(w4)
    assert doc_dup_key(w9) != doc_dup_key(other)


def test_dup_key_employee_vendor_doc_uses_subtype_case_insensitive():
    a = _doc(owner_payee_id=7, category='employee_vendor_doc', doc_subtype="Driver's License")
    b = _doc(owner_payee_id=7, category='employee_vendor_doc', doc_subtype="driver's license")
    assert doc_dup_key(a) == doc_dup_key(b) == (7, "employee_vendor_doc:driver's license")
    # legacy row: sub-type still in `vendor`
    c = _doc(owner_payee_id=7, category='employee_vendor_doc', vendor="Driver's License")
    assert doc_dup_key(c) == doc_dup_key(a)


def test_dup_key_none_without_owner_or_kind_or_for_evidence():
    assert doc_dup_key(_doc(category='tax_form', tax_form_kind='w9')) is None      # no owner
    assert doc_dup_key(_doc(owner_payee_id=7, category='employee_vendor_doc')) is None  # no sub-type
    assert doc_dup_key(_doc(owner_payee_id=7, category='receipt')) is None
    assert doc_dup_key(_doc(owner_payee_id=7, category='invoice', vendor='Vega')) is None


def test_dup_key_reads_alias_owner_and_filename_kind():
    legacy = _doc(crew_member_id=7, category='tax_form', filed_filename='2026_TAX_W-9_Vega.pdf')
    assert doc_owner_id(legacy) == 7
    assert doc_dup_key(legacy) == (7, 'tax_form:w9')


def test_pick_dup_pair_lower_id_is_original():
    a, b = _doc(id=5), _doc(id=9)
    assert pick_dup_pair(b, a) == (a, b)
    assert pick_dup_pair(a, b) == (a, b)


# ── 2. The one W-9 date rule ───────────────────────────────────────────────

def _payee(id=7, w9_received_on=None, w9_doc_upload_id=None):
    return NS(id=id, w9_received_on=w9_received_on, w9_doc_upload_id=w9_doc_upload_id)


def test_w9_date_prefers_received_on_then_doc_date_then_upload_day():
    assert w9_date_from_doc(_doc(category='tax_form', tax_form_kind='w9',
                                 received_on=date(2026, 3, 1), doc_date=date(2026, 1, 1))) == date(2026, 3, 1)
    assert w9_date_from_doc(_doc(category='tax_form', tax_form_kind='w9',
                                 doc_date=date(2026, 1, 1))) == date(2026, 1, 1)
    assert w9_date_from_doc(_doc(category='tax_form', tax_form_kind='w9')) == date(2026, 10, 1)
    assert w9_date_from_doc(_doc(category='tax_form', tax_form_kind='w4')) is None
    assert w9_date_from_doc(_doc(category='employee_vendor_doc', tax_form_kind='w9')) is None


def test_w9_rule_first_form_sets_pointer():
    doc = _doc(id=11, owner_payee_id=7, category='tax_form', tax_form_kind='w9',
               received_on=date(2026, 5, 2))
    assert w9_update_for(_payee(), doc) == {"w9_received_on": date(2026, 5, 2), "w9_doc_upload_id": 11}


def test_w9_rule_newer_wins_older_never_replaces():
    p = _payee(w9_received_on=date(2026, 5, 2), w9_doc_upload_id=11)
    newer = _doc(id=12, owner_payee_id=7, category='tax_form', tax_form_kind='w8ben',
                 received_on=date(2026, 9, 9))
    older = _doc(id=13, owner_payee_id=7, category='tax_form', tax_form_kind='w9',
                 received_on=date(2025, 1, 1))
    assert w9_update_for(p, newer) == {"w9_received_on": date(2026, 9, 9), "w9_doc_upload_id": 12}
    assert w9_update_for(p, older) is None


def test_w9_rule_same_day_replacement_moves_pointer_and_same_doc_edit_updates():
    p = _payee(w9_received_on=date(2026, 5, 2), w9_doc_upload_id=11)
    same_day = _doc(id=14, owner_payee_id=7, category='tax_form', tax_form_kind='w9',
                    received_on=date(2026, 5, 2))
    assert w9_update_for(p, same_day)["w9_doc_upload_id"] == 14
    edited = _doc(id=11, owner_payee_id=7, category='tax_form', tax_form_kind='w9',
                  received_on=date(2026, 4, 1))
    assert w9_update_for(p, edited) == {"w9_received_on": date(2026, 4, 1), "w9_doc_upload_id": 11}
    unchanged = _doc(id=11, owner_payee_id=7, category='tax_form', tax_form_kind='w9',
                     received_on=date(2026, 5, 2))
    assert w9_update_for(p, unchanged) is None


def test_w9_rule_ignores_other_owners_and_non_w9():
    p = _payee(id=7)
    assert w9_update_for(p, _doc(owner_payee_id=8, category='tax_form', tax_form_kind='w9')) is None
    assert w9_update_for(p, _doc(owner_payee_id=7, category='tax_form', tax_form_kind='i9')) is None
    assert w9_update_for(p, _doc(owner_payee_id=7, category='insurance')) is None


# ── 3. Why here ────────────────────────────────────────────────────────────

def test_review_reason_order():
    sug = {"crew_member_id": 7, "confidence": 0.9}
    assert doc_review_reason(_doc(category=None, status='review')) == REVIEW_TYPE_UNCLEAR
    assert doc_review_reason(_doc(category='tax_form', status='done'), suggestion=sug) == REVIEW_CONFIRM_PAYEE
    assert doc_review_reason(_doc(category='invoice', status='done'), suggestion=sug) == REVIEW_CONFIRM_PAYEE
    # an owner already set → the suggestion no longer matters
    assert doc_review_reason(_doc(category='tax_form', owner_payee_id=7), suggestion=sug) is None
    assert doc_review_reason(_doc(category='receipt', is_duplicate=True)) == REVIEW_POSSIBLE_DUPLICATE
    assert doc_review_reason(_doc(category='receipt', is_duplicate=True, status='duplicate')) is None
    assert doc_review_reason(_doc(category='insurance', owner_payee_id=3)) == REVIEW_EXPIRY_MISSING
    assert doc_review_reason(_doc(category='insurance', owner_payee_id=3, expires_on=date(2027, 1, 1))) is None
    txn = NS(budget_line_id=None, account_code=None, not_project_expense=False)
    assert doc_review_reason(_doc(category='receipt'), txn=txn) == REVIEW_NO_LINE
    coded = NS(budget_line_id=5, account_code=None, not_project_expense=False)
    assert doc_review_reason(_doc(category='receipt'), txn=coded) is None
    assert doc_review_reason(_doc(category='receipt', status='review', confidence=42)) == REVIEW_LOW_OCR
    assert doc_review_reason(_doc(category='receipt', status='review', confidence=95)) == REVIEW_TYPE_UNCLEAR
    # a receipt never needs an owner, so a suggestion alone is not a reason
    assert doc_review_reason(_doc(category='receipt'), suggestion=sug) is None


def test_every_reason_has_a_label():
    for k in (REVIEW_TYPE_UNCLEAR, REVIEW_CONFIRM_PAYEE, REVIEW_POSSIBLE_DUPLICATE,
              REVIEW_EXPIRY_MISSING, REVIEW_NO_LINE, REVIEW_LOW_OCR):
        assert REVIEW_REASON_LABELS[k]


# ── 4. Path rule: paperwork under the Payee, evidence under the project ────

def test_paperwork_with_owner_files_under_payee_never_uploader():
    for cat in ('tax_form', 'employee_vendor_doc', 'contract', 'insurance', 'release', 'legal'):
        d = _doc(category=cat, owner_payee_id=7, vendor='Acme Insurance')
        assert doc_folder_segments(d, payee_name='Theo_Lindqvist', uploader_folder='steven') \
            == ['Theo_Lindqvist'], cat


def test_paperwork_without_owner_keeps_uploader_layout():
    assert doc_folder_segments(_doc(category='tax_form'), payee_name=None, uploader_folder='steven') == ['steven']
    assert doc_folder_segments(_doc(category='insurance', vendor='Northshore'), None, 'steven') \
        == ['steven', 'Northshore']


def test_evidence_stays_under_project_even_with_owner():
    inv = _doc(category='invoice', owner_payee_id=7, vendor='Vega Camera LLC')
    assert doc_folder_segments(inv, payee_name='Marisol_Vega', uploader_folder='steven') \
        == ['steven', 'Vega Camera LLC']
    rec = _doc(category='receipt', owner_payee_id=7, vendor='Sweetgreen')
    assert doc_folder_segments(rec, payee_name='Marisol_Vega', uploader_folder='steven') == ['steven']
    po = _doc(category='purchase_order', vendor='Lensrentals')
    assert doc_folder_segments(po, None, 'steven') == ['steven', 'Lensrentals']
    misc = _doc(category='misc', owner_payee_id=7)
    assert doc_folder_segments(misc, payee_name='X', uploader_folder='steven') == ['steven']


# ── COI expiry ─────────────────────────────────────────────────────────────

def test_expiry_state():
    today = date(2026, 11, 1)
    assert expiry_state(None, today) is None
    assert expiry_state(date(2026, 10, 31), today) == 'bad'
    assert expiry_state(date(2026, 12, 15), today) == 'warn'      # within 60 days
    assert expiry_state(date(2026, 12, 31), today) == 'warn'      # exactly 60
    assert expiry_state(date(2027, 1, 1), today) == 'ok'
