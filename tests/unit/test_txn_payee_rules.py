"""Payment attribution + QuickBooks vendor mirror rules (DOCUMENTS_HANDOFF
Phase 3, 2026-11-02).

Pure unit tests — no Flask, no DB, no QuickBooks. Pins:

  • set_txn_payee precedence: fill NULL; replace only the named sources;
    a 'manual' value is never replaced by an automatic write
  • line_owner_id / payee_name_index / resolve_payee_candidates (the
    backfill's decision: one voice sets, two voices that disagree is a
    conflict, never a guess)
  • qbo_sync.vendor_fields (no TIN), match_vendor_to_payee (id, then exact
    DisplayName, never fuzzy), apply_vendor_to_payee (what the mirror owns)
  • vendor_create_payload carries no Id / SyncToken; create_vendor refuses
    one that does — FPBudget can create in QuickBooks, never update
  • _extract_txn_fields keeps EntityRef.value as qbo_entity_ref and
    _attribute_from_entity_ref honours the precedence rule

Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import (  # noqa: E402
    set_txn_payee, line_owner_id, payee_name_index, resolve_payee_candidates,
)
import qbo_sync  # noqa: E402
from qbo_sync import (  # noqa: E402
    vendor_fields, match_vendor_to_payee, apply_vendor_to_payee,
    vendor_create_payload, create_vendor, _extract_txn_fields,
    _attribute_from_entity_ref, _qbo_quote,
)


def _txn(**kw):
    base = dict(id=1, payee_id=None, payee_source=None, doc_upload_id=None,
                budget_line_id=None, vendor=None, project_id=7)
    base.update(kw)
    return NS(**base)


# ── set_txn_payee ──────────────────────────────────────────────────────────

def test_fill_null_any_source():
    t = _txn()
    assert set_txn_payee(t, 5, 'qbo') is True
    assert (t.payee_id, t.payee_source) == (5, 'qbo')


def test_automatic_source_never_replaces_manual():
    t = _txn(payee_id=5, payee_source='manual')
    for src in ('qbo', 'line', 'doc', 'alias'):
        assert set_txn_payee(t, 9, src, replace=('qbo', 'line', 'doc', 'alias', 'manual')) is False
    assert t.payee_id == 5 and t.payee_source == 'manual'


def test_manual_replaces_anything():
    t = _txn(payee_id=5, payee_source='qbo')
    assert set_txn_payee(t, 9, 'manual') is True
    assert (t.payee_id, t.payee_source) == (9, 'manual')
    assert set_txn_payee(t, 9, 'manual') is False     # same value, no change


def test_replace_only_named_sources():
    t = _txn(payee_id=5, payee_source='line')
    assert set_txn_payee(t, 9, 'qbo') is False                   # not in replace
    assert set_txn_payee(t, 9, 'qbo', replace=('qbo',)) is False # line is not qbo
    assert set_txn_payee(t, 9, 'line', replace=('line',)) is True
    assert (t.payee_id, t.payee_source) == (9, 'line')
    assert set_txn_payee(t, 9, 'line', replace=('line',)) is False


def test_clear_only_own_or_named_source():
    t = _txn(payee_id=5, payee_source='line')
    assert set_txn_payee(t, None, 'doc') is False
    assert t.payee_id == 5
    assert set_txn_payee(t, None, 'line') is True
    assert t.payee_id is None and t.payee_source is None
    t = _txn(payee_id=5, payee_source='manual')
    assert set_txn_payee(t, None, 'line', replace=('manual',)) is False, "manual is never cleared by automation"


def test_unknown_source_rejected():
    with pytest.raises(ValueError):
        set_txn_payee(_txn(), 1, 'guess')


# ── backfill decisions ─────────────────────────────────────────────────────

def test_line_owner_id_exactly_one():
    assert line_owner_id([3, 3, None]) == 3
    assert line_owner_id([3, 4]) is None
    assert line_owner_id([]) is None
    assert line_owner_id(None) is None


def test_payee_name_index_marks_shared_names_ambiguous():
    idx = payee_name_index([NS(id=1, name="Vega Camera LLC", legal_name="Vega Camera, LLC", company=None),
                            NS(id=2, name="Theo Lindqvist", legal_name=None, company="Vega Camera LLC"),
                            NS(id=3, name="Mara Quinn", legal_name="Mara Quinn", company=None)])
    assert idx["vega camera llc"] is None          # two payees → never guessed
    assert idx["vega camera, llc"] == 1
    assert idx["theo lindqvist"] == 2
    assert idx["mara quinn"] == 3


def test_resolve_single_voice_sets_with_its_source():
    doc_owner = {10: 2}
    lines = {20: {3}}
    r = resolve_payee_candidates(_txn(doc_upload_id=10), doc_owner, lines)
    assert (r["payee_id"], r["source"]) == (2, 'doc')
    r = resolve_payee_candidates(_txn(budget_line_id=20), doc_owner, lines)
    assert (r["payee_id"], r["source"]) == (3, 'line')
    r = resolve_payee_candidates(_txn(vendor="VEGA CAMERA LLC #4421"), doc_owner, lines,
                                 alias_by_key={(None, "vega camera llc"): 1})
    assert (r["payee_id"], r["source"]) == (1, 'alias')
    r = resolve_payee_candidates(_txn(vendor="Theo Lindqvist"), doc_owner, lines,
                                 name_index={"theo lindqvist": 2})
    assert (r["payee_id"], r["source"]) == (2, 'alias')


def test_resolve_project_alias_beats_global():
    r = resolve_payee_candidates(_txn(vendor="Lyft", project_id=7), {}, {},
                                 alias_by_key={(None, "lyft"): 1, (7, "lyft"): 4})
    assert r["payee_id"] == 4


def test_resolve_agreeing_voices_set_disagreeing_conflict():
    r = resolve_payee_candidates(_txn(doc_upload_id=10, budget_line_id=20), {10: 2}, {20: {2}})
    assert (r["payee_id"], r["source"]) == (2, 'doc') and not r.get("conflict")
    r = resolve_payee_candidates(_txn(doc_upload_id=10, budget_line_id=20), {10: 2}, {20: {3}})
    assert r["payee_id"] is None and r["conflict"] is True
    assert sorted(r["candidates"]) == [('doc', 2), ('line', 3)]


def test_resolve_shared_line_and_empty_vendor_name_nobody():
    r = resolve_payee_candidates(_txn(budget_line_id=20, vendor="   "), {}, {20: {2, 3}},
                                 name_index={"": 9})
    assert r["payee_id"] is None and not r.get("conflict")


# ── vendor mirror ──────────────────────────────────────────────────────────

RAW_VENDOR = {
    "Id": "88", "SyncToken": "3", "DisplayName": "Vega Camera LLC",
    "CompanyName": "Vega Camera, LLC", "GivenName": "Ana", "FamilyName": "Vega",
    "PrimaryEmailAddr": {"Address": "ar@vegacamera.com"},
    "PrimaryPhone": {"FreeFormNumber": "(617) 555-0100"},
    "BillAddr": {"Line1": "12 Harbor St", "Line2": "Unit 4", "City": "Boston",
                 "CountrySubDivisionCode": "MA", "PostalCode": "02110"},
    "Vendor1099": True, "Active": True,
    "TaxIdentifier": "XXX-XX-1234",
    "MetaData": {"LastUpdatedTime": "2026-10-30T12:00:00-07:00"},
}


def test_vendor_fields_mirrors_what_it_should_and_no_tin():
    f = vendor_fields(RAW_VENDOR)
    assert f["qbo_vendor_id"] == "88" and f["display_name"] == "Vega Camera LLC"
    assert f["company_name"] == "Vega Camera, LLC" and f["email"] == "ar@vegacamera.com"
    assert f["phone"] == "(617) 555-0100" and f["address_line1"] == "12 Harbor St"
    assert f["address_line2"] == "Unit 4" and f["city"] == "Boston"
    assert f["state"] == "MA" and f["postal_code"] == "02110"
    assert f["vendor_1099"] is True and f["active"] is True and f["sync_token"] == "3"
    assert f["qbo_updated_at"] is not None
    assert not any("tax" in k.lower() or "tin" in k.lower() for k in f), "no TIN is ever mirrored"
    assert vendor_fields({"DisplayName": "no id"}) is None


def test_match_by_id_then_exact_name_never_fuzzy():
    linked = NS(id=1, name="Old Name")
    theo = NS(id=2, name="Theo Lindqvist")
    f = vendor_fields(dict(RAW_VENDOR, DisplayName="Theo Lindqvist"))
    assert match_vendor_to_payee(f, {"88": linked}, {"theo lindqvist": [theo]}) == (linked, 'id')
    assert match_vendor_to_payee(f, {}, {"theo lindqvist": [theo]}) == (theo, 'name')
    assert match_vendor_to_payee(f, {}, {"theo lindqvist": [theo, NS(id=3, name="Theo Lindqvist")]}) == (None, None)
    assert match_vendor_to_payee(f, {}, {"theo lindqvis": [theo]}) == (None, None)
    assert match_vendor_to_payee(vendor_fields(dict(RAW_VENDOR, DisplayName="THEO LINDQVIST")),
                                 {}, {"theo lindqvist": [theo]}) == (theo, 'name')


def _cm(**kw):
    base = dict(id=1, name="Vega Camera LLC", legal_name=None, company=None, email=None, phone=None,
                address_line1=None, address_line2=None, city=None, state=None, postal_code=None,
                qbo_vendor_1099=None, qbo_sync_token=None, qbo_synced_at=None, qbo_origin=None,
                qbo_vendor_id=None, active=True, kind='vendor', is_w2_employee=False)
    base.update(kw)
    return NS(**base)


def test_apply_vendor_to_payee_owns_contact_fields_not_the_name():
    cm = _cm(name="Vega (crew label)", email="old@x.com")
    changed = apply_vendor_to_payee(cm, vendor_fields(RAW_VENDOR))
    assert cm.name == "Vega (crew label)", "the FPBudget name is never overwritten"
    assert cm.legal_name == "Vega Camera, LLC" and cm.company == "Vega Camera, LLC"
    assert cm.email == "ar@vegacamera.com" and cm.phone == "(617) 555-0100"
    assert cm.city == "Boston" and cm.qbo_vendor_1099 is True
    assert cm.qbo_origin == 'qbo' and cm.qbo_synced_at is not None and cm.qbo_sync_token == "3"
    assert 'email' in changed and 'name' not in changed


def test_apply_vendor_empty_values_do_not_blank_and_active_only_for_vendors():
    cm = _cm(email="keep@x.com", kind='person', active=True)
    f = vendor_fields(dict(RAW_VENDOR, PrimaryEmailAddr=None, Active=False))
    apply_vendor_to_payee(cm, f)
    assert cm.email == "keep@x.com"
    assert cm.active is True, "a person's crew-roster state is FPBudget's"
    v = _cm(kind='vendor', active=True)
    apply_vendor_to_payee(v, f)
    assert v.active is False


# ── the one write ──────────────────────────────────────────────────────────

def test_create_payload_from_payee_fields():
    cm = _cm(name="Vega Camera LLC", legal_name="Vega Camera, LLC", email="ar@vegacamera.com",
             phone="617-555-0100", address_line1="12 Harbor St", city="Boston", state="MA",
             postal_code="02110")
    body = vendor_create_payload(cm, w9_on_file=True)
    assert body["DisplayName"] == "Vega Camera, LLC"
    assert body["CompanyName"] == "Vega Camera, LLC"
    assert body["PrimaryEmailAddr"] == {"Address": "ar@vegacamera.com"}
    assert body["PrimaryPhone"] == {"FreeFormNumber": "617-555-0100"}
    assert body["BillAddr"] == {"Line1": "12 Harbor St", "City": "Boston",
                                "CountrySubDivisionCode": "MA", "PostalCode": "02110"}
    assert body["Vendor1099"] is True
    for k in ("Id", "SyncToken", "sparse"):
        assert k not in body, "a create body can never address an existing record"
    assert "TaxIdentifier" not in body


def test_create_payload_person_without_w9():
    cm = _cm(name="Theo Lindqvist", kind='person')
    body = vendor_create_payload(cm, w9_on_file=False)
    assert body["DisplayName"] == "Theo Lindqvist"
    assert body["GivenName"] == "Theo" and body["FamilyName"] == "Lindqvist"
    assert body["Vendor1099"] is False and "BillAddr" not in body and "CompanyName" not in body


def test_create_vendor_refuses_update_shaped_bodies():
    for bad in ({"Id": "88", "DisplayName": "x"}, {"SyncToken": "1", "DisplayName": "x"},
                {"sparse": True, "DisplayName": "x"}):
        with pytest.raises(ValueError):
            create_vendor(NS(realm_id="1"), None, bad)


def test_module_has_no_update_helper():
    names = [n for n in dir(qbo_sync) if 'vendor' in n.lower()]
    assert not any(('update' in n.lower() or 'sparse' in n.lower() or 'delete' in n.lower())
                   for n in names), names


def test_qbo_quote_escapes_apostrophes():
    assert _qbo_quote("O'Brien's") == "'O\\'Brien\\'s'"


# ── EntityRef on synced charges ────────────────────────────────────────────

ACCT_META = {"41": {"id": "41", "name": "Chase Ink", "type": "Credit Card", "mask": "8432"}}


def test_extract_keeps_entity_ref_only_for_vendors():
    txn = {"Id": "1001", "TxnDate": "2026-10-12", "TotalAmt": 37.93,
           "AccountRef": {"value": "41"},
           "EntityRef": {"value": "88", "name": "Vega Camera LLC", "type": "Vendor"},
           "Line": []}
    r = _extract_txn_fields(txn, "Purchase", "41", ACCT_META)
    assert r["qbo_entity_ref"] == "88" and r["vendor"] == "Vega Camera LLC"
    r = _extract_txn_fields(dict(txn, EntityRef={"value": "88", "name": "Vega Camera LLC"}),
                            "Purchase", "41", ACCT_META)
    assert r["qbo_entity_ref"] == "88", "an untyped ref is a vendor (QuickBooks' default)"
    r = _extract_txn_fields(dict(txn, EntityRef={"value": "88", "name": "Sam Staff", "type": "Employee"}),
                            "Purchase", "41", ACCT_META)
    assert r["qbo_entity_ref"] is None and r["vendor"] == "Sam Staff", \
        "ids are per entity type — an Employee 88 is not Vendor 88"
    r = _extract_txn_fields(dict(txn, EntityRef=None), "Purchase", "41", ACCT_META)
    assert r["qbo_entity_ref"] is None


def test_extract_deposit_line_entity():
    txn = {"Id": "2", "TxnDate": "2026-10-12", "TotalAmt": 500,
           "DepositToAccountRef": {"value": "7"},
           "Line": [{"DepositLineDetail": {"Entity": {"value": "91", "name": "Client Co", "type": "Customer"}}}]}
    r = _extract_txn_fields(txn, "Deposit", "7", ACCT_META)
    assert r["qbo_entity_ref"] is None and r["vendor"] == "Client Co"
    txn["Line"][0]["DepositLineDetail"]["Entity"]["type"] = "Vendor"
    assert _extract_txn_fields(txn, "Deposit", "7", ACCT_META)["qbo_entity_ref"] == "91"


def test_attribute_from_entity_ref_rules():
    t = _txn()
    assert _attribute_from_entity_ref(t, "88", {"88": 5}) is True and t.payee_source == 'qbo'
    assert _attribute_from_entity_ref(t, "88", {"88": 5}) is False          # no change
    assert _attribute_from_entity_ref(t, "88", {"88": 6}) is True            # QuickBooks moved it
    assert t.payee_id == 6
    t = _txn(payee_id=5, payee_source='manual')
    assert _attribute_from_entity_ref(t, "88", {"88": 6}) is False and t.payee_id == 5
    t = _txn(payee_id=5, payee_source='doc')
    assert _attribute_from_entity_ref(t, "88", {"88": 6}) is False
    assert _attribute_from_entity_ref(_txn(), "99", {"88": 6}) is False      # not linked yet
    assert _attribute_from_entity_ref(_txn(), None, {"88": 6}) is False
