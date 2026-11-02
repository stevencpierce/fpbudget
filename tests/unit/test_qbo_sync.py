"""qbo_sync field extraction and is_expense sign handling (ACTUALS_HANDOFF
Phase 0, 2026-10-30).

Pure unit tests — no Flask, no DB, no QuickBooks. Small fixture dicts shaped
like the QBO API's Purchase / Deposit / BillPayment / CreditCardCredit
entities go into _extract_txn_fields; the row dict it returns is asserted
field by field. The key invariants:

  • Purchase            → expense, UNLESS Credit=true (a card refund) → credit
  • BillPayment         → expense (money out of the bank)
  • Deposit / CreditCardCredit → credit
  • repair_is_expense relies on the same mapping, so a Purchase with
    Credit=true that was imported as an expense is reported as a flip.

Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from qbo_sync import _extract_txn_fields  # noqa: E402


ACCT_META = {
    "41": {"id": "41", "name": "Chase Ink", "type": "Credit Card", "mask": "8432"},
    "7":  {"id": "7",  "name": "Ops Checking", "type": "Bank", "mask": "1101"},
}


def _purchase(**kw):
    base = {
        "Id": "1001", "TxnDate": "2026-10-12", "TotalAmt": 37.93,
        "PaymentType": "CreditCard",
        "AccountRef": {"value": "41", "name": "Chase Ink"},
        "EntityRef": {"value": "88", "name": "Lyft"},
        "PrivateNote": "LYFT *RIDE MON 8AM",
        "Line": [{"Amount": 37.93, "Description": "Ride to set",
                  "AccountBasedExpenseLineDetail": {
                      "AccountRef": {"value": "61", "name": "Travel"}}}],
    }
    base.update(kw)
    return base


# ── sign handling ──────────────────────────────────────────────────────────

def test_purchase_is_expense():
    r = _extract_txn_fields(_purchase(), "Purchase", "41", ACCT_META)
    assert r["is_expense"] is True
    assert r["amount"] == 37.93


def test_purchase_credit_true_is_a_refund():
    r = _extract_txn_fields(_purchase(Credit=True), "Purchase", "41", ACCT_META)
    assert r["is_expense"] is False, "Purchase with Credit=true is a card refund"


def test_purchase_credit_false_is_expense():
    r = _extract_txn_fields(_purchase(Credit=False), "Purchase", "41", ACCT_META)
    assert r["is_expense"] is True


def test_credit_card_credit_is_a_credit():
    r = _extract_txn_fields(_purchase(), "CreditCardCredit", "41", ACCT_META)
    assert r["is_expense"] is False


def test_deposit_is_a_credit():
    dep = {"Id": "55", "TxnDate": "2026-10-01", "TotalAmt": 1500.0,
           "Line": [{"Amount": 1500.0,
                     "DepositLineDetail": {"Entity": {"name": "Cliburn Media"}}}]}
    r = _extract_txn_fields(dep, "Deposit", "7", ACCT_META)
    assert r["is_expense"] is False
    assert r["vendor"] == "Cliburn Media"       # payer pulled from the deposit line


def test_bill_payment_is_expense():
    bp = {"Id": "77", "TxnDate": "2026-10-05", "TotalAmt": 2200.0,
          "VendorRef": {"name": "Gear House"}, "PayType": "Check",
          "CheckPayment": {"CheckNum": "1042"}}
    r = _extract_txn_fields(bp, "BillPayment", "7", ACCT_META)
    assert r["is_expense"] is True
    assert "check #1042" in r["note"]


# ── field extraction ───────────────────────────────────────────────────────

def test_extracted_identity_fields():
    r = _extract_txn_fields(_purchase(), "Purchase", "41", ACCT_META)
    assert r["qbo_txn_id"] == "1001"
    assert r["qbo_txn_type"] == "Purchase"
    assert r["qbo_account_id"] == "41"
    assert r["txn_date"] == "2026-10-12"
    assert r["vendor"] == "Lyft"
    assert r["qbo_category"] == "Travel"
    assert r["account_name"] == "CC: 8432"


def test_bank_account_display_tag():
    r = _extract_txn_fields(_purchase(), "Purchase", "7", ACCT_META)
    assert r["account_name"] == "Acct: 1101"


def test_unknown_account_falls_back_to_ref_name():
    r = _extract_txn_fields(_purchase(), "Purchase", "999", ACCT_META)
    assert r["account_name"] == "Chase Ink"


def test_missing_id_returns_none():
    assert _extract_txn_fields(_purchase(Id=""), "Purchase", "41", ACCT_META) is None
    p = _purchase(); del p["Id"]
    assert _extract_txn_fields(p, "Purchase", "41", ACCT_META) is None


def test_vendor_fallback_chain():
    # No EntityRef → first line description → PrivateNote (bank text).
    p = _purchase(); del p["EntityRef"]
    r = _extract_txn_fields(p, "Purchase", "41", ACCT_META)
    assert r["vendor"] == "Ride to set"
    p = _purchase(); del p["EntityRef"]; p["Line"] = []
    r = _extract_txn_fields(p, "Purchase", "41", ACCT_META)
    assert r["vendor"] == "LYFT *RIDE MON 8AM"
    p = _purchase(); del p["EntityRef"]; p["Line"] = []; p["PrivateNote"] = ""
    r = _extract_txn_fields(p, "Purchase", "41", ACCT_META)
    assert r["vendor"] == ""


def test_item_based_line_category():
    p = _purchase(Line=[{"Amount": 37.93, "Description": "Gaff tape",
                         "ItemBasedExpenseLineDetail": {
                             "ItemRef": {"name": "Expendables"}}}])
    r = _extract_txn_fields(p, "Purchase", "41", ACCT_META)
    assert r["qbo_category"] == "Expendables"


def test_note_dedupes_and_caps():
    p = _purchase(DocNumber="A-9", PrivateNote="Memo text",
                  Line=[{"Amount": 10, "Description": "Thing one"},
                        {"Amount": 27.93, "Description": "Thing two"}])
    r = _extract_txn_fields(p, "Purchase", "41", ACCT_META)
    assert r["note"].startswith("Memo text · ref #A-9 · CreditCard · ")
    assert "Thing one $10.00 + Thing two $27.93" in r["note"]
    assert len(r["note"]) <= 480


# ── repair_is_expense agrees with the extractor ────────────────────────────

def test_repair_flip_decision_uses_same_mapping():
    """repair_is_expense compares each local row's is_expense to the freshly
    extracted value. Reproduce its flip decision on stub rows so a change to
    the mapping above (e.g. dropping the Credit=true branch) breaks here."""
    fresh = _extract_txn_fields(_purchase(Credit=True), "Purchase", "41", ACCT_META)
    local_is_expense = True             # imported pre-CR-3 as a positive expense
    to_credit = local_is_expense and not fresh["is_expense"]
    assert to_credit, "a Purchase with Credit=true must be flipped to a credit"
    fresh2 = _extract_txn_fields(_purchase(), "CreditCardCredit", "41", ACCT_META)
    assert not fresh2["is_expense"]


# ── BillPayment names its vendor in VendorRef (Nov 2 2026) ─────────────────

def _bill_payment(**kw):
    base = {
        "Id": "1300000322", "TxnDate": "2026-10-20", "TotalAmt": 4500.0,
        "PayType": "Check",
        "VendorRef": {"value": "214", "name": "Ryan Mauskopf"},
        "CheckPayment": {"BankAccountRef": {"value": "7", "name": "Ops Checking"}},
        "Line": [{"Amount": 4500.0, "LinkedTxn": [{"TxnId": "900", "TxnType": "Bill"}]}],
    }
    base.update(kw)
    return base


def test_bill_payment_vendor_comes_from_vendorref():
    r = _extract_txn_fields(_bill_payment(), "BillPayment", "7", ACCT_META)
    assert r["vendor"] == "Ryan Mauskopf"
    assert r["qbo_entity_ref"] == "214"
    assert r["is_expense"] is True and r["amount"] == 4500.0


def test_bill_payment_entityref_still_wins_when_present():
    r = _extract_txn_fields(_bill_payment(EntityRef={"value": "9", "name": "Someone Else", "type": "Vendor"}),
                            "BillPayment", "7", ACCT_META)
    assert r["vendor"] == "Someone Else" and r["qbo_entity_ref"] == "9"


def test_purchase_ignores_vendorref():
    r = _extract_txn_fields(_purchase(VendorRef={"value": "5", "name": "Wrong"}), "Purchase", "41", ACCT_META)
    assert r["vendor"] == "Lyft" and r["qbo_entity_ref"] == "88"
