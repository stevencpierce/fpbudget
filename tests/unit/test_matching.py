"""Actuals Phase 5 — THE matching engine and the duplicate rules
(ACTUALS_HANDOFF, 2026-11-03). Pure: no Flask, no DB, no QuickBooks.

  1. score_match / rank_candidates — exact, tip, posted-later, wrong
     vendor, two equal candidates → none strong, far-apart amounts → no
     candidate, refund pairing.
  2. suggest_pairs — greedy one-to-one, rejected pairs never proposed.
  3. is_duplicate_twin / find_duplicate_groups / pick_keeper — the 3-day
     window for two QuickBooks rows (Ryan Mauskopf, Oct 19 / Oct 20),
     same-day only for manual rows, the SAME qbo id is a sibling not a
     duplicate, the keeper order (receipt → coded → earlier → lower id),
     Keep both dismissal.
  4. merge_duplicate_into — the field moves, splits and evidence re-pointed.
  5. qbo_deletion_action + parse_cdc_payload — a stubbed CDC payload with a
     status "Deleted" stub lands in `deleted`, never in rows.

Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from datetime import date
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import (  # noqa: E402
    score_match, rank_candidates, suggest_pairs, MATCH_SUGGEST_MIN, MATCH_STRONG_MIN,
    is_duplicate_twin, find_duplicate_groups, pick_keeper, dup_group_key,
    duplicate_delete_mode, merge_duplicate_into, txn_snapshot, qbo_deletion_action,
    compute_row_flags, VOIDED_IN_QBO_REASON,
)
from qbo_sync import parse_cdc_payload  # noqa: E402


def charge(id=1, amount=88.44, date_='2026-09-28', vendor='LYFT *RIDE MON 8AM', card='4417', **kw):
    return NS(id=id, amount=amount, txn_date=date_, vendor=vendor, card_last4=card, **kw)


def receipt(id=10, amount=88.44, date_=date(2026, 9, 28), vendor='Lyft', card='4417', **kw):
    return NS(id=id, amount=amount, doc_date=date_, vendor=vendor, card_last4=card, **kw)


# ── 1. scoring ────────────────────────────────────────────────────────────

def test_exact_same_day_same_vendor_is_a_strong_suggestion():
    s = score_match(charge(), receipt())
    assert s["confidence"] == 1.0
    assert 'exact amount' in s["reasons"] and 'same day' in s["reasons"] and 'same vendor' in s["reasons"]
    r = rank_candidates(charge(), [receipt()])
    assert r[0]["strong"] is True


def test_tip_range_scores_0_7_on_amount_with_reason():
    s = score_match(charge(amount=104.00), receipt(amount=88.44))   # +17.6%, restaurant / ride tip
    assert s["amount_score"] == 0.7
    assert 'tip?' in s["reasons"]
    assert s["confidence"] >= MATCH_STRONG_MIN        # 0.35 + 0.3 + 0.2 + card
    s2 = score_match(charge(amount=88.44), receipt(amount=104.00))   # charge BELOW the receipt: not a tip
    assert s2 is None or 'tip?' not in s2["reasons"]


def test_within_five_dollars_either_way_is_a_candidate_and_beyond_is_not():
    assert score_match(charge(amount=90.00), receipt(amount=88.44))["amount_score"] == 0.7
    assert score_match(charge(amount=84.00), receipt(amount=88.44))["amount_score"] == 0.7
    assert score_match(charge(amount=120.00), receipt(amount=88.44)) is None


def test_posted_later_within_three_days_scores_0_8():
    s = score_match(charge(date_='2026-09-30'), receipt(date_=date(2026, 9, 28)))
    assert s["date_score"] == 0.8 and 'posted later' in s["reasons"]
    s7 = score_match(charge(date_='2026-10-04'), receipt(date_=date(2026, 9, 28)))
    assert s7["date_score"] == 0.5
    s9 = score_match(charge(date_='2026-10-08'), receipt(date_=date(2026, 9, 28)))
    assert s9["date_score"] == 0.0


def test_wrong_vendor_is_a_suggestion_but_not_strong():
    s = score_match(charge(vendor='FEDEX OFFICE', card=None), receipt(vendor='Lyft', card=None))
    assert s["vendor_score"] == 0.2 and 'different vendor' in s["reasons"]
    assert MATCH_SUGGEST_MIN <= s["confidence"] < MATCH_STRONG_MIN
    r = rank_candidates(charge(vendor='FEDEX OFFICE', card=None), [receipt(vendor='Lyft', card=None)])
    assert r and r[0]["strong"] is False


def test_vendor_token_overlap_scores_0_6():
    s = score_match(charge(vendor='SQ *CASA VIDEO LLC'), receipt(vendor='Casa Video Rentals'))
    assert s["vendor_score"] in (0.6, 1.0)        # key equal or overlapping words
    s2 = score_match(charge(vendor='Seaport Garage LLC'), receipt(vendor='Parking — Seaport'))
    assert s2["vendor_score"] == 0.6


def test_two_equal_candidates_means_none_strong():
    c = charge()
    a, b = receipt(id=10), receipt(id=11)
    r = rank_candidates(c, [a, b])
    assert len(r) == 2 and r[0]["confidence"] == r[1]["confidence"] == 1.0
    assert not any(x["strong"] for x in r)


def test_strong_needs_a_runner_up_gap_of_0_2():
    c = charge(card=None)
    good = receipt(id=10, card=None)                                 # 1.0
    close = receipt(id=11, vendor='Uber', card=None)                 # 0.84: same amount/day, wrong vendor
    r = rank_candidates(c, [good, close])
    assert r[0]["key"] == 10 and r[0]["strong"] is False             # gap 0.16 < 0.2
    far = receipt(id=12, vendor='Uber', date_=date(2026, 9, 20), card=None)   # amount only + 0.04
    r2 = rank_candidates(c, [good, far])
    assert r2[0]["strong"] is True and len(r2) == 1                  # 0.54 is below the floor


def test_amount_only_coincidence_months_apart_is_never_a_suggestion():
    s = score_match(charge(vendor='FEDEX', date_='2026-03-01', card=None),
                    receipt(vendor='Turboscribe', date_=date(2026, 8, 11), card=None))
    assert s["confidence"] < MATCH_SUGGEST_MIN
    assert rank_candidates(charge(vendor='FEDEX', date_='2026-03-01', card=None),
                           [receipt(vendor='Turboscribe', date_=date(2026, 8, 11), card=None)]) == []


def test_refund_pairs_on_magnitude_with_a_flag():
    s = score_match(charge(amount=-120.00), receipt(amount=120.00))
    assert s and 'refund?' in s["reasons"] and s["amount_score"] == 1.0


def test_missing_amount_or_zero_is_no_candidate():
    assert score_match(charge(amount=None), receipt()) is None
    assert score_match(charge(amount=0), receipt(amount=0)) is None


def test_rank_skips_rejected_pairs():
    r = rank_candidates(charge(), [receipt(id=10), receipt(id=11, vendor='Uber')], key=lambda d: d.id, rejected={10})
    assert [x["key"] for x in r] == [11]


# ── 2. pairing ────────────────────────────────────────────────────────────

def test_suggest_pairs_is_one_to_one_best_first():
    c1 = charge(id=1, amount=88.44)
    c2 = charge(id=2, amount=88.44, date_='2026-09-29', vendor='LYFT')
    r1 = receipt(id=10, amount=88.44)
    pairs = suggest_pairs([c1, c2], [r1], charge_key=lambda t: t.id, doc_key=lambda d: d.id)
    assert len(pairs) == 1 and pairs[0][0].id == 1 and pairs[0][1].id == 10
    pairs2 = suggest_pairs([c1, c2], [r1], rejected={(1, 10)}, charge_key=lambda t: t.id, doc_key=lambda d: d.id)
    assert pairs2[0][0].id == 2


# ── 3. duplicates ─────────────────────────────────────────────────────────

def bp(id, date_, amount=4500.00, qid=None, vendor='Ryan Mauskopf', **kw):
    base = dict(id=id, amount=amount, txn_date=date_, vendor=vendor, card_last4=None,
                qbo_txn_id=qid, qbo_txn_type='BillPayment', source='qbo_sync',
                budget_line_id=None, account_code=None, account_code_name=None, doc_upload_id=None,
                match_status='unmatched', match_confidence=None, match_source=None,
                not_project_expense=False, has_splits=False, has_evidence=False, note=None)
    base.update(kw)
    return NS(**base)


def test_two_qbo_billpayments_a_day_apart_are_twins_and_a_group():
    a, b = bp(1, '2026-10-19', qid='1300000319'), bp(2, '2026-10-20', qid='1300000322')
    assert is_duplicate_twin(a, b) and is_duplicate_twin(b, a)
    groups = find_duplicate_groups([a, b])
    assert len(groups) == 1
    g = groups[0]
    assert g["key"] == dup_group_key([a, b]) == 'dup:1-2'
    assert g["keeper"].id == 1 and g["keeper_reason"] == 'earlier date'


def test_same_qbo_id_is_a_sibling_not_a_duplicate():
    a, b = bp(1, '2026-10-19', qid='1300000319'), bp(2, '2026-10-19', qid='1300000319')
    assert not is_duplicate_twin(a, b)
    assert find_duplicate_groups([a, b]) == []


def test_manual_rows_need_the_same_day_and_four_days_is_too_far_for_qbo():
    a = bp(1, '2026-10-19', qid=None, source='manual_entry')
    b = bp(2, '2026-10-20', qid=None, source='manual_entry')
    assert not is_duplicate_twin(a, b)
    assert is_duplicate_twin(a, bp(3, '2026-10-19', qid=None, source='manual_entry'))
    assert not is_duplicate_twin(bp(1, '2026-10-19', qid='1'), bp(2, '2026-10-23', qid='2'))


def test_keeper_order_receipt_then_coded_then_earlier_then_lower_id():
    coded = bp(5, '2026-10-20', qid='2', budget_line_id=77)
    bare = bp(4, '2026-10-19', qid='1')
    assert pick_keeper([bare, coded])[0].id == 5
    with_receipt = bp(6, '2026-10-21', qid='3', doc_upload_id=9, match_status='confirmed')
    k, why = pick_keeper([bare, coded, with_receipt])
    assert k.id == 6 and why == 'has the receipt'
    same_day = [bp(9, '2026-10-19', qid='8'), bp(8, '2026-10-19', qid='9')]
    k2, why2 = pick_keeper(same_day)
    assert k2.id == 8 and why2 == 'first imported'


def test_keep_both_dismissal_hides_the_group_and_excluded_rows_are_ignored():
    a, b = bp(1, '2026-10-19', qid='1'), bp(2, '2026-10-20', qid='2')
    assert find_duplicate_groups([a, b], dismissed={'dup:1-2'}) == []
    b.not_project_expense = True
    assert find_duplicate_groups([a, b]) == []


def test_delete_mode_hard_delete_only_for_bare_bank_rows():
    assert duplicate_delete_mode(bp(1, '2026-10-19', qid='1')) == 'delete'
    assert duplicate_delete_mode(bp(1, '2026-10-19', qid='1', budget_line_id=3)) == 'merge'
    assert duplicate_delete_mode(bp(1, '2026-10-19', qid='1', doc_upload_id=3)) == 'merge'
    assert duplicate_delete_mode(bp(1, '2026-10-19', qid=None, source='manual_entry')) == 'merge'


# ── 4. merge ──────────────────────────────────────────────────────────────

def test_merge_moves_coding_receipt_reason_splits_and_evidence_onto_the_keeper():
    keeper = bp(1, '2026-10-19', qid='1', note='bank memo')
    extra = bp(2, '2026-10-20', qid='2', budget_line_id=44, account_code=2601,
               account_code_name='Camera package', doc_upload_id=9, match_status='confirmed',
               no_charge_reason=None, payment_method='ach', note='paid on terms')
    sp = NS(transaction_id=2, budget_line_id=44, amount=4500.0)
    ev = NS(transaction_id=2, doc_upload_id=9)
    out = merge_duplicate_into(keeper, extra, splits=[sp], evidence=[ev])
    assert keeper.account_code == 2601 and keeper.account_code_name == 'Camera package'
    assert keeper.doc_upload_id == 9 and keeper.match_status == 'confirmed'
    assert keeper.payment_method == 'ach'
    assert keeper.note == 'bank memo | paid on terms'
    assert sp.transaction_id == 1 and ev.transaction_id == 1
    assert out["splits"] == 1 and out["evidence"] == 1
    # a split parent carries no line of its own
    assert keeper.budget_line_id is None
    assert 'doc_upload_id' in out["moved"] and 'account_code' in out["moved"]


def test_merge_never_overwrites_what_the_keeper_already_has():
    keeper = bp(1, '2026-10-19', qid='1', budget_line_id=10, account_code=2601, doc_upload_id=5,
                match_status='confirmed')
    extra = bp(2, '2026-10-20', qid='2', budget_line_id=44, account_code=3401, doc_upload_id=9,
               match_status='suggested')
    out = merge_duplicate_into(keeper, extra)
    assert keeper.budget_line_id == 10 and keeper.account_code == 2601 and keeper.doc_upload_id == 5
    assert out["moved"] == []


def test_snapshot_is_json_safe_and_complete_enough_to_recreate():
    from decimal import Decimal
    t = bp(7, '2026-10-19', qid='1', amount=Decimal('4500.00'))
    snap = txn_snapshot(t)
    assert snap["amount"] == 4500.0 and snap["qbo_txn_id"] == '1' and snap["source"] == 'qbo_sync'
    assert 'vendor' in snap and 'budget_line_id' in snap and 'not_project_expense' in snap


# ── 5. QuickBooks deletions ───────────────────────────────────────────────

def test_deletion_action_deletes_bare_rows_and_voids_the_rest():
    assert qbo_deletion_action(bp(1, '2026-10-19', qid='1')) == 'delete'
    assert qbo_deletion_action(bp(1, '2026-10-19', qid='1', budget_line_id=3)) == 'void'
    assert qbo_deletion_action(bp(1, '2026-10-19', qid='1', doc_upload_id=3)) == 'void'
    assert qbo_deletion_action(bp(1, '2026-10-19', qid='1', has_splits=True)) == 'void'


def test_voided_flag_is_red_even_while_excluded():
    f = compute_row_flags({"budget_line_id": 5, "assigned": True, "has_backup": True,
                           "not_project_expense": True, "voided": True, "amount": 10})
    assert f["red"] == ['voided_in_qbo']
    assert VOIDED_IN_QBO_REASON == 'voided in QuickBooks'


CDC = {
    "time": "2026-11-03T10:00:00.000-08:00",
    "CDCResponse": [{"QueryResponse": [
        {"BillPayment": [
            {"Id": "1300000322", "status": "Deleted", "domain": "QBO",
             "MetaData": {"LastUpdatedTime": "2026-11-02T09:00:00-08:00"}},
            {"Id": "1300000319", "TxnDate": "2026-10-19", "TotalAmt": 4500.00,
             "VendorRef": {"value": "88", "name": "Ryan Mauskopf"},
             "CheckPayment": {"BankAccountRef": {"value": "7"}},
             "Line": []},
        ]},
        {"Purchase": [
            {"Id": "555", "status": "Deleted", "domain": "QBO"},
            {"Id": "556", "TxnDate": "2026-10-20", "TotalAmt": 12.50,
             "AccountRef": {"value": "99"}, "Line": []},     # not a selected account
        ]},
    ]}],
}


def test_parse_cdc_payload_separates_deleted_stubs_from_rows():
    rows, deleted, server_time = parse_cdc_payload(CDC, account_ids=["7", "41"])
    assert [r["qbo_txn_id"] for r in rows] == ["1300000319"]
    assert rows[0]["vendor"] == "Ryan Mauskopf" and rows[0]["qbo_txn_type"] == "BillPayment"
    assert deleted == [{"qbo_txn_id": "1300000322", "qbo_txn_type": "BillPayment"},
                       {"qbo_txn_id": "555", "qbo_txn_type": "Purchase"}]
    assert server_time is not None and server_time.year == 2026
