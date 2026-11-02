"""QuickBooks Online ingestion module.

Ported from FPBudgetSync/budget_sync.py during the 2026-04-30 cutover.
What changed in the port:

  • Google Sheets writer removed entirely. Sheets are no longer the
    source-of-truth — Transaction rows in the FPBudget DB are.
  • CHART_OF_ACCOUNTS replaced by FP_COA_SECTIONS (the renumbered MMB-
    style structure). DEFAULT_QBO_MAPPINGS rewritten to point at the
    new codes (3700 not 8000 for meals, 3500 not 7000 for travel,
    6000 not 14000 for insurance, etc.).
  • SyncedTransaction model removed — replaced by the partial unique
    index `uq_transaction_qbo` on (project_id, qbo_txn_id, qbo_txn_type)
    in the Transaction table itself. Halves the row count.
  • Receipt model not ported here — receipts live as DocUpload rows
    in FPBudget, and the DocUpload→Transaction linkage is the
    Actuals tab's job (actuals.py).

This module is purely about pulling data from QuickBooks into the
shared `transaction` table. Everything else (matching, linking,
budget-line assignment) is in actuals.py.
"""
import os
import json
import logging
import datetime

import requests

log = logging.getLogger(__name__)


# ── QBO category → FP COA mapping (NEW codes) ─────────────────────────
# Used by sync_project to seed Transaction.suggested_account_code on
# every imported row. The user confirms (or overrides) these in the
# Actuals UI and we promote the user's choice to a CategoryMapping
# row (learned mapping) so subsequent imports of the same QBO
# category auto-suggest correctly.
DEFAULT_QBO_MAPPINGS = {
    "meals and entertainment":      (3700, "Production Meals & Craft Services"),
    "entertainment":                (3700, "Production Meals & Craft Services"),
    "food and beverage":            (3700, "Production Meals & Craft Services"),
    "catering":                     (3700, "Production Meals & Craft Services"),
    "travel":                       (3500, "Travel"),
    "travel expenses":              (3500, "Travel"),
    "transportation":               (3400, "Transportation"),
    "automobile":                   (3400, "Transportation"),
    "shipping":                     (3600, "Shipping"),
    "postage & delivery":           (3600, "Shipping"),
    "freight & courier":            (3600, "Shipping"),
    "insurance":                    (6000, "Insurance"),
    "general liability insurance":  (6000, "Insurance"),
    "professional services":        (6800, "Production Company Fee"),
    "legal & professional fees":    (6500, "Administrative"),
    "legal":                        (6500, "Administrative"),
    "accounting":                   (6500, "Administrative"),
    "office supplies & software":   (6400, "Software & Digital Tools"),
    "office supplies":              (6500, "Administrative"),
    "advertising":                  (6300, "Marketing & EPK"),
    "marketing":                    (6300, "Marketing & EPK"),
    "music":                        (4800, "Music & Composition"),
    "licenses & permits":           (6100, "Licensing"),
    "payroll":                      (2100, "Talent"),
    "wages & salaries":             (2100, "Talent"),
    "equipment rental":             (2600, "Camera Equipment"),
    "rent or lease":                (3300, "Locations"),
    "location":                     (3300, "Locations"),
    "utilities":                    (3300, "Locations"),
    "bank charges & fees":          (6500, "Administrative"),
    "bank charges":                 (6500, "Administrative"),
    "miscellaneous":                (6700, "Miscellaneous"),
}


def _qbo_base_url():
    """Return the QBO API base URL based on QBO_ENVIRONMENT.
    'production' → live; anything else → sandbox."""
    env = (os.getenv("QBO_ENVIRONMENT", "sandbox") or "").lower()
    if env == "production":
        return "https://quickbooks.api.intuit.com/v3/company"
    return "https://sandbox-quickbooks.api.intuit.com/v3/company"


# How many days behind sync_through to re-query on every sync. Catches
# bank-feed items that get accepted into QBO long after their TxnDate.
LOOKBACK_DAYS = 14
# QBO CDC API only returns changes within the last 30 days. Cap at 29.
CDC_MAX_LOOKBACK_DAYS = 29


# ── OAuth token management ────────────────────────────────────────────

def _headers(token):
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def refresh_qbo_token(conn, db):
    """Trade the refresh_token for a new access_token. Intuit may also
    rotate the refresh_token itself; we save whatever they send back."""
    # Audit H4 (2026-07-20): two workers refreshing concurrently each POST the
    # same refresh_token; Intuit rotates it and the second writer clobbers the
    # stored value with a now-dead token, killing QBO auth until re-OAuth.
    # Serialize under an advisory lock and re-read: if another worker already
    # refreshed while we waited, use its fresh token and skip the POST.
    from sqlalchemy import text as _lk_text
    db.session.execute(_lk_text("SELECT pg_advisory_lock(:k)"), {"k": 776500001})
    try:
        _tok_before = conn.access_token
        db.session.refresh(conn)
        if conn.access_token != _tok_before and conn.access_token:
            return  # a sibling worker just refreshed — reuse its token
        return _refresh_qbo_token_locked(conn, db)
    finally:
        try:
            db.session.execute(_lk_text("SELECT pg_advisory_unlock(:k)"), {"k": 776500001})
        except Exception:
            pass


def _refresh_qbo_token_locked(conn, db):
    """Body of refresh_qbo_token — runs holding advisory lock 776500001."""
    import base64
    client_id     = os.getenv("QBO_CLIENT_ID")
    client_secret = os.getenv("QBO_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("QBO_CLIENT_ID / QBO_CLIENT_SECRET not set")
    creds = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = requests.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        headers={
            "Authorization": f"Basic {creds}",
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "refresh_token", "refresh_token": conn.refresh_token},
        timeout=30,
    )
    resp.raise_for_status()
    tokens = resp.json()
    conn.access_token  = tokens["access_token"]
    conn.refresh_token = tokens.get("refresh_token", conn.refresh_token)
    conn.token_expiry  = (datetime.datetime.utcnow()
                          + datetime.timedelta(seconds=tokens.get("expires_in", 3600)))
    db.session.commit()
    return conn


def get_valid_token(conn, db):
    """Return a non-expired access_token, refreshing if we're within
    5 minutes of expiry."""
    if (not conn.token_expiry
            or datetime.datetime.utcnow() >= conn.token_expiry - datetime.timedelta(minutes=5)):
        refresh_qbo_token(conn, db)
    return conn.access_token


# ── QBO data fetch ────────────────────────────────────────────────────

def list_qbo_accounts(conn, db):
    """List active **Bank** and **Credit Card** accounts on the QBO
    realm. These are the only account types that hold spend the user
    cares about for budget actuals (income/equity/etc are noise).
    User selects per-project from this filtered list.
    """
    token = get_valid_token(conn, db)
    query = (
        "SELECT * FROM Account "
        "WHERE Active = true "
        "AND AccountType IN ('Bank', 'Credit Card') "
        "MAXRESULTS 1000"
    )
    resp  = requests.get(
        f"{_qbo_base_url()}/{conn.realm_id}/query",
        headers=_headers(token),
        params={"query": query},
        timeout=30,
    )
    resp.raise_for_status()
    accts = resp.json().get("QueryResponse", {}).get("Account", [])
    return [
        {
            "id":   a["Id"],
            "name": a["Name"],
            "mask": (a.get("AcctNum") or "")[-4:],
            "type": a.get("AccountType", ""),
        }
        for a in accts
    ]


def get_qbo_account_by_id(conn, db, account_id):
    """Look up a single QBO account by its Id. Returns the same dict
    shape as list_qbo_accounts, or None on failure. Used by sync_project
    to resolve unmatched account refs into human names so the UI can
    say "QBO account #221 (My Card •8432, Credit Card)" instead of
    just "QBO account #221"."""
    if not account_id:
        return None
    try:
        token = get_valid_token(conn, db)
        resp = requests.get(
            f"{_qbo_base_url()}/{conn.realm_id}/account/{account_id}",
            headers=_headers(token),
            timeout=15,
        )
        if resp.status_code != 200:
            return None
        a = resp.json().get("Account") or {}
        if not a.get("Id"):
            return None
        return {
            "id":   a["Id"],
            "name": a.get("Name") or f"Account #{a['Id']}",
            "mask": (a.get("AcctNum") or "")[-4:],
            "type": a.get("AccountType", ""),
        }
    except Exception:
        return None


def _extract_txn_fields(txn, entity, acct_ref, account_meta):
    """Pull the fields we care about out of a raw QBO Purchase/Deposit.
    Returns dict ready to spread into a Transaction row, or None if
    the txn lacks an id."""
    txn_id = txn.get("Id", "")
    if not txn_id:
        return None

    # is_expense semantics (review CR-3, 2026-06-04):
    #   Purchase                → expense, UNLESS Credit=true (a CC refund
    #                             recorded as a Purchase credit — was imported
    #                             as a POSITIVE expense, inflating actuals).
    #   BillPayment             → expense (real money out of the bank; was
    #                             False, making vendor-bill spend invisible).
    #   Deposit / CreditCardCredit → credit/refund.
    if entity == "Purchase":
        is_expense = not bool(txn.get("Credit"))
    elif entity == "BillPayment":
        is_expense = True
    else:                       # Deposit, CreditCardCredit
        is_expense = False
    amount     = float(txn.get("TotalAmt", 0))
    raw_date   = txn.get("TxnDate", "")  # YYYY-MM-DD

    # Vendor: pull EVERY clue QBO offers before giving up (owner
    # 2026-10-31: "items come in with no vendor and no account… no clue
    # what something is — pull as much data from QuickBooks as we can").
    # Chain: payee EntityRef → Deposit line payer → first line
    # description → the raw bank text in PrivateNote (better than blank).
    lines = txn.get("Line", []) or []
    _eref = txn.get("EntityRef") or {}
    vendor = _eref.get("name", "")
    # The QuickBooks Vendor id behind the payee (Documents Phase 3): kept as
    # qbo_entity_ref so the charge attributes to the Payee that mirrors
    # that vendor — now, or later when the vendor gets linked. ONLY a
    # Vendor-typed ref: QuickBooks ids are per entity type, so a Customer
    # or Employee ref of the same number must never be read as a vendor.
    entity_ref = None
    if _eref.get("value") and (_eref.get("type") or "Vendor") == "Vendor":
        entity_ref = str(_eref["value"])
    if not vendor:
        for _l in lines:
            _ent = (((_l.get("DepositLineDetail") or {}).get("Entity")) or {})
            if _ent.get("name"):
                vendor = _ent["name"]
                if not entity_ref and _ent.get("value") and _ent.get("type") == "Vendor":
                    entity_ref = str(_ent["value"])
                break
    if not vendor:
        vendor = lines[0].get("Description", "") if lines else ""
    _pnote = (txn.get("PrivateNote") or "").strip()
    if not vendor and _pnote:
        import re as _re_v
        vendor = _re_v.sub(r"\s+", " ", _pnote)[:48]

    # Note: everything useful, de-duplicated — bank memo, doc/ref number,
    # payment type, check number, and the per-line breakdown (the WHO and
    # WHAT of multi-line invoices).
    _bits = []
    if _pnote:
        _bits.append(_pnote)
    if txn.get("DocNumber"):
        _bits.append(f"ref #{txn['DocNumber']}")
    if txn.get("PaymentType"):
        _bits.append(str(txn["PaymentType"]))
    _chk = ((txn.get("CheckPayment") or {}).get("CheckNum"))
    if _chk:
        _bits.append(f"check #{_chk}")
    _lb = []
    for _l in lines[:5]:
        _d = (_l.get("Description") or "").strip()
        if not _d:
            continue
        try:
            _a = float(_l.get("Amount") or 0)
        except (TypeError, ValueError):
            _a = 0
        _lb.append(f"{_d} ${_a:,.2f}" if _a else _d)
    if _lb and (len(_lb) > 1 or _lb[0] != vendor):
        _bits.append(" + ".join(_lb)[:220])
    note = " · ".join(dict.fromkeys(_bits))[:480]

    # Account-name display tag (Credit Card vs Bank, with last-4)
    meta = (account_meta or {}).get(acct_ref, {})
    if meta:
        prefix    = "CC" if meta.get("type") == "Credit Card" else "Acct"
        mask      = meta.get("mask", "")
        acct_disp = f"{prefix}: {mask}" if mask else prefix
    else:
        acct_disp = (txn.get("AccountRef") or {}).get("name", "")

    # First line's account name = the QBO category we'll learn against
    qbo_category = None
    for line in txn.get("Line", []):
        detail   = line.get("AccountBasedExpenseLineDetail") or {}
        cat_name = (detail.get("AccountRef") or {}).get("name")
        if not cat_name:   # item-based purchase lines carry an ItemRef instead
            cat_name = (((line.get("ItemBasedExpenseLineDetail") or {})
                         .get("ItemRef")) or {}).get("name")
        if cat_name:
            qbo_category = cat_name
            break

    return {
        "qbo_txn_id":      txn_id,
        "qbo_txn_type":    entity,        # Purchase | Deposit
        "qbo_account_id":  acct_ref,
        "qbo_category":    qbo_category,
        "txn_date":        raw_date,
        "vendor":          vendor,
        "amount":          amount,
        "is_expense":      is_expense,
        "account_name":    acct_disp,     # display only (Card / Acct + last-4)
        "note":            note,
        "qbo_entity_ref":  entity_ref,    # Vendor id behind EntityRef (may be None)
    }


def fetch_transactions(conn, db, account_ids, start_date, end_date,
                       skip_keys=None, account_meta=None):
    """Fetch Purchases + Deposits + BillPayments by TxnDate range.
    skip_keys is a set of (qbo_txn_id, qbo_txn_type) already in our DB.

    QBO entity coverage:
      Purchase    — credit card / cash purchases (most common)
      Deposit     — incoming deposits
      BillPayment — payments made against bills, recorded against
                    the bank/CC. The line-item Bill is an AP entry
                    not yet paid — including it here would double-
                    count once BillPayment fires.

    Returns:
      list[dict] of transaction rows
      .seen_refs (attribute on the list) = set of every AccountRef
        seen on returned rows. Used by sync_project to surface
        "found N txns on unselected accounts X, Y" warnings.
    """
    token     = get_valid_token(conn, db)
    skip_keys = skip_keys or set()
    # list subclass so we can stash diagnostic metadata on the return
    # value (built-in list rejects attribute assignment).
    class _TxnRows(list):
        pass
    out       = _TxnRows()
    seen_unmatched_refs = set()  # account refs that had rows but weren't selected

    # Diagnostic dump so we can see exactly where data is lost when the
    # sync returns 0 rows. Logged once per fetch.
    log.info(
        f"[qbo] fetch_transactions: env={(os.getenv('QBO_ENVIRONMENT') or 'sandbox').lower()} "
        f"realm={conn.realm_id} accounts={account_ids} "
        f"window={start_date}..{end_date}"
    )

    # Diagnostic: total Purchase count in window with NO filters, so we
    # can tell "QBO has 1 Purchase" from "our query missed N Purchases".
    try:
        diag_q = (f"SELECT COUNT(*) FROM Purchase WHERE "
                  f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'")
        diag = requests.get(
            f"{_qbo_base_url()}/{conn.realm_id}/query",
            headers=_headers(token), params={"query": diag_q}, timeout=30,
        )
        if diag.status_code == 200:
            n = diag.json().get("QueryResponse", {}).get("totalCount")
            log.info(f"[qbo] diagnostic: QBO has totalCount={n} Purchase rows in {start_date}..{end_date}")
    except Exception as _e:
        log.warning(f"[qbo] diagnostic count failed: {_e}")

    # One query per entity (was N×3 — query has no account filter so
    # there's no reason to repeat it per account). CreditCardCredit
    # added to capture CC refunds. JournalEntry omitted — multi-line
    # splits need a different parser.
    account_id_set = set(str(a) for a in account_ids)
    # CreditCardCredit actually queried now (review CR-3 — the comment above
    # claimed it was covered but the tuple never included it, so CC refunds
    # were never imported at all).
    for entity in ("Purchase", "Deposit", "BillPayment", "CreditCardCredit"):
        # Paginate with STARTPOSITION. A single `MAXRESULTS 1000` query silently
        # dropped every row past the first 1000 in a wide window — those txns
        # then become permanently unsyncable once the window ages out of the
        # lookback. (Code review 2026-06-04.)
        rows = []
        start_pos, page_size = 1, 1000
        while True:
            query = (
                f"SELECT * FROM {entity} WHERE "
                f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' "
                f"STARTPOSITION {start_pos} MAXRESULTS {page_size}"
            )
            resp = requests.get(
                f"{_qbo_base_url()}/{conn.realm_id}/query",
                headers=_headers(token), params={"query": query}, timeout=60,
            )
            if resp.status_code != 200:
                log.warning(f"[qbo] {entity} query failed at pos {start_pos}: "
                            f"{resp.status_code} {resp.text[:200]}"
                            + (" (PARTIAL — later pages missing)" if start_pos > 1 else ""))
                break
            page = resp.json().get("QueryResponse", {}).get(entity, [])
            rows.extend(page)
            if len(page) < page_size:
                break
            start_pos += page_size
            if start_pos > 50000:        # safety cap — abnormal volume
                log.warning(f"[qbo] {entity}: pagination safety cap at {start_pos}")
                break
        log.info(f"[qbo] {entity}: QBO returned {len(rows)} rows pre-filter "
                 f"(paged in {((len(rows)-1)//page_size)+1 if rows else 0} request(s))")
        kept = 0
        skipped_dup = 0
        seen_refs = set()
        for txn in rows:
            if entity == "Deposit":
                acct_ref = (txn.get("DepositToAccountRef") or {}).get("value")
            elif entity == "BillPayment":
                acct_ref = (txn.get("CheckPayment") or {}).get("BankAccountRef", {}).get("value") \
                        or (txn.get("CreditCardPayment") or {}).get("CCAccountRef", {}).get("value") \
                        or (txn.get("APAccountRef") or {}).get("value")
            else:
                # Purchase or CreditCardCredit — top-level AccountRef
                acct_ref = (txn.get("AccountRef") or {}).get("value")
            if acct_ref:
                seen_refs.add(acct_ref)
            if acct_ref not in account_id_set:
                continue
            fields = _extract_txn_fields(txn, entity, acct_ref, account_meta)
            if not fields:
                continue
            if (fields["qbo_txn_id"], fields["qbo_txn_type"]) in skip_keys:
                skipped_dup += 1
                continue
            out.append(fields)
            kept += 1
        # Distinguish three "0 imported" cases:
        # (a) kept=0 AND skipped_dup=0 → rows existed but none matched a
        #     user-selected account (warn + surface unmatched refs).
        # (b) kept=0 AND skipped_dup>0 → rows matched but were already
        #     imported on a previous sync. Not a problem; just info.
        # (c) kept>0 → normal success.
        if rows and kept == 0 and skipped_dup == 0:
            log.warning(
                f"[qbo] {entity}: 0 of {len(rows)} matched user-selected accounts. "
                f"Account refs seen on returned rows: {sorted(seen_refs)}"
            )
            # Only refs we actually didn't have selected → surface to UI.
            seen_unmatched_refs.update(seen_refs - account_id_set)
        elif rows and kept == 0 and skipped_dup > 0:
            log.info(
                f"[qbo] {entity}: 0 new (all {skipped_dup} already imported). "
                f"refs={sorted(seen_refs)}"
            )
        elif kept:
            log.info(f"[qbo] {entity}: kept {kept}/{len(rows)} (dup-skipped {skipped_dup})")
    out.sort(key=lambda f: f.get("txn_date") or "")
    # Stash unmatched refs on the return value so sync_project can
    # surface them. Subtract account_ids that DID match so the warning
    # is purely about accounts the user hasn't enabled.
    out_refs = seen_unmatched_refs - account_id_set
    try:
        # Lists allow attribute assignment in Python so this works.
        out.unmatched_account_refs = sorted(out_refs)  # type: ignore[attr-defined]
    except Exception:
        pass
    return out


def fetch_transactions_cdc(conn, db, account_ids, changed_since,
                           skip_keys=None, account_meta=None):
    """Fetch via QBO Change Data Capture (LastUpdatedTime). Catches
    bank-feed items accepted into QBO long after their TxnDate.

    Returns (rows, server_time)."""
    token     = get_valid_token(conn, db)
    skip_keys = skip_keys or set()
    iso       = changed_since.strftime("%Y-%m-%dT%H:%M:%S")
    resp      = requests.get(
        f"{_qbo_base_url()}/{conn.realm_id}/cdc",
        headers=_headers(token),
        params={"entities": "Purchase,Deposit,BillPayment,CreditCardCredit",
                "changedSince": iso},
        timeout=60,
    )
    if resp.status_code != 200:
        log.warning(f"[qbo] CDC failed: {resp.status_code} {resp.text[:200]}")
        return [], None
    payload     = resp.json()
    server_time = _parse_qbo_timestamp(payload.get("time"))
    out         = []
    for response_block in payload.get("CDCResponse", []):
        for query_resp in response_block.get("QueryResponse", []):
            for entity in ("Purchase", "Deposit", "BillPayment", "CreditCardCredit"):
                for txn in query_resp.get(entity, []):
                    if entity == "Deposit":
                        acct_ref = (txn.get("DepositToAccountRef") or {}).get("value")
                    elif entity == "BillPayment":
                        acct_ref = ((txn.get("CheckPayment") or {}).get("BankAccountRef", {}).get("value")
                                    or (txn.get("CreditCardPayment") or {}).get("CCAccountRef", {}).get("value")
                                    or (txn.get("APAccountRef") or {}).get("value"))
                    else:   # Purchase / CreditCardCredit — top-level AccountRef
                        acct_ref = (txn.get("AccountRef") or {}).get("value")
                    if acct_ref not in account_ids:
                        continue
                    fields = _extract_txn_fields(txn, entity, acct_ref, account_meta)
                    if not fields:
                        continue
                    if (fields["qbo_txn_id"], fields["qbo_txn_type"]) in skip_keys:
                        continue
                    out.append(fields)
    return out, server_time


def _parse_qbo_timestamp(ts_str):
    """Parse QBO's ISO 8601 timestamp into a UTC datetime."""
    if not ts_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.datetime.strptime(ts_str, fmt)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except ValueError:
            continue
    log.warning(f"[qbo] Could not parse timestamp: {ts_str}")
    return None


# ── Project sync (the public entry point) ─────────────────────────────

def _find_unreconciled_doc_match(db, project_id, qbo_row):
    """Receipt-first reconciliation matcher.

    Returns the existing doc-sourced Transaction in `project_id` that
    most plausibly corresponds to `qbo_row` (a dict from
    fetch_transactions), or None if no match is good enough.

    Match window: source='doc_upload' AND qbo_txn_id IS NULL AND
    txn_date within ±5 calendar days AND amount within ±$1.00 (or
    exact).

    Vendor match is best-effort: if both sides have a vendor, fuzzy
    match must be ≥ 0.6. If either side is empty, fall back to the
    date+amount match alone (common for cash receipts where the user
    didn't type a vendor name).
    """
    from models import Transaction
    import datetime as _dt
    if not qbo_row.get("txn_date") or qbo_row.get("amount") in (None, ""):
        return None
    try:
        target_date = _dt.datetime.strptime(qbo_row["txn_date"], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
    try:
        target_amt = float(qbo_row["amount"])
    except (ValueError, TypeError):
        return None
    win_lo = (target_date - _dt.timedelta(days=5)).isoformat()
    win_hi = (target_date + _dt.timedelta(days=5)).isoformat()

    # Pull candidates: same project, doc-only, no QBO id yet, in window.
    cands = (db.session.query(Transaction)
             .filter(Transaction.project_id == project_id,
                     Transaction.source == 'doc_upload',
                     Transaction.qbo_txn_id.is_(None),
                     Transaction.txn_date >= win_lo,
                     Transaction.txn_date <= win_hi)
             .all())
    if not cands:
        return None

    def _vendor_sim(a, b):
        a = (a or "").strip().lower()
        b = (b or "").strip().lower()
        if not a or not b:
            return None  # signal: insufficient data
        # Cheap token-overlap: count shared whitespace tokens / max-len.
        ta, tb = set(a.split()), set(b.split())
        if not ta or not tb:
            return 0.0
        return len(ta & tb) / max(len(ta), len(tb))

    best = None
    best_score = 0.0
    for c in cands:
        try:
            c_amt = float(c.amount or 0)
        except (ValueError, TypeError):
            continue
        amt_diff = abs(c_amt - target_amt)
        if amt_diff > 1.00:
            continue
        # Score: 1.0 for exact amount, decay linearly to 0.0 at $1.00.
        amt_score = max(0.0, 1.0 - amt_diff)
        # Date proximity: 1.0 same day, 0.5 at ±5d, linear.
        try:
            c_date = _dt.datetime.strptime(c.txn_date, "%Y-%m-%d").date()
            day_diff = abs((c_date - target_date).days)
        except (ValueError, TypeError):
            day_diff = 5
        date_score = max(0.0, 1.0 - (day_diff / 10.0))
        # Vendor: bonus if it agrees, no penalty if missing.
        vsim = _vendor_sim(c.vendor, qbo_row.get("vendor"))
        if vsim is None:
            vendor_score = 0.5  # neutral when we can't compare
        elif vsim >= 0.6:
            vendor_score = 1.0
        else:
            # Vendors disagree → reject unless amount is exact.
            if amt_diff > 0.01:
                continue
            vendor_score = 0.2
        score = (amt_score * 0.5) + (date_score * 0.3) + (vendor_score * 0.2)
        if score > best_score:
            best_score = score
            best = c
    # Threshold: 0.7 keeps obvious matches, rejects coincidental ones.
    return best if best_score >= 0.7 else None


def repair_is_expense(project_sheet, conn, db, start_date, end_date, apply=False,
                      direction='both'):
    """Repair pre-CR-3 rows: re-fetch QBO txns in [start_date, end_date] WITHOUT
    skipping existing ones, and correct is_expense on already-imported local
    rows whose direction is now known to be wrong:
      • CC refunds (Purchase with Credit=true) imported as POSITIVE expenses
        → flip to credit (they were inflating actuals).
      • BillPayments imported as credits → flip to expense.
    Also reports QBO txns with no local row (e.g. CreditCardCredit refunds that
    predate the entity being queried) so they can be re-synced. Dry-run unless
    apply=True; only the is_expense boolean is touched.

    direction: which corrections to APPLY (reporting always covers both).
      'credit_only'  — only expense→credit flips (removes wrongly-counted spend;
                       always safe).
      'expense_only' — only credit→expense flips (ADDS spend; double-counts if
                       the project also imports the same charges via bank CSV —
                       check overlap first).
      'both'         — apply everything. (Code review 2026-06-04.)"""
    from models import Transaction
    account_ids = json.loads(project_sheet.qbo_account_ids or "[]")
    if not account_ids:
        raise ValueError("No QBO accounts configured for this project.")
    try:
        account_meta = {a["id"]: a for a in list_qbo_accounts(conn, db)}
    except Exception:
        account_meta = {}
    rows = fetch_transactions(conn, db, account_ids, start_date, end_date,
                              skip_keys=set(), account_meta=account_meta)
    flip_to_credit, flip_to_expense, missing = [], [], []
    fixed = 0
    for r in rows:
        local = (Transaction.query
                 .filter(Transaction.project_id == project_sheet.id,
                         Transaction.qbo_txn_id == r["qbo_txn_id"],
                         Transaction.qbo_txn_type == r["qbo_txn_type"])
                 .first())
        if local is None:
            if len(missing) < 200:
                missing.append({"qbo_txn_id": r["qbo_txn_id"], "type": r["qbo_txn_type"],
                                "date": r["txn_date"], "vendor": r["vendor"],
                                "amount": r["amount"], "is_expense": r["is_expense"]})
            continue
        if bool(local.is_expense) != bool(r["is_expense"]):
            rec = {"id": local.id, "date": local.txn_date, "vendor": local.vendor,
                   "amount": float(local.amount or 0), "type": r["qbo_txn_type"],
                   "was_expense": bool(local.is_expense), "now_expense": bool(r["is_expense"])}
            to_credit = bool(local.is_expense)        # expense→credit flip
            (flip_to_credit if to_credit else flip_to_expense).append(rec)
            apply_this = apply and (
                direction == 'both'
                or (direction == 'credit_only' and to_credit)
                or (direction == 'expense_only' and not to_credit))
            if apply_this:
                local.is_expense = bool(r["is_expense"])
                local.updated_at = datetime.datetime.utcnow()
                fixed += 1
    if apply and fixed:
        db.session.commit()
    return {"window": [start_date, end_date], "fetched": len(rows),
            "applied": apply, "direction": direction, "fixed": fixed,
            "flip_to_credit_count": len(flip_to_credit),
            "flip_to_expense_count": len(flip_to_expense),
            "missing_in_local_count": len(missing),
            "flip_to_credit": flip_to_credit[:100],
            "flip_to_expense": flip_to_expense[:100],
            "missing_in_local": missing[:100]}


def _payee_by_qbo_vendor(db):
    """{qbo_vendor_id: crew_member_id} for every linked Payee."""
    from models import CrewMember
    try:
        return {str(v): i for i, v in
                db.session.query(CrewMember.id, CrewMember.qbo_vendor_id)
                .filter(CrewMember.qbo_vendor_id.isnot(None)).all()}
    except Exception as _e:
        log.warning(f"[qbo] payee map unavailable: {_e}")
        return {}


def _attribute_from_entity_ref(txn, entity_ref, payee_by_vendor):
    """Set payee_id / payee_source='qbo' on a synced row from its EntityRef
    when a Payee mirrors that vendor. Fills a NULL; replaces only an
    earlier 'qbo' value (QuickBooks changed the payee); never touches a
    value another source — a person above all — set. Returns True when the
    row changed."""
    if not entity_ref:
        return False
    cm_id = payee_by_vendor.get(str(entity_ref))
    if not cm_id:
        return False
    try:
        from actuals import set_txn_payee
        return set_txn_payee(txn, cm_id, 'qbo', replace=('qbo',))
    except Exception as _e:
        log.warning(f"[qbo] payee attribution failed on txn {getattr(txn, 'id', None)}: {_e}")
        return False


def sync_project(project_sheet, conn, db, start_date=None, end_date=None):
    """Pull QBO transactions for one project into the shared Transaction
    table. Idempotent — re-running is a no-op for txns already imported.

    Args:
      project_sheet: ProjectSheet row. Reads qbo_account_ids,
                     sync_through, last_cdc_sync; writes them back.
      conn:          QBOConnection row.
      db:            SQLAlchemy `db` instance.
      start_date:    'YYYY-MM-DD' (default: project_sheet.sync_through
                     or 90 days ago).
      end_date:      'YYYY-MM-DD' (default: today, capped at yesterday
                     by the safe-watermark logic).

    Returns:
      dict {
        'imported':         int,    # rows newly written to Transaction
        'reconciled':       int,    # merged into existing doc-only rows
                                    # (receipt-first reconciliation)
        'cdc_additions':    int,    # subset of imported that came via CDC
        'sync_through':     str,    # new watermark
        'effective_start':  str,
        'effective_end':    str,
      }
    """
    from models import Transaction, CategoryMapping
    import datetime as dt

    account_ids = json.loads(project_sheet.qbo_account_ids or "[]")
    if not account_ids:
        raise ValueError("No QBO accounts configured for this project.")

    today     = dt.date.today()
    yesterday = (today - dt.timedelta(days=1)).isoformat()
    if not end_date:
        end_date = today.isoformat()
    if not start_date:
        if project_sheet.sync_through:
            start_date = project_sheet.sync_through
        else:
            start_date = (today - dt.timedelta(days=90)).isoformat()

    # 1. Safe end — never advance past yesterday (today's bank feed
    # may still be settling).
    safe_end = min(end_date, yesterday)

    # 2. Overlap window — re-query LOOKBACK_DAYS behind the watermark.
    if project_sheet.sync_through:
        try:
            overlap_start = (
                dt.datetime.strptime(project_sheet.sync_through, "%Y-%m-%d")
                - dt.timedelta(days=LOOKBACK_DAYS)
            ).strftime("%Y-%m-%d")
            effective_start = min(start_date, overlap_start)
        except ValueError:
            effective_start = start_date
    else:
        effective_start = start_date

    log.info(
        f"[qbo {project_sheet.name}] sync window {effective_start} → {safe_end} "
        f"(requested {start_date} → {end_date}, overlap={LOOKBACK_DAYS}d)"
    )

    # 3. Build dedupe set from the Transaction table directly. The
    # partial unique index uq_transaction_qbo enforces idempotency at
    # the DB level too — this in-memory check is just a fast path.
    existing = (db.session.query(Transaction.qbo_txn_id, Transaction.qbo_txn_type)
                .filter(Transaction.project_id == project_sheet.id,
                        Transaction.qbo_txn_id.isnot(None))
                .all())
    skip_keys = {(qid, qtype) for qid, qtype in existing}

    # 4. Account metadata for nicer display strings.
    try:
        all_accounts = list_qbo_accounts(conn, db)
        account_meta = {a["id"]: a for a in all_accounts}
    except Exception as _ae:
        log.warning(f"[qbo] account metadata fetch failed: {_ae}")
        account_meta = {}

    # 5. Primary fetch — by TxnDate range with overlap.
    # Fetch WITHOUT skipping known keys: rows we already imported flow
    # through so blanks can be back-filled (a charge categorized or
    # named in QBO AFTER our first pull now updates here — previously it
    # was frozen at first sight forever). Inserts still dedupe below.
    rows = fetch_transactions(
        conn, db, account_ids, effective_start, safe_end,
        skip_keys=set(), account_meta=account_meta,
    )
    seen = {(r["qbo_txn_id"], r["qbo_txn_type"]) for r in rows}

    # 6. CDC sweep — by LastUpdatedTime, catches late-accepted items.
    cdc_count       = 0
    cdc_server_time = None
    try:
        now_utc = dt.datetime.utcnow()
        if project_sheet.last_cdc_sync:
            earliest_allowed = now_utc - dt.timedelta(days=CDC_MAX_LOOKBACK_DAYS)
            cdc_since = max(project_sheet.last_cdc_sync, earliest_allowed)
        else:
            cdc_since = max(
                dt.datetime.strptime(effective_start, "%Y-%m-%d") - dt.timedelta(days=1),
                now_utc - dt.timedelta(days=CDC_MAX_LOOKBACK_DAYS),
            )
        cdc_rows, cdc_server_time = fetch_transactions_cdc(
            conn, db, account_ids, cdc_since,
            skip_keys=set(), account_meta=account_meta,
        )
        for r in cdc_rows:
            key = (r["qbo_txn_id"], r["qbo_txn_type"])
            if key not in seen:
                rows.append(r)
                seen.add(key)
                cdc_count += 1
        if cdc_count:
            log.info(f"[qbo {project_sheet.name}] CDC added {cdc_count} late-accepted txns")
    except Exception as _ce:
        log.warning(f"[qbo {project_sheet.name}] CDC sweep failed (non-fatal): {_ce}")

    # 7. Write to Transaction. Build a one-shot lookup of learned
    # category → COA mappings so we don't hit the DB per row.
    mappings = {
        m.qbo_category.lower(): (m.coa_code, m.coa_name)
        for m in CategoryMapping.query.filter(CategoryMapping.vendor_name.is_(None)).all()
    }
    # Seed defaults for any mapping the DB doesn't yet have a learned
    # row for (don't override learned mappings — those have priority).
    for cat, (code, name) in DEFAULT_QBO_MAPPINGS.items():
        mappings.setdefault(cat, (code, name))

    _existing_map = {}
    for _t in (Transaction.query
               .filter(Transaction.project_id == project_sheet.id,
                       Transaction.qbo_txn_id.isnot(None)).all()):
        _existing_map[(_t.qbo_txn_id, _t.qbo_txn_type)] = _t

    # Payee attribution for free (Documents Phase 3): EntityRef.value →
    # the Payee carrying that qbo_vendor_id. No matching, no guessing. A
    # vendor QuickBooks names that no Payee mirrors yet attributes nobody
    # (its id is kept on the row for when it is linked).
    payee_by_vendor = _payee_by_qbo_vendor(db)
    attributed = 0

    imported = 0
    reconciled = 0
    enriched = 0
    _seen_this_run = set()
    for r in rows:
        _key = (r["qbo_txn_id"], r["qbo_txn_type"])
        if _key in _seen_this_run:
            continue
        _seen_this_run.add(_key)
        _ex = _existing_map.get(_key)
        if _ex is not None:
            # Already imported — fill BLANKS only (never touch values the
            # user, or an earlier pull, already set).
            _f = False
            if not (_ex.vendor or '').strip() and r["vendor"]:
                _ex.vendor = r["vendor"]; _f = True
            if not (_ex.note or '').strip() and r["note"]:
                _ex.note = r["note"]; _f = True
            if not _ex.qbo_category and r["qbo_category"]:
                _ex.qbo_category = r["qbo_category"]; _f = True
                if _ex.suggested_account_code is None and not _ex.account_code:
                    _m = mappings.get(r["qbo_category"].lower())
                    if _m:
                        _ex.suggested_account_code = _m[0]
            if r.get("qbo_entity_ref") and not getattr(_ex, 'qbo_entity_ref', None):
                _ex.qbo_entity_ref = r["qbo_entity_ref"]; _f = True
            if _attribute_from_entity_ref(_ex, r.get("qbo_entity_ref"), payee_by_vendor):
                attributed += 1; _f = True
            if _f:
                enriched += 1
            continue
        suggested_code = suggested_name = None
        if r.get("qbo_category"):
            mapping = mappings.get(r["qbo_category"].lower())
            if mapping:
                suggested_code, suggested_name = mapping

        # Cross-project claimed check: same QBO txn id assigned
        # elsewhere with an account_code → flag this one as not-mine.
        already_claimed = (
            db.session.query(Transaction)
            .filter(Transaction.qbo_txn_id   == r["qbo_txn_id"],
                    Transaction.qbo_txn_type == r["qbo_txn_type"],
                    Transaction.project_id   != project_sheet.id,
                    Transaction.account_code.isnot(None))
            .first()
        )

        # ── Receipt-first reconciliation ─────────────────────────────
        # If the user already uploaded a receipt for this expense and
        # tagged it to a budget line BEFORE the QBO charge synced, we
        # have a doc_upload-sourced Transaction in our DB that's the
        # "same" expense. Detect that and merge into it instead of
        # creating a parallel row (which would double-count actuals).
        # Match criteria: same project, source='doc_upload', no
        # qbo_txn_id yet, txn_date within ±5d, amount within ±$1.00.
        existing_doc_txn = _find_unreconciled_doc_match(
            db, project_sheet.id, r,
        )
        if existing_doc_txn is not None:
            # Merge: promote the doc-only row into a reconciled row by
            # stamping the QBO ingest fields onto it. Preserve the
            # user's existing budget_line_id / doc_upload_id work.
            existing_doc_txn.source         = 'reconciled'
            existing_doc_txn.qbo_txn_id     = r["qbo_txn_id"]
            existing_doc_txn.qbo_txn_type   = r["qbo_txn_type"]
            existing_doc_txn.qbo_account_id = r["qbo_account_id"]
            existing_doc_txn.qbo_category   = r["qbo_category"]
            # Trust QBO for the canonical amount + vendor + date; the
            # user-typed receipt values may have been approximate.
            _amt_delta = abs(float(existing_doc_txn.amount or 0)
                             - float(r["amount"] or 0))
            existing_doc_txn.amount         = r["amount"]
            existing_doc_txn.is_expense     = r["is_expense"]
            # Vendor: prefer the receipt's OCR'd / human-typed value
            # over QBO's. QBO's vendor field carries credit-card processor
            # noise — "AMAZON MKTPL*BJ3NU6KN0", "LYFT *RIDE MON 8AM" —
            # whereas the receipt OCR produces the clean human-readable
            # name ("Amazon", "Lyft"). Falls back to QBO if the doc has
            # no vendor on it. 2026-05-15 user report: had to manually
            # "overwrite name" via the modal after every reconcile —
            # causing a page reload and losing scroll position. Inverting
            # the precedence here removes that step entirely.
            existing_doc_txn.vendor         = existing_doc_txn.vendor or r["vendor"]
            existing_doc_txn.txn_date       = r["txn_date"] or existing_doc_txn.txn_date
            if r.get("qbo_entity_ref") and not getattr(existing_doc_txn, 'qbo_entity_ref', None):
                existing_doc_txn.qbo_entity_ref = r["qbo_entity_ref"]
            if _attribute_from_entity_ref(existing_doc_txn, r.get("qbo_entity_ref"), payee_by_vendor):
                attributed += 1
            # Suggested, never confirmed (owner decision 2026-10-30: every
            # receipt match, exact or near, waits for a click — Concur's
            # "smart matching, confirmed by a person"). Audit H3 (2026-07-20)
            # auto-confirmed exact-amount pairings; that branch is gone. The
            # user's coding on the row is untouched either way; only the
            # match state is left for them to confirm or unmatch.
            existing_doc_txn.match_status = 'suggested'
            if _amt_delta > 0.01:
                _dnote = (f"[QBO reconcile: bank amount differed from "
                          f"receipt by ${_amt_delta:,.2f} — review]")
                if _dnote not in (existing_doc_txn.note or ''):
                    existing_doc_txn.note = (((existing_doc_txn.note or '')
                                              + ' ' + _dnote).strip())[:500]
            # Fire cross-project claim propagation: reconciliation
            # stamped a qbo_txn_id onto this doc row, which means QBO
            # siblings on other projects now share that id and should
            # be claimed (or fuzzy-matched siblings, since the row also
            # has a doc_upload_id — receipt = affirmative claim signal).
            # Import locally to dodge circular import at module-load
            # (app.py imports qbo_sync; qbo_sync importing app.py would
            # cycle). app must be initialized when sync runs anyway.
            try:
                from app import _sync_claim_state as _scs
                _scs(existing_doc_txn)
            except Exception as _re:
                log.warning(f"[qbo {project_sheet.name}] cross-project claim "
                            f"on reconciled txn #{existing_doc_txn.id} failed: {_re}")
            log.info(
                f"[qbo {project_sheet.name}] reconciled QBO {r['qbo_txn_type']}"
                f"/{r['qbo_txn_id']} → existing doc Transaction id={existing_doc_txn.id}"
            )
            reconciled += 1
            continue

        _new = Transaction(
            project_id              = project_sheet.id,
            source                  = 'qbo_sync',
            qbo_txn_id              = r["qbo_txn_id"],
            qbo_txn_type            = r["qbo_txn_type"],
            qbo_account_id          = r["qbo_account_id"],
            qbo_category            = r["qbo_category"],
            qbo_entity_ref          = r.get("qbo_entity_ref"),
            txn_date                = r["txn_date"],
            vendor                  = r["vendor"],
            amount                  = r["amount"],
            is_expense              = r["is_expense"],
            account_code_name       = r["account_name"],   # display string (CC: 1234)
            note                    = r["note"],
            not_project_expense     = bool(already_claimed),
            suggested_account_code  = suggested_code,
            account_code            = None,                # user-confirms via Actuals UI
            match_status            = 'unmatched',
        )
        if _attribute_from_entity_ref(_new, r.get("qbo_entity_ref"), payee_by_vendor):
            attributed += 1
        db.session.add(_new)
        imported += 1

    # 8. Watermark advance.
    project_sheet.last_synced  = datetime.datetime.utcnow()
    project_sheet.sync_through = safe_end
    if cdc_server_time:
        project_sheet.last_cdc_sync = cdc_server_time
    elif imported or not project_sheet.last_cdc_sync:
        project_sheet.last_cdc_sync = datetime.datetime.utcnow()

    db.session.commit()
    log.info(f"[qbo {project_sheet.name}] imported {imported} txns "
             f"(reconciled={reconciled}, enriched={enriched}, cdc={cdc_count}, "
             f"payee-attributed={attributed}); watermark → {safe_end}")
    # Surface unmatched account refs — transactions QBO returned that
    # weren't on accounts the user selected for this project. Resolve
    # to display names if we can so the UI can say "add Chase 8432" not
    # "add account 221".
    unmatched_refs = list(getattr(rows, 'unmatched_account_refs', []) or [])
    unmatched_named = []
    if unmatched_refs:
        for ref in unmatched_refs:
            meta = account_meta.get(ref) if account_meta else None
            # If we don't have metadata in the cached list (because the
            # ref's account type was filtered out previously, or the
            # account is inactive), look it up directly by id so the
            # UI can show the real name + account type.
            if not meta:
                meta = get_qbo_account_by_id(conn, db, ref)
            if meta:
                lbl = meta.get("name") or f"#{ref}"
                if meta.get("mask"):
                    lbl = f"{lbl} (•{meta['mask']})"
                if meta.get("type"):
                    lbl = f"{lbl} [{meta['type']}]"
                unmatched_named.append({"id": ref, "label": lbl})
            else:
                unmatched_named.append({"id": ref, "label": f"QBO account #{ref}"})
    return {
        "imported":        imported,
        "enriched":        enriched,      # existing rows whose blanks got filled
        "reconciled":      reconciled,    # merged into existing doc Transactions
        "cdc_additions":   cdc_count,
        "attributed":      attributed,    # rows given a payee from EntityRef
        "sync_through":    safe_end,
        "effective_start": effective_start,
        "effective_end":   safe_end,
        "unmatched_accounts": unmatched_named,
    }


def seed_default_category_mappings(db):
    """Idempotent: insert any DEFAULT_QBO_MAPPINGS entries that don't
    already exist as CategoryMapping rows. Called once at boot so
    fresh installs auto-suggest sensibly without learning data."""
    from models import CategoryMapping
    inserted = 0
    for cat, (code, name) in DEFAULT_QBO_MAPPINGS.items():
        exists = (CategoryMapping.query
                  .filter(db.func.lower(CategoryMapping.qbo_category) == cat,
                          CategoryMapping.vendor_name.is_(None))
                  .first())
        if exists:
            continue
        db.session.add(CategoryMapping(
            qbo_category=cat, vendor_name=None,
            coa_code=code, coa_name=name,
            usage_count=0,
        ))
        inserted += 1
    if inserted:
        db.session.commit()
        log.info(f"[qbo] seeded {inserted} default category mappings")
    return inserted


# ── Vendor mirror (DOCUMENTS_HANDOFF Phase 3, 2026-11-02) ─────────────
#
# QuickBooks is the system of record for vendors and 1099s. sync_vendors()
# mirrors the Vendor entity onto Payees ONE WAY (QuickBooks → FPBudget):
# every QuickBooks vendor lands in the qbo_vendor table; a Payee that
# carries the vendor's id has its mirrored fields refreshed; a Payee whose
# name matches a vendor's DisplayName exactly (and carries no id yet) is
# linked; everything else waits for the Link screen (/admin/qbo-vendors).
# No Payee is ever created here, and NOTHING is ever written to QuickBooks
# from this function.
#
# The ONE write FPBudget makes to QuickBooks is Create (ensure_vendor_for_
# payee → create_vendor): a Payee with no qbo_vendor_id may become a NEW
# vendor there. There is deliberately no update, sparse-update or delete
# helper in this module — a record that exists in QuickBooks is
# QuickBooks's (Steven, Nov 1 2026).

VENDOR_PAGE_SIZE = 1000


def _qbo_str(v, n=200):
    s = (v or "")
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    return s[:n] if s else None


def vendor_fields(v):
    """The mirrored fields of one raw QBO Vendor dict → a plain dict keyed
    like QBOVendor's columns. Pure (tests pin it). No TIN is read."""
    vid = v.get("Id")
    if not vid:
        return None
    addr = v.get("BillAddr") or {}
    meta = v.get("MetaData") or {}
    return {
        "qbo_vendor_id": str(vid),
        "display_name":  _qbo_str(v.get("DisplayName")),
        "company_name":  _qbo_str(v.get("CompanyName")),
        "given_name":    _qbo_str(v.get("GivenName"), 100),
        "family_name":   _qbo_str(v.get("FamilyName"), 100),
        "email":         _qbo_str((v.get("PrimaryEmailAddr") or {}).get("Address")),
        "phone":         _qbo_str((v.get("PrimaryPhone") or {}).get("FreeFormNumber"), 50),
        "address_line1": _qbo_str(addr.get("Line1")),
        "address_line2": _qbo_str(" ".join(x for x in (addr.get("Line2"), addr.get("Line3")) if x)),
        "city":          _qbo_str(addr.get("City"), 120),
        "state":         _qbo_str(addr.get("CountrySubDivisionCode"), 40),
        "postal_code":   _qbo_str(addr.get("PostalCode"), 20),
        "vendor_1099":   (bool(v["Vendor1099"]) if v.get("Vendor1099") is not None else None),
        "active":        (bool(v["Active"]) if v.get("Active") is not None else None),
        "sync_token":    _qbo_str(v.get("SyncToken"), 20),
        "qbo_updated_at": _parse_qbo_timestamp(meta.get("LastUpdatedTime")),
    }


# CrewMember columns the mirror OVERWRITES on every sync for a linked
# Payee (QuickBooks owns them). `name` is NOT among them: it is the label
# every budget, call sheet and picker shows and a person Payee's name is
# theirs; the QuickBooks DisplayName lives on the qbo_vendor row. A value
# QuickBooks leaves EMPTY does not blank the local copy.
MIRRORED_PAYEE_FIELDS = ('legal_name', 'email', 'phone', 'address_line1',
                         'address_line2', 'city', 'state', 'postal_code')


def apply_vendor_to_payee(cm, f, now=None):
    """Write the mirrored fields of vendor dict `f` (vendor_fields()) onto
    CrewMember `cm`. Pure on the object (no DB). Returns the list of fields
    that changed."""
    now = now or datetime.datetime.utcnow()
    changed = []

    def _set(field, value):
        if value in (None, '') or getattr(cm, field, None) == value:
            return
        setattr(cm, field, value)
        changed.append(field)

    _set('legal_name', f.get("company_name") or f.get("display_name"))
    if f.get("company_name"):
        _set('company', f["company_name"])
    for fld in ('email', 'phone', 'address_line1', 'address_line2', 'city', 'state', 'postal_code'):
        _set(fld, f.get(fld))
    if f.get("vendor_1099") is not None and cm.qbo_vendor_1099 != f["vendor_1099"]:
        cm.qbo_vendor_1099 = f["vendor_1099"]; changed.append('qbo_vendor_1099')
    # Active mirrors for vendor / loan-out Payees (the vendor record IS the
    # payee); a person Payee's crew-roster state is FPBudget's.
    if f.get("active") is not None and getattr(cm, 'kind', None) in ('vendor', 'loan_out') \
            and bool(cm.active) != f["active"]:
        cm.active = f["active"]; changed.append('active')
    if f.get("sync_token") and cm.qbo_sync_token != f["sync_token"]:
        cm.qbo_sync_token = f["sync_token"]
    cm.qbo_synced_at = now
    if not cm.qbo_origin:
        cm.qbo_origin = 'qbo'
    return changed


def match_vendor_to_payee(f, by_vendor_id, by_name):
    """Which Payee a QuickBooks vendor is, by the two rules and nothing
    else. Pure (tests pin it).

      by_vendor_id  {qbo_vendor_id: payee}   — a linked Payee wins
      by_name       {lower(name): [payee…]}  — over name AND legal_name of
                    Payees with NO qbo_vendor_id

    Returns (payee, how) with how in 'id' | 'name', or (None, None): a
    DisplayName that two unlinked Payees share is ambiguous and is left
    for the Link screen; nothing is ever fuzzy."""
    cm = by_vendor_id.get(f["qbo_vendor_id"])
    if cm is not None:
        return cm, 'id'
    key = (f.get("display_name") or '').strip().lower()
    if key:
        hits = by_name.get(key) or []
        uniq = {id(x): x for x in hits}
        if len(uniq) == 1:
            return next(iter(uniq.values())), 'name'
    return None, None


def _vendor_query(conn, db, where="", start_pos=1, page_size=VENDOR_PAGE_SIZE):
    token = get_valid_token(conn, db)
    query = (f"SELECT * FROM Vendor {where} "
             f"STARTPOSITION {start_pos} MAXRESULTS {page_size}").replace("  ", " ")
    resp = requests.get(f"{_qbo_base_url()}/{conn.realm_id}/query",
                        headers=_headers(token), params={"query": query}, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Vendor query failed: {resp.status_code} {resp.text[:200]}")
    payload = resp.json()
    return (payload.get("QueryResponse", {}).get("Vendor", []) or [],
            _parse_qbo_timestamp(payload.get("time")))


def fetch_vendors(conn, db, changed_since=None):
    """Every QuickBooks vendor (active and inactive) on a full pull, or
    those changed since `changed_since` through ChangeDataCapture.
    Returns (list of raw Vendor dicts, server_time)."""
    if changed_since is None:
        rows, server_time, start_pos = [], None, 1
        while True:
            page, t = _vendor_query(conn, db, "WHERE Active IN (true, false)", start_pos)
            server_time = server_time or t
            rows.extend(page)
            if len(page) < VENDOR_PAGE_SIZE:
                break
            start_pos += VENDOR_PAGE_SIZE
            if start_pos > 50000:
                log.warning("[qbo-vendors] pagination safety cap")
                break
        return rows, server_time
    token = get_valid_token(conn, db)
    resp = requests.get(f"{_qbo_base_url()}/{conn.realm_id}/cdc",
                        headers=_headers(token),
                        params={"entities": "Vendor",
                                "changedSince": changed_since.strftime("%Y-%m-%dT%H:%M:%S")},
                        timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Vendor CDC failed: {resp.status_code} {resp.text[:200]}")
    payload = resp.json()
    out = []
    for block in payload.get("CDCResponse", []) or []:
        for qr in block.get("QueryResponse", []) or []:
            out.extend(qr.get("Vendor", []) or [])
    return out, _parse_qbo_timestamp(payload.get("time"))


def sync_vendors(conn, db, fetch=None):
    """One-way mirror QuickBooks Vendor → FPBudget (see the section
    comment). Full pull when conn.vendor_cdc_since is NULL or older than
    the CDC window, CDC otherwise. `fetch` lets tests inject the pull.

    Returns {"mode", "fetched", "upserted", "refreshed", "linked",
             "unlinked", "ambiguous", "server_time"}. Commits. Never
    writes to QuickBooks."""
    from models import CrewMember, QBOVendor
    now = datetime.datetime.utcnow()
    since = getattr(conn, 'vendor_cdc_since', None)
    if since is not None and (now - since).days >= CDC_MAX_LOOKBACK_DAYS:
        since = None
    mode = 'cdc' if since else 'full'
    raw, server_time = (fetch or fetch_vendors)(conn, db, since)

    members = CrewMember.query.all()
    by_vendor_id = {str(m.qbo_vendor_id): m for m in members if m.qbo_vendor_id}
    by_name = {}
    for m in members:
        # Linked already, or a W-2 employee (Wrapbook's, never a vendor):
        # not a candidate for the exact-name link.
        if m.qbo_vendor_id or getattr(m, 'is_w2_employee', False):
            continue
        for raw_name in (m.name, m.legal_name):
            k = (raw_name or '').strip().lower()
            if k and m not in by_name.setdefault(k, []):
                by_name[k].append(m)

    upserted = refreshed = linked = unlinked = ambiguous = 0
    for v in raw:
        if (v.get("status") or '').lower() == 'deleted':
            continue
        f = vendor_fields(v)
        if not f:
            continue
        row = QBOVendor.query.get(f["qbo_vendor_id"])
        if row is None:
            row = QBOVendor(qbo_vendor_id=f["qbo_vendor_id"])
            db.session.add(row)
        for k, val in f.items():
            setattr(row, k, val)
        row.synced_at = now
        upserted += 1

        cm, how = match_vendor_to_payee(f, by_vendor_id, by_name)
        if cm is None:
            key = (f.get("display_name") or '').strip().lower()
            if key and len(by_name.get(key) or []) > 1:
                ambiguous += 1
                log.info(f"[qbo-vendors] '{f.get('display_name')}' matches "
                         f"{len(by_name[key])} payees by name — left for the Link screen")
            unlinked += 1
            continue
        if how == 'name':
            cm.qbo_vendor_id = f["qbo_vendor_id"]
            by_vendor_id[f["qbo_vendor_id"]] = cm
            for raw_name in (cm.name, cm.legal_name):
                k = (raw_name or '').strip().lower()
                if k and cm in (by_name.get(k) or []):
                    by_name[k].remove(cm)
            linked += 1
            log.info(f"[qbo-vendors] linked payee #{cm.id} '{cm.name}' to QuickBooks "
                     f"vendor {f['qbo_vendor_id']} by exact DisplayName")
        else:
            refreshed += 1
        apply_vendor_to_payee(cm, f, now)

    # Watermark: the server's clock (CDC compares against it), minus a
    # small overlap so an edit landing during the pull is not missed.
    conn.vendor_cdc_since = (server_time or now) - datetime.timedelta(minutes=5)
    db.session.commit()
    log.info(f"[qbo-vendors] {mode}: fetched {len(raw)}, upserted {upserted}, "
             f"refreshed {refreshed}, linked {linked}, unlinked {unlinked}, ambiguous {ambiguous}")
    return {"mode": mode, "fetched": len(raw), "upserted": upserted,
            "refreshed": refreshed, "linked": linked, "unlinked": unlinked,
            "ambiguous": ambiguous, "server_time": server_time}


# ── The one write: Create a Vendor from a Payee ───────────────────────

def _qbo_quote(s):
    """A string literal for a QuickBooks query (single quotes doubled by
    backslash-escaping, per the QBO query grammar)."""
    return "'" + (s or "").replace("\\", "\\\\").replace("'", "\\'") + "'"


def find_vendor_by_display_name(conn, db, display_name):
    """The QuickBooks Vendor whose DisplayName equals `display_name`
    exactly (QuickBooks compares DisplayName case-insensitively and keeps
    it unique), active or not. Returns the raw Vendor dict or None."""
    name = (display_name or '').strip()
    if not name:
        return None
    rows, _t = _vendor_query(conn, db,
                             f"WHERE DisplayName = {_qbo_quote(name)} AND Active IN (true, false)",
                             1, 10)
    for v in rows:
        if (v.get("DisplayName") or '').strip().lower() == name.lower():
            return v
    return rows[0] if rows else None


def vendor_create_payload(cm, w9_on_file=False):
    """The body of the ONE write: a new QuickBooks Vendor from a Payee's
    own fields. Pure (tests pin it). DisplayName = legal_name or name;
    CompanyName, PrimaryEmailAddr, PrimaryPhone, BillAddr when present;
    Vendor1099 = true when a W-9 is on file. Carries NO Id and NO
    SyncToken — this body can only ever create."""
    display = (cm.legal_name or cm.name or '').strip()
    body = {"DisplayName": display[:100]}
    company = (cm.company or (cm.legal_name if getattr(cm, 'kind', None) != 'person' else None) or '').strip()
    if company:
        body["CompanyName"] = company[:100]
    if getattr(cm, 'kind', None) == 'person' and not cm.company:
        parts = (cm.name or '').strip().split()
        if len(parts) >= 2:
            body["GivenName"] = parts[0][:100]
            body["FamilyName"] = " ".join(parts[1:])[:100]
    if cm.email:
        body["PrimaryEmailAddr"] = {"Address": cm.email.strip()[:100]}
    if cm.phone:
        body["PrimaryPhone"] = {"FreeFormNumber": cm.phone.strip()[:30]}
    addr = {}
    if cm.address_line1:
        addr["Line1"] = cm.address_line1.strip()[:500]
    if cm.address_line2:
        addr["Line2"] = cm.address_line2.strip()[:500]
    if cm.city:
        addr["City"] = cm.city.strip()[:255]
    if cm.state:
        addr["CountrySubDivisionCode"] = cm.state.strip()[:255]
    if cm.postal_code:
        addr["PostalCode"] = cm.postal_code.strip()[:30]
    if addr:
        body["BillAddr"] = addr
    body["Vendor1099"] = bool(w9_on_file)
    return body


def create_vendor(conn, db, body):
    """POST a NEW Vendor to QuickBooks. Refuses a body that names an
    existing record (Id / SyncToken / sparse) so this helper can never
    update one. Returns the created Vendor dict."""
    for k in ("Id", "SyncToken", "sparse"):
        if k in body:
            raise ValueError(f"create_vendor refuses a body carrying {k!r}: "
                             "FPBudget never updates a QuickBooks record")
    token = get_valid_token(conn, db)
    resp = requests.post(f"{_qbo_base_url()}/{conn.realm_id}/vendor",
                         headers={**_headers(token), "Content-Type": "application/json"},
                         json=body, timeout=60)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Vendor create failed: {resp.status_code} {resp.text[:300]}")
    v = (resp.json() or {}).get("Vendor") or {}
    if not v.get("Id"):
        raise RuntimeError("Vendor create returned no Id")
    return v


def ensure_vendor_for_payee(conn, db, cm, w9_on_file=False):
    """Give a Payee a QuickBooks vendor record, by the Nov 1 rule:

      * refuse (ValueError) when the Payee already carries a qbo_vendor_id
        or is a W-2 employee (employees go to Wrapbook);
      * an exact DisplayName match in QuickBooks → LINK only (store its Id,
        mirror its fields, qbo_origin='qbo'); never duplicated, never
        updated;
      * else CREATE a Vendor from the Payee's fields and store the Id with
        qbo_origin='fpbudget'. After that the record is QuickBooks's.

    Does NOT commit. Returns {"action": 'linked'|'created', "vendor_id",
    "display_name", "payload"}."""
    from models import QBOVendor
    if cm.qbo_vendor_id:
        raise ValueError("This payee is already linked to a QuickBooks vendor.")
    if getattr(cm, 'is_w2_employee', False):
        raise ValueError("W-2 employees are paid through Wrapbook and are not QuickBooks vendors.")
    body = vendor_create_payload(cm, w9_on_file)
    if not body["DisplayName"]:
        raise ValueError("The payee needs a name before it can go to QuickBooks.")
    existing = find_vendor_by_display_name(conn, db, body["DisplayName"])
    now = datetime.datetime.utcnow()
    if existing:
        v, action, origin = existing, 'linked', 'qbo'
    else:
        v, action, origin = create_vendor(conn, db, body), 'created', 'fpbudget'
    f = vendor_fields(v)
    cm.qbo_vendor_id = f["qbo_vendor_id"]
    cm.qbo_origin = origin
    apply_vendor_to_payee(cm, f, now)
    row = QBOVendor.query.get(f["qbo_vendor_id"]) or QBOVendor(qbo_vendor_id=f["qbo_vendor_id"])
    for k, val in f.items():
        setattr(row, k, val)
    row.synced_at = now
    db.session.add(row)
    return {"action": action, "vendor_id": f["qbo_vendor_id"],
            "display_name": f.get("display_name"), "payload": body if action == 'created' else None}
