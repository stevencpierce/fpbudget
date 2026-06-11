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
    "office supplies & software":   (6400, "Web Build & Software Development"),
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

    vendor = (txn.get("EntityRef") or {}).get("name", "")
    if not vendor:
        lines  = txn.get("Line", [])
        vendor = lines[0].get("Description", "") if lines else ""

    note = txn.get("PrivateNote", "") or txn.get("DocNumber", "")

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
        query = (
            f"SELECT * FROM {entity} WHERE "
            f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' "
            f"MAXRESULTS 1000"
        )
        resp = requests.get(
            f"{_qbo_base_url()}/{conn.realm_id}/query",
            headers=_headers(token), params={"query": query}, timeout=60,
        )
        if resp.status_code != 200:
            log.warning(f"[qbo] {entity} query failed: {resp.status_code} {resp.text[:200]}")
            continue
        rows = resp.json().get("QueryResponse", {}).get(entity, [])
        log.info(f"[qbo] {entity}: QBO returned {len(rows)} rows pre-filter")
        if len(rows) >= 1000:
            log.warning(f"[qbo] {entity} query hit MAXRESULTS 1000 — narrow the date range.")
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


def repair_is_expense(project_sheet, conn, db, start_date, end_date, apply=False):
    """Repair pre-CR-3 rows: re-fetch QBO txns in [start_date, end_date] WITHOUT
    skipping existing ones, and correct is_expense on already-imported local
    rows whose direction is now known to be wrong:
      • CC refunds (Purchase with Credit=true) imported as POSITIVE expenses
        → flip to credit (they were inflating actuals).
      • BillPayments imported as credits → flip to expense.
    Also reports QBO txns with no local row (e.g. CreditCardCredit refunds that
    predate the entity being queried) so they can be re-synced. Dry-run unless
    apply=True; only the is_expense boolean is touched. (Code review 2026-06-04.)"""
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
            (flip_to_credit if local.is_expense else flip_to_expense).append(rec)
            if apply:
                local.is_expense = bool(r["is_expense"])
                local.updated_at = datetime.datetime.utcnow()
                fixed += 1
    if apply and fixed:
        db.session.commit()
    return {"window": [start_date, end_date], "fetched": len(rows),
            "applied": apply, "fixed": fixed,
            "flip_to_credit_count": len(flip_to_credit),
            "flip_to_expense_count": len(flip_to_expense),
            "missing_in_local_count": len(missing),
            "flip_to_credit": flip_to_credit[:100],
            "flip_to_expense": flip_to_expense[:100],
            "missing_in_local": missing[:100]}


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
    rows = fetch_transactions(
        conn, db, account_ids, effective_start, safe_end,
        skip_keys=skip_keys, account_meta=account_meta,
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
            skip_keys=skip_keys, account_meta=account_meta,
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

    imported = 0
    reconciled = 0
    for r in rows:
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
            # Confirmed because the user manually placed the receipt
            # against a budget line — they own the categorization.
            if existing_doc_txn.budget_line_id or existing_doc_txn.account_code:
                existing_doc_txn.match_status = 'confirmed'
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

        db.session.add(Transaction(
            project_id              = project_sheet.id,
            source                  = 'qbo_sync',
            qbo_txn_id              = r["qbo_txn_id"],
            qbo_txn_type            = r["qbo_txn_type"],
            qbo_account_id          = r["qbo_account_id"],
            qbo_category            = r["qbo_category"],
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
        ))
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
             f"(reconciled={reconciled}, cdc={cdc_count}); "
             f"watermark → {safe_end}")
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
        "reconciled":      reconciled,    # merged into existing doc Transactions
        "cdc_additions":   cdc_count,
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
