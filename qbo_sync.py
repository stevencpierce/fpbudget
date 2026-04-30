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
    """List all active Bank + Credit Card accounts on the QBO realm.
    Used by the project-settings UI to let the user pick which
    accounts feed this project."""
    token = get_valid_token(conn, db)
    query = ("SELECT * FROM Account WHERE AccountType IN ('Bank', 'Credit Card') "
             "AND Active = true MAXRESULTS 100")
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


def _extract_txn_fields(txn, entity, acct_ref, account_meta):
    """Pull the fields we care about out of a raw QBO Purchase/Deposit.
    Returns dict ready to spread into a Transaction row, or None if
    the txn lacks an id."""
    txn_id = txn.get("Id", "")
    if not txn_id:
        return None

    is_expense = (entity == "Purchase")
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
    """Fetch Purchases + Deposits by TxnDate range. skip_keys is a set
    of (qbo_txn_id, qbo_txn_type) already in our DB."""
    token     = get_valid_token(conn, db)
    skip_keys = skip_keys or set()
    out       = []

    for acct_id in account_ids:
        for entity in ("Purchase", "Deposit"):
            query = (
                f"SELECT * FROM {entity} WHERE "
                f"TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' "
                f"MAXRESULTS 1000"
            )
            resp = requests.get(
                f"{_qbo_base_url()}/{conn.realm_id}/query",
                headers=_headers(token),
                params={"query": query},
                timeout=60,
            )
            if resp.status_code != 200:
                log.warning(f"[qbo] {entity} query failed: {resp.status_code} {resp.text[:200]}")
                continue
            rows = resp.json().get("QueryResponse", {}).get(entity, [])
            if len(rows) >= 1000:
                log.warning(
                    f"[qbo] {entity} query hit MAXRESULTS 1000 — narrow the date range."
                )
            for txn in rows:
                acct_ref = (
                    (txn.get("DepositToAccountRef") or {}).get("value")
                    if entity == "Deposit"
                    else (txn.get("AccountRef") or {}).get("value")
                )
                if acct_ref != acct_id:
                    continue
                fields = _extract_txn_fields(txn, entity, acct_ref, account_meta)
                if not fields:
                    continue
                if (fields["qbo_txn_id"], fields["qbo_txn_type"]) in skip_keys:
                    continue
                out.append(fields)
    out.sort(key=lambda f: f.get("txn_date") or "")
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
        params={"entities": "Purchase,Deposit", "changedSince": iso},
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
            for entity in ("Purchase", "Deposit"):
                for txn in query_resp.get(entity, []):
                    acct_ref = (
                        (txn.get("DepositToAccountRef") or {}).get("value")
                        if entity == "Deposit"
                        else (txn.get("AccountRef") or {}).get("value")
                    )
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
             f"(cdc={cdc_count}); watermark → {safe_end}")
    return {
        "imported":        imported,
        "cdc_additions":   cdc_count,
        "sync_through":    safe_end,
        "effective_start": effective_start,
        "effective_end":   safe_end,
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
