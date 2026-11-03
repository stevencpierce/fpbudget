"""Actuals module — the consumption layer of the three-legged stool.

  BudgetLine (planned)  ←  Transaction (actual)  →  DocUpload (backup)

This module owns:
  • The ONE "counts as spend" predicate (counts_as_spend) and the ONE
    document-class rule (doc_class / is_ledger_category) every rollup and
    every upload path reads
  • Linking transactions to budget lines — since Phase 1 (2026-07) a
    transaction codes DIRECTLY to the current Working line; no Actual
    budget is cloned on this path
  • The ONE "which budget is current" rule (get_current_working_budget /
    get_current_estimated_budget / get_live_budget): current = the newest
    of its kind. Estimated → Working creation lives in app.py
    (_create_budget_from_source, the single copy engine)
  • Auto-match scaffolding (suggest, never silently write)

Ingestion (pulling QBO bank txns into the Transaction table) lives
in `qbo_sync.py` — separate concern. This module is what happens
*after* a Transaction exists, regardless of source.

  • The company ledger rules (2026-11-03): pool_filter (what a project's
    Actuals sees), claim_for / release (who owns a QuickBooks charge)

Convention: this module never imports `app` at module level. The one
helper that lives in app.py and is needed here (_log_activity) is
imported lazily inside the function that calls it.
"""
import logging
import re as _re
from datetime import datetime
from sqlalchemy.orm import attributes

from models import (db, Budget, BudgetLine, Transaction, ProjectSheet, DocUpload,
                    MatchRejection, CrewAssignment)

log = logging.getLogger(__name__)


# ── Document classes (DOCUMENTS_HANDOFF Phase 0, 2026-10-30) ──────────────
# One rule for which document types create a ledger Transaction. Replaces the
# five hand-copied non-ledger lists that disagreed (app.py upload / update /
# keep-duplicate / admin reconcile / rollup filter, and the JS copy in b03.js).
#
#   evidence    — backs a spend event: receipt, invoice, payroll, timecard.
#                 Creates a source='doc_upload' Transaction that waits for a
#                 bank match or a cash log. An unknown / blank category is
#                 treated as evidence (the uploader has not classified it yet;
#                 a receipt-shaped row is safer than a silently dropped one).
#   paperwork   — lives with the person or vendor: tax forms, IDs, contracts,
#                 COIs, releases, legal, misc. Never a Transaction.
#   commitment  — a promise to spend, not spend: estimate, quote, PO. Never a
#                 Transaction; the invoice that follows is the evidence.
DOC_CLASS_EVIDENCE   = 'evidence'
DOC_CLASS_PAPERWORK  = 'paperwork'
DOC_CLASS_COMMITMENT = 'commitment'

EVIDENCE_CATEGORIES   = frozenset({'receipt', 'invoice', 'payroll', 'timecard'})
PAPERWORK_CATEGORIES  = frozenset({'tax_form', 'employee_vendor_doc', 'contract',
                                   'insurance', 'release', 'legal', 'misc'})
COMMITMENT_CATEGORIES = frozenset({'estimate', 'quote', 'purchase_order'})
# Every category that must NOT create / keep a ledger Transaction.
NON_LEDGER_CATEGORIES = frozenset(PAPERWORK_CATEGORIES | COMMITMENT_CATEGORIES)


def doc_class(category):
    """Classify a DocUpload.category → 'evidence' | 'paperwork' | 'commitment'.

    Case-insensitive, whitespace-tolerant. None / '' / unknown → 'evidence'
    (see the module note above: unclassified uploads behave like receipts
    until someone sets a type)."""
    c = (category or '').strip().lower()
    if c in PAPERWORK_CATEGORIES:
        return DOC_CLASS_PAPERWORK
    if c in COMMITMENT_CATEGORIES:
        return DOC_CLASS_COMMITMENT
    return DOC_CLASS_EVIDENCE


def is_ledger_category(category):
    """True when a document of this category creates / keeps a Transaction
    (its doc_class is 'evidence')."""
    return doc_class(category) == DOC_CLASS_EVIDENCE


def is_non_ledger_category(category):
    """True for paperwork and commitments — doc types that never sit on the
    actuals ledger. Exact inverse of is_ledger_category()."""
    return not is_ledger_category(category)


def non_ledger_categories():
    """Sorted list of every non-ledger category — for SQL IN() filters and
    for serving the rule to JavaScript (window.__BJ) so no hand copy of the
    list lives in a .js file."""
    return sorted(NON_LEDGER_CATEGORIES)


# ── Document ownership rules (DOCUMENTS_HANDOFF Phase 2, 2026-11-01) ───────
# Pure functions over plain objects (a DocUpload or a SimpleNamespace with
# the same attribute names) so they unit-test without Flask or a DB. The
# DB-touching callers in app.py read these, never re-derive them.

TAX_FORM_KINDS = ('w9', 'w8ben', 'w8bene', 'w4', 'i9', 'other')
TAX_FORM_KIND_LABELS = {'w9': 'W-9', 'w8ben': 'W-8BEN', 'w8bene': 'W-8BEN-E',
                        'w4': 'W-4', 'i9': 'I-9', 'other': 'Other tax form'}
# The kinds that satisfy "W-9 on file" (a W-8 is the foreign payee's W-9).
W9_KINDS = frozenset({'w9', 'w8ben', 'w8bene'})

# Sub-type string / filename → tax_form_kind. W-8BEN-E must be tested
# before W-8BEN. Same table as alembic 0023's data step.
_TAX_KIND_PATTERNS = (
    ('w8bene', _re.compile(r"w-?8\s*ben-?\s*e(?![a-z])", _re.I)),
    ('w8ben',  _re.compile(r"w-?8", _re.I)),
    ('w9',     _re.compile(r"w-?9", _re.I)),
    ('w4',     _re.compile(r"\bw-?4\b", _re.I)),
    ('i9',     _re.compile(r"\bi-?9\b", _re.I)),
)

# Review reasons the Docs Inbox states in its "Why here" column, in the
# order they are tested: the first that applies wins.
REVIEW_TYPE_UNCLEAR      = 'type_unclear'
REVIEW_CONFIRM_PAYEE     = 'confirm_payee'
REVIEW_POSSIBLE_DUPLICATE = 'possible_duplicate'
REVIEW_EXPIRY_MISSING    = 'expiry_missing'
REVIEW_NO_LINE           = 'no_line'
REVIEW_LOW_OCR           = 'low_ocr'
REVIEW_REASON_LABELS = {
    REVIEW_TYPE_UNCLEAR:       'Type unclear',
    REVIEW_CONFIRM_PAYEE:      'Confirm payee',
    REVIEW_POSSIBLE_DUPLICATE: 'Possible duplicate',
    REVIEW_EXPIRY_MISSING:     'Expiry date missing',
    REVIEW_NO_LINE:            'No line yet',
    REVIEW_LOW_OCR:            'Low OCR confidence',
}
LOW_OCR_CONFIDENCE = 60.0        # DocUpload.confidence is 0-100
COI_WARN_DAYS = 60


def tax_form_kind_from_text(text):
    """The tax_form_kind a sub-type string, OCR vendor or filename names
    ('W-9', 'w9_lindqvist.pdf', 'W-8BEN-E'), else None."""
    s = text or ''
    for kind, rx in _TAX_KIND_PATTERNS:
        if rx.search(s):
            return kind
    return None


def doc_owner_id(doc):
    """The owner of a document: owner_payee_id first, the alias column
    crew_member_id for rows the 0023 data step has not touched."""
    return (getattr(doc, 'owner_payee_id', None)
            or getattr(doc, 'crew_member_id', None) or None)


def doc_dup_key(doc):
    """The GLOBAL duplicate identity of a person document, or None when the
    document is not the kind that can duplicate.

    Two documents are the same paperwork when they have the same owner and
    the same kind: a tax form's tax_form_kind (two W-9s for one Payee are
    the same form, whichever projects collected them), an employee/vendor
    doc's sub-type (two driver's licences). A paperwork row with no owner
    or no kind has no identity yet and is never flagged. Project is NOT part
    of the key — that was the Phase 0 bug ("a W-9 collected again on
    another project is not recognized")."""
    owner = doc_owner_id(doc)
    if not owner:
        return None
    cat = (getattr(doc, 'category', None) or '').strip().lower()
    if cat == 'tax_form':
        kind = (getattr(doc, 'tax_form_kind', None) or '').strip().lower()
        if not kind:
            kind = tax_form_kind_from_text(" ".join(str(x or '') for x in (
                getattr(doc, 'doc_subtype', None), getattr(doc, 'filed_filename', None),
                getattr(doc, 'original_filename', None)))) or ''
        return (owner, f"tax_form:{kind}") if kind else None
    if cat == 'employee_vendor_doc':
        sub = (getattr(doc, 'doc_subtype', None) or getattr(doc, 'vendor', None) or '')
        sub = sub.strip().lower()
        return (owner, f"employee_vendor_doc:{sub}") if sub else None
    return None


def pick_dup_pair(a, b):
    """Of two documents with the same doc_dup_key, which is the original
    and which the newer copy: the lower id is the original."""
    return (a, b) if (a.id or 0) <= (b.id or 0) else (b, a)


def w9_date_from_doc(doc):
    """The date a W-9 / W-8 document was received: received_on, else the
    document date, else the upload day. None when the document is not a
    W-9 / W-8 tax form."""
    if (getattr(doc, 'category', None) or '').strip().lower() != 'tax_form':
        return None
    if (getattr(doc, 'tax_form_kind', None) or '').strip().lower() not in W9_KINDS:
        return None
    d = getattr(doc, 'received_on', None) or getattr(doc, 'doc_date', None)
    if d is None:
        up = getattr(doc, 'uploaded_at', None)
        d = up.date() if (up is not None and hasattr(up, 'date')) else None
    return d


def w9_update_for(payee, doc):
    """The ONE W-9 date rule: when a W-9 / W-8 document is saved with an
    owner, the owner's w9_received_on / w9_doc_upload_id move to it if it
    is NEWER (or the payee has none, or the payee already points at this
    very document and its date changed). Returns {"w9_received_on",
    "w9_doc_upload_id"} to write, or None when nothing changes. Pure."""
    d = w9_date_from_doc(doc)
    if d is None or doc_owner_id(doc) != getattr(payee, 'id', None):
        return None
    cur = getattr(payee, 'w9_received_on', None)
    cur_doc = getattr(payee, 'w9_doc_upload_id', None)
    update = {"w9_received_on": d, "w9_doc_upload_id": doc.id}
    if cur is None:
        return update                       # first W-9 for this payee
    if cur_doc == doc.id:
        return update if d != cur else None  # same form, date corrected
    if d >= cur:
        return update                       # newer (or same-day) form wins
    return None                             # an older form never replaces a newer one


def doc_review_reason(doc, txn=None, suggestion=None, today=None):
    """Why a document sits in Needs review — ONE reason, derived server-side
    so the inbox's "Why here" column says the same thing on every page.

      type_unclear        no category, or status 'review' with no type
      confirm_payee       a type that needs an owner (every paperwork type,
                          plus invoice / payroll / timecard) with no owner
                          and a crew-suggest match waiting to be confirmed
      possible_duplicate  flagged is_duplicate but not yet resolved
      expiry_missing      a COI (insurance) with no expires_on
      no_line             an evidence doc whose charge has no budget line
      low_ocr             OCR confidence under LOW_OCR_CONFIDENCE
    Returns the reason key or None (nothing to review).

    `txn` is the representative Transaction (or None); `suggestion` is the
    crew-suggest result ({crew_member_id, confidence} or None)."""
    cat = (getattr(doc, 'category', None) or '').strip().lower()
    status = (getattr(doc, 'status', None) or '').lower()
    if not cat:
        return REVIEW_TYPE_UNCLEAR
    dup_pending = bool(getattr(doc, 'is_duplicate', False)) and status != 'duplicate'
    needs_owner = (doc_class(cat) == DOC_CLASS_PAPERWORK and cat != 'misc') \
        or cat in ('invoice', 'payroll', 'timecard')
    if needs_owner and not doc_owner_id(doc) and suggestion \
            and suggestion.get('crew_member_id'):
        return REVIEW_CONFIRM_PAYEE
    if dup_pending:
        return REVIEW_POSSIBLE_DUPLICATE
    if cat == 'insurance' and not getattr(doc, 'expires_on', None):
        return REVIEW_EXPIRY_MISSING
    if doc_class(cat) == DOC_CLASS_EVIDENCE and txn is not None \
            and not getattr(txn, 'budget_line_id', None) \
            and not getattr(txn, 'account_code', None) \
            and not getattr(txn, 'not_project_expense', False):
        return REVIEW_NO_LINE
    conf = getattr(doc, 'confidence', None)
    try:
        if conf is not None and float(conf) < LOW_OCR_CONFIDENCE and status == 'review':
            return REVIEW_LOW_OCR
    except (TypeError, ValueError):
        pass
    if status == 'review':
        return REVIEW_TYPE_UNCLEAR
    return None


def doc_folder_segments(doc, payee_name=None, uploader_folder='unknown'):
    """The sub-folders under a type folder a document files into — the one
    path rule the analyzer layout, _doc_intended_path and _refile_doc share.

      paperwork with an owner      [<Payee name>]            never the uploader
      paperwork, no owner yet      [<uploader>]
      evidence / commitment, AP    [<uploader>, <vendor>]    invoice, contract*,
                                                            PO, insurance*
      everything else              [<uploader>]
    (* contract and insurance are paperwork, so with an owner they take the
    Payee folder; only ownerless ones keep the legacy uploader/vendor shape.)
    `payee_name` is the owner's display name, already filename-safe."""
    cat = (getattr(doc, 'category', None) or 'misc').strip().lower()
    klass = doc_class(cat)
    if klass == DOC_CLASS_PAPERWORK and cat != 'misc' and doc_owner_id(doc) and payee_name:
        return [payee_name]
    if cat in ('invoice', 'contract', 'purchase_order', 'insurance'):
        vendor = (getattr(doc, 'vendor', None) or '').strip() or 'Unknown_Vendor'
        return [uploader_folder, vendor]
    return [uploader_folder]


def expiry_state(expires_on, today=None, warn_days=COI_WARN_DAYS):
    """COI expiry: 'bad' when past, 'warn' within warn_days, 'ok' otherwise,
    None when there is no date."""
    if not expires_on:
        return None
    from datetime import date as _date
    today = today or _date.today()
    if expires_on < today:
        return 'bad'
    if (expires_on - today).days <= warn_days:
        return 'warn'
    return 'ok'


# ── The one "counts as spend" predicate (ACTUALS_HANDOFF Phase 0) ─────────

def counts_as_spend(split_parent_ids=None):
    """Return ONE SQLAlchemy clause that says whether a Transaction row is
    real project spend. Every rollup (section totals, per-line Actual column,
    line-totals.json) must filter on this and nothing else, so the three
    never disagree again.

    A row counts when ALL of:
      • not_project_expense is false (not excluded / not claimed elsewhere)
      • it is an actual expense event: imported rows (qbo_sync, csv_import,
        reconciled, manual_entry, invoice_split…) always are; a doc-born row
        (source='doc_upload') only once the user has explicitly created an
        expense from it (activated_at set) — documents are evidence, not spend
      • backup_of_txn_id is NULL (a receipt row absorbed as backup for another
        charge must not count twice)
      • it is not an itemized / split container: the invoice_split children
        (or, since Phase 2, the transaction_split rows) carry the coding, so
        the parent's full amount must not count as well. Pass
        `split_parent_ids` (from app._split_parent_id_set(pid), which covers
        both kinds) to exclude a known set of parent ids; when omitted the
        clause falls back to correlated NOT EXISTS subqueries, which is
        correct but two extra subqueries per rollup. Pass an EMPTY set to
        skip the container rule on purpose (split_sums_by_line does: there
        the parent must qualify so its pieces count).

    Project scoping, line / section scoping and the signed-amount rule stay
    with the caller — this is only the "is it spend at all" question."""
    from sqlalchemy import and_, or_, not_, exists
    clauses = [
        Transaction.not_project_expense == False,            # noqa: E712
        or_(Transaction.source != 'doc_upload',
            Transaction.activated_at.isnot(None)),
        Transaction.backup_of_txn_id.is_(None),
    ]
    if split_parent_ids is not None:
        ids = [int(i) for i in split_parent_ids if i is not None]
        if ids:
            clauses.append(Transaction.id.notin_(ids))
    else:
        from sqlalchemy.orm import aliased
        from models import TransactionSplit as _TS
        _Child = aliased(Transaction)
        clauses.append(not_(exists().where(and_(
            _Child.parent_transaction_id == Transaction.id,
            _Child.source == 'invoice_split'))))
        # …and transaction_split parents (Phase 2, 2026-10-30): their
        # pieces reach the rollups through split_sums_by_line.
        clauses.append(not_(exists().where(
            _TS.transaction_id == Transaction.id)))
    return and_(*clauses)


def row_counts_as_spend(txn, split_parent_ids=None):
    """Pure-Python twin of counts_as_spend() for code that already holds the
    rows (and for unit tests). Same four rules, same answer."""
    if getattr(txn, 'not_project_expense', False):
        return False
    if getattr(txn, 'source', None) == 'doc_upload' \
            and getattr(txn, 'activated_at', None) is None:
        return False
    if getattr(txn, 'backup_of_txn_id', None) is not None:
        return False
    if split_parent_ids and getattr(txn, 'id', None) in split_parent_ids:
        return False
    return True


def signed_amount_value(amount, is_expense):
    """The signed-amount rule as a plain number: an expense counts positive,
    a credit / refund counts NEGATIVE so it nets against its line (owner
    decision 2026-10-30 — credits act as credits, as in QuickBooks). Mirrors
    app._signed_amount(), which is the SQL form of the same rule."""
    try:
        a = float(amount or 0)
    except (TypeError, ValueError):
        a = 0.0
    return a if is_expense else -a


# ── Safety rules (Cleanup A, 2026-11-02) ──────────────────────────────────
# Pure, DB-free predicates behind the data-loss fixes in app.py and
# routes/actuals.py. Each is unit-tested in tests/unit/test_safety_rules.py
# with SimpleNamespace rows, so the rule is pinned independently of the
# route that applies it.

# Rows whose vendor / amount / date came from the BANK (or a bank export).
# They are the record of what was actually paid; a receipt edit, a review-
# queue "accept amount", or a manual amount edit must never rewrite them.
BANK_SOURCES = ('qbo_sync', 'csv_import', 'reconciled')


def is_bank_sourced(txn):
    """True when the row's money facts come from the bank."""
    return getattr(txn, 'source', None) in BANK_SOURCES


def doc_edit_writes_through_to(txn):
    """Which linked Transaction rows a DocUpload edit (vendor / amount / date /
    note / card) may overwrite: ONLY the document's own ledger row
    (source='doc_upload'). A matched bank charge keeps the bank's values —
    the old write-through rewrote qbo_sync / reconciled / csv_import rows
    with the receipt's values and a blank doc date wiped the bank date.
    invoice_split children are excluded too (their amount is the subline's)."""
    return getattr(txn, 'source', None) == 'doc_upload'


def amount_edit_allowed(txn, new_amount, split_amounts):
    """Rule for editing a transaction's amount by hand.

    Returns (ok, reason). Refused when:
      • the row is bank-sourced (qbo_sync / csv_import / reconciled) — the
        amount is what the bank says it is;
      • the row is split across lines and the new amount no longer equals
        the split sum to the cent — clear the split first.
    A no-op edit (same amount) is always allowed.
    """
    try:
        old_c = _cents(getattr(txn, 'amount', None))
        new_c = _cents(new_amount)
    except ValueError:
        return False, "amount must be a number"
    if old_c == new_c:
        return True, None
    if is_bank_sourced(txn):
        return False, ("This amount came from the bank and can't be edited here. "
                       "If the bank charge is wrong, fix it in QuickBooks and sync again.")
    if split_amounts:
        try:
            split_c = sum(_cents(a) for a in split_amounts)
        except ValueError:
            split_c = None
        if split_c != new_c:
            return False, ("This transaction is split across budget lines and the "
                           "new amount doesn't match the split total. Clear the "
                           "split first, then change the amount.")
    return True, None


# is_duplicate_twin lives in the Phase 5 duplicates section below (one rule).


def review_resolve_allowed(action, txn, flag_txn_ids):
    """Rules for the review-queue resolve actions that touch a transaction.

    Returns (ok, reason).
      • the transaction must be one the flag is ABOUT (flag_txn_ids);
      • delete_txn is refused for anything but the document's own row
        (source='doc_upload') — bank charges are unlinked / excluded, never
        deleted from a queue click.
    """
    if txn is None:
        return False, "transaction not found"
    if getattr(txn, 'id', None) not in set(flag_txn_ids or ()):
        return False, "That transaction isn't part of this review item."
    if action == 'delete_txn' and getattr(txn, 'source', None) != 'doc_upload':
        return False, ("Only a document's own row can be deleted from the review "
                       "queue. This is a bank charge — mark it not-project or "
                       "un-code it instead.")
    return True, None


def anomaly_flag_txn_ids(flag, payload):
    """Every transaction id a review-queue flag refers to: its transaction_id
    column plus any ids carried in its payload (double_coded members,
    people_line / vendor_cleanup transaction_id)."""
    ids = set()
    tid = getattr(flag, 'transaction_id', None)
    if tid:
        ids.add(int(tid))
    payload = payload or {}
    for key in ('transaction_id', 'tid'):
        v = payload.get(key)
        if v:
            try:
                ids.add(int(v))
            except (TypeError, ValueError):
                pass
    for m in payload.get('members') or []:
        if isinstance(m, dict) and m.get('tid'):
            try:
                ids.add(int(m['tid']))
            except (TypeError, ValueError):
                pass
    return ids


def budget_lock_denial(budget, anchor):
    """The budget write-gate's lock rule as a pure function: a version is
    read-only when its own `locked` flag is set OR its Estimated anchor's is.
    Returns the error text, or None when writes are allowed. Used by the
    before_request gate AND inside upsert_line / delete_line so the mobile
    API (which calls those handlers directly) respects the lock too."""
    if budget is None:
        return None
    locked = bool(getattr(budget, 'locked', False))
    if not locked and anchor is not None:
        locked = bool(getattr(anchor, 'locked', False))
    if locked:
        return ("This version is locked (read-only). Unlock it via ✎ in the "
                "version menu first.")
    return None


# ── Transaction splits (Concur's Allocate — ACTUALS_HANDOFF Phase 2) ──────
# One transaction across several budget lines. A split parent has
# budget_line_id NULL and reaches every rollup ONLY through its
# transaction_split rows, which must sum to the parent's amount to the cent.
# The validation is pure (unit-tested); the SQL rollup helper is the ONE
# function the three rollups (section totals, budget_view per-line, the
# line-totals.json refresh) call so they never disagree about splits.

PAYMENT_METHODS = ('company_card', 'cash', 'personal_card', 'check', 'ach', 'other')
# The ONE label map (Cleanup D): served to templates / JS as
# PAYMENT_METHOD_LABELS / window.__FP_PAYMENT_METHODS — the Actuals screen's
# "How was it paid?" radios and the table's card cell both read it (the
# radios used to lack ACH; a01.js had its own shorter map).
PAYMENT_METHOD_LABELS = {'company_card': 'Company card', 'cash': 'Cash',
                         'personal_card': 'Personal card', 'check': 'Check',
                         'ach': 'ACH', 'other': 'Other'}

# log-expense reason_kind → payment_method, for callers that still send the
# older shape (reason_kind cash / check / other_card / other).
REASON_KIND_TO_PAYMENT_METHOD = {'cash': 'cash', 'check': 'check',
                                 'other_card': 'personal_card', 'other': 'other'}


def _cents(x):
    """Money as integer cents (round-half-up on the absolute value), so
    sum-to-the-cent comparisons never depend on float noise."""
    from decimal import Decimal, ROUND_HALF_UP
    try:
        d = Decimal(str(x if x is not None else 0))
    except Exception:
        raise ValueError(f"not a number: {x!r}")
    return int(d.copy_abs().quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) * 100) * (-1 if d < 0 else 1)


def validate_splits(txn_amount, splits, allowed_line_ids=None):
    """Validate a split request. Pure — no DB.

    splits: list of {budget_line_id, amount, note?}. Rules:
      • at least two rows (one row is an assignment, not a split)
      • every row names a budget_line_id; when allowed_line_ids is given,
        every id must be in it (lines of THIS project's current Working)
      • every amount is a number, non-zero, and has the same sign as the
        transaction (a refund splits into negative pieces)
      • the amounts sum to the transaction amount TO THE CENT
    Returns (clean_rows, None) on success, where clean_rows carry
    budget_line_id:int, amount:float (2dp), note:str|None — or
    (None, {"error": str, "delta": float|None}) on failure. delta is
    (sum of splits) - (transaction amount), in dollars, when that is the
    problem."""
    if not isinstance(splits, (list, tuple)) or len(splits) < 2:
        return None, {"error": "A split needs at least two lines.", "delta": None}
    try:
        total_c = _cents(txn_amount)
    except ValueError:
        return None, {"error": "Transaction amount is not a number.", "delta": None}
    if total_c == 0:
        return None, {"error": "A $0 transaction cannot be split.", "delta": None}
    clean, sum_c = [], 0
    for i, row in enumerate(splits, 1):
        if not isinstance(row, dict):
            return None, {"error": f"Split {i} is not an object.", "delta": None}
        lid = row.get('budget_line_id')
        try:
            lid = int(lid)
        except (TypeError, ValueError):
            return None, {"error": f"Split {i} has no budget line.", "delta": None}
        if allowed_line_ids is not None and lid not in allowed_line_ids:
            return None, {"error": f"Split {i}: line #{lid} is not on this "
                                   "project's current Working budget.",
                          "delta": None}
        try:
            amt_c = _cents(row.get('amount'))
        except ValueError:
            return None, {"error": f"Split {i}: amount is not a number.", "delta": None}
        if amt_c == 0:
            return None, {"error": f"Split {i}: amount cannot be $0.", "delta": None}
        if (amt_c > 0) != (total_c > 0):
            return None, {"error": f"Split {i}: amount must have the same sign "
                                   "as the transaction.", "delta": None}
        note = (row.get('note') or '').strip()[:300] or None
        clean.append({"budget_line_id": lid, "amount": amt_c / 100.0, "note": note})
        sum_c += amt_c
    if sum_c != total_c:
        delta = (sum_c - total_c) / 100.0
        return None, {"error": f"Splits total ${sum_c / 100.0:,.2f} but the "
                               f"transaction is ${total_c / 100.0:,.2f} "
                               f"({'over' if delta > 0 else 'short'} by "
                               f"${abs(delta):,.2f}).",
                      "delta": delta}
    return clean, None


def validate_manual_expense(body):
    """Validate a hand-typed manual expense (no receipt, no QBO charge).
    Pure — no DB. body: {vendor, amount, txn_date, budget_line_id?,
    payment_method, paid_by_crew_member_id?, note?, reason?}.

    Rules: vendor required; amount a non-zero number (negative = a credit);
    txn_date YYYY-MM-DD; payment_method one of PAYMENT_METHODS; reason
    required when payment_method is cash; paid_by_crew_member_id and
    budget_line_id integers when present. Returns (clean, None) or
    (None, error_string)."""
    import re
    if not isinstance(body, dict):
        return None, "Body must be a JSON object."
    vendor = (body.get('vendor') or '').strip()
    if not vendor:
        return None, "vendor is required."
    try:
        amt_c = _cents(body.get('amount'))
    except ValueError:
        return None, "amount must be a number."
    if amt_c == 0:
        return None, "amount cannot be $0."
    d = (body.get('txn_date') or '').strip()[:10]
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', d):
        return None, "txn_date must be YYYY-MM-DD."
    try:
        datetime.strptime(d, '%Y-%m-%d')
    except ValueError:
        return None, "txn_date is not a real date."
    pm = (body.get('payment_method') or '').strip().lower()
    if pm not in PAYMENT_METHODS:
        return None, ("payment_method is required: one of "
                      + ", ".join(PAYMENT_METHODS) + ".")
    reason = (body.get('reason') or '').strip()[:300] or None
    if pm == 'cash' and not reason:
        return None, "A reason is required for a cash expense."
    def _opt_int(k):
        v = body.get(k)
        if v in (None, '', 0, '0'):
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            raise ValueError(f"{k} must be an integer.")
    try:
        lid = _opt_int('budget_line_id')
        who = _opt_int('paid_by_crew_member_id')
    except ValueError as e:
        return None, str(e)
    return {"vendor": vendor[:300], "amount": abs(amt_c) / 100.0,
            "is_expense": amt_c > 0, "txn_date": d, "payment_method": pm,
            "paid_by_crew_member_id": who, "budget_line_id": lid,
            "note": (body.get('note') or '').strip() or None,
            "reason": reason}, None


def split_sums_by_line(project_id, line_ids=None):
    """{budget_line_id: signed split total} for the project — the ONE helper
    every rollup adds to its direct-coded sums (section totals, budget_view
    per-line, line-totals.json). A transaction with splits has
    budget_line_id NULL, so it is absent from the direct sums and reaches
    its lines only here. Only splits whose PARENT counts as spend are
    summed (not-project / backup rows drop out with their splits). Sign
    follows the parent: a split of a refund nets against its line."""
    from sqlalchemy import func, case
    from models import TransactionSplit
    q = (db.session.query(
            TransactionSplit.budget_line_id,
            func.sum(case((Transaction.is_expense == True, TransactionSplit.amount),   # noqa: E712
                          else_=-TransactionSplit.amount)))
         .join(Transaction, Transaction.id == TransactionSplit.transaction_id)
         .filter(Transaction.project_id == project_id,
                 TransactionSplit.budget_line_id.isnot(None),
                 counts_as_spend(set())))
    if line_ids is not None:
        ids = [int(i) for i in line_ids]
        if not ids:
            return {}
        q = q.filter(TransactionSplit.budget_line_id.in_(ids))
    return {lid: float(amt or 0) for lid, amt in
            q.group_by(TransactionSplit.budget_line_id).all()}


def split_pieces_for_lines(project_id, line_ids):
    """The split PIECES that land on these lines, as (split, parent) pairs,
    for the per-line drill-downs (ledger.json / transactions.json). The
    sums above already count them; the lists did not show them at all, so
    a charge split across two lines looked uncoded when you opened either
    line (Steven, Nov 2 2026). Only pieces whose parent counts as spend."""
    from models import TransactionSplit
    ids = [int(i) for i in (line_ids or [])]
    if not ids:
        return []
    rows = (db.session.query(TransactionSplit, Transaction)
            .join(Transaction, Transaction.id == TransactionSplit.transaction_id)
            .filter(Transaction.project_id == project_id,
                    TransactionSplit.budget_line_id.in_(ids),
                    counts_as_spend(set()))
            .order_by(Transaction.txn_date.asc().nullsfirst(), TransactionSplit.id.asc())
            .all())
    return [(sp, t) for sp, t in rows]


def split_sums_by_section_code(project_id):
    """{account_code: signed split total} — the section-level twin of
    split_sums_by_line for _actuals_by_section_code (top sheet)."""
    from sqlalchemy import func, case
    from models import TransactionSplit
    rows = (db.session.query(
                BudgetLine.account_code,
                func.sum(case((Transaction.is_expense == True, TransactionSplit.amount),   # noqa: E712
                              else_=-TransactionSplit.amount)))
            .join(Transaction, Transaction.id == TransactionSplit.transaction_id)
            .join(BudgetLine, BudgetLine.id == TransactionSplit.budget_line_id)
            .filter(Transaction.project_id == project_id,
                    counts_as_spend(set()))
            .group_by(BudgetLine.account_code).all())
    return {code: float(amt or 0) for code, amt in rows if code is not None}


# ── Company ledger: the pool, claiming and releasing (2026-11-03) ─────────
# ONE row per QuickBooks transaction for the whole company. On a
# QuickBooks-sourced row (qbo_txn_id set) project_id means "claimed by":
# NULL = unclaimed, in the company pool. Rows born elsewhere (receipt,
# manual entry, bank CSV) keep the project_id they were created with.
#
#   pool_filter(pid)   the ONE criterion for "a project's transactions":
#                      its own rows + every unclaimed QuickBooks row.
#                      Rollups that go by budget_line_id need nothing —
#                      a coded row is always claimed.
#   claim_for(t, pid)  every claim signal (line, section, split, receipt,
#                      not-project) calls this; refuses a row another
#                      project holds.
#   release(t)         when the last signal is gone from a QuickBooks row
#                      it returns to the pool.
# Both log through app._log_activity (lazy import, fail-open).

class ClaimError(ValueError):
    """The row is claimed by a different project (the UI never offers that)."""


def pool_filter(project_id):
    """SQLAlchemy criterion: the rows a project's Actuals works with — its
    own rows plus every unclaimed QuickBooks row in the company pool. A
    row claimed by another project never satisfies it."""
    return db.or_(Transaction.project_id == project_id,
                  db.and_(Transaction.project_id.is_(None),
                          Transaction.qbo_txn_id.isnot(None)))


def in_pool(txn, project_id):
    """Python twin of pool_filter for a row already in hand."""
    if txn is None:
        return False
    if txn.project_id == project_id:
        return True
    return txn.project_id is None and bool(txn.qbo_txn_id)


def pool_txn(project_id, tid):
    """The transaction by id if it is in this project's pool, else None."""
    return Transaction.query.filter(Transaction.id == tid, pool_filter(project_id)).first()


def is_qbo_row(txn):
    return bool(getattr(txn, 'qbo_txn_id', None))


def claim_signals(txn, has_splits=None):
    """Which claim signals the row carries right now. has_splits may be
    passed when the caller already knows (bulk reads); else it is queried."""
    from models import TransactionSplit
    if has_splits is None:
        has_splits = (db.session.query(TransactionSplit.id)
                      .filter_by(transaction_id=txn.id).first() is not None) if txn.id else False
    return {
        'line':        bool(txn.budget_line_id),
        'section':     bool(txn.account_code) and not txn.budget_line_id,
        'split':       bool(has_splits),
        'receipt':     bool(txn.doc_upload_id),
        'not_project': bool(txn.not_project_expense),
    }


def is_claimed(txn, has_splits=None):
    return any(claim_signals(txn, has_splits).values())


def _ledger_log(action, txn, project_id, note):
    try:
        from app import _log_activity
        _log_activity(action=action, entity_type='transaction', entity_id=txn.id,
                      entity_label=(txn.vendor or f'Txn #{txn.id}')[:80],
                      project_id=project_id,
                      after={'project_id': txn.project_id, 'qbo_txn_id': txn.qbo_txn_id,
                             'qbo_txn_type': txn.qbo_txn_type,
                             'amount': float(txn.amount or 0)},
                      note=note[:500])
    except Exception:
        log.info("[ledger] activity log skipped for txn %s", getattr(txn, 'id', None), exc_info=True)


def claim_for(txn, project_id, why='coded'):
    """Set project_id = project_id on a QuickBooks row that is unclaimed or
    already this project's. Raises ClaimError when another project holds
    it. Returns True when the row changed hands. Does not commit — the
    caller's write path does, as it always did."""
    if txn is None or not project_id:
        return False
    project_id = int(project_id)
    if txn.project_id == project_id:
        return False
    if txn.project_id is not None:
        other = ProjectSheet.query.get(txn.project_id)
        raise ClaimError(
            f"This charge is already claimed by "
            f"{(other.name if other else f'project #{txn.project_id}')} — "
            f"resolve it there (or in Admin → Maintenance → Claimed in two projects).")
    txn.project_id = project_id
    txn.updated_at = datetime.utcnow()
    _ledger_log('claim', txn, project_id,
                f"Claimed QuickBooks {txn.qbo_txn_type or 'charge'}/{txn.qbo_txn_id or '?'} "
                f"for this project ({why})")
    return True


def release(txn, why='last claim signal removed', has_splits=None):
    """Return a QuickBooks row to the company pool when no claim signal is
    left on it (no line, no section, no splits, no receipt, not excluded).
    A row born in a project (receipt / manual / CSV) is never released.
    Returns True when it was released. Does not commit."""
    if txn is None or not is_qbo_row(txn) or txn.project_id is None:
        return False
    if is_claimed(txn, has_splits):
        return False
    pid = txn.project_id
    txn.project_id = None
    txn.match_status = 'unmatched'
    txn.match_confidence = None
    txn.updated_at = datetime.utcnow()
    _ledger_log('release', txn, pid,
                f"Released QuickBooks {txn.qbo_txn_type or 'charge'}/{txn.qbo_txn_id or '?'} "
                f"to the company pool ({why})")
    return True


def claim_conflicts_for(project_id):
    """The flagged rows this project holds (the Actuals banner)."""
    return (Transaction.query
            .filter(Transaction.project_id == project_id,
                    Transaction.claim_conflict == True)        # noqa: E712
            .count())


# ── Workspace rows: qbo_txn_id grouping and flags (Phase 2) ───────────────

def group_pool_rows(rows, key=lambda t: (getattr(t, 'qbo_txn_id', None),
                                         getattr(t, 'qbo_txn_type', None))):
    """Collapse sibling rows of the same QBO charge (one per project that
    synced the account) into ONE representative per (qbo_txn_id,
    qbo_txn_type). Rows with no qbo_txn_id (manual / CSV / receipt-born)
    are their own group keyed by id. Pure.

    Within a group the representative is the row that carries the claim
    (budget_line_id / account_code / splits / not_project with a reason),
    else the first row. Returns [(representative, [all rows in group])] in
    first-seen order."""
    groups, order = {}, []
    for t in rows:
        qid, qtype = key(t)
        k = ('qbo', qid, qtype) if qid else ('id', getattr(t, 'id', None), None)
        if k not in groups:
            groups[k] = []
            order.append(k)
        groups[k].append(t)
    out = []
    for k in order:
        members = groups[k]
        rep = members[0]
        for m in members:
            if (getattr(m, 'budget_line_id', None) or getattr(m, 'account_code', None)
                    or getattr(m, 'has_splits', False)):
                rep = m
                break
        out.append((rep, members))
    return out


def compute_row_flags(row, *, today=None, date_far_days=7):
    """Red / yellow flags for one workspace row. Pure; row is a dict or an
    object with: budget_line_id, splits (list of {amount}), split_sum_ok
    (bool | None), assigned (bool | None — overrides the line / split
    test), amount, is_expense, has_backup (evidence row or
    no_charge_reason), receipt_amount, receipt_date, txn_date,
    possible_duplicate (bool), project_id,
    payment_method, not_project_expense, source.

    Red (blocks nothing, never disappears on its own):
      no_line        — not assigned and not split and not excluded
      needs_backup   — assigned (or split) with no evidence and no reason
      split_mismatch — splits exist but do not sum to the amount
      voided_in_qbo  — QuickBooks voided / deleted the record (Phase 5)
    Yellow:
      possible_duplicate — a suspected duplicate was flagged
      amount_mismatch    — matched receipt amount differs from the charge
      date_far           — receipt date more than date_far_days from charge
      paid_by_other      — payment method is personal_card / cash / other
                           (someone other than the company paid; reconcile)
    Returns {"red": [...], "yellow": [...]}."""
    g = (row.get if isinstance(row, dict) else lambda k, d=None: getattr(row, k, d))
    red, yellow = [], []
    excluded = bool(g('not_project_expense', False))
    splits = g('splits') or []
    # `assigned` (optional) lets a caller count section-only coding or an
    # itemized parent as assigned; otherwise a line or a split is what counts.
    assigned = g('assigned')
    if assigned is None:
        assigned = bool(g('budget_line_id')) or bool(splits)
    assigned = bool(assigned)
    if not excluded:
        if not assigned:
            red.append('no_line')
        if splits:
            ok = g('split_sum_ok')
            if ok is None:
                try:
                    ok = _cents(sum(float(s.get('amount') if isinstance(s, dict)
                                          else getattr(s, 'amount', 0) or 0)
                                    for s in splits)) == _cents(g('amount'))
                except Exception:
                    ok = False
            if not ok:
                red.append('split_mismatch')
        if assigned and not g('has_backup', False):
            red.append('needs_backup')
    if g('possible_duplicate', False):
        yellow.append('possible_duplicate')
    # Phase 5: QuickBooks voided / deleted the record after we imported it;
    # the row was kept because it carried coding or a receipt. Red even
    # while excluded — the person decides what happens to the work on it.
    if g('voided', False):
        red.append('voided_in_qbo')
    ra, ca = g('receipt_amount'), g('amount')
    if ra is not None and ca is not None:
        try:
            if _cents(ra) != _cents(ca):
                yellow.append('amount_mismatch')
        except ValueError:
            pass
    rd, td = g('receipt_date'), g('txn_date')
    if rd and td:
        try:
            d1 = datetime.strptime(str(rd)[:10], '%Y-%m-%d').date()
            d2 = datetime.strptime(str(td)[:10], '%Y-%m-%d').date()
            if abs((d1 - d2).days) > date_far_days:
                yellow.append('date_far')
        except ValueError:
            pass
    if (g('payment_method') or '') in ('personal_card', 'cash', 'other'):
        yellow.append('paid_by_other')
    return {"red": red, "yellow": yellow}


# ── Fringes to 6500 Administrative (ACTUALS_HANDOFF Phase 4, 2026-11-01) ──

FRINGE_HOME_SECTION = 6500


def resolve_fringe_line(lines, section, home=FRINGE_HOME_SECTION):
    """Find a labor DEPARTMENT's Payroll Fringes line among line-likes
    (id, line_tag, account_code, fringe_for_section). Pure; app.
    _ensure_fringe_rollup_line uses it so the register, the parser and the
    grid resolve the same line.

    Returns (line, is_legacy):
      * the line tagged fringe_rollup whose fringe_for_section == section
        (the Phase 4 shape, in 6500) → (line, False);
      * else the pre-Phase-4 per-section line (fringe_rollup, account_code
        == section, fringe_for_section NULL) → (line, True) so the caller
        adopts it into 6500 keeping its id and its coded actuals;
      * else (None, False). The old single 6500 pot (account_code == home,
        no department) is never a department's line."""
    try:
        section = int(section)
    except (TypeError, ValueError):
        return None, False
    for l in lines or []:
        if (getattr(l, 'line_tag', None) or '') != 'fringe_rollup':
            continue
        if getattr(l, 'fringe_for_section', None) == section:
            return l, False
    if section == home:
        return None, False
    for l in lines or []:
        if ((getattr(l, 'line_tag', None) or '') == 'fringe_rollup'
                and getattr(l, 'fringe_for_section', None) is None
                and getattr(l, 'account_code', None) == section):
            return l, True
    return None, False


def parse_people_split(amount, wages, fringes, workers_comp=0.0, platform_fee=0.0):
    """Tie-out rule for one parsed payroll row (Phase 4; pure, unit-tested
    in tests/unit/test_fringe_lines.py; app.docs_parse_people calls it).

    amount is the person's total on the document. When the AI separated the
    parts, the split is used as given; the difference between amount and
    (wages + fringes + workers_comp + platform_fee) is booked to the
    person's line (wages) and returned as `delta` when it exceeds $1 so the
    UI can flag it, instead of silently merging everything back into one
    row. When no split exists (wages missing) the whole amount is wages.
    Returns {wages, fringes, workers_comp, platform_fee, delta}."""
    def _f(v):
        try:
            return round(float(v), 2)
        except (TypeError, ValueError):
            return None
    amount = _f(amount) or 0.0
    wages, fringes = _f(wages), _f(fringes)
    wc = _f(workers_comp) or 0.0
    fee = _f(platform_fee) or 0.0
    if wages is None or wages < 0:
        return {"wages": amount, "fringes": 0.0, "workers_comp": 0.0,
                "platform_fee": 0.0, "delta": 0.0}
    fringes = fringes if (fringes is not None and fringes >= 0) else 0.0
    wc = max(wc, 0.0)
    fee = max(fee, 0.0)
    diff = round(amount - (wages + fringes + wc + fee), 2)
    delta = diff if abs(diff) > 1.0 else 0.0
    return {"wages": round(wages + diff, 2), "fringes": fringes,
            "workers_comp": wc, "platform_fee": fee, "delta": delta}


# ── Workspace screen helpers (ACTUALS_HANDOFF Phase 3, 2026-11-01) ────────

def build_line_picker_options(groups):
    """The ordered line-picker option structure the Actuals workspace and
    its modals share (the shape /register's #rg-line-opts used). Pure:
    `groups` is [{code, name, lines:[line-like]}] where a line-like has
    id, account_code, description, account_name, parent_line_id, line_tag.

    Rules (owner 2026-10-31): header / spacer rows are never targets;
    child lines (kit fees, agent fees …) ride indented right under their
    parent. Since Phase 4 (Steven, Oct 30) the per-department Payroll
    Fringes lines live in 6500 Administrative — they anchor the bottom of
    the 6500 group as kind='fringe' with their department name, and ARE
    coding targets (the payroll parser codes each person's employer
    fringes there). The hidden auto-calc lines (Workers' Comp, Production
    Insurance, Payroll Service Fee, Production Company Fee) list the same
    way as kind='auto'. A labor section never offers a fringe target.
    Returns [{code, name, opts:[{id, label, kind, parent_id}]}]."""
    _COMPUTED = ('fringe_rollup', 'auto_calc')
    out = []
    for g in groups or []:
        raw_lines = g.get("lines") if isinstance(g, dict) else getattr(g, "lines", [])
        lines = [l for l in (raw_lines or [])
                 if (getattr(l, 'line_tag', None) or '') not in ('header', 'spacer')]
        if not lines:
            continue
        kids = {}
        for l in lines:
            if getattr(l, 'parent_line_id', None):
                kids.setdefault(l.parent_line_id, []).append(l)

        def _opt(l, kind):
            d = (getattr(l, 'description', None) or getattr(l, 'account_name', None) or 'untitled')[:60]
            return {"id": l.id, "kind": kind,
                    "parent_id": getattr(l, 'parent_line_id', None),
                    "label": (d if kind in ('fringe', 'auto') else f"{l.account_code} · {d}")}
        def _tag(l):
            return getattr(l, 'line_tag', None) or ''
        ordered, seen = [], set()
        for l in lines:
            if l.id in seen or getattr(l, 'parent_line_id', None) or _tag(l) in _COMPUTED:
                continue
            ordered.append(_opt(l, 'top')); seen.add(l.id)
            for k in kids.get(l.id, []):
                ordered.append(_opt(k, 'child')); seen.add(k.id)
        for l in lines:          # orphan children whose parent is elsewhere
            if l.id not in seen and _tag(l) not in _COMPUTED:
                ordered.append(_opt(l, 'child' if getattr(l, 'parent_line_id', None) else 'top'))
                seen.add(l.id)
        for l in lines:          # computed lines last (fringes by department, then autos)
            if _tag(l) == 'fringe_rollup':
                ordered.append(_opt(l, 'fringe'))
        for l in lines:
            if _tag(l) == 'auto_calc':
                ordered.append(_opt(l, 'auto'))
        code = g.get("code") if isinstance(g, dict) else getattr(g, "code", None)
        name = g.get("name") if isinstance(g, dict) else getattr(g, "name", None)
        out.append({"code": code, "name": name, "opts": ordered})
    return out


ADMIN_POOL_CSV_COLUMNS = ("date", "vendor", "amount", "card_last4", "account", "source",
                          "project", "line", "backup", "status",
                          "qbo_txn_id", "qbo_txn_type", "payment_method", "id")


def admin_pool_csv(rows):
    """The super-admin pool as CSV text (one row per charge, the same
    columns the page shows plus the ids). Pure; `rows` is the list
    _admin_pool_rows() builds. Status is 'not project' / 'conflict' /
    'claimed' / 'unclaimed' exactly as the ledger page labels it."""
    import csv as _csv
    import io as _io
    buf = _io.StringIO()
    w = _csv.writer(buf, lineterminator="\n")
    w.writerow(ADMIN_POOL_CSV_COLUMNS)
    for r in rows or []:
        status = ('not project' if r.get("excluded")
                  else 'conflict' if r.get("claim_conflict")
                  else 'claimed' if r.get("project_id") else 'unclaimed')
        amt = r.get("amount")
        w.writerow([
            r.get("date") or '', r.get("vendor") or '',
            ('' if amt is None else f"{float(amt):.2f}"),
            r.get("card_last4") or '', r.get("account") or '', r.get("source") or '',
            r.get("project") or '', r.get("line") or '', r.get("backup") or '',
            status,
            r.get("qbo_txn_id") or '', r.get("qbo_txn_type") or '',
            r.get("payment_method") or '', r.get("id") or '',
        ])
    return buf.getvalue()


# ── Vendor names: the ONE set of cleaning rules (Cleanup D, 2026-11-02) ──
# Three questions, three functions — never a private copy:
#   canon_vendor(v)       the STORAGE key for VendorAlias.raw_canonical and
#                         VendorCategoryMap.vendor_canonical. Its output is
#                         pinned by the rows already in the database, so it
#                         must not change shape (no prefix stripping here).
#   vendor_match_key(v)   the FUZZY-match form: processor prefix off, the
#                         " *ride-id" tail off, trailing reference tails off,
#                         then canon_vendor. app._normalize_vendor is an
#                         alias of this; _vendor_similarity builds on it.
#   payee_name_key(v)     the EXACT-name key (strip + lower) that
#                         payee_name_index and qbo_sync.match_vendor_to_payee
#                         index a payee's name / legal name / company under.

# Prefixes card processors and banks prepend to the merchant. ONE list
# (merged from app._normalize_vendor's and the '*' rule in _vendor_similarity);
# longest first so 'amazon mktpl*' wins over 'amazon mktp '.
PROCESSOR_PREFIXES = (
    'remote online deposit ', 'amazon mktpl*', 'amazon mktpl ', 'amazon mktp ',
    'amzn mktp* ', 'amzn mktp ', 'pos debit ', 'paypal *', 'paypal*', 'tst* ',
    'tst*', 'sq *', 'sq*', 'in *', 'in*', 'pp*',
)


def strip_processor_prefix(v):
    """Drop ONE leading payment-processor / bank prefix ('SQ *CASA VIDEO' →
    'CASA VIDEO', 'TST* TACO' → 'TACO', 'PAYPAL *SOUTHWESTAI' →
    'SOUTHWESTAI'). Case-insensitive; the rest of the string keeps its case.
    Strings with no known prefix come back unchanged (whitespace-trimmed)."""
    s = (v or '').strip()
    low = s.lower()
    for p in PROCESSOR_PREFIXES:
        if low.startswith(p):
            return s[len(p):].strip()
    return s


def canon_vendor(v):
    """Normalize a vendor string into the learned-mapping key: lowercase,
    drop store/terminal numbers + punctuation, collapse whitespace. Keeps
    'AMAZON MKTPLACE #1234' and 'amazon mktplace' on the same key.
    STORAGE KEY — see the note above; do not add prefix stripping here."""
    s = (v or '').lower()
    s = _re.sub(r'[#*]+\s*\d+', ' ', s)        # store / terminal numbers
    s = _re.sub(r'\b\d{3,}\b', ' ', s)          # long digit runs
    s = _re.sub(r'[^a-z0-9 ]+', ' ', s)         # punctuation
    s = _re.sub(r'\s+', ' ', s).strip()
    return s[:200]


def vendor_match_key(v):
    """The fuzzy-match form of a vendor string: 'LYFT *RIDE MON 8AM' and
    'Lyft' both come out 'lyft'; 'SQ *CASA VIDEO #12' → 'casa video'.
    Processor prefix off (strip_processor_prefix), the ' *…' tail off, a
    trailing reference tail ('… 8AM', '… 2026-05-01') off, then
    canon_vendor's rules."""
    if not v:
        return ''
    s = strip_processor_prefix(str(v))
    s = _re.split(r'\s+\*', s, maxsplit=1)[0]          # " *ride id" tails
    s = _re.sub(r'\s+\d[\w\-: ]*$', '', s).strip()      # trailing refs / dates
    return canon_vendor(s)


def payee_name_key(v):
    """The exact-name key a payee's name / legal name / company is indexed
    under (and a vendor string is looked up by): trimmed, lower-cased."""
    return (v or '').strip().lower()


def record_vendor_category(project_id, vendor, account_code, budget_line_id=None):
    """Upsert the learned vendor→COA mapping so future docs short-circuit the
    LLM. Strengthens confirm_count on repeat confirmations. Does NOT commit —
    the caller commits with its own change. Fail-soft."""
    if not vendor or account_code is None:
        return
    cv = canon_vendor(vendor)
    if not cv:
        return
    try:
        from models import VendorCategoryMap
        row = (VendorCategoryMap.query
               .filter_by(project_id=project_id, vendor_canonical=cv).first())
        if row:
            row.account_code = account_code
            if budget_line_id is not None:
                row.budget_line_id = budget_line_id
            row.confirm_count = (row.confirm_count or 0) + 1
            row.last_confirmed_at = datetime.utcnow()
        else:
            db.session.add(VendorCategoryMap(
                project_id=project_id, vendor_canonical=cv,
                account_code=account_code, budget_line_id=budget_line_id,
                confirm_count=1, last_confirmed_at=datetime.utcnow()))
    except Exception as e:
        log.warning("[vendor-cat] record failed for %r → %s: %s", vendor, account_code, e)


# ── Learned raw-vendor → clean display-name aliases (2026-06-18 cleanup) ──
def apply_vendor_alias_lookup(project_id, raw_vendor):
    """Return a known clean display name for this raw vendor (project-specific
    first, then global), or None. Lets cleanup short-circuit the LLM."""
    cv = canon_vendor(raw_vendor)
    if not cv:
        return None
    try:
        from models import VendorAlias
        row = (VendorAlias.query
               .filter(VendorAlias.raw_canonical == cv,
                       db.or_(VendorAlias.project_id == project_id,
                              VendorAlias.project_id.is_(None)))
               .order_by(VendorAlias.project_id.isnot(None).desc(),
                         VendorAlias.confirm_count.desc())
               .first())
        return row.clean_name if row else None
    except Exception as e:
        log.warning("[vendor-alias] lookup failed for %r: %s", raw_vendor, e)
        return None


def record_vendor_alias(project_id, raw_vendor, clean_name):
    """Upsert the learned raw→clean vendor alias. Does NOT commit. Fail-soft."""
    if not raw_vendor or not clean_name:
        return
    cv = canon_vendor(raw_vendor)
    if not cv:
        return
    clean_name = clean_name.strip()[:200]
    try:
        from models import VendorAlias
        row = (VendorAlias.query
               .filter_by(project_id=project_id, raw_canonical=cv).first())
        if row:
            row.clean_name = clean_name
            row.confirm_count = (row.confirm_count or 0) + 1
            row.last_confirmed_at = datetime.utcnow()
        else:
            db.session.add(VendorAlias(
                project_id=project_id, raw_canonical=cv, clean_name=clean_name,
                confirm_count=1, last_confirmed_at=datetime.utcnow()))
    except Exception as e:
        log.warning("[vendor-alias] record failed for %r → %r: %s", raw_vendor, clean_name, e)


def propagate_vendor_rename(project_id, old_vendor, new_vendor, exclude_tid=None):
    """Smart vendor rename: relabel every OTHER transaction + document in the
    project that shares the OLD vendor's canonical key (canon_vendor) to NEW, and
    learn the alias so future imports auto-apply. canon_vendor() groups variants
    like 'BRITISH A 1254247311910' / '...908' onto one key, so renaming one
    'British Airways' charge sweeps the rest. Does NOT commit (caller commits).
    Returns {count, items:[{type,id,old}], canon}. Fail-soft. (User 2026-06-22.)"""
    new_vendor = (new_vendor or '').strip()
    cv = canon_vendor(old_vendor)
    if not new_vendor or not cv:
        return {"count": 0, "items": [], "canon": cv}
    changed = []
    try:
        seen_docs = set()
        txns = (Transaction.query
                .filter(Transaction.project_id == project_id,
                        Transaction.vendor.isnot(None))
                .all())
        for t in txns:
            if exclude_tid and t.id == exclude_tid:
                if t.doc_upload_id:
                    seen_docs.add(t.doc_upload_id)
                continue
            if canon_vendor(t.vendor) == cv and (t.vendor or '') != new_vendor:
                changed.append({"type": "txn", "id": t.id, "old": t.vendor})
                t.vendor = new_vendor[:300]
                if t.doc_upload_id:
                    d = DocUpload.query.get(t.doc_upload_id)
                    if d and (d.vendor or '') != new_vendor:
                        changed.append({"type": "doc", "id": d.id, "old": d.vendor})
                        d.vendor = new_vendor[:200]
                    seen_docs.add(t.doc_upload_id)
        docs = (DocUpload.query
                .filter(DocUpload.project_id == project_id,
                        DocUpload.vendor.isnot(None))
                .all())
        for d in docs:
            if d.id in seen_docs:
                continue
            if canon_vendor(d.vendor) == cv and (d.vendor or '') != new_vendor:
                changed.append({"type": "doc", "id": d.id, "old": d.vendor})
                d.vendor = new_vendor[:200]
        record_vendor_alias(project_id, old_vendor, new_vendor)
    except Exception as e:
        log.warning("[vendor-rename] propagate failed: %s", e)
    return {"count": len(changed), "items": changed, "canon": cv}


# ── Double-coded duplicate detection (2026-06-18) ─────────────────────────
def scan_double_coded(project_id):
    """Find groups of transactions that look like the SAME spend coded into the
    budget more than once (double-counted actuals). Clusters by
    abs(amount)|date|canon_vendor|card_last4; flags a cluster only when ≥2 of
    its members are CODED (account_code or budget_line_id set), excluding members
    that share a split_group or the same doc_upload_id (legitimate splits / the
    same receipt). Read-only — returns cluster dicts, writes nothing.

    Returns: [{key, amount, overcount, vendor, date, members:[{tid, vendor,
    amount, coded, account_code, account_code_name, source, doc_upload_id,
    split_group}]}]"""
    rows = (Transaction.query
            .filter(Transaction.project_id == project_id,
                    Transaction.not_project_expense == False,   # noqa: E712
                    # Itemized sublines share vendor/date (and often amounts)
                    # by design — not duplicates. (Review fix 2026-07.)
                    Transaction.source != 'invoice_split',
                    Transaction.amount.isnot(None))
            .all())

    # Pre-load any linked documents so the dedup can use their distinguishing
    # identifiers (invoice/ticket number, original filename) — not just the
    # canonicalized vendor. (User 2026-06-22.)
    _doc_ids = {t.doc_upload_id for t in rows if t.doc_upload_id}
    _docmap = {}
    if _doc_ids:
        try:
            for d in DocUpload.query.filter(DocUpload.id.in_(_doc_ids)).all():
                _docmap[d.id] = d
        except Exception:
            _docmap = {}

    def _ident_sig(t):
        """Signature of the charge's DISTINGUISHING identifiers. canon_vendor()
        deliberately strips long digit runs (so 'AMAZON #1234' groups with
        'AMAZON #5678'), but for duplicate detection those numbers are exactly
        what separate two real purchases — e.g. two British Airways tickets
        'BRITISH A 1254247311910' vs '...908'. Recover them from the raw vendor
        text plus the linked doc's invoice number / filename. Two charges with
        different signatures are NOT the same spend."""
        ids = set(_re.findall(r'\d{4,}', (t.vendor or '')))
        d = _docmap.get(t.doc_upload_id) if t.doc_upload_id else None
        if d is not None:
            dn = _re.sub(r'\s+', '', str(getattr(d, 'doc_number', None) or ''))
            if dn:
                ids.add('dn:' + dn.lower())
            for tok in _re.findall(r'\d{4,}', (getattr(d, 'original_filename', None) or '')):
                ids.add('fn:' + tok)
        return '|'.join(sorted(ids))

    clusters = {}
    for t in rows:
        try:
            amt = float(t.amount)
        except (TypeError, ValueError):
            continue
        if amt == 0:
            continue
        date = (t.txn_date or '')[:10]
        # Key on the SIGNED amount so a charge (+) and its refund (−) of the same
        # magnitude are NOT treated as duplicates — only same-sign repeats are.
        # (User 2026-06-18.) Also key on the distinguishing-identifier signature
        # so two charges that differ only by ticket/invoice number stay separate
        # (User 2026-06-22.)
        sign = '+' if amt > 0 else '-'
        key = "%s%.2f|%s|%s|%s|%s" % (sign, abs(amt), date, canon_vendor(t.vendor),
                                      (t.card_last4 or '').strip(), _ident_sig(t))
        clusters.setdefault(key, []).append(t)

    out = []
    for key, members in clusters.items():
        if len(members) < 2:
            continue
        coded = [m for m in members
                 if (m.account_code is not None or m.budget_line_id is not None)]
        if len(coded) < 2:
            continue
        # Exclude legitimate splits: every coded member shares one split_group,
        # or they all point at the same receipt.
        sgs = {m.split_group for m in coded if m.split_group}
        if len(sgs) == 1 and all(m.split_group for m in coded):
            continue
        doc_ids = {m.doc_upload_id for m in coded if m.doc_upload_id}
        if len(doc_ids) == 1 and all(m.doc_upload_id for m in coded):
            continue
        amt = abs(float(coded[0].amount))
        out.append({
            "key": key,
            "amount": round(amt, 2),
            "overcount": round(amt * (len(coded) - 1), 2),   # extra copies
            "vendor": coded[0].vendor or '',
            "date": (coded[0].txn_date or '')[:10],
            "members": [{
                "tid": m.id, "vendor": m.vendor or '',
                "amount": float(m.amount) if m.amount is not None else None,
                "coded": (m.account_code is not None or m.budget_line_id is not None),
                "account_code": m.account_code,
                "account_code_name": m.account_code_name,
                "source": m.source, "doc_upload_id": m.doc_upload_id,
                "split_group": m.split_group,
            } for m in members],
        })
    out.sort(key=lambda c: c["overcount"], reverse=True)
    return out


def scan_duplicate_receipts(project_id):
    """Find RECEIPT documents that duplicate the same spend — e.g. a receipt
    'asking to be filed' when another receipt for that exact charge is already
    matched + coded. Complements scan_double_coded (which only fires when ≥2
    TRANSACTIONS are coded, so it misses a fresh dup sitting next to one
    already-matched receipt). Clusters receipt/invoice DocUploads by
    sign+abs(amount)|date|canon_vendor; a cluster with ≥2 docs means the same
    spend has multiple receipts. Read-only. (User 2026-06-22.)

    Returns [{key, amount, vendor, date, keep_doc_id,
              docs:[{id, filename, matched, coded, charge_tid}]}], duplicates first."""
    docs = (DocUpload.query
            .filter(DocUpload.project_id == project_id,
                    DocUpload.category.in_(('receipt', 'invoice')),
                    DocUpload.status != 'deleted',
                    DocUpload.status != 'duplicate',
                    DocUpload.is_duplicate == False,            # noqa: E712
                    DocUpload.amount.isnot(None))
            .all())
    if not docs:
        return []

    # Which docs are matched to an electronic charge, and which are coded.
    charge_by_doc, docrow_by_doc = {}, {}
    for t in (Transaction.query
              .filter(Transaction.project_id == project_id,
                      Transaction.doc_upload_id.isnot(None)).all()):
        if t.source in ('qbo_sync', 'csv_import', 'reconciled'):
            charge_by_doc[t.doc_upload_id] = t
        elif t.source == 'doc_upload':
            docrow_by_doc[t.doc_upload_id] = t

    def _coded(doc_id):
        c = charge_by_doc.get(doc_id)
        if c and (c.account_code is not None or c.budget_line_id is not None):
            return True
        r = docrow_by_doc.get(doc_id)
        return bool(r and (r.account_code is not None or r.budget_line_id is not None))

    clusters = {}
    for d in docs:
        try:
            amt = float(d.amount)
        except (TypeError, ValueError):
            continue
        if amt == 0:
            continue
        date = (d.doc_date.isoformat() if d.doc_date else '')[:10]
        sign = '+' if amt > 0 else '-'
        key = "%s%.2f|%s|%s" % (sign, abs(amt), date, canon_vendor(d.vendor))
        clusters.setdefault(key, []).append(d)

    out = []
    for key, members in clusters.items():
        if len(members) < 2:
            continue
        rows = [{
            "id": d.id,
            "filename": d.filed_filename or d.original_filename or f"Doc #{d.id}",
            "matched": d.id in charge_by_doc,
            "coded": _coded(d.id),
            "charge_tid": (charge_by_doc[d.id].id if d.id in charge_by_doc else None),
        } for d in members]
        # Keeper = a matched receipt, else a coded one, else the oldest doc id.
        keeper = next((r for r in rows if r["matched"]), None) \
            or next((r for r in rows if r["coded"]), None) \
            or min(rows, key=lambda r: r["id"])
        # Duplicates listed first (the ones the user should remove).
        rows.sort(key=lambda r: (r["id"] == keeper["id"], r["id"]))
        amt0 = abs(float(members[0].amount))
        out.append({
            "key": key, "amount": round(amt0, 2),
            "vendor": members[0].vendor or '',
            "date": (members[0].doc_date.isoformat() if members[0].doc_date else '')[:10],
            "keep_doc_id": keeper["id"], "docs": rows,
        })
    # Surface clusters where one side is already settled (matched/coded) first —
    # those are the highest-confidence "you already have this" duplicates.
    out.sort(key=lambda c: (not any(d["matched"] or d["coded"] for d in c["docs"]),
                            -c["amount"]))
    return out


# ── Which budget is "current" ─────────────────────────────────────────────
# "Current" means the NEWEST of its kind (Cleanup C, 2026-11-02): the live
# Working is the newest non-archived Working, the live Estimated the newest
# non-archived Estimated — and they may carry different version numbers
# (Estimated v8 can exist while spend is still tracked on Working v7). These
# three helpers are the ONE place that rule lives; app.py's rollups, pickers,
# the budget page header and the mismatch scan all read them.

def _current_of_kind(project_id, mode):
    """Newest non-archived, non-Actual budget of `mode` ('working' |
    'estimated'): a version_status='current' row wins, then the highest
    version_number, then the newest id. None when the project has none."""
    # NULL-safe: legacy rows can carry is_actual / version_status NULL
    # (a bare `is_actual == False` drops NULL rows — bit us 2026-05-19).
    q = (Budget.query
         .filter(Budget.project_id == project_id,
                 db.func.coalesce(Budget.is_actual, False) == False,      # noqa: E712
                 db.func.coalesce(Budget.version_status, 'current') != 'archived'))
    if mode == 'working':
        q = q.filter(Budget.budget_mode == 'working')
    else:
        q = q.filter(db.or_(Budget.budget_mode != 'working',
                            Budget.budget_mode.is_(None)))
    return (q.order_by(
                db.case((Budget.version_status == 'current', 0), else_=1),
                Budget.version_number.desc().nullslast(),
                Budget.id.desc())
             .first())


def get_current_working_budget(project_id):
    """The project's live Working budget (spend is tracked here), or None."""
    return _current_of_kind(project_id, 'working')


def get_current_estimated_budget(project_id):
    """The project's live Estimated budget, or None."""
    return _current_of_kind(project_id, 'estimated')


def get_live_budget(project_id):
    """The canonical budget rollups read from: the current Working, else the
    current Estimated (pre-Working projects code straight to Estimated
    lines). None only when the project has no budget at all."""
    return get_current_working_budget(project_id) or get_current_estimated_budget(project_id)


def pick_current(budgets, mode):
    """Pure twin of get_current_*: apply the same rule to an already-loaded
    list of Budget-like objects (version_status, version_number, id,
    budget_mode, is_actual). Used by budget_view, which has the project's
    budgets in hand, and by the unit tests."""
    def _kind(b):
        return 'working' if getattr(b, 'budget_mode', None) == 'working' else 'estimated'
    cands = [b for b in budgets
             if not getattr(b, 'is_actual', False)
             and getattr(b, 'version_status', None) != 'archived'
             and _kind(b) == mode]
    if not cands:
        return None
    return sorted(cands, key=lambda b: (
        0 if getattr(b, 'version_status', None) == 'current' else 1,
        -(getattr(b, 'version_number', None) or 0),
        -(getattr(b, 'id', None) or 0)))[0]


def working_exists_for_version(budgets, version_number):
    """The header rule: a Working budget exists for THIS version number
    (not merely any Working at all). Drives the Viewing toggle, the
    "Start Working vN" button and the yellow state-B strip."""
    vn = version_number or 1
    return any(getattr(b, 'budget_mode', None) == 'working'
               and not getattr(b, 'is_actual', False)
               and getattr(b, 'version_status', None) != 'archived'
               and (getattr(b, 'version_number', None) or 1) == vn
               for b in budgets)


# The ONE variance-basis key set (budget page control, Top Sheet, Export
# dialog, PDF). Every basis reads "+ = under": vs_estimated = Est − Actual,
# vs_working = Working − Actual, est_to_working = Est − Working. The old
# page keys (e_vs_w / w_vs_a / e_vs_a) and export keys (est_v_work /
# work_v_act / est_v_act) are accepted as aliases so bookmarks keep working.
VARIANCE_BASES = ('vs_estimated', 'vs_working', 'est_to_working')
VARIANCE_BASIS_ALIASES = {'est_v_work': 'est_to_working', 'work_v_act': 'vs_working',
                          'est_v_act': 'vs_estimated',
                          'e_vs_w': 'est_to_working', 'w_vs_a': 'vs_working',
                          'e_vs_a': 'vs_estimated'}
VARIANCE_LABELS = {'vs_estimated': 'Var vs Estimated', 'vs_working': 'Var vs Working',
                   'est_to_working': 'Est → Working'}


def normalize_variance_basis(key, default='vs_working'):
    """Map any spelling (current or legacy) to the one key set; unknown or
    empty → `default`."""
    k = VARIANCE_BASIS_ALIASES.get(key or '', key or '')
    return k if k in VARIANCE_BASES else default


def variance_value(basis, estimated, working, actual):
    """The number a variance cell shows for `basis` (+ = under)."""
    basis = normalize_variance_basis(basis)
    e, w, a = float(estimated or 0), float(working or 0), float(actual or 0)
    if basis == 'est_to_working':
        return e - w
    if basis == 'vs_working':
        return w - a
    return e - a


def line_overages(planned_by_line, actual_by_line, min_dollars=50.0, min_pct=0.10):
    """Pure rule for the "line over plan" scan: a line is flagged when its
    coded spend exceeds the plan by at least `min_dollars` AND `min_pct`.
    Lines with no positive plan are skipped. Returns
    [(line_id, planned, actual, over)] in line-id order."""
    out = []
    for lid in sorted(planned_by_line):
        planned = float(planned_by_line[lid] or 0)
        if planned <= 0:
            continue
        actual = float(actual_by_line.get(lid, 0.0) or 0)
        over = actual - planned
        if over < min_dollars or over < planned * min_pct:
            continue
        out.append((lid, planned, actual, over))
    return out


def version_strip_state(estimated, working, viewed=None):
    """Which of the three header states to render (TabStrip artboard):
      'paired'  — Estimated vN and Working vN both exist (A)
      'ahead'   — the newest Estimated has no Working of its own; spend is
                  still tracked on an older Working (B)
      'none'    — no Working yet (C)
    `estimated` / `working` are the live budgets (or None)."""
    if working is None:
        return 'none'
    ev = (getattr(estimated, 'version_number', None) or 1) if estimated else None
    wv = getattr(working, 'version_number', None) or 1
    if ev is not None and ev > wv:
        return 'ahead'
    return 'paired'


def ensure_section_in_working_budget(project_id, account_code, account_name):
    """Ensure the project's Working budget has at least one line under
    the given account_code. Used when a user codes a transaction to a
    section that doesn't yet exist in their budget — instead of leaving
    the section invisible, we auto-add a placeholder line so the section
    shows on the budget view (with $0 estimate / $0 working until the
    user fills it in).

    Behavior:
      - No Working budget yet: auto-init it (clone from Estimated, or
        create a fresh shell if there's no Estimated either).
      - Working has a line with this account_code already: no-op.
      - Otherwise: insert a single placeholder line. Also mirror into
        the Actual budget if one exists.

    Returns a dict with what happened so the caller can surface a toast:
      {'created': bool, 'working_line_id': int|None, 'actual_line_id': int|None,
       'working_was_just_created': bool}
    """
    from datetime import datetime as _dt
    if not account_code:
        return {'created': False, 'working_line_id': None,
                'actual_line_id': None, 'working_was_just_created': False}
    try:
        code_int = int(account_code)
    except (TypeError, ValueError):
        return {'created': False, 'working_line_id': None,
                'actual_line_id': None, 'working_was_just_created': False}

    # Get the Working budget. OWNER DECISION 2026-09-01: never auto-init a
    # Working just because actuals are being coded — pre-SOW the Estimated is
    # the live document and a clone would freeze wrong numbers. With no
    # Working, seed the placeholder into the current Estimated instead (the
    # txn codes to the Estimated line; the explicit "Initialize Working"
    # action later carries lines + codings onto the new Working).
    working = get_current_working_budget(project_id)
    working_was_just_created = False
    if not working:
        est = get_current_estimated_budget(project_id)
        if not est:
            # No Estimated either — bail. Coding still works at the
            # transaction level; we just can't auto-add a line without
            # any budget shell to put it in. This is a brand-new
            # project edge case.
            return {'created': False, 'working_line_id': None,
                    'actual_line_id': None, 'working_was_just_created': False}
        existing_est = (BudgetLine.query
                        .filter_by(budget_id=est.id, account_code=code_int)
                        .first())
        if existing_est:
            return {'created': False, 'working_line_id': existing_est.id,
                    'actual_line_id': None, 'working_was_just_created': False}
        est_line = BudgetLine(
            budget_id          = est.id,
            account_code       = code_int,
            account_name       = (account_name or '')[:100],
            description        = '(Auto-added — no estimate)',
            is_labor           = False,
            quantity           = 1,
            days               = 1,
            rate               = 0,
            fringe_type        = 'N',
            agent_pct          = 0,
            rate_type          = 'flat_day',
            estimated_total    = 0,
            working_total      = 0,
            sort_order         = 99999,
        )
        db.session.add(est_line)
        db.session.flush()
        return {'created': True, 'working_line_id': est_line.id,
                'actual_line_id': None, 'working_was_just_created': False}

    # Already a line with this code? no-op.
    existing = (BudgetLine.query
                .filter_by(budget_id=working.id, account_code=code_int)
                .first())
    if existing:
        return {'created': False, 'working_line_id': existing.id,
                'actual_line_id': None,
                'working_was_just_created': working_was_just_created}

    # Also seed an Estimated peer line if Estimated exists. Per user
    # 2026-05-01 — unbudgeted actuals should appear in Working AND
    # Estimated as $0 (so the section shows up regardless of which
    # budget the user is viewing). The Working line gets source_line_id
    # pointing at the Estimated peer to maintain the chain.
    estimated = get_current_estimated_budget(project_id)
    estimated_peer_id = None
    if estimated:
        # Skip if Estimated already has a line under this code.
        existing_est = (BudgetLine.query
                        .filter_by(budget_id=estimated.id, account_code=code_int)
                        .first())
        if existing_est:
            estimated_peer_id = existing_est.id
        else:
            est_line = BudgetLine(
                budget_id          = estimated.id,
                account_code       = code_int,
                account_name       = (account_name or '')[:100],
                description        = '(Auto-added — no estimate)',
                is_labor           = False,
                quantity           = 1,
                days               = 1,
                rate               = 0,
                fringe_type        = 'N',
                agent_pct          = 0,
                rate_type          = 'flat_day',
                estimated_total    = 0,
                working_total      = 0,
                sort_order         = 99999,
            )
            db.session.add(est_line)
            db.session.flush()
            estimated_peer_id = est_line.id

    # Insert Working placeholder. Sort order: large number so it lands at the
    # bottom of its section (existing lines keep their relative order).
    placeholder = BudgetLine(
        budget_id          = working.id,
        account_code       = code_int,
        account_name       = (account_name or '')[:100],
        description        = '(Auto-added — no estimate)',
        is_labor           = False,
        quantity           = 1,
        days               = 1,
        rate               = 0,
        fringe_type        = 'N',
        agent_pct          = 0,
        rate_type          = 'flat_day',
        estimated_total    = 0,
        working_total      = 0,
        sort_order         = 99999,
        source_line_id     = estimated_peer_id,
    )
    db.session.add(placeholder)
    db.session.flush()  # get placeholder.id
    placeholder_actual_id = None

    # PHASE 1 (money-path migration 2026-07): transactions now code DIRECTLY
    # to Working lines, so we no longer mirror this placeholder onto the Actual
    # clone. Working + Estimated mirroring above is unchanged. (Legacy Actual
    # budgets keep their existing peer lines; a placeholder created now just
    # won't have an Actual peer — direct-coded txns don't need one.)
    #   placeholder_actual_id stays None; the return contract is unchanged.

    return {'created': True,
            'working_line_id': placeholder.id,
            'actual_line_id':  placeholder_actual_id,
            'working_was_just_created': working_was_just_created}


# ── Transaction linking (codes straight to the current Working line) ──

def link_transaction_to_line(transaction_id, working_line_id, user_id=None):
    """Atomic: code a Transaction to a budget line.

    The transaction codes DIRECTLY to the current WORKING budget's line (Phase
    1 money-path migration, 2026-07). No Actual budget is cloned and no Actual
    peer line is materialized on this path. An Estimated pick is translated to
    its Working peer when a current Working exists; when none exists the txn
    codes to the Estimated line itself (owner decision 2026-09-01: no silent
    Working init on first coding — the later "Initialize Working" button
    carries the codings across).

    Side effects on the row: budget_line_id / account_code / account_code_name
    set, match_status → 'confirmed', not_project_expense → False (assigning a
    line is a claim), AI suggestion cleared, vendor→COA mapping reinforced,
    one-way line→person stamp on the doc. An unclaimed QuickBooks row is
    claimed by the line's project (claim_for). Commits.

    Backward compat: if the PICKED line turns out to be on an ACTUAL budget
    (re-coding from the legacy Actual view / stale UI state), we translate it
    back to its Working peer (via source_line_id, falling back to
    account_code+description match on the current Working) before writing, so
    the stored budget_line_id is always a WORKING id. If no Working peer can be
    found we keep pointing at the actual line (never lose the coding) and log.

    Returns dict: {
      'transaction_id': int,
      'budget_line_id': int (the WORKING line the txn now points at),
      'actual_budget_id': int (the budget id of the line written — now the
                               Working budget's id; kept for return-shape
                               compat with callers/UI),
      'actual_was_just_created': bool,   # always False now (no Actual cloned)
      'working_was_just_created': bool,
    }

    Raises ValueError on bad inputs.
    """
    txn = Transaction.query.get(transaction_id)
    if not txn:
        raise ValueError(f"transaction {transaction_id} not found")
    working_line = BudgetLine.query.get(working_line_id)
    if not working_line:
        raise ValueError(f"budget_line {working_line_id} not found")

    # Which budget is the picked line on? A legacy ACTUAL line is translated
    # back to its Working peer below; a Working / Estimated line is used as-is
    # (or swapped for its Working peer when one exists).
    line_budget = Budget.query.get(working_line.budget_id)
    # An unclaimed QuickBooks row (company pool) is claimed by the line's
    # project here; a row another project holds is refused (ClaimError).
    project_id = txn.project_id or (line_budget.project_id if line_budget else None)
    if not project_id:
        raise ValueError(f"transaction {transaction_id} has no project")
    # Cross-project guard: a stale UI or crafted request must not code one
    # project's money to another project's budget line. (Code review 2026-06-04.)
    if line_budget and line_budget.project_id != project_id:
        raise ValueError(
            f"budget line {working_line_id} belongs to project "
            f"{line_budget.project_id}, not transaction {transaction_id}'s "
            f"project {project_id}")
    claim_for(txn, project_id, why='coded to a line')
    actual_was_just_made  = False
    working_was_just_made = False

    if line_budget and line_budget.is_actual:
        # PHASE 1: the pick is on a legacy ACTUAL budget (re-coding from the
        # Actual view / stale UI). Translate it back to its Working peer so we
        # store a WORKING id — never an Actual id — going forward.
        target_line = None
        if working_line.source_line_id:
            _peer = BudgetLine.query.get(working_line.source_line_id)
            # source_line_id on an Actual line points at its Working peer.
            if _peer and _peer.budget_id:
                _peer_budget = Budget.query.get(_peer.budget_id)
                if _peer_budget and not _peer_budget.is_actual \
                        and _peer_budget.project_id == project_id:
                    target_line = _peer
        if target_line is None:
            # Fall back to matching the current Working budget by
            # account_code + description (structural drift / missing back-ptr).
            working_budget = get_current_working_budget(project_id)
            if working_budget:
                _match = (BudgetLine.query
                          .filter_by(budget_id=working_budget.id,
                                     account_code=working_line.account_code)
                          .filter(BudgetLine.description == working_line.description)
                          .first())
                if not _match:
                    _match = (BudgetLine.query
                              .filter_by(budget_id=working_budget.id,
                                         account_code=working_line.account_code)
                              .first())
                if _match:
                    target_line = _match
        if target_line is not None:
            working_line = target_line
        else:
            # No Working peer found — keep the coding on the actual line rather
            # than dropping it, but warn so it can be remapped in Phase 2.
            log.warning(
                "[actuals/phase1] txn %s picked ACTUAL line %s with no Working "
                "peer (source_line_id=%s); coding to the Actual line as a "
                "fallback to avoid data loss",
                txn.id, working_line.id, working_line.source_line_id)
    else:
        # Picked from Working or Estimated. If a current Working budget exists
        # and the pick was on Estimated, translate it to its Working peer so we
        # store a Working id. We do NOT clone/write an Actual budget anymore
        # (Phase 1): the txn codes directly to Working.
        #
        # OWNER DECISION 2026-09-01: if NO Working exists, do NOT auto-init one.
        # Pre-SOW the Estimated *is* the live document — a Working silently
        # cloned off it at first coding freezes numbers that are still moving
        # ("our numbers wouldn't be right if we initialized a working"). The
        # txn codes straight to the picked (Estimated) line; the explicit
        # "Initialize Working vN" button later carries these codings onto the
        # new Working (create_working_from_estimated carries them across).
        working_budget = get_current_working_budget(project_id)
        if working_budget and line_budget and not line_budget.is_actual \
                and line_budget.id != working_budget.id:
            _peer = (BudgetLine.query
                     .filter_by(budget_id=working_budget.id,
                                source_line_id=working_line.id)
                     .first())
            if _peer:
                working_line = _peer

    txn.budget_line_id    = working_line.id
    txn.account_code      = working_line.account_code
    txn.account_code_name = working_line.account_name
    txn.match_status      = 'confirmed'
    # Assigning a line IS a claim: a charge that was marked not-project and is
    # now given a line must come back into every total. set-coa already did
    # this; set-line did not (ACTUALS_HANDOFF Phase 0, 2026-10-30).
    txn.not_project_expense = False
    # …and the recoverable-exclusion audit columns go with it (Phase 1): the
    # row is back in the project, so "who excluded it and why" no longer
    # applies. The ActivityLog keeps the history.
    txn.excluded_at         = None
    txn.excluded_by_user_id = None
    txn.excluded_reason     = None
    txn.updated_at        = datetime.utcnow()
    if user_id:
        txn.created_via_user_id = user_id
    # The row is now coded → drop any advisory AI suggestion, and reinforce the
    # learned vendor→category mapping so future uploads can short-circuit the
    # model. (2026-06-18 auto-coding.)
    txn.ai_suggested_code = None
    txn.ai_suggested_code_name = None
    txn.ai_code_confidence = None
    txn.ai_code_reason = None
    record_vendor_category(project_id, txn.vendor, working_line.account_code)

    # ── Payment attribution (Documents Phase 3) ──────────────────────────
    # The line has exactly one crew member → this money went to them. Fills
    # a NULL payee_id and replaces an earlier 'line' value (a re-coding
    # moves the attribution with the row); never touches a value another
    # source set. A line with no single person clears a stale 'line' value.
    try:
        _owner = single_line_owner(working_line.id)
        set_txn_payee(txn, _owner, 'line', replace=('line',))
    except Exception:
        log.info("[actuals/payee] line attribution skipped (fail-open) for txn %s",
                 getattr(txn, 'id', None), exc_info=True)

    # ── ONE-WAY line→person auto-assign (User 2026-07) ────────────────────
    # If this txn carries a document that has NO person yet, and the line we
    # just coded it to has an unambiguous assigned person, stamp that person
    # onto the doc. This is DELIBERATELY one-directional: coding an invoice to
    # a person's budget line files it under that person, but we NEVER infer a
    # line from a doc's person (one person can occupy many lines) and NEVER
    # overwrite a person already on the doc. Fail-open — a hiccup here must not
    # block the coding that just committed above.
    try:
        if txn.doc_upload_id:
            doc = DocUpload.query.get(txn.doc_upload_id)
            if doc is not None and not doc.crew_member_id:
                person_id = working_line.assigned_crew_id
                if not person_id:
                    # No direct assignee — accept it only if the line has
                    # EXACTLY ONE crew-assignment member (unambiguous).
                    members = [ca.crew_member_id for ca in
                               CrewAssignment.query.filter_by(
                                   budget_line_id=working_line.id).all()
                               if ca.crew_member_id]
                    uniq = set(members)
                    if len(uniq) == 1:
                        person_id = next(iter(uniq))
                if person_id:
                    doc.crew_member_id = person_id
                    log.info(
                        "[actuals/auto-assign] doc #%s → crew_member %s via "
                        "line %s (txn %s); one-way line→person",
                        doc.id, person_id, working_line.id, txn.id)
    except Exception:
        log.info("[actuals/auto-assign] skipped (fail-open) for txn %s",
                 getattr(txn, 'id', None), exc_info=True)

    db.session.commit()

    # ── Absorb the attached receipt's own row (Nov 2 2026) ────────────────
    # A bank charge that already carries a receipt (doc_upload_id set, e.g.
    # an invoice attached before the charge was coded, or the invoice's
    # auto-match confirmed without a line) still had the receipt's own
    # source='doc_upload' placeholder row sitting on a line. Coding the
    # charge then DOUBLE-COUNTED the line (invoice row + bank row — Steven's
    # Ryan Mauskopf $4,500 ACH on Producer / 1st AD). confirm_match is the
    # one merge path (carries evidence, backup links, payee; deletes the
    # placeholder); run it whenever a sister row exists. Fail-open.
    try:
        if txn.doc_upload_id and txn.source != 'doc_upload':
            _sister = (Transaction.query
                       .filter(Transaction.doc_upload_id == txn.doc_upload_id,
                               Transaction.source == 'doc_upload',
                               Transaction.id != txn.id).first())
            if _sister is not None:
                confirm_match(txn.id)
                db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        log.warning("[actuals/link] receipt-row absorb failed for txn %s "
                    "(coding kept)", getattr(txn, 'id', None), exc_info=True)

    return {
        'transaction_id':           txn.id,
        'budget_line_id':           working_line.id,
        # 'actual_budget_id' kept for return-shape compat — now the budget id
        # of the line the txn points at (the Working budget, or the legacy
        # Actual budget in the no-peer fallback case).
        'actual_budget_id':         working_line.budget_id,
        'actual_was_just_created':  actual_was_just_made,
        'working_was_just_created': working_was_just_made,
    }


def unlink_transaction(transaction_id):
    """Clear ALL coding on a transaction — budget_line_id, account_code,
    account_code_name, match_status. Doesn't delete the Actual line
    even if it becomes empty (user may relink later).

    Per user 2026-04-30: "clear assignments are not holding." Root
    cause was clearing budget_line_id but leaving account_code set,
    so the row still looked coded on the next render and the section-
    only option re-selected itself. Now we wipe the whole coding tuple."""
    txn = Transaction.query.get(transaction_id)
    if not txn:
        raise ValueError(f"transaction {transaction_id} not found")
    txn.budget_line_id    = None
    txn.account_code      = None
    txn.account_code_name = None
    txn.match_status      = 'unmatched'
    txn.updated_at        = datetime.utcnow()
    # An attribution that came ONLY from the line goes with the line
    # (Documents Phase 3); any other source stays.
    try:
        set_txn_payee(txn, None, 'line')
    except Exception:
        pass
    # A QuickBooks row with nothing left on it goes back to the company pool.
    release(txn, why='coding cleared')
    db.session.commit()
    return {'transaction_id': txn.id}


# ── Auto-match (suggest, never silently confirm) ─────────────────────

def _vendor_similarity(a, b):
    """Crude fuzzy vendor matcher. Returns 0.0–1.0.

    No external dep: lowercases, strips punctuation, then computes:
      • exact match               → 1.0
      • substring containment     → 0.85 if one wholly contains the other
      • token-set Jaccard (≥0.5)  → ratio of shared words / union
      • prefix match (≥4 chars)   → 0.7

    Card/bank descriptors prefix the real merchant with a payment processor —
    "SQ *CASA VIDEO", "IN *GOVISION LLC", "TST* TACO", "PAYPAL *SOUTHWESTAI".
    So we also try the merchant part AFTER the "*" and take the best score, so
    those land at full strength instead of being diluted by the prefix token.
    (User 2026-06-02.) Anything below 0.45 is treated as no-match by the caller."""
    if not a or not b:
        return 0.0
    # ONE cleaning rule (Cleanup D): canon_vendor for the base form,
    # vendor_match_key for the processor-prefix-free form. (This used to
    # carry its own lowercase/punctuation normaliser.)
    norm = canon_vendor

    def _variants(raw):
        vs = set()
        base = norm(raw)
        if base:
            vs.add(base)
        mk = vendor_match_key(raw)
        if mk:
            vs.add(mk)
        if '*' in (raw or ''):                 # merchant after an unlisted processor mark
            tail = norm((raw or '').split('*')[-1])
            if tail:
                vs.add(tail)
        return vs

    def _core(A, B):
        if not A or not B:
            return 0.0
        if A == B:
            return 1.0
        if A in B or B in A:
            return 0.85
        at, bt = set(A.split()), set(B.split())
        inter, union = at & bt, at | bt
        if inter and union:
            jaccard = len(inter) / len(union)
            if jaccard >= 0.5:
                return jaccard
        if len(A) >= 4 and len(B) >= 4 and A[:4] == B[:4]:
            return 0.7
        return 0.0

    best = 0.0
    for A in _variants(a):
        for B in _variants(b):
            s = _core(A, B)
            if s > best:
                best = s
    return best


def ensure_doc_txns(project_id):
    """Create the missing doc-born Transaction row for every receipt /
    invoice DocUpload in the project (owner 2026-10-07: "lots of
    transactions coming in that are not being matched that should
    match"). The auto-matcher can only pair a charge with a receipt
    that HAS a source='doc_upload' row — but several upload paths
    historically never created one, so those receipts were invisible
    to matching entirely. Idempotent: skips docs that already have any
    Transaction, duplicates, trashed docs, and docs with no OCR'd
    amount (nothing to match on). Returns the number created."""
    created = 0
    docs = (DocUpload.query
            .filter(DocUpload.project_id == project_id,
                    DocUpload.category.in_(('receipt', 'invoice')),
                    DocUpload.amount.isnot(None))
            .all())
    if not docs:
        return 0
    have = {r[0] for r in db.session.query(Transaction.doc_upload_id)
            .filter(Transaction.project_id == project_id,
                    Transaction.doc_upload_id.isnot(None)).all()}
    for d in docs:
        if d.id in have:
            continue
        if getattr(d, 'is_duplicate', False) or (d.status or '') in ('duplicate', 'deleted'):
            continue
        try:
            db.session.add(Transaction(
                project_id=project_id, source='doc_upload', doc_upload_id=d.id,
                vendor=d.vendor, amount=d.amount,
                txn_date=d.doc_date.isoformat() if d.doc_date else None,
                card_last4=getattr(d, 'card_last4', None),
                is_expense=True, match_status='unmatched'))
            created += 1
        except Exception:
            db.session.rollback()
            return created
    if created:
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            created = 0
    return created


def run_auto_match(project_id):
    """The auto-matcher every path calls after a sync, an upload or a
    workspace load. Since Phase 5 (2026-11-03) it is a thin name over
    refresh_match_suggestions — THE matching engine (score_match /
    rank_candidates / suggest_pairs) — so the chip on a charge, the
    candidate panel and the Review-matches dialog all agree. Writes
    'suggested', never 'confirmed'. Returns the engine's counts
    ({'suggestions', 'cleared', 'inspected', 'qbo_unmatched', 'doc_open',
    'suggested', 'scanned'})."""
    return refresh_match_suggestions(project_id)


def find_match_candidates(project_id, amount_tol=0.0, date_window=3,
                          use_vendor=False, card=None, limit_charges=300):
    """Configurable candidate finder for the Review-matches screen (User
    2026-06-16). For every still-unmatched electronic charge, return a ranked
    shortlist of candidate receipts that satisfy the criteria, each with a
    confidence %. Amount is the primary gate (uses the FX-converted USD amount
    already stored on foreign receipts). date_window=None means "ignore date";
    use_vendor=False means "don't even check the vendor name". The caller
    suggests the top candidate when confidence ≥ 80% and it isn't a near-tie,
    and always shows the alternatives so the user can confirm or switch.

    Efficient on huge projects: receipts are sorted by amount and the per-charge
    scan is bounded to the [amount-tol, amount+tol] slice via bisect.
    """
    import datetime as _dt, bisect as _bisect
    amount_tol = max(0.0, float(amount_tol or 0.0))

    qbo_unmatched = (Transaction.query
                     .filter(pool_filter(project_id),
                             Transaction.source.in_(('qbo_sync', 'csv_import')),
                             Transaction.match_status == 'unmatched',
                             Transaction.doc_upload_id.is_(None),
                             Transaction.not_project_expense == False)  # noqa: E712
                     .all())
    doc_open = (Transaction.query
                .filter_by(project_id=project_id, source='doc_upload',
                           not_project_expense=False)
                .all())
    _dup_doc_ids = {r[0] for r in db.session.query(DocUpload.id)
                    .filter(DocUpload.project_id == project_id,
                            db.or_(DocUpload.is_duplicate == True,       # noqa: E712
                                   DocUpload.status == 'duplicate',
                                   DocUpload.status == 'deleted')).all()}
    _proof_doc_ids = {r[0] for r in db.session.query(DocUpload.id)
                      .filter(DocUpload.project_id == project_id,
                              DocUpload.category.in_(('receipt', 'invoice'))).all()}
    taken_docs = {t.doc_upload_id for t in Transaction.query.filter(
        pool_filter(project_id),
        Transaction.source.in_(('qbo_sync', 'csv_import')),
        Transaction.doc_upload_id.isnot(None)).all() if t.doc_upload_id}
    rejected_pairs = {(r.transaction_id, r.doc_upload_id) for r in
                      MatchRejection.query.filter_by(project_id=project_id).all()}

    # Doc display metadata (filename + image flag for the thumbnail).
    doc_meta = {}
    _need = [t.doc_upload_id for t in doc_open if t.doc_upload_id]
    if _need:
        for d in (DocUpload.query
                  .filter(DocUpload.id.in_(set(_need))).all()):
            doc_meta[d.id] = {
                "file": d.filed_filename or d.original_filename or f"Doc #{d.id}",
                "is_image": bool(d.content_type and d.content_type.startswith('image/')),
                "card": (d.card_last4 or None),
            }

    # Available cards (last-4) across the unmatched charges + open receipts —
    # lets the UI offer a "card" dropdown; selecting one narrows both sides to
    # the same card. (User 2026-06-16.) Computed before filtering so the full
    # list always shows.
    card = (str(card).strip() if card not in (None, '', 'any', 'Any', 'all') else None)
    _all_cards = set()
    for t in qbo_unmatched:
        _c = (getattr(t, 'card_last4', None) or '').strip()
        if _c:
            _all_cards.add(_c)
    for _m in doc_meta.values():
        _c = (_m.get('card') or '').strip()
        if _c:
            _all_cards.add(_c)
    available_cards = sorted(_all_cards)

    # Build the receipt pool from DocUpload DIRECTLY (not from doc_upload txns)
    # so EVERY available receipt is matchable — including ones whose own ledger
    # row was removed by a prior match/dedup. This mirrors the per-row 'find
    # receipt' picker (which is why that surfaces receipts Find-matches used to
    # miss). (User 2026-06-17.)
    _docs_pool = (DocUpload.query
                  .filter(DocUpload.project_id == project_id,
                          DocUpload.category.in_(('receipt', 'invoice')),
                          DocUpload.status != 'deleted',
                          DocUpload.status != 'duplicate',
                          DocUpload.is_duplicate == False)   # noqa: E712
                  .all())
    d_rows = []
    for doc in _docs_pool:
        if doc.id in taken_docs:                      # already linked to a charge
            continue
        if doc.amount is None or not doc.doc_date:
            continue
        _dcard = (doc.card_last4 or '').strip()
        if card and _dcard != card:
            continue
        if _dcard:
            _all_cards.add(_dcard)
        d_rows.append((float(doc.amount), doc.doc_date, doc))
    # Sort/bisect on the MAGNITUDE so refunds match too: a refund receipt is
    # stored negative (e.g. -107.17) while the bank credit is the same dollar
    # amount with a different sign — same |amount|, opposite sign. Matching on
    # |amount| pairs them; the sign difference is surfaced as a "refund?" flag.
    # (User 2026-06-17.)
    d_rows.sort(key=lambda r: abs(r[0]))
    d_amounts = [abs(r[0]) for r in d_rows]
    available_cards = sorted(_all_cards)   # include the receipt pool's cards

    def _confidence(amt_delta, day_gap, vendor_sim):
        # Filtering (amount_tol / date_window / use_vendor) decides which pairs
        # are *eligible*; confidence always scores the ACTUAL closeness so a
        # coincidental same-amount pair (different vendor, months apart) never
        # reads as a confident suggestion even with date/vendor filters off.
        # Amount is primary; date proximity (30-day horizon) + vendor similarity
        # corroborate. (User 2026-06-16 — fixed the "$20 FedEx ≈ $20 Turboscribe
        # 163 days apart = 100%" false positive.)
        amt_s = 1.0 if amt_delta < 0.01 else max(0.0, 1.0 - amt_delta / max(amount_tol, 1.0))
        date_s = max(0.0, 1.0 - day_gap / 30.0)
        return 0.50 * amt_s + 0.30 * date_s + 0.20 * vendor_sim

    results = []
    for q in qbo_unmatched:
        if q.amount is None or not q.txn_date:
            continue
        if card and (getattr(q, 'card_last4', None) or '').strip() != card:
            continue
        try:
            q_amt = float(q.amount)
            q_dt = _dt.date.fromisoformat(q.txn_date[:10])
        except (TypeError, ValueError):
            continue
        q_abs = abs(q_amt)
        lo = _bisect.bisect_left(d_amounts, q_abs - amount_tol - 0.001)
        hi = _bisect.bisect_right(d_amounts, q_abs + amount_tol + 0.001)
        cands = []
        for idx in range(lo, hi):
            d_amt, d_dt, d = d_rows[idx]          # d is a DocUpload
            if (q.id, d.id) in rejected_pairs:
                continue
            day_gap = abs((d_dt - q_dt).days)
            if date_window is not None and day_gap > date_window:
                continue
            vendor_sim = _vendor_similarity(q.vendor, d.vendor)
            if use_vendor and vendor_sim < 0.30:
                continue
            amt_delta = abs(abs(d_amt) - q_abs)            # magnitude delta
            opposite_sign = (d_amt < 0) != (q_amt < 0)     # likely a refund pairing
            conf = _confidence(amt_delta, day_gap, vendor_sim)
            cands.append({
                "doc_upload_id": d.id,
                "file": (d.filed_filename or d.original_filename or ("Doc #" + str(d.id))),
                "is_image": bool(d.content_type and d.content_type.startswith('image/')),
                "card": (d.card_last4 or None),
                "vendor": d.vendor, "amount": d_amt,
                "date": d.doc_date.isoformat() if d.doc_date else None,
                "amount_delta": round(amt_delta, 2),
                "opposite_sign": opposite_sign,
                "day_gap": day_gap, "vendor_match": vendor_sim >= 0.45,
                "confidence": round(conf, 4),
            })
        if not cands:
            continue
        cands.sort(key=lambda c: (-c["confidence"], c["amount_delta"], c["day_gap"]))
        top = cands[0]
        ambiguous = len(cands) > 1 and cands[1]["confidence"] >= top["confidence"] - 0.05
        suggested = (top["confidence"] >= 0.80) and not ambiguous
        results.append({
            "charge": {"id": q.id, "vendor": q.vendor, "amount": q_amt,
                       "date": q.txn_date[:10] if q.txn_date else None,
                       "card4": getattr(q, 'card_last4', None)},
            "suggested_doc_id": top["doc_upload_id"] if suggested else None,
            "top_confidence": top["confidence"],
            "ambiguous": ambiguous,
            "candidates": cands[:6],
        })
    # Best (most confident) charges first; cap payload.
    results.sort(key=lambda r: -r["top_confidence"])
    return {
        "criteria": {"amount_tol": amount_tol, "date_window": date_window,
                     "use_vendor": use_vendor, "card": card},
        "available_cards": available_cards,
        "charges_with_candidates": len(results),
        "results": results[:limit_charges],
    }


def find_split_candidates(project_id, date_window=5, card=None, max_combo=3):
    """Detect SPLIT receipts — one receipt whose total equals the SUM of several
    unmatched charges (e.g. Turo posts a rental as two card charges). The card
    statement shows the individual charges; the receipt shows the total, so
    `total == sum(charges)` is the tell. Conservative on purpose: candidate
    charges must share the receipt's card OR have a strong vendor match, fall
    within ±date_window days, each be smaller than the total, and a 2–3 charge
    combination must sum to the total within ±$0.02. (User 2026-06-16.)"""
    import datetime as _dt, itertools as _it

    charges = (Transaction.query
               .filter(pool_filter(project_id),
                       Transaction.source.in_(('qbo_sync', 'csv_import')),
                       Transaction.match_status == 'unmatched',
                       Transaction.doc_upload_id.is_(None),
                       Transaction.not_project_expense == False)  # noqa: E712
               .all())
    doc_open = (Transaction.query
                .filter_by(project_id=project_id, source='doc_upload',
                           not_project_expense=False).all())
    _dup = {r[0] for r in db.session.query(DocUpload.id).filter(
        DocUpload.project_id == project_id,
        db.or_(DocUpload.is_duplicate == True, DocUpload.status == 'duplicate',  # noqa: E712
               DocUpload.status == 'deleted')).all()}
    _proof = {r[0] for r in db.session.query(DocUpload.id).filter(
        DocUpload.project_id == project_id,
        DocUpload.category.in_(('receipt', 'invoice'))).all()}
    taken = {t.doc_upload_id for t in Transaction.query.filter(
        pool_filter(project_id),
        Transaction.source.in_(('qbo_sync', 'csv_import')),
        Transaction.doc_upload_id.isnot(None)).all() if t.doc_upload_id}
    docmeta = {}
    _ids = [t.doc_upload_id for t in doc_open if t.doc_upload_id]
    if _ids:
        for d in DocUpload.query.filter(DocUpload.id.in_(set(_ids))).all():
            docmeta[d.id] = {"file": d.filed_filename or d.original_filename or f"Doc #{d.id}",
                             "card": d.card_last4,
                             "is_image": bool(d.content_type and d.content_type.startswith('image/'))}
    card = (str(card).strip() if card not in (None, '', 'any', 'Any', 'all') else None)

    def _pdate(s):
        try:
            return _dt.date.fromisoformat(s[:10])
        except (TypeError, ValueError):
            return None
    crows = [(c, float(c.amount), _pdate(c.txn_date)) for c in charges
             if c.amount is not None and c.txn_date]

    results = []
    for d in doc_open:
        did = d.doc_upload_id
        if (not did or did in _dup or did not in _proof or did in taken
                or d.amount is None or not d.txn_date):
            continue
        total = round(float(d.amount), 2)
        ddate = _pdate(d.txn_date)
        if not ddate:
            continue
        dcard = (docmeta.get(did, {}).get('card') or '').strip()
        if card and dcard != card:
            continue
        cands = []
        for c, camt, cdate in crows:
            if camt >= total - 0.005:        # a single ~= total is a 1:1 match, not a split
                continue
            if cdate and abs((cdate - ddate).days) > date_window:
                continue
            ccard = (getattr(c, 'card_last4', None) or '').strip()
            same_card = bool(dcard and ccard and dcard == ccard)
            if not (same_card or _vendor_similarity(d.vendor, c.vendor) >= 0.45):
                continue
            cands.append((c, camt, cdate, same_card))
        if len(cands) < 2:
            continue
        cands = cands[:12]                   # bound the subset-sum search
        found = None
        for n in range(2, max_combo + 1):
            for combo in _it.combinations(cands, n):
                if abs(round(sum(x[1] for x in combo), 2) - total) <= 0.02:
                    found = combo
                    break
            if found:
                break
        if not found:
            continue
        m = docmeta.get(did, {})
        results.append({
            "doc": {"id": did, "file": m.get("file"), "is_image": m.get("is_image", False),
                    "vendor": d.vendor, "amount": total,
                    "date": d.txn_date[:10] if d.txn_date else None, "card": dcard or None},
            "charges": [{"tid": x[0].id, "vendor": x[0].vendor, "amount": x[1],
                         "date": x[0].txn_date[:10] if x[0].txn_date else None,
                         "card": (getattr(x[0], 'card_last4', None) or None),
                         "same_card": x[3]} for x in found],
            "total": total, "n": len(found),
        })
    return {"splits": results[:100], "count": len(results)}


def confirm_match(qbo_transaction_id):
    """User confirms a suggested match. Merges the doc_upload txn into
    the qbo_sync txn — the QBO row keeps its identity (it's the bank
    record), the doc_upload row is deleted since it's now redundant."""
    q = Transaction.query.get(qbo_transaction_id)
    if not q:
        raise ValueError(f"transaction {qbo_transaction_id} not found")
    # Must have a linked receipt to confirm. We accept BOTH 'suggested' and
    # 'confirmed' states: assigning a budget line first (link_transaction_to_line)
    # flips the row to 'confirmed' before the user clicks Confirm on the receipt
    # match, and we still need to merge the receipt's sister txn in that case.
    # (User 2026-06-03 — "Confirm failed: not in 'suggested' state".)
    if not q.doc_upload_id:
        raise ValueError("no linked receipt (doc_upload_id) to confirm")
    if q.match_status not in ('suggested', 'confirmed'):
        raise ValueError(f"unexpected match_status '{q.match_status}' — cannot confirm")
    # The receipt's project owns the charge (company ledger): a pooled row
    # is claimed here; a row another project holds is refused.
    try:
        _doc_for_claim = DocUpload.query.get(q.doc_upload_id)
        if _doc_for_claim is not None and _doc_for_claim.project_id:
            claim_for(q, _doc_for_claim.project_id, why='receipt confirmed')
    except ClaimError:
        raise
    except Exception:
        log.info("[actuals/claim] confirm claim skipped for txn %s", q.id, exc_info=True)
    # Find the doc_upload Transaction that backs the same DocUpload.
    sister = (Transaction.query
              .filter_by(doc_upload_id=q.doc_upload_id, source='doc_upload')
              .first())
    if sister and sister.id != q.id:
        # Preserve everything useful from the sister (the receipt's own row)
        # before deleting it. Previously coding was promoted ONLY when q had
        # none, silently losing a different line / card / note when both rows
        # were coded. (Code review 2026-06-04.)
        if not q.budget_line_id and sister.budget_line_id:
            q.budget_line_id      = sister.budget_line_id
            q.account_code        = sister.account_code
            q.account_code_name   = sister.account_code_name
        elif (sister.budget_line_id and q.budget_line_id
              and sister.budget_line_id != q.budget_line_id):
            # Both coded to DIFFERENT lines — keep the bank row's coding but
            # record the receipt's so it isn't silently lost.
            _n = (q.note or '')
            q.note = (_n + (' | ' if _n else '') +
                      f"receipt was coded to {sister.account_code or ''} "
                      f"{sister.account_code_name or ''}".strip())[:500]
        if not q.card_last4 and sister.card_last4:
            q.card_last4 = sister.card_last4
        if not (q.note or '').strip() and sister.note:
            q.note = sister.note
        # The sister may carry ExpenseEvidence rows (stamped when the
        # receipt was coded / activated as an expense) and may be the
        # target of backup/parent links. Re-point them at the surviving
        # bank row BEFORE the delete — Postgres rejects the delete
        # otherwise (2026-10-05: every link-doc on an already-coded
        # invoice 500'd with "expense_evidence_transaction_id_fkey …
        # still referenced").
        from models import ExpenseEvidence as _EE
        _q_doc_ids = {e.doc_upload_id for e in
                      _EE.query.filter_by(transaction_id=q.id).all()}
        for _ev in _EE.query.filter_by(transaction_id=sister.id).all():
            if _ev.doc_upload_id in _q_doc_ids:
                db.session.delete(_ev)        # duplicate — drop
            else:
                _ev.transaction_id = q.id     # move onto the bank row
        Transaction.query.filter_by(backup_of_txn_id=sister.id).update(
            {Transaction.backup_of_txn_id: q.id}, synchronize_session=False)
        Transaction.query.filter_by(parent_transaction_id=sister.id).update(
            {Transaction.parent_transaction_id: q.id}, synchronize_session=False)
        # Keep the explicit this-is-an-expense stamp from the receipt row.
        if getattr(sister, 'activated_at', None) and not getattr(q, 'activated_at', None):
            q.activated_at = sister.activated_at
        # …and the receipt row's payee when the bank row has none (Phase 3).
        if getattr(sister, 'payee_id', None) and not getattr(q, 'payee_id', None):
            q.payee_id = sister.payee_id
            q.payee_source = sister.payee_source or 'doc'
        db.session.delete(sister)
    # Documents Phase 3: the confirmed document names the payee. Fills a
    # NULL payee_id (a QuickBooks EntityRef or a person's pick already on
    # the bank row stays).
    try:
        _doc = DocUpload.query.get(q.doc_upload_id)
        if _doc is not None:
            set_txn_payee(q, doc_owner_id(_doc), 'doc')
    except Exception:
        log.info("[actuals/payee] doc attribution skipped on confirm for txn %s", q.id, exc_info=True)
    # Mark reconciled (not still 'qbo_sync') so the QBO-purge tool — which
    # deletes source='qbo_sync' rows — can never wipe a confirmed match. Mirrors
    # the sync-time and manual-merge reconcile paths; qbo_txn_id is preserved so
    # a future sync still dedupes against it. (Code review 2026-06-04.)
    if q.source in ('qbo_sync', 'csv_import'):
        q.source = 'reconciled'
    q.match_status = 'confirmed'
    q.updated_at   = datetime.utcnow()
    # A2 (2026-07-20): a confirmed match makes the receipt this expense's
    # PRIMARY evidence in the unified attachment table. Fail-open.
    try:
        from models import ExpenseEvidence
        if q.doc_upload_id and not ExpenseEvidence.query.filter_by(
                transaction_id=q.id, doc_upload_id=q.doc_upload_id).first():
            db.session.add(ExpenseEvidence(transaction_id=q.id,
                                           doc_upload_id=q.doc_upload_id,
                                           kind='primary'))
    except Exception:
        pass
    db.session.commit()
    return {'transaction_id': q.id, 'merged_doc_txn': sister.id if sister else None}


def dismiss_suggestion(transaction_id, remember=True, user_id=None):
    """User rejected the auto-matcher's suggestion ("Not a match"). Clears the
    match pointers and — when remember=True — records the rejected pair so
    run_auto_match never proposes this charge↔receipt pairing again. Without
    that persistence the next auto-match run just re-suggested it."""
    t = Transaction.query.get(transaction_id)
    if not t:
        raise ValueError(f"transaction {transaction_id} not found")
    rejected_doc = t.doc_upload_id
    if remember and rejected_doc:
        exists = (MatchRejection.query
                  .filter_by(transaction_id=t.id, doc_upload_id=rejected_doc).first())
        if not exists:
            db.session.add(MatchRejection(
                project_id=t.project_id, transaction_id=t.id,
                doc_upload_id=rejected_doc, created_by=user_id))
    t.doc_upload_id    = None
    t.match_status     = 'unmatched'
    t.match_confidence = None
    t.updated_at       = datetime.utcnow()
    release(t, why='receipt suggestion dismissed')
    db.session.commit()
    return {'transaction_id': t.id, 'rejected_doc_upload_id': rejected_doc if remember else None}


def unmatch_receipt(transaction_id):
    """Reverse a match (suggested OR confirmed): unlink the receipt from this
    bank charge and put the receipt BACK in the unlinked pool so it can be
    re-matched. The bank charge itself survives (it's a real spend); only the
    receipt link is removed. If confirming had deleted the receipt's own
    doc_upload row, recreate it (ledger types only) so it reappears for
    matching. (User 2026-06-02.)"""
    t = Transaction.query.get(transaction_id)
    if not t:
        raise ValueError(f"transaction {transaction_id} not found")
    doc_id = t.doc_upload_id
    info = {"transaction_id": t.id, "unlinked_doc_id": doc_id,
            "vendor": t.vendor, "restored_receipt": False}
    if t.source == 'doc_upload':
        # This row IS the receipt's own row — "unmatch" here just clears any
        # coding/confirm, doesn't unlink itself from itself.
        t.match_status = 'unmatched'
        t.match_confidence = None
        t.updated_at = datetime.utcnow()
        db.session.commit()
        return info
    t.doc_upload_id    = None
    t.match_status     = 'unmatched'
    t.match_confidence = None
    t.updated_at       = datetime.utcnow()
    _receipt_pid = t.project_id
    if doc_id:
        doc = DocUpload.query.get(doc_id)
        if doc and is_ledger_category(doc.category):
            _receipt_pid = doc.project_id or _receipt_pid
            has_sister = (Transaction.query
                          .filter_by(doc_upload_id=doc_id, source='doc_upload')
                          .first())
            if not has_sister:
                db.session.add(Transaction(
                    project_id=_receipt_pid, source='doc_upload', doc_upload_id=doc_id,
                    vendor=doc.vendor, amount=doc.amount,
                    txn_date=doc.doc_date.isoformat() if doc.doc_date else None,
                    is_expense=True, match_status='unmatched'))
                info["restored_receipt"] = True
    # The receipt was the only claim → the charge returns to the pool.
    release(t, why='receipt unmatched')
    db.session.commit()
    return info


def last_match_for_project(project_id):
    """The most recently confirmed receipt↔charge match in a project — the
    target for 'Undo last match'. (User 2026-06-02.)"""
    return (Transaction.query
            .filter(Transaction.project_id == project_id,
                    Transaction.match_status == 'confirmed',
                    Transaction.doc_upload_id.isnot(None))
            .order_by(Transaction.updated_at.desc().nullslast(),
                      Transaction.id.desc())
            .first())


# ═══ Phase 5 — THE matching engine (ACTUALS_HANDOFF, 2026-11-03) ═══════════
# ONE rule for "does this receipt belong to this charge", used by every path:
# the auto-matcher after a sync or an upload, the candidate panel on the
# Receipts view, Find backup on a charge, and the Review-matches dialog.
# Pure scoring (score_match / rank_candidates / suggest_pairs) with the DB
# wrappers (match_candidates / refresh_match_suggestions) underneath.
#
# Confidence = 0.5 × amount + 0.3 × date + 0.2 × vendor (+ 0.1 same card):
#   amount  exact 1.0 · tip range (charge above the receipt by ≤ $5 or ≤ 20%)
#           or within $5 either way 0.7 · else no candidate
#   date    same day 1.0 · ±3 days 0.8 ("posted later") · ±7 days 0.5 · else 0
#   vendor  vendor_match_key equal 1.0 · token overlap 0.6 · else 0.2
# A pair is a SUGGESTION at ≥ 0.55. The top candidate is STRONG at ≥ 0.85
# when the runner-up trails by ≥ 0.20 (two equal candidates → none strong).
# Suggestions are written as match_status='suggested'; nothing here ever
# writes 'confirmed' — a person clicks (Steven, Nov 3: "suggest, let me
# review, click approve").

MATCH_SUGGEST_MIN   = 0.55
MATCH_STRONG_MIN    = 0.85
MATCH_STRONG_GAP    = 0.20
MATCH_LOOKBACK_DAYS = 400
MATCH_TIP_DOLLARS   = 5.0
MATCH_TIP_PCT       = 0.20
VOIDED_IN_QBO_REASON = 'voided in QuickBooks'


def _match_date(v):
    """A date from a date, a datetime or a 'YYYY-MM-DD…' string; None otherwise."""
    import datetime as _dt
    if v is None or v == '':
        return None
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    try:
        return datetime.strptime(str(v)[:10], '%Y-%m-%d').date()
    except (TypeError, ValueError):
        return None


def match_facts(obj):
    """The four facts the engine scores on, read from a Transaction, a
    DocUpload, a workspace row dict or a plain dict: amount, date (txn_date
    / doc_date / date), vendor, card (card_last4 / card)."""
    g = (obj.get if isinstance(obj, dict) else lambda k, d=None: getattr(obj, k, d))
    amt = g('amount')
    try:
        amt = float(amt) if amt is not None else None
    except (TypeError, ValueError):
        amt = None
    date = None
    for k in ('txn_date', 'doc_date', 'date'):
        date = _match_date(g(k))
        if date:
            break
    card = (g('card_last4') or g('card') or '')
    return {"amount": amt, "date": date, "vendor": (g('vendor') or ''),
            "card": str(card).strip()}


def _vendor_tokens(key):
    return {t for t in (key or '').split() if len(t) >= 3}


def vendor_score(a, b):
    """1.0 same vendor_match_key · 0.6 share a word (or one contains the
    other) · 0.2 otherwise (unknown vendors score 0.2 too, never 0)."""
    ka, kb = vendor_match_key(a), vendor_match_key(b)
    if not ka or not kb:
        return 0.2, 'vendor unknown'
    if ka == kb:
        return 1.0, 'same vendor'
    if ka in kb or kb in ka or (_vendor_tokens(ka) & _vendor_tokens(kb)):
        return 0.6, 'vendor words overlap'
    return 0.2, 'different vendor'


def score_match(charge, receipt):
    """Score one charge ↔ receipt pair. Pure. Returns None when the amounts
    rule the pair out (or either amount is missing); else
      {"confidence", "amount_score", "date_score", "vendor_score",
       "card_bonus", "day_gap", "amount_delta", "reasons": [...]}"""
    c, r = match_facts(charge), match_facts(receipt)
    if c["amount"] is None or r["amount"] is None:
        return None
    ca, ra = abs(c["amount"]), abs(r["amount"])
    if ca == 0 or ra == 0:
        return None
    reasons = []
    delta = ca - ra
    if abs(delta) <= 0.01:
        a_s = 1.0
        reasons.append('exact amount')
    elif 0 < delta <= max(MATCH_TIP_DOLLARS, MATCH_TIP_PCT * ra):
        a_s = 0.7
        reasons.append('tip?')
    elif abs(delta) <= MATCH_TIP_DOLLARS:
        a_s = 0.7
        reasons.append('within $5')
    else:
        return None
    if (c["amount"] < 0) != (r["amount"] < 0):
        reasons.append('refund?')
    gap = None
    if c["date"] and r["date"]:
        gap = (c["date"] - r["date"]).days
        ag = abs(gap)
        if ag == 0:
            d_s = 1.0
            reasons.append('same day')
        elif ag <= 3:
            d_s = 0.8
            reasons.append('posted later' if gap > 0 else 'posted earlier')
        elif ag <= 7:
            d_s = 0.5
            reasons.append(f'{ag} days apart')
        else:
            d_s = 0.0
            reasons.append(f'{ag} days apart')
    else:
        d_s = 0.0
        reasons.append('no date')
    v_s, v_why = vendor_score(c["vendor"], r["vendor"])
    reasons.append(v_why)
    bonus = 0.0
    if c["card"] and r["card"] and c["card"] == r["card"]:
        bonus = 0.1
        reasons.append('same card')
    conf = min(1.0, 0.5 * a_s + 0.3 * d_s + 0.2 * v_s + bonus)
    return {"confidence": round(conf, 3), "amount_score": a_s, "date_score": d_s,
            "vendor_score": v_s, "card_bonus": bonus, "day_gap": gap,
            "amount_delta": round(delta, 2), "reasons": reasons}


def rank_candidates(target, pool, *, key=None, rejected=frozenset(), limit=None):
    """Every item of `pool` that is a suggestion for `target`, best first:
    [{"item", "key", "strong", **score}]. `key(item)` identifies an item
    for the rejected set (pairs the person already said are not a match).
    Only the first entry can be strong (≥ MATCH_STRONG_MIN with a runner-up
    gap ≥ MATCH_STRONG_GAP, or no runner-up at all). Pure."""
    key = key or (lambda x: getattr(x, 'id', None) if not isinstance(x, dict) else x.get('id'))
    out = []
    for item in pool:
        k = key(item)
        if k in rejected:
            continue
        s = score_match(target, item) if not _is_receipt_like(target) else score_match(item, target)
        if not s or s["confidence"] < MATCH_SUGGEST_MIN:
            continue
        out.append({"item": item, "key": k, "strong": False, **s})
    out.sort(key=lambda x: (-x["confidence"], abs(x["amount_delta"]), abs(x["day_gap"] or 0)))
    if out and out[0]["confidence"] >= MATCH_STRONG_MIN:
        if len(out) == 1 or out[0]["confidence"] - out[1]["confidence"] >= MATCH_STRONG_GAP:
            out[0]["strong"] = True
    return out[:limit] if limit else out


def _is_receipt_like(obj):
    """A DocUpload / receipt row (has doc_date or a doc_id) as opposed to a
    charge — only matters for which side of score_match it lands on
    (the tip rule is directional: the charge may exceed the receipt)."""
    g = (obj.get if isinstance(obj, dict) else lambda k, d=None: getattr(obj, k, d))
    if g('doc_date', None) is not None or g('doc_id', None) is not None:
        return True
    if g('txn_date', None) is not None:
        return False
    return bool(g('is_receipt', False))


def suggest_pairs(charges, receipts, *, rejected=frozenset(), charge_key=None, doc_key=None):
    """Greedy one-to-one pairing of open charges with open receipts: every
    scored pair ≥ MATCH_SUGGEST_MIN, best first, each side used once.
    `rejected` holds (charge_key, doc_key) pairs. Returns
    [(charge, receipt, score_with_strong)]. Pure; O(charges × receipts)."""
    charge_key = charge_key or (lambda t: getattr(t, 'id', None))
    doc_key = doc_key or (lambda d: getattr(d, 'id', None))
    scored = []
    per_charge = {}
    for ch in charges:
        ck = charge_key(ch)
        ranked = rank_candidates(ch, receipts, key=doc_key,
                                 rejected={dk for (cck, dk) in rejected if cck == ck})
        per_charge[ck] = ranked
        for r in ranked:
            scored.append((r["confidence"], ck, r["key"], ch, r["item"], r))
    scored.sort(key=lambda x: (-x[0], abs(x[5]["amount_delta"]), abs(x[5]["day_gap"] or 0)))
    used_c, used_d, out = set(), set(), []
    for conf, ck, dk, ch, doc, s in scored:
        if ck in used_c or dk in used_d:
            continue
        used_c.add(ck); used_d.add(dk)
        out.append((ch, doc, dict(s)))
    return out


# ── DB wrappers ───────────────────────────────────────────────────────────

def _match_cutoff(days=MATCH_LOOKBACK_DAYS, today=None):
    import datetime as _dt
    return ((today or _dt.date.today()) - _dt.timedelta(days=days)).isoformat()


def open_charges(project_id, days=MATCH_LOOKBACK_DAYS, include_suggested=True):
    """Charges that want backup: electronic or hand-typed rows in this
    project's POOL (pool_filter: its own rows + the unclaimed company
    QuickBooks rows) with no receipt linked (doc_upload_id NULL, no evidence row),
    not excluded, not a backup / split child, dated within `days`. With
    include_suggested the heuristic 'suggested' rows come back too (their
    suggestion is recomputed); AI-made suggestions are left alone."""
    from models import ExpenseEvidence as _EE
    cutoff = _match_cutoff(days)
    q = (Transaction.query
         .filter(pool_filter(project_id),
                 Transaction.not_project_expense == False,            # noqa: E712
                 Transaction.backup_of_txn_id.is_(None),
                 db.or_(Transaction.source.is_(None),
                        Transaction.source.notin_(('doc_upload', 'invoice_split'))),
                 db.or_(Transaction.txn_date.is_(None), Transaction.txn_date >= cutoff)))
    rows = q.all()
    with_evidence = {r[0] for r in db.session.query(_EE.transaction_id)
                     .filter(_EE.transaction_id.in_([t.id for t in rows] or [-1])).all()}
    out = []
    for t in rows:
        if t.id in with_evidence:
            continue
        if t.doc_upload_id:
            if not include_suggested or t.match_status != 'suggested' or (t.match_source or '') == 'ai':
                continue
        if (t.match_status or '') == 'confirmed' and t.doc_upload_id:
            continue
        out.append(t)
    return out


def open_receipts(project_id, days=MATCH_LOOKBACK_DAYS):
    """Receipts / invoices in the inbox: not a duplicate, not trashed, with
    an amount, not linked to any charge (no bank row points at them, no
    evidence row), not turned into an expense (activated) and not excluded.
    Dated within `days` (undated ones stay — they just score 0 on date)."""
    from models import ExpenseEvidence as _EE
    import datetime as _dt
    cutoff = _dt.date.today() - _dt.timedelta(days=days)
    docs = (DocUpload.query
            .filter(DocUpload.project_id == project_id,
                    DocUpload.category.in_(('receipt', 'invoice')),
                    DocUpload.amount.isnot(None),
                    DocUpload.is_duplicate == False,                # noqa: E712
                    db.or_(DocUpload.status.is_(None),
                           DocUpload.status.notin_(('duplicate', 'deleted', 'error'))))
            .all())
    if not docs:
        return []
    ids = [d.id for d in docs]
    linked = {r[0] for r in db.session.query(Transaction.doc_upload_id)
              .filter(Transaction.doc_upload_id.in_(ids),
                      Transaction.source != 'doc_upload',
                      db.or_(Transaction.match_status == 'confirmed',
                             Transaction.match_source == 'ai')).all()}
    linked |= {r[0] for r in db.session.query(_EE.doc_upload_id)
               .filter(_EE.doc_upload_id.in_(ids)).all()}
    own = {}
    for t in Transaction.query.filter(Transaction.doc_upload_id.in_(ids),
                                      Transaction.source == 'doc_upload').all():
        own.setdefault(t.doc_upload_id, t)
    out = []
    for d in docs:
        if d.id in linked:
            continue
        o = own.get(d.id)
        if o is not None and (o.activated_at is not None or o.not_project_expense):
            continue
        if d.doc_date and d.doc_date < cutoff:
            continue
        out.append(d)
    return out


def _rejected_pairs(project_id):
    return {(r.transaction_id, r.doc_upload_id) for r in
            MatchRejection.query.filter_by(project_id=project_id).all()}


def match_candidates(project_id, txn=None, doc=None, limit=8, days=MATCH_LOOKBACK_DAYS):
    """THE candidate list for one side. txn → ranked open receipts (incl.
    unfiled inbox ones); doc → ranked open charges. Each entry is
    rank_candidates' shape ({"item", "key", "strong", "confidence",
    "reasons", ...}). Read-only."""
    rejected = _rejected_pairs(project_id)
    if txn is not None:
        pool = open_receipts(project_id, days)
        return rank_candidates(txn, pool, key=lambda d: d.id,
                               rejected={dk for (tk, dk) in rejected if tk == txn.id},
                               limit=limit)
    if doc is not None:
        pool = open_charges(project_id, days)
        return rank_candidates(doc, pool, key=lambda t: t.id,
                               rejected={tk for (tk, dk) in rejected if dk == doc.id},
                               limit=limit)
    return []


def refresh_match_suggestions(project_id, days=MATCH_LOOKBACK_DAYS):
    """Recompute every heuristic receipt↔charge suggestion in a project
    (bounded to the last `days`): every unmatched receipt against every
    charge without backup, greedy best-first, one receipt per charge.
    Writes match_status='suggested' + match_confidence + doc_upload_id +
    match_source='heuristic' on the charge — NEVER 'confirmed'. Stale
    heuristic suggestions that no longer rank are cleared; AI-made and
    confirmed links are untouched. Company ledger: a suggestion claims a
    pooled charge for this project (claim_for); clearing a stale one
    releases it again when nothing else holds it (release). Commits.
    Returns counts."""
    try:
        ensure_doc_txns(project_id)
    except Exception as _ee:
        log.warning(f"[match] ensure_doc_txns failed: {_ee}")
    charges = open_charges(project_id, days, include_suggested=True)
    receipts = open_receipts(project_id, days)
    rejected = _rejected_pairs(project_id)
    pairs = suggest_pairs(charges, receipts, rejected=rejected,
                          charge_key=lambda t: t.id, doc_key=lambda d: d.id)
    now = datetime.utcnow()
    paired = {}
    for ch, doc, s in pairs:
        paired[ch.id] = (doc.id, s["confidence"])
    written = cleared = 0
    for t in charges:
        hit = paired.get(t.id)
        if hit:
            doc_id, conf = hit
            if (t.doc_upload_id != doc_id or t.match_status != 'suggested'
                    or float(t.match_confidence or 0) != conf):
                t.doc_upload_id = doc_id
                t.match_status = 'suggested'
                t.match_confidence = conf
                t.match_source = 'heuristic'
                t.updated_at = now
            claim_for(t, project_id, why='receipt suggested')
            written += 1
        elif t.doc_upload_id and t.match_status == 'suggested' and (t.match_source or '') != 'ai':
            t.doc_upload_id = None
            t.match_status = 'unmatched'
            t.match_confidence = None
            t.match_source = None
            t.updated_at = now
            release(t, why='stale receipt suggestion cleared')
            cleared += 1
    db.session.commit()
    log.info(f"[match] project #{project_id}: {written} suggestions "
             f"({cleared} stale cleared) from {len(charges)} charges × {len(receipts)} receipts")
    return {'suggestions': written, 'cleared': cleared,
            'inspected': len(charges) * len(receipts),
            'qbo_unmatched': len(charges), 'doc_open': len(receipts),
            'suggested': written, 'scanned': len(charges)}


# ═══ Phase 5 — duplicates: one rule for "these two charges are one event" ══

DUP_WINDOW_QBO_DAYS = 3


def duplicate_day_window(a, b):
    """3 days when both rows are QuickBooks records (the same event can post
    twice a day apart — Ryan Mauskopf's two BillPayments, Oct 19 / Oct 20),
    0 days (same day only) for CSV / manual rows."""
    if getattr(a, 'qbo_txn_id', None) and getattr(b, 'qbo_txn_id', None):
        return DUP_WINDOW_QBO_DAYS
    return 0


def is_duplicate_twin(candidate, other, day_window=None):
    """True when `other` is an existing twin of `candidate` — same |amount|,
    dates within `day_window` (default duplicate_day_window: 3 days for two
    QuickBooks rows, same day otherwise), same vendor (vendor_match_key) or
    same card, a different id and a DIFFERENT QuickBooks id (one QuickBooks
    id is one row company-wide since the ledger; two flagged rows with the
    same id are a claim conflict, not a duplicate). When both rows carry a
    QuickBooks payee (qbo_entity_ref — the EntityRef / VendorRef) that
    payee decides: different payees are NEVER twins, whatever the vendor
    text or card says (Joel Knutson vs Ryan Mauskopf, $4,500 a day apart,
    where a receipt had renamed one row); the same payee counts as the
    same vendor. Deleting a 'duplicate' bank row is only allowed when such
    a twin survives; without one the charge would simply vanish from the
    ledger."""
    if other is None or candidate is None:
        return False
    if getattr(other, 'id', None) == getattr(candidate, 'id', None):
        return False
    try:
        if _cents(getattr(candidate, 'amount', None)) == 0:
            return False
        if abs(_cents(getattr(candidate, 'amount', None))) != abs(_cents(getattr(other, 'amount', None))):
            return False
    except ValueError:
        return False
    q1, q2 = getattr(candidate, 'qbo_txn_id', None), getattr(other, 'qbo_txn_id', None)
    if q1 and q2 and str(q1) == str(q2):
        return False
    if day_window is None:
        day_window = duplicate_day_window(candidate, other)
    d1 = _match_date(getattr(candidate, 'txn_date', None))
    d2 = _match_date(getattr(other, 'txn_date', None))
    if d1 is None or d2 is None:
        if (getattr(candidate, 'txn_date', None) or '')[:10] != (getattr(other, 'txn_date', None) or '')[:10]:
            return False
    elif abs((d1 - d2).days) > day_window:
        return False
    e1 = str(getattr(candidate, 'qbo_entity_ref', None) or '').strip()
    e2 = str(getattr(other, 'qbo_entity_ref', None) or '').strip()
    if e1 and e2:
        return e1 == e2
    v1 = vendor_match_key(getattr(candidate, 'vendor', None))
    v2 = vendor_match_key(getattr(other, 'vendor', None))
    c1 = (getattr(candidate, 'card_last4', None) or '').strip()
    c2 = (getattr(other, 'card_last4', None) or '').strip()
    if v1 and v1 == v2:
        return True
    if c1 and c1 == c2:
        return True
    return False


def dup_group_key(members):
    """Stable key for a duplicate group: the sorted member ids. Remembered
    in transaction_dup_dismissal when the person says Keep both."""
    return 'dup:' + '-'.join(str(i) for i in sorted(int(getattr(m, 'id')) for m in members))


def txn_has_confirmed_receipt(t):
    return bool(getattr(t, 'doc_upload_id', None)) and (getattr(t, 'match_status', None) == 'confirmed')


def txn_is_coded(t):
    return bool(getattr(t, 'budget_line_id', None) or getattr(t, 'has_splits', False)
                or getattr(t, 'account_code', None))


def pick_keeper(members):
    """The row to KEEP from a duplicate group, in order: the one with a
    receipt confirmed; the one coded to a line; the earlier date; the lower
    id. Returns (keeper, reason). Pure."""
    def _k(t):
        return (0 if txn_has_confirmed_receipt(t) else 1,
                0 if txn_is_coded(t) else 1,
                (getattr(t, 'txn_date', None) or '9999-99-99')[:10],
                int(getattr(t, 'id', 0) or 0))
    keeper = min(members, key=_k)
    if txn_has_confirmed_receipt(keeper):
        reason = 'has the receipt'
    elif txn_is_coded(keeper):
        reason = 'coded to a line'
    elif any((getattr(m, 'txn_date', None) or '')[:10] != (getattr(keeper, 'txn_date', None) or '')[:10]
             for m in members):
        reason = 'earlier date'
    else:
        reason = 'first imported'
    return keeper, reason


def find_duplicate_groups(rows, dismissed=()):
    """Groups of pool rows that look like one event (is_duplicate_twin,
    pairwise, union-find). Rows that are excluded, receipt-born, split
    children or without an amount are ignored. Groups whose key is in
    `dismissed` (Keep both) are dropped. Returns
    [{"key", "members": [rows], "keeper": row, "keeper_reason"}], largest
    amount first. Pure; O(n²) inside each |amount| bucket."""
    pool = []
    for t in rows:
        if getattr(t, 'not_project_expense', False):
            continue
        if getattr(t, 'source', None) in ('doc_upload', 'invoice_split'):
            continue
        try:
            if _cents(getattr(t, 'amount', None)) == 0:
                continue
        except ValueError:
            continue
        pool.append(t)
    buckets = {}
    for t in pool:
        buckets.setdefault(abs(_cents(t.amount)), []).append(t)
    parent, linked = {}, set()

    def _find(i):
        while parent.get(i, i) != i:
            i = parent[i]
        return i

    def _union(a, b):
        ra, rb = _find(a), _find(b)
        if ra != rb:
            parent[rb] = ra
        linked.update((a, b))
    for members in buckets.values():
        if len(members) < 2:
            continue
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                if is_duplicate_twin(members[i], members[j]):
                    _union(members[i].id, members[j].id)
    groups = {}
    for t in pool:
        if t.id in linked:
            groups.setdefault(_find(t.id), []).append(t)
    out = []
    dismissed = set(dismissed or ())
    for members in groups.values():
        if len(members) < 2:
            continue
        key = dup_group_key(members)
        if key in dismissed:
            continue
        keeper, why = pick_keeper(members)
        members = sorted(members, key=lambda m: (m.id != keeper.id, (m.txn_date or ''), m.id))
        out.append({"key": key, "members": members, "keeper": keeper, "keeper_reason": why})
    out.sort(key=lambda g: (-abs(float(g["members"][0].amount or 0)), g["key"]))
    return out


def duplicate_delete_mode(extra):
    """How 'Delete the extra' treats a row: 'delete' (hard delete — a
    qbo_sync / csv_import row with no coding and no receipt) or 'merge'
    (move its coding, splits, receipt and evidence onto the keeper first,
    then delete). Pure."""
    bare = (not txn_is_coded(extra) and not getattr(extra, 'doc_upload_id', None)
            and not getattr(extra, 'has_evidence', False))
    if bare and getattr(extra, 'source', None) in ('qbo_sync', 'csv_import'):
        return 'delete'
    return 'merge'


MERGE_FIELDS = ('budget_line_id', 'account_code', 'account_code_name',
                'doc_upload_id', 'match_status', 'match_confidence', 'match_source',
                'no_charge_reason', 'payment_method', 'paid_by_crew_member_id',
                'payee_id', 'payee_source', 'activated_at', 'card_last4', 'note')


def merge_duplicate_into(keeper, extra, splits=(), evidence=()):
    """Move everything the extra row carries onto the keeper before the
    extra is deleted: coding (line / section) when the keeper has none,
    the receipt link + match state when the keeper has none, the
    missing-backup reason, payment method / payer / payee, the
    activated stamp, the card and the note (appended), every
    TransactionSplit and ExpenseEvidence row (re-pointed to the keeper).
    Pure in the sense of CLAUDE.md: it sets attributes on the objects it is
    given and touches no session. Returns {"moved": [field names],
    "splits": n, "evidence": n}."""
    moved = []
    keeper_uncoded = (not getattr(keeper, 'budget_line_id', None)
                      and not getattr(keeper, 'account_code', None)
                      and not getattr(keeper, 'has_splits', False))
    if keeper_uncoded:
        for f in ('budget_line_id', 'account_code', 'account_code_name'):
            if getattr(extra, f, None) and not getattr(keeper, f, None):
                setattr(keeper, f, getattr(extra, f))
                moved.append(f)
    if getattr(extra, 'doc_upload_id', None) and not getattr(keeper, 'doc_upload_id', None):
        keeper.doc_upload_id = extra.doc_upload_id
        keeper.match_status = getattr(extra, 'match_status', None) or 'confirmed'
        keeper.match_confidence = getattr(extra, 'match_confidence', None)
        keeper.match_source = getattr(extra, 'match_source', None)
        moved += ['doc_upload_id', 'match_status']
    for f in ('no_charge_reason', 'payment_method', 'paid_by_crew_member_id',
              'payee_id', 'payee_source', 'activated_at', 'card_last4'):
        if getattr(extra, f, None) and not getattr(keeper, f, None):
            setattr(keeper, f, getattr(extra, f))
            moved.append(f)
    xn = (getattr(extra, 'note', None) or '').strip()
    kn = (getattr(keeper, 'note', None) or '').strip()
    if xn and xn not in kn:
        keeper.note = ((kn + ' | ' if kn else '') + xn)[:500]
        moved.append('note')
    n_s = 0
    for sp in splits or ():
        sp.transaction_id = keeper.id
        n_s += 1
    n_e = 0
    for ev in evidence or ():
        ev.transaction_id = keeper.id
        n_e += 1
    if n_s and getattr(keeper, 'budget_line_id', None) and 'budget_line_id' in moved:
        # The extra's coding was its splits: a split parent has no line.
        keeper.budget_line_id = None
    return {"moved": moved, "splits": n_s, "evidence": n_e}


TXN_SNAPSHOT_FIELDS = ('project_id', 'source', 'vendor', 'amount', 'is_expense', 'txn_date',
                       'card_last4', 'note', 'account_code', 'account_code_name',
                       'budget_line_id', 'doc_upload_id', 'match_status', 'match_confidence',
                       'match_source', 'qbo_txn_id', 'qbo_txn_type', 'qbo_account_id',
                       'qbo_category', 'qbo_entity_ref', 'not_project_expense',
                       'no_charge_reason', 'payment_method', 'paid_by_crew_member_id',
                       'payee_id', 'payee_source', 'claim_conflict', 'claim_conflict_reason',
                       'suggested_budget_line_id', 'suggested_account_code',
                       'excluded_reason', 'excluded_by_user_id', 'parent_transaction_id',
                       'backup_of_txn_id', 'split_group', 'activated_at', 'excluded_at')


def txn_snapshot(t):
    """Every column a deleted transaction needs to come back through Undo
    (JSON-safe). Pure."""
    import datetime as _dt
    from decimal import Decimal as _D
    out = {}
    for f in TXN_SNAPSHOT_FIELDS:
        v = getattr(t, f, None)
        if isinstance(v, _D):
            v = float(v)
        elif isinstance(v, (_dt.datetime, _dt.date)):
            v = v.isoformat()
        out[f] = v
    return out


def qbo_deletion_action(txn):
    """What a QuickBooks deletion / void does to the FPBudget row: 'delete'
    when the row is uncoded with no receipt (nothing of the person's work
    is on it), 'void' otherwise (keep it, excluded, flagged 'Voided in
    QuickBooks', for them to decide). Pure."""
    if txn_is_coded(txn) or getattr(txn, 'doc_upload_id', None) or getattr(txn, 'has_evidence', False):
        return 'void'
    return 'delete'


THRESHOLD_FALLBACK_PRE_2026 = 600.0
THRESHOLD_FALLBACK_2026_ON  = 2000.0

CARD_PAYMENT_METHODS = frozenset({'company_card', 'personal_card'})

PAYEE_KINDS = ('person', 'loan_out', 'vendor')

W9_ON_FILE        = 'on_file'
W9_MISSING        = 'missing'
W9_NOT_REQUIRED   = 'not_required'    # W-2 employee: Wrapbook, not 1099
W9_UNDER_THRESHOLD = 'under_threshold'

# A payee-year is "shared" when a budget line it was paid through carries
# more than one crew member: the money is real but belongs to nobody in
# particular until Transaction.payee_id (Phase 3) says who.
SHARED_LINE = 'shared_line'


def threshold_for_year(year, rows=None):
    """The reporting floor for a calendar year. `rows` is {year: amount}
    (from reporting_threshold); a missing year falls back to the IRS rule:
    600 before 2026, 2000 from 2026 on."""
    try:
        y = int(year)
    except (TypeError, ValueError):
        y = None
    if rows and y is not None and y in rows and rows[y] is not None:
        try:
            return float(rows[y])
        except (TypeError, ValueError):
            pass
    if y is None:
        return THRESHOLD_FALLBACK_2026_ON
    return THRESHOLD_FALLBACK_PRE_2026 if y < 2026 else THRESHOLD_FALLBACK_2026_ON


def reporting_threshold(year):
    """DB wrapper: threshold_for_year() over the reporting_threshold table."""
    from models import ReportingThreshold
    rows = {}
    try:
        for r in ReportingThreshold.query.all():
            rows[int(r.tax_year)] = float(r.amount)
    except Exception as _e:   # table missing on a never-migrated DB → fallback
        log.warning("[payees] reporting_threshold unavailable: %s", _e)
    return threshold_for_year(year, rows)


def reporting_threshold_rows():
    """{year: amount} for every seeded year (directory year picker)."""
    from models import ReportingThreshold
    try:
        return {int(r.tax_year): float(r.amount) for r in ReportingThreshold.query.all()}
    except Exception:
        return {}


def is_card_payment(txn):
    """Card vs non-card for 1099 purposes: card payments are reported by the
    card network on a 1099-K, so only NON-card money counts toward the
    1099-NEC floor. A row is card when payment_method is company_card or
    personal_card, or when payment_method is unset and the row carries a
    card_last4 (a synced card charge)."""
    pm = getattr(txn, 'payment_method', None)
    if pm:
        return pm in CARD_PAYMENT_METHODS
    return bool(getattr(txn, 'card_last4', None))


def txn_year(txn):
    """Calendar year of txn_date (YYYY-MM-DD string) or None when undated."""
    d = getattr(txn, 'txn_date', None) or ''
    try:
        return int(str(d)[:4])
    except (TypeError, ValueError):
        return None


def attribute_payee(txn, doc_owner_by_id, line_owners_by_id):
    """Who a transaction was paid to when Transaction.payee_id is NULL —
    the read-time FALLBACK (Documents Phase 3 writes payee_id at the
    moment the fact is known; paid_by_payee() prefers it and calls this
    only for rows nothing has attributed yet):

      1. its document's owner  (doc_upload_id → DocUpload owner)
      2. else its budget line  (budget_line_id → the ONE crew member on the
         line via CrewAssignment.crew_member_id / BudgetLine.assigned_crew_id)

    `doc_owner_by_id`   {doc_upload_id: crew_member_id}
    `line_owners_by_id` {budget_line_id: set(crew_member_id)}

    Returns a crew_member_id, SHARED_LINE when the line carries more than
    one person (the money is never split or double-counted — it lands in
    the shared bucket), or None when nothing names a payee."""
    did = getattr(txn, 'doc_upload_id', None)
    if did and doc_owner_by_id.get(did):
        return doc_owner_by_id[did]
    lid = getattr(txn, 'budget_line_id', None)
    if lid:
        owners = line_owners_by_id.get(lid) or set()
        if len(owners) == 1:
            return next(iter(owners))
        if len(owners) > 1:
            return SHARED_LINE
    return None


def aggregate_paid(rows):
    """Fold (payee, year, amount, is_card) rows into
    {payee: {year: {card, non_card, total, n}}}. `payee` may be SHARED_LINE;
    `year` may be None (undated)."""
    out = {}
    for payee, year, amount, card in rows:
        if payee is None:
            continue
        bucket = out.setdefault(payee, {}).setdefault(
            year, {"card": 0.0, "non_card": 0.0, "total": 0.0, "n": 0})
        amt = float(amount or 0)
        bucket["card" if card else "non_card"] += amt
        bucket["total"] += amt
        bucket["n"] += 1
    for payee in out.values():
        for b in payee.values():
            for k in ("card", "non_card", "total"):
                b[k] = round(b[k], 2)
    return out


def paid_by_payee(payee_id=None, year=None, by='year'):
    """Paid per (payee, calendar year), split card / non-card — the ONE
    reporting query behind the Payee directory, the Payee page, the CSV
    and the Wrap Book 1099 check.

    Rows: every Transaction that counts_as_spend() (the Phase 0 rule: not
    excluded, activated if doc-born, not a backup, not a split container),
    signed (credits subtract), attributed by attribute_payee(): the
    document's owner first, else the single crew member on the budget line.
    A line with more than one crew member attributes to nobody and is
    counted once under the SHARED_LINE key, never double-counted.
    transaction_split pieces attribute through THEIR line (the parent is a
    container and is skipped by counts_as_spend).

    Returns {payee_id_or_SHARED_LINE: {year: {card, non_card, total, n}}}
    filtered to `payee_id` / `year` when given (year None = every year;
    undated rows sit under year None). With by='project' the inner key is
    the transaction's project_id instead of the year (the Payee page's
    Projects table) — same rows, same attribution.

    Documents Phase 3: a row with Transaction.payee_id set attributes to
    THAT payee, full stop (it was written when the fact was known — the
    QuickBooks EntityRef, the coded line, the document's owner, an alias
    or a person). The doc-owner / line inference above runs only for rows
    whose payee_id is still NULL, so nothing that counted before
    disappears; a split piece of a row with payee_id follows the row."""
    from sqlalchemy import or_, and_, func as _f
    from models import TransactionSplit

    # Attribution maps (two bulk queries + one for splits) — the fallback
    # for rows with no payee_id. The doc owner is owner_payee_id (Phase 2)
    # with the alias column crew_member_id for rows the data step missed.
    doc_owner = {did: cid for did, cid in
                 db.session.query(DocUpload.id,
                                  _f.coalesce(DocUpload.owner_payee_id, DocUpload.crew_member_id))
                 .filter(or_(DocUpload.owner_payee_id.isnot(None),
                             DocUpload.crew_member_id.isnot(None))).all()}
    line_owners = {}
    for lid, cid in (db.session.query(CrewAssignment.budget_line_id,
                                      CrewAssignment.crew_member_id)
                     .filter(CrewAssignment.crew_member_id.isnot(None)).all()):
        line_owners.setdefault(lid, set()).add(cid)
    for lid, cid in (db.session.query(BudgetLine.id, BudgetLine.assigned_crew_id)
                     .filter(BudgetLine.assigned_crew_id.isnot(None)).all()):
        line_owners.setdefault(lid, set()).add(cid)

    q = Transaction.query.filter(counts_as_spend())
    if year is not None:
        q = q.filter(Transaction.txn_date.like(f"{int(year)}-%"))
    if payee_id is not None:
        # Narrow to rows that CAN name this payee (an explicit payee_id,
        # their doc or their line); _who() still decides.
        doc_ids = [d for d, c in doc_owner.items() if c == payee_id]
        line_ids = [l for l, s in line_owners.items() if payee_id in s]
        conds = [Transaction.payee_id == payee_id]
        if doc_ids:
            conds.append(and_(Transaction.payee_id.is_(None),
                              Transaction.doc_upload_id.in_(doc_ids)))
        if line_ids:
            conds.append(and_(Transaction.payee_id.is_(None),
                              Transaction.budget_line_id.in_(line_ids)))
        q = q.filter(or_(*conds))

    def _who(t):
        pid_ = getattr(t, 'payee_id', None)
        if pid_:
            return pid_
        return attribute_payee(t, doc_owner, line_owners)

    _key = (lambda t: t.project_id) if by == 'project' else txn_year
    rows = []
    for t in q.all():
        who = _who(t)
        if who is None:
            continue
        rows.append((who, _key(t),
                     signed_amount_value(t.amount, t.is_expense), is_card_payment(t)))

    # Split pieces (parents are excluded by counts_as_spend).
    try:
        sq = (db.session.query(TransactionSplit, Transaction)
              .join(Transaction, Transaction.id == TransactionSplit.transaction_id)
              .filter(Transaction.not_project_expense == False,       # noqa: E712
                      Transaction.backup_of_txn_id.is_(None)))
        if year is not None:
            sq = sq.filter(Transaction.txn_date.like(f"{int(year)}-%"))
        for sp, t in sq.all():
            owners = line_owners.get(sp.budget_line_id) or set()
            who = getattr(t, 'payee_id', None) or (
                next(iter(owners)) if len(owners) == 1
                else (SHARED_LINE if owners else None))
            if who is None or (payee_id is not None and who != payee_id):
                continue
            rows.append((who, _key(t),
                         signed_amount_value(sp.amount, t.is_expense), is_card_payment(t)))
    except Exception as _e:
        log.warning("[payees] split attribution skipped: %s", _e)

    out = aggregate_paid(rows)
    if payee_id is not None:
        return {payee_id: out.get(payee_id, {})} if payee_id in out else {}
    return out


def w9_status_for(payee, non_card=None, threshold=None, fallback_date=None):
    """The ONE "W-9 on file" rule.

      not_required     is_w2_employee (Wrapbook payroll, no 1099)
      on_file          w9_received_on is set (Phase 1 truth) — or, until
                       Phase 2 writes it on upload, `fallback_date` from a
                       tax_form document that names a W-9 / W-8
      under_threshold  no W-9, and this year's non-card total is below the
                       floor (only when non_card and threshold are given)
      missing          no W-9 and the year is (or may be) reportable

    Returns {"status", "date" (ISO or None), "source" ('payee'|'doc'|None)}."""
    if getattr(payee, 'is_w2_employee', False):
        return {"status": W9_NOT_REQUIRED, "date": None, "source": None}
    d = getattr(payee, 'w9_received_on', None)
    if d:
        return {"status": W9_ON_FILE,
                "date": d.isoformat() if hasattr(d, 'isoformat') else str(d),
                "source": 'payee'}
    if fallback_date:
        return {"status": W9_ON_FILE,
                "date": (fallback_date.isoformat() if hasattr(fallback_date, 'isoformat')
                         else str(fallback_date)),
                "source": 'doc'}
    if non_card is not None and threshold is not None and float(non_card) < float(threshold):
        return {"status": W9_UNDER_THRESHOLD, "date": None, "source": None}
    return {"status": W9_MISSING, "date": None, "source": None}


def w9_fallback_dates(payee_ids=None):
    """{crew_member_id: date} from tax_form documents: the Phase 2 signal
    first (tax_form_kind in W9_KINDS, received_on), then the pre-Phase-2
    signal (sub-type / OCR vendor / filename names a W-9 / W-8) for rows
    the 0023 data step has not classified. ONE query. Newest wins."""
    q = (db.session.query(DocUpload.owner_payee_id, DocUpload.crew_member_id,
                          DocUpload.tax_form_kind, DocUpload.received_on,
                          DocUpload.doc_date, DocUpload.uploaded_at,
                          DocUpload.doc_subtype, DocUpload.vendor,
                          DocUpload.filed_filename, DocUpload.original_filename)
         .filter(db.or_(DocUpload.owner_payee_id.isnot(None),
                        DocUpload.crew_member_id.isnot(None)),
                 DocUpload.category == 'tax_form',
                 DocUpload.is_duplicate == False,                  # noqa: E712 — pending dup ≠ on file
                 DocUpload.status.notin_(('deleted', 'duplicate'))))
    if payee_ids is not None:
        ids = list(payee_ids)
        if not ids:
            return {}
        q = q.filter(db.or_(DocUpload.owner_payee_id.in_(ids),
                            DocUpload.crew_member_id.in_(ids)))
    out = {}
    for (owner, alias, kind, received_on, doc_date, uploaded_at,
         sub, vendor, f1, f2) in q.all():
        cid = owner or alias
        k = (kind or '').lower()
        if not k:
            k = tax_form_kind_from_text(" ".join(str(x or '') for x in (sub, vendor, f1, f2))) or ''
        if k not in W9_KINDS:
            continue
        d = received_on or doc_date or (uploaded_at.date() if uploaded_at else None)
        if d and (cid not in out or d > out[cid]):
            out[cid] = d
    return out


def payee_w9_status(payee, year=None, non_card=None, threshold=None):
    """DB wrapper for w9_status_for(): resolves the document fallback for
    one payee and the year's threshold when not supplied."""
    fb = None
    if not getattr(payee, 'w9_received_on', None) and not getattr(payee, 'is_w2_employee', False):
        fb = w9_fallback_dates([payee.id]).get(payee.id)
    if non_card is not None and threshold is None:
        threshold = reporting_threshold(year)
    return w9_status_for(payee, non_card=non_card, threshold=threshold, fallback_date=fb)


def flag_1099(non_card, threshold, w9_status, qbo_vendor_id=None,
              qbo_vendor_1099=None, card=0.0):
    """The 1099 column for one payee-year: what QuickBooks will be able to
    file. Returns {"level": red|amber|green|grey, "label", "reportable",
    "needs_attention"}.

      grey   Not required (W-2) · Under threshold · Card only
      red    reportable and the W-9 is missing — QuickBooks cannot file it
      amber  reportable, W-9 on file, but QuickBooks cannot file it yet:
             Not in QuickBooks (no qbo_vendor_id) or 1099 off · mismatch
             (Vendor1099 false / unknown on the mirrored record)
      green  reportable and ready: W-9 on file, linked, 1099 tracking on

    needs_attention is true for every reportable year that is not green."""
    nc = float(non_card or 0)
    th = float(threshold or 0)
    if w9_status == W9_NOT_REQUIRED:
        return {"level": "grey", "label": "Not required", "reportable": False,
                "needs_attention": False}
    if nc < th:
        label = "Card only" if (nc <= 0 and float(card or 0) > 0) else "Under threshold"
        return {"level": "grey", "label": label, "reportable": False,
                "needs_attention": False}
    if w9_status != W9_ON_FILE:
        return {"level": "red", "label": "Reportable · W-9 missing",
                "reportable": True, "needs_attention": True}
    if not qbo_vendor_id:
        return {"level": "amber", "label": "Not in QuickBooks",
                "reportable": True, "needs_attention": True}
    if qbo_vendor_1099 is not True:
        return {"level": "amber", "label": "1099 off · mismatch",
                "reportable": True, "needs_attention": True}
    return {"level": "green", "label": "Reportable · ready",
            "reportable": True, "needs_attention": False}


def qbo_matches_w9(w9_status, qbo_vendor_id, qbo_vendor_1099):
    """The QuickBooks card's "Matches W-9" row: yes / no / mismatch /
    unlinked. Both on → yes; both off → no; disagree → mismatch; no
    QuickBooks record → unlinked."""
    if not qbo_vendor_id:
        return "unlinked"
    have_w9 = (w9_status == W9_ON_FILE)
    flag = bool(qbo_vendor_1099)
    if have_w9 and flag:
        return "yes"
    if not have_w9 and not flag:
        return "no"
    return "mismatch"


# ── Payment attribution (DOCUMENTS_HANDOFF Phase 3, 2026-11-02) ───────────
#
# Transaction.payee_id says WHO the money went to. It is written at the
# moment the fact becomes known — QuickBooks names the vendor on the synced
# charge, a line with one crew member is coded, a document with an owner
# is matched, a vendor string is linked to a Payee — and never inferred at
# read time. The pure helpers here (set_txn_payee, resolve_payee_candidates,
# line_owner_id) take plain objects so tests/unit/test_txn_payee_rules.py
# can pin the precedence without a database.

PAYEE_SOURCES = ('qbo', 'line', 'doc', 'alias', 'manual')
PAYEE_SOURCE_MANUAL = 'manual'


def set_txn_payee(txn, payee_id, source, replace=()):
    """Write payee_id / payee_source on a Transaction by the ONE rule:

      * a NULL payee_id is filled;
      * an existing value is replaced only when its payee_source is in
        `replace` (a sync may replace its own earlier 'qbo' value, a
        re-coding its own earlier 'line' value, …);
      * a value a person set by hand ('manual') is never replaced by an
        automatic write — only another 'manual' write may do that.

    payee_id None with source S clears the attribution when the current
    source is S or in `replace` (a doc whose owner was removed, a line that
    lost its one person). Returns True when the row changed."""
    if source not in PAYEE_SOURCES:
        raise ValueError(f"unknown payee_source {source!r}")
    cur_id = getattr(txn, 'payee_id', None)
    cur_src = getattr(txn, 'payee_source', None)
    if payee_id is None:
        if cur_id is None:
            return False
        if cur_src == source or (cur_src in replace and cur_src != PAYEE_SOURCE_MANUAL):
            txn.payee_id = None
            txn.payee_source = None
            return True
        return False
    payee_id = int(payee_id)
    if cur_id is None:
        txn.payee_id = payee_id
        txn.payee_source = source
        return True
    if source == PAYEE_SOURCE_MANUAL:
        if cur_id == payee_id and cur_src == PAYEE_SOURCE_MANUAL:
            return False
        txn.payee_id = payee_id
        txn.payee_source = source
        return True
    if cur_src == PAYEE_SOURCE_MANUAL or cur_src not in replace:
        return False
    if cur_id == payee_id and cur_src == source:
        return False
    txn.payee_id = payee_id
    txn.payee_source = source
    return True


def line_owner_id(owners):
    """The ONE crew member on a budget line, or None when the line carries
    none or more than one. `owners` is any iterable of crew_member_ids
    (CrewAssignment.crew_member_id + BudgetLine.assigned_crew_id)."""
    uniq = {int(o) for o in (owners or ()) if o}
    return next(iter(uniq)) if len(uniq) == 1 else None


def line_owners_map(line_ids=None):
    """{budget_line_id: set(crew_member_id)} from CrewAssignment and
    BudgetLine.assigned_crew_id, in two bulk queries (optionally narrowed
    to `line_ids`)."""
    out = {}
    q1 = (db.session.query(CrewAssignment.budget_line_id, CrewAssignment.crew_member_id)
          .filter(CrewAssignment.crew_member_id.isnot(None)))
    q2 = (db.session.query(BudgetLine.id, BudgetLine.assigned_crew_id)
          .filter(BudgetLine.assigned_crew_id.isnot(None)))
    if line_ids is not None:
        ids = list(line_ids) or [0]
        q1 = q1.filter(CrewAssignment.budget_line_id.in_(ids))
        q2 = q2.filter(BudgetLine.id.in_(ids))
    for lid, cid in q1.all():
        out.setdefault(lid, set()).add(cid)
    for lid, cid in q2.all():
        out.setdefault(lid, set()).add(cid)
    return out


def single_line_owner(budget_line_id):
    """DB wrapper: the one crew member on this line, else None."""
    if not budget_line_id:
        return None
    return line_owner_id(line_owners_map([budget_line_id]).get(budget_line_id))


def payee_name_index(members):
    """{lowercased name: crew_member_id} over name / legal_name / company,
    with a name that two payees share mapped to None (ambiguous — never
    guessed). `members` yields objects with id, name, legal_name, company."""
    idx = {}
    for m in members:
        for raw in (getattr(m, 'name', None), getattr(m, 'legal_name', None),
                    getattr(m, 'company', None)):
            k = payee_name_key(raw)   # the ONE exact-name key (Cleanup D)
            if not k:
                continue
            if k in idx and idx[k] != m.id:
                idx[k] = None
            else:
                idx.setdefault(k, m.id)
    return idx


def resolve_payee_candidates(txn, doc_owner_by_id, line_owners_by_id,
                             alias_by_key=None, name_index=None):
    """Every signal that names a payee for one transaction, for the backfill:

      doc    its document's owner
      line   the ONE crew member on its budget line
      alias  VendorAlias.payee_id for canon_vendor(vendor) — `alias_by_key`
             is {(project_id, canon): payee_id}; the project row wins, then
             the global (None) row
      alias  an exact (case-insensitive) match of the vendor string to a
             payee's name / legal name / company (`name_index`), when
             exactly one payee carries it

    Returns {"payee_id", "source", "candidates": [(source, id)…]}. When the
    candidates disagree, payee_id is None and the caller LOGS the conflict
    instead of guessing; when they agree (or only one speaks) the first in
    the order above names the source."""
    cands = []
    did = getattr(txn, 'doc_upload_id', None)
    if did and doc_owner_by_id.get(did):
        cands.append(('doc', int(doc_owner_by_id[did])))
    lid = getattr(txn, 'budget_line_id', None)
    if lid:
        lo = line_owner_id(line_owners_by_id.get(lid))
        if lo:
            cands.append(('line', lo))
    vendor = getattr(txn, 'vendor', None) or ''
    if vendor.strip():
        cv = canon_vendor(vendor)
        if alias_by_key and cv:
            pid = getattr(txn, 'project_id', None)
            a = alias_by_key.get((pid, cv))
            if a is None:
                a = alias_by_key.get((None, cv))
            if a:
                cands.append(('alias', int(a)))
        if name_index:
            n = name_index.get(payee_name_key(vendor))
            if n:
                cands.append(('alias', int(n)))
    ids = {c[1] for c in cands}
    if not ids:
        return {"payee_id": None, "source": None, "candidates": cands}
    if len(ids) > 1:
        return {"payee_id": None, "source": None, "candidates": cands, "conflict": True}
    return {"payee_id": cands[0][1], "source": cands[0][0], "candidates": cands}


def payee_for_vendor_string(project_id, vendor):
    """The Payee a vendor string pays, for CSV-imported and manual rows:
    VendorAlias.payee_id (this project's row first, then the global row),
    else an exact case-insensitive match of the string to ONE payee's
    name / legal name / company. Returns a crew_member_id or None. Never
    fuzzy — a near miss attributes nobody."""
    vendor = (vendor or '').strip()
    if not vendor:
        return None
    try:
        from models import VendorAlias, CrewMember
        cv = canon_vendor(vendor)
        if cv:
            row = (VendorAlias.query
                   .filter(VendorAlias.raw_canonical == cv,
                           VendorAlias.payee_id.isnot(None),
                           db.or_(VendorAlias.project_id == project_id,
                                  VendorAlias.project_id.is_(None)))
                   .order_by(VendorAlias.project_id.isnot(None).desc(),
                             VendorAlias.confirm_count.desc())
                   .first())
            if row:
                return row.payee_id
        from sqlalchemy import func as _f
        key = vendor.lower()
        hits = (CrewMember.query
                .filter(db.or_(_f.lower(CrewMember.name) == key,
                               _f.lower(CrewMember.legal_name) == key,
                               _f.lower(CrewMember.company) == key))
                .limit(2).all())
        return hits[0].id if len(hits) == 1 else None
    except Exception as e:
        log.warning("[payee] vendor-string lookup failed for %r: %s", vendor, e)
        return None


def propagate_doc_owner_to_txns(upload, replace=('doc', 'line', 'alias', 'qbo')):
    """A document's owner was set (or cleared) by a person in the editor or
    the inbox: carry it onto every transaction that document backs. A
    person's pick on the document outranks every automatic source but a
    'manual' pick on the transaction itself. Does NOT commit. Returns the
    number of rows changed. Fail-open."""
    try:
        owner = doc_owner_id(upload)
        n = 0
        for t in Transaction.query.filter(Transaction.doc_upload_id == upload.id).all():
            if set_txn_payee(t, owner, 'doc', replace=replace):
                n += 1
        return n
    except Exception as e:
        log.warning("[payee] doc owner propagation skipped for doc %s: %s",
                    getattr(upload, 'id', None), e)
        return 0


def attribute_txns_for_qbo_vendor(qbo_vendor_id, payee_id):
    """A QuickBooks vendor was just linked to a Payee: every synced charge
    that carries that vendor's id (qbo_entity_ref) and no payee yet, or an
    earlier 'qbo' value, now attributes to the Payee. Does NOT commit.
    Returns the number of rows changed."""
    if not qbo_vendor_id or not payee_id:
        return 0
    n = 0
    for t in Transaction.query.filter(Transaction.qbo_entity_ref == str(qbo_vendor_id)).all():
        if set_txn_payee(t, payee_id, 'qbo', replace=('qbo',)):
            n += 1
    return n


def link_vendor_alias_to_payee(project_id, raw_vendor, payee_id, clean_name=None):
    """Link a raw vendor string to a Payee (VendorAlias.payee_id), creating
    the alias row when the string is new. Then every transaction with that
    vendor string and no payee attributes to the Payee ('alias'). Does NOT
    commit. Returns (alias_row, rows_attributed)."""
    from models import VendorAlias
    cv = canon_vendor(raw_vendor)
    if not cv:
        raise ValueError("That vendor name is empty after cleanup.")
    row = VendorAlias.query.filter_by(project_id=project_id, raw_canonical=cv).first()
    if row is None:
        row = VendorAlias(project_id=project_id, raw_canonical=cv,
                          clean_name=(clean_name or raw_vendor or '').strip()[:200] or cv,
                          confirm_count=1, last_confirmed_at=datetime.utcnow())
        db.session.add(row)
    return row, link_alias_row_to_payee(row, payee_id)


def link_alias_row_to_payee(alias_row, payee_id):
    """Point an existing VendorAlias row at a Payee and attribute every
    transaction whose vendor string canonicalises to it (project-scoped
    when the alias is). Does NOT commit. Returns rows attributed."""
    alias_row.payee_id = int(payee_id)
    db.session.flush()
    n = 0
    q = Transaction.query.filter(Transaction.vendor.isnot(None))
    if alias_row.project_id is not None:
        q = q.filter(Transaction.project_id == alias_row.project_id)
    for t in q.all():
        if canon_vendor(t.vendor) == alias_row.raw_canonical and \
                set_txn_payee(t, payee_id, 'alias', replace=('alias',)):
            n += 1
    return n


def unlink_vendor_alias(alias_row):
    """Remove the Payee link from a vendor alias and clear the 'alias'
    attribution it produced (other sources stay). Does NOT commit."""
    pid_ = alias_row.payee_id
    alias_row.payee_id = None
    n = 0
    if pid_:
        q = Transaction.query.filter(Transaction.payee_id == pid_,
                                     Transaction.payee_source == 'alias')
        if alias_row.project_id is not None:
            q = q.filter(Transaction.project_id == alias_row.project_id)
        for t in q.all():
            if canon_vendor(t.vendor) == alias_row.raw_canonical and set_txn_payee(t, None, 'alias'):
                n += 1
    return n
