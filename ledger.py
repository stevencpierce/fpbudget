"""Company ledger — the data step and the conflict tools (2026-11-03).

ONE row per QuickBooks transaction for the whole company (owner decision
2026-11-03). Before this, every project that synced an account received
its OWN COPY of each charge, and hiding the copies relied on claim
propagation that missed receipt-only, split-only and section-only claims
(GINTS 2026 synced the charges GrubHub already held). This module:

  preflight(db)        the pre-flight report: every (qbo_txn_id,
                       qbo_txn_type) group, what would be kept, deleted
                       or flagged — read-only, shown in Admin →
                       Maintenance before anything is touched
  run_migration(db)    the collapse itself, ONE transaction, behind the
                       system_task_log marker + a pg advisory lock; it
                       never runs on its own at boot — the owner starts
                       it from Maintenance after reading the preview
  undo_merge(db, id)   put a removed (or demoted / released) row back from
                       transaction_merge_log
  conflicts(db)        the "claimed in two projects" review list
  resolve_keep         keep one project's claim; the loser's receipt goes
                       back to Receipts to place (loser_mode
                       'receipt_to_place') OR stays coded as its own
                       receipt-born row because it was a WRONG reconcile —
                       a different charge (loser_mode 'receipt_stays_coded')
  resolve_unmatch_receipt / resolve_clear
                       the payee-differs actions
  release_row          "Release to unclaimed + unmatch receipt" on ANY
                       single claimed QuickBooks row (owner 2026-11-03: the
                       Joel / Ryan $4,500 payments go back to the pool and
                       their invoices back to Receipts to place, to redo)
Every action writes a transaction_merge_log snapshot and _log_activity,
so each is reversible through "Undo a ledger merge".

Rules of the collapse, per group of rows sharing a QuickBooks identity:
  "claimed" = budget_line_id OR account_code OR a transaction_split child
              OR doc_upload_id OR not_project_expense
  0 claimed → keep ONE (a real vendor name over "—" / blank, then the
              newest id), project_id → NULL (the pool); log + delete the rest
  1 claimed → keep the claimed row; log + delete the rest (anything that
              still points at a deleted row — evidence, splits, backup /
              parent links, match rejections, anomaly flags — is re-pointed
              at the kept row first)
  2+ claimed → keep ALL, claim_conflict = True on each, nothing deleted;
              they go to the review list
  payee differs → a reconciled row (receipt + QuickBooks) whose receipt-
              side vendor clearly differs from the QuickBooks payee is
              flagged too, reason "receipt payee differs from QuickBooks"
              (the Joel / Ryan case) — surfaced, never resolved silently

This module never imports app at module level; the activity log is
reached lazily (fail-open) like actuals.py does.
"""
import json
import logging
from datetime import datetime
from decimal import Decimal

from sqlalchemy import text

from models import (db, Transaction, TransactionSplit, TransactionMergeLog, ProjectSheet,
                    DocUpload, ExpenseEvidence, MatchRejection, AnomalyFlag, QBOConnection,
                    QBOVendor)

log = logging.getLogger(__name__)

MARKER = 'company_ledger_v1'
LOCK_KEY = 776700001
PAYEE_DIFFERS = 'receipt payee differs from QuickBooks'
# Appended to a payee-differs reason when a person answered "Different"
# (2026-11-04): the flag stays, the "Same person?" offer stops.
DIFFERENT_CONFIRMED = '[different payees confirmed]'
TWO_PROJECTS = 'claimed by two projects'

# The company-wide unique index (also in alembic 0025 and the essential pass).
# "transaction" is a reserved word on sqlite — quoted.
COMPANY_INDEX_SQL = (
    'CREATE UNIQUE INDEX IF NOT EXISTS uq_transaction_qbo_company '
    'ON "transaction" (qbo_txn_id, qbo_txn_type) '
    'WHERE qbo_txn_id IS NOT NULL AND claim_conflict IS NOT TRUE'
)
DUP_GROUPS_SQL = (
    'SELECT COUNT(*) FROM (SELECT qbo_txn_id, qbo_txn_type FROM "transaction" '
    'WHERE qbo_txn_id IS NOT NULL AND claim_conflict IS NOT TRUE '
    'GROUP BY qbo_txn_id, qbo_txn_type HAVING COUNT(*) > 1) d'
)

_BLANK_VENDORS = frozenset({'', '—', '-', '?', 'unknown', 'n/a'})


# ── Small helpers ──────────────────────────────────────────────────────────

def _json_safe(v):
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def row_to_dict(txn):
    """Every column of a Transaction as JSON-safe values (the merge log)."""
    out = {}
    for col in Transaction.__table__.columns:
        out[col.name] = _json_safe(getattr(txn, col.name, None))
    return out


def dict_to_row(d, keep_id=True):
    """A Transaction built from a merge-log snapshot."""
    t = Transaction()
    for col in Transaction.__table__.columns:
        if col.name == 'id' and not keep_id:
            continue
        if col.name not in d:
            continue
        v = d[col.name]
        if v is not None and isinstance(col.type, db.Numeric):
            v = Decimal(str(v))
        elif v is not None and isinstance(col.type, db.DateTime):
            try:
                v = datetime.fromisoformat(v)
            except (TypeError, ValueError):
                v = None
        setattr(t, col.name, v)
    return t


def vendor_is_real(v):
    return (v or '').strip().lower() not in _BLANK_VENDORS


def _vendor_tokens(v):
    from actuals import payee_tokens
    return payee_tokens(v or '')


def payees_differ(receipt_vendor, qbo_vendor, qbo_ref=None):
    """True when both names are real, are NOT the same payee by the ONE
    identity rule (actuals.same_payee — equal names, or a person and their
    loan-out company: "Two Foot Shuffle LLC" vs "Danielle Miller" once the
    loan-out is linked) and share no distinctive word — "Ryan Mauskopf" vs
    "Joel Knutson", "You Choose, LLC" vs "Sarah Caye Productions" (LLC /
    Productions never count as a shared word). "Lyft" vs "LYFT *RIDE"
    share a token → same."""
    from actuals import same_payee
    if not (vendor_is_real(receipt_vendor) and vendor_is_real(qbo_vendor)):
        return False
    if same_payee(receipt_vendor, (qbo_vendor, qbo_ref)):
        return False
    a, b = _vendor_tokens(receipt_vendor), _vendor_tokens(qbo_vendor)
    if not a or not b:
        return False
    return not (a & b)


def _log(action, txn_id, project_id, label, note, before=None, after=None):
    try:
        from app import _log_activity
        _log_activity(action=action, entity_type='transaction', entity_id=txn_id,
                      entity_label=(label or f'Txn #{txn_id}')[:80], project_id=project_id,
                      before=before, after=after, note=note[:500])
    except Exception:
        log.info("[ledger] activity log skipped (%s #%s)", action, txn_id, exc_info=True)


# ── Group analysis (pure, read-only) ───────────────────────────────────────

def _split_parent_ids(ids):
    if not ids:
        return set()
    return {r[0] for r in db.session.query(TransactionSplit.transaction_id)
            .filter(TransactionSplit.transaction_id.in_(list(ids))).distinct().all()}


def _signals(t, split_parents):
    """The four claim signals (2026-11-04): line, section, split, receipt.
    "Not project" is NOT a claim — in the old per-project model it mostly
    meant "not THIS project's" (the sibling propagation stamped it on every
    copy to hide it); it becomes a per-project dismissal instead."""
    return {
        'line': bool(t.budget_line_id),
        'section': bool(t.account_code) and not t.budget_line_id,
        'split': t.id in split_parents,
        'receipt': bool(t.doc_upload_id),
    }


def _is_claimed_row(t, split_parents):
    return any(_signals(t, split_parents).values())


def _is_coded_row(t, split_parents):
    """Line, section or split — the coding signals (a receipt alone is
    not coding). A project that keeps a row it coded never gets a
    dismissal of that row (2026-11-05)."""
    s = _signals(t, split_parents)
    return bool(s['line'] or s['section'] or s['split'])


def _qbo_payee_for_group(members, vendor_names):
    """(name, from_ref) — the payee QuickBooks names for this charge. From
    the mirrored vendor by EntityRef, or the vendor on a plain qbo_sync
    member that carries an EntityRef (QuickBooks' VendorRef name): a real
    vendor or person, from_ref=True. Else the raw text on a plain qbo_sync
    member (often card-processor noise — "TST*RIVER BAR"), from_ref=False."""
    for m in members:
        ref = getattr(m, 'qbo_entity_ref', None)
        if ref and vendor_names.get(str(ref)):
            return vendor_names[str(ref)], True
    for m in members:
        if m.source == 'qbo_sync' and getattr(m, 'qbo_entity_ref', None) and vendor_is_real(m.vendor):
            return m.vendor, True
    for m in members:
        if m.source == 'qbo_sync' and vendor_is_real(m.vendor):
            return m.vendor, False
    return None, False


def _is_processor_noise(v):
    from actuals import strip_processor_prefix
    s = (v or '').strip()
    return bool(s) and strip_processor_prefix(s) != s


def payee_flag_applies(receipt_vendor, qbo_payee, from_ref, qbo_ref=None):
    """Flag "receipt payee differs from QuickBooks" only when QuickBooks
    names a real vendor / person (EntityRef / VendorRef) — never against
    raw card-processor text (TST*, SQ *, AMAZON MKTPL* …) — and the two
    are different payees (payees_differ: not the same person through a
    loan-out, no distinctive word shared)."""
    if not from_ref or not qbo_payee or _is_processor_noise(qbo_payee):
        return False
    return payees_differ(receipt_vendor, qbo_payee, qbo_ref)


def qbo_payee_from_reason(reason):
    """'receipt payee differs from QuickBooks (Danielle Miller)' → 'Danielle
    Miller' (None when the reason names nobody)."""
    import re as _re
    m = _re.search(r'\(([^()]*)\)', reason or '')
    return (m.group(1).strip() or None) if m else None


def _vendor_user_set(t):
    return (vendor_is_real(t.vendor)
            and (t.source in ('reconciled', 'manual_entry', 'doc_upload')
                 or bool(getattr(t, 'ai_cleaned_at', None))))


def _vendor_weak(v):
    return not vendor_is_real(v) or _is_processor_noise(v)


def _keep_pick(members):
    """0 claimed: a reconciled row over a bare qbo_sync one, a person-set
    vendor over a raw one, a real vendor name over a blank one, then the
    newest id."""
    return sorted(members, key=lambda m: (0 if m.source == 'reconciled' else 1,
                                          0 if _vendor_user_set(m) else 1,
                                          0 if vendor_is_real(m.vendor) else 1,
                                          -m.id))[0]


def _carry_plan(keeper, copy):
    """What a copy about to be deleted hands to the keeper so no person's
    data is lost (2026-11-04): a vendor name a person set (reconciled /
    manual / AI-cleaned) when the keeper's is blank, "—" or processor
    noise; a note when the keeper has none (appended when both differ);
    card, payee, QuickBooks category / entity ref when the keeper has
    none. Pure: returns {field: value}."""
    out = {}
    if _vendor_user_set(copy) and _vendor_weak(keeper.vendor) and copy.vendor != keeper.vendor:
        out['vendor'] = copy.vendor
    cn, kn = (copy.note or '').strip(), (keeper.note or '').strip()
    if cn and not kn:
        out['note'] = copy.note
    elif cn and cn != kn and cn not in kn:
        out['note'] = (kn + f" [from copy #{copy.id}] " + cn)[:2000]
    for f in ('card_last4', 'qbo_category', 'qbo_entity_ref', 'qbo_account_id'):
        if getattr(copy, f, None) and not getattr(keeper, f, None):
            out[f] = getattr(copy, f)
    if getattr(copy, 'payee_id', None) and not getattr(keeper, 'payee_id', None):
        out['payee_id'] = copy.payee_id
        out['payee_source'] = copy.payee_source
    return out


def _np_reason(t):
    return (t.excluded_reason or t.no_charge_reason
            or 'Not project (marked before the company ledger)')[:300]


def analyze(db_, keys=None):
    """Group every QuickBooks row by identity and decide (2026-11-04 rule).
    Claim signals are line, section, split, receipt — never not_project.

      1 real claimer (or several in ONE project) → keep it, delete the
              other copies
      0 real claimers → keep ONE row in the pool (a reconciled row over a
              bare qbo_sync one); delete the rest
      2+ real claimers in different projects → keep each, flag conflict
      every not_project mark on any copy → a dismissal for that copy's
              project, on the kept row
      a deleted copy's person-set vendor / note → carried to the keeper
              when the keeper's is blank or processor noise

    `keys` narrows to a set of (qbo_txn_id, qbo_txn_type) (tests, a
    targeted re-run). Returns the id lists plus per-group decisions for
    the preview."""
    q = Transaction.query.filter(Transaction.qbo_txn_id.isnot(None))
    if keys is not None:
        q = q.filter(Transaction.qbo_txn_id.in_([k[0] for k in keys] or ['']))
    rows = q.order_by(Transaction.qbo_txn_id, Transaction.qbo_txn_type, Transaction.id).all()
    if keys is not None:
        rows = [t for t in rows if (t.qbo_txn_id, t.qbo_txn_type) in set(keys)]
    groups = {}
    for t in rows:
        groups.setdefault((t.qbo_txn_id, t.qbo_txn_type), []).append(t)
    split_parents = _split_parent_ids([t.id for t in rows])
    try:
        vendor_names = {v.qbo_vendor_id: v.display_name for v in QBOVendor.query.all()}
    except Exception:
        vendor_names = {}
    pnames = {p.id: p.name for p in ProjectSheet.query.all()}

    def _pn(pid):
        return pnames.get(pid, '—' if pid is None else f'#{pid}')

    keep_ids, delete_pairs, conflict_ids, payee_flags, decisions = [], [], [], [], []
    unclaim_ids, dismissals, carries = [], [], []
    by_project = {}

    def _bp(pid, k, n=1):
        d = by_project.setdefault(_pn(pid), {"kept_claimed": 0, "deleted": 0, "released_to_pool": 0,
                                             "conflicts": 0, "dismissals": 0, "payee_flags": 0})
        d[k] += n

    dup_groups = 0
    for key, members in groups.items():
        claimed = [m for m in members if _is_claimed_row(m, split_parents)]
        qbo_payee, from_ref = _qbo_payee_for_group(members, vendor_names)
        if len(members) > 1:
            dup_groups += 1
        actions = {}     # id -> (action, why)
        kept_list = []
        if not claimed:
            keep = _keep_pick(members)
            kept_list = [keep]
            if keep.project_id is not None:
                unclaim_ids.append(keep.id)
                _bp(keep.project_id, "released_to_pool")
                actions[keep.id] = ('keep', 'no line, section, split or receipt anywhere — released to the pool'
                                    + (' (preferred: reconciled)' if keep.source == 'reconciled' else ''))
            else:
                actions[keep.id] = ('keep', 'already in the pool')
            what = 'nobody claims it: keep one row in the pool'
        elif len({m.project_id for m in claimed}) == 1:
            claimed.sort(key=lambda m: (-sum(_signals(m, split_parents).values()), m.id))
            keep = claimed[0]
            kept_list = [keep]
            _bp(keep.project_id, "kept_claimed")
            actions[keep.id] = ('keep', 'the one project that claims it ('
                                + ', '.join(k for k, v in _signals(keep, split_parents).items() if v) + ')')
            what = f'claimed by {_pn(keep.project_id)} only: keep that row'
            if keep.doc_upload_id and payee_flag_applies(keep.vendor, qbo_payee, from_ref,
                                                         getattr(keep, 'qbo_entity_ref', None)):
                payee_flags.append((keep.id, qbo_payee))
                _bp(keep.project_id, "payee_flags")
        else:
            kept_list = list(claimed)
            keep = claimed[0]
            for m in claimed:
                conflict_ids.append(m.id)
                _bp(m.project_id, "conflicts")
                actions[m.id] = ('conflict', 'claims it too (' + ', '.join(
                    k for k, v in _signals(m, split_parents).items() if v) + ') — review')
            what = 'claimed by two projects: keep each claim, flag for review'
        keep_ids.extend(m.id for m in kept_list)
        kept_set = {m.id for m in kept_list}
        for m in members:
            if m.id in kept_set:
                continue
            why = ('only a Not project mark — becomes a dismissal for ' + _pn(m.project_id)
                   if m.not_project_expense else
                   'a copy with no claim' + (f' ({m.source})' if m.source != 'qbo_sync' else ''))
            delete_pairs.append((m.id, keep.id, ('copy: ' + why)[:120]))
            _bp(m.project_id, "deleted")
            actions[m.id] = ('delete', why)
            plan = _carry_plan(keep, m)
            if plan:
                carries.append((keep.id, m.id, plan))
                actions[m.id] = ('delete', why + '; carries ' + ', '.join(
                    f"{k} '{v}'" if k == 'vendor' else k for k, v in plan.items()) + f' to #{keep.id}')
        # every not_project mark becomes a dismissal for that copy's project
        # — NEVER for a project that keeps a row with a line, section or
        # split (2026-11-05: the production run of 11-04 did exactly that
        # on GrubHub's split BillPayments and hid $29,600 of coded fees).
        seen_np = set()
        coded_keepers = {c.project_id for c in kept_list if _is_coded_row(c, split_parents)}
        for m in members:
            if m.not_project_expense and m.project_id is not None and m.project_id not in seen_np:
                if m.id in kept_set and m.id not in unclaim_ids:
                    continue          # a claimed row's own exclusion stays as it is
                if claimed and m.project_id in {c.project_id for c in kept_list}:
                    continue          # the claiming project holds the row itself
                if m.project_id in coded_keepers:
                    continue          # the keeper is coded by that project: no dismissal, ever
                seen_np.add(m.project_id)
                dismissals.append((keep.id, m.project_id, _np_reason(m),
                                   m.excluded_by_user_id, m.excluded_at))
                _bp(m.project_id, "dismissals")
        if len(members) > 1 or len(claimed) > 1 or any(f[0] in kept_set for f in payee_flags) \
                or (kept_list and kept_list[0].id in unclaim_ids and kept_list[0].not_project_expense):
            parts = []
            for m in members:
                a, why = actions.get(m.id, ('keep', ''))
                parts.append(f"{a} #{m.id} ({_pn(m.project_id)}, {m.source}, '{m.vendor or '—'}'): {why}")
            decisions.append({
                "qbo_txn_type": key[1], "qbo_txn_id": key[0],
                "amount": float(members[0].amount or 0), "date": members[0].txn_date,
                "what": what + ' — ' + '; '.join(parts),
                "qbo_payee": qbo_payee, "qbo_payee_from_quickbooks_vendor": from_ref,
                "dismissals": sorted({_pn(m.project_id) for m in members
                                      if m.not_project_expense and m.project_id in seen_np}),
                "rows": [{"id": m.id, "project_id": m.project_id, "project": _pn(m.project_id),
                          "vendor": m.vendor, "source": m.source,
                          "signals": [k for k, v in _signals(m, split_parents).items() if v],
                          "not_project_mark": bool(m.not_project_expense),
                          "action": actions.get(m.id, ('keep', ''))[0],
                          "why": actions.get(m.id, ('keep', ''))[1],
                          "keep": m.id in kept_set,
                          "conflict": (len(kept_list) > 1 and m.id in kept_set),
                          "payee_differs": any(f[0] == m.id for f in payee_flags)}
                         for m in members],
            })
    return {"rows": len(rows), "groups": len(groups), "dup_groups": dup_groups,
            "keep": keep_ids, "unclaim": unclaim_ids, "delete": delete_pairs,
            "conflicts": conflict_ids, "payee_flags": payee_flags,
            "dismissals": dismissals, "carries": carries,
            "by_project": by_project, "decisions": decisions}


def preflight(db_=None, keys=None):
    """The report the Maintenance page shows before the run. Counts first
    (with a per-project breakdown), then the decisions: every group with
    more than one row, every conflict, every payee flag, every released
    Not-project row — each row with its action and why. Read-only."""
    a = analyze(db, keys=keys)
    counts = {"qbo_rows": a["rows"], "groups": a["groups"],
              "groups_with_copies": a["dup_groups"],
              "to_delete": len(a["delete"]),
              "to_release_to_pool": len(a["unclaim"]),
              "conflicts_two_projects": len(a["conflicts"]),
              "payee_differs": len(a["payee_flags"]),
              "dismissals_to_create": len(a["dismissals"]),
              "vendor_or_note_carried": len(a["carries"])}
    log.info("[ledger preflight] %s", counts)
    done = marker_done()
    return {"ok": True, "already_run": done, "counts": counts,
            "by_project": dict(sorted(a["by_project"].items())),
            "decisions": a["decisions"][:600],
            "decisions_total": len(a["decisions"]),
            "index_sql": COMPANY_INDEX_SQL}


# ── The run ────────────────────────────────────────────────────────────────

def _ensure_task_log():
    try:
        db.session.execute(text(
            "CREATE TABLE IF NOT EXISTS system_task_log ("
            " task_name VARCHAR(80) PRIMARY KEY,"
            " last_run_at TIMESTAMP NOT NULL,"
            " last_result TEXT)"))
        db.session.commit()
    except Exception:
        db.session.rollback()


def marker_done():
    _ensure_task_log()
    try:
        return db.session.execute(text("SELECT 1 FROM system_task_log WHERE task_name = :t"),
                                  {'t': MARKER}).fetchone() is not None
    except Exception:
        db.session.rollback()
        return False


def _repoint(removed_id, kept_id):
    """Everything that still points at a row about to be deleted moves to
    the kept row (nothing is lost): evidence, splits, backup / parent
    links, match rejections, anomaly flags."""
    moved = {}
    kept_docs = {e.doc_upload_id for e in ExpenseEvidence.query.filter_by(transaction_id=kept_id).all()}
    n = 0
    for ev in ExpenseEvidence.query.filter_by(transaction_id=removed_id).all():
        if ev.doc_upload_id in kept_docs:
            db.session.delete(ev)
        else:
            ev.transaction_id = kept_id
            n += 1
    if n:
        moved['evidence'] = n
    n = (TransactionSplit.query.filter_by(transaction_id=removed_id)
         .update({TransactionSplit.transaction_id: kept_id}, synchronize_session=False))
    if n:
        moved['splits'] = n
    n = (Transaction.query.filter_by(backup_of_txn_id=removed_id)
         .update({Transaction.backup_of_txn_id: kept_id}, synchronize_session=False))
    if n:
        moved['backup_links'] = n
    n = (Transaction.query.filter_by(parent_transaction_id=removed_id)
         .update({Transaction.parent_transaction_id: kept_id}, synchronize_session=False))
    if n:
        moved['children'] = n
    kept_rej = {r.doc_upload_id for r in MatchRejection.query.filter_by(transaction_id=kept_id).all()}
    n = 0
    for r in MatchRejection.query.filter_by(transaction_id=removed_id).all():
        if r.doc_upload_id in kept_rej:
            db.session.delete(r)
        else:
            r.transaction_id = kept_id
            n += 1
    if n:
        moved['match_rejections'] = n
    n = (AnomalyFlag.query.filter_by(transaction_id=removed_id)
         .update({AnomalyFlag.transaction_id: kept_id}, synchronize_session=False))
    if n:
        moved['anomaly_flags'] = n
    return moved


def snapshot(txn, reason, kept_id=None):
    """A transaction_merge_log entry holding the whole row as it is NOW.
    Written before a delete, a demotion (QuickBooks identity dropped) or a
    release, so "Undo a ledger merge" can put it back."""
    entry = TransactionMergeLog(
        qbo_txn_id=txn.qbo_txn_id, qbo_txn_type=txn.qbo_txn_type,
        kept_txn_id=kept_id, removed_txn_id=txn.id, removed_project_id=txn.project_id,
        removed_row_json=json.dumps(row_to_dict(txn), default=str),
        reason=(reason or '')[:120], created_at=datetime.utcnow())
    db.session.add(entry)
    db.session.flush()
    return entry


IN_PLACE_REASONS = ('demoted', 'released')   # the row still exists; undo restores it in place


def remove_twin(txn, kept_id, reason):
    """Log the row to transaction_merge_log, re-point what depends on it,
    delete it. Flushes; the caller commits."""
    entry = snapshot(txn, reason, kept_id)
    moved = _repoint(txn.id, kept_id)
    db.session.delete(txn)
    db.session.flush()
    return entry, moved


def seed_company_accounts():
    """QBOConnection.enabled_account_ids = its current set ∪ every
    project's retired qbo_account_ids. Returns the list."""
    conn = QBOConnection.query.first()
    if conn is None:
        return []
    ids = []
    try:
        ids = [str(x) for x in (json.loads(conn.enabled_account_ids or '[]') or [])]
    except Exception:
        ids = []
    for (raw,) in db.session.query(ProjectSheet.qbo_account_ids).all():
        try:
            for x in (json.loads(raw or '[]') or []):
                if str(x) not in ids and x not in (None, '', '0', 0):
                    ids.append(str(x))
        except Exception:
            continue
    conn.enabled_account_ids = json.dumps(ids)
    # The watermarks move to the connection: the OLDEST project watermark
    # keeps the next company sync from skipping a window some project
    # never reached; the newest last_synced is informational.
    if not conn.sync_through:
        marks = [m for (m,) in db.session.query(ProjectSheet.sync_through).all() if m]
        conn.sync_through = min(marks) if marks else None
    if not conn.last_synced:
        conn.last_synced = db.session.query(db.func.max(ProjectSheet.last_synced)).scalar()
    if not conn.last_cdc_sync:
        conn.last_cdc_sync = db.session.query(db.func.min(ProjectSheet.last_cdc_sync)).scalar()
    return ids


def run_migration(user_id=None, keys=None):
    """The collapse: one transaction, marker + advisory lock, idempotent
    (a second run finds nothing to do and reports that). Returns the
    report dict. Raises on a failure after rolling everything back."""
    _ensure_task_log()
    is_pg = 'postgresql' in str(db.engine.url).lower()
    if is_pg:
        got = db.session.execute(text("SELECT pg_try_advisory_lock(:k)"), {'k': LOCK_KEY}).scalar()
        if not got:
            raise RuntimeError("Another worker is running the ledger migration — try again in a minute.")
    try:
        a = analyze(db, keys=keys)
        pnames = {p.id: p.name for p in ProjectSheet.query.all()}
        by_id = {t.id: t for t in Transaction.query.filter(Transaction.qbo_txn_id.isnot(None)).all()}
        deleted, released, flagged, moved_total = 0, 0, 0, {}
        # 1. person-set data on the copies goes to the keeper first
        carried = 0
        for kept_id, copy_id, plan in a["carries"]:
            k = by_id.get(kept_id)
            if k is None:
                continue
            for f, v in plan.items():
                if f == 'vendor' and not _vendor_weak(k.vendor):
                    continue      # an earlier copy already gave it a name
                setattr(k, f, v)
            carried += 1
        for removed_id, kept_id, reason in a["delete"]:
            t = by_id.get(removed_id)
            if t is None:
                continue
            _entry, moved = remove_twin(t, kept_id, reason)
            for k, v in moved.items():
                moved_total[k] = moved_total.get(k, 0) + v
            deleted += 1
        for tid in a["unclaim"]:
            t = by_id.get(tid)
            if t is not None and t.project_id is not None:
                pid = t.project_id
                t.project_id = None
                t.match_status = 'unmatched'
                # a Not project mark is a dismissal now (written below), never
                # a company-wide exclusion of a pool row
                t.not_project_expense = False
                t.excluded_at = t.excluded_by_user_id = t.excluded_reason = None
                if t.source == 'reconciled' and not t.doc_upload_id:
                    t.source = 'qbo_sync'
                t.updated_at = datetime.utcnow()
                released += 1
                _log('release', t.id, pid, t.vendor,
                     f"Company ledger: unclaimed copy released to the pool (was in {pnames.get(pid, pid)})")
        for tid in a["conflicts"]:
            t = by_id.get(tid)
            if t is not None:
                t.claim_conflict = True
                t.claim_conflict_reason = TWO_PROJECTS
                flagged += 1
                _log('claim_conflict', t.id, t.project_id, t.vendor,
                     f"Company ledger: {t.qbo_txn_type}/{t.qbo_txn_id} is claimed by two projects — review")
        for tid, qbo_payee in a["payee_flags"]:
            t = by_id.get(tid)
            if t is not None:
                t.claim_conflict = True
                t.claim_conflict_reason = f"{PAYEE_DIFFERS} ({qbo_payee})"[:200]
                flagged += 1
                _log('claim_conflict', t.id, t.project_id, t.vendor,
                     f"Company ledger: receipt payee '{t.vendor}' differs from QuickBooks payee "
                     f"'{qbo_payee}' on {t.qbo_txn_type}/{t.qbo_txn_id} — review")
        # every legacy not_project mark → a dismissal for that copy's project
        from models import TransactionPoolDismissal as _TPD
        n_dis = 0
        from actuals import coded_here as _coded_here
        for kept_id, pid, reason, uid, at in a["dismissals"]:
            kept = by_id.get(kept_id) or Transaction.query.get(kept_id)
            if kept is None:
                continue
            if _TPD.query.filter_by(transaction_id=kept_id, project_id=pid).first():
                continue
            if _coded_here(kept, pid):
                # the guard analyze() applies, re-checked on the live row at
                # write time: a project never dismisses a row it coded
                log.warning("[ledger migration] no dismissal for project %s on txn %s — it codes it", pid, kept_id)
                continue
            db.session.add(_TPD(transaction_id=kept_id, project_id=pid, reason=reason,
                                created_at=at or datetime.utcnow(), created_by=uid))
            n_dis += 1
        # a kept row its project CODED (line / section / split) is never
        # hidden under that project's Not project: a legacy flag on it goes
        # (the sibling propagation stamped it on every copy) — 2026-11-05
        unhidden = 0
        for kept_id in a["keep"]:
            t = by_id.get(kept_id)
            if t is not None and t.not_project_expense and _coded_here(t, t.project_id):
                t.not_project_expense = False
                t.excluded_at = t.excluded_by_user_id = t.excluded_reason = None
                t.updated_at = datetime.utcnow()
                unhidden += 1
                _log('update', t.id, t.project_id, t.vendor,
                     "Company ledger: a coded row carried a legacy Not project flag — cleared "
                     "(a charge this project coded is never hidden under its Not project)")
        accounts = seed_company_accounts()
        db.session.flush()
        remaining = db.session.execute(text(DUP_GROUPS_SQL)).scalar()
        index_created = False
        if remaining == 0:
            db.session.execute(text("DROP INDEX IF EXISTS uq_transaction_qbo"))
            db.session.execute(text(COMPANY_INDEX_SQL))
            index_created = True
        summary = (f"groups {a['groups']}, copies {a['dup_groups']}; deleted {deleted}, "
                   f"released {released}, flagged {flagged}, dismissals {n_dis}, "
                   f"carried {carried}, unhidden {unhidden}; accounts {len(accounts)}; "
                   f"index {'created' if index_created else 'NOT created — ' + str(remaining) + ' duplicate groups remain'}")
        db.session.execute(text(
            "INSERT INTO system_task_log (task_name, last_run_at, last_result) "
            "VALUES (:t, CURRENT_TIMESTAMP, :r) ON CONFLICT (task_name) DO UPDATE "
            "SET last_run_at = EXCLUDED.last_run_at, last_result = EXCLUDED.last_result"),
            {'t': MARKER, 'r': summary[:2000]})
        db.session.commit()
        log.info("[ledger migration] %s", summary)
        return {"ok": True, "deleted": deleted, "released": released, "flagged": flagged,
                "dismissals": n_dis, "carried": carried, "unhidden": unhidden, "moved": moved_total,
                "accounts": accounts, "index_created": index_created,
                "remaining_duplicate_groups": int(remaining or 0), "summary": summary}
    except Exception:
        db.session.rollback()
        raise
    finally:
        if is_pg:
            try:
                db.session.execute(text("SELECT pg_advisory_unlock(:k)"), {'k': LOCK_KEY})
                db.session.commit()
            except Exception:
                db.session.rollback()


# ── Undo ───────────────────────────────────────────────────────────────────

def undo_merge(log_id):
    """Put a removed twin back from its merge-log snapshot. Keeps the old
    id when it is free. The restored row is flagged claim_conflict so the
    company unique index accepts it next to the kept row; the review list
    then shows the pair for a person to settle."""
    entry = TransactionMergeLog.query.get(log_id)
    if entry is None:
        raise ValueError(f"merge log #{log_id} not found")
    if entry.restored_at:
        raise ValueError(f"merge log #{log_id} was already restored as transaction #{entry.restored_txn_id}")
    d = json.loads(entry.removed_row_json or '{}')
    existing = Transaction.query.get(d.get('id')) if d.get('id') else None
    if existing is not None and any((entry.reason or '').startswith(r) for r in IN_PLACE_REASONS):
        return _undo_in_place(entry, existing, d)
    keep_id = existing is None if d.get('id') else False
    t = dict_to_row(d, keep_id=bool(keep_id))
    t.claim_conflict = True
    t.claim_conflict_reason = f"restored from merge log #{entry.id}"
    # Links the snapshot carried that may be gone now are dropped rather
    # than failing the restore.
    if t.doc_upload_id and DocUpload.query.get(t.doc_upload_id) is None:
        t.doc_upload_id = None
    if t.budget_line_id:
        from models import BudgetLine
        if BudgetLine.query.get(t.budget_line_id) is None:
            t.budget_line_id = None
    if t.project_id and ProjectSheet.query.get(t.project_id) is None:
        t.project_id = None
    t.parent_transaction_id = None
    t.backup_of_txn_id = None
    db.session.add(t)
    db.session.flush()
    entry.restored_at = datetime.utcnow()
    entry.restored_txn_id = t.id
    kept = Transaction.query.get(entry.kept_txn_id) if entry.kept_txn_id else None
    if kept is not None and not kept.claim_conflict:
        kept.claim_conflict = True
        kept.claim_conflict_reason = f"twin restored from merge log #{entry.id}"
    db.session.commit()
    _log('restore', t.id, t.project_id, t.vendor,
         f"Company ledger: restored row #{t.id} from merge log #{entry.id} "
         f"({entry.qbo_txn_type}/{entry.qbo_txn_id})")
    return {"ok": True, "transaction_id": t.id, "kept_txn_id": entry.kept_txn_id}


def _undo_in_place(entry, t, d):
    """The row was demoted (QuickBooks identity dropped) or released (to
    the pool, receipt unmatched): put every column back from the snapshot.
    A receipt-born sister row created at that time goes away again; a twin
    that now holds the QuickBooks identity makes both rows claim_conflict
    for the review list."""
    snap = dict_to_row(d, keep_id=True)
    for col in Transaction.__table__.columns:
        if col.name == 'id':
            continue
        setattr(t, col.name, getattr(snap, col.name))
    if t.doc_upload_id and DocUpload.query.get(t.doc_upload_id) is None:
        t.doc_upload_id = None
    if t.budget_line_id:
        from models import BudgetLine
        if BudgetLine.query.get(t.budget_line_id) is None:
            t.budget_line_id = None
    if t.project_id and ProjectSheet.query.get(t.project_id) is None:
        t.project_id = None
    if t.doc_upload_id and t.source != 'doc_upload':
        for sis in Transaction.query.filter(Transaction.doc_upload_id == t.doc_upload_id,
                                            Transaction.source == 'doc_upload',
                                            Transaction.id != t.id).all():
            _delete_evidence(sis.id)
            db.session.delete(sis)
        MatchRejection.query.filter_by(transaction_id=t.id, doc_upload_id=t.doc_upload_id)\
            .delete(synchronize_session=False)
    t.updated_at = datetime.utcnow()
    twins = []
    if t.qbo_txn_id:
        twins = Transaction.query.filter(Transaction.qbo_txn_id == t.qbo_txn_id,
                                         Transaction.qbo_txn_type == t.qbo_txn_type,
                                         Transaction.id != t.id).all()
    if twins:
        t.claim_conflict = True
        t.claim_conflict_reason = f"restored from merge log #{entry.id}"
        for tw in twins:
            if not tw.claim_conflict:
                tw.claim_conflict = True
                tw.claim_conflict_reason = f"twin restored from merge log #{entry.id}"
    entry.restored_at = datetime.utcnow()
    entry.restored_txn_id = t.id
    db.session.commit()
    _log('restore', t.id, t.project_id, t.vendor,
         f"Company ledger: row #{t.id} restored in place from merge log #{entry.id} ({entry.reason})")
    return {"ok": True, "transaction_id": t.id, "kept_txn_id": entry.kept_txn_id, "in_place": True}


# ── Review list: claimed in two projects / payee differs ──────────────────

def _signal_text(t, split_parents, line_labels):
    parts = []
    if t.budget_line_id:
        parts.append(f"line {line_labels.get(t.budget_line_id, '#%s' % t.budget_line_id)}")
    elif t.account_code:
        parts.append(f"section {t.account_code} {t.account_code_name or ''}".strip())
    if t.id in split_parents:
        parts.append("split")
    if t.doc_upload_id:
        parts.append(f"receipt #{t.doc_upload_id}")
    if t.not_project_expense:
        parts.append("not a project expense")
    return parts


def conflicts(project_id=None):
    """One card per flagged charge: the QuickBooks facts and each project's
    claim. project_id narrows to the cards that involve that project."""
    q = Transaction.query.filter(Transaction.claim_conflict == True)     # noqa: E712
    rows = q.order_by(Transaction.qbo_txn_id, Transaction.id).all()
    if not rows:
        return []
    keys = {(t.qbo_txn_id, t.qbo_txn_type) for t in rows}
    members = {}
    for t in Transaction.query.filter(Transaction.qbo_txn_id.in_([k[0] for k in keys])).all():
        if (t.qbo_txn_id, t.qbo_txn_type) in keys:
            members.setdefault((t.qbo_txn_id, t.qbo_txn_type), []).append(t)
    split_parents = _split_parent_ids([t.id for ms in members.values() for t in ms])
    pnames = {p.id: p.name for p in ProjectSheet.query.all()}
    line_ids = {t.budget_line_id for ms in members.values() for t in ms if t.budget_line_id}
    line_labels = {}
    if line_ids:
        from models import BudgetLine
        for l in BudgetLine.query.filter(BudgetLine.id.in_(list(line_ids))).all():
            line_labels[l.id] = f"{l.account_code} · {(l.description or l.account_name or '')[:40]}"
    doc_ids = {t.doc_upload_id for ms in members.values() for t in ms if t.doc_upload_id}
    docs = {}
    if doc_ids:
        for d in DocUpload.query.filter(DocUpload.id.in_(list(doc_ids))).all():
            docs[d.id] = d
    try:
        vendor_names = {v.qbo_vendor_id: v.display_name for v in QBOVendor.query.all()}
    except Exception:
        vendor_names = {}
    cards = []
    for key, ms in sorted(members.items(), key=lambda kv: (kv[1][0].txn_date or '', kv[0])):
        if project_id is not None and not any(m.project_id == project_id for m in ms):
            continue
        qbo_payee, _from_ref = _qbo_payee_for_group(ms, vendor_names)
        reasons = {m.claim_conflict_reason for m in ms if m.claim_conflict_reason}
        if qbo_payee is None:
            # The migration wrote the QuickBooks name into the reason …
            for r in reasons:
                if (r or '').startswith(PAYEE_DIFFERS):
                    qbo_payee = qbo_payee_from_reason(r)
                    if qbo_payee:
                        break
        if qbo_payee is None:
            # … or the twin that carried it is in the merge log.
            for e in (TransactionMergeLog.query.filter_by(qbo_txn_id=key[0], qbo_txn_type=key[1])
                      .order_by(TransactionMergeLog.id.desc()).all()):
                try:
                    d = json.loads(e.removed_row_json or '{}')
                except Exception:
                    continue
                if d.get('source') == 'qbo_sync' and vendor_is_real(d.get('vendor')):
                    qbo_payee = d.get('vendor')
                    break
        kind = 'two_projects' if len({m.project_id for m in ms if m.claim_conflict}) > 1 or len(ms) > 1 \
            else ('payee' if any((r or '').startswith(PAYEE_DIFFERS) for r in reasons) else 'other')
        first = ms[0]
        # Loan-outs (2026-11-04): a payee card offers "Same person — X is
        # Y's loan-out" (actuals.loanout_proposal) unless someone said
        # Different already.
        payee_info = None
        if kind == 'payee':
            try:
                _pf = next((m for m in ms if _payee_flagged(m)), None)
                payee_info = payee_differs_info(_pf) if _pf is not None else None
                if payee_info is not None:
                    payee_info["txn_id"] = _pf.id
            except Exception:
                log.info("[ledger] payee info skipped", exc_info=True)
        cards.append({
            "qbo_txn_id": key[0], "qbo_txn_type": key[1],
            "kind": kind, "reasons": sorted(reasons),
            "amount": float(first.amount or 0), "is_expense": bool(first.is_expense),
            "date": first.txn_date, "account": first.account_code_name if not first.account_code else None,
            "card_last4": first.card_last4,
            "qbo_payee": qbo_payee,
            "payee_differs": payee_info,
            "claims": [{
                "id": m.id, "project_id": m.project_id,
                "project": pnames.get(m.project_id, 'Unclaimed' if m.project_id is None else f'#{m.project_id}'),
                "vendor": m.vendor, "source": m.source,
                "signals": _signal_text(m, split_parents, line_labels),
                "receipt": ((docs[m.doc_upload_id].filed_filename or docs[m.doc_upload_id].original_filename)
                            if m.doc_upload_id in docs else None),
                "receipt_vendor": (docs[m.doc_upload_id].vendor if m.doc_upload_id in docs else None),
                "flagged": bool(m.claim_conflict),
                "reason": m.claim_conflict_reason,
            } for m in ms],
        })
    return cards


def _create_index_if_clean():
    try:
        remaining = db.session.execute(text(DUP_GROUPS_SQL)).scalar()
        if remaining == 0:
            db.session.execute(text(COMPANY_INDEX_SQL))
            return True
    except Exception:
        db.session.rollback()
    return False


LOSER_MODES = ('receipt_to_place', 'receipt_stays_coded')


def resolve_keep(qbo_txn_id, qbo_txn_type, keep_project_id, user_id=None,
                 loser_mode='receipt_to_place'):
    """Keep <project>'s claim and settle the other project's row:

      receipt_to_place    (default) the loser loses its coding, its receipt
                          goes back to that project's Receipts to place as
                          a doc-only row, the loser row is logged + deleted
      receipt_stays_coded the loser was a WRONG reconcile — its receipt is
                          a different charge of the same amount. The loser
                          row keeps its project, its coding and its receipt
                          but drops the QuickBooks identity (source back to
                          doc_upload); logged as 'demoted' so undo puts the
                          identity back. A loser with no receipt is deleted
                          as in the default mode.

    The winner's flag is cleared either way."""
    from actuals import is_ledger_category
    if loser_mode not in LOSER_MODES:
        raise ValueError(f"loser_mode must be one of {LOSER_MODES}")
    ms = Transaction.query.filter_by(qbo_txn_id=qbo_txn_id, qbo_txn_type=qbo_txn_type).all()
    winners = [m for m in ms if m.project_id == keep_project_id]
    if not winners:
        raise ValueError("That project holds no row for this charge.")
    winner = sorted(winners, key=lambda m: m.id)[0]
    removed = []
    for m in ms:
        if m.id == winner.id:
            continue
        pid = m.project_id
        if loser_mode == 'receipt_stays_coded' and m.doc_upload_id:
            entry = snapshot(m, 'demoted: wrong reconcile, receipt stays coded as its own row', winner.id)
            m.qbo_txn_id = None
            m.qbo_txn_type = None
            m.qbo_account_id = None
            m.qbo_entity_ref = None
            m.source = 'doc_upload'
            m.activated_at = m.activated_at or datetime.utcnow()   # it is this project's expense
            m.match_status = 'confirmed' if (m.budget_line_id or m.account_code) else 'unmatched'
            m.claim_conflict = False
            m.claim_conflict_reason = None
            m.updated_at = datetime.utcnow()
            removed.append({"id": m.id, "project_id": pid, "merge_log_id": entry.id, "demoted": True})
            _log('update', m.id, pid, m.vendor,
                 f"Company ledger: {qbo_txn_type}/{qbo_txn_id} was a wrong reconcile here — "
                 f"receipt stays coded as its own row; QuickBooks identity removed (merge log #{entry.id})")
            continue
        # The loser's receipt goes back to its project as a receipt-born
        # row (the same restore unmatch_receipt does).
        if m.doc_upload_id and pid:
            doc = DocUpload.query.get(m.doc_upload_id)
            if doc is not None and is_ledger_category(doc.category):
                if not Transaction.query.filter_by(doc_upload_id=doc.id, source='doc_upload').first():
                    db.session.add(Transaction(
                        project_id=doc.project_id or pid, source='doc_upload', doc_upload_id=doc.id,
                        vendor=doc.vendor, amount=doc.amount,
                        txn_date=doc.doc_date.isoformat() if doc.doc_date else None,
                        is_expense=True, match_status='unmatched'))
        TransactionSplit.query.filter_by(transaction_id=m.id).delete(synchronize_session=False)
        _delete_evidence(m.id)
        entry, _moved = remove_twin(m, winner.id, f"conflict resolved: kept in project {keep_project_id}")
        removed.append({"id": entry.removed_txn_id, "project_id": pid, "merge_log_id": entry.id})
        _log('delete', entry.removed_txn_id, pid, m.vendor,
             f"Company ledger: {qbo_txn_type}/{qbo_txn_id} kept in "
             f"{(ProjectSheet.query.get(keep_project_id) or ProjectSheet(name='?')).name}; "
             f"this project's row removed (merge log #{entry.id})")
    winner.claim_conflict = False
    winner.claim_conflict_reason = None
    winner.updated_at = datetime.utcnow()
    db.session.flush()
    _create_index_if_clean()
    db.session.commit()
    _log('update', winner.id, winner.project_id, winner.vendor,
         f"Company ledger: kept {qbo_txn_type}/{qbo_txn_id} in this project")
    return {"ok": True, "kept_txn_id": winner.id, "removed": removed}


def _delete_evidence(tid):
    ExpenseEvidence.query.filter_by(transaction_id=tid).delete(synchronize_session=False)


def resolve_unmatch_receipt(txn_id, user_id=None):
    """Payee-differs case: the receipt goes back to Receipts to place, the
    charge keeps the QuickBooks payee and stays claimed only while it
    still has a line or section (else it is released to the pool)."""
    from actuals import unmatch_receipt
    t = Transaction.query.get(txn_id)
    if t is None:
        raise ValueError(f"transaction {txn_id} not found")
    pid = t.project_id
    reason = t.claim_conflict_reason or ''
    qbo_payee = qbo_payee_from_reason(reason) if reason.startswith(PAYEE_DIFFERS) else None
    receipt_vendor = t.vendor
    info = unmatch_receipt(t.id)          # clears the link, restores the receipt row, releases
    t = Transaction.query.get(txn_id)
    if t is not None:
        if qbo_payee:
            t.vendor = qbo_payee
        if t.source == 'reconciled':
            t.source = 'qbo_sync'
        t.claim_conflict = False
        t.claim_conflict_reason = None
        t.updated_at = datetime.utcnow()
        db.session.commit()
        _log('update', t.id, pid, t.vendor,
             f"Company ledger: receipt ('{receipt_vendor}') unmatched from QuickBooks "
             f"{t.qbo_txn_type}/{t.qbo_txn_id}; charge keeps the QuickBooks payee"
             + (" and stays here" if t.project_id else " and returned to the pool"))
    return {"ok": True, **info, "released": (t is not None and t.project_id is None)}


def release_row(txn_id, user_id=None):
    """Release to unclaimed + unmatch receipt, on ANY single claimed
    QuickBooks row: the row goes back to the pool (project_id NULL, no
    line / section / splits / exclusion, source qbo_sync) and its receipt
    or invoice goes back to its project's Receipts to place as a doc-only
    row. The pair is remembered as a rejected match so the auto-matcher
    does not re-suggest it. Snapshot first ('released …') so undo restores
    the row in place."""
    from actuals import is_ledger_category
    t = Transaction.query.get(txn_id)
    if t is None:
        raise ValueError(f"transaction {txn_id} not found")
    if not t.qbo_txn_id:
        raise ValueError("Only a QuickBooks row can be released to the pool.")
    pid = t.project_id
    entry = snapshot(t, 'released to unclaimed; receipt unmatched')
    doc_id = t.doc_upload_id
    restored_receipt = False
    if doc_id:
        doc = DocUpload.query.get(doc_id)
        if doc is not None and is_ledger_category(doc.category):
            if not Transaction.query.filter_by(doc_upload_id=doc_id, source='doc_upload').first():
                db.session.add(Transaction(
                    project_id=doc.project_id or pid, source='doc_upload', doc_upload_id=doc_id,
                    vendor=doc.vendor, amount=doc.amount,
                    txn_date=doc.doc_date.isoformat() if doc.doc_date else None,
                    card_last4=getattr(doc, 'card_last4', None),
                    is_expense=True, match_status='unmatched'))
                restored_receipt = True
        if not MatchRejection.query.filter_by(transaction_id=t.id, doc_upload_id=doc_id).first() and pid:
            db.session.add(MatchRejection(project_id=pid, transaction_id=t.id,
                                          doc_upload_id=doc_id, created_by=user_id))
    TransactionSplit.query.filter_by(transaction_id=t.id).delete(synchronize_session=False)
    _delete_evidence(t.id)
    Transaction.query.filter_by(backup_of_txn_id=t.id).update(
        {Transaction.backup_of_txn_id: None}, synchronize_session=False)
    t.project_id = None
    t.doc_upload_id = None
    t.budget_line_id = t.account_code = None
    t.account_code_name = None
    t.suggested_budget_line_id = None
    t.match_status = 'unmatched'
    t.match_confidence = None
    t.match_source = None
    t.not_project_expense = False
    t.excluded_at = t.excluded_by_user_id = t.excluded_reason = None
    t.activated_at = None
    t.split_group = None
    t.source = 'qbo_sync'
    t.claim_conflict = False
    t.claim_conflict_reason = None
    t.updated_at = datetime.utcnow()
    db.session.flush()
    _create_index_if_clean()
    db.session.commit()
    _log('release', t.id, pid, t.vendor,
         f"Company ledger: {t.qbo_txn_type}/{t.qbo_txn_id} released to unclaimed"
         + (f"; receipt #{doc_id} back to Receipts to place" if doc_id else '')
         + f" (merge log #{entry.id})")
    return {"ok": True, "transaction_id": t.id, "merge_log_id": entry.id,
            "restored_receipt": restored_receipt, "from_project_id": pid}


def resolve_clear(txn_id, user_id=None):
    """'It's fine' — clear the flag on one row. A two-projects pair keeps
    both rows flagged until Keep decides; this is for the payee case."""
    t = Transaction.query.get(txn_id)
    if t is None:
        raise ValueError(f"transaction {txn_id} not found")
    twins = Transaction.query.filter(Transaction.qbo_txn_id == t.qbo_txn_id,
                                     Transaction.qbo_txn_type == t.qbo_txn_type,
                                     Transaction.id != t.id).count()
    if twins:
        raise ValueError("This charge still has a row in another project — choose which project keeps it.")
    t.claim_conflict = False
    t.claim_conflict_reason = None
    t.updated_at = datetime.utcnow()
    db.session.flush()
    _create_index_if_clean()
    db.session.commit()
    _log('update', t.id, t.project_id, t.vendor,
         f"Company ledger: flag cleared on {t.qbo_txn_type}/{t.qbo_txn_id} (it's fine)")
    return {"ok": True, "transaction_id": t.id}


# ── Payee differs → "Same person" / "Different" (loan-outs, 2026-11-04) ───
# Owner, Nov 4: "Danielle Miller is Two Foot Shuffle LLC." A payee-differs
# flag whose two names are a person and their loan-out company is not a
# conflict: "Same person" links the loan-out (actuals.add_loanout — a
# PayeeLoanout row, never a QuickBooks write) and clears the flag;
# "Different" keeps the flag and stops asking. Both are click-to-confirm
# and logged. The ONE identity rule is actuals.same_payee.

def _payee_flagged(t):
    return bool(t is not None and t.claim_conflict
                and (t.claim_conflict_reason or '').startswith(PAYEE_DIFFERS))


def _qbo_vendor_name(ref):
    if not ref:
        return None
    try:
        v = QBOVendor.query.get(str(ref))
        return v.display_name if v is not None else None
    except Exception:
        return None


def payee_differs_info(t):
    """What the detail window / the conflict card shows for a payee-differs
    row: the two names, whether a person already said Different, and the
    "Same person" proposal (actuals.loanout_proposal) — or None when the
    row is not flagged for its payee."""
    from actuals import loanout_proposal
    from models import CrewMember
    if not _payee_flagged(t):
        return None
    reason = t.claim_conflict_reason or ''
    qbo_payee = qbo_payee_from_reason(reason) or _qbo_vendor_name(t.qbo_entity_ref)
    doc = DocUpload.query.get(t.doc_upload_id) if t.doc_upload_id else None
    receipt_payee = (doc.vendor if doc is not None and vendor_is_real(doc.vendor) else None) or t.vendor
    proposal = None
    try:
        proposal = loanout_proposal(qbo_payee, t.qbo_entity_ref, receipt_payee,
                                    people=CrewMember.query.filter(CrewMember.active.isnot(False)).all(),
                                    qbo_vendors=QBOVendor.query.all())
    except Exception as e:
        log.info("[ledger] loan-out proposal skipped for #%s: %s", t.id, e)
    return {"qbo_payee": qbo_payee, "receipt_payee": receipt_payee,
            "different_confirmed": DIFFERENT_CONFIRMED in reason,
            "proposal": proposal}


def recheck_payee_flags(txn_ids=None, user_id=None):
    """Clear every "receipt payee differs" flag whose names are now the same
    payee by actuals.same_payee (a loan-out was just linked). A row a
    person marked Different, or one that still has a twin in another
    project, is left alone. Each clear is logged. Does NOT commit.
    Returns the cleared row ids."""
    from actuals import same_payee
    q = Transaction.query.filter(Transaction.claim_conflict == True,      # noqa: E712
                                 Transaction.claim_conflict_reason.like(PAYEE_DIFFERS + '%'))
    if txn_ids:
        q = q.filter(Transaction.id.in_(list(txn_ids)))
    cleared = []
    for t in q.all():
        reason = t.claim_conflict_reason or ''
        if DIFFERENT_CONFIRMED in reason:
            continue
        qbo_payee = qbo_payee_from_reason(reason) or _qbo_vendor_name(t.qbo_entity_ref)
        doc = DocUpload.query.get(t.doc_upload_id) if t.doc_upload_id else None
        names = [n for n in (t.vendor, doc.vendor if doc is not None else None) if vendor_is_real(n)]
        if not qbo_payee or not any(same_payee(n, (qbo_payee, t.qbo_entity_ref)) for n in names):
            continue
        if t.qbo_txn_id and Transaction.query.filter(Transaction.qbo_txn_id == t.qbo_txn_id,
                                                     Transaction.qbo_txn_type == t.qbo_txn_type,
                                                     Transaction.id != t.id).count():
            continue
        t.claim_conflict = False
        t.claim_conflict_reason = None
        t.updated_at = datetime.utcnow()
        cleared.append(t.id)
        _log('update', t.id, t.project_id, t.vendor,
             f"Company ledger: payee-differs flag cleared on {t.qbo_txn_type}/{t.qbo_txn_id} — "
             f"'{names[0] if names else t.vendor}' and '{qbo_payee}' are the same payee (loan-out)",
             before={"claim_conflict": True, "claim_conflict_reason": reason},
             after={"claim_conflict": False})
    if cleared:
        db.session.flush()
        _create_index_if_clean()
    return cleared


def resolve_same_person(txn_id, user_id=None, person_id=None, company_name=None, qbo_vendor_id=None):
    """"Same person — Two Foot Shuffle LLC is Danielle Miller's loan-out".
    Links the loan-out (the proposal unless the caller names it), clears
    this row's flag and every other payee-differs flag the link now
    explains, and attributes the charge to the person. Commits. Raises
    ValueError when the row is not flagged for its payee or nothing names
    a person."""
    from actuals import add_loanout, LoanoutError, set_txn_payee
    from models import CrewMember
    t = Transaction.query.get(txn_id)
    if not _payee_flagged(t):
        raise ValueError("This charge is not flagged for a different payee.")
    info = payee_differs_info(t) or {}
    prop = dict(info.get("proposal") or {})
    if person_id:
        prop["person_id"] = int(person_id)
    if company_name:
        prop["company_name"] = company_name
    if qbo_vendor_id is not None:
        prop["qbo_vendor_id"] = qbo_vendor_id or None
    person = CrewMember.query.get(prop.get("person_id")) if prop.get("person_id") else None
    if person is None or not (prop.get("company_name") or '').strip():
        raise ValueError("Neither name is a person Payee yet. Add the loan-out on the person's Payee page.")
    try:
        lo, created, n_attr = add_loanout(person, prop["company_name"], prop.get("qbo_vendor_id"), user_id=user_id)
    except LoanoutError as e:
        raise ValueError(str(e))
    reason = t.claim_conflict_reason
    t.claim_conflict = False
    t.claim_conflict_reason = None
    t.updated_at = datetime.utcnow()
    set_txn_payee(t, person.id, 'alias', replace=('alias', 'qbo'))
    db.session.flush()
    others = recheck_payee_flags(user_id=user_id)
    _create_index_if_clean()
    _log('update', t.id, t.project_id, t.vendor,
         f"Same person: {lo.company_name} is {person.name}'s loan-out (linked from the payee-differs flag on "
         f"{t.qbo_txn_type}/{t.qbo_txn_id}); flag cleared"
         + (f"; {len(others)} other flag(s) cleared" if others else '')
         + (f"; {n_attr} charge(s) now attribute to {person.name}" if n_attr else ''),
         before={"claim_conflict": True, "claim_conflict_reason": reason},
         after={"claim_conflict": False, "loanout_id": lo.id})
    try:
        from app import _log_activity
        _log_activity(action='create' if created else 'update', entity_type='payee_loanout',
                      entity_id=lo.id, entity_label=f"{person.name} · via {lo.company_name}"[:80],
                      project_id=t.project_id,
                      note=f"Loan-out linked: {lo.company_name} → {person.name}"
                           + (f" (QuickBooks vendor {lo.qbo_vendor_id})" if lo.qbo_vendor_id else ''))
    except Exception:
        log.info("[ledger] loan-out activity log skipped", exc_info=True)
    db.session.commit()
    return {"ok": True, "transaction_id": t.id, "loanout_id": lo.id, "created": created,
            "person_id": person.id, "person_name": person.name, "company_name": lo.company_name,
            "cleared_other_ids": others, "attributed": n_attr}


def resolve_different(txn_id, user_id=None):
    """"Different" — the two names are different payees: the flag stays
    (settle it with Unmatch receipt / It's fine) and the "Same person?"
    offer is not shown again. Logged. Commits."""
    t = Transaction.query.get(txn_id)
    if not _payee_flagged(t):
        raise ValueError("This charge is not flagged for a different payee.")
    reason = t.claim_conflict_reason or ''
    if DIFFERENT_CONFIRMED not in reason:
        t.claim_conflict_reason = (reason[:200 - len(DIFFERENT_CONFIRMED) - 1] + ' ' + DIFFERENT_CONFIRMED)
        t.updated_at = datetime.utcnow()
        db.session.commit()
        _log('update', t.id, t.project_id, t.vendor,
             f"Payee differs on {t.qbo_txn_type}/{t.qbo_txn_id}: confirmed different payees — flag kept",
             before={"claim_conflict_reason": reason}, after={"claim_conflict_reason": t.claim_conflict_reason})
    return {"ok": True, "transaction_id": t.id, "flagged": True}
