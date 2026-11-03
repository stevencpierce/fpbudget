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
    from actuals import vendor_match_key
    k = vendor_match_key(v or '')
    return set(k.replace('-', ' ').split()) if k else set()


def payees_differ(receipt_vendor, qbo_vendor):
    """True when both names are real and share no word — "Ryan Mauskopf"
    vs "Joel Knutson". "Lyft" vs "LYFT *RIDE" share a token → same."""
    if not (vendor_is_real(receipt_vendor) and vendor_is_real(qbo_vendor)):
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
    return {
        'line': bool(t.budget_line_id),
        'section': bool(t.account_code) and not t.budget_line_id,
        'split': t.id in split_parents,
        'receipt': bool(t.doc_upload_id),
        'not_project': bool(t.not_project_expense),
    }


def _qbo_payee_for_group(members, vendor_names):
    """The payee QuickBooks names for this charge: the mirrored vendor by
    EntityRef when any member carries one, else the vendor on a plain
    qbo_sync member (the receipt never touched that string)."""
    for m in members:
        ref = getattr(m, 'qbo_entity_ref', None)
        if ref and vendor_names.get(str(ref)):
            return vendor_names[str(ref)]
    for m in members:
        if m.source == 'qbo_sync' and vendor_is_real(m.vendor):
            return m.vendor
    return None


def _keep_pick(members):
    """0 claimed: a real vendor name over a blank one, then the newest id."""
    return sorted(members, key=lambda m: (0 if vendor_is_real(m.vendor) else 1, -m.id))[0]


def analyze(db_):
    """Group every QuickBooks row by identity and decide. Returns
    {groups, dup_groups, keep, delete, conflicts, payee_flags, decisions}
    where decisions is a list of per-group dicts (what / why), bounded for
    display, and the id lists are complete."""
    rows = (Transaction.query
            .filter(Transaction.qbo_txn_id.isnot(None))
            .order_by(Transaction.qbo_txn_id, Transaction.qbo_txn_type, Transaction.id)
            .all())
    groups = {}
    for t in rows:
        groups.setdefault((t.qbo_txn_id, t.qbo_txn_type), []).append(t)
    split_parents = _split_parent_ids([t.id for t in rows])
    try:
        vendor_names = {v.qbo_vendor_id: v.display_name for v in QBOVendor.query.all()}
    except Exception:
        vendor_names = {}
    pnames = {p.id: p.name for p in ProjectSheet.query.all()}

    keep_ids, delete_pairs, conflict_ids, payee_flags, decisions = [], [], [], [], []
    unclaim_ids = []
    dup_groups = 0
    for key, members in groups.items():
        claimed = [m for m in members if any(_signals(m, split_parents).values())]
        qbo_payee = _qbo_payee_for_group(members, vendor_names)
        if len(members) > 1:
            dup_groups += 1
        if len(claimed) == 0:
            keep = _keep_pick(members)
            keep_ids.append(keep.id)
            if keep.project_id is not None:
                unclaim_ids.append(keep.id)
            losers = [m for m in members if m.id != keep.id]
            delete_pairs.extend((m.id, keep.id, 'unclaimed twin') for m in losers)
            what = 'keep one, release to the pool'
            kept_list = [keep]
        elif len(claimed) == 1:
            keep = claimed[0]
            keep_ids.append(keep.id)
            losers = [m for m in members if m.id != keep.id]
            delete_pairs.extend((m.id, keep.id, 'twin of a claimed row') for m in losers)
            what = 'keep the claimed row'
            kept_list = [keep]
            # The Joel / Ryan case: a receipt row matched to a payment
            # QuickBooks says went to somebody else.
            if keep.doc_upload_id and qbo_payee and payees_differ(keep.vendor, qbo_payee):
                payee_flags.append((keep.id, qbo_payee))
        else:
            projects = {m.project_id for m in claimed}
            if len(projects) == 1:
                # Two claimed copies in the SAME project (a doc row and a
                # qbo row that both carry the id, say) — keep the one with
                # the most signals; nothing to review across projects.
                claimed.sort(key=lambda m: (-sum(_signals(m, split_parents).values()), m.id))
                keep = claimed[0]
                keep_ids.append(keep.id)
                losers = [m for m in members if m.id != keep.id]
                delete_pairs.extend((m.id, keep.id, 'twin of a claimed row (same project)') for m in losers)
                what = 'keep the fuller claimed row (same project)'
                kept_list = [keep]
                if keep.doc_upload_id and qbo_payee and payees_differ(keep.vendor, qbo_payee):
                    payee_flags.append((keep.id, qbo_payee))
            else:
                kept_list = list(claimed)
                keep_ids.extend(m.id for m in claimed)
                conflict_ids.extend(m.id for m in claimed)
                losers = [m for m in members if m not in claimed]
                delete_pairs.extend((m.id, claimed[0].id, 'unclaimed twin of conflicting rows') for m in losers)
                what = 'claimed by two projects — keep all, flag'
        if len(members) > 1 or len(claimed) > 1 or any(f[0] in {k.id for k in kept_list} for f in payee_flags):
            decisions.append({
                "qbo_txn_type": key[1], "qbo_txn_id": key[0],
                "amount": float(members[0].amount or 0), "date": members[0].txn_date,
                "what": what,
                "qbo_payee": qbo_payee,
                "rows": [{"id": m.id, "project_id": m.project_id,
                          "project": pnames.get(m.project_id, '—' if m.project_id is None else f'#{m.project_id}'),
                          "vendor": m.vendor, "source": m.source,
                          "signals": [k for k, v in _signals(m, split_parents).items() if v],
                          "keep": m in kept_list,
                          "conflict": (len(kept_list) > 1 and m in kept_list),
                          "payee_differs": any(f[0] == m.id for f in payee_flags)}
                         for m in members],
            })
    return {"rows": len(rows), "groups": len(groups), "dup_groups": dup_groups,
            "keep": keep_ids, "unclaim": unclaim_ids, "delete": delete_pairs,
            "conflicts": conflict_ids, "payee_flags": payee_flags,
            "decisions": decisions}


def preflight(db_=None):
    """The report the Maintenance page shows before the run. Counts first,
    then the decisions (every group with more than one row, every
    conflict, every payee flag). Read-only."""
    a = analyze(db)
    counts = {"qbo_rows": a["rows"], "groups": a["groups"],
              "groups_with_copies": a["dup_groups"],
              "to_delete": len(a["delete"]),
              "to_release_to_pool": len(a["unclaim"]),
              "conflicts_two_projects": len(a["conflicts"]),
              "payee_differs": len(a["payee_flags"])}
    log.info("[ledger preflight] %s", counts)
    done = marker_done()
    return {"ok": True, "already_run": done, "counts": counts,
            "decisions": a["decisions"][:400],
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


def run_migration(user_id=None):
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
        a = analyze(db)
        pnames = {p.id: p.name for p in ProjectSheet.query.all()}
        by_id = {t.id: t for t in Transaction.query.filter(Transaction.qbo_txn_id.isnot(None)).all()}
        deleted, released, flagged, moved_total = 0, 0, 0, {}
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
        accounts = seed_company_accounts()
        db.session.flush()
        remaining = db.session.execute(text(DUP_GROUPS_SQL)).scalar()
        index_created = False
        if remaining == 0:
            db.session.execute(text("DROP INDEX IF EXISTS uq_transaction_qbo"))
            db.session.execute(text(COMPANY_INDEX_SQL))
            index_created = True
        summary = (f"groups {a['groups']}, copies {a['dup_groups']}; deleted {deleted}, "
                   f"released {released}, flagged {flagged}; accounts {len(accounts)}; "
                   f"index {'created' if index_created else 'NOT created — ' + str(remaining) + ' duplicate groups remain'}")
        db.session.execute(text(
            "INSERT INTO system_task_log (task_name, last_run_at, last_result) "
            "VALUES (:t, CURRENT_TIMESTAMP, :r) ON CONFLICT (task_name) DO UPDATE "
            "SET last_run_at = EXCLUDED.last_run_at, last_result = EXCLUDED.last_result"),
            {'t': MARKER, 'r': summary[:2000]})
        db.session.commit()
        log.info("[ledger migration] %s", summary)
        return {"ok": True, "deleted": deleted, "released": released, "flagged": flagged,
                "moved": moved_total, "accounts": accounts, "index_created": index_created,
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
        qbo_payee = _qbo_payee_for_group(ms, vendor_names)
        reasons = {m.claim_conflict_reason for m in ms if m.claim_conflict_reason}
        if qbo_payee is None:
            # The migration wrote the QuickBooks name into the reason …
            for r in reasons:
                if (r or '').startswith(PAYEE_DIFFERS) and '(' in r:
                    qbo_payee = r[r.index('(') + 1:].rstrip(')').strip() or None
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
        cards.append({
            "qbo_txn_id": key[0], "qbo_txn_type": key[1],
            "kind": kind, "reasons": sorted(reasons),
            "amount": float(first.amount or 0), "is_expense": bool(first.is_expense),
            "date": first.txn_date, "account": first.account_code_name if not first.account_code else None,
            "card_last4": first.card_last4,
            "qbo_payee": qbo_payee,
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
    qbo_payee = None
    if reason.startswith(PAYEE_DIFFERS) and '(' in reason:
        qbo_payee = reason[reason.index('(') + 1:].rstrip(')').strip() or None
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
