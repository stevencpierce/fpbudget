"""Actuals module — the consumption layer of the three-legged stool.

  BudgetLine (planned)  ←  Transaction (actual)  →  DocUpload (backup)

This module owns:
  • Cloning Working → Actual on first transaction link
  • Finding the Actual equivalent of a Working line (and vice versa)
  • Linking transactions to budget lines (with auto-clone)
  • Auto-match scaffolding (suggest, never silently write)

Ingestion (pulling QBO bank txns into the Transaction table) lives
in `qbo_sync.py` — separate concern. This module is what happens
*after* a Transaction exists, regardless of source.
"""
import logging
from datetime import datetime
from sqlalchemy.orm import attributes

from models import db, Budget, BudgetLine, Transaction, ProjectSheet

log = logging.getLogger(__name__)


# ── Working → Actual cloning ──────────────────────────────────────────

def get_current_actual_budget(project_id):
    """Return the project's current Actual budget, or None."""
    return (Budget.query
            .filter_by(project_id=project_id, is_actual=True,
                       version_status='current')
            .first())


def get_current_working_budget(project_id):
    """Return the project's current Working budget. Working budgets are
    distinguished from Estimated by having a parent_budget_id set
    (their Estimated peer); is_actual=False for both Working and
    Estimated. Returns the most recent current Working, or None."""
    return (Budget.query
            .filter_by(project_id=project_id, is_actual=False,
                       version_status='current')
            .filter(Budget.parent_budget_id.isnot(None))
            .order_by(Budget.version_number.desc().nullslast(),
                      Budget.id.desc())
            .first())


def clone_working_to_actual(working_budget):
    """Create a new Actual budget by deep-copying a Working budget.

    Every BudgetLine on the Working budget gets a peer on the Actual
    budget with `source_line_id` pointing back at the Working line.
    The new Actual is marked `is_actual=True, version_status='current'`
    and linked back to the Working it was cloned from via
    `parent_budget_id`.

    Returns the new Budget object (not yet committed; caller commits).
    """
    if not working_budget:
        raise ValueError("clone_working_to_actual: working_budget is None")
    if working_budget.is_actual:
        raise ValueError("clone_working_to_actual: source is already an Actual")

    # Reuse a current Actual if one already exists — never clone twice.
    existing = get_current_actual_budget(working_budget.project_id)
    if existing:
        return existing

    # Copy every persisted column from the Working budget so the Actual
    # carries the same project metadata, fee settings, dates, etc.
    skip_cols = {'id', 'created_at', 'updated_at', 'version_status',
                 'parent_budget_id', 'is_actual'}
    new_budget = Budget()
    for col in Budget.__table__.columns:
        if col.name in skip_cols:
            continue
        setattr(new_budget, col.name, getattr(working_budget, col.name))
    new_budget.is_actual        = True
    new_budget.version_status   = 'current'
    new_budget.parent_budget_id = working_budget.id
    # Visible distinction in the budget list dropdown.
    base_name = (working_budget.name or 'Budget').strip()
    new_budget.name = f"{base_name} — Actual"
    new_budget.created_at = datetime.utcnow()
    new_budget.updated_at = datetime.utcnow()
    db.session.add(new_budget)
    db.session.flush()  # assign new_budget.id before we wire lines

    # Clone lines. We build a map old_id → new_line so child lines
    # (parent_line_id back-pointers within the same budget) translate.
    line_map = {}
    src_lines = sorted(working_budget.lines, key=lambda l: (l.sort_order or 0, l.id))
    line_skip = {'id', 'budget_id', 'parent_line_id', 'source_line_id',
                 'orphan_from_working', 'crew_assignments', 'schedule_days',
                 'assigned_crew'}
    for src in src_lines:
        new_line = BudgetLine()
        for col in BudgetLine.__table__.columns:
            if col.name in line_skip:
                continue
            setattr(new_line, col.name, getattr(src, col.name))
        new_line.budget_id            = new_budget.id
        new_line.source_line_id       = src.id
        new_line.orphan_from_working  = False
        # parent_line_id translation: if the source had a parent
        # (kit fee, child line, etc.), and that parent is already in
        # our map, point at the new parent. If parent is processed
        # later, we'll fix it in a second pass.
        if src.parent_line_id and src.parent_line_id in line_map:
            new_line.parent_line_id = line_map[src.parent_line_id].id
        else:
            new_line.parent_line_id = None
        db.session.add(new_line)
        db.session.flush()
        line_map[src.id] = new_line

    # Second pass: resolve any parent_line_id refs that pointed at a
    # source line we hadn't yet cloned when we first saw the child.
    for src in src_lines:
        if src.parent_line_id and src.parent_line_id in line_map:
            new_child = line_map[src.id]
            if not new_child.parent_line_id:
                new_child.parent_line_id = line_map[src.parent_line_id].id

    log.info(
        f"[actuals] Cloned Working budget #{working_budget.id} → Actual "
        f"#{new_budget.id} ({len(line_map)} lines) for project "
        f"#{working_budget.project_id}"
    )
    return new_budget


def working_to_actual_line(working_line_id, actual_budget_id=None):
    """Given a BudgetLine id from the Working budget, return the
    equivalent line from the project's current Actual budget.
    Returns None if no Actual exists yet, or no matching line is found
    (rare — only happens if the Actual was synced and the source line
    was deleted in Working post-clone)."""
    src = BudgetLine.query.get(working_line_id)
    if not src:
        return None
    if actual_budget_id is None:
        # Look up the project's current Actual via the Working line's
        # budget → project_id chain.
        working = Budget.query.get(src.budget_id)
        if not working:
            return None
        actual = get_current_actual_budget(working.project_id)
        if not actual:
            return None
        actual_budget_id = actual.id
    return (BudgetLine.query
            .filter_by(budget_id=actual_budget_id, source_line_id=working_line_id)
            .first())


# ── Transaction linking (the auto-clone trigger lives here) ──────────

def link_transaction_to_line(transaction_id, working_line_id, user_id=None):
    """Atomic: link a Transaction to a budget line.

    The user picks a line from the Working budget. If no Actual budget
    exists yet for this project, we clone Working → Actual first, then
    translate the picked Working line to its Actual equivalent and
    write `transaction.budget_line_id`.

    Returns dict: {
      'transaction_id': int,
      'budget_line_id': int (the Actual line),
      'actual_budget_id': int,
      'actual_was_just_created': bool,   # tells UI whether to flash "Actual budget started" toast
    }

    Raises ValueError on bad inputs.
    """
    txn = Transaction.query.get(transaction_id)
    if not txn:
        raise ValueError(f"transaction {transaction_id} not found")
    working_line = BudgetLine.query.get(working_line_id)
    if not working_line:
        raise ValueError(f"budget_line {working_line_id} not found")

    project_id = txn.project_id
    if not project_id:
        raise ValueError(f"transaction {transaction_id} has no project")

    # Detect whether the picked line is already on an Actual (user's
    # second link in this project) vs on Working (first link → triggers
    # the auto-clone).
    line_budget = Budget.query.get(working_line.budget_id)
    if line_budget and line_budget.is_actual:
        # User picked from the existing Actual. No clone needed.
        actual_line   = working_line
        was_just_made = False
    else:
        # User picked from Working. Auto-clone if needed.
        actual = get_current_actual_budget(project_id)
        was_just_made = actual is None
        if was_just_made:
            working_budget = get_current_working_budget(project_id)
            if not working_budget:
                # Edge case: user is linking to a line that's on
                # Estimated (not yet finalized as Working). Treat
                # that line's budget as the source.
                working_budget = line_budget
            actual = clone_working_to_actual(working_budget)
        actual_line = working_to_actual_line(working_line_id, actual.id)
        if not actual_line:
            # Line wasn't cloned (clone happened before this line was
            # added, or some structural drift). Fall back: create a
            # peer line on the Actual budget with source_line_id set.
            actual_line = _materialize_missing_actual_line(working_line, actual.id)

    txn.budget_line_id = actual_line.id
    txn.account_code      = actual_line.account_code
    txn.account_code_name = actual_line.account_name
    txn.match_status      = 'confirmed'
    txn.updated_at        = datetime.utcnow()
    if user_id:
        txn.created_via_user_id = user_id  # only sets if NULL? actually overwrite is fine — last-toucher
    db.session.commit()

    return {
        'transaction_id':         txn.id,
        'budget_line_id':         actual_line.id,
        'actual_budget_id':       actual_line.budget_id,
        'actual_was_just_created': was_just_made,
    }


def _materialize_missing_actual_line(working_line, actual_budget_id):
    """Create a peer Actual line for a Working line that wasn't part of
    the original clone. Used as a safety net for Working-→-Actual
    structural drift."""
    new_line = BudgetLine()
    skip = {'id', 'budget_id', 'parent_line_id', 'source_line_id',
            'orphan_from_working', 'crew_assignments', 'schedule_days',
            'assigned_crew'}
    for col in BudgetLine.__table__.columns:
        if col.name in skip:
            continue
        setattr(new_line, col.name, getattr(working_line, col.name))
    new_line.budget_id      = actual_budget_id
    new_line.source_line_id = working_line.id
    new_line.parent_line_id = None
    db.session.add(new_line)
    db.session.flush()
    log.info(
        f"[actuals] Materialized missing Actual line for Working "
        f"#{working_line.id} on Actual budget #{actual_budget_id}"
    )
    return new_line


def unlink_transaction(transaction_id):
    """Clear the budget_line_id on a transaction. Doesn't delete the
    Actual line even if it becomes empty — user may relink later."""
    txn = Transaction.query.get(transaction_id)
    if not txn:
        raise ValueError(f"transaction {transaction_id} not found")
    txn.budget_line_id = None
    txn.match_status   = 'unmatched'
    txn.updated_at     = datetime.utcnow()
    db.session.commit()
    return {'transaction_id': txn.id}


# ── Working → Actual sync (additive only — deletions become orphans) ──

def sync_working_to_actual(project_id, user_id=None):
    """After Actual exists, structural changes in Working don't
    automatically propagate. This pulls them on demand:

      • New lines in Working that have no Actual peer → create peer
      • Working lines renamed or recoded → update peer's name/code
        (does NOT touch peer's amount fields — Actual owns its own)
      • Working lines deleted (no peer source) → flip orphan_from_working
        on the Actual peer if it has linked transactions; otherwise
        delete the Actual peer too

    Returns summary dict.
    """
    actual = get_current_actual_budget(project_id)
    if not actual:
        return {'error': 'no_actual_budget'}
    working = get_current_working_budget(project_id)
    if not working:
        return {'error': 'no_working_budget'}

    summary = {'added': 0, 'updated': 0, 'orphaned': 0, 'deleted': 0}

    # Index the Actual lines by source_line_id for O(1) lookup.
    actual_by_src = {}
    for line in actual.lines:
        if line.source_line_id is not None:
            actual_by_src[line.source_line_id] = line

    working_ids = set()
    for w in working.lines:
        working_ids.add(w.id)
        peer = actual_by_src.get(w.id)
        if peer is None:
            _materialize_missing_actual_line(w, actual.id)
            summary['added'] += 1
        else:
            changed = False
            if peer.account_code != w.account_code:
                peer.account_code = w.account_code
                changed = True
            if peer.account_name != w.account_name:
                peer.account_name = w.account_name
                changed = True
            if peer.description != w.description:
                peer.description = w.description
                changed = True
            if peer.orphan_from_working:
                peer.orphan_from_working = False  # back in sync
                changed = True
            if changed:
                summary['updated'] += 1

    # Working lines deleted since the clone — find Actual lines whose
    # source_line_id no longer exists in Working.
    for src_id, peer in actual_by_src.items():
        if src_id in working_ids:
            continue
        has_txns = bool(
            Transaction.query.filter_by(budget_line_id=peer.id).first()
        )
        if has_txns:
            if not peer.orphan_from_working:
                peer.orphan_from_working = True
                summary['orphaned'] += 1
        else:
            db.session.delete(peer)
            summary['deleted'] += 1

    db.session.commit()
    log.info(f"[actuals] Sync W→A project #{project_id}: {summary}")
    return summary
