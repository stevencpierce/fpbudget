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


def get_current_estimated_budget(project_id):
    """Return the project's current Estimated budget, or None.
    Estimated = is_actual=False, parent_budget_id IS NULL (top of chain)."""
    return (Budget.query
            .filter_by(project_id=project_id, is_actual=False,
                       version_status='current',
                       parent_budget_id=None)
            .order_by(Budget.id.desc())
            .first())


def clone_estimated_to_working(estimated_budget):
    """Initialize a Working budget from an Estimated. Used when the user
    starts actualizing without first having created a Working budget —
    the system silently spins one up so the chain Estimated → Working →
    Actual is preserved.

    Same shape as clone_working_to_actual but produces a Working budget
    (is_actual=False, parent_budget_id=<estimated_id>). Lines get
    source_line_id pointing back at their Estimated peer.

    Returns the new Budget object (uncommitted; caller commits).
    """
    if not estimated_budget:
        raise ValueError("clone_estimated_to_working: estimated_budget is None")
    if estimated_budget.is_actual:
        raise ValueError("clone_estimated_to_working: source must be Estimated, not Actual")
    if estimated_budget.parent_budget_id is not None:
        raise ValueError("clone_estimated_to_working: source already has a parent (is itself a Working/Actual)")

    # Reuse a current Working if one already exists.
    existing = get_current_working_budget(estimated_budget.project_id)
    if existing:
        return existing

    skip_cols = {'id', 'created_at', 'updated_at', 'version_status',
                 'parent_budget_id', 'is_actual'}
    new_budget = Budget()
    for col in Budget.__table__.columns:
        if col.name in skip_cols:
            continue
        setattr(new_budget, col.name, getattr(estimated_budget, col.name))
    new_budget.is_actual        = False
    new_budget.version_status   = 'current'
    new_budget.parent_budget_id = estimated_budget.id
    base_name = (estimated_budget.name or 'Budget').strip()
    new_budget.name = f"{base_name} — Working"
    new_budget.created_at = datetime.utcnow()
    new_budget.updated_at = datetime.utcnow()
    new_budget.working_initialized_at = datetime.utcnow()
    db.session.add(new_budget)
    db.session.flush()

    line_map = {}
    src_lines = sorted(estimated_budget.lines, key=lambda l: (l.sort_order or 0, l.id))
    line_skip = {'id', 'budget_id', 'parent_line_id', 'source_line_id',
                 'orphan_from_working', 'crew_assignments', 'schedule_days',
                 'assigned_crew'}
    for src in src_lines:
        new_line = BudgetLine()
        for col in BudgetLine.__table__.columns:
            if col.name in line_skip:
                continue
            setattr(new_line, col.name, getattr(src, col.name))
        new_line.budget_id      = new_budget.id
        new_line.source_line_id = src.id
        new_line.orphan_from_working = False
        if src.parent_line_id and src.parent_line_id in line_map:
            new_line.parent_line_id = line_map[src.parent_line_id].id
        else:
            new_line.parent_line_id = None
        db.session.add(new_line)
        db.session.flush()
        line_map[src.id] = new_line
    for src in src_lines:
        if src.parent_line_id and src.parent_line_id in line_map:
            new_child = line_map[src.id]
            if not new_child.parent_line_id:
                new_child.parent_line_id = line_map[src.parent_line_id].id

    log.info(
        f"[actuals] Initialized Working budget #{new_budget.id} from "
        f"Estimated #{estimated_budget.id} ({len(line_map)} lines) for "
        f"project #{estimated_budget.project_id}"
    )
    return new_budget


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

    # Get / auto-init Working budget.
    working = get_current_working_budget(project_id)
    working_was_just_created = False
    if not working:
        est = get_current_estimated_budget(project_id)
        if est:
            working = clone_estimated_to_working(est)
            db.session.add(working)
            db.session.flush()  # get id
            working_was_just_created = True
        else:
            # No Estimated either — bail. Coding still works at the
            # transaction level; we just can't auto-add a line without
            # any budget shell to put it in. This is a brand-new
            # project edge case.
            return {'created': False, 'working_line_id': None,
                    'actual_line_id': None, 'working_was_just_created': False}

    # Already a line with this code? no-op.
    existing = (BudgetLine.query
                .filter_by(budget_id=working.id, account_code=code_int)
                .first())
    if existing:
        return {'created': False, 'working_line_id': existing.id,
                'actual_line_id': None,
                'working_was_just_created': working_was_just_created}

    # Insert placeholder. Sort order: large number so it lands at the
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
        created_at         = _dt.utcnow(),
    )
    db.session.add(placeholder)
    db.session.flush()  # get placeholder.id
    placeholder_actual_id = None

    # Mirror into Actual if it exists, with source_line_id pointing
    # at the new Working line. Without this back-pointer, the actuals
    # rollup query (which joins on source_line_id) couldn't surface
    # transactions linked to this line.
    actual = get_current_actual_budget(project_id)
    if actual:
        actual_line = BudgetLine(
            budget_id          = actual.id,
            account_code       = code_int,
            account_name       = (account_name or '')[:100],
            description        = '(Auto-added — no estimate)',
            is_labor           = False,
            quantity           = 1,
            days               = 1,
            rate               = 0,
            kit_fee            = 0,
            fringe_code        = 'N',
            agent_pct          = 0,
            rate_type          = 'flat',
            estimated_total    = 0,
            working_total      = 0,
            sort_order         = 99999,
            source_line_id     = placeholder.id,
            created_at         = _dt.utcnow(),
        )
        db.session.add(actual_line)
        db.session.flush()
        placeholder_actual_id = actual_line.id

    return {'created': True,
            'working_line_id': placeholder.id,
            'actual_line_id':  placeholder_actual_id,
            'working_was_just_created': working_was_just_created}


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
    # second link in this project) vs on Working / Estimated (first
    # link → triggers the auto-init / auto-clone chain).
    line_budget = Budget.query.get(working_line.budget_id)
    actual_was_just_made  = False
    working_was_just_made = False

    if line_budget and line_budget.is_actual:
        # Already on Actual — straight passthrough.
        actual_line = working_line
    else:
        # Picked from Working or Estimated. Make sure we have BOTH a
        # Working (the source-of-truth for the actuals clone) AND an
        # Actual (where transactions land).
        working_budget = get_current_working_budget(project_id)
        if not working_budget:
            # User actualized off Estimated without first creating a
            # Working budget. Auto-init Working from Estimated so the
            # chain Estimated → Working → Actual is preserved. Per
            # user 2026-04-30: "if somebody gets your estimated to
            # actual, we should go ahead and create + initialize a
            # working budget for them at that point."
            estimated = get_current_estimated_budget(project_id)
            # Fall back to the picked line's own budget if neither
            # working nor estimated exists in canonical form.
            source = estimated or line_budget
            if not source:
                raise ValueError(
                    f"project {project_id} has no budget to clone from"
                )
            working_budget = clone_estimated_to_working(source)
            working_was_just_made = True
            # If the user's picked line was on Estimated, translate to
            # the equivalent Working line we just created (so its
            # source_line_id chain is right when we clone to Actual).
            if line_budget and not line_budget.is_actual and line_budget.parent_budget_id is None:
                _peer = (BudgetLine.query
                         .filter_by(budget_id=working_budget.id,
                                    source_line_id=working_line.id)
                         .first())
                if _peer:
                    working_line = _peer

        actual = get_current_actual_budget(project_id)
        actual_was_just_made = actual is None
        if actual_was_just_made:
            actual = clone_working_to_actual(working_budget)
        actual_line = working_to_actual_line(working_line.id, actual.id)
        if not actual_line:
            actual_line = _materialize_missing_actual_line(working_line, actual.id)

    txn.budget_line_id    = actual_line.id
    txn.account_code      = actual_line.account_code
    txn.account_code_name = actual_line.account_name
    txn.match_status      = 'confirmed'
    txn.updated_at        = datetime.utcnow()
    if user_id:
        txn.created_via_user_id = user_id
    db.session.commit()

    return {
        'transaction_id':           txn.id,
        'budget_line_id':           actual_line.id,
        'actual_budget_id':         actual_line.budget_id,
        'actual_was_just_created':  actual_was_just_made,
        'working_was_just_created': working_was_just_made,
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
    db.session.commit()
    return {'transaction_id': txn.id}


# ── Auto-match (suggest, never silently confirm) ─────────────────────

def _vendor_similarity(a, b):
    """Crude fuzzy vendor matcher. Returns 0.0–1.0.

    No external dep: lowercases, strips punctuation, then computes:
      • exact match               → 1.0
      • token-set Jaccard         → ratio of shared words / union
      • substring containment     → 0.85 if one wholly contains the other
      • prefix match (≥4 chars)   → 0.7
    Anything below 0.45 is treated as no-match by the caller."""
    import re as _re
    if not a or not b:
        return 0.0
    norm = lambda s: _re.sub(r'[^a-z0-9 ]', '', s.lower()).strip()
    A, B = norm(a), norm(b)
    if not A or not B:
        return 0.0
    if A == B:
        return 1.0
    if A in B or B in A:
        return 0.85
    a_tok, b_tok = set(A.split()), set(B.split())
    if a_tok and b_tok:
        inter = a_tok & b_tok
        union = a_tok | b_tok
        if inter and union:
            jaccard = len(inter) / len(union)
            if jaccard >= 0.5:
                return jaccard
    if len(A) >= 4 and len(B) >= 4 and A[:4] == B[:4]:
        return 0.7
    return 0.0


def run_auto_match(project_id):
    """Find candidate doc-upload ↔ qbo-sync pairings within a project.

    Pairs an unmatched QBO Transaction with a DocUpload Transaction
    when:
      • amount within ±$0.01
      • txn_date within ±3 days
      • vendor similarity >= 0.45

    Writes match_status='suggested' + match_confidence + sets
    qbo_txn.doc_upload_id (TENTATIVELY — user confirms via the row's
    Confirm button to make it stick; Override clears it).

    Doesn't actually merge rows. The merge happens on confirm:
      1. The qbo_sync Transaction keeps its identity (it's the row of
         record from the bank).
      2. Its doc_upload_id is now permanent.
      3. The doc_upload Transaction (the receipt's auto-created row)
         is deleted, since the QBO txn is now the canonical record.

    Returns summary dict.
    """
    import datetime as _dt
    qbo_unmatched = (Transaction.query
                     .filter_by(project_id=project_id, source='qbo_sync',
                                match_status='unmatched',
                                doc_upload_id=None,
                                not_project_expense=False)
                     .all())
    doc_open = (Transaction.query
                .filter_by(project_id=project_id, source='doc_upload',
                           not_project_expense=False)
                .all())

    suggestions = 0
    inspected = 0
    for q in qbo_unmatched:
        if q.amount is None or not q.txn_date:
            continue
        try:
            q_dt = _dt.date.fromisoformat(q.txn_date[:10])
        except (TypeError, ValueError):
            continue
        best = None
        best_score = 0.0
        for d in doc_open:
            inspected += 1
            if d.amount is None or not d.txn_date:
                continue
            if abs(float(d.amount) - float(q.amount)) > 0.01:
                continue
            try:
                d_dt = _dt.date.fromisoformat(d.txn_date[:10])
            except (TypeError, ValueError):
                continue
            day_gap = abs((d_dt - q_dt).days)
            if day_gap > 3:
                continue
            vendor_score = _vendor_similarity(q.vendor, d.vendor)
            if vendor_score < 0.45:
                continue
            # Composite score: amount=1.0 (already gated), date proximity
            # (1.0 = same day, 0.7 at 3 days), vendor fuzzy.
            date_score = max(0.0, 1.0 - (day_gap / 10.0))
            score = (1.0 + date_score + vendor_score) / 3.0
            if score > best_score:
                best_score = score
                best = d
        if best:
            q.doc_upload_id    = best.doc_upload_id
            q.match_status     = 'suggested'
            q.match_confidence = round(best_score, 3)
            q.updated_at       = datetime.utcnow()
            suggestions += 1
            log.info(
                f"[actuals automatch] QBO txn #{q.id} ({q.vendor!r}, "
                f"${q.amount}) → doc upload #{best.doc_upload_id} via "
                f"DocTxn #{best.id}, score={best_score:.3f}"
            )
    db.session.commit()
    log.info(
        f"[actuals automatch] project #{project_id}: {suggestions} suggestions "
        f"from {inspected} pair inspections ({len(qbo_unmatched)} unmatched "
        f"QBO × {len(doc_open)} open docs)"
    )
    return {
        'suggestions':     suggestions,
        'inspected':       inspected,
        'qbo_unmatched':   len(qbo_unmatched),
        'doc_open':        len(doc_open),
    }


def confirm_match(qbo_transaction_id):
    """User confirms a suggested match. Merges the doc_upload txn into
    the qbo_sync txn — the QBO row keeps its identity (it's the bank
    record), the doc_upload row is deleted since it's now redundant."""
    q = Transaction.query.get(qbo_transaction_id)
    if not q:
        raise ValueError(f"transaction {qbo_transaction_id} not found")
    if q.match_status != 'suggested' or not q.doc_upload_id:
        raise ValueError("not in 'suggested' state with a doc_upload_id")
    # Find the doc_upload Transaction that backs the same DocUpload.
    sister = (Transaction.query
              .filter_by(doc_upload_id=q.doc_upload_id, source='doc_upload')
              .first())
    if sister and sister.id != q.id:
        # Promote the QBO txn's coding from the sister if it had any
        # (rare — sister was the placeholder; user might have coded it
        # before the match landed).
        if not q.budget_line_id and sister.budget_line_id:
            q.budget_line_id      = sister.budget_line_id
            q.account_code        = sister.account_code
            q.account_code_name   = sister.account_code_name
        db.session.delete(sister)
    q.match_status = 'confirmed'
    q.updated_at   = datetime.utcnow()
    db.session.commit()
    return {'transaction_id': q.id, 'merged_doc_txn': sister.id if sister else None}


def dismiss_suggestion(transaction_id):
    """User rejected the auto-matcher's suggestion. Clears the match
    pointers without deleting the suggestion log."""
    t = Transaction.query.get(transaction_id)
    if not t:
        raise ValueError(f"transaction {transaction_id} not found")
    t.doc_upload_id    = None
    t.match_status     = 'unmatched'
    t.match_confidence = None
    t.updated_at       = datetime.utcnow()
    db.session.commit()
    return {'transaction_id': t.id}


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
