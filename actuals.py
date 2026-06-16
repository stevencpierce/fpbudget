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

from models import (db, Budget, BudgetLine, Transaction, ProjectSheet, DocUpload,
                    MatchRejection)

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
            fringe_type        = 'N',
            agent_pct          = 0,
            rate_type          = 'flat_day',
            estimated_total    = 0,
            working_total      = 0,
            sort_order         = 99999,
            source_line_id     = placeholder.id,
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
    # Cross-project guard: a stale UI or crafted request must not code one
    # project's money to another project's budget line. (Code review 2026-06-04.)
    if line_budget and line_budget.project_id != project_id:
        raise ValueError(
            f"budget line {working_line_id} belongs to project "
            f"{line_budget.project_id}, not transaction {transaction_id}'s "
            f"project {project_id}")
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


def ensure_actual_mirrors(project_id, working_bid=None, actual_bid=None):
    """ADDITIVE-only guarantee that every current Working line has an Actual
    peer. Never updates or deletes — safe to call on every Actual-view load so a
    line added in Working immediately appears in Actual (and an actualized
    expense can be dragged onto it). The caller may pass the resolved working /
    actual budget ids (budget_view resolves them via _budget_type, which is the
    authoritative pick); otherwise we resolve here. Returns count created.
    (User 2026-06-02.)"""
    actual = (Budget.query.get(actual_bid) if actual_bid
              else get_current_actual_budget(project_id))
    working = (Budget.query.get(working_bid) if working_bid
               else (Budget.query
                     .filter_by(project_id=project_id, is_actual=False,
                                version_status='current', budget_mode='working')
                     .order_by(Budget.id.desc()).first()
                     or get_current_working_budget(project_id)))
    if not actual or not working:
        return 0
    have = {l.source_line_id for l in actual.lines if l.source_line_id is not None}
    added = 0
    for w in working.lines:
        if w.id not in have:
            _materialize_missing_actual_line(w, actual.id)
            added += 1
    if added:
        db.session.flush()
        # Second pass: connect child lines (kit fees etc.) to their parent's
        # mirror via the source chain.
        a_lines = BudgetLine.query.filter_by(budget_id=actual.id).all()
        a_by_src = {l.source_line_id: l for l in a_lines if l.source_line_id}
        w_by_id = {w.id: w for w in working.lines}
        for a in a_lines:
            if not a.source_line_id:
                continue
            w = w_by_id.get(a.source_line_id)
            np = (a_by_src.get(w.parent_line_id).id
                  if (w and w.parent_line_id and a_by_src.get(w.parent_line_id)) else None)
            if a.parent_line_id != np:
                a.parent_line_id = np
        db.session.commit()
        log.info(f"[actuals] ensure_actual_mirrors project={project_id}: +{added} mirror line(s)")
    return added


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
      • substring containment     → 0.85 if one wholly contains the other
      • token-set Jaccard (≥0.5)  → ratio of shared words / union
      • prefix match (≥4 chars)   → 0.7

    Card/bank descriptors prefix the real merchant with a payment processor —
    "SQ *CASA VIDEO", "IN *GOVISION LLC", "TST* TACO", "PAYPAL *SOUTHWESTAI".
    So we also try the merchant part AFTER the "*" and take the best score, so
    those land at full strength instead of being diluted by the prefix token.
    (User 2026-06-02.) Anything below 0.45 is treated as no-match by the caller."""
    import re as _re
    if not a or not b:
        return 0.0
    norm = lambda s: _re.sub(r'\s+', ' ', _re.sub(r'[^a-z0-9 ]', ' ', (s or '').lower())).strip()

    def _variants(raw):
        vs = set()
        base = norm(raw)
        if base:
            vs.add(base)
        if '*' in (raw or ''):                 # merchant after the processor mark
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
    # Electronic bank-feed rows that want a receipt: QBO pulls AND bank/
    # credit-card CSV imports. (Previously qbo_sync only, so CSV-imported
    # charges never got matched to receipts — user 2026-06-02.)
    qbo_unmatched = (Transaction.query
                     .filter(Transaction.project_id == project_id,
                             Transaction.source.in_(('qbo_sync', 'csv_import')),
                             Transaction.match_status == 'unmatched',
                             Transaction.doc_upload_id.is_(None),
                             Transaction.not_project_expense == False)
                     .all())
    doc_open = (Transaction.query
                .filter_by(project_id=project_id, source='doc_upload',
                           not_project_expense=False)
                .all())
    # Never match against receipts flagged as duplicates (pending review still
    # has a txn; confirmed ones are moved to /_DUPLICATES/). They stay out of
    # every list until "Keep" pulls them back. (User 2026-06-02.)
    _dup_doc_ids = {r[0] for r in db.session.query(DocUpload.id)
                    .filter(DocUpload.project_id == project_id,
                            db.or_(DocUpload.is_duplicate == True,        # noqa: E712
                                   DocUpload.status == 'duplicate',
                                   DocUpload.status == 'deleted')).all()}   # Trash (2026-06-11)
    if _dup_doc_ids:
        doc_open = [t for t in doc_open if t.doc_upload_id not in _dup_doc_ids]
    # Only receipts/invoices are valid spend proof — restrict the receipt pool
    # to them so estimates/employee-docs/etc. never get matched. (User 2026-06-02.)
    _proof_doc_ids = {r[0] for r in db.session.query(DocUpload.id)
                      .filter(DocUpload.project_id == project_id,
                              DocUpload.category.in_(('receipt', 'invoice'))).all()}
    doc_open = [t for t in doc_open if t.doc_upload_id in _proof_doc_ids]

    # Receipts already linked to an electronic txn (suggested OR confirmed)
    # are spoken for — never re-suggest one to a second charge.
    taken_docs = {t.doc_upload_id for t in Transaction.query.filter(
        Transaction.project_id == project_id,
        Transaction.source.in_(('qbo_sync', 'csv_import')),
        Transaction.doc_upload_id.isnot(None)).all() if t.doc_upload_id}

    # User-rejected pairs ("Not a match") — never propose these again.
    rejected_pairs = {(r.transaction_id, r.doc_upload_id) for r in
                      MatchRejection.query.filter_by(project_id=project_id).all()}

    # Pre-parse dates once.
    q_rows = []
    for q in qbo_unmatched:
        if q.amount is None or not q.txn_date:
            continue
        try:
            q_rows.append((q, _dt.date.fromisoformat(q.txn_date[:10])))
        except (TypeError, ValueError):
            continue
    d_rows = []
    for d in doc_open:
        if d.amount is None or not d.txn_date:
            continue
        try:
            d_rows.append((d, _dt.date.fromisoformat(d.txn_date[:10])))
        except (TypeError, ValueError):
            continue

    suggestions = 0
    inspected = 0

    def _pass(accept, label):
        """One matching sweep. `accept(vendor_score, day_gap)` decides
        eligibility; amount is always gated exact. Strong (vendor-similar)
        pass runs first so it claims receipts before the looser pass."""
        nonlocal suggestions, inspected
        for q, q_dt in q_rows:
            if q.match_status == 'suggested':     # already matched in pass 1
                continue
            best, best_score = None, 0.0
            for d, d_dt in d_rows:
                inspected += 1
                if d.doc_upload_id in taken_docs:
                    continue
                if (q.id, d.doc_upload_id) in rejected_pairs:
                    continue
                if abs(float(d.amount) - float(q.amount)) > 0.01:
                    continue
                day_gap = abs((d_dt - q_dt).days)
                vendor_score = _vendor_similarity(q.vendor, d.vendor)
                if not accept(vendor_score, day_gap):
                    continue
                date_score = max(0.0, 1.0 - (day_gap / 10.0))
                # vendor_score still feeds the score, so vendor-less (Tier-2)
                # matches rank below real name matches in the review list.
                score = (1.0 + date_score + vendor_score) / 3.0
                if score > best_score:
                    best_score, best = score, d
            if best:
                q.doc_upload_id    = best.doc_upload_id
                q.match_status     = 'suggested'
                q.match_confidence = round(best_score, 3)
                q.updated_at       = datetime.utcnow()
                taken_docs.add(best.doc_upload_id)
                suggestions += 1
                log.info(f"[actuals automatch:{label}] txn #{q.id} "
                         f"({q.vendor!r}, ${q.amount}) → doc upload "
                         f"#{best.doc_upload_id}, score={best_score:.3f}")

    # Pass 1 — strong: vendor-similar AND within 3 days.
    _pass(lambda v, g: v >= 0.45 and g <= 3, "strong")
    # Pass 2 — exact amount + within 5 days, vendor optional. Amount+date is a
    # solid signal for bank descriptors that don't resemble the receipt's
    # vendor name (e.g. "SQ *CASA VIDEO"); surfaced as a suggestion to confirm.
    # (User 2026-06-02.)
    _pass(lambda v, g: g <= 5, "amt+date")
    # Pass 2b — exact amount + SAME VENDOR, WIDE date window (≤60d). Travel and
    # subscriptions post weeks after the receipt date (book a flight 7/1, charged
    # 7/26), so a same-vendor exact-amount pair shouldn't be rejected just for a
    # date gap. The vendor gate keeps coincidental same-amounts apart ($20 SaaS
    # vs $20 FedEx). (User 2026-06-11 — "same amount, similar name, dates just
    # differ".)
    _pass(lambda v, g: v >= 0.50 and g <= 60, "amt+vendor-widedate")

    # Pass 3 — TIP-tolerant. A card charge routinely exceeds the receipt by a
    # tip (restaurants, rideshare): receipt $40.00 → card $48.00. Passes 1-2
    # gate amount to the exact penny and miss every one of these. Here the
    # charge must be the SAME vendor, within +30% of and not below the receipt,
    # and within 2 days. Lower confidence (amount isn't exact) so it sorts below
    # exact matches and reads as "eyeball me". (User 2026-06-11.)
    def _pass_tip():
        nonlocal suggestions, inspected
        for q, q_dt in q_rows:
            if q.match_status == 'suggested':
                continue
            best, best_score = None, 0.0
            for d, d_dt in d_rows:
                if d.doc_upload_id in taken_docs:
                    continue
                if (q.id, d.doc_upload_id) in rejected_pairs:
                    continue
                dv, qv = float(d.amount), float(q.amount)
                if dv <= 0:
                    continue
                ratio = qv / dv
                if ratio < 1.0 or ratio > 1.30:        # charge ≥ receipt, ≤ +30%
                    continue
                day_gap = abs((d_dt - q_dt).days)
                if day_gap > 2:
                    continue
                inspected += 1
                vendor_score = _vendor_similarity(q.vendor, d.vendor)
                if vendor_score < 0.45:                 # same vendor required here
                    continue
                tip_close = max(0.0, 1.0 - (ratio - 1.0) / 0.30)   # 1.0 exact → 0 at +30%
                # Confidence band ~0.45–0.62: clearly below exact matches.
                score = 0.45 + 0.10 * vendor_score + 0.07 * tip_close
                if score > best_score:
                    best_score, best = score, d
            if best:
                q.doc_upload_id    = best.doc_upload_id
                q.match_status     = 'suggested'
                q.match_confidence = round(best_score, 3)
                q.updated_at       = datetime.utcnow()
                taken_docs.add(best.doc_upload_id)
                suggestions += 1
                log.info(f"[actuals automatch:tip] txn #{q.id} ({q.vendor!r}, "
                         f"${q.amount}) → doc #{best.doc_upload_id} "
                         f"(receipt ${best.amount}), score={best_score:.3f}")
    _pass_tip()
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


def find_match_candidates(project_id, amount_tol=0.0, date_window=3,
                          use_vendor=False, limit_charges=300):
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
                     .filter(Transaction.project_id == project_id,
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
        Transaction.project_id == project_id,
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
            }

    # Build the receipt pool (amount, date) and sort by amount for bisect.
    d_rows = []
    for d in doc_open:
        did = d.doc_upload_id
        if (did is None or did in _dup_doc_ids or did not in _proof_doc_ids
                or did in taken_docs or d.amount is None or not d.txn_date):
            continue
        try:
            d_dt = _dt.date.fromisoformat(d.txn_date[:10])
        except (TypeError, ValueError):
            continue
        d_rows.append((float(d.amount), d_dt, d))
    d_rows.sort(key=lambda r: r[0])
    d_amounts = [r[0] for r in d_rows]

    def _confidence(amt_delta, day_gap, vendor_sim):
        amt_s = 1.0 if amt_delta < 0.01 else max(0.0, 1.0 - amt_delta / max(amount_tol, 1.0))
        if date_window is None:
            date_s = None
        else:
            date_s = 1.0 if day_gap == 0 else max(0.0, 1.0 - day_gap / max(float(date_window), 1.0))
        if use_vendor and date_s is not None:
            return 0.50 * amt_s + 0.25 * date_s + 0.25 * vendor_sim
        if use_vendor:
            return 0.65 * amt_s + 0.35 * vendor_sim
        if date_s is not None:
            return 0.65 * amt_s + 0.35 * date_s
        return amt_s

    results = []
    for q in qbo_unmatched:
        if q.amount is None or not q.txn_date:
            continue
        try:
            q_amt = float(q.amount)
            q_dt = _dt.date.fromisoformat(q.txn_date[:10])
        except (TypeError, ValueError):
            continue
        lo = _bisect.bisect_left(d_amounts, q_amt - amount_tol - 0.001)
        hi = _bisect.bisect_right(d_amounts, q_amt + amount_tol + 0.001)
        cands = []
        for idx in range(lo, hi):
            d_amt, d_dt, d = d_rows[idx]
            if (q.id, d.doc_upload_id) in rejected_pairs:
                continue
            day_gap = abs((d_dt - q_dt).days)
            if date_window is not None and day_gap > date_window:
                continue
            vendor_sim = _vendor_similarity(q.vendor, d.vendor)
            if use_vendor and vendor_sim < 0.30:
                continue
            conf = _confidence(abs(d_amt - q_amt), day_gap, vendor_sim)
            meta = doc_meta.get(d.doc_upload_id, {})
            cands.append({
                "doc_upload_id": d.doc_upload_id,
                "file": meta.get("file"), "is_image": meta.get("is_image", False),
                "vendor": d.vendor, "amount": d_amt, "date": d.txn_date[:10] if d.txn_date else None,
                "amount_delta": round(abs(d_amt - q_amt), 2),
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
        "criteria": {"amount_tol": amount_tol, "date_window": date_window, "use_vendor": use_vendor},
        "charges_with_candidates": len(results),
        "results": results[:limit_charges],
    }


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
        db.session.delete(sister)
    # Mark reconciled (not still 'qbo_sync') so the QBO-purge tool — which
    # deletes source='qbo_sync' rows — can never wipe a confirmed match. Mirrors
    # the sync-time and manual-merge reconcile paths; qbo_txn_id is preserved so
    # a future sync still dedupes against it. (Code review 2026-06-04.)
    if q.source in ('qbo_sync', 'csv_import'):
        q.source = 'reconciled'
    q.match_status = 'confirmed'
    q.updated_at   = datetime.utcnow()
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
    db.session.commit()
    return {'transaction_id': t.id, 'rejected_doc_upload_id': rejected_doc if remember else None}


_NON_LEDGER_DOC_TYPES = {'tax_form', 'contract', 'release', 'legal', 'insurance',
                         'misc', 'employee_vendor_doc', 'estimate', 'quote',
                         'purchase_order'}


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
    if doc_id:
        doc = DocUpload.query.get(doc_id)
        if doc and (doc.category or '') not in _NON_LEDGER_DOC_TYPES:
            has_sister = (Transaction.query
                          .filter_by(doc_upload_id=doc_id, source='doc_upload')
                          .first())
            if not has_sister:
                db.session.add(Transaction(
                    project_id=t.project_id, source='doc_upload', doc_upload_id=doc_id,
                    vendor=doc.vendor, amount=doc.amount,
                    txn_date=doc.doc_date.isoformat() if doc.doc_date else None,
                    is_expense=True, match_status='unmatched'))
                info["restored_receipt"] = True
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
