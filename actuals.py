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
import re as _re
from datetime import datetime
from sqlalchemy.orm import attributes

from models import (db, Budget, BudgetLine, Transaction, ProjectSheet, DocUpload,
                    MatchRejection, CrewAssignment)

log = logging.getLogger(__name__)


# ── Learned vendor → COA mapping (2026-06-18 AI auto-coding) ──────────────
def canon_vendor(v):
    """Normalize a vendor string into the learned-mapping key: lowercase,
    drop store/terminal numbers + punctuation, collapse whitespace. Keeps
    'AMAZON MKTPLACE #1234' and 'amazon mktplace' on the same key."""
    s = (v or '').lower()
    s = _re.sub(r'[#*]+\s*\d+', ' ', s)        # store / terminal numbers
    s = _re.sub(r'\b\d{3,}\b', ' ', s)          # long digit runs
    s = _re.sub(r'[^a-z0-9 ]+', ' ', s)         # punctuation
    s = _re.sub(r'\s+', ' ', s).strip()
    return s[:200]


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


# ── Working → Actual cloning ──────────────────────────────────────────

def get_current_actual_budget(project_id):
    """Return the project's current Actual budget, or None."""
    return (Budget.query
            .filter_by(project_id=project_id, is_actual=True,
                       version_status='current')
            .first())


def get_current_working_budget(project_id):
    """Return the project's current Working budget, or None.

    AUDIT FIX (2026-07, CRITICAL-2): the old filter was `parent_budget_id IS
    NOT NULL` with no budget_mode check — but a v2+ ESTIMATED also carries
    parent_budget_id (pointing at its predecessor, app.py budget_new), so this
    could return an Estimated as the "Working" and actuals would clone/code
    against the wrong budget. budget_mode is the reliable discriminator
    ('working' from create_working_from_estimated / this module's clone)."""
    return (Budget.query
            .filter_by(project_id=project_id, is_actual=False,
                       version_status='current')
            .filter(Budget.budget_mode.in_(('working', 'actual')))
            .order_by(Budget.version_number.desc().nullslast(),
                      Budget.id.desc())
            .first())


def get_current_estimated_budget(project_id):
    """Return the project's current Estimated budget, or None.

    AUDIT FIX (2026-07, CRITICAL-3): the old filter required
    parent_budget_id IS NULL, but budget_new stamps parent_budget_id on every
    v2+ Estimated — so any project past v1 returned None here and the
    auto-actualization path seeded from nothing/stale v1. Resolve by
    budget_mode like the rest of app.py does."""
    return (Budget.query
            .filter_by(project_id=project_id, is_actual=False,
                       version_status='current')
            .filter(~Budget.budget_mode.in_(('working', 'actual')))
            .order_by(Budget.version_number.desc().nullslast(),
                      Budget.id.desc())
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
    # AUDIT FIX (2026-07): v2+ Estimated budgets legitimately carry
    # parent_budget_id (→ their predecessor Estimated), so the old
    # "parent must be NULL" guard broke auto-actualization on any project
    # past v1. Discriminate by budget_mode instead.
    if (estimated_budget.budget_mode or '') in ('working', 'actual'):
        raise ValueError("clone_estimated_to_working: source is a Working/Actual budget, not an Estimated")

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
    # AUDIT FIX (2026-07, MEDIUM-9): skip working_total/manual_actual so this
    # auto-clone starts with the same clean Working baseline as the UI path
    # (_copy_budget_lines) — the two engines previously diverged.
    line_skip = {'id', 'budget_id', 'parent_line_id', 'source_line_id',
                 'orphan_from_working', 'crew_assignments', 'schedule_days',
                 'assigned_crew', 'working_total', 'manual_actual'}
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

    # AUDIT FIX (2026-07, CRITICAL-1): this clone previously copied LINES ONLY —
    # no ScheduleDay/ProductionDay/TravelDetail and no CrewAssignments. The next
    # sync_schedule_driven_lines on the new Working found zero schedule rows and
    # silently zeroed every meal/per-diem/hotel/flight figure, and all people
    # assignments vanished. Copy crew here and reuse the UI path's schedule
    # copier (late import — app imports this module at load).
    id_map = {old_id: nl.id for old_id, nl in line_map.items()}
    try:
        src_line_ids = list(id_map.keys())
        if src_line_ids:
            for ca in CrewAssignment.query.filter(
                    CrewAssignment.budget_line_id.in_(src_line_ids)).all():
                db.session.add(CrewAssignment(
                    budget_line_id=id_map[ca.budget_line_id],
                    instance=ca.instance or 1,
                    crew_member_id=ca.crew_member_id,
                    name_override=ca.name_override,
                    rate_override=ca.rate_override,
                    fringe_override=ca.fringe_override,
                    agent_override=ca.agent_override,
                    omit_flags=ca.omit_flags,
                    role_number=ca.role_number,
                ))
    except Exception:
        log.exception("[actuals] crew-assignment copy on working-clone failed")
    try:
        from app import _copy_schedule_days
        _copy_schedule_days(estimated_budget.id, new_budget.id, id_map,
                            dest_mode='working')
    except Exception:
        log.exception("[actuals] schedule copy on working-clone failed")

    log.info(
        f"[actuals] Initialized Working budget #{new_budget.id} from "
        f"Estimated #{estimated_budget.id} ({len(line_map)} lines, crew + "
        f"schedule copied) for project #{estimated_budget.project_id}"
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
    # The row is now coded → drop any advisory AI suggestion, and reinforce the
    # learned vendor→category mapping so future uploads can short-circuit the
    # model. (2026-06-18 auto-coding.)
    txn.ai_suggested_code = None
    txn.ai_suggested_code_name = None
    txn.ai_code_confidence = None
    txn.ai_code_reason = None
    record_vendor_category(project_id, txn.vendor, actual_line.account_code)
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
                # Gate on MAGNITUDE so refunds match: a refund receipt is stored
                # negative while its bank credit is the same dollars, opposite
                # sign. (User 2026-06-17.)
                if abs(abs(float(d.amount)) - abs(float(q.amount))) > 0.01:
                    continue
                day_gap = abs((d_dt - q_dt).days)
                vendor_score = _vendor_similarity(q.vendor, d.vendor)
                if not accept(vendor_score, day_gap):
                    continue
                date_score = max(0.0, 1.0 - (day_gap / 10.0))
                # vendor_score still feeds the score, so vendor-less (Tier-2)
                # matches rank below real name matches in the review list.
                score = (1.0 + date_score + vendor_score) / 3.0
                # Prefer a same-sign pair when both exist (a normal expense),
                # but still allow the opposite-sign refund pairing to win when
                # it's the only magnitude match.
                if (float(d.amount) < 0) != (float(q.amount) < 0):
                    score *= 0.9
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
               .filter(Transaction.project_id == project_id,
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
        Transaction.project_id == project_id,
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
