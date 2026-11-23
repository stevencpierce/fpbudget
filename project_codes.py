"""Project stage, project code and reference numbers — the ONE rules
(INTAKE_WRAP_HANDOFF Phase 1, 2026-11-23).

Stage sits BESIDE status (Steven, Nov 23): `ProjectSheet.stage` is
estimate | awarded | wrapping, NULL for every project from before stages
(reads as awarded, so nothing live stops receiving charges); wrapped stays
`status='wrapped'`. Read it only through `project_stage(p)`; "may this
project receive charges or documents" is `accepts_spend(p)`.

Project code: client initials + YYMM of production start (`GH-2609`), a
letter for a second project of that client in that month (`GH-2609B`).
Set at Award, then frozen (a super-admin may correct it until a reference
number exists). Recognise codes only against the real list
(`find_codes_in`), never a loose letters-digits pattern — GrubHub's own PO
was `WG-5465`.

Reference number: `<code>-<seq:04d>` (`GH-2609-0147`), one per expense,
given the first time the expense is coded (line, section or split) in a
project that has a code, never reused or renumbered. The ONE write is
`assign_ref_numbers(pid)`; a flush hook calls it for every project whose
transactions were touched, so every coding path is covered without each
one remembering to.
"""
import logging
import re
from datetime import date, datetime

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.orm import Session

from models import db, Budget, ProjectSheet, Transaction, TransactionSplit

log = logging.getLogger(__name__)

STAGES = ('estimate', 'awarded', 'wrapping', 'wrapped')
STAGE_LABELS = {'estimate': 'Estimate', 'awarded': 'Awarded',
                'wrapping': 'Wrapping', 'wrapped': 'Wrapped', 'archived': 'Archived'}
# Stages a person sets by hand; wrapped only comes from the wrap route.
SETTABLE_STAGES = ('estimate', 'awarded', 'wrapping')

ESTIMATE_MESSAGE = ("This project is still an Estimate — it cannot receive charges or "
                    "documents until it is awarded (Project setup → Award).")

CODE_RE = re.compile(r'^[A-Z]{1,6}-\d{4}[A-Z]?$')
_INITIALS_RE = re.compile(r'^[A-Z]{1,6}$')
_COMPANY_WORDS = {'LLC', 'INC', 'INC.', 'CO', 'CO.', 'CORP', 'LTD', 'THE', 'AND', '&',
                  'PRODUCTIONS', 'PRODUCTION', 'COMPANY'}


class StageError(ValueError):
    """A stage / code / award request that cannot be applied."""


# ── Stage ────────────────────────────────────────────────────────────────

def project_stage(p):
    """estimate | awarded | wrapping | wrapped | archived — the ONE rule."""
    if p is None:
        return None
    status = getattr(p, 'status', None) or 'active'
    if status in ('wrapped', 'archived'):
        return status
    stage = getattr(p, 'stage', None)
    return stage if stage in SETTABLE_STAGES else 'awarded'


def stage_label(p):
    return STAGE_LABELS.get(project_stage(p), '')


def accepts_spend(p):
    """May this project receive charges and documents? Only an Estimate
    may not (wrapped / archived keep today's behaviour until the Phase 2
    read-only lock)."""
    return p is not None and project_stage(p) != 'estimate'


def accepts_spend_id(pid):
    if not pid:
        return True
    return accepts_spend(db.session.get(ProjectSheet, int(pid)))


def set_stage(p, stage):
    """Move between estimate / awarded / wrapping. Awarded needs a code
    (use award()); Estimate is refused once the project holds money."""
    if stage not in SETTABLE_STAGES:
        raise StageError(f"Unknown stage: {stage}")
    cur = project_stage(p)
    if cur in ('wrapped', 'archived'):
        raise StageError(f"This project is {STAGE_LABELS[cur].lower()}; reactivate it first.")
    if stage in ('awarded', 'wrapping') and not p.project_code:
        raise StageError("Award the project first — it needs a project code.")
    if stage == 'estimate' and estimate_holdings(p.id)['total']:
        raise StageError("This project already holds charges or documents; "
                         "it cannot go back to Estimate.")
    p.stage = stage
    return p


# ── Project code ─────────────────────────────────────────────────────────

def suggest_initials(client_name, conn=None):
    """A first guess a person corrects: the initials already used for this
    client, else the capitals of a one-word name (GrubHub → GH) or the
    first letter of each word (Global Industrial → GI)."""
    name = (client_name or '').strip()
    if not name:
        return ''
    conn = conn or db.session.connection()
    prior = conn.execute(sa.text(
        "SELECT client_initials FROM project_sheet WHERE lower(client_name) = :n "
        "AND client_initials IS NOT NULL ORDER BY id DESC LIMIT 1"), {'n': name.lower()}).first()
    if prior and prior[0]:
        return prior[0]
    return initials_from_name(name)


def initials_from_name(name):
    words = [w for w in re.split(r'[\s,/]+', name or '') if w and w.upper() not in _COMPANY_WORDS]
    if not words:
        return ''
    if len(words) == 1:
        caps = ''.join(ch for ch in words[0] if ch.isupper())
        return (caps if len(caps) >= 2 else words[0][:2]).upper()[:6]
    return ''.join(w[0] for w in words if w[0].isalnum()).upper()[:6]


def normalize_initials(s):
    s = re.sub(r'[^A-Za-z]', '', s or '').upper()
    if not _INITIALS_RE.match(s):
        raise StageError("Client initials are 1–6 letters, e.g. GH.")
    return s


def code_base(initials, start_date):
    if not start_date:
        raise StageError("A production start date (month) is needed for the code.")
    return f"{normalize_initials(initials)}-{start_date.strftime('%y%m')}"


def next_free_code(base, exclude_pid=None, conn=None):
    """GH-2609 if free, else GH-2609B, GH-2609C …"""
    conn = conn or db.session.connection()
    taken = {r[0] for r in conn.execute(sa.text(
        "SELECT project_code FROM project_sheet WHERE project_code LIKE :b AND id <> :x"),
        {'b': base + '%', 'x': int(exclude_pid or 0)}) if r[0]}
    if base not in taken:
        return base
    for letter in 'BCDEFGHIJKLMNOPQRSTUVWXYZ':
        if base + letter not in taken:
            return base + letter
    raise StageError(f"Every code from {base} to {base}Z is taken.")


def validate_code(code, exclude_pid=None):
    code = (code or '').strip().upper()
    if not CODE_RE.match(code):
        raise StageError("A project code looks like GH-2609 or GH-2609B.")
    clash = (ProjectSheet.query
             .filter(ProjectSheet.project_code == code,
                     ProjectSheet.id != (exclude_pid or 0)).first())
    if clash:
        raise StageError(f"{code} is already used by {clash.name}.")
    return code


def find_codes_in(text):
    """The real project codes that appear in `text` (an email subject, an
    invoice) — matched against the list, never a pattern alone."""
    if not text:
        return []
    up = text.upper()
    codes = [c for (c,) in db.session.query(ProjectSheet.project_code)
             .filter(ProjectSheet.project_code.isnot(None)).all()]
    hits = []
    for c in sorted(codes, key=len, reverse=True):
        if re.search(r'(?<![A-Z0-9])' + re.escape(c) + r'(?![A-Z0-9])', up) and \
                not any(h.startswith(c) for h in hits):
            hits.append(c)
    return hits


def award(p, initials, start_date, end_date=None, code=None):
    """Estimate → Awarded: set initials, dates and the code (suggested
    from initials + start month unless one is given), then number the
    project's coded expenses. Does not commit. Returns the numbers given."""
    if project_stage(p) in ('wrapped', 'archived'):
        raise StageError("This project is not active.")
    if end_date and start_date and end_date < start_date:
        raise StageError("The end date is before the start date.")
    initials = normalize_initials(initials)
    if p.project_code:
        if code and code.strip().upper() != p.project_code:
            raise StageError(f"This project's code is already {p.project_code}.")
    else:
        p.project_code = (validate_code(code, p.id) if code
                          else next_free_code(code_base(initials, start_date), p.id))
    p.client_initials = initials
    p.start_date = start_date or p.start_date
    p.end_date = end_date or p.end_date
    if project_stage(p) == 'estimate' or p.stage is None:
        p.stage = 'awarded'
    p.awarded_at = p.awarded_at or datetime.utcnow()
    db.session.flush()
    return assign_ref_numbers(p.id)


def change_code(p, code):
    """Super-admin correction — only while no reference number exists,
    since the numbers carry the code into file names."""
    if p.ref_seq_last:
        raise StageError("Reference numbers already carry this code; it is frozen.")
    p.project_code = validate_code(code, p.id)
    return p


# ── Reference numbers ────────────────────────────────────────────────────

def ref_label(code, seq):
    return f"{code}-{int(seq):04d}" if code and seq else None


def ref_labels(txns):
    """{txn_id: 'GH-2609-0147'} for rows that have a number."""
    rows = [t for t in txns if getattr(t, 'ref_seq', None) and getattr(t, 'ref_project_id', None)]
    if not rows:
        return {}
    pids = {t.ref_project_id for t in rows}
    codes = dict(db.session.query(ProjectSheet.id, ProjectSheet.project_code)
                 .filter(ProjectSheet.id.in_(pids)).all())
    return {t.id: ref_label(codes.get(t.ref_project_id), t.ref_seq) for t in rows
            if codes.get(t.ref_project_id)}


def ref_for(txn):
    return ref_labels([txn]).get(getattr(txn, 'id', None))


# One SQL rule for "this row is an expense of project :pid that needs its
# number": coded here (line, section, split pieces, or legacy itemization
# children), real spend (not Not project, not an unconfirmed document row,
# not a receipt absorbed as backup, not itself a child piece, not flagged
# as claimed in two projects), and not already numbered by this project.
_ELIGIBLE_SQL = """
SELECT t.id FROM "transaction" t
 WHERE t.project_id = :pid
   AND (t.ref_seq IS NULL OR t.ref_project_id IS NULL OR t.ref_project_id <> :pid)
   AND t.parent_transaction_id IS NULL
   AND t.backup_of_txn_id IS NULL
   AND COALESCE(t.not_project_expense, :f) = :f
   AND COALESCE(t.claim_conflict, :f) = :f
   AND (t.source IS NULL OR t.source <> 'doc_upload' OR t.activated_at IS NOT NULL)
   AND (t.budget_line_id IS NOT NULL OR t.account_code IS NOT NULL
        OR EXISTS (SELECT 1 FROM transaction_split s WHERE s.transaction_id = t.id)
        OR EXISTS (SELECT 1 FROM "transaction" c WHERE c.parent_transaction_id = t.id))
 ORDER BY t.txn_date, t.id
"""


def eligible_count(pid, conn=None):
    conn = conn or db.session.connection()
    return len(conn.execute(sa.text(_ELIGIBLE_SQL), {'pid': int(pid), 'f': False}).fetchall())


def assign_ref_numbers(pid, conn=None):
    """Give every eligible expense of project `pid` its number, oldest
    first, continuing the project's sequence. No-op without a code or for
    an Estimate. Core SQL only (safe inside a flush hook); the project row
    is locked on Postgres so two workers never hand out the same number.
    Does not commit. Returns the count given."""
    conn = conn or db.session.connection()
    pg = conn.dialect.name == 'postgresql'
    row = conn.execute(sa.text(
        "SELECT project_code, stage, status, ref_seq_last FROM project_sheet WHERE id = :pid"
        + (" FOR UPDATE" if pg else "")), {'pid': int(pid)}).first()
    if not row or not row[0]:
        return 0
    if row[2] == 'active' and row[1] == 'estimate':
        return 0
    ids = [r[0] for r in conn.execute(sa.text(_ELIGIBLE_SQL), {'pid': int(pid), 'f': False})]
    if not ids:
        return 0
    seq = int(row[3] or 0)
    for tid in ids:
        seq += 1
        conn.execute(sa.text('UPDATE "transaction" SET ref_project_id = :pid, ref_seq = :seq '
                             'WHERE id = :tid'), {'pid': int(pid), 'seq': seq, 'tid': tid})
    conn.execute(sa.text("UPDATE project_sheet SET ref_seq_last = :seq WHERE id = :pid"),
                 {'seq': seq, 'pid': int(pid)})
    return len(ids)


# ── Automatic Award: a Working budget means the job is real ─────────────
# Steven, Nov 23: making a Working budget IS the award, so it must not be a
# second step. The first Working budget (new, or a budget switched to
# Working) awards the project: stage awarded, code from the client's
# initials + the production start month, then its coded expenses are
# numbered. When the initials or the start month cannot be worked out the
# project is still awarded (it takes charges) and Project setup asks for
# the code once (needs_code). Core SQL only — it runs inside a flush.

_SLUG_MONTH_RE = re.compile(r'^(\d{4})-(\d{2})_')


def _plan_code(conn, pid):
    """(initials, start_date, start_guessed, code) for a project, the code
    None when a person must supply initials or a start month. The start
    month is the project's start date, else a budget's, else — guessed,
    and said so on Project setup — the month in its Dropbox folder name
    (the month it was created)."""
    row = conn.execute(sa.text(
        "SELECT client_name, client_initials, start_date, dropbox_folder FROM project_sheet "
        "WHERE id = :pid"), {'pid': int(pid)}).first()
    client, initials, start, slug = row if row else (None, None, None, None)
    initials = initials or suggest_initials(client, conn) or ''
    guessed = False
    if not start:
        b = conn.execute(sa.text(
            "SELECT start_date FROM budget WHERE project_id = :pid AND start_date IS NOT NULL "
            "ORDER BY CASE WHEN budget_mode = 'working' THEN 0 ELSE 1 END, id DESC LIMIT 1"),
            {'pid': int(pid)}).first()
        start = b[0] if b else None
    if isinstance(start, str):
        try:
            start = datetime.strptime(start[:10], '%Y-%m-%d').date()
        except ValueError:
            start = None
    if not start and slug:
        m = _SLUG_MONTH_RE.match(slug)
        if m and 1 <= int(m.group(2)) <= 12:
            start, guessed = date(int(m.group(1)), int(m.group(2)), 1), True
    try:
        initials = normalize_initials(initials) if initials else ''
    except StageError:
        initials = ''
    if not initials or not start:
        return initials or None, start, guessed, None
    return initials, start, guessed, next_free_code(code_base(initials, start), pid, conn)


def auto_award(pid, conn=None):
    """Award project `pid` if it has no code yet. Returns {'code', 'numbered',
    'needs_code'} or None when there was nothing to do."""
    conn = conn or db.session.connection()
    row = conn.execute(sa.text(
        "SELECT id, name, client_name, client_initials, start_date, project_code, stage, status "
        "FROM project_sheet WHERE id = :pid"), {'pid': int(pid)}).first()
    if not row or (row[7] or 'active') == 'archived':
        return None
    if row[5]:
        if row[6] == 'estimate':
            conn.execute(sa.text("UPDATE project_sheet SET stage = 'awarded' WHERE id = :pid"),
                         {'pid': int(pid)})
        return None
    initials, start, guessed, code = _plan_code(conn, pid)
    stage = 'awarded' if (row[7] or 'active') == 'active' else row[6]
    conn.execute(sa.text(
        "UPDATE project_sheet SET stage = :stage, project_code = :code, client_initials = "
        "COALESCE(:ini, client_initials), start_date = COALESCE(start_date, :start), "
        "awarded_at = COALESCE(awarded_at, :now) WHERE id = :pid"),
        {'stage': stage, 'code': code, 'ini': initials, 'start': None if guessed else start,
         'now': datetime.utcnow(), 'pid': int(pid)})
    n = assign_ref_numbers(pid, conn) if code else 0
    return {'code': code, 'numbered': n, 'needs_code': code is None}


def needs_code(p):
    """Awarded (or later) but no code yet — Project setup asks for it. A
    project from before stages (stage NULL) is left to the Maintenance
    backfill rather than nagged on every page."""
    return (p is not None and not p.project_code and getattr(p, 'stage', None) is not None
            and project_stage(p) in ('awarded', 'wrapping'))


def _has_working(conn, pid):
    return conn.execute(sa.text(
        "SELECT 1 FROM budget WHERE project_id = :pid AND budget_mode = 'working' "
        "AND COALESCE(is_actual, :f) = :f LIMIT 1"), {'pid': int(pid), 'f': False}).first() is not None


def backfill_plan():
    """Existing projects, as Admin → Maintenance previews them: every
    active or wrapped project with a Working budget and no code gets one
    (earliest production start first, so it takes the plain code and a
    later one the letter); an active project with NO Working budget that
    holds no charges or documents becomes an Estimate. Nothing is written."""
    conn = db.session.connection()
    rows = conn.execute(sa.text(
        "SELECT id, name, client_name, client_initials, start_date, status, stage "
        "FROM project_sheet WHERE project_code IS NULL AND status IN ('active', 'wrapped')")).fetchall()
    award, estimate, manual = [], [], []
    for r in rows:
        if _has_working(conn, r[0]):
            initials, start, guessed, _ = _plan_code(conn, r[0])
            item = {'id': r[0], 'name': r[1], 'client': r[2], 'status': r[5],
                    'initials': initials, 'start': start, 'guessed': guessed,
                    'to_number': eligible_count(r[0], conn)}
            (award if (initials and start) else manual).append(item)
        elif r[5] == 'active' and r[6] != 'estimate' and not estimate_holdings(r[0])['total']:
            estimate.append({'id': r[0], 'name': r[1], 'client': r[2]})
    award.sort(key=lambda x: (x['start'], x['id']))
    # Codes in that order, each seeing the ones before it.
    planned = set()
    for it in award:
        it['code'] = next_free_code_after(code_base(it['initials'], it['start']), planned,
                                          it['id'], conn)
        planned.add(it['code'])
    return {'award': award, 'manual': manual, 'estimate': estimate}


def next_free_code_after(base, also_taken, exclude_pid, conn):
    taken = {r[0] for r in conn.execute(sa.text(
        "SELECT project_code FROM project_sheet WHERE project_code LIKE :b AND id <> :x"),
        {'b': base + '%', 'x': int(exclude_pid or 0)}) if r[0]} | set(also_taken)
    for cand in [base] + [base + c for c in 'BCDEFGHIJKLMNOPQRSTUVWXYZ']:
        if cand not in taken:
            return cand
    raise StageError(f"Every code from {base} to {base}Z is taken.")


def backfill_apply():
    """Apply backfill_plan(). Does not commit. Returns the plan with
    'numbered' on each awarded project."""
    plan = backfill_plan()
    conn = db.session.connection()
    for it in plan['award']:
        conn.execute(sa.text(
            "UPDATE project_sheet SET project_code = :code, client_initials = :ini, "
            "start_date = COALESCE(start_date, :start), awarded_at = COALESCE(awarded_at, :now), "
            "stage = CASE WHEN status = 'active' THEN 'awarded' ELSE stage END "
            "WHERE id = :pid AND project_code IS NULL"),
            {'code': it['code'], 'ini': it['initials'],
             'start': None if it['guessed'] else it['start'],
             'now': datetime.utcnow(), 'pid': it['id']})
        it['numbered'] = assign_ref_numbers(it['id'], conn)
    for it in plan['manual']:
        conn.execute(sa.text(
            "UPDATE project_sheet SET stage = 'awarded' WHERE id = :pid AND status = 'active' "
            "AND (stage IS NULL OR stage = 'estimate')"), {'pid': it['id']})
    for it in plan['estimate']:
        conn.execute(sa.text("UPDATE project_sheet SET stage = 'estimate' WHERE id = :pid "
                             "AND project_code IS NULL"), {'pid': it['id']})
    return plan


# ── Flush hook: number on first coding, whatever path coded it ───────────

_KEY = '_fp_ref_pids'


_AWARD_KEY = '_fp_award_pids'


def _note_pids(session, flush_context):
    pids = session.info.setdefault(_KEY, set())
    award = session.info.setdefault(_AWARD_KEY, set())
    for obj in list(session.new) + list(session.dirty):
        if isinstance(obj, (Transaction, TransactionSplit)) and obj.project_id:
            pids.add(int(obj.project_id))
        elif isinstance(obj, Budget) and obj.project_id and obj.budget_mode == 'working' \
                and not getattr(obj, 'is_actual', False):
            # A new Working budget, or a budget switched to Working.
            if obj in session.new or sa.inspect(obj).attrs.budget_mode.history.has_changes():
                award.add(int(obj.project_id))


def _number_noted(session, flush_context):
    award = session.info.pop(_AWARD_KEY, None) or set()
    pids = (session.info.pop(_KEY, None) or set()) - award
    if not pids and not award:
        return
    try:
        conn = session.connection()
    except Exception:
        return
    pg = conn.dialect.name == 'postgresql'
    for pid in sorted(award):
        try:
            if pg:
                with conn.begin_nested():
                    auto_award(pid, conn)
            else:
                auto_award(pid, conn)
        except Exception:
            log.warning("[refs] automatic award skipped for project %s", pid, exc_info=True)
    for pid in sorted(pids):
        try:
            if pg:
                with conn.begin_nested():
                    assign_ref_numbers(pid, conn)
            else:
                assign_ref_numbers(pid, conn)
        except Exception:
            # Fail open: numbering never blocks a save; the next touch of
            # the project (or the Award / setup page) numbers what is left.
            log.warning("[refs] numbering skipped for project %s", pid, exc_info=True)


def install_hooks():
    if getattr(install_hooks, '_done', False):
        return
    event.listen(Session, 'after_flush', _note_pids)
    event.listen(Session, 'after_flush_postexec', _number_noted)
    install_hooks._done = True


# ── Estimate projects that already hold money ────────────────────────────

def estimate_holdings(pid):
    """What an Estimate project holds that it should not: its own
    transactions and live documents."""
    from models import DocUpload
    txns = Transaction.query.filter(Transaction.project_id == int(pid)).count()
    docs = (DocUpload.query.filter(DocUpload.project_id == int(pid))
            .filter(sa.or_(DocUpload.status.is_(None), DocUpload.status != 'deleted')).count())
    return {'transactions': txns, 'documents': docs, 'total': txns + docs}


def estimate_projects_holding_money():
    out = []
    for p in (ProjectSheet.query.filter(ProjectSheet.stage == 'estimate',
                                        ProjectSheet.status == 'active')
              .order_by(ProjectSheet.name).all()):
        h = estimate_holdings(p.id)
        if h['total']:
            out.append({'project': p, **h})
    return out
