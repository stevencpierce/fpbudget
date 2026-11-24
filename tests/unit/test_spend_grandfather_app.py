"""Grandfather activated_at on legacy coded receipts (2026-11-24).

counts_as_spend() counts a doc-born row only once activated_at is set; the
backfill that stamped rows coded before that rule lived only in Alembic 0003,
which production has never run. Boots the real app on sqlite and pins:

  1. a coded, unactivated doc_upload row is $0 spend before and counts after,
     stamped with its own created_at
  2. an uncoded receipt stays unactivated (evidence, not spend); imported and
     already-activated rows are untouched
  3. the boot wrapper writes its marker and never runs a second time

Skipped automatically where app.py's runtime deps are not installed.
Run: python -m pytest tests/unit/test_spend_grandfather_app.py -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
import tempfile
from datetime import datetime

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

_DB_DIR = tempfile.mkdtemp(prefix="fpb-actgf-")
_PRESET = os.environ.get("DATABASE_URL", "")
if _PRESET and not _PRESET.startswith("sqlite:"):
    pytest.skip("DATABASE_URL is not sqlite; refusing to boot", allow_module_level=True)
os.environ.setdefault("DATABASE_URL", "sqlite:///" + os.path.join(_DB_DIR, "t.db"))
os.environ.setdefault("SECRET_KEY", "test")

try:
    _cwd = os.getcwd()
    os.chdir(ROOT)
    import app as fpapp                       # noqa: E402
    from models import db, ProjectSheet, Budget, BudgetLine, Transaction  # noqa: E402
    import actuals                            # noqa: E402
    from sqlalchemy import text               # noqa: E402
    _IMPORT_ERR = None
except Exception as _e:                       # pragma: no cover
    _IMPORT_ERR = _e
finally:
    try:
        os.chdir(_cwd)
    except Exception:
        pass

pytestmark = pytest.mark.skipif(_IMPORT_ERR is not None,
                                reason=f"app.py not importable here: {_IMPORT_ERR}")

OLD = datetime(2026, 6, 1, 12, 0, 0)


@pytest.fixture(scope="module")
def ctx():
    app = fpapp.app
    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        p = ProjectSheet(name="Grandfather activated_at", status="active")
        db.session.add(p)
        db.session.flush()
        b = Budget(project_id=p.id, name="GF Working", budget_mode="working",
                   version_status="current")
        db.session.add(b)
        db.session.flush()
        ln = BudgetLine(budget_id=b.id, account_code=2601, account_name="Camera",
                        description="Lens", quantity=1, rate_type="flat_day",
                        days=1, rate=100, sort_order=1)
        db.session.add(ln)
        db.session.commit()
        yield {"pid": p.id, "line": ln.id}


def _mk(pid, **kw):
    base = dict(project_id=pid, amount=100, is_expense=True, txn_date="2026-06-01",
                match_status='unmatched', not_project_expense=False)
    base.update(kw)
    t = Transaction(**base)
    db.session.add(t)
    db.session.commit()
    return t.id


def _spend_ids(pid):
    return {t.id for t in Transaction.query.filter(
        Transaction.project_id == pid, actuals.counts_as_spend(set())).all()}


def test_body_stamps_only_coded_receipts(ctx):
    pid, line = ctx["pid"], ctx["line"]
    with fpapp.app.app_context():
        legacy = _mk(pid, source='doc_upload', budget_line_id=line, created_at=OLD)
        section = _mk(pid, source='doc_upload', account_code=2600, created_at=OLD)
        uncoded = _mk(pid, source='doc_upload', created_at=OLD)
        imported = _mk(pid, source='qbo_sync', budget_line_id=line)
        done_at = datetime(2026, 8, 1)
        already = _mk(pid, source='doc_upload', budget_line_id=line, activated_at=done_at)

        before = _spend_ids(pid)
        assert legacy not in before and section not in before   # the $0 bug
        assert imported in before and already in before

        fpapp._grandfather_activated_at_body()
        db.session.expire_all()

        after = _spend_ids(pid)
        assert {legacy, section, imported, already} <= after
        assert uncoded not in after
        assert db.session.get(Transaction, legacy).activated_at == OLD
        assert db.session.get(Transaction, uncoded).activated_at is None
        assert db.session.get(Transaction, imported).activated_at is None
        assert db.session.get(Transaction, already).activated_at == done_at


def test_wrapper_runs_once(ctx):
    pid, line = ctx["pid"], ctx["line"]
    with fpapp.app.app_context():
        # At boot the test DB had no tables yet, so run the wrapper now.
        fpapp._grandfather_activated_at()
        row = db.session.execute(text(
            "SELECT last_result FROM system_task_log WHERE task_name = :t"),
            {"t": fpapp._ACTIVATED_AT_MARKER}).fetchone()
        assert row is not None
        late = _mk(pid, source='doc_upload', budget_line_id=line, created_at=OLD)
        fpapp._grandfather_activated_at()
        db.session.expire_all()
        assert db.session.get(Transaction, late).activated_at is None
