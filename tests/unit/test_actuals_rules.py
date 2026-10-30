"""Actuals money rules (handoff 2026-10-30, Phase 0).

Real rows in an in-memory SQLite DB through the real models — no app.py, no
browser. Pins:
  • counts_as_spend(): the ONE "counts toward actuals" filter every rollup uses
  • link_transaction_to_line(): assigning a line clears a Not-project mark,
    but never breaks another project's cross-project claim

Run: pytest tests/unit  (wired into CI).
"""
import os
import sys
from decimal import Decimal

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

flask = pytest.importorskip("flask")
pytest.importorskip("flask_sqlalchemy")

from models import db, Budget, BudgetLine, DocUpload, ProjectSheet, Transaction  # noqa: E402
import actuals  # noqa: E402


@pytest.fixture()
def app():
    a = flask.Flask(__name__)
    a.config["SQLALCHEMY_DATABASE_URI"] = "sqlite://"
    db.init_app(a)
    with a.app_context():
        db.create_all()
        yield a
        db.session.remove()
        db.drop_all()


def _project(name="P"):
    p = ProjectSheet(name=name)
    db.session.add(p)
    db.session.flush()
    b = Budget(project_id=p.id, name=f"{name} Working", budget_mode="working",
               version_status="current", is_actual=False)
    db.session.add(b)
    db.session.flush()
    ln = BudgetLine(budget_id=b.id, account_code=2100, account_name="Camera",
                    description="Camera rental")
    db.session.add(ln)
    db.session.commit()
    return p, b, ln


def _txn(pid, **kw):
    base = dict(project_id=pid, amount=Decimal("100.00"), is_expense=True,
                vendor="Vendor", txn_date="2026-10-01", source="qbo_sync",
                not_project_expense=False)
    base.update(kw)
    t = Transaction(**base)
    db.session.add(t)
    db.session.commit()
    return t


def _spend_ids(pid):
    return {t.id for t in Transaction.query.filter(
        Transaction.project_id == pid, actuals.counts_as_spend()).all()}


# ── counts_as_spend ──────────────────────────────────────────────────────────

def test_plain_charge_counts(app):
    p, _, ln = _project()
    t = _txn(p.id, budget_line_id=ln.id)
    assert _spend_ids(p.id) == {t.id}


def test_not_project_is_excluded(app):
    p, _, ln = _project()
    _txn(p.id, budget_line_id=ln.id, not_project_expense=True)
    assert _spend_ids(p.id) == set()


def test_backup_row_is_excluded(app):
    p, _, ln = _project()
    target = _txn(p.id, budget_line_id=ln.id)
    _txn(p.id, budget_line_id=ln.id, backup_of_txn_id=target.id)
    assert _spend_ids(p.id) == {target.id}


@pytest.mark.parametrize("category,counts", [
    ("receipt", True), ("invoice", True),
    ("estimate", False), ("quote", False), ("purchase_order", False),
    ("contract", False),
])
def test_doc_category_decides(app, category, counts):
    p, _, ln = _project()
    d = DocUpload(project_id=p.id, category=category)
    db.session.add(d)
    db.session.commit()
    t = _txn(p.id, budget_line_id=ln.id, doc_upload_id=d.id)
    assert (t.id in _spend_ids(p.id)) is counts


def test_unactivated_legacy_doc_row_still_counts(app):
    """Production never ran Alembic 0003's activated_at grandfather backfill,
    so a coded legacy doc row with activated_at NULL must keep counting."""
    p, _, ln = _project()
    d = DocUpload(project_id=p.id, category="receipt")
    db.session.add(d)
    db.session.commit()
    t = _txn(p.id, budget_line_id=ln.id, doc_upload_id=d.id,
             source="doc_upload", activated_at=None)
    assert t.id in _spend_ids(p.id)


# ── link_transaction_to_line ─────────────────────────────────────────────────

def test_link_clears_not_project(app):
    p, _, ln = _project()
    t = _txn(p.id, not_project_expense=True)
    actuals.link_transaction_to_line(t.id, ln.id)
    t = db.session.get(Transaction, t.id)
    assert t.budget_line_id == ln.id
    assert t.not_project_expense is False
    assert t.id in _spend_ids(p.id)


def test_link_keeps_cross_project_claim(app):
    """not_project_expense also hides a row another project has claimed;
    assigning it here must not quietly double-count it."""
    other, _, _ = _project("Other")
    p, _, ln = _project("Mine")
    t = _txn(p.id, not_project_expense=True, claimed_by_project_id=other.id)
    actuals.link_transaction_to_line(t.id, ln.id)
    t = db.session.get(Transaction, t.id)
    assert t.not_project_expense is True
