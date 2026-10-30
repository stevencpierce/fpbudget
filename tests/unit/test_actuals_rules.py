"""Actuals rules (ACTUALS_HANDOFF Phase 0, 2026-10-30).

Pure unit tests — no Flask, no DB, no network. Two rules are pinned:

  1. counts_as_spend — the ONE predicate every rollup filters on. Tested
     through row_counts_as_spend (its pure-Python twin) with SimpleNamespace
     rows, and the SQL form is compiled to Postgres text to prove the clause
     says the same thing.
  2. the signed-amount rule — an expense counts +amount, a credit / refund
     counts -amount (signed_amount_value; app._signed_amount is the SQL twin).

Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from datetime import datetime
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import (  # noqa: E402
    counts_as_spend, row_counts_as_spend, signed_amount_value,
)


def _txn(**kw):
    base = dict(id=1, source="qbo_sync", not_project_expense=False,
                activated_at=None, backup_of_txn_id=None, amount=100.0,
                is_expense=True)
    base.update(kw)
    return NS(**base)


# ── counts_as_spend: the four rules ────────────────────────────────────────

def test_bank_row_counts():
    for src in ("qbo_sync", "csv_import", "reconciled", "manual_entry", "invoice_split"):
        assert row_counts_as_spend(_txn(source=src)), src


def test_not_project_excluded():
    assert not row_counts_as_spend(_txn(not_project_expense=True))


def test_doc_born_row_only_counts_once_activated():
    # A receipt is evidence, not spend, until the user creates an expense from it.
    assert not row_counts_as_spend(_txn(source="doc_upload", activated_at=None))
    assert row_counts_as_spend(_txn(source="doc_upload", activated_at=datetime(2026, 10, 1)))


def test_backup_row_excluded():
    # A receipt absorbed as backup for another charge must not count twice.
    assert not row_counts_as_spend(_txn(backup_of_txn_id=42))


def test_split_parent_excluded_children_count():
    parents = {10}
    assert not row_counts_as_spend(_txn(id=10), parents)
    assert row_counts_as_spend(_txn(id=11, source="invoice_split"), parents)
    # No parent set given → parent rule not applied in the pure form (the SQL
    # form falls back to NOT EXISTS; see test_sql_clause_without_parent_set).
    assert row_counts_as_spend(_txn(id=10), None)


def test_rules_combine_with_and():
    # Every rule must hold at once.
    assert not row_counts_as_spend(_txn(source="doc_upload",
                                        activated_at=datetime(2026, 1, 1),
                                        backup_of_txn_id=3))
    assert not row_counts_as_spend(_txn(not_project_expense=True,
                                        source="qbo_sync"))


# ── counts_as_spend: the SQL form says the same thing ──────────────────────

def _compile(clause):
    from sqlalchemy.dialects import postgresql
    return str(clause.compile(dialect=postgresql.dialect(),
                              compile_kwargs={"literal_binds": True}))


def test_sql_clause_with_parent_set():
    sql = _compile(counts_as_spend({3, 7}))
    assert "transaction.not_project_expense = false" in sql
    assert "transaction.source != 'doc_upload' OR transaction.activated_at IS NOT NULL" in sql
    assert "transaction.backup_of_txn_id IS NULL" in sql
    assert "transaction.id NOT IN (3, 7)" in sql
    assert "EXISTS" not in sql


def test_sql_clause_without_parent_set():
    sql = _compile(counts_as_spend(None))
    assert "NOT (EXISTS" in sql
    assert "parent_transaction_id = transaction.id" in sql
    assert "source = 'invoice_split'" in sql


def test_sql_clause_with_empty_parent_set():
    # An empty set means "no parents on this project" — no NOT IN () and no
    # NOT EXISTS; the three cheap rules only.
    sql = _compile(counts_as_spend(set()))
    assert "NOT IN" not in sql
    assert "EXISTS" not in sql
    assert "backup_of_txn_id IS NULL" in sql


def test_sql_clause_is_one_expression():
    # Call sites splat it into .filter() beside project scoping; it must be a
    # single AND-able clause, not a tuple.
    from sqlalchemy.sql.elements import ClauseElement
    assert isinstance(counts_as_spend({1}), ClauseElement)


# ── the signed-amount rule ─────────────────────────────────────────────────

def test_expense_is_positive_credit_is_negative():
    assert signed_amount_value(100, True) == 100.0
    assert signed_amount_value(100, False) == -100.0


def test_refund_nets_against_its_line():
    rows = [_txn(amount=250.00, is_expense=True),
            _txn(amount=40.00, is_expense=False)]      # a refund on the same line
    total = sum(signed_amount_value(t.amount, t.is_expense) for t in rows
                if row_counts_as_spend(t))
    assert total == 210.0


def test_signed_amount_tolerates_bad_inputs():
    assert signed_amount_value(None, True) == 0.0
    assert signed_amount_value("12.50", False) == -12.5
    assert signed_amount_value("not a number", True) == 0.0


def test_app_signed_amount_sql_matches_rule():
    """app._signed_amount() is the SQL twin: CASE WHEN is_expense THEN amount
    ELSE -amount. Read the source text so this test stays DB-free (importing
    app boots Flask + its boot tasks)."""
    src = open(os.path.join(os.path.dirname(__file__), "..", "..", "app.py")).read()
    start = src.index("def _signed_amount():")
    body = src[start:start + 2500]
    assert "else_=-Transaction.amount" in body, \
        "credits must count NEGATIVE (owner decision 2026-10-30), not 0"
