"""Recoverable Not-project state, payment method on manual expenses, and
the transaction_split table (ACTUALS_HANDOFF Phase 1, 2026-10-30).

  transaction.excluded_at / excluded_by_user_id / excluded_reason
      Make "not a project expense" an auditable, recoverable state instead
      of a bare boolean. not_project_expense stays as the fast filter;
      these three say who excluded it, when and why so Recover can put it
      back with its history intact.
  transaction.payment_method / paid_by_crew_member_id
      A manual expense (cash, a personal card, a crew member's
      reimbursement) must say how it was paid so it can be reconciled.
      payment_method: company_card | cash | personal_card | check | ach | other.
  transaction_split
      Concur's Allocate: one transaction across several budget lines.
      Splits for one transaction must sum to its amount (enforced in the
      split route, not by the database).
  uq_transaction_qbo
      The partial unique index that makes QBO re-syncs idempotent lived
      only in boot DDL; promoted here so a fresh database gets it from
      Alembic too.

Every step is idempotent (inspector check first) because production may
run the per-worker essential pass in app.py before — or instead of — this
revision (see CLAUDE.md).

Revision ID: 0020_not_project_recovery
Revises: 0019_no_charge_reason
Create Date: 2026-10-30
"""
from alembic import op
import sqlalchemy as sa

revision = "0020_not_project_recovery"
down_revision = "0019_no_charge_reason"
branch_labels = None
depends_on = None


_TXN_COLUMNS = (
    ("excluded_at",            sa.Column("excluded_at", sa.DateTime, nullable=True)),
    ("excluded_by_user_id",    sa.Column("excluded_by_user_id", sa.Integer, nullable=True)),
    ("excluded_reason",        sa.Column("excluded_reason", sa.String(300), nullable=True)),
    ("payment_method",         sa.Column("payment_method", sa.String(20), nullable=True)),
    ("paid_by_crew_member_id", sa.Column("paid_by_crew_member_id", sa.Integer, nullable=True)),
)


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        tables = set(insp.get_table_names())
    except Exception:
        tables = None   # offline --sql preview

    if tables is None or "transaction" in tables:
        cols = {c["name"] for c in insp.get_columns("transaction")} if tables is not None else set()
        for name, col in _TXN_COLUMNS:
            if name not in cols:
                op.add_column("transaction", col)
        # Partial unique index — Postgres only (sqlite dev DBs skip it; the
        # boot DDL in app.py has always created it there as well).
        if bind.dialect.name == "postgresql":
            op.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_transaction_qbo "
                "ON transaction (project_id, qbo_txn_id, qbo_txn_type) "
                "WHERE qbo_txn_id IS NOT NULL")

    if tables is None or "transaction_split" not in tables:
        op.create_table(
            "transaction_split",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("transaction_id", sa.Integer,
                      sa.ForeignKey("transaction.id"), nullable=False),
            sa.Column("budget_line_id", sa.Integer,
                      sa.ForeignKey("budget_line.id"), nullable=True),
            sa.Column("project_id", sa.Integer, nullable=True),
            sa.Column("amount", sa.Numeric(12, 2), nullable=False),
            sa.Column("note", sa.String(300), nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=True),
            sa.Column("created_by_user_id", sa.Integer, nullable=True),
        )
        op.create_index("ix_transaction_split_transaction_id",
                        "transaction_split", ["transaction_id"])


def downgrade():
    op.drop_index("ix_transaction_split_transaction_id",
                  table_name="transaction_split")
    op.drop_table("transaction_split")
    for name, _col in reversed(_TXN_COLUMNS):
        op.drop_column("transaction", name)
    # uq_transaction_qbo is left in place: boot DDL owns it too.
