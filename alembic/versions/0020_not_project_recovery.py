"""Recoverable Not-project + payment method (handoff 2026-10-30, Phase 1).

transaction.excluded_at / excluded_by_user_id / excluded_reason make a user's
Not-project click auditable and recoverable (not_project_expense alone also
marks cross-project claims and absorbed backup receipts, so it can't tell
them apart). payment_method / paid_by_crew_member_id say how a manual or
receipt-born expense was paid. Also promotes the uq_transaction_qbo partial
unique index, which until now lived only in boot DDL.

Mirrored in app.py _web_worker_essential_columns() — production Postgres has
never run Alembic (CLAUDE.md, 2026-08-18).

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


_COLS = [
    ("excluded_at", sa.DateTime),
    ("excluded_by_user_id", sa.Integer),
    ("excluded_reason", sa.String(300)),
    ("payment_method", sa.String(20)),
    ("paid_by_crew_member_id", sa.Integer),
]


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    cols = {c["name"] for c in insp.get_columns("transaction")}
    for name, typ in _COLS:
        if name not in cols:
            op.add_column("transaction", sa.Column(name, typ, nullable=True))
    if bind.dialect.name == "postgresql":
        op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_transaction_qbo "
                   "ON transaction (project_id, qbo_txn_id, qbo_txn_type) "
                   "WHERE qbo_txn_id IS NOT NULL")


def downgrade():
    for name, _ in reversed(_COLS):
        op.drop_column("transaction", name)
