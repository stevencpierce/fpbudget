"""Per-project "Not project" on a company-pool row (2026-11-04).

  transaction_pool_dismissal  (id, transaction_id, project_id, reason,
                              created_at, created_by), UNIQUE
                              (transaction_id, project_id)

In the company ledger (0025) an unclaimed QuickBooks charge is visible to
every project. "Not project" in one project must hide it from THAT
project's Unclaimed view only — the charge stays in the pool for the rest.
A dismissal row records it; Restore deletes it. Read through
actuals.pool_filter, written by actuals.mark_not_project / undismiss and by
the ledger migration (every legacy not_project mark on a QuickBooks copy
becomes a dismissal for that copy's project).

Idempotent (inspector check first): production may run the per-worker
essential pass in app.py before — or instead of — this revision (CLAUDE.md).

Revision ID: 0026_pool_dismissal
Revises: 0025_company_ledger
Create Date: 2026-11-04
"""
from alembic import op
import sqlalchemy as sa

revision = "0026_pool_dismissal"
down_revision = "0025_company_ledger"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = set(sa.inspect(bind).get_table_names())
    except Exception:
        tables = None   # offline --sql preview
    if tables is None or "transaction_pool_dismissal" not in tables:
        op.create_table(
            "transaction_pool_dismissal",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("transaction_id", sa.Integer, nullable=False),
            sa.Column("project_id", sa.Integer, nullable=False),
            sa.Column("reason", sa.String(300), nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=True),
            sa.Column("created_by", sa.Integer, nullable=True),
        )
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_pool_dismissal "
               "ON transaction_pool_dismissal (transaction_id, project_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_transaction_pool_dismissal_project_id "
               "ON transaction_pool_dismissal (project_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_transaction_pool_dismissal_transaction_id "
               "ON transaction_pool_dismissal (transaction_id)")


def downgrade():
    op.drop_table("transaction_pool_dismissal")
