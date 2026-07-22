"""transaction.activated_at — Actualizing 2.0 A1 (documents vs expenses).

Revision ID: 0003_activated_at
Revises: 0002_analyzer_batch
Create Date: 2026-07-20
"""
from alembic import op
import sqlalchemy as sa

revision = "0003_activated_at"
down_revision = "0002_analyzer_batch"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = sa.inspect(bind).get_table_names()
    except Exception:
        tables = None   # offline --sql preview
    if tables is not None and "transaction" not in tables:
        return   # fresh install: create_all builds the column from the model
    try:
        cols = [c["name"] for c in sa.inspect(bind).get_columns("transaction")]
    except Exception:
        cols = []
    if "activated_at" not in cols:
        op.add_column("transaction",
                      sa.Column("activated_at", sa.DateTime, nullable=True))
    # Grandfather: doc-born rows that were ALREADY coded under the old model
    # count as activated (their coding was the fused create+code click).
    op.execute("UPDATE transaction SET activated_at = NOW() "
               "WHERE source = 'doc_upload' AND activated_at IS NULL "
               "AND (budget_line_id IS NOT NULL OR account_code IS NOT NULL)")


def downgrade():
    op.drop_column("transaction", "activated_at")
