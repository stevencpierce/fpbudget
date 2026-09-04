"""budget_line.fee_disperse_amount — per-line user-tuned dollar share of the
dispersed Production Company Fee (NULL = auto first pass).

Revision ID: 0012_fee_disperse_amount
Revises: 0011_uploader_nullable
Create Date: 2026-09-04
"""
from alembic import op
import sqlalchemy as sa

revision = "0012_fee_disperse_amount"
down_revision = "0011_uploader_nullable"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        cols = {c["name"] for c in sa.inspect(bind).get_columns("budget_line")}
    except Exception:
        cols = set()
    if "fee_disperse_amount" in cols:
        return
    op.add_column("budget_line",
                  sa.Column("fee_disperse_amount", sa.Numeric(12, 2), nullable=True))


def downgrade():
    op.drop_column("budget_line", "fee_disperse_amount")
