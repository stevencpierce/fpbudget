"""budget_line.discussion_note — per-line discussion flag + talking point.

Revision ID: 0013_discussion_note
Revises: 0012_fee_disperse_amount
Create Date: 2026-09-10
"""
from alembic import op
import sqlalchemy as sa

revision = "0013_discussion_note"
down_revision = "0012_fee_disperse_amount"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        cols = {c["name"] for c in sa.inspect(bind).get_columns("budget_line")}
    except Exception:
        cols = set()
    if "discussion_note" in cols:
        return
    op.add_column("budget_line",
                  sa.Column("discussion_note", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("budget_line", "discussion_note")
