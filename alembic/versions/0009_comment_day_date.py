"""budget_comment.day_date — per-day schedule comment threads (line x date).

Revision ID: 0009_comment_day_date
Revises: 0008_api_token
Create Date: 2026-07-22
"""
from alembic import op
import sqlalchemy as sa

revision = "0009_comment_day_date"
down_revision = "0008_api_token"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = sa.inspect(bind).get_table_names()
    except Exception:
        tables = None
    if tables is not None and "budget_comment" not in tables:
        return
    try:
        cols = [c["name"] for c in sa.inspect(bind).get_columns("budget_comment")]
    except Exception:
        cols = []
    if "day_date" not in cols:
        op.add_column("budget_comment", sa.Column("day_date", sa.Date, nullable=True))


def downgrade():
    op.drop_column("budget_comment", "day_date")
