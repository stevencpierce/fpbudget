"""budget.assumptions / exclusions / overall_comments — client-facing
version notes shown on the Assumptions tab, PDF, portal, present.json.

Revision ID: 0014_assumptions
Revises: 0013_discussion_note
Create Date: 2026-09-10
"""
from alembic import op
import sqlalchemy as sa

revision = "0014_assumptions"
down_revision = "0013_discussion_note"
branch_labels = None
depends_on = None

_COLS = ("assumptions", "exclusions", "overall_comments")


def upgrade():
    bind = op.get_bind()
    try:
        cols = {c["name"] for c in sa.inspect(bind).get_columns("budget")}
    except Exception:
        cols = set()
    for name in _COLS:
        if name not in cols:
            op.add_column("budget", sa.Column(name, sa.Text(), nullable=True))


def downgrade():
    for name in _COLS:
        op.drop_column("budget", name)
