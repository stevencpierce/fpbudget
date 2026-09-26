"""budget.hidden_group_labels — per-budget list of derived dept-group
labels ("Camera", "Direction / AD") the user hid. The labels are
auto-rendered from lines' sub-groups, so without stored suppression
there was no way to remove one permanently.

Revision ID: 0016_hidden_group_labels
Revises: 0015_levers_terms
Create Date: 2026-09-26
"""
from alembic import op
import sqlalchemy as sa

revision = "0016_hidden_group_labels"
down_revision = "0015_levers_terms"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    cols = {c["name"] for c in insp.get_columns("budget")}
    if "hidden_group_labels" not in cols:
        op.add_column("budget", sa.Column("hidden_group_labels", sa.Text, nullable=True))


def downgrade():
    op.drop_column("budget", "hidden_group_labels")
