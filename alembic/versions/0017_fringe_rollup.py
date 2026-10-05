"""project_sheet.fringe_rollup — roll labor fringes up to one Payroll
Fringes amount in 6500 instead of folding them into each labor line's
total, so individual lines reconcile 1:1 against payments.

Revision ID: 0017_fringe_rollup
Revises: 0016_hidden_group_labels
Create Date: 2026-10-05
"""
from alembic import op
import sqlalchemy as sa

revision = "0017_fringe_rollup"
down_revision = "0016_hidden_group_labels"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    cols = {c["name"] for c in insp.get_columns("project_sheet")}
    if "fringe_rollup" not in cols:
        op.add_column("project_sheet",
                      sa.Column("fringe_rollup", sa.Boolean,
                                nullable=False, server_default=sa.false()))


def downgrade():
    op.drop_column("project_sheet", "fringe_rollup")
