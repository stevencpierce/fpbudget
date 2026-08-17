"""budget.internal_label + budget.locked — version naming & delete-lock.

Internal-only subtitle for a budget version (never client-facing) and a
lock flag that blocks deletion. (Owner 2026-07-22.)

Revision ID: 0006_budget_version_meta
Revises: 0005_estimate_share_role
Create Date: 2026-07-22
"""
from alembic import op
import sqlalchemy as sa

revision = "0006_budget_version_meta"
down_revision = "0005_estimate_share_role"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = sa.inspect(bind).get_table_names()
    except Exception:
        tables = None   # offline --sql preview
    if tables is not None and "budget" not in tables:
        return   # fresh install: create_all builds the columns from the model
    try:
        cols = [c["name"] for c in sa.inspect(bind).get_columns("budget")]
    except Exception:
        cols = []
    if "internal_label" not in cols:
        op.add_column("budget",
                      sa.Column("internal_label", sa.String(120), nullable=True))
    if "locked" not in cols:
        op.add_column("budget",
                      sa.Column("locked", sa.Boolean, nullable=False,
                                server_default="false"))


def downgrade():
    op.drop_column("budget", "locked")
    op.drop_column("budget", "internal_label")
