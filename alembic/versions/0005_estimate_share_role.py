"""estimate_share.role — CC recipients on client estimate sends.

'approver' (default, legacy NULL-equivalent) can approve / request changes;
'cc' gets a view-only link and can never respond. (Owner 2026-07-22.)

Revision ID: 0005_estimate_share_role
Revises: 0004_expense_evidence
Create Date: 2026-07-22
"""
from alembic import op
import sqlalchemy as sa

revision = "0005_estimate_share_role"
down_revision = "0004_expense_evidence"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = sa.inspect(bind).get_table_names()
    except Exception:
        tables = None   # offline --sql preview
    if tables is not None and "estimate_share" not in tables:
        return   # fresh install: create_all builds the column from the model
    try:
        cols = [c["name"] for c in sa.inspect(bind).get_columns("estimate_share")]
    except Exception:
        cols = []
    if "role" not in cols:
        op.add_column("estimate_share",
                      sa.Column("role", sa.String(12), nullable=False,
                                server_default="approver"))


def downgrade():
    op.drop_column("estimate_share", "role")
