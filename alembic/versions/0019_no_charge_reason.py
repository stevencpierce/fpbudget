"""transaction.no_charge_reason — the REQUIRED stated reason when a
receipt is turned into a standalone expense with no electronic charge
behind it (owner 2026-10-30: "you should have to say this is a cash
transaction, or identify a different employee card, or give a reason").
Presence of the value doubles as the "missing electronic transaction"
flag in the Actuals register.

Revision ID: 0019_no_charge_reason
Revises: 0018_crew_dbx_path
Create Date: 2026-10-30
"""
from alembic import op
import sqlalchemy as sa

revision = "0019_no_charge_reason"
down_revision = "0018_crew_dbx_path"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    cols = {c["name"] for c in insp.get_columns("transaction")}
    if "no_charge_reason" not in cols:
        op.add_column("transaction",
                      sa.Column("no_charge_reason", sa.String(300), nullable=True))


def downgrade():
    op.drop_column("transaction", "no_charge_reason")
