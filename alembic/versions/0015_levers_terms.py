"""budget_lever table + budget.payment_terms — Framework Present levers
and payment terms on the estimate.

Revision ID: 0015_levers_terms
Revises: 0014_assumptions
Create Date: 2026-09-10
"""
from alembic import op
import sqlalchemy as sa

revision = "0015_levers_terms"
down_revision = "0014_assumptions"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if "budget_lever" not in insp.get_table_names():
        op.create_table(
            "budget_lever",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("budget_id", sa.Integer,
                      sa.ForeignKey("budget.id", ondelete="CASCADE"), nullable=False),
            sa.Column("title", sa.String(200), nullable=False),
            sa.Column("scope", sa.Text, nullable=True),
            sa.Column("consequence", sa.Text, nullable=True),
            sa.Column("delta_lines", sa.Numeric(14, 2)),
            sa.Column("fee_applied", sa.Boolean, nullable=False, server_default=sa.true()),
            sa.Column("line_ids", sa.Text, nullable=True),
            sa.Column("status", sa.String(10), nullable=False, server_default="open"),
            sa.Column("decision_note", sa.String(500), nullable=True),
            sa.Column("decided_by", sa.String(200), nullable=True),
            sa.Column("decided_at", sa.DateTime, nullable=True),
            sa.Column("sort_order", sa.Integer, server_default="0"),
            sa.Column("created_at", sa.DateTime, nullable=True),
        )
    try:
        cols = {c["name"] for c in insp.get_columns("budget")}
    except Exception:
        cols = set()
    if "payment_terms" not in cols:
        op.add_column("budget", sa.Column("payment_terms", sa.String(300), nullable=True))


def downgrade():
    op.drop_table("budget_lever")
    op.drop_column("budget", "payment_terms")
