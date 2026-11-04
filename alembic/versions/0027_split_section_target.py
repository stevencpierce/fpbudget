"""A split piece may target a SECTION (2026-11-04).

  transaction_split.account_code  INTEGER NULL

Owner, Nov 4: "split across lines should work exactly the same way as
assign … I can't assign it to a main section." A piece now names EITHER a
budget line (budget_line_id) OR a COA section with no line yet
(account_code) — exactly one of the two (actuals.validate_splits enforces
it). budget_line_id was already nullable (0020). Section pieces count in the
section's Actual like section-only coded charges
(actuals.split_section_sums / split_sums_by_section_code).

Idempotent: production may run the per-worker essential pass in app.py
before — or instead of — this revision (CLAUDE.md).

Revision ID: 0027_split_section_target
Revises: 0026_pool_dismissal
Create Date: 2026-11-04
"""
from alembic import op
import sqlalchemy as sa

revision = "0027_split_section_target"
down_revision = "0026_pool_dismissal"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        cols = {c["name"] for c in sa.inspect(bind).get_columns("transaction_split")}
    except Exception:
        cols = None   # offline --sql preview
    if cols is None or "account_code" not in cols:
        op.add_column("transaction_split", sa.Column("account_code", sa.Integer, nullable=True))


def downgrade():
    op.drop_column("transaction_split", "account_code")
