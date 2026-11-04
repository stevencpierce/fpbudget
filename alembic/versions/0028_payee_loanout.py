"""Loan-out companies on a person Payee (2026-11-04).

  payee_loanout  (id, payee_id → crew_member, company_name,
                  vendor_match_key, qbo_vendor_id NULL, created_at,
                  created_by), UNIQUE (payee_id, vendor_match_key)

Owner, Nov 4: "Danielle Miller is Two Foot Shuffle LLC. So we need to find a
way to match these loan-outs with the people, because we want the
individuals, but then we still need the company attached somehow." One row
per company a person is paid through; qbo_vendor_id optionally names the
company's QuickBooks vendor from the mirror (never created or edited in
QuickBooks from FPBudget). Read through actuals.loanout_index /
actuals.same_payee — the ONE identity rule for the ledger's payee-differs
flag, the matching engine's vendor score, the duplicate twin check, and
document / QuickBooks payee attribution.

Idempotent (inspector check first): production may run the per-worker
essential pass in app.py before — or instead of — this revision (CLAUDE.md).

Revision ID: 0028_payee_loanout
Revises: 0027_split_section_target
Create Date: 2026-11-04
"""
from alembic import op
import sqlalchemy as sa

revision = "0028_payee_loanout"
down_revision = "0027_split_section_target"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = set(sa.inspect(bind).get_table_names())
    except Exception:
        tables = None   # offline --sql preview
    if tables is None or "payee_loanout" not in tables:
        op.create_table(
            "payee_loanout",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("payee_id", sa.Integer, sa.ForeignKey("crew_member.id"), nullable=False),
            sa.Column("company_name", sa.String(200), nullable=False),
            sa.Column("vendor_match_key", sa.String(200), nullable=False),
            sa.Column("qbo_vendor_id", sa.String(40), nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=True),
            sa.Column("created_by", sa.Integer, nullable=True),
        )
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_payee_loanout "
               "ON payee_loanout (payee_id, vendor_match_key)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_payee_loanout_payee_id ON payee_loanout (payee_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_payee_loanout_vendor_match_key "
               "ON payee_loanout (vendor_match_key)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_payee_loanout_qbo_vendor_id ON payee_loanout (qbo_vendor_id)")


def downgrade():
    op.drop_table("payee_loanout")
