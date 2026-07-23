"""api_token — bearer tokens for the mobile app API (Phase 0).

Revision ID: 0005_api_token
Revises: 0004_expense_evidence
Create Date: 2026-07-23
"""
from alembic import op
import sqlalchemy as sa

revision = "0005_api_token"
down_revision = "0004_expense_evidence"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = sa.inspect(bind).get_table_names()
    except Exception:
        tables = None   # offline --sql preview
    if tables is not None and "api_token" in tables:
        return
    op.create_table(
        "api_token",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"),
                  nullable=False, index=True),
        sa.Column("token_hash", sa.String(64), nullable=False, unique=True,
                  index=True),
        sa.Column("device_name", sa.String(120), nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=True),
        sa.Column("last_used_at", sa.DateTime, nullable=True),
        sa.Column("revoked_at", sa.DateTime, nullable=True),
    )


def downgrade():
    op.drop_table("api_token")
