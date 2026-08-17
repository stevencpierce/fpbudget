"""budget_comment — internal notes/comments on budgets + budget lines.

Revision ID: 0007_budget_comments
Revises: 0006_budget_version_meta
Create Date: 2026-07-22
"""
from alembic import op
import sqlalchemy as sa

revision = "0007_budget_comments"
down_revision = "0006_budget_version_meta"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = sa.inspect(bind).get_table_names()
    except Exception:
        tables = None   # offline --sql preview
    if tables is not None and "budget_comment" in tables:
        return
    if tables is not None and "budget" not in tables:
        return   # fresh install: create_all builds it from the model
    op.create_table(
        "budget_comment",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("project_id", sa.Integer,
                  sa.ForeignKey("project_sheet.id"), nullable=False, index=True),
        sa.Column("budget_id", sa.Integer,
                  sa.ForeignKey("budget.id"), nullable=False, index=True),
        sa.Column("budget_line_id", sa.Integer,
                  sa.ForeignKey("budget_line.id"), nullable=True, index=True),
        sa.Column("author_id", sa.Integer,
                  sa.ForeignKey("users.id"), nullable=True),
        sa.Column("body", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime, index=True),
    )


def downgrade():
    op.drop_table("budget_comment")
