"""analyzer_batch — durable doc-analyzer batch state (audit H7).

Revision ID: 0002_analyzer_batch
Revises: 0001_baseline
Create Date: 2026-07-20
"""
from alembic import op
import sqlalchemy as sa

revision = "0002_analyzer_batch"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None


def upgrade():
    # Guarded: on a fresh install db.create_all() may already have built the
    # table from the model (local dev runs the app before alembic).
    bind = op.get_bind()
    try:
        existing = sa.inspect(bind).get_table_names()
    except Exception:
        existing = []   # offline --sql preview: emit the CREATE unconditionally
    if "analyzer_batch" not in existing:
        op.create_table(
            "analyzer_batch",
            sa.Column("batch_token", sa.String(64), primary_key=True),
            sa.Column("raw_json", sa.Text, nullable=True),
            sa.Column("pending_json", sa.Text, nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=True),
            sa.Column("updated_at", sa.DateTime, nullable=True),
        )


def downgrade():
    op.drop_table("analyzer_batch")
