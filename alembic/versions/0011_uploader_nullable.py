"""doc_upload.uploader_id nullable — background drop-folder imports have no human uploader.

Revision ID: 0011_uploader_nullable
Revises: 0010_schedule_waypoint
Create Date: 2026-09-08
"""
from alembic import op
import sqlalchemy as sa

revision = "0011_uploader_nullable"
down_revision = "0010_schedule_waypoint"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = sa.inspect(bind).get_table_names()
    except Exception:
        tables = []
    if "doc_upload" not in tables:
        return
    with op.batch_alter_table("doc_upload") as batch:
        batch.alter_column("uploader_id", existing_type=sa.Integer, nullable=True)


def downgrade():
    with op.batch_alter_table("doc_upload") as batch:
        batch.alter_column("uploader_id", existing_type=sa.Integer, nullable=False)
