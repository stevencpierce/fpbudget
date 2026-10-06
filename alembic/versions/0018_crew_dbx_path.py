"""doc_upload.crew_dbx_path — where a person-linked document's Crew
Database (Dropbox) duplicate landed; doubles as the copy-once marker.

Revision ID: 0018_crew_dbx_path
Revises: 0017_fringe_rollup
Create Date: 2026-10-08
"""
from alembic import op
import sqlalchemy as sa

revision = "0018_crew_dbx_path"
down_revision = "0017_fringe_rollup"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    cols = {c["name"] for c in insp.get_columns("doc_upload")}
    if "crew_dbx_path" not in cols:
        op.add_column("doc_upload", sa.Column("crew_dbx_path", sa.String(500), nullable=True))


def downgrade():
    op.drop_column("doc_upload", "crew_dbx_path")
