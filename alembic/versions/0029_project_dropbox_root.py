"""Where a project's Dropbox folder IS right now (2026-11-04).

  project_sheet.dropbox_root  VARCHAR(500) NULL

Wrap and archive move the project folder to `_WRAPPED PROJECTS/<date>_<slug>`
/ `_ARCHIVED/<date>_<slug>` but the app kept reading it at the ops root
(dropbox_folder), so every stored document path went stale (projects 285
and 288, found live). The move now records the folder's full current path
here (NULL = at the ops root) and rewrites the stored paths
(app._move_project_folder → _remap_project_dropbox_paths). Read it only
through app._project_root_for(project).

Idempotent: production may run the per-worker essential pass in app.py
before — or instead of — this revision (CLAUDE.md).

Revision ID: 0029_project_dropbox_root
Revises: 0028_payee_loanout
Create Date: 2026-11-04
"""
from alembic import op
import sqlalchemy as sa

revision = "0029_project_dropbox_root"
down_revision = "0028_payee_loanout"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        cols = {c["name"] for c in sa.inspect(bind).get_columns("project_sheet")}
    except Exception:
        cols = None   # offline --sql preview
    if cols is None or "dropbox_root" not in cols:
        op.add_column("project_sheet", sa.Column("dropbox_root", sa.String(500), nullable=True))


def downgrade():
    op.drop_column("project_sheet", "dropbox_root")
