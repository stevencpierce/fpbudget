"""Project stage, code and dates; reference numbers on expenses (2026-11-23).

  project_sheet.stage            VARCHAR(12) NULL   estimate | awarded | wrapping
  project_sheet.project_code     VARCHAR(20) NULL   GH-2609, unique when set
  project_sheet.client_initials  VARCHAR(8)  NULL
  project_sheet.start_date       DATE        NULL
  project_sheet.end_date         DATE        NULL
  project_sheet.awarded_at       TIMESTAMP   NULL
  project_sheet.ref_seq_last     INTEGER     NULL
  transaction.ref_project_id     INTEGER     NULL
  transaction.ref_seq            INTEGER     NULL   unique per ref_project_id

INTAKE_WRAP_HANDOFF Phase 1. `stage` sits beside `status` (Steven, Nov 23):
NULL reads as awarded, so every existing project keeps receiving charges.
No data step. Read through project_codes.py.

Idempotent: production may run the per-worker essential pass in app.py
before — or instead of — this revision (CLAUDE.md).

Revision ID: 0030_project_stage_code_refs
Revises: 0029_project_dropbox_root
Create Date: 2026-11-23
"""
from alembic import op
import sqlalchemy as sa

revision = "0030_project_stage_code_refs"
down_revision = "0029_project_dropbox_root"
branch_labels = None
depends_on = None

_PROJECT_COLS = [
    ("stage", sa.String(12)),
    ("project_code", sa.String(20)),
    ("client_initials", sa.String(8)),
    ("start_date", sa.Date()),
    ("end_date", sa.Date()),
    ("awarded_at", sa.DateTime()),
    ("ref_seq_last", sa.Integer()),
]
_TXN_COLS = [
    ("ref_project_id", sa.Integer()),
    ("ref_seq", sa.Integer()),
]


def _cols(bind, table):
    try:
        return {c["name"] for c in sa.inspect(bind).get_columns(table)}
    except Exception:
        return None   # offline --sql preview


def upgrade():
    bind = op.get_bind()
    have = _cols(bind, "project_sheet")
    for name, typ in _PROJECT_COLS:
        if have is None or name not in have:
            op.add_column("project_sheet", sa.Column(name, typ, nullable=True))
    have = _cols(bind, "transaction")
    for name, typ in _TXN_COLS:
        if have is None or name not in have:
            op.add_column("transaction", sa.Column(name, typ, nullable=True))
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_project_sheet_code "
               "ON project_sheet (project_code) WHERE project_code IS NOT NULL")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_transaction_ref "
               "ON \"transaction\" (ref_project_id, ref_seq) WHERE ref_seq IS NOT NULL")


def downgrade():
    op.execute("DROP INDEX IF EXISTS uq_transaction_ref")
    op.execute("DROP INDEX IF EXISTS uq_project_sheet_code")
    for name, _ in _TXN_COLS:
        op.drop_column("transaction", name)
    for name, _ in _PROJECT_COLS:
        op.drop_column("project_sheet", name)
