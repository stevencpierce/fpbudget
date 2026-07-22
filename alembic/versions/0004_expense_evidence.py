"""expense_evidence — unified doc→expense attachments (Actualizing 2.0 A2).

Revision ID: 0004_expense_evidence
Revises: 0003_activated_at
Create Date: 2026-07-20
"""
from alembic import op
import sqlalchemy as sa

revision = "0004_expense_evidence"
down_revision = "0003_activated_at"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = sa.inspect(bind).get_table_names()
    except Exception:
        tables = None   # offline --sql preview
    if tables is not None and "expense_evidence" not in tables:
        op.create_table(
            "expense_evidence",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("transaction_id", sa.Integer,
                      sa.ForeignKey("transaction.id"), nullable=False,
                      index=True),
            sa.Column("doc_upload_id", sa.Integer,
                      sa.ForeignKey("doc_upload.id"), nullable=False,
                      index=True),
            sa.Column("kind", sa.String(16), server_default="backup"),
            sa.Column("created_at", sa.DateTime, nullable=True),
            sa.UniqueConstraint("transaction_id", "doc_upload_id",
                                name="uq_expense_evidence"),
        )
    if tables is not None and "transaction" not in tables:
        return   # fresh install: no rows to backfill
    # Backfill from the three legacy shapes. ON CONFLICT dedupes reruns.
    op.execute("""
        INSERT INTO expense_evidence (transaction_id, doc_upload_id, kind, created_at)
        SELECT t.id, t.doc_upload_id, 'itemized', CURRENT_TIMESTAMP FROM transaction t
         WHERE t.source = 'invoice_split' AND t.doc_upload_id IS NOT NULL
        ON CONFLICT (transaction_id, doc_upload_id) DO NOTHING""")
    op.execute("""
        INSERT INTO expense_evidence (transaction_id, doc_upload_id, kind, created_at)
        SELECT b.backup_of_txn_id, b.doc_upload_id, 'backup', CURRENT_TIMESTAMP FROM transaction b
         WHERE b.backup_of_txn_id IS NOT NULL AND b.doc_upload_id IS NOT NULL
        ON CONFLICT (transaction_id, doc_upload_id) DO NOTHING""")
    op.execute("""
        INSERT INTO expense_evidence (transaction_id, doc_upload_id, kind, created_at)
        SELECT t.id, t.doc_upload_id, 'primary', CURRENT_TIMESTAMP FROM transaction t
         WHERE t.doc_upload_id IS NOT NULL
           AND t.source IN ('doc_upload', 'reconciled', 'qbo_sync', 'csv_import', 'manual_entry')
           AND (t.budget_line_id IS NOT NULL OR t.account_code IS NOT NULL
                OR t.activated_at IS NOT NULL)
        ON CONFLICT (transaction_id, doc_upload_id) DO NOTHING""")


def downgrade():
    op.drop_table("expense_evidence")
