"""Payment attribution + the QuickBooks vendor mirror (DOCUMENTS_HANDOFF
Phase 3, 2026-11-02).

  transaction.payee_id        FK crew_member — WHO the money went to, set
                              when the fact is known (QuickBooks EntityRef,
                              the line's one crew member, the document's
                              owner, a vendor alias, a person) and never
                              inferred at read time.
  transaction.payee_source    qbo | line | doc | alias | manual
  transaction.qbo_entity_ref  the Vendor id QuickBooks sent on the
                              Purchase / BillPayment (EntityRef.value)
  vendor_alias.payee_id       FK crew_member — link a vendor string to a
                              Payee once; every later charge attributes
  qbo_connection.vendor_cdc_since
                              watermark for qbo_sync.sync_vendors (NULL =
                              full pull next run, else CDC from here)
  qbo_vendor                  mirror of every QuickBooks Vendor (display
                              name, company, email, phone, BillAddr,
                              Vendor1099, Active). QuickBooks → FPBudget
                              only. The link to a Payee stays on
                              crew_member.qbo_vendor_id; the Link screen
                              lists the rows no Payee carries. No TIN.

The data step (set payee_id on existing rows where it resolves without
ambiguity) is NOT here: it is the boot task _backfill_txn_payee in app.py
(system_task_log marker + pg advisory lock, per CLAUDE.md), because it
needs the ORM helpers and must log conflicts to the ActivityLog.

Every step is idempotent (inspector check first) because production may
run the per-worker essential pass in app.py before — or instead of — this
revision (see CLAUDE.md).

Revision ID: 0024_txn_payee
Revises: 0023_doc_ownership
Create Date: 2026-11-02
"""
from alembic import op
import sqlalchemy as sa

revision = "0024_txn_payee"
down_revision = "0023_doc_ownership"
branch_labels = None
depends_on = None


# Column factories: sqlite cannot ALTER a table to add a foreign-key
# constraint (alembic raises NotImplementedError), so the FK goes on for
# Postgres only; a sqlite dev DB gets the plain integer column (the ORM
# relationship does not need the constraint to work).
def _fk_int(name, fk):
    def make(dialect):
        if dialect == "postgresql":
            return sa.Column(name, sa.Integer, sa.ForeignKey(fk), nullable=True)
        return sa.Column(name, sa.Integer, nullable=True)
    return make


_TXN_COLUMNS = (
    ("payee_id",       _fk_int("payee_id", "crew_member.id")),
    ("payee_source",   lambda d: sa.Column("payee_source", sa.String(16), nullable=True)),
    ("qbo_entity_ref", lambda d: sa.Column("qbo_entity_ref", sa.String(40), nullable=True)),
)
_ALIAS_COLUMNS = (
    ("payee_id", _fk_int("payee_id", "crew_member.id")),
)
_CONN_COLUMNS = (
    ("vendor_cdc_since", lambda d: sa.Column("vendor_cdc_since", sa.DateTime, nullable=True)),
)


def _add_missing(bind, insp, tables, table, columns):
    if tables is not None and table not in tables:
        return
    cols = {c["name"] for c in insp.get_columns(table)} if tables is not None else set()
    for name, make in columns:
        if name not in cols:
            op.add_column(table, make(bind.dialect.name))


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        tables = set(insp.get_table_names())
    except Exception:
        tables = None   # offline --sql preview

    _add_missing(bind, insp, tables, "transaction", _TXN_COLUMNS)
    _add_missing(bind, insp, tables, "vendor_alias", _ALIAS_COLUMNS)
    _add_missing(bind, insp, tables, "qbo_connection", _CONN_COLUMNS)
    if tables is None or "transaction" in tables:
        # "transaction" is a reserved word on sqlite — quote the table.
        op.execute('CREATE INDEX IF NOT EXISTS ix_transaction_payee_id '
                   'ON "transaction" (payee_id)')
        op.execute('CREATE INDEX IF NOT EXISTS ix_transaction_qbo_entity_ref '
                   'ON "transaction" (qbo_entity_ref)')

    if tables is None or "qbo_vendor" not in tables:
        op.create_table(
            "qbo_vendor",
            sa.Column("qbo_vendor_id", sa.String(40), primary_key=True),
            sa.Column("display_name", sa.String(200), nullable=True),
            sa.Column("company_name", sa.String(200), nullable=True),
            sa.Column("given_name", sa.String(100), nullable=True),
            sa.Column("family_name", sa.String(100), nullable=True),
            sa.Column("email", sa.String(200), nullable=True),
            sa.Column("phone", sa.String(50), nullable=True),
            sa.Column("address_line1", sa.String(200), nullable=True),
            sa.Column("address_line2", sa.String(200), nullable=True),
            sa.Column("city", sa.String(120), nullable=True),
            sa.Column("state", sa.String(40), nullable=True),
            sa.Column("postal_code", sa.String(20), nullable=True),
            sa.Column("vendor_1099", sa.Boolean, nullable=True),
            sa.Column("active", sa.Boolean, nullable=True),
            sa.Column("sync_token", sa.String(20), nullable=True),
            sa.Column("qbo_updated_at", sa.DateTime, nullable=True),
            sa.Column("synced_at", sa.DateTime, nullable=True),
        )


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_transaction_qbo_entity_ref")
    op.execute("DROP INDEX IF EXISTS ix_transaction_payee_id")
    op.drop_table("qbo_vendor")
    for name, _c in reversed(_CONN_COLUMNS):
        op.drop_column("qbo_connection", name)
    for name, _c in reversed(_ALIAS_COLUMNS):
        op.drop_column("vendor_alias", name)
    for name, _c in reversed(_TXN_COLUMNS):
        op.drop_column("transaction", name)
