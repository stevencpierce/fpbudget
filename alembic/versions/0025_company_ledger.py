"""Company-wide ledger (owner decision 2026-11-03).

ONE row per QuickBooks transaction for the whole company:

  transaction.claim_conflict         the ledger migration found this charge
                                     claimed by two projects (both rows are
                                     kept and flagged) or its receipt-side
                                     payee differs from the QuickBooks payee
  transaction.claim_conflict_reason  why
  uq_transaction_qbo_company         partial UNIQUE index on
                                     (qbo_txn_id, qbo_txn_type) WHERE
                                     qbo_txn_id IS NOT NULL AND
                                     claim_conflict IS NOT TRUE
  uq_transaction_qbo                 the old per-project index
                                     (project_id, qbo_txn_id, qbo_txn_type)
                                     is DROPPED
  qbo_connection.sync_through / last_synced / last_cdc_sync
                                     the company-wide sync watermarks (the
                                     per-project columns are retired, kept)
  transaction_merge_log              every twin row the migration or a
                                     conflict resolution removed, as JSON,
                                     restorable by id from Maintenance

The DATA step (collapse the per-project copies, seed the company account
set) is NOT here: it is ledger.run_company_ledger_migration, started from
Admin → Maintenance → "Company ledger — preview / run" (system_task_log
marker + pg advisory lock). It never runs by itself on deploy.

The new unique index can only exist once the data step has collapsed the
duplicate copies, so this revision creates it ONLY when no duplicate
(qbo_txn_id, qbo_txn_type) group remains; otherwise it logs and leaves the
index to the Maintenance run, which creates it at the end. Every step is
idempotent because production may run the per-worker essential pass in
app.py before — or instead of — this revision (see CLAUDE.md).

Revision ID: 0025_company_ledger
Revises: 0024_txn_payee
Create Date: 2026-11-03
"""
import logging

from alembic import op
import sqlalchemy as sa

revision = "0025_company_ledger"
down_revision = "0024_txn_payee"
branch_labels = None
depends_on = None

log = logging.getLogger("alembic.0025")

NEW_INDEX_SQL = (
    'CREATE UNIQUE INDEX IF NOT EXISTS uq_transaction_qbo_company '
    'ON "transaction" (qbo_txn_id, qbo_txn_type) '
    'WHERE qbo_txn_id IS NOT NULL AND claim_conflict IS NOT TRUE'
)
DUP_GROUPS_SQL = (
    'SELECT COUNT(*) FROM (SELECT qbo_txn_id, qbo_txn_type FROM "transaction" '
    'WHERE qbo_txn_id IS NOT NULL AND claim_conflict IS NOT TRUE '
    'GROUP BY qbo_txn_id, qbo_txn_type HAVING COUNT(*) > 1) d'
)

_TXN_COLUMNS = (
    ("claim_conflict", lambda d: sa.Column("claim_conflict", sa.Boolean, nullable=True,
                                           server_default=sa.false())),
    ("claim_conflict_reason", lambda d: sa.Column("claim_conflict_reason", sa.String(200),
                                                  nullable=True)),
)
_CONN_COLUMNS = (
    ("sync_through", lambda d: sa.Column("sync_through", sa.String(10), nullable=True)),
    ("last_synced", lambda d: sa.Column("last_synced", sa.DateTime, nullable=True)),
    ("last_cdc_sync", lambda d: sa.Column("last_cdc_sync", sa.DateTime, nullable=True)),
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
    try:
        insp = sa.inspect(bind)
        tables = set(insp.get_table_names())
    except Exception:
        insp, tables = None, None   # offline --sql preview

    _add_missing(bind, insp, tables, "transaction", _TXN_COLUMNS)
    _add_missing(bind, insp, tables, "qbo_connection", _CONN_COLUMNS)

    if tables is None or "transaction_merge_log" not in tables:
        op.create_table(
            "transaction_merge_log",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("qbo_txn_id", sa.String(50), nullable=True),
            sa.Column("qbo_txn_type", sa.String(20), nullable=True),
            sa.Column("kept_txn_id", sa.Integer, nullable=True),
            sa.Column("removed_txn_id", sa.Integer, nullable=True),
            sa.Column("removed_project_id", sa.Integer, nullable=True),
            sa.Column("removed_row_json", sa.Text, nullable=True),
            sa.Column("reason", sa.String(120), nullable=True),
            sa.Column("restored_at", sa.DateTime, nullable=True),
            sa.Column("restored_txn_id", sa.Integer, nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=True),
        )

    if tables is None or "transaction" in tables:
        op.execute("DROP INDEX IF EXISTS uq_transaction_qbo")
        op.execute('CREATE INDEX IF NOT EXISTS ix_transaction_qbo_txn_id '
                   'ON "transaction" (qbo_txn_id, qbo_txn_type)')
        dups = None
        try:
            dups = bind.execute(sa.text(DUP_GROUPS_SQL)).scalar()
        except Exception as e:   # offline preview or a table without the column yet
            log.warning("0025: could not count duplicate QuickBooks groups (%s)", e)
        if dups == 0:
            op.execute(NEW_INDEX_SQL)
        else:
            log.warning("0025: %s duplicate QuickBooks groups remain — the company "
                        "unique index is created by the ledger migration in "
                        "Admin → Maintenance, not here", dups)


def downgrade():
    op.execute("DROP INDEX IF EXISTS uq_transaction_qbo_company")
    op.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_transaction_qbo '
               'ON "transaction" (project_id, qbo_txn_id, qbo_txn_type) '
               'WHERE qbo_txn_id IS NOT NULL')
    op.drop_table("transaction_merge_log")
    for name, _c in reversed(_CONN_COLUMNS):
        op.drop_column("qbo_connection", name)
    for name, _c in reversed(_TXN_COLUMNS):
        op.drop_column("transaction", name)
