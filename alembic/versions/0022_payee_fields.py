"""Payee record on crew_member + the per-year 1099 reporting threshold
(DOCUMENTS_HANDOFF Phase 1, 2026-11-01).

CrewMember becomes the Payee: anyone the company pays, person or vendor,
and the read-only mirror of a QuickBooks Vendor. No tax identification
number of any kind is stored — QuickBooks holds the TIN and prints the
1099; FPBudget holds the W-9 PDF and the date it was received.

  crew_member.kind                person | loan_out | vendor
                                  (backfilled: 'vendor' where is_vendor)
  crew_member.qbo_vendor_id       QuickBooks Vendor id; unique when set
  crew_member.qbo_sync_token      QBO SyncToken of the mirrored record
  crew_member.qbo_synced_at       when the mirror was last refreshed
  crew_member.qbo_vendor_1099     QuickBooks "Track payments for 1099"
  crew_member.qbo_origin          'qbo' = first seen by the mirror;
                                  'fpbudget' = created in QuickBooks from
                                  here (Steven, Nov 1: FPBudget may CREATE
                                  a vendor there, never update one)
  crew_member.legal_name          name on the W-9 (may differ from display)
  crew_member.address_line1/2, city, state, postal_code
  crew_member.w9_received_on      THE "W-9 on file" truth (Phase 1)
  crew_member.w9_doc_upload_id    the DocUpload that is that W-9
  crew_member.is_w2_employee      on Wrapbook payroll → excluded from
                                  vendor reporting
  crew_member.wrapbook_worker_id  for the later Wrapbook handshake
  crew_member.notes

  reporting_threshold (tax_year PK, amount, source)
      The 1099-NEC reporting floor per calendar year, never a constant:
      $600 through 2025; $2,000 from 2026 (One Big Beautiful Bill Act),
      inflation-indexed after. Seeded for 2025 and 2026; later years are
      added by a super-admin.

One-time backfills (both only touch rows that are still NULL):
  • kind from is_vendor;
  • w9_received_on / w9_doc_upload_id from the newest non-deleted
    tax_form DocUpload already attached to the person whose sub-type
    or filename says W-9 / W-8 (that is the pre-Phase-2 signal; Phase 2's
    tax_form_kind replaces it).

Every step is idempotent (inspector check first) because production may
run the per-worker essential pass in app.py before — or instead of — this
revision (see CLAUDE.md).

Revision ID: 0022_payee_fields
Revises: 0020_not_project_recovery
Create Date: 2026-11-01
"""
import re

from alembic import op
import sqlalchemy as sa

revision = "0022_payee_fields"
down_revision = "0020_not_project_recovery"
branch_labels = None
depends_on = None


_CM_COLUMNS = (
    ("kind",               sa.Column("kind", sa.String(12), nullable=True)),
    ("qbo_vendor_id",      sa.Column("qbo_vendor_id", sa.String(40), nullable=True)),
    ("qbo_sync_token",     sa.Column("qbo_sync_token", sa.String(20), nullable=True)),
    ("qbo_synced_at",      sa.Column("qbo_synced_at", sa.DateTime, nullable=True)),
    ("qbo_vendor_1099",    sa.Column("qbo_vendor_1099", sa.Boolean, nullable=True)),
    ("qbo_origin",         sa.Column("qbo_origin", sa.String(12), nullable=True)),
    ("legal_name",         sa.Column("legal_name", sa.String(200), nullable=True)),
    ("address_line1",      sa.Column("address_line1", sa.String(200), nullable=True)),
    ("address_line2",      sa.Column("address_line2", sa.String(200), nullable=True)),
    ("city",               sa.Column("city", sa.String(120), nullable=True)),
    ("state",              sa.Column("state", sa.String(40), nullable=True)),
    ("postal_code",        sa.Column("postal_code", sa.String(20), nullable=True)),
    ("w9_received_on",     sa.Column("w9_received_on", sa.Date, nullable=True)),
    ("w9_doc_upload_id",   sa.Column("w9_doc_upload_id", sa.Integer, nullable=True)),
    ("is_w2_employee",     sa.Column("is_w2_employee", sa.Boolean, nullable=True,
                                     server_default=sa.false())),
    ("wrapbook_worker_id", sa.Column("wrapbook_worker_id", sa.String(60), nullable=True)),
    ("notes",              sa.Column("notes", sa.Text, nullable=True)),
)

THRESHOLD_SEED = (
    (2025, "600.00",  "IRS, pre-OBBBA"),
    (2026, "2000.00", "One Big Beautiful Bill Act; indexed from 2027"),
)

_W9_HINT = re.compile(r"w-?[89]", re.I)


def _backfill_w9(bind):
    """Set w9_received_on / w9_doc_upload_id from existing tax_form docs for
    people who have none yet. Done in Python so it runs the same on sqlite
    and Postgres. Newest doc (doc_date, then id) wins."""
    rows = bind.execute(sa.text(
        "SELECT id, crew_member_id, doc_date, uploaded_at, vendor, "
        "       filed_filename, original_filename "
        "  FROM doc_upload "
        " WHERE crew_member_id IS NOT NULL AND category = 'tax_form' "
        "   AND (status IS NULL OR status NOT IN ('deleted', 'duplicate'))"
    )).fetchall()
    best = {}
    for r in rows:
        hay = " ".join(str(x or "") for x in (r[4], r[5], r[6]))
        if not _W9_HINT.search(hay):
            continue
        d = r[2]
        if d is None and r[3] is not None:
            d = r[3].date() if hasattr(r[3], "date") else None
        if d is None:
            continue
        key = (str(d), r[0])
        cur = best.get(r[1])
        if cur is None or key > cur[0]:
            best[r[1]] = (key, r[0], d)
    for cmid, (_k, did, d) in best.items():
        bind.execute(sa.text(
            "UPDATE crew_member SET w9_received_on = :d, w9_doc_upload_id = :did "
            " WHERE id = :cid AND w9_received_on IS NULL"),
            {"d": d, "did": did, "cid": cmid})


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        tables = set(insp.get_table_names())
    except Exception:
        tables = None   # offline --sql preview

    if tables is None or "crew_member" in tables:
        cols = {c["name"] for c in insp.get_columns("crew_member")} if tables is not None else set()
        for name, col in _CM_COLUMNS:
            if name not in cols:
                op.add_column("crew_member", col)
        # kind: backfill from is_vendor for rows that are still NULL, then
        # make 'person' the default for everything created from here on.
        op.execute(
            "UPDATE crew_member SET kind = CASE WHEN is_vendor THEN 'vendor' "
            "ELSE 'person' END WHERE kind IS NULL")
        if bind.dialect.name == "postgresql":
            op.execute("ALTER TABLE crew_member ALTER COLUMN kind SET DEFAULT 'person'")
            op.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_crew_member_qbo_vendor "
                "ON crew_member (qbo_vendor_id) WHERE qbo_vendor_id IS NOT NULL")
        else:
            # sqlite: partial unique index syntax is supported since 3.8.
            op.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_crew_member_qbo_vendor "
                "ON crew_member (qbo_vendor_id) WHERE qbo_vendor_id IS NOT NULL")
        if tables is None or "doc_upload" in tables:
            _backfill_w9(bind)

    if tables is None or "reporting_threshold" not in tables:
        op.create_table(
            "reporting_threshold",
            sa.Column("tax_year", sa.Integer, primary_key=True),
            sa.Column("amount", sa.Numeric(12, 2), nullable=False),
            sa.Column("source", sa.String(200), nullable=True),
        )
    for year, amount, source in THRESHOLD_SEED:
        if bind.dialect.name == "postgresql":
            op.execute(
                f"INSERT INTO reporting_threshold (tax_year, amount, source) "
                f"VALUES ({year}, {amount}, '{source}') ON CONFLICT (tax_year) DO NOTHING")
        else:
            op.execute(
                f"INSERT OR IGNORE INTO reporting_threshold (tax_year, amount, source) "
                f"VALUES ({year}, {amount}, '{source}')")


def downgrade():
    op.drop_table("reporting_threshold")
    op.execute("DROP INDEX IF EXISTS uq_crew_member_qbo_vendor")
    for name, _col in reversed(_CM_COLUMNS):
        op.drop_column("crew_member", name)
