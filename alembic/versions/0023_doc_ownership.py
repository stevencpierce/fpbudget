"""Documents follow the Payee (DOCUMENTS_HANDOFF Phase 2, 2026-11-01).

A document has an owner or it has a project, usually both. A W-9 uploaded
from the Payee page has no project; a receipt has a project and usually
no owner; an invoice has both.

  doc_upload.project_id       now NULLABLE
  doc_upload.owner_payee_id   FK crew_member — THE owner. crew_member_id
                              stays as an alias column (every write sets
                              both) until the follow-up removes it.
  doc_upload.tax_form_kind    w9 | w8ben | w8bene | w4 | i9 | other
  doc_upload.doc_subtype      employee_vendor_doc sub-type, moved out of
                              `vendor` ("W-9" is not a vendor name)
  doc_upload.received_on      tax forms
  doc_upload.expires_on       COIs (red when past, amber within 60 days)

Data step (idempotent — every statement only touches rows still NULL, so
re-running, or the per-worker essential pass in app.py running first, is
harmless):
  1. owner_payee_id = crew_member_id where null.
  2. employee_vendor_doc rows: doc_subtype = vendor; tax_form_kind from the
     W-9 / W-8 / W-4 / I-9 sub-types; the W-9 and W-8 ones become
     category='tax_form' (they ARE tax forms — the one W-9 rule reads
     tax_form_kind, not a sub-type string); then vendor is cleared.
  3. tax_form rows whose sub-type, OCR vendor or filename names a W-9 / W-8
     get tax_form_kind and received_on = COALESCE(doc_date, uploaded_at).
  4. crew_member.w9_received_on / w9_doc_upload_id from the newest such
     document, only where still NULL.

Revision ID: 0023_doc_ownership
Revises: 0022_payee_fields
Create Date: 2026-11-01
"""
import re

from alembic import op
import sqlalchemy as sa

revision = "0023_doc_ownership"
down_revision = "0023a_admin_fringe_lines"
branch_labels = None
depends_on = None


_DU_COLUMNS = (
    ("owner_payee_id", sa.Column("owner_payee_id", sa.Integer,
                                 sa.ForeignKey("crew_member.id"), nullable=True)),
    ("tax_form_kind",  sa.Column("tax_form_kind", sa.String(12), nullable=True)),
    ("doc_subtype",    sa.Column("doc_subtype", sa.String(40), nullable=True)),
    ("received_on",    sa.Column("received_on", sa.Date, nullable=True)),
    ("expires_on",     sa.Column("expires_on", sa.Date, nullable=True)),
)

# Sub-type / filename → tax_form_kind. Order matters: W-8BEN-E before W-8BEN.
_KIND_PATTERNS = (
    ("w8bene", re.compile(r"w-?8\s*ben-?\s*e(?![a-z])", re.I)),
    ("w8ben",  re.compile(r"w-?8", re.I)),
    ("w9",     re.compile(r"w-?9", re.I)),
    ("w4",     re.compile(r"\bw-?4\b", re.I)),
    ("i9",     re.compile(r"\bi-?9\b", re.I)),
)
_TAX_KINDS = ("w9", "w8ben", "w8bene")


def kind_from_text(text):
    """The tax_form_kind a sub-type string or filename names, else None.
    Shared with actuals.tax_form_kind_from_text (same table)."""
    s = (text or "")
    for kind, rx in _KIND_PATTERNS:
        if rx.search(s):
            return kind
    return None


def _data_step(bind):
    """Runs the same on sqlite and Postgres: plain SQL for the bulk copies,
    Python for the regex classification (no dialect-specific regex ops)."""
    # 1. owner from the alias column.
    bind.execute(sa.text(
        "UPDATE doc_upload SET owner_payee_id = crew_member_id "
        " WHERE owner_payee_id IS NULL AND crew_member_id IS NOT NULL"))

    # 2. employee_vendor_doc: sub-type out of `vendor`.
    rows = bind.execute(sa.text(
        "SELECT id, vendor FROM doc_upload "
        " WHERE category = 'employee_vendor_doc' AND vendor IS NOT NULL "
        "   AND TRIM(vendor) <> ''")).fetchall()
    for rid, vendor in rows:
        sub = (vendor or "").strip()[:40]
        kind = kind_from_text(sub)
        params = {"id": rid, "sub": sub}
        if kind in _TAX_KINDS:
            bind.execute(sa.text(
                "UPDATE doc_upload SET doc_subtype = COALESCE(doc_subtype, :sub), "
                "       tax_form_kind = COALESCE(tax_form_kind, :kind), "
                "       category = 'tax_form', vendor = NULL "
                " WHERE id = :id"), {**params, "kind": kind})
        elif kind:
            bind.execute(sa.text(
                "UPDATE doc_upload SET doc_subtype = COALESCE(doc_subtype, :sub), "
                "       tax_form_kind = COALESCE(tax_form_kind, :kind), vendor = NULL "
                " WHERE id = :id"), {**params, "kind": kind})
        else:
            bind.execute(sa.text(
                "UPDATE doc_upload SET doc_subtype = COALESCE(doc_subtype, :sub), "
                "       vendor = NULL WHERE id = :id"), params)

    # 3. tax_form rows that name a W-9 / W-8: kind + received_on.
    rows = bind.execute(sa.text(
        "SELECT id, doc_subtype, vendor, filed_filename, original_filename, "
        "       tax_form_kind, received_on, doc_date, uploaded_at "
        "  FROM doc_upload WHERE category = 'tax_form'")).fetchall()
    for r in rows:
        rid, sub, vendor, f1, f2, kind, received_on, doc_date, uploaded_at = r
        if not kind:
            kind = kind_from_text(" ".join(str(x or "") for x in (sub, vendor, f1, f2)))
            if kind in _TAX_KINDS:
                bind.execute(sa.text(
                    "UPDATE doc_upload SET tax_form_kind = :k WHERE id = :id "
                    "   AND tax_form_kind IS NULL"), {"k": kind, "id": rid})
        if kind in _TAX_KINDS and received_on is None:
            d = doc_date
            if d is None and uploaded_at is not None:
                d = uploaded_at.date() if hasattr(uploaded_at, "date") else None
                if d is None and isinstance(uploaded_at, str):
                    d = uploaded_at[:10]
            if d is not None:
                bind.execute(sa.text(
                    "UPDATE doc_upload SET received_on = :d WHERE id = :id "
                    "   AND received_on IS NULL"), {"d": d, "id": rid})

    # 4. The Payee's W-9 pointer from the newest owned W-9 / W-8, where NULL.
    rows = bind.execute(sa.text(
        "SELECT id, owner_payee_id, received_on, doc_date, uploaded_at "
        "  FROM doc_upload "
        " WHERE category = 'tax_form' AND owner_payee_id IS NOT NULL "
        "   AND tax_form_kind IN ('w9', 'w8ben', 'w8bene') "
        "   AND (status IS NULL OR status NOT IN ('deleted', 'duplicate'))")).fetchall()
    best = {}
    for rid, owner, received_on, doc_date, uploaded_at in rows:
        d = received_on or doc_date
        if d is None and uploaded_at is not None:
            d = uploaded_at.date() if hasattr(uploaded_at, "date") else None
        if d is None:
            continue
        key = (str(d), rid)
        cur = best.get(owner)
        if cur is None or key > cur[0]:
            best[owner] = (key, rid, d)
    for owner, (_k, rid, d) in best.items():
        bind.execute(sa.text(
            "UPDATE crew_member SET w9_received_on = :d, w9_doc_upload_id = :did "
            " WHERE id = :cid AND w9_received_on IS NULL"),
            {"d": d, "did": rid, "cid": owner})


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        tables = set(insp.get_table_names())
    except Exception:
        tables = None   # offline --sql preview

    if tables is not None and "doc_upload" not in tables:
        return
    cols = ({c["name"] for c in insp.get_columns("doc_upload")}
            if tables is not None else set())
    for name, col in _DU_COLUMNS:
        if name not in cols:
            op.add_column("doc_upload", col)
    if bind.dialect.name == "postgresql":
        op.execute("ALTER TABLE doc_upload ALTER COLUMN project_id DROP NOT NULL")
        op.execute("CREATE INDEX IF NOT EXISTS ix_doc_upload_owner_payee_id "
                   "ON doc_upload (owner_payee_id)")
    else:
        # sqlite cannot ALTER a column's nullability; batch mode rebuilds the
        # table. Local dev DBs created by db.create_all() are already nullable.
        with op.batch_alter_table("doc_upload") as batch:
            batch.alter_column("project_id", existing_type=sa.Integer, nullable=True)
        op.execute("CREATE INDEX IF NOT EXISTS ix_doc_upload_owner_payee_id "
                   "ON doc_upload (owner_payee_id)")
    _data_step(bind)


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_doc_upload_owner_payee_id")
    for name, _col in reversed(_DU_COLUMNS):
        op.drop_column("doc_upload", name)
    # project_id is left nullable: rows without a project may exist by now
    # and a NOT NULL would refuse the downgrade.
