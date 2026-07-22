"""Baseline — pre-Alembic schema (2026-07-20).

The production schema up to this point was created by db.create_all() plus
app.py's idempotent boot-time DDL (which stays in place, frozen, for the
columns it already manages). This revision is intentionally EMPTY: running
`alembic upgrade head` on the existing production DB is a no-op that simply
creates the alembic_version table and stamps it. On a brand-new database,
db.create_all() at first boot still builds the schema, then this baseline
stamps it.

Every schema change AFTER this date must be a new revision here — do NOT add
to the boot-DDL lists in app.py anymore.
"""

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
