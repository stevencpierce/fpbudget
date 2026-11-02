"""Fringes to 6500 Administrative, one line per labor department
(ACTUALS_HANDOFF Phase 4, Steven's decision of Oct 30 2026).

  budget_line.fringe_for_section   the labor department (COA section code)
                                   whose fringe pot a line_tag='fringe_rollup'
                                   line in 6500 carries; indexed
  budget_line.auto_kind            'wc' | 'ins' | 'fee' | 'pcf' on the hidden
                                   line_tag='auto_calc' lines that give the
                                   auto-calculated Workers' Comp, Production
                                   Insurance, Payroll Service Fee and
                                   Production Company Fee a line for actuals

Schema only. The data migration (one Payroll Fringes line per labor
department on every current Working budget, the four auto-calc lines,
moving the old per-section fringe lines and the parked section:6500
fringe rows onto them) is a boot task in app.py —
_migrate_fringes_to_admin — with a system_task_log marker and an advisory
lock, per CLAUDE.md, so it can also run where this revision never does
(production may never have run Alembic; the per-worker essential pass
mirrors these two columns).

Chain note: Documents Phase 2 adds its own 0023 after 0022_payee_fields
in parallel. Whichever merges second re-chains its down_revision.

Revision ID: 0023a_admin_fringe_lines
Revises: 0022_payee_fields
Create Date: 2026-11-01
"""
from alembic import op
import sqlalchemy as sa

revision = "0023a_admin_fringe_lines"
down_revision = "0022_payee_fields"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    try:
        cols = {c["name"] for c in insp.get_columns("budget_line")}
        idx = {i["name"] for i in insp.get_indexes("budget_line")}
    except Exception:
        cols, idx = set(), set()
    if "fringe_for_section" not in cols:
        op.add_column("budget_line",
                      sa.Column("fringe_for_section", sa.Integer, nullable=True))
    if "auto_kind" not in cols:
        op.add_column("budget_line",
                      sa.Column("auto_kind", sa.String(12), nullable=True))
    if "ix_budget_line_fringe_for_section" not in idx:
        op.create_index("ix_budget_line_fringe_for_section", "budget_line",
                        ["fringe_for_section"])


def downgrade():
    op.drop_index("ix_budget_line_fringe_for_section", table_name="budget_line")
    op.drop_column("budget_line", "auto_kind")
    op.drop_column("budget_line", "fringe_for_section")
