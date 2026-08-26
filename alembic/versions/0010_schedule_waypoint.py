"""schedule_waypoint — automatic schedule restore points (30-min burst rule).

Revision ID: 0010_schedule_waypoint
Revises: 0009_comment_day_date
Create Date: 2026-08-19
"""
from alembic import op
import sqlalchemy as sa

revision = "0010_schedule_waypoint"
down_revision = "0009_comment_day_date"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    try:
        tables = sa.inspect(bind).get_table_names()
    except Exception:
        tables = []
    if "schedule_waypoint" in tables:
        return
    op.create_table(
        "schedule_waypoint",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("project_id", sa.Integer,
                  sa.ForeignKey("project_sheet.id"), nullable=False),
        sa.Column("budget_id", sa.Integer,
                  sa.ForeignKey("budget.id", ondelete="CASCADE"), nullable=False),
        sa.Column("created_at", sa.DateTime, index=True),
        sa.Column("state_time", sa.DateTime, nullable=True),
        sa.Column("label", sa.String(200), nullable=True),
        sa.Column("days_json", sa.Text, nullable=True),
        sa.Column("prod_days_json", sa.Text, nullable=True),
        sa.Column("created_by_user_id", sa.Integer,
                  sa.ForeignKey("users.id"), nullable=True),
    )
    op.create_index("ix_sched_waypoint_budget", "schedule_waypoint", ["budget_id"])


def downgrade():
    op.drop_table("schedule_waypoint")
