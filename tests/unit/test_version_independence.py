"""Concrete regression tests for budget version independence.

Rules under test (per product owner):
  1. Duplicating a version produces a faithful, identical copy.
  2. A version is never altered without explicit user interaction —
     in particular, merely *viewing* a version must not change its totals.
  3. Editing one version never changes another.
  4. Deleting one version never changes another.

These exercise the real app code: _create_budget_from_source,
sync_schedule_driven_lines, calc_top_sheet, _delete_budget_cascade.
"""
from datetime import date

import pytest


def _grand_total(A, budget):
    from app import db
    from models import BudgetLine
    from budget_calc import calc_top_sheet, get_fringe_configs
    lines = (BudgetLine.query.filter_by(budget_id=budget.id)
             .order_by(BudgetLine.account_code, BudgetLine.sort_order).all())
    fr = get_fringe_configs(db.session, budget.project_id)
    prof = budget.payroll_profile
    pw = (budget.payroll_week_start if budget.payroll_week_start is not None
          else (prof.payroll_week_start if prof else 6))
    return calc_top_sheet(budget, lines, fr, {}, prof, pw)["grand_total_estimated"]


def _build_v1(A):
    """A v1 estimated budget: one schedule-driven labor line over 5 work
    days, each a production day with First Meal + Craft Services."""
    from app import db
    from models import (ProjectSheet, Budget, BudgetLine, ScheduleDay,
                        ProductionDay, FringeConfig, PayrollProfile)
    prof = PayrollProfile.query.filter(PayrollProfile.name.ilike('%federal%')).first()
    p = ProjectSheet(name="VerIndep", client_name="Test")
    db.session.add(p); db.session.flush()
    db.session.add(FringeConfig(project_id=p.id, fringe_type='none', label='None',
                                rate=0, is_flat=False))
    v1 = Budget(project_id=p.id, name="VerIndep EST v1", budget_mode='estimated',
                version_number=1, version_status='current',
                payroll_profile_id=prof.id if prof else None, payroll_week_start=6,
                production_insurance_mode='pct', production_insurance_pct=0.015,
                workers_comp_pct=0.03, payroll_fee_pct=0.0175, company_fee_pct=0.18)
    db.session.add(v1); db.session.flush()
    labor = BudgetLine(budget_id=v1.id, account_code=1000,
                       account_name="Production Staff", description="Producer",
                       is_labor=True, use_schedule=True, rate=1000, rate_type='day',
                       fringe_type='none', quantity=1, sort_order=1)
    db.session.add(labor); db.session.flush()
    for d in [date(2026, 6, i) for i in range(1, 6)]:
        db.session.add(ScheduleDay(budget_id=v1.id, budget_line_id=labor.id,
                                   crew_instance=1, date=d, day_type='work',
                                   schedule_mode='estimated'))
        db.session.add(ProductionDay(budget_id=v1.id, date=d, schedule_mode='estimated',
                                     first_meal=True, craft_services=True,
                                     is_production_day=True))
    db.session.commit()
    from budget_calc import sync_schedule_driven_lines
    sync_schedule_driven_lines(v1.id, db.session)
    db.session.commit()
    return p, v1


def test_duplicate_is_faithful(ctx):
    A = ctx
    from app import db
    p, v1 = _build_v1(A)
    t1 = _grand_total(A, v1)
    assert t1 > 0
    v2 = A._create_budget_from_source(p.id, v1, "VerIndep EST v2", 'estimated',
                                      parent_bid=v1.id, version_number=2)
    db.session.commit()
    # Faithful: identical the instant it is created, with NO sync.
    assert _grand_total(A, v2) == pytest.approx(t1, abs=0.005)


def test_view_does_not_mutate_a_duplicate(ctx):
    """A duplicate that copied a value must not drift when re-derived on
    view. This is the 102k->130k bug: freeze-on-view keeps it steady."""
    A = ctx
    from app import db
    from models import BudgetLine
    from budget_calc import sync_schedule_driven_lines
    p, v1 = _build_v1(A)
    # Mimic a value frozen under older calc rules (stale meal lines).
    for ln in BudgetLine.query.filter_by(budget_id=v1.id).all():
        if ln.line_tag in ('craft_services', 'meal_first'):
            ln.estimated_total = float(ln.estimated_total or 0) * 0.4
            ln.quantity = float(ln.quantity or 0) * 0.4
    db.session.commit()
    t1_stale = _grand_total(A, v1)
    v3 = A._create_budget_from_source(p.id, v1, "VerIndep EST v3", 'estimated',
                                      parent_bid=v1.id, version_number=3)
    db.session.commit()
    # Copy is faithful to the (stale) source.
    assert _grand_total(A, v3) == pytest.approx(t1_stale, abs=0.005)
    # The OLD on-view behavior re-derived and drifted. Document that the
    # ONLY thing that moves the number is an explicit sync (an edit action),
    # never the copy itself.
    sync_schedule_driven_lines(v3.id, db.session)
    db.session.commit()
    t3_resynced = _grand_total(A, v3)
    # An explicit recompute is allowed to change it; the point is the copy
    # did not. Guard that the drift is attributable to the sync alone.
    assert t3_resynced != pytest.approx(t1_stale, abs=0.005)


def test_editing_v2_does_not_change_v1(ctx):
    A = ctx
    from app import db
    from models import BudgetLine, ScheduleDay, ProductionDay
    from budget_calc import sync_schedule_driven_lines
    p, v1 = _build_v1(A)
    v2 = A._create_budget_from_source(p.id, v1, "VerIndep EST v2", 'estimated',
                                      parent_bid=v1.id, version_number=2)
    db.session.commit()
    t1_before = _grand_total(A, v1)
    v2_labor = BudgetLine.query.filter_by(budget_id=v2.id, description="Producer").first()
    for d in [date(2026, 6, 8), date(2026, 6, 9)]:
        db.session.add(ScheduleDay(budget_id=v2.id, budget_line_id=v2_labor.id,
                                   crew_instance=1, date=d, day_type='work',
                                   schedule_mode='estimated'))
        db.session.add(ProductionDay(budget_id=v2.id, date=d, schedule_mode='estimated',
                                     first_meal=True, craft_services=True,
                                     is_production_day=True))
    db.session.commit()
    sync_schedule_driven_lines(v2.id, db.session)
    db.session.commit()
    assert _grand_total(A, v2) > t1_before          # v2 grew
    assert _grand_total(A, v1) == pytest.approx(t1_before, abs=0.005)  # v1 steady


def test_deleting_v2_does_not_change_v1(ctx):
    A = ctx
    from app import db
    p, v1 = _build_v1(A)
    t1_before = _grand_total(A, v1)
    v2 = A._create_budget_from_source(p.id, v1, "VerIndep EST v2", 'estimated',
                                      parent_bid=v1.id, version_number=2)
    db.session.commit()
    A._delete_budget_cascade(v2.id)
    db.session.commit()
    assert _grand_total(A, v1) == pytest.approx(t1_before, abs=0.005)
