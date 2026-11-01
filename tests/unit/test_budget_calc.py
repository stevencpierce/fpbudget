"""Real assertions for the budget calc engine (audit C4d, 2026-07-20).

Pure unit tests — no Flask, no DB, no browser. Stub line/fringe objects go in,
exact dollar amounts must come out. Every test pins a documented invariant of
calc_line/_effective_days; if a refactor changes the math, these fail loudly
(the old Playwright "calc tests" asserted nothing).

Run: pytest tests/unit  (wired into CI).
"""
import os
import sys
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from budget_calc import calc_line, calc_line_from_schedule, calc_top_sheet, _effective_days  # noqa: E402


def _fringe_cfgs():
    return {
        "E": NS(is_flat=False, rate=0.20, flat_amount=None),   # 20% payroll fringe
        "F": NS(is_flat=True, rate=None, flat_amount=500.0),   # flat per-person fee
    }


def _line(**kw):
    base = dict(is_labor=False, quantity=None, days=None, rate=None,
                agent_pct=0.0, estimated_total=None, est_ot=0.0,
                fringe_type=None, days_unit="days", rate_type="flat_day",
                days_per_week=5, line_tag=None, auto_kind=None,
                fringe_for_section=None, fee_disperse_amount=None)
    base.update(kw)
    return NS(**base)


# ── Non-labor lines ─────────────────────────────────────────────────────────

def test_nonlabor_qty_days_rate():
    r = calc_line(_line(quantity=2, days=3, rate=100.0), _fringe_cfgs())
    assert r["subtotal"] == 600.00
    assert r["total"] == 600.00
    assert r["fringe_amount"] == 0.0


def test_nonlabor_discount_is_fraction():
    # agent_pct doubles as Disc% on non-labor lines, stored as a fraction.
    r = calc_line(_line(quantity=1, days=2, rate=400.0, agent_pct=0.20), _fringe_cfgs())
    assert r["subtotal"] == 800.00
    assert r["agent_amount"] == 160.00
    assert r["total"] == 640.00


def test_nonlabor_user_zeroed_field_means_zero():
    # Explicitly zeroing any of qty/days/rate zeroes the line — it must NOT
    # fall back to the stale estimated_total (user report 2026-05-07).
    r = calc_line(_line(quantity=0, days=3, rate=100.0, estimated_total=999.0),
                  _fringe_cfgs())
    assert r["total"] == 0.0


def test_nonlabor_uninitialized_honors_stored_estimate():
    r = calc_line(_line(quantity=None, days=None, rate=None,
                        estimated_total=1234.56), _fringe_cfgs())
    assert r["total"] == 1234.56


# ── Labor lines ─────────────────────────────────────────────────────────────

def test_labor_qty_forced_to_one():
    # Labor invariant: 1 person per line — even a stale qty=5 row must not
    # multiply (user 2026-05-04).
    r = calc_line(_line(is_labor=True, quantity=5, days=2, rate=1400.0),
                  _fringe_cfgs())
    assert r["subtotal"] == 2800.00


def test_labor_ot_fringe_pct_and_agent():
    # base 2×1400=2800 + OT 100 = 2900; fringe 20% = 580; agent 10% = 290.
    # Phase 4 (Steven, Oct 30 2026): fringe is reported on the line but
    # never inside its total — it pools on the department's Payroll
    # Fringes line in 6500. total = subtotal + agent only.
    r = calc_line(_line(is_labor=True, days=2, rate=1400.0, est_ot=100.0,
                        fringe_type="E", agent_pct=0.10), _fringe_cfgs())
    assert r["subtotal"] == 2900.00
    assert r["fringe_amount"] == 580.00
    assert r["agent_amount"] == 290.00
    assert r["total"] == 3190.00
    assert r["est_total"] == r["total"]


def test_labor_flat_fringe_charged_once_not_per_day():
    r2 = calc_line(_line(is_labor=True, days=2, rate=1000.0, fringe_type="F"),
                   _fringe_cfgs())
    r9 = calc_line(_line(is_labor=True, days=9, rate=1000.0, fringe_type="F"),
                   _fringe_cfgs())
    assert r2["fringe_amount"] == 500.00
    assert r9["fringe_amount"] == 500.00   # per person, NOT per day


# ── Effective days / weeks ──────────────────────────────────────────────────

def test_week_rate_days_column_is_weeks():
    ln = _line(is_labor=True, days=3, rate=1000.0, rate_type="week")
    assert _effective_days(ln) == 3
    assert calc_line(ln, _fringe_cfgs())["subtotal"] == 3000.00


def test_daily_rate_with_weeks_unit_converts_via_days_per_week():
    ln = _line(is_labor=True, days=2, rate=100.0, days_unit="weeks",
               rate_type="flat_day", days_per_week=5)
    assert _effective_days(ln) == 10
    assert calc_line(ln, _fringe_cfgs())["subtotal"] == 1000.00


def test_rounding_to_cents():
    r = calc_line(_line(is_labor=True, days=3, rate=333.333, fringe_type="E"),
                  _fringe_cfgs())
    assert r["subtotal"] == round(3 * 333.333, 2)
    assert r["fringe_amount"] == round(r["subtotal"] * 0.20, 2)
    assert r["total"] == r["subtotal"]          # fringe lives in 6500, not here


# ── Phase 4: fringes to 6500 Administrative ─────────────────────────────────

def test_fringe_never_inside_line_total_regardless_of_configs():
    # No sentinel, no toggle: the same line gives the same split with any
    # fringe_configs dict (the old '__fringe_rollup__' key is ignored).
    ln = _line(is_labor=True, days=2, rate=1000.0, fringe_type="E")
    plain = calc_line(ln, _fringe_cfgs())
    legacy = calc_line(ln, dict(_fringe_cfgs(), __fringe_rollup__=True))
    assert plain == legacy
    assert plain["fringe_amount"] == 400.00
    assert plain["total"] == 2000.00


def test_computed_lines_calc_as_zero():
    # The 6500 Payroll Fringes lines and the hidden auto-calc lines carry
    # their number on estimated_total (written through from the calc) —
    # calc_line must return zero so no accumulator counts the pot twice.
    fr = _line(line_tag="fringe_rollup", fringe_for_section=2000,
               estimated_total=1234.0, account_code=6500)
    auto = _line(line_tag="auto_calc", auto_kind="wc", estimated_total=99.0,
                 quantity=1, days=1, rate=99.0)
    for ln in (fr, auto):
        r = calc_line(ln, _fringe_cfgs())
        assert r["total"] == 0.0 and r["est_total"] == 0.0
        assert r["subtotal"] == 0.0 and r["fringe_amount"] == 0.0
    sched = calc_line_from_schedule(fr, [NS(crew_instance=1, date=None, day_type="work")],
                                    _fringe_cfgs())
    assert sched["est_total"] == 0.0


def _ts_lines():
    """Two labor departments + one non-labor line + a 6500 Payroll Fringes
    line (already written through at $0) for calc_top_sheet."""
    return [
        _line(id=1, is_labor=True, days=2, rate=1000.0, fringe_type="E",
              account_code=2001, use_schedule=False),               # 2000: fringe 400
        _line(id=2, is_labor=True, days=1, rate=500.0, fringe_type="E",
              account_code=2101, use_schedule=False),               # 2100: fringe 100
        _line(id=3, quantity=1, days=1, rate=300.0, account_code=2601,
              use_schedule=False),                                  # 2600: no fringe
        _line(id=4, line_tag="fringe_rollup", fringe_for_section=2000,
              account_code=6500, estimated_total=400.0, use_schedule=False),
    ]


def _budget(**kw):
    base = dict(budget_mode="working", schedule_days=[], workers_comp_pct=0.0,
                payroll_fee_pct=0.0, production_insurance_mode="off",
                production_insurance_pct=0.0, production_insurance_flat=0.0,
                company_fee_pct=0.0, company_fee_dispersed=False,
                company_fee_mode="pct", company_fee_flat=0.0,
                fee_excluded_sections=None, fee_exclude_fringes=True)
    base.update(kw)
    return NS(**base)


def test_top_sheet_puts_every_department_fringe_pot_in_6500():
    ts = calc_top_sheet(_budget(), _ts_lines(), _fringe_cfgs(), {})
    by_code = {r["code"]: r for r in ts["rows"]}
    assert by_code[2000]["estimated"] == 2000.00      # wages only
    assert by_code[2100]["estimated"] == 500.00
    assert by_code[2600]["estimated"] == 300.00
    assert by_code[6500]["estimated"] == 500.00       # 400 + 100, nothing else
    assert ts["fringe_by_section"] == {2000: 400.00, 2100: 100.00}
    assert ts["fringe_rollup_amount"] == 500.00
    assert ts["fringe_home_section"] == 6500
    assert ts["fringe_rollup"] is True
    # The written-through 6500 line (id 4) must not add to the pot.
    assert ts["line_totals"][4]["est_total"] == 0.0
    assert ts["subtotal_estimated"] == 3300.00


def test_top_sheet_fee_base_excludes_fringes_from_6500_only():
    ts = calc_top_sheet(_budget(company_fee_pct=0.10), _ts_lines(), _fringe_cfgs(), {})
    by_code = {r["code"]: r for r in ts["rows"]}
    assert by_code[6500]["fringe_in_section"] == 500.00
    assert by_code[2000]["fringe_in_section"] == 0.0
    assert ts["company_fee_base"] == 2800.00          # 3300 − 500 fringes
    assert ts["company_fee"] == 280.00


def test_top_sheet_auto_calc_amounts_land_in_6000_and_6500():
    b = _budget(workers_comp_pct=0.03, payroll_fee_pct=0.02,
                production_insurance_mode="flat", production_insurance_flat=150.0)
    ts = calc_top_sheet(b, _ts_lines(), _fringe_cfgs(), {})
    by_code = {r["code"]: r for r in ts["rows"]}
    assert ts["gross_labor_wages"] == 2500.00
    assert ts["workers_comp_amount"] == 75.00
    assert ts["payroll_fee_amount"] == 50.00
    assert ts["production_insurance_amount"] == 150.00
    assert by_code[6000]["estimated"] == 225.00
    assert by_code[6500]["estimated"] == 550.00       # fringes 500 + payroll fee 50
