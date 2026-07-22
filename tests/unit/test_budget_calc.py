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

from budget_calc import calc_line, _effective_days  # noqa: E402


def _fringe_cfgs():
    return {
        "E": NS(is_flat=False, rate=0.20, flat_amount=None),   # 20% payroll fringe
        "F": NS(is_flat=True, rate=None, flat_amount=500.0),   # flat per-person fee
    }


def _line(**kw):
    base = dict(is_labor=False, quantity=None, days=None, rate=None,
                agent_pct=0.0, estimated_total=None, est_ot=0.0,
                fringe_type=None, days_unit="days", rate_type="flat_day",
                days_per_week=5)
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
    r = calc_line(_line(is_labor=True, days=2, rate=1400.0, est_ot=100.0,
                        fringe_type="E", agent_pct=0.10), _fringe_cfgs())
    assert r["subtotal"] == 2900.00
    assert r["fringe_amount"] == 580.00
    assert r["agent_amount"] == 290.00
    assert r["total"] == 3770.00


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
    assert r["total"] == round(r["subtotal"] * 1.20, 2)
