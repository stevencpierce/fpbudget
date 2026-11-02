"""Actuals Phase 4 pure helpers (ACTUALS_HANDOFF, Steven's decision of
Oct 30 2026: fringes live in 6500 Administrative, one line per department).

  1. resolve_fringe_line — which line is a labor department's Payroll
     Fringes line: the Phase 4 shape (fringe_for_section), else the
     pre-Phase-4 per-section line to adopt, never the old 6500 pot.
  2. parse_people_split — the payroll-parse tie-out rule: keep the split,
     book the difference to the person's line, report a delta over $1.
  3. build_line_picker_options — computed lines list under their own
     section (6500 / 6000) as targets, fringes by department then autos;
     a labor section offers no fringe row.

No Flask, no DB. Run: python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from actuals import (resolve_fringe_line, parse_people_split,  # noqa: E402
                     build_line_picker_options)


def _fl(id, code, for_sec=None, tag="fringe_rollup"):
    return NS(id=id, account_code=code, line_tag=tag, fringe_for_section=for_sec,
              description="Payroll Fringes", account_name="Administrative",
              parent_line_id=None)


# ── 1. per-department line resolution ───────────────────────────────────────

def test_resolves_phase4_line_by_department():
    lines = [_fl(1, 6500, 2000), _fl(2, 6500, 2100), _fl(3, 2601, tag=None)]
    assert resolve_fringe_line(lines, 2100) == (lines[1], False)
    assert resolve_fringe_line(lines, "2000") == (lines[0], False)   # str code ok
    assert resolve_fringe_line(lines, 2600) == (None, False)


def test_adopts_legacy_per_section_line_but_never_the_old_6500_pot():
    legacy_2000 = _fl(7, 2000)            # Oct 21 shape: in the labor section
    old_pot = _fl(8, 6500)                # Oct 7 shape: one pot, no department
    lines = [old_pot, legacy_2000]
    ln, legacy = resolve_fringe_line(lines, 2000)
    assert ln is legacy_2000 and legacy is True
    assert resolve_fringe_line(lines, 6500) == (None, False)   # the pot is not a department
    assert resolve_fringe_line(lines, 2100) == (None, False)


def test_phase4_line_wins_over_a_lingering_legacy_line():
    lines = [_fl(7, 2000), _fl(9, 6500, 2000)]
    assert resolve_fringe_line(lines, 2000) == (lines[1], False)


def test_resolver_tolerates_bad_input():
    assert resolve_fringe_line(None, 2000) == (None, False)
    assert resolve_fringe_line([_fl(1, 6500, 2000)], "x") == (None, False)


# ── 2. parse tie-out ────────────────────────────────────────────────────────

def test_split_ties_out_exactly():
    r = parse_people_split(1250.00, 1000.00, 150.00, 60.00, 40.00)
    assert r == {"wages": 1000.00, "fringes": 150.00, "workers_comp": 60.00,
                 "platform_fee": 40.00, "delta": 0.0}


def test_split_within_a_dollar_absorbs_cents_on_the_person_no_flag():
    r = parse_people_split(1250.40, 1000.00, 150.00, 60.00, 40.00)
    assert r["wages"] == 1000.40          # the 40¢ rides on the person's line
    assert r["fringes"] == 150.00 and r["workers_comp"] == 60.00
    assert r["delta"] == 0.0              # no flag under $1


def test_split_over_a_dollar_keeps_split_books_difference_and_flags():
    # Old behavior merged everything back onto the person silently; now the
    # split stays, the person's line takes the difference, delta is reported.
    r = parse_people_split(1300.00, 1000.00, 150.00, 60.00, 40.00)
    assert r["wages"] == 1050.00
    assert r["fringes"] == 150.00 and r["workers_comp"] == 60.00 and r["platform_fee"] == 40.00
    assert r["delta"] == 50.00
    assert round(r["wages"] + r["fringes"] + r["workers_comp"] + r["platform_fee"], 2) == 1300.00


def test_split_negative_delta_is_reported_too():
    r = parse_people_split(1200.00, 1000.00, 150.00, 60.00, 40.00)
    assert r["wages"] == 950.00 and r["delta"] == -50.00


def test_no_split_means_whole_amount_is_wages():
    r = parse_people_split(1250.00, None, None)
    assert r == {"wages": 1250.00, "fringes": 0.0, "workers_comp": 0.0,
                 "platform_fee": 0.0, "delta": 0.0}
    r = parse_people_split("1250", "abc", 10)           # unparsable wages → no split
    assert r["wages"] == 1250.00 and r["fringes"] == 0.0


def test_missing_wc_and_fee_default_to_zero():
    r = parse_people_split(1150.00, 1000.00, 150.00)
    assert r["workers_comp"] == 0.0 and r["platform_fee"] == 0.0 and r["delta"] == 0.0


# ── 3. picker: computed lines under their own section ───────────────────────

def _ln(id, code, desc, parent=None, tag=None, name="Section", **kw):
    return NS(id=id, account_code=code, description=desc, account_name=name,
              parent_line_id=parent, line_tag=tag, **kw)


def test_picker_lists_fringes_by_department_then_autos_under_6500_as_targets():
    groups = [
        {"code": 2000, "name": "Production Staff", "lines": [
            _ln(2, 2001, "Director of Photography")]},
        {"code": 6500, "name": "Administrative", "lines": [
            _ln(9, 6500, "Payroll Service Fee", tag="auto_calc", auto_kind="fee"),
            _ln(5, 6500, "Payroll Fringes — Production Staff", tag="fringe_rollup", fringe_for_section=2000),
            _ln(4, 6501, "Office supplies"),
            _ln(6, 6500, "Payroll Fringes — Talent", tag="fringe_rollup", fringe_for_section=2100),
            _ln(10, 6500, "Production Company Fee", tag="auto_calc", auto_kind="pcf"),
        ]},
    ]
    out = build_line_picker_options(groups)
    assert [(o["id"], o["kind"]) for o in out[0]["opts"]] == [(2, "top")]   # no fringe row in labor
    opts = out[1]["opts"]
    assert [(o["id"], o["kind"]) for o in opts] == [
        (4, "top"), (5, "fringe"), (6, "fringe"), (9, "auto"), (10, "auto")]
    assert opts[1]["label"] == "Payroll Fringes — Production Staff"   # department name, no code
    assert opts[3]["label"] == "Payroll Service Fee"
