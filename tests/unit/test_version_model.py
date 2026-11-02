"""Cleanup C — the version model (2026-11-02).

Pure tests (no app, no database) for the rules the budget page header,
the pickers and the mismatch scan now share, all in actuals.py:

  1. pick_current — "current" = newest of its kind: version_status='current'
     first, then the highest version_number, then the newest id; archived
     and is_actual rows never win
  2. working_exists_for_version — the header rule: a Working for THIS
     version number, not any Working at all
  3. version_strip_state — the three strip states (paired / ahead / none)
  4. the ONE variance key set: aliases from the old page + export keys,
     the label per key, the "+ = under" arithmetic
  5. line_overages — the "over plan" rule the mismatch scan applies to the
     live Working ($50 AND 10% over)

Run: python -m pytest tests/unit/test_version_model.py -q --confcutdir=tests/unit -o addopts=""
"""
import os
import sys
from types import SimpleNamespace as NS

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)

from actuals import (pick_current, working_exists_for_version, version_strip_state,   # noqa: E402
                     VARIANCE_BASES, VARIANCE_BASIS_ALIASES, VARIANCE_LABELS,
                     normalize_variance_basis, variance_value, line_overages)


def B(id, mode, status='current', vn=None, actual=False):
    return NS(id=id, budget_mode=mode, version_status=status, version_number=vn, is_actual=actual)


# ── 1. pick_current ────────────────────────────────────────────────────────

def test_current_flag_wins_over_a_higher_version_number():
    rows = [B(1, 'working', 'superseded', 1), B(2, 'working', 'current', 1), B(3, 'working', 'superseded', 2)]
    assert pick_current(rows, 'working').id == 2


def test_without_a_current_flag_the_newest_version_wins_then_the_newest_id():
    rows = [B(1, 'working', 'superseded', 1), B(2, 'working', 'superseded', 2), B(3, 'working', 'superseded', 2)]
    assert pick_current(rows, 'working').id == 3
    rows = [B(5, 'estimated', 'superseded', 1), B(4, 'estimated', 'superseded', 3)]
    assert pick_current(rows, 'estimated').id == 4


def test_archived_and_actual_rows_never_win():
    rows = [B(1, 'working', 'archived', 9), B(2, 'working', 'current', 1, actual=True), B(3, 'working', 'superseded', 1)]
    assert pick_current(rows, 'working').id == 3
    assert pick_current([B(1, 'working', 'archived', 1)], 'working') is None


def test_null_mode_counts_as_estimated_and_a_version_pair_can_differ():
    # NULL budget_mode is an Estimated (the old "NULL + parent_budget_id" =
    # Working filter is gone); Estimated v2 + Working v1 is a legal live pair.
    rows = [B(1, None, 'superseded', 1), B(2, 'working', 'current', 1), B(3, 'estimated', 'current', 2)]
    assert pick_current(rows, 'estimated').id == 3
    assert pick_current(rows, 'working').id == 2
    assert pick_current([B(7, None, 'current', 1)], 'estimated').id == 7
    assert pick_current([B(7, None, 'current', 1)], 'working') is None


# ── 2. working_exists_for_version ──────────────────────────────────────────

def test_working_for_this_version_not_any_working():
    rows = [B(1, 'estimated', 'superseded', 1), B(2, 'working', 'current', 1), B(3, 'estimated', 'current', 2)]
    assert working_exists_for_version(rows, 1) is True
    assert working_exists_for_version(rows, 2) is False      # Working v1 exists, but not for v2
    assert working_exists_for_version(rows, None) is True    # None reads as v1


def test_archived_or_actual_working_does_not_count():
    rows = [B(1, 'working', 'archived', 2), B(2, 'working', 'current', 2, actual=True)]
    assert working_exists_for_version(rows, 2) is False


# ── 3. version_strip_state ─────────────────────────────────────────────────

def test_strip_states():
    assert version_strip_state(B(1, 'estimated', vn=7), B(2, 'working', vn=7)) == 'paired'
    assert version_strip_state(B(1, 'estimated', vn=8), B(2, 'working', vn=7)) == 'ahead'
    assert version_strip_state(B(1, 'estimated', vn=1), None) == 'none'
    assert version_strip_state(None, None) == 'none'
    # Working newer than the Estimated on file (odd data) is still "paired"
    assert version_strip_state(B(1, 'estimated', vn=1), B(2, 'working', vn=2)) == 'paired'


# ── 4. the variance key set ────────────────────────────────────────────────

def test_one_key_set_and_every_legacy_spelling_maps_onto_it():
    assert VARIANCE_BASES == ('vs_estimated', 'vs_working', 'est_to_working')
    assert set(VARIANCE_LABELS) == set(VARIANCE_BASES)
    assert VARIANCE_LABELS['vs_working'] == 'Var vs Working'
    assert VARIANCE_LABELS['vs_estimated'] == 'Var vs Estimated'
    assert VARIANCE_LABELS['est_to_working'] == 'Est → Working'
    # old page keys
    assert normalize_variance_basis('e_vs_w') == 'est_to_working'
    assert normalize_variance_basis('w_vs_a') == 'vs_working'
    assert normalize_variance_basis('e_vs_a') == 'vs_estimated'
    # old export keys
    assert normalize_variance_basis('est_v_work') == 'est_to_working'
    assert normalize_variance_basis('work_v_act') == 'vs_working'
    assert normalize_variance_basis('est_v_act') == 'vs_estimated'
    # current keys pass through; junk → default
    for k in VARIANCE_BASES:
        assert normalize_variance_basis(k) == k
    assert normalize_variance_basis('nope') == 'vs_working'
    assert normalize_variance_basis(None, default='est_to_working') == 'est_to_working'
    assert set(VARIANCE_BASIS_ALIASES.values()) <= set(VARIANCE_BASES)


def test_plus_means_under_on_every_basis():
    e, w, a = 100.0, 120.0, 90.0
    assert variance_value('est_to_working', e, w, a) == -20.0     # Working over the estimate
    assert variance_value('vs_working', e, w, a) == 30.0          # spent less than Working
    assert variance_value('vs_estimated', e, w, a) == 10.0
    assert variance_value('e_vs_w', e, w, a) == -20.0             # alias


# ── 5. line_overages (mismatch scan on the live Working) ───────────────────

def test_line_overages_flags_fifty_dollars_and_ten_percent_over():
    planned = {1: 1000.0, 2: 100.0, 3: 0.0, 4: 400.0, 5: 200.0}
    actual = {1: 1150.0,     # $150 and 15% over → flagged
              2: 140.0,      # 40% over but only $40 → not flagged (needs $50)
              3: 500.0,      # no plan → skipped
              4: 430.0,      # $30 / 7.5% → not flagged
              5: 260.0}      # $60 and 30% → flagged
    out = line_overages(planned, actual)
    assert [(lid, over) for lid, _p, _a, over in out] == [(1, 150.0), (5, 60.0)]
    assert line_overages({}, {}) == []
