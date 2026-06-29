# Forensic + Red-Team Audit — Cleaver 2025 Working Budget

**Date:** 2026-06-29
**Scope:** Calculation engine correctness for the Working budget (`budget_calc.py`)
plus the persistence path in `app.py`. Forensic-accountant lens (does every line
add up?) and software red-team lens (where can the math be silently wrong?).

---

## Important scope note — what this audit could and could not test

Your literal Cleaver 2025 line items live in the production Postgres database
(`DATABASE_URL`, a non-synced secret per `render.yaml`). That database is **not
reachable** from the code-review environment, so I could not pull your actual
numbers. Instead I audited the **engine that computes every line** — if the math
is wrong here, every Cleaver line that flows through it is wrong, regardless of
the inputs.

**To reconcile your real numbers line-by-line, use the tool already built into
the app:**

```
GET /projects/<pid>/budget/<bid>/audit.json      (super-admin only)
```

`budget_audit_json` (app.py:4960) re-runs the calc engine against the DB exactly
as stored, cross-checks each line's arithmetic, reconciles section + grand-total
roll-ups, and reports `estimated_drift` / `working_drift` warnings. Run it
against the Cleaver 2025 Working budget and it will give you the per-line gut
check. **Caveat — see Finding E:** it shares one blind spot with the engine.

---

## Findings

### Finding A — Non-labor line with a blank `Days` field silently computes to $0  ★ highest priority

**The two code paths disagree on what a blank `Days` value means.**

- **Persist path** (`app.py:7134-7142`, `upsert_line`): `d = float(ln.days or 1)`
  → a blank/zero `Days` is treated as **1**, so `estimated_total = rate × qty × 1`.
- **Calc engine** (`budget_calc.py:799-816`, `calc_line`): `days = _float(line.days, 0.0)`
  → a blank/zero `Days` is treated as **0**, and because the guard is
  `if qty > 0 and days > 0 and rate > 0`, the line falls through to `pre = 0.0`.

Clearing the field stores `ln.days = 0` (`app.py:7070-7078`, `_ZERO_ON_BLANK`),
so this is reachable from the UI.

**Reproduced** (qty = 3, days = 0, rate = $200 — e.g. "3 drives @ $200, no day count"):

| Source | Value |
|---|---|
| `estimated_total` stored by `upsert_line` | **$600.00** |
| `calc_line` est_total (grid + Top Sheet) | **$0.00** |
| Discrepancy | **$600 understated in the working total** |

A purchase line you entered with a quantity and a rate but no day count reads as
**$0** in the grid and the Top Sheet, while the stored `estimated_total` says
$600. The number is ambiguous and depends on which code path a given view uses.

**Where to check in Cleaver 2025:** any non-labor line where the Days/Duration
cell is blank or 0. `audit.json` surfaces these as `estimated_drift` warnings
(stored ≠ computed). This is the first thing to grep your audit output for.

**This is a real internal inconsistency, not a judgment call** — the only
question is which behavior you want (treat blank days as 1, i.e. qty × rate; or
keep zeroing but fix the persist path so `estimated_total` is also $0). The two
paths must agree.

---

### Finding B — Weekly-rate labor lines round partial weeks UP to a full week

`calc_line_from_schedule` (budget_calc.py:1086):
`total_week_count += math.ceil(len(active) / days_per_week)`.

**Reproduced** (weekly rate $5,000, `days_per_week` = 5):

| Days scheduled | Weeks billed | Subtotal |
|---|---|---|
| 1 | 1 | $5,000 |
| 6 | 2 | $10,000 |

A single overage day past a 5-day week bills an entire **extra $5,000 week**. A
one-day pickup on a weekly-rate line bills a full week.

This may be intentional (weekly-rate minimums are real in production), but on a
larger crew it can materially overstate the budget. **Confirm it matches your
deal terms** for any weekly-rate roles in Cleaver 2025. If partial weeks should
prorate, this is the line to change.

---

### Finding C — Production Company Fee is charged on insurance & payroll burdens by default

Defaults (`models.py`): `company_fee_pct = 0.18`, `fee_excluded_sections = NULL`
(→ **every** section is fee-eligible), `workers_comp_pct = 0.03`,
`payroll_fee_pct = 0.0175`, `production_insurance` = pct @ `0.015`.

In `calc_top_sheet` the auto-calculated burdens are injected into their sections
(WC + Production Insurance → 6000; Payroll Fee → 6500), and the fee base includes
those sections unless you explicitly exclude them. Labor **fringes** are excluded
from the fee base by default (`fee_exclude_fringes = True`), but **workers' comp,
the payroll-service fee, and production insurance are not.**

Net effect by default: the 18% company fee is marked up on top of your workers'
comp, payroll fee, and production insurance. ShowBiz/MMB convention usually
treats insurance as pass-through (no production fee on it). **Verify against your
fee agreement;** if these should be exempt, tick sections 6000 / 6500 as
fee-exempt in budget Settings (`fee_excluded_sections`).

---

### Finding D — Account codes below 1000 are silently dropped from the Top Sheet

`section_for_code` (budget_calc.py:1249-1257) returns `None` for any code below
the first section (1000). Such a line is excluded from every section total and
from the grand total — **but** if it's a labor line it still feeds
`gross_labor_wages`, so it would still drive workers' comp / payroll-fee / company
fee. That asymmetry means a line could move the burden totals without ever
appearing in the subtotal it belongs to.

After the 2026-04 COA renumber all codes are ≥ 1000, so this should not bite a
clean budget — but a half-migrated or hand-edited line with a legacy code (e.g.
700) would vanish from the roll-up. Low real-world likelihood; genuine robustness
gap. `audit.json` would show this as a section reconciliation delta.

---

### Finding E — The built-in audit tool shares one blind spot with the engine (red-team)

`budget_audit_json`'s "independent" non-labor subtotal recompute (app.py:5098-5111)
is a **copy of `calc_line`'s formula**, including the same
`if qf>0 and dyf>0 and rtf>0 … else 0.0` gate. So for the Finding A case it
computes $0 and compares against the engine's $0 → **"PASS"**. The arithmetic
check cannot independently catch the blank-days zeroing because it isn't actually
independent of the assumption being tested.

It is *partially* saved by the separate `estimated_drift` warning
(app.py:5159-5167), which compares the stored `estimated_total` against the
computed total and would flag the $600-vs-$0 line as a warning. So: **read the
warnings, not just the PASS/FAIL.** A truly independent check would compute
`qty × rate` (no days gate) and compare.

---

## Recommended next steps

1. **Run `audit.json` against Cleaver 2025 Working** and pull every
   `estimated_drift` / `working_drift` warning and any section delta. Share the
   output and I'll reconcile it line-by-line.
2. **Decide the blank-days semantics (Finding A)** — qty × rate, or zero-and-fix-
   the-store. Then make the persist path and `calc_line` agree. This is the one
   unambiguous bug.
3. **Confirm policy on Findings B and C** (weekly rounding, fee on insurance/
   burdens) against your deal terms; both are one-line changes once decided.
4. I can implement any of the above and add regression tests (the blank-days case
   is currently untested — `tests/test_budget.py` has 7 tests, none covering it).

*No code was changed by this audit — financial-calc behavior shouldn't be altered
without your sign-off on intended semantics.*
