"""
FPBudget Calculation Engine
Handles labor rate / OT / fringe / company fee calculations.
"""

from decimal import Decimal

# ── COA sections (used for Top Sheet grouping) ────────────────────────────────

FP_COA_SECTIONS = [
    (100,   "Pre-Production Locations"),
    (600,   "Above the Line"),
    (700,   "Talent"),
    (800,   "Rehearsal"),
    (900,   "Casting"),
    (1000,  "Production Staff"),
    (1200,  "Post-Production Staff"),
    (2000,  "Camera Equipment"),
    (3000,  "Grip & Electric"),
    (3100,  "Processing"),
    (3200,  "Control Room"),
    (3300,  "Sound"),
    (4000,  "Art"),
    (4500,  "Hair & Makeup"),
    (5000,  "Wardrobe"),
    (6000,  "Transportation"),
    (7000,  "Travel"),
    (7500,  "Shipping"),
    (8000,  "Production Meals / Craft Services"),
    (8500,  "Sanitation"),
    (9000,  "Location"),
    (11000, "Post-Production Equipment"),
    (11500, "Post-Production Locations"),
    (11600, "Post-Production Services"),
    (12000, "Licensing"),
    (12500, "Composition & Mastering"),
    (13000, "Distribution"),
    (13200, "Software & Office Supplies"),
    (14000, "Insurance"),
    (15000, "Administrative"),
    (16000, "Marketing"),
    (17000, "EPK / Behind the Scenes"),
    (18000, "Title Sequence"),
    (19000, "Residuals"),
    (20000, "Misc"),
    (20500, "Production Company Fee"),
]

# ── Default fringe rates (seeded on startup) ─────────────────────────────────

FP_FRINGE_DEFAULTS = [
    # (fringe_type, label,          rate,   is_flat, flat_amount, ot_applies)
    ("E", "Exempt",         0.0000, False, None,  False),  # Exempt: no OT calculated
    ("N", "Non Union",      0.1937, False, None,  True),
    ("L", "Loan Out",       0.0000, True,  18.00, True),   # $18 flat per person per day
    ("U", "Union Generic",  0.3800, False, None,  True),
    ("S", "SAG",            0.4420, False, None,  True),
    ("I", "IATSE",          0.3800, False, None,  True),
    ("D", "DGA",            0.1595, False, None,  True),
]

# ── Payroll profiles (seeded on startup) ──────────────────────────────────────
# (name, description, daily_st, daily_dt, ot_mult, dt_mult, weekly_st, weekly_ot_mult, seventh_day, week_start, sort)
SYSTEM_PAYROLL_PROFILES = [
    ("Flat Rate / No OT",                "Flat day rate — no overtime calculation",
     None,  None, 1.5, 2.0, None, 1.5, None,      0, 0),
    ("Federal 40-Hour (FLSA)",           "OT after 40 hrs/week only (Federal)",
     None,  None, 1.5, 2.0, 40.0, 1.5, None,      0, 1),
    ("Commercial 8hr Day",               "OT after 8 hrs/day, DT after 12 hrs/day",
     8.0,  12.0,  1.5, 2.0, None, 1.5, None,      0, 2),
    ("Commercial 10hr Day",              "OT after 10 hrs/day, DT after 12 hrs/day",
     10.0, 12.0,  1.5, 2.0, None, 1.5, None,      0, 3),
    ("New York 40-Hour",                 "OT after 40 hrs/week (New York State)",
     None,  None, 1.5, 2.0, 40.0, 1.5, None,      0, 4),
    ("California 8/40",                  "OT after 8 hrs/day AND 40 hrs/week, DT after 12 hrs/day, 7th day all OT",
     8.0,  12.0,  1.5, 2.0, 40.0, 1.5, "ot_all",  0, 5),
    ("California 10/40 (Entertainment)", "OT after 10 hrs/day AND 40 hrs/week, DT after 14 hrs/day, 7th day all OT",
     10.0, 14.0,  1.5, 2.0, 40.0, 1.5, "ot_all",  0, 6),
]

# Hours per standard day by rate_type (None = flat rate, no hourly breakdown)
RATE_TYPE_HOURS = {
    'day_8':        8.0,
    'day_10':      10.0,
    'day_12':      12.0,
    'flat_day':    None,
    'flat_project': None,
    'hourly':       1.0,
    'custom':       None,
    'week':         None,   # weekly flat rate — no hourly OT; schedule drives week count
}

# ── Day type multipliers ──────────────────────────────────────────────────────

DAY_TYPE_MULTIPLIERS = {
    "work":     1.0,
    "travel":   1.0,
    "hold":     0.5,
    "half":     0.5,
    "off":      0.0,
    "kill_fee": 0.2,
    "custom":   1.0,   # uses rate_multiplier column directly
}


# ── Role group classification (for travel budget linking) ─────────────────────

_ROLE_GROUP_CODES = {700: "talent", 600: "atl"}

def get_role_group(account_code):
    """Map a COA code to a travel role group: 'talent' | 'atl' | 'crew'."""
    return _ROLE_GROUP_CODES.get(int(account_code or 0), "crew")


# ── Schedule-driven auto-created line definitions ─────────────────────────────
# Maps line_tag → (account_code, account_name, description, default_unit_rate, section_sort_order)
# section_sort_order controls ordering within account section (lower = first)
SCHEDULE_LINE_DEFS = {
    # Production Meals / Craft Services (8000) — must stay in this order
    # quantity = headcount (auto from schedule), days = shoot days with flag, rate = per-person cost
    "craft_services":          (8000, "Production Meals / Craft Services", "Craft Services",            20.00, 0),
    "meal_courtesy_breakfast": (8000, "Production Meals / Craft Services", "Courtesy Breakfast",        12.00, 1),
    "meal_first":              (8000, "Production Meals / Craft Services", "First Meal",                25.00, 2),
    "meal_second":             (8000, "Production Meals / Craft Services", "Second Meal",               25.00, 3),
    "working_meal":            (8000, "Production Meals / Craft Services", "Working Meals",             25.00, 4),
    # Travel (7000)
    "hotel_talent":            (7000, "Travel", "Hotel — Talent",                                      200.00, 10),
    "hotel_atl":               (7000, "Travel", "Hotel — Above the Line",                              250.00, 11),
    "hotel_crew":              (7000, "Travel", "Hotel — Crew",                                        150.00, 12),
    "flight_talent":           (7000, "Travel", "Flights — Talent",                                    500.00, 20),
    "flight_atl":              (7000, "Travel", "Flights — Above the Line",                            600.00, 21),
    "flight_crew":             (7000, "Travel", "Flights — Crew",                                      400.00, 22),
    "mileage_talent":          (7000, "Travel", "Mileage — Talent",                                     50.00, 30),
    "mileage_atl":             (7000, "Travel", "Mileage — Above the Line",                             50.00, 31),
    "mileage_crew":            (7000, "Travel", "Mileage — Crew",                                       50.00, 32),
    # Per diem: foundation — quantity driven by per_diem cell_flag; meal offsets deferred
    "per_diem":                (7000, "Travel", "Per Diem",                                             75.00, 40),
}

# Canonical order for meals within the 8000 section
_MEAL_TAG_ORDER = ["craft_services", "meal_courtesy_breakfast", "meal_first", "meal_second", "working_meal"]


def sync_schedule_driven_lines(budget_id, db_session):
    """
    Scan schedule data for this budget and keep auto-created budget lines
    (meals, hotel, flights, mileage, per diem) up-to-date.

    Called after every gantt day save or production day (meal) toggle.

    For each SCHEDULE_LINE_DEFS tag:
      - Count instances from schedule cell_flags / ProductionDay flags
      - Find or create the corresponding BudgetLine (matched by line_tag)
      - Set quantity = count, estimated_total = count × unit_rate

    Per-diem meal-offset logic (breakfast/lunch/dinner components) is
    intentionally deferred. When implemented it will adjust the quantity
    based on which meal components are NOT provided on that day by ProductionDay.
    """
    import json as _json
    from models import Budget, BudgetLine, ScheduleDay, ProductionDay

    budget = db_session.query(Budget).filter_by(id=budget_id).first()
    if not budget:
        return

    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    all_lines     = db_session.query(BudgetLine).filter_by(budget_id=budget_id).all()
    labor_by_id   = {ln.id: ln for ln in all_lines if ln.is_labor}

    # Track both days (number of flagged columns) and headcount (rows on those
    # days) separately for each tag. The BudgetLine stores days × qty × rate
    # which matches the user's mental model:
    #   "If First Meal is checked in 2 columns and there are 10 people
    #    scheduled those days, that's 2 days × 10 qty × rate"
    #
    # day_dates[tag] = set of dates flagged for this tag
    # day_hcs[tag]   = list of per-day headcounts (same length as day_dates)
    day_dates = {tag: set() for tag in SCHEDULE_LINE_DEFS}
    day_hcs   = {tag: [] for tag in SCHEDULE_LINE_DEFS}

    # Count flags from ScheduleDay rows (current schedule mode only).
    # ALSO accept NULL schedule_mode rows (legacy data pre-schedule_mode column)
    # so older budgets still sync meal/travel totals.
    from sqlalchemy import or_ as _or
    sched_days = db_session.query(ScheduleDay).filter(
        ScheduleDay.budget_id == budget_id,
        _or(ScheduleDay.schedule_mode == sched_mode,
            ScheduleDay.schedule_mode == None),
    ).all()

    # Build date → crew headcount map (non-off days only)
    date_headcount = {}
    for sd in sched_days:
        if sd.day_type != 'off':
            date_headcount[sd.date] = date_headcount.get(sd.date, 0) + 1

    # Craft services = every shoot day; headcount = crew that day
    for d, hc in date_headcount.items():
        day_dates['craft_services'].add(d)
        day_hcs['craft_services'].append(hc)

    # Per-crew-cell flags (working_meal, hotel, flight, mileage, per_diem)
    # Aggregate by (tag, date): count distinct dates as days; headcount is the
    # number of crew with that flag checked on each date.
    per_tag_date_crew = {}  # {tag: {date: crew_count}}
    for sd in sched_days:
        if sd.day_type == 'off':
            continue
        flags = {}
        if sd.cell_flags:
            try:
                flags = _json.loads(sd.cell_flags)
            except (ValueError, TypeError):
                flags = {}

        parent = labor_by_id.get(sd.budget_line_id)
        # Travel role_group is a classifier: 'talent' | 'atl' | 'crew'.
        # Must be derived from account_code, NOT from parent.role_group which
        # stores the sub-department name ('Direction / AD', 'Camera', 'Sound')
        # — those produce invalid tag keys like 'flight_Direction / AD'.
        rg = get_role_group(parent.account_code) if parent else 'crew'

        # Guard: rg must be one of 'talent' | 'atl' | 'crew' to yield a valid tag.
        # If anything else (shouldn't happen, but defensive), default to 'crew'.
        if rg not in ('talent', 'atl', 'crew'):
            rg = 'crew'

        flag_tag_map = {
            'working_meal': 'working_meal',
            'hotel':        f'hotel_{rg}',
            'flight':       f'flight_{rg}',
            'mileage':      f'mileage_{rg}',
            'per_diem':     'per_diem',
        }
        for flag_key, tag in flag_tag_map.items():
            if flags.get(flag_key) and tag in SCHEDULE_LINE_DEFS:
                per_tag_date_crew.setdefault(tag, {})
                per_tag_date_crew[tag][sd.date] = per_tag_date_crew[tag].get(sd.date, 0) + 1

    # Collapse per-tag per-date crew counts into day_dates + day_hcs
    for tag, date_map in per_tag_date_crew.items():
        for d, crew_on_day in date_map.items():
            day_dates[tag].add(d)
            day_hcs[tag].append(crew_on_day)

    # Meal flags on ProductionDay: one flag per date (not per crew member).
    # Use the scheduled headcount for that date as qty.
    prod_days = db_session.query(ProductionDay).filter(
        ProductionDay.budget_id == budget_id,
        _or(ProductionDay.schedule_mode == sched_mode,
            ProductionDay.schedule_mode == None),
    ).all()
    for pd in prod_days:
        hc = date_headcount.get(pd.date, 0) or 0
        # Skip days with no one scheduled — those columns don't really exist
        if hc == 0:
            continue
        if pd.courtesy_breakfast:
            day_dates['meal_courtesy_breakfast'].add(pd.date)
            day_hcs['meal_courtesy_breakfast'].append(hc)
        if pd.first_meal:
            day_dates['meal_first'].add(pd.date)
            day_hcs['meal_first'].append(hc)
        if pd.second_meal:
            day_dates['meal_second'].add(pd.date)
            day_hcs['meal_second'].append(hc)

    # Derive: days = number of flagged columns; headcount = average rows per day
    counts      = {tag: len(day_dates[tag]) for tag in SCHEDULE_LINE_DEFS}
    headcounts  = {}
    for tag in SCHEDULE_LINE_DEFS:
        hcs = day_hcs[tag]
        headcounts[tag] = round(sum(hcs) / len(hcs)) if hcs else 0

    # Existing auto lines by tag
    existing_auto = {ln.line_tag: ln for ln in all_lines
                     if getattr(ln, 'line_tag', None) in SCHEDULE_LINE_DEFS}

    # Adopt any untagged meal/travel lines that match description+account_code.
    # This prevents duplicates when a template-seeded line already exists.
    for tag, defn in SCHEDULE_LINE_DEFS.items():
        if tag not in existing_auto:
            ac, an, desc = defn[0], defn[1], defn[2]
            orphan = next(
                (ln for ln in all_lines
                 if int(ln.account_code) == ac
                 and (ln.description or '').strip().lower() == desc.lower()
                 and not ln.line_tag),
                None
            )
            if orphan:
                orphan.line_tag = tag
                existing_auto[tag] = orphan

    for tag, count in counts.items():
        if count == 0 and tag not in existing_auto:
            continue  # Don't create zero-count lines that were never used

        ac, an, desc, default_rate, section_sort = SCHEDULE_LINE_DEFS[tag]
        hc = headcounts.get(tag, 0) or 1  # default to 1 if no headcount derived

        if tag in existing_auto:
            ln = existing_auto[tag]
            # User opted out of auto-calc for this line — leave it alone
            if getattr(ln, 'sync_omit', False):
                continue
        else:
            ln = BudgetLine(
                budget_id=budget_id,
                account_code=ac,
                account_name=an,
                description=desc,
                is_labor=False,
                line_tag=tag,
                unit_rate=default_rate,
                rate=default_rate,
                quantity=hc,
                days=max(count, 1),
                sort_order=section_sort,
            )
            db_session.add(ln)
            db_session.flush()

        # Always update days = number of flagged columns (shoot days).
        # Update quantity = headcount of rows on those days — but if the user
        # manually tweaked quantity (unit_rate marker remembers auto-computed hc),
        # only update when the auto-detected hc has CHANGED.
        ln.days = count if count > 0 else 1
        unit_r = _float(getattr(ln, 'unit_rate', None), default_rate) or default_rate
        # Ensure rate is set (may be 0 on older rows created before this fix)
        if not _float(getattr(ln, 'rate', None)):
            ln.rate = unit_r
        # Auto-update quantity to the rows-on-those-days average. User can still
        # override by editing the line (it will get overwritten on next sync —
        # for locked values users should use the sync_omit context menu).
        if hc > 0:
            ln.quantity = hc
        qty_val = _float(getattr(ln, 'quantity', None), 1.0) or 1.0
        effective_rate = _float(getattr(ln, 'rate', None)) or unit_r
        # estimated_total = qty × days × rate (matches user's mental model)
        ln.estimated_total = round(qty_val * count * effective_rate, 2)

    # Re-sort the meals section: Courtesy Breakfast → First Meal → Second Meal →
    # Working Meals → Craft Services → everything else (by current sort_order).
    meal_lines = {ln.line_tag: ln for ln in all_lines
                  if int(getattr(ln, 'account_code', None) or 0) == 8000}
    ordered_meal_lines = []
    for t in _MEAL_TAG_ORDER:
        if t in meal_lines:
            ordered_meal_lines.append(meal_lines[t])
    # Append any non-tagged 8000 lines (e.g. Craft Services) in their existing order
    non_tagged_8000 = sorted(
        [ln for ln in all_lines
         if int(getattr(ln, 'account_code', None) or 0) == 8000
         and ln.line_tag not in _MEAL_TAG_ORDER],
        key=lambda x: (x.sort_order or 0, x.id)
    )
    ordered_meal_lines.extend(non_tagged_8000)
    for i, ln in enumerate(ordered_meal_lines):
        ln.sort_order = i

    try:
        db_session.commit()
    except Exception:
        db_session.rollback()

    # Log summary so Render logs show what this sync actually did
    try:
        import logging as _log
        nz = {k: (counts[k], headcounts.get(k, 0))
              for k in counts if counts[k] > 0}
        if nz:
            _log.info("[sync] budget=%s mode=%s sched_days=%d prod_days=%d days×qty=%s",
                      budget_id, sched_mode, len(sched_days), len(prod_days), nz)
    except Exception:
        pass


def _float(val, default=0.0):
    """Safe float conversion from Decimal or None."""
    if val is None:
        return default
    return float(val)


def get_fringe_configs(db_session, project_id=None):
    """
    Return dict keyed by fringe_type.
    Project-level rows override global rows (project_id=None).
    """
    from models import FringeConfig
    rows = db_session.query(FringeConfig).filter(
        (FringeConfig.project_id == None) | (FringeConfig.project_id == project_id)
    ).all()
    # global defaults first, then project overrides on top
    result = {}
    for r in sorted(rows, key=lambda x: (x.project_id is not None)):
        result[r.fringe_type] = r
    return result


def calc_day_labor_hours(total_hours, daily_st, daily_dt):
    """
    Split total_hours for one day into (straight, ot, dt).
    daily_st: daily straight-time threshold (None = no daily OT rule)
    daily_dt: daily DT threshold (None = no DT, all overflow is OT)
    """
    if daily_st is None:
        return (total_hours, 0.0, 0.0)
    st_f = float(daily_st)
    straight = min(total_hours, st_f)
    remaining = max(0.0, total_hours - straight)
    if daily_dt is None:
        return (straight, remaining, 0.0)
    dt_f = float(daily_dt)
    ot_cap = max(0.0, dt_f - st_f)
    ot = min(remaining, ot_cap)
    dt = max(0.0, total_hours - dt_f)
    return (straight, ot, dt)


def _effective_days(line):
    """
    Return the effective number of billing units for a non-schedule labor line.
    For rate_type='week', the 'days' column stores weeks directly (days_unit should be 'weeks').
    For day-based rate types with days_unit='weeks', convert weeks → days using days_per_week.
    """
    raw    = _float(line.days, 1.0)
    unit   = getattr(line, 'days_unit', 'days') or 'days'
    rt     = (getattr(line, 'rate_type', None) or 'flat_day')
    dpw    = max(_float(getattr(line, 'days_per_week', 5), 5.0), 1.0)

    if rt == 'week':
        # weekly rate: raw is already the number of weeks (regardless of days_unit)
        return raw
    if unit == 'weeks':
        # row is week-based but has a daily rate type → convert to days for daily billing
        return raw * dpw
    return raw


import json as _json

# ── Phase slot auto-mapping by COA code ───────────────────────────────────────


def calc_line(line, fringe_configs):
    """
    Calculate one BudgetLine.
    Returns dict: subtotal, fringe_amount, agent_amount, total, est_total
    For rate_type='week': rate is per-week, days column = number of weeks.
    """
    if not line.is_labor:
        qty      = _float(line.quantity, 1.0)
        days     = _float(line.days, 1.0)
        rate     = _float(line.rate, 0.0)
        discount = _float(line.agent_pct, 0.0)   # stored as fraction (0.15 = 15%)
        # Pre-discount subtotal: qty × days × rate, or fall back to estimated_total
        if rate > 0:
            pre = round(qty * days * rate, 2)
        else:
            pre = _float(line.estimated_total)
        disc_amt = round(pre * discount, 2)
        est      = round(pre - disc_amt, 2)
        return {"subtotal": pre, "fringe_amount": 0.0, "agent_amount": disc_amt,
                "total": est, "est_total": est}

    eff_days = _effective_days(line)
    base     = _float(line.quantity) * eff_days * _float(line.rate)
    ot       = _float(line.est_ot)
    subtotal = base + ot

    cfg = fringe_configs.get(line.fringe_type)
    if cfg and cfg.is_flat:
        # Flat fringe per person per billing unit
        fringe_amount = _float(line.quantity) * eff_days * _float(cfg.flat_amount)
    elif cfg:
        fringe_amount = subtotal * _float(cfg.rate)
    else:
        fringe_amount = 0.0

    agent_amount = subtotal * _float(line.agent_pct)
    total = subtotal + fringe_amount + agent_amount

    return {
        "subtotal":      round(subtotal, 2),
        "fringe_amount": round(fringe_amount, 2),
        "agent_amount":  round(agent_amount, 2),
        "total":         round(total, 2),
        "est_total":     round(total, 2),
    }


def calc_days_ot_status(rate_type, schedule_days, payroll_profile=None, payroll_week_start=0, ot_applies=True):
    """
    Compute per-day OT/DT status for Gantt cell highlighting.
    Returns dict: {date_iso: 'dt' | 'ot' | None}
    ot_applies: if False (e.g. Exempt fringe), returns empty dict — no OT for this line.
    """
    from datetime import timedelta
    from collections import defaultdict

    result = {}

    st_hours_per_day = RATE_TYPE_HOURS.get(rate_type)

    # Always highlight cells that have manually-set est_ot_hours, regardless of
    # payroll profile or fringe type.  This covers flat-rate Talent lines, Exempt
    # fringe, and any line whose budget has no payroll profile assigned.
    if not st_hours_per_day or not payroll_profile or not ot_applies:
        for d in schedule_days:
            if d.day_type not in ('work', 'travel'):
                continue
            if _float(getattr(d, 'est_ot_hours', None), 0.0) > 0:
                result[d.date.isoformat()] = 'ot'
        return result

    week_start   = int(payroll_week_start or 0)
    daily_st     = payroll_profile.daily_st_hours
    daily_dt     = payroll_profile.daily_dt_hours
    weekly_st    = payroll_profile.weekly_st_hours
    seventh_rule = payroll_profile.seventh_day_rule

    def get_week_key(date_val):
        dow = date_val.weekday()
        days_back = (dow - week_start) % 7
        return date_val - timedelta(days=int(days_back))

    weekly_st_accum  = defaultdict(float)
    weekly_days_work = defaultdict(list)

    for d in sorted(schedule_days, key=lambda x: x.date):
        if d.day_type not in ('work', 'travel'):
            continue

        ot_added  = _float(getattr(d, 'est_ot_hours', None), 0.0)
        total_hrs = st_hours_per_day + ot_added

        wk = get_week_key(d.date)
        weekly_days_work[wk].append(d.date)
        is_seventh = (seventh_rule == 'ot_all' and len(weekly_days_work[wk]) == 7)

        if is_seventh:
            st_hrs = 0.0
            ot_hrs = min(total_hrs, 8.0)
            dt_hrs = max(0.0, total_hrs - 8.0)
        else:
            st_hrs, ot_hrs, dt_hrs = calc_day_labor_hours(total_hrs, daily_st, daily_dt)
            if weekly_st is not None:
                weekly_st_f  = float(weekly_st)
                accum        = weekly_st_accum[wk]
                remaining_st = max(0.0, weekly_st_f - accum)
                if st_hrs > remaining_st:
                    overflow = st_hrs - remaining_st
                    st_hrs   = remaining_st
                    ot_hrs  += overflow
                weekly_st_accum[wk] += st_hrs

        if dt_hrs > 0:
            result[d.date.isoformat()] = 'dt'
        elif ot_hrs > 0:
            result[d.date.isoformat()] = 'ot'
        else:
            result[d.date.isoformat()] = None

    return result


def _run_payroll_calc(rate, rate_type, qty, schedule_days, payroll_profile, payroll_week_start, ot_applies=True):
    """
    Shared payroll calculation core. Returns (st_base, ot_base, dt_base, day_count).
    All three amounts are BEFORE multiplying by qty.
    ot_applies: if False (e.g. Exempt fringe), treat as flat rate — no OT/DT regardless of profile.
    """
    from datetime import timedelta
    from collections import defaultdict

    # If OT is disabled for this fringe type, force flat-rate mode
    effective_profile = payroll_profile if ot_applies else None
    st_hours_per_day = RATE_TYPE_HOURS.get(rate_type) if effective_profile else None
    use_hourly       = (st_hours_per_day is not None and st_hours_per_day > 0)
    week_start       = int(payroll_week_start or 0)

    def get_week_key(date_val):
        dow = date_val.weekday()
        days_back = (dow - week_start) % 7
        return date_val - timedelta(days=int(days_back))

    sorted_days = sorted(schedule_days, key=lambda d: d.date)
    day_count   = len(sorted_days)
    st_base = ot_base = dt_base = 0.0

    if use_hourly:
        hourly_rate  = rate / st_hours_per_day
        ot_mult      = _float(effective_profile.ot_multiplier, 1.5)
        dt_mult      = _float(effective_profile.dt_multiplier, 2.0)
        daily_st     = effective_profile.daily_st_hours
        daily_dt     = effective_profile.daily_dt_hours
        weekly_st    = effective_profile.weekly_st_hours
        seventh_rule = effective_profile.seventh_day_rule

        weekly_st_accum  = defaultdict(float)
        weekly_days_work = defaultdict(list)

        for d in sorted_days:
            mult = DAY_TYPE_MULTIPLIERS.get(d.day_type, 0.0)
            if d.day_type == 'off' or mult == 0.0:
                continue

            if d.day_type not in ('work', 'travel'):
                m = _float(d.rate_multiplier, 1.0) if d.day_type == 'custom' else mult
                st_base += rate * m
                continue

            ot_added  = _float(getattr(d, 'est_ot_hours', None), 0.0)
            total_hrs = st_hours_per_day + ot_added

            wk = get_week_key(d.date)
            weekly_days_work[wk].append(d.date)
            is_seventh = (seventh_rule == 'ot_all' and len(weekly_days_work[wk]) == 7)

            if is_seventh:
                st_hrs = 0.0
                ot_hrs = min(total_hrs, 8.0)
                dt_hrs = max(0.0, total_hrs - 8.0)
            else:
                st_hrs, ot_hrs, dt_hrs = calc_day_labor_hours(total_hrs, daily_st, daily_dt)
                if weekly_st is not None:
                    weekly_st_f  = float(weekly_st)
                    accum        = weekly_st_accum[wk]
                    remaining_st = max(0.0, weekly_st_f - accum)
                    if st_hrs > remaining_st:
                        overflow = st_hrs - remaining_st
                        st_hrs   = remaining_st
                        ot_hrs  += overflow
                    weekly_st_accum[wk] += st_hrs

            st_base += hourly_rate * st_hrs
            ot_base += hourly_rate * ot_mult * ot_hrs
            dt_base += hourly_rate * dt_mult * dt_hrs

    else:
        for d in sorted_days:
            mult = DAY_TYPE_MULTIPLIERS.get(d.day_type, 0.0)
            if d.day_type == 'custom':
                mult = _float(d.rate_multiplier, 1.0)
            st_base += rate * mult

    return st_base * qty, ot_base * qty, dt_base * qty, day_count


def calc_line_from_schedule(line, schedule_days, fringe_configs,
                             payroll_profile=None, payroll_week_start=0):
    """
    Derive line total from ScheduleDay rows.

    Key fix: when a line has qty > 1, each crew_instance gets its own set of
    ScheduleDay rows. We compute OT/DT PER INSTANCE to avoid inflating the
    weekly hours accumulator (e.g. 2 people × 5 days was incorrectly treated
    as 10 hours of one person, triggering OT on day 3 of the week under Fed40).

    For rate_type='week': counts scheduled days → billing weeks per instance.
    """
    import math as _math
    from collections import defaultdict

    rate = _float(line.rate)
    qty  = _float(line.quantity, 1.0)

    cfg        = fringe_configs.get(line.fringe_type) if fringe_configs else None
    ot_applies = getattr(cfg, 'ot_applies', True) if cfg is not None else True

    if not schedule_days:
        # No schedule: fall back to manual qty × days × rate via calc_line
        return calc_line(line, fringe_configs)

    # Group days by crew_instance so each person's OT/weekly accumulation is independent
    inst_map = defaultdict(list)
    for d in schedule_days:
        inst_map[d.crew_instance or 1].append(d)

    num_instances = len(inst_map)
    total_day_count = sum(len(v) for v in inst_map.values())

    # ── Weekly flat rate: convert scheduled days → weeks per instance ─────────
    if (getattr(line, 'rate_type', None) or '') == 'week':
        dpw = max(_float(getattr(line, 'days_per_week', 5), 5.0), 1.0)
        total_week_count  = 0
        active_day_count  = 0
        for i_days in inst_map.values():
            active = [d for d in i_days
                      if d.day_type != 'off' and DAY_TYPE_MULTIPLIERS.get(d.day_type, 0.0) > 0]
            active_day_count  += len(active)
            total_week_count  += _math.ceil(len(active) / dpw) if active else 0
        # If only 1 instance scheduled but qty > 1, scale by qty (backward compat)
        if num_instances == 1 and qty > 1:
            total_week_count = round(total_week_count * qty)
            active_day_count = round(active_day_count * qty)
        base      = total_week_count * rate
        legacy_ot = 0.0  # est_ot is a manual-mode override; ignored in schedule-driven calc
        subtotal  = base
        if cfg and cfg.is_flat:
            fringe_amount = active_day_count * _float(cfg.flat_amount)
        elif cfg:
            fringe_amount = subtotal * _float(cfg.rate)
        else:
            fringe_amount = 0.0
        agent_amount = subtotal * _float(line.agent_pct)
        total = subtotal + fringe_amount + agent_amount
        return {
            "subtotal":      round(subtotal, 2),
            "st_amount":     round(base, 2),
            "ot_amount":     round(legacy_ot, 2),
            "dt_amount":     0.0,
            "fringe_amount": round(fringe_amount, 2),
            "agent_amount":  round(agent_amount, 2),
            "total":         round(total, 2),
            "est_total":     round(total, 2),
            "day_count":     active_day_count,
            "week_count":    total_week_count,
        }
    # ─────────────────────────────────────────────────────────────────────────

    # Compute per-instance payroll (each instance = 1 person)
    st_base = ot_base = dt_base = 0.0
    for i_days in inst_map.values():
        i_st, i_ot, i_dt, _ = _run_payroll_calc(
            rate, line.rate_type, 1, i_days, payroll_profile, payroll_week_start,
            ot_applies=ot_applies
        )
        st_base += i_st
        ot_base += i_ot
        dt_base += i_dt

    # If only 1 instance scheduled but qty > 1, scale by qty (backward compat)
    if num_instances == 1 and qty > 1:
        st_base  *= qty
        ot_base  *= qty
        dt_base  *= qty
        total_day_count = round(total_day_count * qty)

    legacy_ot = 0.0  # est_ot is a manual-mode override; ignored in schedule-driven calc
    base      = st_base + ot_base + dt_base
    subtotal  = base

    if cfg and cfg.is_flat:
        # Flat fringe per person per day (e.g. Loan-Out $18/day)
        fringe_amount = total_day_count * _float(cfg.flat_amount)
    elif cfg:
        fringe_amount = subtotal * _float(cfg.rate)
    else:
        fringe_amount = 0.0

    agent_amount = subtotal * _float(line.agent_pct)
    total        = subtotal + fringe_amount + agent_amount

    return {
        "subtotal":      round(subtotal, 2),
        "st_amount":     round(st_base, 2),
        "ot_amount":     round(ot_base + legacy_ot, 2),
        "dt_amount":     round(dt_base, 2),
        "fringe_amount": round(fringe_amount, 2),
        "agent_amount":  round(agent_amount, 2),
        "total":         round(total, 2),
        "est_total":     round(total, 2),
        "day_count":     total_day_count,
    }


def calc_top_sheet(budget, lines, fringe_configs, actuals_by_code, payroll_profile=None, payroll_week_start=0):
    """
    Build Top Sheet rows grouped by COA section.

    Returns:
        rows: list of dicts per COA section
        grand_total_estimated, grand_total_actual, company_fee
        workers_comp_amount, payroll_fee_amount, gross_labor_wages
    """
    # Compute per-line totals — filter schedule days by the correct schedule_mode
    sched_mode = 'working' if budget.budget_mode in ('working', 'actual') else 'estimated'
    line_totals = {}
    for ln in lines:
        if ln.use_schedule:
            sched = [d for d in budget.schedule_days
                     if d.budget_line_id == ln.id and d.schedule_mode == sched_mode]
            result = calc_line_from_schedule(ln, sched, fringe_configs, payroll_profile, payroll_week_start)
        else:
            result = calc_line(ln, fringe_configs)
        line_totals[ln.id] = result

    # Gross labor wages = subtotals of all labor lines (base + OT, before fringe/agent)
    gross_labor_wages = sum(line_totals[ln.id]["subtotal"] for ln in lines if ln.is_labor)

    # Auto-calculated % line items
    workers_comp_pct = _float(getattr(budget, 'workers_comp_pct', 0) or 0)
    payroll_fee_pct  = _float(getattr(budget, 'payroll_fee_pct',  0) or 0)
    workers_comp_amount = round(gross_labor_wages * workers_comp_pct, 2)
    payroll_fee_amount  = round(gross_labor_wages * payroll_fee_pct,  2)

    # Group by COA section (by account_code range)
    # Build a lookup: section_start → {estimated, actual, account_name}
    section_map = {}
    for start, name in FP_COA_SECTIONS:
        section_map[start] = {"code": start, "account": name, "estimated": 0.0, "actual": 0.0}

    def section_for_code(code):
        """Return the section start for a given account code."""
        best = None
        for start, _ in FP_COA_SECTIONS:
            if code >= start:
                best = start
            else:
                break
        return best

    for ln in lines:
        sec = section_for_code(ln.account_code)
        if sec is not None and sec in section_map:
            section_map[sec]["estimated"] += line_totals[ln.id]["est_total"]

    # Inject auto-calculated amounts into their home sections
    if workers_comp_amount and 14000 in section_map:
        section_map[14000]["estimated"] += workers_comp_amount
    if payroll_fee_amount and 15000 in section_map:
        section_map[15000]["estimated"] += payroll_fee_amount

    for code, actual_sum in actuals_by_code.items():
        if code is None:
            continue
        sec = section_for_code(int(code))
        if sec is not None and sec in section_map:
            section_map[sec]["actual"] += float(actual_sum)

    # Build ordered rows, skip empty sections
    rows = []
    subtotal_est = 0.0
    subtotal_act = 0.0
    for start, _ in FP_COA_SECTIONS:
        sec = section_map[start]
        if sec["estimated"] == 0.0 and sec["actual"] == 0.0:
            continue
        variance = sec["estimated"] - sec["actual"]
        rows.append({
            "code":      sec["code"],
            "account":   sec["account"],
            "estimated": round(sec["estimated"], 2),
            "actual":    round(sec["actual"], 2),
            "variance":  round(variance, 2),
        })
        subtotal_est += sec["estimated"]
        subtotal_act += sec["actual"]

    fee_pct     = float(budget.company_fee_pct)
    dispersed   = bool(getattr(budget, 'company_fee_dispersed', False))

    if dispersed:
        # Spread the fee proportionally into each section row
        for row in rows:
            raw = row["estimated"]
            fee_amt = round(raw * fee_pct, 2)
            row["raw_estimated"] = raw
            row["fee_amount"]    = fee_amt
            row["estimated"]     = round(raw + fee_amt, 2)
        company_fee_est = round(subtotal_est * fee_pct, 2)
        grand_total_est = round(subtotal_est + company_fee_est, 2)
    else:
        for row in rows:
            row["raw_estimated"] = row["estimated"]
            row["fee_amount"]    = 0.0
        company_fee_est = round(subtotal_est * fee_pct, 2)
        grand_total_est = round(subtotal_est + company_fee_est, 2)

    grand_total_act = subtotal_act   # no fee on actuals (pass-through)
    grand_variance  = grand_total_est - grand_total_act

    return {
        "rows":              rows,
        "subtotal_estimated": round(subtotal_est, 2),
        "subtotal_actual":    round(subtotal_act, 2),
        "company_fee_pct":    fee_pct,
        "company_fee":        round(company_fee_est, 2),
        "company_fee_dispersed": dispersed,
        "grand_total_estimated": round(grand_total_est, 2),
        "grand_total_actual":    round(grand_total_act, 2),
        "grand_variance":        round(grand_variance, 2),
        "gross_labor_wages":     round(gross_labor_wages, 2),
        "workers_comp_pct":      workers_comp_pct,
        "workers_comp_amount":   workers_comp_amount,
        "payroll_fee_pct":       payroll_fee_pct,
        "payroll_fee_amount":    payroll_fee_amount,
    }


def calc_line_detail(line, schedule_days, fringe_configs, payroll_profile=None, payroll_week_start=0):
    """
    Verbose payroll breakdown for the View Calc popover.
    Returns per-week ST/OT/DT hours + costs, plus totals and the rule description.
    Read-only display data only — does not affect any stored values.
    """
    from datetime import timedelta
    from collections import defaultdict

    rate      = _float(line.rate)
    qty       = _float(line.quantity, 1.0)
    rate_type = line.rate_type or 'day_10'
    hours_pd  = RATE_TYPE_HOURS.get(rate_type)

    cfg        = fringe_configs.get(line.fringe_type) if fringe_configs else None
    ot_applies = getattr(cfg, 'ot_applies', True) if cfg is not None else True
    eff_prof   = payroll_profile if ot_applies else None
    use_hourly = bool(eff_prof and hours_pd)
    week_start = int(payroll_week_start or 0)

    def week_key(d):
        return d - timedelta(days=(d.weekday() - week_start) % 7)

    active  = [d for d in sorted(schedule_days, key=lambda x: x.date) if d.day_type != 'off']
    by_week = defaultdict(list)
    for d in active:
        by_week[week_key(d.date)].append(d)

    weeks_out = []

    if use_hourly:
        daily_st = eff_prof.daily_st_hours
        daily_dt = eff_prof.daily_dt_hours
        weekly_st = eff_prof.weekly_st_hours
        seventh   = eff_prof.seventh_day_rule
        ot_mult   = _float(eff_prof.ot_multiplier, 1.5)
        dt_mult   = _float(eff_prof.dt_multiplier, 2.0)
        hourly    = rate / hours_pd
        wk_accum  = defaultdict(float)
        wk_work   = defaultdict(int)

        for wk in sorted(by_week):
            st_h = ot_h = dt_h = 0.0
            for d in by_week[wk]:
                if d.day_type in ('work', 'travel'):
                    total_h = hours_pd + _float(getattr(d, 'est_ot_hours', None), 0.0)
                    wk_work[wk] += 1
                    is7 = (seventh == 'ot_all' and wk_work[wk] == 7)
                    if is7:
                        d_st, d_ot, d_dt = 0.0, min(total_h, 8.0), max(0.0, total_h - 8.0)
                    else:
                        d_st, d_ot, d_dt = calc_day_labor_hours(total_h, daily_st, daily_dt)
                        if weekly_st is not None:
                            rem = max(0.0, float(weekly_st) - wk_accum[wk])
                            if d_st > rem:
                                d_ot += d_st - rem
                                d_st  = rem
                            wk_accum[wk] += d_st
                else:
                    m = DAY_TYPE_MULTIPLIERS.get(d.day_type, 0.5)
                    d_st, d_ot, d_dt = hours_pd * m, 0.0, 0.0
                st_h += d_st
                ot_h += d_ot
                dt_h += d_dt

            weeks_out.append({
                'week_of':  wk.strftime('%-m/%-d/%y'),
                'days':     len(by_week[wk]),
                'st_hours': round(st_h, 2),
                'ot_hours': round(ot_h, 2),
                'dt_hours': round(dt_h, 2),
                'st_cost':  round(hourly * st_h * qty, 2),
                'ot_cost':  round(hourly * ot_mult * ot_h * qty, 2),
                'dt_cost':  round(hourly * dt_mult * dt_h * qty, 2),
            })
    else:
        for wk in sorted(by_week):
            wk_days = by_week[wk]
            cost = sum(
                rate * (DAY_TYPE_MULTIPLIERS.get(d.day_type, 0.0)
                        if d.day_type != 'custom'
                        else _float(getattr(d, 'rate_multiplier', 1.0), 1.0))
                for d in wk_days
            ) * qty
            weeks_out.append({
                'week_of':  wk.strftime('%-m/%-d/%y'),
                'days':     len(wk_days),
                'st_hours': None, 'ot_hours': None, 'dt_hours': None,
                'st_cost':  round(cost, 2), 'ot_cost': 0.0, 'dt_cost': 0.0,
            })

    # Authoritative totals from existing calc
    if schedule_days:
        totals = calc_line_from_schedule(line, schedule_days, fringe_configs,
                                         payroll_profile, payroll_week_start)
    else:
        totals = calc_line(line, fringe_configs)

    # Rule description
    if not ot_applies:
        rule = 'Exempt fringe — no OT/DT'
    elif eff_prof:
        parts = []
        if eff_prof.daily_st_hours:
            parts.append(f'OT after {float(eff_prof.daily_st_hours):.0f}h/day')
        if eff_prof.weekly_st_hours:
            parts.append(f'OT after {float(eff_prof.weekly_st_hours):.0f}h/week')
        if eff_prof.seventh_day_rule == 'ot_all':
            parts.append('7th-day OT')
        if not parts:
            parts.append('flat rate')
        rule = f'{eff_prof.name}: {", ".join(parts)}'
    else:
        rule = 'No payroll profile — flat rate'

    # Hourly rates (only meaningful when rate type has defined hours/day)
    if use_hourly and hours_pd:
        ot_mult_val = _float(eff_prof.ot_multiplier, 1.5) if eff_prof else 1.5
        dt_mult_val = _float(eff_prof.dt_multiplier, 2.0) if eff_prof else 2.0
        st_hourly = round(rate / hours_pd, 4)
        ot_hourly = round(st_hourly * ot_mult_val, 4)
        dt_hourly = round(st_hourly * dt_mult_val, 4)
    else:
        st_hourly = ot_hourly = dt_hourly = None

    return {
        'label':          line.description or line.account_name,
        'rate':           rate,
        'rate_type':      rate_type,
        'hours_per_day':  hours_pd,
        'st_hourly':      st_hourly,
        'ot_hourly':      ot_hourly,
        'dt_hourly':      dt_hourly,
        'qty':            qty,
        'fringe_type':    line.fringe_type,
        'ot_applies':     ot_applies,
        'rule':           rule,
        'total_days':     len(active),
        'total_st_hours': round(sum(w['st_hours'] or 0 for w in weeks_out), 2),
        'total_ot_hours': round(sum(w['ot_hours'] or 0 for w in weeks_out), 2),
        'total_dt_hours': round(sum(w['dt_hours'] or 0 for w in weeks_out), 2),
        'weeks':          weeks_out,
        'st_cost':        totals.get('st_amount', totals.get('subtotal', 0.0)),
        'ot_cost':        totals.get('ot_amount', 0.0),
        'dt_cost':        totals.get('dt_amount', 0.0),
        'fringe_cost':    totals['fringe_amount'],
        'agent_cost':     totals['agent_amount'],
        'total_cost':     totals['total'],
    }


def seed_fringes(db_session):
    """Seed global fringe defaults. Creates missing rows and updates ot_applies on existing rows."""
    from models import FringeConfig
    for ft, label, rate, is_flat, flat_amt, ot_applies in FP_FRINGE_DEFAULTS:
        existing = db_session.query(FringeConfig).filter_by(
            project_id=None, fringe_type=ft
        ).first()
        if not existing:
            db_session.add(FringeConfig(
                project_id=None,
                fringe_type=ft,
                label=label,
                rate=rate,
                is_flat=is_flat,
                flat_amount=flat_amt,
                ot_applies=ot_applies,
            ))
        else:
            # Always sync ot_applies in case column was just added
            existing.ot_applies = ot_applies
    db_session.commit()


def seed_payroll_profiles(db_session):
    """Seed built-in payroll profiles if not already present."""
    from models import PayrollProfile
    for (name, desc, d_st, d_dt, ot_m, dt_m, w_st, w_ot_m, seventh, wk_start, sort) in SYSTEM_PAYROLL_PROFILES:
        exists = db_session.query(PayrollProfile).filter_by(
            name=name, is_system=True).first()
        if not exists:
            db_session.add(PayrollProfile(
                name=name, description=desc, is_system=True,
                daily_st_hours=d_st, daily_dt_hours=d_dt,
                ot_multiplier=ot_m, dt_multiplier=dt_m,
                weekly_st_hours=w_st, weekly_ot_multiplier=w_ot_m,
                seventh_day_rule=seventh,
                payroll_week_start=wk_start,
                sort_order=sort,
            ))
    db_session.commit()


def seed_standard_template(db_session):
    """Seed the 'FP Standard' budget template with all COA lines at $0."""
    from models import BudgetTemplate, BudgetTemplateLine

    # Full COA from the Framework Productions chart of accounts
    COA = [
        (100,   "Pre-Production Locations",    False),
        (600,   "Above the Line",              True),
        (700,   "Talent",                      True),
        (800,   "Rehearsal",                   False),
        (900,   "Casting",                     True),
        (1000,  "Production Staff",            True),
        (1200,  "Post-Production Staff",       True),
        (2000,  "Camera Equipment",            False),
        (3000,  "Grip & Electric",             True),
        (3100,  "Processing",                  False),
        (3200,  "Control Room",                False),
        (3300,  "Sound",                       True),
        (4000,  "Art",                         False),
        (4500,  "Hair & Makeup",               True),
        (5000,  "Wardrobe",                    False),
        (6000,  "Transportation",              False),
        (7000,  "Travel",                      False),
        (7500,  "Shipping",                    False),
        (8000,  "Production Meals / Craft Services", False),
        (8500,  "Sanitation",                  False),
        (9000,  "Location",                    False),
        (11000, "Post-Production Equipment",   False),
        (11500, "Post-Production Locations",   False),
        (11600, "Post-Production Services",    False),
        (12000, "Licensing",                   False),
        (12500, "Composition & Mastering",     False),
        (13000, "Distribution",                False),
        (13200, "Software & Office Supplies",  False),
        (14000, "Insurance",                   False),
        (15000, "Administrative",              False),
        (16000, "Marketing",                   False),
        (17000, "EPK / Behind the Scenes",     False),
        (18000, "Title Sequence",              False),
        (19000, "Residuals",                   False),
        (20000, "Misc",                        False),
        (20500, "Production Company Fee",      False),
    ]

    existing = db_session.query(BudgetTemplate).filter_by(name="FP Standard").first()
    if existing:
        return

    tmpl = BudgetTemplate(
        name="FP Standard",
        description="Framework Productions standard budget template — all COA sections at $0"
    )
    db_session.add(tmpl)
    db_session.flush()

    for i, (code, name, is_labor) in enumerate(COA):
        db_session.add(BudgetTemplateLine(
            template_id=tmpl.id,
            account_code=code,
            account_name=name,
            is_labor=is_labor,
            sort_order=i,
        ))
    db_session.commit()


# ── Catalog seed ──────────────────────────────────────────────────────────────
# (code, name, label, group, is_labor, rate, qty, days, kit, fringe, union_fringe,
#  agent_pct, comp, unit)
FP_CATALOG_SEED = [
    # Above the Line (code 600)
    (600,  "Above the Line",    "Director",                    None,            True,  1500, 1, 1, 0, "N", None, 0.00, "labor", "day"),
    (600,  "Above the Line",    "Executive Producer",          None,            True,  1200, 1, 1, 0, "N", None, 0.00, "labor", "day"),
    (600,  "Above the Line",    "Producer",                    None,            True,  1000, 1, 1, 0, "N", None, 0.00, "labor", "day"),
    (600,  "Above the Line",    "Creative Director",           None,            True,  1200, 1, 1, 0, "N", None, 0.00, "labor", "day"),

    # Talent (code 700)
    (700,  "Talent",            "Principal Talent",            None,            True,  825,  1, 1, 0, "N", "S",  0.10, "labor", "day"),
    (700,  "Talent",            "Host",                        None,            True,  1000, 1, 1, 0, "N", "S",  0.10, "labor", "day"),
    (700,  "Talent",            "Extra / Background",          None,            True,  200,  1, 1, 0, "N", "S",  0.00, "labor", "day"),

    # Production Staff (code 1000) — user's most recent edits
    (1000, "Production Staff",  "Line Producer",               "Production",     True,  1200, 1, 1, 0, "N", "D",  0.00, "labor", "day"),
    (1000, "Production Staff",  "UPM",                         "Production",     True,  1000, 1, 1, 0, "N", "D",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Supervising Producer",        "Production",     True,  800,  1, 1, 0, "N", "N",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Production Supervisor",       "Production",     True,  900,  1, 1, 0, "N", "N",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Production Coordinator",      "Production",     True,  750,  1, 1, 0, "N", "N",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Production Assistant",        "Production",     True,  350,  1, 1, 0, "N", "N",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Live Director",               "Direction / AD", True,  700,  1, 1, 0, "N", "D",  0.00, "labor", "day"),
    (1000, "Production Staff",  "1st AD",                      "Direction / AD", True,  900,  1, 1, 0, "N", "D",  0.00, "labor", "day"),
    (1000, "Production Staff",  "2nd AD",                      "Direction / AD", True,  750,  1, 1, 0, "N", "D",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Key PA",                      "Direction / AD", True,  350,  1, 1, 0, "N", "N",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Director of Photography",     "Camera",         True,  1200, 1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Camera Operator",             "Camera",         True,  900,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Robotic Camera Operator",     "Camera",         True,  500,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "1st AC",                      "Camera",         True,  800,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "2nd AC",                      "Camera",         True,  650,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "DIT",                         "Camera",         True,  850,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Video Engineer",              "Camera",         True,  750,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Lighting Designer",           "Grip & Electric",True,  1000, 1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Gaffer",                      "Grip & Electric",True,  825,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Key Grip",                    "Grip & Electric",True,  825,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Sound Mixer",                 "Sound",          True,  900,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Boom Operator",               "Sound",          True,  650,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Technical Producer",          "Control Room",   True,  1000, 1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Technical Director",          "Control Room",   True,  900,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Graphics and Playback",       "Control Room",   True,  500,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1000, "Production Staff",  "Switcher Operator",           "Control Room",   True,  750,  1, 1, 0, "N", "I",  0.00, "labor", "day"),

    # Post-Production Staff (code 1200)
    (1200, "Post-Production Staff", "Editor",                  None,            True,  900,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1200, "Post-Production Staff", "Assistant Editor",        None,            True,  650,  1, 1, 0, "N", "I",  0.00, "labor", "day"),
    (1200, "Post-Production Staff", "Colorist",                None,            True,  900,  1, 1, 0, "N", "I",  0.00, "labor", "day"),

    # Camera Equipment (code 2000, non-labor rentals)
    (2000, "Camera Equipment",  "Camera Package Rental",       None,            False, 1500, 1, 1, 0, None, None, 0.00, "rental",  "day"),
    (2000, "Camera Equipment",  "Lens Kit Rental",             None,            False, 500,  1, 1, 0, None, None, 0.00, "rental",  "day"),
    (2000, "Camera Equipment",  "Media / Hard Drives",         None,            False, 300,  1, 1, 0, None, None, 0.00, "purchase","flat"),

    # Grip & Electric (code 3000)
    (3000, "Grip & Electric",   "Lighting Package",            None,            False, 1500, 1, 1, 0, None, None, 0.00, "rental", "day"),
    (3000, "Grip & Electric",   "Grip Package",                None,            False, 800,  1, 1, 0, None, None, 0.00, "rental", "day"),

    # Sound (code 3300)
    (3300, "Sound",             "Sound Package Rental",        None,            False, 500,  1, 1, 0, None, None, 0.00, "rental", "day"),

    # Travel (code 7000)
    (7000, "Travel",            "Flight",                      None,            False, 500,  1, 1, 0, None, None, 0.00, "expense", "flat"),
    (7000, "Travel",            "Hotel Night",                 None,            False, 200,  1, 1, 0, None, None, 0.00, "expense", "day"),
]


def seed_catalog(db_session):
    """Seed the global Quick Entry catalog. Idempotent: only adds missing items."""
    from models import CatalogItem
    existing_keys = {(c.category_code, c.label) for c in CatalogItem.query.all()}
    added = 0
    for i, row in enumerate(FP_CATALOG_SEED):
        (code, cname, label, group, is_labor, rate, qty, days, kit,
         fringe, union_fringe, agent_pct, comp, unit) = row
        if (code, label) in existing_keys:
            continue
        db_session.add(CatalogItem(
            category_code=code,
            category_name=cname,
            label=label,
            group_name=group,
            is_labor=is_labor,
            rate=rate,
            qty=qty,
            days=days,
            kit_fee=kit,
            fringe=fringe,
            union_fringe=union_fringe,
            agent_pct=agent_pct,
            comp=comp,
            unit=unit,
            sort_order=i * 10,
            is_active=True,
        ))
        added += 1
    if added:
        db_session.commit()
    return added
