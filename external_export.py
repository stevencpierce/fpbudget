"""MMB / ShowBiz exporters and preview builders.

Task 3 of the COA project. Reads BudgetLine rows for a given budget, looks
up each line's target external account via RoleTagMapping (keyed on
catalog_item.role_tag), and emits a tab-delimited file that the external
budgeting software can import.

Preview builder returns the same data structured as a list of sections
with lines and subtotals, suitable for rendering in the right-drawer
preview UI.

This module is intentionally self-contained: no Flask imports, no request
context. Callers pass in a Budget object (or its id) and get bytes /
dicts back.

Wrapbook CSV export (from prior session backlog) will slot in here with
the same shape once this is shaken out.
"""

from __future__ import annotations

import csv
import io
from collections import OrderedDict


# ── Column schemas ────────────────────────────────────────────────────────────

MMB_COLUMNS = [
    "Account#", "Account Description", "Fringe", "Qty", "Unit", "X",
    "Rate", "Sub-Total", "Total",
]

SHOWBIZ_COLUMNS = [
    "Acct#", "Description", "Amount", "Fringe", "Units", "X", "Rate", "Total",
]


# ── Shared row-building logic ─────────────────────────────────────────────────

def _line_target(line, mapping_by_tag, mapping_by_fallback):
    """Resolve (target_code:str, target_name:str) for a BudgetLine.

    Priority:
      1. If line.catalog_item_id → CatalogItem.role_tag → RoleTagMapping.
      2. Fuzzy (account_code, description) against mapping_by_fallback.
      3. Fall through to FP internal (account_code, account_name).
    """
    # 1) FK-based lookup
    ci = getattr(line, 'catalog_item', None)
    if ci is None and getattr(line, 'catalog_item_id', None):
        # Avoid forcing an eager load if caller hasn't joined; best-effort
        try:
            from models import CatalogItem
            ci = CatalogItem.query.get(line.catalog_item_id)
        except Exception:
            ci = None
    if ci is not None and getattr(ci, 'role_tag', None):
        m = mapping_by_tag.get(ci.role_tag)
        if m:
            return m

    # 2) Fallback via (account_code, lowered description)
    key = (int(line.account_code or 0), (line.description or "").lower().strip())
    if key in mapping_by_fallback:
        return mapping_by_fallback[key]
    # 2b) fallback by account_code alone
    for (code, _desc), val in mapping_by_fallback.items():
        if code == key[0]:
            return val

    # 3) Pass-through
    return (str(line.account_code or ""), line.account_name or "")


def _line_amount(line):
    """Returns the estimated_total-equivalent for a BudgetLine. For non-labor
    lines this is the stored estimated_total; for labor lines it's the gross
    (qty × days × rate) before fringes/OT — matches MMB's expectation that
    Fringe is a separate column."""
    if getattr(line, 'is_labor', False):
        try:
            q = float(line.quantity or 1)
            d = float(line.days or 1)
            r = float(line.rate or 0)
            return round(q * d * r, 2)
        except Exception:
            return float(line.estimated_total or 0)
    return float(getattr(line, 'estimated_total', 0) or 0)


def _fringe_label(line):
    """MMB/ShowBiz expect a short fringe label (e.g. 'N', 'S', 'I') — same
    values we already store on BudgetLine.fringe_type. Returns '' for
    non-labor rows."""
    if not getattr(line, 'is_labor', False):
        return ""
    return (getattr(line, 'fringe_type', None) or "").upper()


def _rate_type_unit_x(line):
    """Translate FP rate_type → (Unit, X) columns for MMB/ShowBiz.

    MMB's X column is the multiplier between Unit and Rate (e.g. 'Day' × 10
    for a day_10 rate). We collapse our internal rate_type:
      day_8    → ('Day', 1),
      day_10   → ('Day', 1),
      day_12   → ('Day', 1),
      week     → ('Week', 1),
      flat_day → ('Day', 1),
      flat_project → ('Flat', 1),
      hourly   → ('Hour', 1),
      custom   → ('Ea', 1),
    Non-labor rows use the stored `.days_unit` column if set, else 'Ea'.
    """
    rt = getattr(line, 'rate_type', None) or 'day_10'
    if rt.startswith('day_'):
        return ('Day', 1)
    if rt == 'week':
        return ('Week', 1)
    if rt == 'flat_project':
        return ('Flat', 1)
    if rt == 'hourly':
        return ('Hour', 1)
    if not getattr(line, 'is_labor', False):
        unit = (getattr(line, 'days_unit', None) or 'ea').lower()
        return (unit.capitalize() or 'Ea', 1)
    return ('Day', 1)


# ── Internal: load mappings once per export ──────────────────────────────────

def _load_mappings(target):
    """target ∈ {'mmb', 'showbiz'}. Returns (by_tag, by_fallback).

    by_tag:      role_tag → (code:str, name:str)
    by_fallback: (internal_code:int, lowered_description:str) → (code, name)
                 Also keyed on (internal_code, '') for code-only fallback.
    """
    from models import RoleTagMapping, CatalogItem
    code_col = 'mmb_account_code' if target == 'mmb' else 'showbiz_account_code'
    name_col = 'mmb_account_name' if target == 'mmb' else 'showbiz_account_name'

    by_tag = {}
    by_fallback = {}

    mappings = RoleTagMapping.query.all()
    # Index by role_tag
    for m in mappings:
        code = getattr(m, code_col) or ""
        name = getattr(m, name_col) or ""
        if code or name:
            by_tag[m.role_tag] = (code, name)

    # Build fallback index using CatalogItem labels for (internal_code, desc)
    ci_by_tag = {ci.role_tag: ci for ci in CatalogItem.query.all() if ci.role_tag}
    for m in mappings:
        ci = ci_by_tag.get(m.role_tag)
        if not ci:
            continue
        code = getattr(m, code_col) or ""
        name = getattr(m, name_col) or ""
        if code or name:
            by_fallback[(int(ci.category_code or 0),
                         (ci.label or '').lower())] = (code, name)
            # Also a code-only fallback (last one wins; arbitrary)
            by_fallback[(int(ci.category_code or 0), "")] = (code, name)
    return by_tag, by_fallback


# ── Public API ────────────────────────────────────────────────────────────────

def _build_rows(budget, target):
    """Returns list of dicts: [{ 'target_code', 'target_name', 'amount',
    'fringe', 'qty', 'unit', 'x', 'rate', 'sub_total', 'line': BudgetLine }]
    """
    from models import BudgetLine, CatalogItem

    lines = (BudgetLine.query.filter_by(budget_id=budget.id)
             .order_by(BudgetLine.account_code, BudgetLine.sort_order, BudgetLine.id)
             .all())

    # Eager-load CatalogItem relations for speed
    ci_map = {ci.id: ci for ci in CatalogItem.query.all()}

    by_tag, by_fallback = _load_mappings(target)

    rows = []
    for ln in lines:
        # Attach catalog_item for _line_target() to use without extra queries
        if getattr(ln, 'catalog_item_id', None):
            setattr(ln, 'catalog_item', ci_map.get(ln.catalog_item_id))
        tc, tn = _line_target(ln, by_tag, by_fallback)
        unit, x = _rate_type_unit_x(ln)
        qty = float(ln.quantity or 1)
        days = float(ln.days or 1)
        rate = float(ln.rate or 0)
        sub_total = float(ln.estimated_total or 0) if not getattr(ln, 'is_labor', False) else round(qty * days * rate, 2)
        rows.append({
            "target_code": tc,
            "target_name": tn,
            "internal_code": ln.account_code,
            "internal_name": ln.account_name,
            "description":  ln.description or "",
            "fringe":       _fringe_label(ln),
            "qty":          qty,
            "days":         days,
            "unit":         unit,
            "x":            x,
            "rate":         rate,
            "sub_total":    sub_total,
            "amount":       _line_amount(ln),
            "is_labor":     getattr(ln, 'is_labor', False),
            "line_id":      ln.id,
        })
    return rows


def _group_rows(rows):
    """Group rows by target_code (fall back to internal_code when empty).
    Returns OrderedDict[group_key] → {'code', 'name', 'rows': [...], 'subtotal'}"""
    groups = OrderedDict()
    for r in rows:
        key = r["target_code"] or f"__internal_{r['internal_code']}"
        if key not in groups:
            groups[key] = {
                "code": r["target_code"] or str(r["internal_code"] or ""),
                "name": r["target_name"] or r["internal_name"] or "",
                "rows": [],
                "subtotal": 0.0,
            }
        groups[key]["rows"].append(r)
        groups[key]["subtotal"] += float(r["amount"] or 0)
    # Sort by code (ascending) — empty codes go last
    def _sort_key(item):
        k, _ = item
        if k.startswith("__internal_"):
            return ("ZZZ", k)
        return ("A", k)
    return OrderedDict(sorted(groups.items(), key=_sort_key))


# ── Tab-delimited MMB export ──────────────────────────────────────────────────

def export_mmb_tab(budget) -> bytes:
    rows = _build_rows(budget, target='mmb')
    groups = _group_rows(rows)
    buf = io.StringIO()
    w = csv.writer(buf, delimiter='\t', lineterminator='\n')
    # Header row
    w.writerow(MMB_COLUMNS)
    grand = 0.0
    for _k, g in groups.items():
        # Section header row
        w.writerow([g["code"], g["name"], "", "", "", "", "", "", ""])
        for r in g["rows"]:
            w.writerow([
                g["code"],
                r["description"] or g["name"],
                r["fringe"],
                r["qty"],
                r["unit"],
                r["x"],
                r["rate"],
                r["sub_total"],
                r["amount"],
            ])
        w.writerow(["", f"  {g['name']} Subtotal", "", "", "", "", "", "", round(g["subtotal"], 2)])
        grand += g["subtotal"]
    w.writerow([])
    w.writerow(["", "GRAND TOTAL", "", "", "", "", "", "", round(grand, 2)])
    return buf.getvalue().encode('utf-8')


# ── Tab-delimited ShowBiz export ──────────────────────────────────────────────

def export_showbiz_tab(budget) -> bytes:
    rows = _build_rows(budget, target='showbiz')
    groups = _group_rows(rows)
    buf = io.StringIO()
    w = csv.writer(buf, delimiter='\t', lineterminator='\n')
    w.writerow(SHOWBIZ_COLUMNS)
    grand = 0.0
    for _k, g in groups.items():
        w.writerow([g["code"], g["name"], "", "", "", "", "", ""])
        for r in g["rows"]:
            w.writerow([
                g["code"],
                r["description"] or g["name"],
                r["amount"],
                r["fringe"],
                r["qty"],
                r["x"],
                r["rate"],
                r["sub_total"],
            ])
        w.writerow(["", f"  {g['name']} Subtotal", round(g["subtotal"], 2), "", "", "", "", ""])
        grand += g["subtotal"]
    w.writerow([])
    w.writerow(["", "GRAND TOTAL", round(grand, 2), "", "", "", "", ""])
    return buf.getvalue().encode('utf-8')


# ── Preview builders (JSON-serializable dicts for the UI drawer) ──────────────

def preview_mmb(budget):
    rows = _build_rows(budget, target='mmb')
    groups = _group_rows(rows)
    return _preview_format(groups)


def preview_showbiz(budget):
    rows = _build_rows(budget, target='showbiz')
    groups = _group_rows(rows)
    return _preview_format(groups)


def _preview_format(groups):
    sections = []
    grand = 0.0
    for _k, g in groups.items():
        sections.append({
            "code":      g["code"],
            "name":      g["name"],
            "subtotal":  round(g["subtotal"], 2),
            "lines":     [{
                "description": r["description"],
                "qty":         r["qty"],
                "rate":        r["rate"],
                "unit":        r["unit"],
                "fringe":      r["fringe"],
                "amount":      r["amount"],
                "internal_code": r["internal_code"],
            } for r in g["rows"]],
        })
        grand += g["subtotal"]
    return {"sections": sections, "grand_total": round(grand, 2)}
