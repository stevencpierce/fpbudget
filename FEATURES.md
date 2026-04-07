# FPBudget — Feature Registry

**App:** FPBudget (Framework Productions Budget Planning)
**Live URL:** https://fp-budget.onrender.com
**Repo:** https://github.com/stevencpierce/fpbudget
**Stack:** Flask · SQLAlchemy · SQLite/Postgres · WeasyPrint · Jinja2

> Update this file whenever a feature is added, changed, or removed.
> Status: `✅ Live` · `🔧 In Progress` · `📋 Planned` · `⚠️ Known Issue` · `🗑️ Removed`

---

## Projects & Budgets

| Status | Feature | Plain English Description |
|--------|---------|--------------------------|
| ✅ Live | Project Dashboard | Landing page showing all projects with quick links to each budget |
| ✅ Live | Create Project | Start a new production project with a name and optional description |
| ✅ Live | Multiple Budgets per Project | Each project can have several budget versions (Estimated, Working, Actual) |
| ✅ Live | Budget Modes | Three modes: Estimated (baseline), Working (live forecast), Actual (real spend) |
| ✅ Live | Create Working Budget from Estimated | Locks the estimated as a frozen snapshot, opens a working copy for live tracking |
| ✅ Live | Budget Versioning | Each budget has a version name (e.g. "v1 Client Approved") |

---

## Budget Entry

| Status | Feature | Plain English Description |
|--------|---------|--------------------------|
| ✅ Live | ⚡ Quick Entry Panel | Slide-over panel with every COA department; pre-filled suggested rates for all labor and expense items; check multiple items across departments and add them all at once |
| ✅ Live | + Single Line (modal) | Add one line at a time — pick department, description, labor vs. flat, qty/days/rate |
| ✅ Live | + Line per Section | Each department section has its own quick-add button that pre-selects that department |
| ✅ Live | Inline Editing | Click any cell in a budget line to edit it in place — saves automatically |
| ✅ Live | Labor Lines (qty × days × rate) | Full union/non-union labor math: ST/OT/DT, fringe, agent % |
| ✅ Live | Non-Labor / Flat Lines | Expense lines: qty × days × unit rate, or a single flat dollar amount |
| ✅ Live | Delete Line | Remove any line with a trash icon; prompts confirmation |
| ✅ Live | Reorder Lines (drag) | Drag lines up/down within a section to reorder them |
| ✅ Live | Search / Filter Lines | Search box filters all lines across all departments in real time |

---

## Rates & Math

| Status | Feature | Plain English Description |
|--------|---------|--------------------------|
| ✅ Live | Rate Types | 10hr Day (default), 8hr Day, 12hr Day, Flat Day, Flat Project, Hourly, Custom |
| ✅ Live | OT / DT Calculation | Auto-calculates overtime and double time based on rate type and hours |
| ✅ Live | Fringe / Benefits | Assign a fringe bucket (None, Union, Employer, State, Local, Payroll) to each labor line |
| ✅ Live | Agent % | Optional agent commission percentage per labor line |
| ✅ Live | Workers' Comp | Auto-calculated as a % of total labor; set in budget settings |
| ✅ Live | Payroll Service Fee | Auto-calculated as a % of total labor; set in budget settings |
| ✅ Live | Production Company Fee | Flat % added on top of all costs; shown separately or dispersed into line rates |
| ✅ Live | Dispersed Fee Mode | When enabled, the production fee is spread invisibly into every line rate — no fee line shown on any export |

---

## Top Sheet (Budget Summary)

| Status | Feature | Plain English Description |
|--------|---------|--------------------------|
| ✅ Live | Top Sheet Tab | One-page summary showing every COA department with its total, subtotal, fee, and grand total |
| ✅ Live | Estimated vs. Working Variance | In working mode, shows frozen estimate, current working total, and dollar variance per department |
| ✅ Live | Department Drill-Down | Double-click any department row to jump to those detail lines |
| ✅ Live | Tax Credit / Incentive Lines | Separate section below the grand total for tax incentive tracking |
| ✅ Live | Collapsible Department Rows | Toggle detail lines for any department directly on the top sheet |

---

## Working Budget Tracking

| Status | Feature | Plain English Description |
|--------|---------|--------------------------|
| ✅ Live | Frozen Estimated Snapshot | When you create a Working budget, the Estimated column locks to that moment's values and never changes |
| ✅ Live | Working Total Column | Shows live recalculated totals as you make changes |
| ✅ Live | Variance Column | Color-coded over/under variance between Estimated and Working per line and per section |
| ✅ Live | Cross-Budget Reference | View the Working budget's totals as a column while viewing the Estimated budget |
| ✅ Live | Float Bar (bottom) | Sticky bar always showing Subtotal · Fee · Grand Total — updates live as you type |

---

## Schedule

| Status | Feature | Plain English Description |
|--------|---------|--------------------------|
| ✅ Live | Schedule Tab | Day-by-day calendar showing shoot days for the project |
| ✅ Live | Day Types | Assign each day as Shoot, Travel, Prep, Strike, Hold, Off, etc. |
| ✅ Live | Schedule-Driven Labor | Labor lines can be linked to the schedule — their days auto-populate from shoot days |
| ✅ Live | OT from Schedule | When a day's hours exceed the rate-type threshold, OT is auto-calculated |
| ✅ Live | Estimated vs. Working Schedule | Separate schedule grids for Estimated and Working modes |

---

## Crew / Contact Sheet

| Status | Feature | Plain English Description |
|--------|---------|--------------------------|
| ✅ Live | Crew Tab | Auto-generated contact sheet from all labor lines in the budget |
| ✅ Live | Contact Fields | Name, phone, email, role, department per crew member |
| ✅ Live | Department Grouping | Crew grouped by COA department in alphabetical department order |
| ✅ Live | Multi-select for Export | Ctrl/Cmd-click to select multiple crew; right-click to omit from printed sheet |
| ✅ Live | Kit Fees on Crew | Kit fee amount tracked per crew assignment |

---

## Exports

| Status | Feature | Plain English Description |
|--------|---------|--------------------------|
| ✅ Live | Top Sheet PDF | One-page landscape PDF in Movie Magic / Showbiz industry format |
| ✅ Live | Full Detail PDF | Multi-page PDF with every line item, section totals, and variance columns |
| ✅ Live | Top Sheet CSV | Comma-separated summary export for Excel |
| ✅ Live | Line Detail CSV | Full line-by-line CSV export |
| ✅ Live | Company Header on PDF | PDF includes company name, address, phone, email from global Settings |
| ✅ Live | Production Details on PDF | PDF includes client name, version, prepared-by info from budget Settings |
| ✅ Live | Dispersed Fee on PDF | When dispersed mode is on, no fee line appears anywhere on PDF exports |
| 📋 Planned | Gmail Draft Integration | Send budget PDF as a Gmail draft attachment for approval workflows |

---

## Settings

| Status | Feature | Plain English Description |
|--------|---------|--------------------------|
| ✅ Live | Budget Settings | Name, start/end date, target budget, company fee %, dispersed toggle, payroll settings |
| ✅ Live | Production Details (per budget) | Client name, prepared-by name/title/email/phone stored on each budget |
| ✅ Live | Company Profile (global) | Company name, address, phone, email, website — used on all PDF exports |
| ✅ Live | Fringe Config | Set the % rate for each fringe bucket (Union, Employer, State, Local, Payroll) |
| ✅ Live | Payroll Profiles | Named payroll setups (e.g. "ADP Weekly") with week-start day and payroll fee % |
| ✅ Live | Templates | Save any budget as a reusable template; apply a template to a new budget |
| ✅ Live | FP Standard Template | Built-in template with all 29 COA sections pre-loaded at $0 |

---

## Admin / System

| Status | Feature | Plain English Description |
|--------|---------|--------------------------|
| ✅ Live | Login / Auth | Email + password login; session-based auth via Flask-Login |
| ✅ Live | Health Check | `/health` endpoint for Render uptime monitoring |
| ✅ Live | Auto DB Migrations | New database columns added automatically on startup — no manual migration needed |
| ✅ Live | Postgres + SQLite | Uses Postgres on Render (production), SQLite locally (development) |
| ✅ Live | CSV Import | Upload a CSV to bulk-import budget lines into any section |

---

## Known Issues / In Progress

| Status | Feature | Notes |
|--------|---------|-------|
| ⚠️ Known Issue | Template Apply UI | "Apply Template" dropdown in Settings tab has empty options — route exists but template picker not wired to live data |

---

## Workflow: Offline → Live

```
1. Work locally:   python app.py   →   http://localhost:5000
2. Test your changes until the feature works correctly
3. Commit:         git add -A && git commit -m "describe what you built"
4. Push live:      git push origin main
5. Render auto-deploys in ~2 minutes
```

No manual deploy steps needed after initial setup.
