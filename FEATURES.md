# FPBudget — Feature Registry

**App:** FPBudget (Framework Productions Budget Planning)
**Live URL:** https://fp-budget.onrender.com
**Repo:** https://github.com/stevencpierce/fpbudget
**Stack:** Flask · SQLAlchemy · Postgres · WeasyPrint · Jinja2 · Flask-SocketIO (gthread)
**Last Updated:** 2026-04-28 (commit `a3fefae`)

> Status icons: ✅ Live · 🔧 In Progress · 📋 Planned · ⚠️ Known Issue · 🗑️ Removed

---

## Projects & Budgets

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Project dashboard | Landing page lists every project with quick links to each budget |
| ✅ | Create project | New project: name, client, optional template; auto-creates first budget |
| ✅ | Template at creation | Pick a pre-built template — budget lines auto-populate |
| ✅ | Multiple budgets per project | Each project can have several budget versions in parallel |
| ✅ | Budget modes | Three modes: **Estimated** (baseline), **Working** (live), **Actual** (real spend) |
| ✅ | Version management | Working budget can be archived & cloned; estimated stays read-only when a working sibling exists |
| ✅ | Project archive / reactivate | Archived projects move to `_ARCHIVED/` in Dropbox; reactivate restores them |
| ✅ | Project access control | Per-project member roles (`viewer`, `editor`, `dept_head`, `admin`, `super_admin`) |

---

## Budget Line Editing

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Inline editing | Click any cell on the line table to edit description, qty, days, rate, fringe, agent% |
| ✅ | + Single Line | Add one line to a section via modal |
| ✅ | + Quick Entry | Add many lines at once from a curated catalog of common production roles + items |
| ✅ | Duplicate row | Copies an existing line; prompts whether to copy schedule cells too |
| ✅ | Drag-reorder | Drag rows up/down; sort_order persists |
| ✅ | Bulk delete | Multi-select rows then delete |
| ✅ | Section auto-totals | Each section header shows running total; recalculates on every edit |
| ✅ | Auto-line: Workers' Comp | % of gross labor wages, default 3%, lands in section 6000 (Insurance) |
| ✅ | Auto-line: Production Liability Insurance | Mode picker (off / % of labor / flat $); default ON at 1.5% on new budgets |
| ✅ | Auto-line: Payroll Service Fee | % of gross labor wages, default 1.75%, lands in section 6500 (Administrative) |
| ✅ | Production Company Fee — flat or dispersed | Toggle in Settings; flat shows a separate fee line, dispersed spreads into section totals |
| ✅ | Per-section fee exemption | Per-budget checklist of COA sections that pass through without the fee markup |
| ✅ | Fringes excluded from fee base | Default ON: P&W / P/H/W fringes don't compound the Prod Co Fee |
| ✅ | Replace Qty with Payroll Co. on labor | Labor rows show payroll-company text input (placeholder for future linked-payroll feature); non-labor rows keep Qty |
| ✅ | Conditional column rendering | Payroll / OT / Fringe / Agent% / Disc% columns hide when no row in the section has data |
| ✅ | OT Detail panel | Right-click any labor line → ST/OT/DT breakdown by week per the payroll profile |
| ✅ | Schedule Detail panel | Click any auto-calc line (per diem, hotel, flight, etc.) → per-day, per-person breakdown with drift indicator |

---

## Schedule (Gantt)

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Calendar grid | Each labor line → row; each date → column; click cells to set day type |
| ✅ | Day types | Work / Travel / Hold / Half / Kill Fee / Custom / Off — color-coded |
| ✅ | Multi-instance crew | Lines with qty>1 expand to one row per crew member |
| ✅ | Crew assignment | Click "+ Assign" → pick from crew DB or add new person inline |
| ✅ | Manual OT hours | Right-click cell → "OT Hours" → enters manually; flows into payroll calc on hourly rate types |
| ✅ | Per-cell flags | Right-click cell → toggle flight / hotel / car rental / mileage / per diem / working meal |
| ✅ | Per-Diem variants | 4 separate flags: Full / Breakfast / Lunch / Dinner — each has its own auto-line + rate |
| ✅ | Production Day toggle | Top-row marker for "real on-set days vs prep / remote / travel" — feeds downstream calcs |
| ✅ | Meal flags (per-date) | Top-row toggles: Courtesy Breakfast / 1st Meal / 2nd Meal — applied to all working crew that date |
| ✅ | Working Meal flag (per-cell) | Per-person opt-in to a working meal on a specific day |
| ✅ | Multi-select + Delete | Drag-select cells then Delete/Backspace clears day type + all flags + OT in one action |
| ✅ | Copy/paste | Cmd+C / Cmd+V across selected cells |
| ✅ | Schedule sync to budget | Every cell change re-runs sync; auto-lines (per diem, hotel, flight, mileage, meals) update in lockstep |
| ✅ | Notes | Right-click → add free-text note per cell |

---

## Travel Tab

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Day-card layout | Each scheduled date is its own collapsible card; header shows pretty date + crew count |
| ✅ | Editable day-type | Pill-style dropdown per row to change Work → Travel → Hold etc. without leaving the tab |
| ✅ | Flag toggles per row | Pill buttons for Flight / Hotel / Car Rental / Mileage |
| ✅ | Per-Diem dropdown | Select No PD / Full / Breakfast / Lunch / Dinner per row |
| ✅ | "Add details" button | Click to open centered modal with reservation fields (flight #, airline, depart/arrive, hotel name, check-in/out, room type, rental co, pickup/return, miles, route, confirmation #, notes) |
| ✅ | Detail persistence | TravelDetail rows live alongside ScheduleDay; survive flag-toggle cycles |
| ✅ | "+ Add to this day" | Per-card button to add another crew member to an existing date with one click |
| ✅ | "Only flagged" filter | Hide unflagged crew once the list grows |
| ✅ | Expand all / Collapse all | Bulk controls above the day list |
| 🔧 | Call sheet email integration | Travel details to flow into call sheet emails per-crew per-date |

---

## Catering Tab

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Day-card layout | Mirrors Travel tab — every scheduled day is its own collapsible card |
| ✅ | Day-of-week + Production-Week label | Each card shows weekday + "Wk N" matching the payroll cycle |
| ✅ | Production Day badge | Cards for production days get a green badge |
| ✅ | Per-day meal toggles | Courtesy Breakfast / 1st Meal / 2nd Meal in each card body |
| ✅ | Per-card people lists | Working crew, working-meal opt-ins, per-diem people (with mode) listed in the body |
| ✅ | Expected $ per day | Computed live from headcount × rate per active meal type |
| ✅ | Per-person Per Diem rollup | Below day cards: weekly $ matrix per person + project total + day count |
| ✅ | Per-person Working Meal rollup | Same shape, only renders if any line has working-meal opt-ins |
| ✅ | Caterer Bill entry | Add daily/weekly billed amounts with vendor + period + note |
| ✅ | Drift indicator | Top summary shows expected vs caterer-billed with delta color-coded |

---

## Documents (Receipt OCR + Filing)

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Drag-drop or browse upload | Drop receipts/invoices/PDFs/HEIC into the docs tab |
| ✅ | Camera capture | Mobile-friendly "Take Photo" button |
| ✅ | Veryfi OCR | Each upload runs through Veryfi → vendor / amount / date / category extracted |
| ✅ | Auto-rename | High-confidence docs get filed with `YYYY-MM-DD_RECEIPT_Category_Vendor_Amount.pdf` naming |
| ✅ | Auto-file to Dropbox | Receipts → `01_ADMIN/PROCESSED DOCUMENTS/`, invoices → `01_ADMIN/CONTRACTS & INVOICES/`, etc. by detected type |
| ✅ | Low-confidence review queue | Files that don't pass auto-file confidence threshold flagged for manual review |
| ✅ | Sort + filter list | Previously-uploaded list sortable by upload date / filename / type / vendor / amount / size |
| ✅ | Inline rename | Click any filed filename → edit → Dropbox file is renamed in lockstep |
| ✅ | Click-to-open detail modal | Click a row → preview pane (image or PDF) + editable vendor / amount / doc date / category / note |
| ✅ | Verify against Dropbox | Background check on every "Filed" row; missing files → red banner |
| ✅ | Scan Dropbox (reconcile) | Super-admin tool: walks the project's doc folders and creates DocUpload rows for any orphan files |
| ✅ | Duplicate routing | Same content uploaded twice → second copy routes to `/_DUPLICATES/` subfolder, original stays clean |
| ✅ | Wipe all (testing) | Super-admin tool: clears every DocUpload + moves filed files to `/_TRASH/{date}/` |

---

## Top Sheet

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Section-grouped totals | All COA sections rolled up: Estimated / Working / Actual / Variance |
| ✅ | Auto-calc inline rows | Workers' Comp / Production Liability / Payroll Service Fee shown as muted sub-lines under their home sections |
| ✅ | Fee-exempt annotation | Sections marked exempt show "(Prod Co Fee exempt)" inline in italic amber |
| ✅ | Tax credits row | Subtracted from grand total when applicable |
| ✅ | Float bar | Sticky bottom bar shows Subtotal · Prod Fee · Grand Total — recalculates live |
| ✅ | Variance highlighting | Over/under styled green / red |
| ✅ | Click section → jump to budget | Top sheet rows navigate into the section detail |

---

## Exports

| Status | Feature | Plain English |
|---|---|---|
| ✅ | PDF — Top Sheet | Section-grouped one-pager |
| ✅ | PDF — Full Detail | Every line, organized by section |
| ✅ | CSV — Top Sheet | Section totals |
| ✅ | CSV — Line Detail | Every line with its math |
| ✅ | MMB tab-delimited | Movie Magic Budgeting import format |
| ✅ | ShowBiz tab-delimited | ShowBiz Budgeting import format |
| ✅ | Export Options dialog | Pre-export prompt: suppress zero lines, override Prod Co Fee dispersed/separate |
| ✅ | Suppress zero lines | Hide rows whose total rounds to $0 from the export |
| ✅ | Per-export fee override | Pick "use current setting" / "separate line" / "dispersed" per export without changing the saved budget |
| ✅ | Conditional columns | Empty Payroll / OT / Fringe / Agent% / Disc% columns drop entirely |
| ✅ | Dispersed-mode rounding (PDF only) | Each line + section + grand total rounds to nearest $10 deterministically — clean numbers for client presentation |

---

## Schedule-Driven Auto-Calcs

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Per Diem (4 variants) | Full / Breakfast / Lunch / Dinner — each its own budget line, fed by per-cell schedule flags |
| ✅ | Hotels by role group | Hotel — Talent / ATL / Crew — separate lines fed by hotel flag + role group of parent line |
| ✅ | Flights by role group | Same shape as hotel |
| ✅ | Mileage by role group | Same shape |
| ✅ | Working Meals | Per-cell flag → per-person count |
| ✅ | First / Second Meal | Per-date flag × working headcount that day |
| ✅ | Courtesy Breakfast | Same |
| ✅ | Craft Services | Auto-set on every non-off scheduled day |
| ✅ | Manual OT on flat / weekly rates | "+2h" entered on a schedule cell now calculates OT even when rate type is flat / week / no payroll profile |
| ✅ | Schedule mode dedup | Legacy NULL-mode rows + new mode-tagged rows don't double-count |

---

## Settings

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Project name & client | Edit names from Settings tab |
| ✅ | Production Company Fee % | Plus dispersed toggle |
| ✅ | Sections exempt from Prod Co Fee | Per-COA-section checkboxes |
| ✅ | Exclude fringes from fee base | Default ON |
| ✅ | Workers' Comp % | Default 3% |
| ✅ | Payroll Service Fee % | Default 1.75% |
| ✅ | Production Liability Insurance | Mode (off / % / flat) + value |
| ✅ | Payroll profile | Pick the union profile that drives ST/OT/DT calculations |
| ✅ | Payroll week start | Override the profile's default week-start day |
| ✅ | Timezone | Per budget |
| ✅ | Production details for PDFs | Client name, prepared by, title, email, phone — appears on PDF cover |

---

## Admin / Catalog

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Global Quick Entry catalog | Super-admin manages the curated list of common roles + items shown in QE |
| ✅ | Bulk operations | Multi-select catalog items → delete or move to another category |
| ✅ | Code repair tool | Super-admin: re-map account codes when COA changes |
| ✅ | Veryfi diagnostic | Super-admin status page for OCR config |
| ✅ | Dropbox diagnostic | Super-admin status page for filing path config |

---

## 📋 Activity Tab + Undo (planned — next build)

User request: every change made to a budget gets logged with user, timestamp, and a per-action `$ delta` showing how the change moved the grand total. List view by tab/section with filters. Per-row "Undo" button that reverses the change.

| Visibility level | What they see |
|---|---|
| Super admin | Everything across every project |
| Admin | Everything in projects they have admin role on |
| Department head | Only changes made within their `dept_code` |
| Editor / Viewer | Only their own changes |

Implementation outline:
- New `AuditLog` table: `(id, user_id, project_id, budget_id, action, target_type, target_id, payload_before_json, payload_after_json, budget_total_before, budget_total_after, created_at)`
- Hook: small wrapper around every mutating endpoint that snapshots before/after totals
- Activity tab: paginated list with filters (user, action type, date range, target type)
- Undo: replay the inverse of the recorded change. Some actions inherently undoable (field edits, deletes, schedule cell flips); some not (e.g. Dropbox file moves) — those rows show "Cannot undo" with explanation.

---

## Cross-Cutting

| Status | Feature | Plain English |
|---|---|---|
| ✅ | Live multi-user | Socket.io presence + live patches when teammates edit the same budget |
| ✅ | Auto-save | Inline edits save on blur; no explicit "Save" needed |
| ✅ | Per-worker schema self-heal | Critical column adds run on every gunicorn worker boot — production deploys never break on schema drift |
| ✅ | Conversation archive | Each Claude session snapshot lands in Dropbox at `SOFTWARE BUILDS/FPBudget/conversation_archive/` |
| ✅ | Cross-device session resume | `claude --resume <id>` continues this conversation on either Mac via Dropbox sync |

---

## Glossary

- **Dispersed mode**: Production Company Fee is spread proportionally across every section's total instead of shown as its own line. Some clients prefer this for presentations.
- **Fee base**: The dollar amount the Prod Co Fee applies to. Sections marked exempt and (when configured) labor fringes are subtracted from it.
- **Schedule mode**: Each schedule view is either "estimated" or "working" — they're independent so locking the estimate doesn't freeze you out of the working schedule.
- **Auto-line**: A budget line whose qty/days are computed from the schedule rather than typed in. Identified by a `line_tag` like `per_diem_full` or `hotel_crew`.

---

## Recent Pushes

| Commit | Date | Summary |
|---|---|---|
| `a3fefae` | 2026-04-28 | PDF dispersed-rounding: flat $10, deterministic per-line |
| `db92d05` | 2026-04-28 | Conditional columns (Payroll / OT / Fringe / Agent% / Disc%) |
| `7f7d120` | 2026-04-28 | Travel modal centered + larger fonts; fix Catering "Loading…" stuck |
| `a6c163f` | 2026-04-27 | Catering day-cards + per-person weekly rollups |
| `cdf373c` | 2026-04-27 | Drop emojis, editable day-type in Travel |
| `953c2a3` | 2026-04-27 | Travel day-card layout + Add Travel Day fix |
| `8474e8b` | 2026-04-27 | Production Liability Insurance auto-line |
| `7156182` | 2026-04-27 | Per-export options dialog (suppress zeros + fee override) |
