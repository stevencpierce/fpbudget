# FPBudget — Feature Registry

**App:** FPBudget (Framework Productions Budget Planning)
**Live URL:** https://fp-budget.onrender.com
**Repo:** https://github.com/stevencpierce/fpbudget
**Stack:** Flask · SQLAlchemy · Postgres · WeasyPrint · Jinja2 · Flask-SocketIO (gthread)
**Last Updated:** 2026-05-11 (commit `bf42832`)

> Status icons: ✅ Live · 🔧 In Progress · 📋 Planned · ⚠️ Known Issue · 🗑️ Removed

---

## What's New (2026-04-29 → 2026-05-11)

Compact, grouped summary of everything added since the previous Last Updated.
Detail rows added to the per-category tables below; full commit list at the
bottom under "Recent Pushes".

### Chart of Accounts overhaul
| ✅ | COA renumbered to MMB / ShowBiz industry-standard structure | Old codes 100–20500 remapped to 1000–6800. Inline data migration runs once per DB and writes a `coa_change_log` audit row for every old→new pair. |
| ✅ | Migration covers budget_line, budget_template_line, catalog_item, users.dept_code | Atomic CASE WHEN UPDATE per table in a single transaction. Merge collisions on `catalog_item` get `" (legacy)"` suffixed. |
| ✅ | Transaction.account_code migration | Separate CASE WHEN pass on `transaction` rows (rewritten 2026-05-08 to fix a chain-bug from the earlier sequential version). |

### Quick Entry catalog + role mapping (scaffolding)
| ✅ | CatalogItem.role_tag + phase columns | Promotes labor catalog items into the master role list with a stable slug. |
| ✅ | RoleTagMapping table | Maps internal role tags to MMB / ShowBiz target account codes. Super-admin editable via `/admin/role-mapping`. |
| ✅ | BudgetLine.catalog_item_id FK | Lets exports look up the role-tag mapping per line. |
| 📋 | QE_CATEGORIES unification with DB | Replace the hardcoded JS array with a `/api/catalog` fetch on page load. Phase 2 — see HANDOFF.md. |

### Purchase Order system rebuild
| ✅ | PO calc: Cap / Lines / Estimates / Billed / Remaining | Five distinct numbers replace the old single "budgeted" rollup. |
| ✅ | Over-cap warning fires only on real spend | `billed_total` (sum of Transaction.amount on PO's lines) is the threshold; lines/estimates above cap show as soft yellow heads-up. |
| ✅ | Receipts attached note | When attached receipts exceed billed, an info banner shows "$X not yet posted as transactions". |
| ✅ | Estimate / receipt attachment split by doc.category | Estimates: estimate/quote/contract/PO. Receipts: receipt/invoice. Mismatch banner compares lines vs estimates specifically. |
| ✅ | Read-only PO badge in cross-views | Purple pill (📋 PO-XXXX) on each non-labor line in Estimated/Actual views; vendor name on hover. Distinct from the editable green badge in Working view. |
| ✅ | PO surfaced in Actuals transaction expansion | Header pill (📋 PO-XXXX · Vendor) above each line's transaction detail. |
| ✅ | Inline-edit PO #/vendor/Cap on the PO card | Click any of the three → input → Enter saves → page refreshes computed fields. Skip the modal for fast edits. |
| ✅ | Note on POs page: rollups read from Working budget only | Explicit info box explaining the canonical-budget selection. |
| ✅ | PO over-cap based on receipts not budget projection | Pre-rewrite logic that's now superseded by billed_total. |

### Variance system
| ✅ | Three bases: Estimated V Working / Working V Actual / Estimated V Actual | Renamed from "W vs E" etc. so labels match the math (X V Y = X − Y). |
| ✅ | Math + label convention | Positive = remaining/under budget (BLUE). Negative = over budget (RED). |
| ✅ | Real-time variance update on inline-save | Save handler updates data-e/-w/-a on per-line + section + grand-total cells, then re-applies the active basis. No more stale variance after edits. |
| ✅ | Variance basis persistence across budget mode switches | localStorage `fpbudget.varianceBasis` retains the user's choice between Estimated/Working/Actual navigations. |
| ✅ | Variance basis column-header highlight | Blue box around the first budget tier, red around the second, so the active comparison is clear at a glance. |

### View differentiation
| ✅ | Per-view color scheme | Estimated = blue, Working = orange, Actual = red. Applied via `<body class="view-estimated">` etc. |
| ✅ | Section block top borders match view color | 3px accent strip across each section card. |
| ✅ | Mode-switcher button highlights match view color | Active button background = view color. |
| ✅ | Section header dollar badge tinted per view | The dollar pill next to each section heading uses the view's color. |
| ✅ | "Total [Section]" label colored + bolder per view | Reinforces view at scroll depth. |
| ✅ | Active grand-total column value colored per view | Larger font + matching color. |
| ✅ | Top accent strip across budget area | 4px colored bar on the page. |

### Sticky UI + scroll persistence
| ✅ | Sticky tabs row | Stays at top of viewport on scroll. Contains the Estimated/Working/Actual mode switcher. |
| ✅ | Sticky toolbar | Quick Entry + variance buttons + search box stay just below the tabs. |
| ✅ | Sticky Top Sheet variance toggle | Same offset as the budget-tab toolbar. |
| ✅ | Preserve scroll on budget mode switch | sessionStorage capture before navigation, restore after page load. 60-second TTL, project-scoped. |

### Cross-budget live data
| ✅ | Per-line E/W/A columns read live | Estimated_line_totals / working_line_totals / actual_line_totals dicts feed the cross-view cells. |
| ✅ | Top Sheet's Working column reads live | working_by_section pulls from working_line_results (live calc) instead of frozen ln.working_total snapshots. |
| ✅ | working_line_results dict | Full _wres calc dict cached per Working line id, reused across multiple downstream consumers. |
| ⚠️ | Section subtotal badges still partial | Some per-section badges in the line-item view still read frozen snapshots in fallback paths. Per-line cells are live; the badges are cosmetic. C-7 cleanup deferred. |

### Performance / memory
| ✅ | N+1 ScheduleDay queries eliminated | Bulk-fetch once per cross-budget, bucket by line_id in Python. Cuts query count from ~200 to 2 per request. |
| ✅ | Same-budget cross-view fast path | When viewing the Working budget, skip the redundant query+calc for working_line_totals — reuse `line_results`. |
| ✅ | Sister-budget resolution moved earlier | current_working_bid / current_estimated_bid resolved before any block that uses them — eliminates `peer_actual_bid` style UnboundLocalErrors. |
| ✅ | Gunicorn `--max-requests 100 --jitter 20` | Workers voluntarily recycle every 80–120 requests so memory sheds before Render's OOM killer triggers. |
| ✅ | Thread count bumped 4 → 8 per worker | 16 concurrent slots (2 workers × 8 threads) to survive socket.io long-polling tying up threads. |

### Root cause fix: is_actual budget misclassification
| ✅ | Filter out is_actual=True from current_working_bid resolution | The Actual budget shares budget_mode='working' with the real Working budget; is_actual is the only discriminator. Before this fix, the Actual could win the lookup when newer, causing cross-views to read its data instead of Working's. Same fix applied to has_working_budget and the version_groups builder. |

### Error handling & debug
| ✅ | Global 500 handler with ERR-XXXX ref | Unhandled exceptions log full traceback under `ERR-<8 hex>` and show the user a small page with just the ref. Grep Render logs for the ref to find the stack. |
| ✅ | Calc-trace HTML debug page | `/admin/debug/budget/<bid>` — auto-refreshing table of every line + both calc paths' outputs + frozen snapshot comparison. Red rows = snapshot mismatch, yellow = NULL-mode schedule legacy. |
| ✅ | Line JSON debug endpoint | `/admin/debug/line/<bid>?account_code=…&description=…` — full data dump per line for surgical debugging. |
| ✅ | Trash-purge fixed | Was silently failing every boot due to `db.engine` accessed outside app context. Wrapped the entire `_maybe_run_trash_purge` body in `app.app_context()`. |

### Actuals + transactions
| ✅ | Actuals view: transaction rollup as Actual column | Across all three budget modes (Estimated/Working/Actual), the Actual column shows sum of linked transactions. |
| ✅ | Actuals view: receipt-first reconciliation | Need-receipt scoping, cross-project claims. |
| ✅ | Actuals view: drag transactions to reassign lines | Inline drag-drop in the per-line transaction detail expansion. |
| ✅ | Actuals view: drop on "Not Project" | Quick exclusion target for personal expenses caught in the QBO sync. |
| ✅ | Auto-clone Working → Actual on first transaction link | Creates a separate Actual budget the first time a transaction is linked to a line. |
| ✅ | QBO sync: Bank + Credit Card account picker | Filter to relevant account types, one-click "Add account" button. |
| ✅ | QBO sync: per-entity query (no broad scans) | Query each entity once, log diagnostic counts. |
| ✅ | QBO sync: drop invalid entities | Removed CreditCardCredit (invalid) and Bill (out of scope). |
| ✅ | Actuals: auto-reload after QBO account save | Re-renders the sync panel gate. |

### Travel + per-diem
| ✅ | Per-person travel/per-diem mirror rows | Read-only italic child rows under each labor line showing that person's slice of Travel section aggregates. Source of truth stays in the aggregates. |
| ✅ | Travel day-card layout | Each date is its own card with day's travelers grouped inside. Click header to collapse/expand. |
| ✅ | Editable day-type in Travel | Drop the emojis, in-line day-type editor. |
| ✅ | Travel "Only flagged" filter | Hide crew not marked for any travel item. |

### Catering
| ✅ | Catering day-cards + per-person weekly rollups | Same UX as Travel. |
| ✅ | Craft Services becomes per-day toggle | Previously auto-counted; now user-toggled per ProductionDay flag. |

### Schedule-driven lines
| ✅ | Stop clobbering user edits on schedule-driven lines | Manual edits to qty/days/rate/estimated_total auto-set sync_omit=True so the next page load doesn't overwrite. |
| ✅ | sync-omitted visual cue | Italic + small icon indicator on rows where auto-sync is disabled. |
| ✅ | Schedule-driven labor: blue italic "Days" label | Italic description for visual distinction. |
| ✅ | Schedule clone copies is_production_day + TravelDetail rows | Verbatim, not just the day_type / cell_flags. |
| ✅ | Inherit every Budget setting on clone | Not just half. Includes payroll_profile, payroll_week_start, fee settings, etc. |

### Top Sheet
| ✅ | Label totals with active mode | "Subtotal (Working, Before Prod. Fee)" — clarifies which budget the column reflects. |
| ✅ | Auto-line amounts shown in Working column | Workers' Comp / Production Insurance / Payroll Fee mirror into both Estimated and Working columns. |
| ✅ | Apply fee dispersal + Production Insurance to Working column | Top Sheet's Working column matches Budget tab when nothing's changed. |

### Project management
| ✅ | Project duplicate | Clone settings + folder structure to a new project. |
| ✅ | Rename with folder | Rename a project AND its Dropbox folder atomically. |
| ✅ | Rock-solid clone settings | Cover every Budget setting in the clone path. |

### Fringes
| ✅ | Per-project fringes | Override the global library at the project level. |

### Calc fixes
| ✅ | Fix "line halves on edit" | Blank numeric fields now mean zero, not 1.0 fallback. |
| ✅ | Labor qty invariant | Labor lines force qty=1 on save (multi-person split into separate rows). |
| ⚠️ | Travel-Flights "exactly half" workaround | Auto-managed lines (per_diem, flight, hotel) have a `qty_val or 1.0` fallback that produces half-value when qty=0. **Workaround:** apply 100% discount instead of zeroing qty. **Permanent fix pending** — see HANDOFF.md. |

### Activity log
| ✅ | Activity timeline tab | Per-budget audit trail of every line create/update/delete with before/after JSON. |
| ✅ | Dollar delta column | $ change per activity row. |
| ✅ | Filterable by entity type + action | budget / docs / actuals / qbo / all. |

### Docs / Document Analyzer
| ✅ | Docs tab embedded in budget | Per-project doc list with upload, OCR review, manual filing. |
| ✅ | Veryfi OCR integration | Auto-extracts vendor/amount/date/category. |
| ✅ | Dropbox auto-filing | ≥90% confidence auto-files to the correct subfolder; <90% goes to review queue. |
| ✅ | Duplicate detection | Hash-based, scoped per project, excludes review-status rows. |
| ✅ | Estimate docs → PO | Create PO from a doc, or add to existing PO. |
| ✅ | PO doc attachments | Source estimate + additional attachments with role tagging. |
| ✅ | Mobile uploader — Commit 1 (GET /upload) | Phone-first upload page at `/upload`. Project picker (localStorage-persisted), 📷 Take Photo, 🖼 Choose from Library, drag-drop on desktop, per-file status queue. Allowlisted for `docs_only` role. Reuses the existing `/docs/<pid>/upload` pipeline (Veryfi OCR + Dropbox filing + dup detection). Shipped 2026-05-11 (commit `69b1460`). |
| ✅ | Mobile uploader — Commit 2 (PWA + nav) | PWA manifest, apple touch icons, "Add to Home Screen" support, admin nav link. Shipped 2026-05-11 (commit `2232855`). |

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
| `2232855` | 2026-05-11 | Mobile uploader: PWA manifest + icons + nav links (Commit 2) |
| `69b1460` | 2026-05-11 | Mobile uploader: GET /upload with project picker (Commit 1) |
| `bf42832` | 2026-05-11 | HANDOFF.md: mark session ended, note Dropbox copy location |
| `881ebfc` | 2026-05-11 | Add HANDOFF.md — session state for cross-device continuity |
| `96cb8ab` | 2026-05-10 | Subtotal headings adopt view color (estimated=blue, working=orange, actual=red) |
| `e543747` | 2026-05-10 | Sticky tabs/toolbar + preserve scroll on budget switch |
| `7da5bd5` | 2026-05-10 | View differentiation + variance basis persistence |
| `fb6cf10` | 2026-05-10 | Variance colors: negative = red, positive = blue |
| `eb09ad5` | 2026-05-10 | Inline-edit PO number, vendor, and cap directly on the card |
| `c677a06` | 2026-05-10 | PO surfacing: visible badge in cross-views, scope note on POs page, PO badge on actuals txn detail |
| `a0b7c4a` | 2026-05-10 | PO calc: split estimates/receipts/billed, alert only on real overspend |
| `d663d15` | 2026-05-10 | Real-time variance: refresh data-e/-w/-a after inline-save |
| `34915f3` | 2026-05-10 | Variance basis: rename labels to match math, 'X V Y' = X minus Y |
| `74ec8e7` | 2026-05-10 | Add --max-requests to gunicorn for graceful worker recycling |
| `6ff4d90` | 2026-05-08 | C-6: Bulk-fetch ScheduleDay in cross-view loops (N+1 → 2 queries) |
| `9bdc882` | 2026-05-08 | Reapply C-5: Exclude is_actual budgets from current_working_bid (THE root-cause fix) |
| `feea9b7` | 2026-05-08 | C-4: Skip redundant N+1 query loops on same-budget cross-views |
| `193cbc0` | 2026-05-08 | Reapply C-3: Top Sheet's working_by_section reads live, not snapshots |
| `48f1b4e` | 2026-05-08 | C-2v2: Move sister-budget resolution + working_line_results above working_by_section |
| `9194c04` | 2026-05-08 | Option B: Calc-trace HTML page at /admin/debug/budget/<bid> |
| `05009fb` | 2026-05-08 | Add /admin/debug/line/<bid> debug endpoint |
| `7c01e41` | 2026-05-08 | Add global 500 handler with error-ref + traceback |
| `cbd6692` | 2026-05-08 | Fix 502s, trash-purge, and COA transaction migration |
| `fa84feb` | 2026-05-07 | Fix UnboundLocalError on peer_actual_bid in budget_view |
| `ca69b35` | 2026-05-07 | Actual column = transaction rollup across all views |
| `e95c698` | 2026-05-07 | PO over-cap fires on receipts, not line projection |
| `3b5e480` | 2026-05-07 | PO/crew are project-level: read-only dots in Estimated and Actual |
| `db5dfec` | 2026-05-07 | Highlight column headers based on active variance basis |
| `50873d7` | 2026-05-07 | PO card: show line breakdown + flag doc-vs-line mismatch |
| `1e409b8` | 2026-05-07 | Actual view: hide qty/duration/rate, subtotal = sum of transactions |
| `4536aee` | 2026-05-07 | Fix "line halves on edit" — blank numeric fields now mean zero |
| `e4771b9` | 2026-05-07 | sync-omitted: visual cue for manually-overridden schedule lines |
| `479a0ee` | 2026-05-07 | Stop clobbering user edits on schedule-driven budget lines |
| `9f4e7ea` | 2026-05-07 | Defensive guards for cross-project claim queries |
| `22dc659` | 2026-05-07 | Budget view: show all 3 budgets (E/W/A) live in every mode |
| `b1a80eb` | 2026-05-07 | Actuals: receipt-first reconciliation, need-receipt scoping, cross-project claims |
| `7d08d2c` | 2026-05-07 | QBO sync: drop CreditCardCredit (invalid entity), fix misleading warning |
| `541e429` | 2026-05-07 | QBO actuals: filter picker to Bank+CC, add one-click "Add account" button |
| `2606a4b` | 2026-05-07 | Working view: live Estimated column instead of frozen snapshot |
| `2f36534` | 2026-05-07 | Budget tab: variance basis selector matching Top Sheet |
| `1fd8b25` | 2026-05-07 | QBO sync: add Bill/BillPayment + diagnostic logging |
| `f05e48f` | 2026-05-07 | Top Sheet: label totals with active mode + fix header alignment |
| `1c9e8c7` | 2026-05-07 | Project duplicate + rename-with-folder + rock-solid clone settings |
| `b4265ca` | 2026-05-06 | Top Sheet: show auto-line amounts in Working column too |
| `0b42051` | 2026-05-06 | Top Sheet: apply fee dispersal + Production Insurance to Working column |
| `ba96369` | 2026-05-06 | Sync source + clone before snapshotting working_total |
| `12825ed` | 2026-05-06 | Self-heal: add missing Budget columns on worker boot |
| `466d0a5` | 2026-05-06 | Craft Services becomes a per-day toggle, not auto-counted |
| `88f5f09` | 2026-05-06 | Schedule clone now copies is_production_day + TravelDetail rows verbatim |
| `9827ca7` | 2026-05-06 | Inherit every Budget setting on clone, not just half of them |
| `53ccc0e` | 2026-05-06 | Inherit payroll profile + week start when cloning Working from Estimated |
| `d57f9c9` | 2026-05-06 | PO/Doc fixes: rollup, OCR, multi-select drag, manual edit propagation |
| `db1b99c` | 2026-05-05 | Fix Docs row layout + PO actions in detail modal + harden po_save |
| `90db305` | 2026-05-05 | PO page: render the budget tab strip so context isn't lost |
| `068d1f3` | 2026-05-05 | Estimate docs → PO: Create PO from doc + Add to existing PO |
| `5f04c8c` | 2026-05-05 | Plan A: per-person travel/per-diem mirror rows on labor lines |
| `8814cf4` | 2026-05-05 | Right-click on manual OT field → bulk clear OT |
| `5778363` | 2026-05-05 | Clearer OT column: distinguish manual vs schedule-driven |
| `b4c9b48` | 2026-05-05 | Kit-fee rows: broader detection + larger click targets on editables |
| `111d894` | 2026-05-05 | Per-project fringes — overrides global library |
| `0a8d9b6` | 2026-05-04 | + PO button on non-labor lines: pick existing or create inline |
| `a3fefae` | 2026-04-28 | PDF dispersed-rounding: flat $10, deterministic per-line |
| `db92d05` | 2026-04-28 | Conditional columns (Payroll / OT / Fringe / Agent% / Disc%) |
| `7f7d120` | 2026-04-28 | Travel modal centered + larger fonts; fix Catering "Loading…" stuck |
| `a6c163f` | 2026-04-27 | Catering day-cards + per-person weekly rollups |
| `cdf373c` | 2026-04-27 | Drop emojis, editable day-type in Travel |
| `953c2a3` | 2026-04-27 | Travel day-card layout + Add Travel Day fix |
| `8474e8b` | 2026-04-27 | Production Liability Insurance auto-line |
| `7156182` | 2026-04-27 | Per-export options dialog (suppress zeros + fee override) |
