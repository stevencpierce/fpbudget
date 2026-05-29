# FP Budget — User Guide

**Version:** v1.1.0  ·  **Updated:** 2026-05-29
Live at: https://fp-budget.onrender.com

A plain-English guide to what the software does and how to use it. Written
for humans, not engineers. Skim the headings, jump to what you need.

---

## 1. The big picture

FP Budget is a film/event **production budgeting + actuals** tool. One
project flows through three stages:

1. **Estimated** — your first planned budget.
2. **Working** — the live, negotiated budget you actually run the show on.
3. **Actual** — what really got spent, reconciled against QuickBooks bank
   charges and uploaded receipts/invoices.

Everything hangs off a **Chart of Accounts (COA)** — numbered sections like
2000 Production Staff, 2100 Talent, 3300 Locations, etc.

You switch stages with the **Estimated / Working / Actual** buttons at the
top right. The **Variance basis** buttons (Estimated-v-Working, etc.)
control which two columns the Variance column compares.

---

## 2. Getting around

- **Projects** (top nav) — pick a project to open its budget.
- Inside a project, the **tabs** are: Top Sheet, Budget, Schedule, Travel,
  Catering, Contacts, Locations, POs, Sub-Budgets, Call Sheets, **Docs**,
  **Actuals**, Activity, Tools, Settings.
- Your active tab is remembered in the URL, so a refresh keeps your place.
- **Crew**, **Locations**, **Templates**, **Fringes**, **Admin** live in the
  top nav and are shared across projects.

---

## 3. Building a budget (Budget tab)

Lines are grouped by COA section. Each labor line = **one person**
(multiple people = multiple lines, or use a quantity that splits them).

**Adding lines**
- **+ Single Line** — add one blank line to a section.
- **⚡ Quick Entry** — the fast way. A catalog of common roles/items by
  department. Search across **all departments** at once, check what you
  want, set qty/days/rate/fringe, and **Add Selected**. Union/non-union
  toggles adjust rates where applicable.
- **Right-click a line** for the row menu: insert above/below, insert a
  **header** or **spacer** (visual dividers), **Add Kit Fee** (a child line
  under a labor row), **Duplicate**, **Change Group**, **Move to section**.

**Editing**
- Click any field (rate, days, qty, description, type) to edit; changes
  **save automatically**.
- **Drag** the handle to reorder lines; child rows (kit fees, mileage,
  per-diem) follow their parent.
- **Duplicate a row**: you're asked (in a clean Yes/No/Cancel box) whether
  to also copy its schedule days. "No" makes the copy start in plain
  estimated mode.

**Fringe codes** (the Fringe column): E = exempt/none, N = standard payroll
burden, plus L/U/S/I/D for specific cases. Fringe is added on top of the
base in the line total.

---

## 4. The Schedule (Schedule tab / Gantt)

- Set the **Production Day** count and dates at the top.
- Schedule-driven labor and location lines pull their day counts from the
  grid (they show in blue/italic and are checked on the days they work).
- Paste works like a spreadsheet: copy a run of days from one row, click a
  single day on another row, paste — it tiles the same pattern.

---

## 5. Crew & assignments

- **+ Assign** on a labor line opens the picker. Search existing crew, or
  **+ New Person** — type a name not found and the form auto-fills the name;
  fill phone/email/department and **Save & Assign**. (The form clears each
  time, so one person's details never carry into the next.)
- **Crew** (top nav) is the master people database — edit anyone, see which
  projects they're on, store agents/reps, default rates.
- Adding a person **dedups**: if the name/email already exists, it reuses
  the existing record instead of making a duplicate.

---

## 6. POs and Sub-Budgets

- **POs** — purchase orders: track vendor commitments and a cap; lines get a
  PO badge and roll up against the PO.
- **Sub-Budgets** — group a subset of lines into a named slice you can
  export as a client-facing mini-PDF and track as an actualized rollup.

---

## 7. Docs tab — receipts, invoices & paperwork

This is where every document lives. Upload, classify, file to Dropbox, and
review.

**Uploading**
- **Drag files** onto the dropzone, **or drag whole folders** (it searches
  every subfolder), **or** "📁 choose a folder", **or** Take Photo / From
  Gallery on mobile.
- Each file is OCR-analyzed (Veryfi): it detects the **vendor, amount,
  date, document type**, names it by convention, and files it into the
  right Dropbox subfolder automatically (at high confidence).
- **Upload All** processes the queue; the button shows the count and locks
  while uploading so you can't double-submit.

**Finding things (subtabs + search)**
- Tabs across the top: **Review** (the prominent landing tab — anything
  needing a decision: low-confidence OCR + possible duplicates), **All**,
  then one tab per **document type** (Receipts, Invoices, Estimates, POs…),
  and finally **Duplicates** (confirmed duplicates, parked for un-flagging).
- **Search box** filters by vendor / filename / note within the active tab.
- **Sort by** vendor, date, amount, type, confidence, etc.
- The **Type · Confidence** column shows the detected type and OCR
  confidence % (green ≥90, amber ≥70, red below).

**The document panel** (click any row)
- Big preview + editable fields: vendor, **Amount (USD)**, doc date, type,
  document #, note, and crew/location links.
- **Foreign-currency invoices:** put the **USD** value (what hit the card)
  in *Amount (USD)* and the native figure + code (e.g. 1,500,000 / KRW) in
  *Original amount / Currency*. USD reconciles + rolls into the budget; the
  original is preserved and shown under the amount.
- **‹ Prev / Next ›** (or ← / → keys) page through the current
  filtered/sorted list without closing. Esc closes.
- **Editing + Save re-files Dropbox automatically** — change the vendor,
  type, date, or amount and the filed file is moved + renamed to match (no
  orphaned files).

**Duplicate review**
- If a byte-identical file is uploaded, it's flagged (not auto-hidden).
- **⇆ Compare group** opens all identical copies side-by-side; mark each
  **Keep** or **Duplicate** (at least one must be kept), then **Apply**.
- **Keep both** = they're genuinely separate. **It's a dupe** = filed away
  to a `/_DUPLICATES/` Dropbox subfolder.
- **Move all flagged → /_DUPLICATES/** bulk-confirms everything still
  flagged (each group's original is kept automatically).
- An assigned document (coded to a budget line) is **never** treated as a
  duplicate — its link is protected.

---

## 8. Actuals tab — reconcile spend

Match real spend (bank charges + receipts) to budget lines.

- **Sync QBO** pulls bank/credit-card transactions from QuickBooks (pick
  which accounts feed the project in Settings).
- The **stat cards** (Finished / Coded / Need coding / Need receipt / …)
  filter the list.
- **Code a transaction**: pick a budget line from the per-row dropdown, or
  drag a line from the **Chart of Accounts** sidebar onto the transaction
  (or drag the transaction onto the tree). First link auto-creates the
  Actual budget from Working.
- **💡 Smart suggestion** — on uncoded rows, the system matches the vendor
  name to a budget line (by description + assigned person) and offers a
  one-click chip. Refreshes each time you open the Actuals tab.
- **Add Receipt** attaches a receipt to a transaction; **Reconcile** pane
  pairs unmatched transactions ↔ unlinked receipts.
- The **Chart of Accounts** sidebar shows section spacers/sub-headers,
  per-line **👤 assigned person** and **📋 PO** badges, and a larger,
  legible layout.

---

## 9. Exports

From the Budget tab export menu (each opens an options dialog —
suppress-zeros, fee handling, and a PDF **column picker**):
- **PDF** — Top Sheet or Full Detail.
- **CSV** — Top Sheet or Line Detail.
- **Movie Magic Budgeting** (.txt) and **ShowBiz Budgeting** (.txt), with a
  preview drawer.

---

## 10. Collaboration

- Multiple people can work a budget at once; you see who's viewing
  (avatars) and live edits patch in. If two people edit the same field, the
  higher-role edit wins and the other person gets a notice.

---

## 11. Dropbox filing (behind the scenes)

Documents are filed under your operations folder:
`{project}/01_ADMIN/PROCESSED DOCUMENTS/{TYPE}/{uploader}/[{vendor}]/{date_TYPE_vendor_amount.pdf}`
- Every upload's original bytes are also kept in a `_SOURCE_ARCHIVE/` folder
  as a permanent fail-safe.
- Editing a doc re-files it to match; confirmed duplicates go to
  `/_DUPLICATES/`. An **admin reconcile** tool can re-sync a whole project's
  files to match the software if anything drifts.

---

## 12. Access levels (current + planned)

Roles today: super_admin, admin, line_producer, dept_head, docs_only.
**Planned (notated, not yet enforced everywhere):** document/employee-packet
visibility scoped by role — admins see all (incl. other/former projects),
line producers see only their project, lower roles none. See the roadmap.

---

## Versioning & maintenance

- This guide ships **inside the repo** and is versioned with the code.
- **v1.1.0 (2026-05-29)** — current stable: Docs overhaul (folder import,
  duplicate group review, foreign-currency amounts, save-time Dropbox
  re-filing), Actuals smart suggestions, crew dedup, reconcile tooling.
- v1.0.0 (2026-04-06) — original production baseline.
- Going forward: bump the version here + tag the repo (`vX.Y.Z`) on each
  meaningful release, and keep a snapshot in
  `…/SOFTWARE BUILDS/FPBudget/`.
