# Session Handoff — 2026-07-13/14 (call-sheet hardening sprint)

**Purpose:** full context-transfer for continuing this work from another machine.
On the laptop: clone/pull this repo, open Claude Code in the repo directory, and say
*"Read SESSION_HANDOFF_2026-07-14.md and continue where it leaves off."*

---

## 1. What this app is

**FPBudget** — Framework Productions' production-management platform.
Flask + SQLAlchemy + Postgres, deployed on Render. Veryfi OCR for receipts,
Dropbox for file archive, weasyprint for PDFs, Twilio for SMS, ntfy for
Steven's phone notifications.

- Live: **https://fp-budget.onrender.com** · repo `github.com/stevencpierce/fpbudget`, branch `main`
- Render service `fp-budget` (`srv-d7a217qa214c73ct41j0`), DB `fp-budget-db`
- Health: `GET /readyz` → `{"commit": <12-char sha>, "db": "up"}`
- ntfy topic for deploy/long-task notifications: `fpbudget-2UNogKZFtFM`
- Steven Pierce (steven@thefp.tv) = super_admin, product owner. Non-programmer;
  reports bugs by voice (expect transcription slips: "Clibrin/Clydework twenty
  five" = CLIBURN 25, "Jeff Goldwyn" = Jeff Goldblum, etc.)

**Active production context (mid-July 2026):**
- Project 20 = **CLIBURN 25** (Working budget **311**, Estimated 296, tz America/Chicago)
- Budget 311 was renamed **"Enter Stage Right S1"** by Steven (intentional) — it now
  carries a Jul 15 recording session at Levels Audio Inc (1026 Highland Ave, LA)
  with Jeff Goldblum narration (crew member id 83, Narrator line 12004),
  plus CLIBURN Jul 14–16 shoot days. Real sends have gone out; the call-sheet
  system is in live production use **right now**.

## 2. Working discipline (follow this)

Standing instruction from Steven: **"farm out to lower models when possible"** —
delegate builds to Opus subagents with detailed specs; the main session reviews,
validates, commits, deploys, live-verifies, and sends ntfy.

**Per-deploy pipeline (never skip):**
1. `venv/bin/python -m py_compile app.py models.py`
2. Jinja parse changed templates
3. JS check: **strip Jinja `{#..#}` comments FIRST**, then regex-split `<script>`
   blocks, `node --check` each. budget.html block-23 has a pre-existing false
   positive — compare failures against `git show HEAD:<file>`; only NEW ones matter.
4. **Route-binding audit**: for each `@app.route` line, the next `def`'s params
   must contain every `<...:name>` URL placeholder (catches stolen routes —
   see §5). Script lives in git history (commit ed2aeca message).
5. CSS brace balance if style.css touched.
6. Commit (`Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`, `--no-gpg-sign`),
   push, poll `/readyz` for the new sha **5–6 consecutive times** (Render
   auto-deploy misfires stale commits under rapid pushes — if it thrashes,
   Manual Deploy from the dashboard).
7. **Live-verify in a real browser on real data** (reversible probes: create →
   verify → delete/restore; never leave test rows — DELETE, don't blank).
8. ntfy Steven, mark the task done, update memory (memory files are LOCAL to
   the Mac at ~/.claude/projects/.../memory/ — this handoff doc is the portable copy).

**Security/behavioral constraints (standing):** never handle credentials/API keys
(Steven adds env vars in Render himself); never `git --no-verify`; AI features
advisory + fail-open; no financial mutations without explicit authorization;
never put personal data in URL params.

## 3. Everything shipped this session (all live + verified)

Chronological, with commits:

| Commit | What |
|---|---|
| 8b61d5a | Rich schedule sections: Today's Shooting Schedule + Advance Schedule as 1–6 free-text columns w/ B/I/U/align toolbar; call sheet ALWAYS the white designed theme (☀ toggle removed) |
| 6abf125 | Hotfix: TDZ crash — csRichScheduleInit() ran before `const CS_RICH_STATE`; killed the whole script block on load |
| 18e7df3 | Logo strip v2: slot snapping → one centered auto-flow row, drag-inserts-at-cursor, auto-wrap, 10+ logos; weasyprint flex-wrap verified |
| 8a0fe62 | Light-theme legibility: schedule/gantt day-type colors, chips, flags readable in the light app theme (`[data-theme="light"]` block ~style.css:2678) |
| 16e9d1b | **AD-sheet design pass**: boxed masthead, black GENERAL CREW CALL band, solid section bands, dense tables, editing chrome hidden until hover, Distribution panel full-width |
| 0fae9c5 | Design round 2: empty sections render NOTHING in recipient/pdf (per-section `_*_has` predicates, callsheet.html ~146-177); placeholder-leak fix; location letter badges in PDF (`pdf_ico('loc_*')`); PDF back to 2 tight pages |
| ed2aeca | **/gantt/day route was bound to `_reject_insane_year`** (helper inserted between decorators and handler, Jul 9) — every single-cell schedule save 500'd for 4 days; drag ranges worked via the batch endpoint |
| 590c246 | Rep modal: per-save `reloadWithTab()` wiped the next entry mid-typing (felt like a 3–4 rep cap); now in-modal refresh + ONE reload on close (`_repDirty`); visible error alerts |
| d2802b3 | Union/client/support modals opened at document top (`.modal-overlay` misused as top-level container) — id-scoped `position:fixed` + centering |
| 79cd92d | **Cache-busting**: all style.css/JS links now `?v={{ APP_COMMIT }}` — browsers were serving day-old assets through reloads |
| 4d5dfce | **Clients merge**: ProjectClientContact retired (table kept read-only); ProjectClient + `source`/`created_at`; one-time boot migration (marker `client_contact_merge_v1` in system_task_log + pg advisory lock 778811742 + CI-email upsert); estimate-send upserts ProjectClient (call-sheet flags stay False); ONE 🤝 Clients section w/ source chips |
| 03c7841 | Support-contact save 500 on blank company/email: JS sends explicit nulls; `dict.get(x,"")` default only covers MISSING keys → use `(x or "").strip()` |
| 5714e57 | Auto-fill geocode ladder: Nominatim returns nothing for unit numbers (`#2407`) → full addr → unit-stripped → city/zip tail |
| 94a3453 | Sun times in the SHOOT LOCATION's timezone (Open-Meteo `timezone=auto` parsed first; was browser tz — LA sunset showed 11:06 PM); 'TBD' counts as unfilled |
| 060b3ea | **Send list groups**: ⚖ Union + 🤝 Representation below crew — always listed, NEVER pre-checked, no persistence; removed the notify_callsheet auto-add; flags render as muted 'flagged' chips only |
| fbdb670 | **Vendors & Partners**: ProjectPartner model + boot DDL; People-tab 🚚 section (after Union Contacts); CRUD `/projects/<pid>/partners[...]`; third never-pre-checked send group; recipient_type 'partner' → crew view |
| 6cd5219 | **Per-view hides**: `cs_data.hidden` stays universal; new `cs_data.hidden_views={key:[view,…]}`; 👁 on Internal = universal, 👁 on ?view=talent/crew/client/union = that audience only; pink 'hidden: Talent' chips; server-side second display:none loop for per-audience PDFs; dept boxes collapse per-view |
| 9639123 | `callsheet_preview_as` accepts `?type=` (was HARDCODED crew — per-audience previews were impossible) |
| 7cb20c8 | **Union view fix**: union audience now shows logistics + KP (contact info still excluded); `_recipient_view_for_type('union')` → 'union' (was falling through to crew — editor and output disagreed both directions) |
| 1c28b8e | Send-panel 👁 Preview buttons pass the recipient's `data-type` through (`csPreviewRecipient(asVal, recType)`) — previews finally obey per-view hides |
| d0fec50 | **Cast time grid**: # / Name / Role / Pickup / Call / HMU / On Set / Wrap / Drop-Off; `cs_data.talent_times` (`sec\|\|role\|\|name` keys); CALL shares crew_call_times (single source, mirror guards last-wins clobber); rows cs-hideable (`talent_row:<key>`); '📋 Your times' on talent personal card; empty = dash internally, blank for recipients |

Earlier same-arc work already in production (see git log / HANDOFF.md / memory):
recipient full-sheet views, per-audience PDFs + Dropbox archive
(`01_ADMIN/CALL SHEETS/`), travel multi-entry + car service + hotel range sync,
location taxonomy, per-audience send messages, manual recipients, logos library,
Key Personnel picker, Company Health rollup, estimate-send upgrades.

## 4. IN FLIGHT right now

- **Rep view inheritance** (task: reps get their client's view — Jeff's agent →
  talent view; a crew member's rep → crew view). An Opus subagent was building
  this when this handoff was written. Resolution: SupportContact match
  (email-CI then name, scoped to crew with assignments on the budget) →
  crew_member → line section (Talent → 'talent', else 'crew') → applied in
  `_build_callsheet_recipient_context` (covers screen + preview) AND
  prepare_send's per-recipient PDF choice. Fallback 'crew'.
  **If the working tree has uncommitted app.py/callsheet.html changes, that's
  this build** — review (§2 pipeline), commit, deploy, verify:
  preview Zach Grove (`?as=zach.grove@caa.com&type=rep`) → expect talent view.
- After that: nothing queued. Steven will keep reporting from live use
  (shoot days Jul 14–16 + Levels session).

## 5. Hard-won gotchas (violating these has SHIPPED bugs — read before editing)

1. **callsheet.html is ONE template, three modes**: internal editing /
   `recipient_mode` (read-only + personal card) / `pdf_mode` (server-rendered,
   NO JS, weasyprint). Every change must work in all three. `cs-screen-only`
   marks edit-chrome.
2. **Whole-payload save**: `doAutoSave()` posts the entire `cs_data` JSON.
   New per-day state must ride `collectCallSheetData()` or it silently drops.
3. **`crew_call_times` and `talent_times` are DICTS** keyed `sec||role||name` —
   never iterate as arrays (a `.forEach` on it once emptied the whole send list).
4. **TDZ**: top-level JS init calls go AFTER every `const/let` they touch —
   an early call kills the entire script block silently (6abf125).
5. **Route stealing**: never insert a helper `def` between `@app.route`
   decorators and the handler (ed2aeca — 4 days of 500s). Run the route audit.
6. **Null-safe JSON**: client JS sends explicit `null`s; `dict.get(k, "")`
   default only applies when the key is MISSING → `(data.get(k) or "")`.
7. **`.modal-overlay` is a backdrop class** for inside `.modal` (position:fixed)
   — never a top-level modal container (d2802b3). New modals: copy #rep-modal.
8. **Static assets are cache-busted** via `?v={{ APP_COMMIT }}` — keep the
   param on any new `<link>/<script>`; the HTML itself is not cached.
9. **Boot DDL** (`_web_worker_essential_columns`) runs on EVERY worker boot —
   everything in it must be idempotent; one-time data migrations need a marker
   row + pg advisory lock (see `_migrate_client_contacts_into_clients`).
10. **Render is emoji-fontless** — PDFs use `pdf_ico()` letter badges
    (FLT/HTL/CAR/… + `loc_*`); drop NotoEmoji-Regular.ttf into static/fonts/
    to upgrade (README there).
11. **Audience flags** come from `_callsheet_audience_flags(view)` (matrix) —
    contact info gating: crew_contact/kp_contact/talent_contact/reps/clients.
    Recipient view per type: `_recipient_view_for_type` (client/talent/union
    mapped; rep pending inheritance build; partner/manual/other → crew).
12. **Per-view hides**: effective hide for view V = `hidden[key]` OR
    `V in hidden_views[key]`. Internal editor never hides; per-audience PDFs
    enforce server-side (Jinja loop in the pdf_mode style block).
13. **Sends** cache one PDF per audience (`_pdf_for_view`); each
    CallSheetRecipient stores its own token + viewed/confirmed; Dropbox archive
    fail-open. The emailed-PDF *fallback* renderer (legacy summary) lacks
    logos/dept-drags — only hit if the real renderer errors or yields <5KB.
14. **Verification tricks**: heavy budget page freezes CDP (probe from light
    pages; reload if frozen). PDF visual check: inject pdf.js from cdnjs into
    the authed tab, render pages to a canvas overlay, screenshot, then remove
    the overlay (a.click downloads are flaky; curl 302s on auth). Flipping
    `data-theme` via script leaves stale transition colors — verify theme
    changes with a real reload.
15. **Stale tabs**: twice, "the fix doesn't work" was an old tab. Fixes in page
    code/HTML need one reload; check `/readyz` commit + `typeof newFn` first.

## 6. Backlog (not urgent, memory-tracked)

- Dept-head checkbox + per-dept contact pages (call-sheet Phase 2 remnant)
- Tier 1 Committed/EFC budget columns; Tier 3 register polish
- Wrapbook payroll importer (3-leg round-trip plan)
- Timecards build-out (design locked: employee=timecard, loan-out/vendor=invoice)
- Actuals tool consolidation (CONSOLIDATION_PLAN_2026-06.md)
- CODE_REVIEW_2026-06-04.md HIGH backlog
- 22 stale Mundy-Test file paths; Daniel Hernandez $2,500 suspect split
- Versioning migration: actuals-live-on-Working (decided, migration pending)
- CLIBURN 25 matching test: 43 person invoices reset fresh 2026-07-08 for
  Steven's auto-match test; restore snapshot = `cliburn25_person_invoice_snapshot_2026-07-08.json`
  (LOCAL, gitignored, on the Mac in the repo dir)

## 7. Fast orientation for a fresh session

- `app.py` (~32k lines) = everything backend. Call-sheet region ~21200–23600:
  `_callsheet_full_context`, `_callsheet_audience_flags`, `callsheet_view`,
  `_render_callsheet_real_pdf`, `_build_callsheet_pdf_bytes`,
  `callsheet_prepare_send`, `callsheet_preview_as`, `_recipient_view_for_type`,
  `_build_callsheet_recipient_context`. Travel ~17400. Gantt ~16300–17000.
  Rep/union/partner/client CRUD ~21250–21550.
- `templates/callsheet.html` (~3900 lines) = the whole call-sheet system.
  Sections + predicates top; distribution panel JS ~3300–3800; rich schedule +
  logos + hides JS ~2700–3400.
- `templates/budget.html` (~22k lines) = the budget page (all tabs).
- `static/style.css`: designed light/PDF theme `:is(body.light-preview, body.pdf-mode)`
  ~3900+; app light theme `[data-theme="light"]`.
- Older docs in repo: HANDOFF.md (earlier-era), FEATURES.md, USER_GUIDE.md.

*Compiled 2026-07-14 from the live working session on Steven's Mac (Claude Code).*
