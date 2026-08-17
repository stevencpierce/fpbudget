# FPBudget Mobile App — Plan (2026-07-23)

Owner request: a mobile app for (1) dead-simple document/receipt upload for
low-level users, and (2) a richer experience for higher-level users — view and
edit budgets (estimated + working), call sheets, people, and view travel &
catering with basic interactions. Actualizing is explicitly out of scope for
now. Main open question: how to manage a second build (app store) alongside
the web app on Render.

This doc records the recommended architecture and phasing so any dev session
can pick up the work. Facts below were verified against the codebase on
2026-07-23.

---

## What already exists (verified)

- **A working mobile upload PWA.** `GET /upload` → `templates/mobile_upload.html`
  is a mobile-first, installable (manifest: `static/upload-manifest.json`,
  standalone display) upload page with camera/photo-library picker. No service
  worker, so no offline support, but cookie config (180-day remember cookie,
  `app.py:816`, `3467-3472`) was already tuned so the installed icon survives
  relaunch without re-login. This is the seed of the "turnkey uploader."
- **A docs-only user tier.** `User.role == 'docs_only'` (`models.py:37-39`)
  redirects to the docs dashboard at login and is route-gated to an allowlist
  (`_DOCS_ONLY_ALLOWED`, `app.py:3204`). Per-project access adds
  `owner|editor|viewer|docs_only` via `ProjectAccess` (`models.py:52-58`,
  `_require_project_role` at `app.py:3276`). **The permission model for
  "different access within a single app" already exists server-side.**
- **A large JSON surface.** ~714 `jsonify` call sites vs ~40 full-page
  renders. Budget line CRUD (`lines.json`, `line`, `line/insert`, `reorder`,
  `assign-crew`), call-sheet save/contacts, people summaries/profiles, travel
  and catering endpoints all exist as JSON. The two big *views* (budget grid,
  call sheet) are server-rendered HTML only; a mobile client would assemble
  them from the data endpoints. Some grid endpoints (travel/catering/gantt)
  return HTML fragments and need clean-JSON variants for mobile.
- **Auth is session-cookie only.** Flask-Login + custom CSRF header
  (`X-CSRFToken`, `app.py:761-767`). No token/JWT/API-key auth for users.
- **Upload pipeline.** `POST /docs/<pid>/upload` (multipart) → SHA-256 dedup →
  Veryfi OCR → auto-file to Dropbox (source of truth; R2 upload was removed).
  Requires the project to have a Dropbox folder or it 400s.

## Decision 1 — One app, not two

Ship a **single app with role-based experience**, mirroring what the server
already does:

- `docs_only` login → the app opens straight into the upload flow (pick
  project → camera/library → upload → see status). Nothing else visible.
- `viewer`/`editor`/`owner`/admin login → tab bar: Budgets, Call Sheets,
  People, Travel, Catering, Docs.

Rationale: one codebase, one App Store listing, one thing to keep updated, and
the server-side gates already enforce who sees what. If a separately-branded
"uploader only" app is ever wanted for optics, the same codebase can produce a
second build target later — don't pay that cost now.

## Decision 2 — Tech: React Native + Expo (native app), not a web wrapper

- The "not clunky like the web app on a phone" requirement rules out wrapping
  the existing desktop-heavy pages (budget.html is a multi-MB page).
- React Native + Expo gives iOS + Android from one JS/TS codebase, real native
  feel, first-class camera/photo-library APIs, and — critically — **EAS Update
  (over-the-air updates)**: JS-level changes push to installed apps in
  minutes, *without* App Store review. Only native-module changes require a
  new store build. This defuses most of the "two builds to keep in sync" fear.
- Alternative considered: polishing the existing `/upload` PWA further. Keep
  it (it works today and is a good stopgap), but the full product should be
  native.

## Decision 3 — Backend stays the single brain; add a thin `/api/v1` layer

The Flask app on Render remains the only backend. The mobile app is a pure
client. Nothing about budget math, permissions, or storage is duplicated.

Add to the Flask app (normal web-app work, deploys with `main` as usual):

1. **Token auth for mobile.** `POST /api/v1/auth/login` (email+password) →
   issue a long-lived random device token stored hashed in a new table
   (Alembic revision; e.g. `api_token: id, user_id, token_hash, device_name,
   created_at, last_used_at, revoked_at`). Requests send
   `Authorization: Bearer <token>`. A small decorator resolves the token to a
   user and reuses all existing role/`ProjectAccess` checks unchanged.
   `/api/` prefix is CSRF-exempt (token auth doesn't need CSRF).
2. **Wrapper endpoints, not rewrites.** Each `/api/v1/...` route is a thin
   wrapper that calls the same internals the web routes use. Priority order
   matches the phases below. Where a web endpoint already returns clean JSON
   (e.g. `lines.json`, `callsheet/contacts`, `people/summary`), the API
   version is mostly auth-translation. Where the web returns HTML fragments
   (travel/catering grids), add a JSON variant.
3. **Versioning discipline.** Installed apps lag server deploys, so `/api/v1`
   endpoints must stay backward-compatible once the app ships: add fields
   freely, never remove/rename without a `/api/v2`. OTA updates shrink but do
   not eliminate this window.

## Decision 4 — Repo layout: monorepo

Put the app in `mobile/` inside this repo. Render ignores it (no changes to
the web build); EAS builds from the subdirectory. One repo means one place for
sessions to work, and server API changes + the app code that consumes them
land in the same commit/PR. Split later only if it becomes painful.

**How "two builds" actually works day to day:**
- Web change → push `main` → Render deploys (unchanged, today's flow).
- Mobile JS change → `eas update` → installed apps get it OTA, no review.
- Mobile native change (rare: new permissions, new native modules) →
  `eas build` + store submission → Apple review (~1-2 days), Android faster.
- Apple/Google developer accounts needed ($99/yr Apple, $25 one-time Google).
  TestFlight lets the crew use the app for months before any public listing.

## Phasing

**Phase 0 — API foundation (Flask only, no app yet)** — ✅ SHIPPED 2026-07-23
Implemented (see `routes/api_v1.py`, `api_auth.py`, Alembic `0005_api_token`):
- `POST /api/v1/auth/login` — JSON `{email, password, device_name?}` →
  `{token, user}`. Token is `fpb_`-prefixed, shown once, stored as SHA-256
  in `api_token`. Per-worker throttle: 10 failures / 15 min per email+IP.
  `must_change_password` users are told to set a password on the web first.
- All other `/api/v1` routes: `Authorization: Bearer fpb_...`, resolved by a
  Flask-Login `request_loader` in app.py. Cookie sessions are **rejected**
  on `/api/v1` (that's what makes its CSRF exemption safe).
- `GET /api/v1/me` — user + projects with per-project `role` and
  `can_upload_docs` (false ⇒ no Dropbox folder yet).
- `POST /api/v1/projects/<pid>/docs/upload` and
  `GET /api/v1/docs/<uid>/status` — thin aliases over the existing
  validated Veryfi/Dropbox pipeline; central project-access gate applies.
- `POST /api/v1/auth/logout` — revokes the presented token
  (`revoked_at`; rows kept for device/audit history).
Not yet built (fast follow when the app needs it): token list/revoke UI in
web admin.

**Phase 1 — Uploader app (the turnkey product)** — ✅ BUILT 2026-07-24
Expo SDK 57 app in `mobile/` (see mobile/README.md for run/build steps):
login (server override hidden behind "Server settings") → project picker
with per-project role + upload-readiness → camera / photo library
(multi-select, up to 10) → serial upload queue with progress + retry →
OCR results (vendor/amount) shown from the synchronous upload response →
recent-uploads list via the new `GET /api/v1/projects/<pid>/docs/recent`.
Token in the OS keychain (expo-secure-store). Projects without a Dropbox
folder show a friendly "ask your line producer" note instead of buttons.
Verified: `tsc --noEmit` clean, Metro bundle exports, server smoke tests.
Still to do before crew hands: real app icon, EAS build + TestFlight
(needs Apple Developer account), then field-test with a real receipt.

**Phase 2 — Budget view + edit** — ✅ BUILT 2026-07-24
Server: `GET /api/v1/projects/<pid>/budgets` (non-archived list),
`GET .../budgets/<bid>/summary` (sections + grand totals + every line with
server-computed totals — calc_top_sheet gained an additive `line_totals`
return key so the phone NEVER does budget math), and thin wrappers over the
existing `upsert_line`/`delete_line` handlers (activity log, estimated-edit
protection, and schedule guards all apply unchanged). docs_only users get a
JSON 403 on /api/ instead of an HTML redirect.
App: project home menu (Budgets / Upload docs; docs_only logins skip
straight to upload) → budget list with mode badges → budget screen with
grand-total card, collapsible COA sections, auto-lines (WC / payroll fee /
insurance / company fee), tap-a-line bottom-sheet editor (description,
qty/days/rate/OT, note; view-only for viewers; delete with confirm).
Handles the estimated-protection 409 with a native confirm → override
resend; schedule-conflict 409s point the user to the website.
Verified: tsc clean, Metro bundle exports, 8-scenario API smoke test incl.
math parity, role gates, 409 flows.

**Phase 3 — Call sheets + People**
Call sheet: day picker → view assembled sheet, edit key fields via the
existing whole-payload save (must send `_rev` concurrency token; times dicts
are keyed `sec||role||name` — see CLAUDE.md gotchas). People: roster from
people/crew JSON endpoints, tap-to-call/email, basic edits.

**Phase 4 — Travel & Catering (view + light interaction)**
Clean-JSON variants of the travel/catering grid endpoints; app shows per-day
travel details and catering/meal info with the basic toggles. Read-mostly is
acceptable per owner.

**Out of scope for now:** actualizing/transactions in the app, offline
editing (offline *upload queue* in Phase 1 is worth it; offline budget editing
is not), Android/iOS tablet layouts, public app-store listing (TestFlight
first).

## Risks / notes

- Money stays `db.Numeric` server-side; API serializes as strings ("1234.56"),
  never floats.
- Uploads require the project's Dropbox folder to exist — surface clearly.
- Rate-limit and audit `/api/v1/auth/login`; tokens revocable from the web
  admin.
- Keep API wrappers out of app.py's giant body where possible — a
  `routes/api_v1.py` slice per the M1 splitting pattern (same app object, no
  blueprints, imported at bottom).
