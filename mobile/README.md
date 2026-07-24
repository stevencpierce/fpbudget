# FP Budget — mobile app (Phase 1: receipt/doc uploader)

React Native + Expo (SDK 57) client for the FPBudget Flask backend. Phase 1
scope (see ../MOBILE_APP_PLAN.md): log in → pick a project → photograph or
select receipts → upload through the existing Veryfi/Dropbox pipeline → see
OCR results (vendor / amount) and recent uploads. `docs_only` users get this
as their whole app; higher roles will gain Budgets/Call Sheets tabs in later
phases.

## How it talks to the server

Everything goes through the `/api/v1` endpoints added in Phase 0
(`routes/api_v1.py`): bearer-token auth (token stored in the OS keychain),
`/me` for the project list, and the upload/recent/status doc endpoints.
Server URL defaults to production (`lib/config.ts`) and can be changed from
"Server settings" on the login screen.

## Running it in development

Requires Node 20+. From this `mobile/` directory:

```bash
npm install
npx expo start
```

Then scan the QR code with the **Expo Go** app (App Store / Play Store) on a
phone on the same Wi-Fi. Log in with a real FPBudget account. To test
against a local Flask server, set the server URL on the login screen (use
your machine's LAN IP, and run Flask with SESSION_COOKIE_SECURE=0 —
irrelevant to the app but needed if you also test the web UI over HTTP).

## Real builds (TestFlight / Play Store)

Uses EAS (Expo Application Services — free tier is fine to start):

```bash
npm install -g eas-cli
eas login                # Expo account
eas build:configure      # one-time: creates eas.json
eas build --platform ios # needs the Apple Developer account ($99/yr)
eas submit --platform ios
```

Day-to-day JS changes after the first store build ship over-the-air with
`eas update` — no app-store review. Only native changes (new permissions,
new native modules, SDK upgrades) need a fresh `eas build` + store review.

## Layout

- `App.tsx` — auth gate + screen switching (login / projects / upload)
- `lib/api.ts` — API client; token in expo-secure-store; XHR upload with
  progress (the server runs OCR inside the upload request, so the response
  already carries vendor/amount)
- `lib/types.ts` — /api/v1 response shapes
- `screens/` — Login, Projects, Upload (queue + recent uploads)

The queue uploads one file at a time on purpose — OCR runs server-side
inside each request and the Render worker has 512 MB; serial uploads keep
memory flat and match how the web PWA behaves.
