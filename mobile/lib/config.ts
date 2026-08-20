// Default FPBudget server. Overridable from the login screen's "Server"
// field (stored on-device) so TestFlight builds can point at a staging
// instance without a rebuild.
export const DEFAULT_SERVER = "https://fp-budget.onrender.com";

export const APP_DISPLAY_NAME = "FP Budget";

// Visible on the login screen so support can tell WHICH bundle a phone is
// actually running (stale caches lie). Bump on every user-facing change.
export const APP_BUILD = "build 6 · 2026-08-19";
