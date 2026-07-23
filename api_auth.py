"""Pure helpers for the mobile-app bearer-token auth (Phase 0, 2026-07-23).

No Flask, no DB — importable from app.py, routes/api_v1.py, and unit tests
alike. The raw token is shown to the client exactly once at login; only its
SHA-256 hex lands in the api_token table, so a DB leak never leaks usable
tokens.
"""
import hashlib
import re
import secrets

TOKEN_PREFIX = "fpb_"
# token_urlsafe(32) → 43 url-safe chars; anchor both ends so a truncated or
# padded token never matches.
_TOKEN_RE = re.compile(r"^fpb_[A-Za-z0-9_\-]{40,64}$")


def generate_token():
    """Return a fresh raw bearer token (prefixed so leaked strings are
    recognizable in logs/scanners as FPBudget API tokens)."""
    return TOKEN_PREFIX + secrets.token_urlsafe(32)


def hash_token(raw):
    """SHA-256 hex of the raw token — the only form ever stored."""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_bearer(header_value):
    """Extract the raw token from an Authorization header.

    Returns the token string only for a well-formed `Bearer fpb_...` header;
    None for anything else (missing, Basic auth, malformed token).
    """
    if not header_value:
        return None
    parts = header_value.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    raw = parts[1].strip()
    return raw if _TOKEN_RE.match(raw) else None
