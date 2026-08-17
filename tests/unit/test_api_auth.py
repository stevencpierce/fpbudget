"""Unit tests for api_auth (mobile bearer-token helpers, Phase 0).

Pure — no Flask, no DB, same style as test_budget_calc.py.
Run: pytest tests/unit  (wired into CI).
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from api_auth import generate_token, hash_token, parse_bearer, TOKEN_PREFIX  # noqa: E402


def test_generate_token_shape():
    t = generate_token()
    assert t.startswith(TOKEN_PREFIX)
    assert len(t) > 40
    # url-safe charset only (goes into an Authorization header verbatim)
    body = t[len(TOKEN_PREFIX):]
    assert all(c.isalnum() or c in "_-" for c in body)


def test_generate_token_unique():
    assert len({generate_token() for _ in range(100)}) == 100


def test_hash_token_deterministic_and_hex():
    t = generate_token()
    h1, h2 = hash_token(t), hash_token(t)
    assert h1 == h2
    assert len(h1) == 64
    int(h1, 16)  # raises if not hex
    assert hash_token(generate_token()) != h1


def test_hash_never_equals_raw():
    t = generate_token()
    assert hash_token(t) != t


def test_parse_bearer_valid():
    t = generate_token()
    assert parse_bearer(f"Bearer {t}") == t
    assert parse_bearer(f"bearer {t}") == t          # scheme case-insensitive
    assert parse_bearer(f"Bearer   {t}") == t        # extra whitespace


def test_parse_bearer_rejects_garbage():
    t = generate_token()
    assert parse_bearer(None) is None
    assert parse_bearer("") is None
    assert parse_bearer(t) is None                   # missing scheme
    assert parse_bearer("Basic dXNlcjpwdw==") is None
    assert parse_bearer("Bearer") is None
    assert parse_bearer("Bearer notaprefix_abc123") is None
    assert parse_bearer("Bearer fpb_short") is None
    assert parse_bearer(f"Bearer {t}; evil=1") is None
    assert parse_bearer(f"Bearer {t}\n") == t        # trailing ws stripped
