# Headless unit tests (budget / versioning logic)

These run the real Flask app in-process against a throwaway SQLite DB — no
browser, no Render, no Dropbox/OCR/PDF system libs (those modules are stubbed
in `conftest.py`). Use them to lock down budget math and **version
independence** rules with concrete assertions.

## Run

```bash
python3 -m venv .venv
.venv/bin/pip install flask flask-login flask-sqlalchemy flask-mail \
    python-dotenv werkzeug sqlalchemy pytest
.venv/bin/python -m pytest tests/unit/ --confcutdir=tests/unit -o addopts=""
```

`--confcutdir=tests/unit` keeps pytest from loading the Playwright E2E
harness in `tests/conftest.py` (which needs `allure`/`playwright`).

## What's covered

`test_version_independence.py` asserts the product rules:

1. **Duplicate is faithful** — a new version equals its source the instant
   it's created, with no sync.
2. **Viewing never mutates** — a duplicate that copied a (stale) value does
   not drift on its own; only an explicit recompute/edit changes a number.
   (This is the v2 102k→130k regression.)
3. **Editing one version never changes another.**
4. **Deleting one version never changes another.**

Add a new test here for every concrete versioning bug we find, so it can
never silently come back.
