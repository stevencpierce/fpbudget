# FPBudget tests

Two suites. Both run in CI on every push (`.github/workflows/ci.yml`).

## Unit suite — `tests/unit/`

Pure-Python rules plus a handful of modules that boot `app.py` on a
throw-away sqlite file and drive it with Flask's test client
(`test_*_app.py`, `*_boot.py`). They need the app's real dependencies.

    pip install -r requirements-ci.txt
    python -m pytest tests/unit -q --confcutdir=tests/unit -o addopts=""

CI fails the job if any unit module skips — a skip there means a
dependency is missing, not that the test is optional.

## Browser suite — `tests/test_*.py`

Playwright against a running app. The target is the seeded local app
(`scripts/seed_local.py`: sqlite, one super-admin, one line producer, two
projects, a Working budget, transactions in every Actuals state, three
receipts). Never point it at production: `tests/conftest.py` refuses
`fp-budget.onrender.com` unless `ALLOW_PROD_TESTS=1`.

    pip install -r requirements-ci.txt -r tests/requirements-test.txt
    python -m playwright install chromium
    python scripts/seed_local.py --port 5055 &
    BASE_URL=http://127.0.0.1:5055 ADMIN_EMAIL=local-admin@thefp.tv ADMIN_PASSWORD=localpass-123 \
      TEST_EMAIL=local-editor@thefp.tv TEST_PASSWORD=localpass-123 \
      python -m pytest tests -q --ignore=tests/unit -o addopts=""

`HEADED=1` opens a visible browser. Failure screenshots land in
`tests/artifacts/` (CI uploads that folder). Tests that need Dropbox,
QuickBooks or Veryfi skip unless the matching env var is set.

`tests/dashboard.py` is a small local runner UI for the browser suite
(`python tests/dashboard.py`, then http://localhost:5005); it reads
`tests/.env` for the target and credentials.
