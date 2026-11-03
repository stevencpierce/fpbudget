"""
Shared Playwright fixtures for FPBudget E2E tests.

Environment variables:
    BASE_URL        – FPBudget instance (default: http://localhost:5000)
    ADMIN_EMAIL     – Admin email   (default: steven@thefp.tv)
    ADMIN_PASSWORD  – Admin password (required)
    TEST_EMAIL      – Non-admin test user email (default: claudes-tester-app@thefp.tv)
    TEST_PASSWORD   – Test user password (required by the tests that use it)
    HEADED          – Set to "1" to run in headed mode
    E2E_ARTIFACTS   – Folder for step / failure screenshots (default: tests/artifacts)

The seeded local app (scripts/seed_local.py) is the intended target:
    python scripts/seed_local.py --port 5055 &
    BASE_URL=http://127.0.0.1:5055 ADMIN_EMAIL=local-admin@thefp.tv \
      ADMIN_PASSWORD=localpass-123 TEST_EMAIL=local-editor@thefp.tv \
      TEST_PASSWORD=localpass-123 python -m pytest tests -q --ignore=tests/unit -o addopts=""
"""

from __future__ import annotations

import os
import re
import pytest
from pathlib import Path
from dotenv import load_dotenv
from playwright.sync_api import Page, Browser, expect

# Load .env from project root or tests dir
load_dotenv(Path(__file__).parent / ".env")
load_dotenv(Path(__file__).parent.parent / ".env")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# SAFETY (audit C4c, 2026-07-20): the suite creates and deletes real projects,
# so it must NEVER default to production. Point BASE_URL at a staging
# deployment; running against the live site now requires an explicit
# ALLOW_PROD_TESTS=1 acknowledgement.
BASE_URL = os.getenv("BASE_URL", "http://localhost:5000")
if "fp-budget.onrender.com" in BASE_URL and os.getenv("ALLOW_PROD_TESTS") != "1":
    raise RuntimeError(
        "Refusing to run the test suite against PRODUCTION "
        f"({BASE_URL}) — it creates/deletes real projects. Set BASE_URL to a "
        "staging instance, or set ALLOW_PROD_TESTS=1 if you really mean it.")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "steven@thefp.tv")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
TEST_EMAIL = os.getenv("TEST_EMAIL", "claudes-tester-app@thefp.tv")
TEST_PASSWORD = os.getenv("TEST_PASSWORD", "")

COLD_START_TIMEOUT = 60_000  # Render free-tier cold start can be slow
DEFAULT_TIMEOUT = 15_000

ARTIFACT_DIR = Path(os.getenv("E2E_ARTIFACTS", Path(__file__).parent / "artifacts"))


# ---------------------------------------------------------------------------
# Pytest configuration
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "admin: tests requiring admin privileges")
    config.addinivalue_line("markers", "collab: multi-browser collaboration tests")
    config.addinivalue_line("markers", "slow: tests that take a long time")


# ---------------------------------------------------------------------------
# Screenshot on failure → tests/artifacts/<test>.png (CI uploads the folder)
# ---------------------------------------------------------------------------

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call" and report.failed:
        page = None
        for key in ("page", "admin_page", "test_user_page", "anon_page"):
            page = item.funcargs.get(key)
            if page is not None:
                break
        ws = item.funcargs.get("ws")
        if page is None and isinstance(ws, tuple):
            page = ws[0]
        if page is not None and not page.is_closed():
            try:
                ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
                safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", item.nodeid)
                page.screenshot(path=str(ARTIFACT_DIR / f"FAILED-{safe}.png"), full_page=True)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Browser-level fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def browser_type_launch_args():
    """Override Playwright defaults for all browsers."""
    args = {}
    if os.getenv("HEADED", "0") == "1":
        args["headless"] = False
        args["slow_mo"] = 100
    return args


@pytest.fixture(scope="session")
def base_url():
    return BASE_URL


# ---------------------------------------------------------------------------
# Auth helper (reusable across fixtures)
# ---------------------------------------------------------------------------

def _login(page: Page, email: str, password: str):
    """Fill login form and wait for redirect away from /login."""
    page.goto(f"{BASE_URL}/login", wait_until="networkidle", timeout=COLD_START_TIMEOUT)
    page.fill('input[type="email"]', email)
    page.fill('input[type="password"]', password)
    page.click('button[type="submit"]')
    # Wait for navigation away from login (could go to / or /profile)
    page.wait_for_function(
        "() => !window.location.pathname.includes('/login')",
        timeout=DEFAULT_TIMEOUT,
    )
    page.wait_for_load_state("networkidle")
    # If redirected to profile for password reset, navigate to projects
    if "/profile" in page.url:
        page.goto(f"{BASE_URL}/", wait_until="networkidle", timeout=DEFAULT_TIMEOUT)


def _new_page(browser: Browser) -> Page:
    context = browser.new_context(
        base_url=BASE_URL,
        viewport={"width": 1280, "height": 800},
    )
    context.set_default_timeout(DEFAULT_TIMEOUT)
    return context.new_page()


# ---------------------------------------------------------------------------
# Page fixtures — one per role
# ---------------------------------------------------------------------------

@pytest.fixture
def admin_page(browser: Browser) -> Page:
    """A fresh browser context logged in as admin."""
    page = _new_page(browser)
    _login(page, ADMIN_EMAIL, ADMIN_PASSWORD)
    yield page
    page.context.close()


@pytest.fixture
def test_user_page(browser: Browser) -> Page:
    """A fresh browser context logged in as the (non-admin) test user."""
    if not TEST_PASSWORD:
        pytest.skip("TEST_EMAIL / TEST_PASSWORD not set — no non-admin test user")
    page = _new_page(browser)
    _login(page, TEST_EMAIL, TEST_PASSWORD)
    yield page
    page.context.close()


@pytest.fixture
def anon_page(browser: Browser) -> Page:
    """An unauthenticated browser context."""
    page = _new_page(browser)
    yield page
    page.context.close()


# ---------------------------------------------------------------------------
# Collab fixture — two independent browser contexts
# ---------------------------------------------------------------------------

@pytest.fixture
def collab_pages(browser: Browser) -> tuple[Page, Page]:
    """Two browser contexts: admin + test user, both logged in."""
    if not TEST_PASSWORD:
        pytest.skip("TEST_EMAIL / TEST_PASSWORD not set — no non-admin test user")
    page_a = _new_page(browser)
    page_b = _new_page(browser)
    _login(page_a, ADMIN_EMAIL, ADMIN_PASSWORD)
    _login(page_b, TEST_EMAIL, TEST_PASSWORD)
    yield page_a, page_b
    page_a.context.close()
    page_b.context.close()


# ---------------------------------------------------------------------------
# One throwaway project per test module (Cleanup F, 2026-11-02)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def project(browser: Browser, request):
    """Create ONE project from the FP Standard template for the whole module,
    on its own admin page, and delete it at the end. Yields a namespace with
    .page (logged-in admin page), .pid, .bid, .name and .budget_url. Tests
    that share it must navigate to the screen they need first."""
    import time
    from types import SimpleNamespace
    from tests.utils.helpers import create_project, delete_project, go_to_projects

    mod = request.module.__name__.rsplit(".", 1)[-1].replace("test_", "")
    name = f"E2E-{mod}-{int(time.time()) % 100000}"
    page = _new_page(browser)
    _login(page, ADMIN_EMAIL, ADMIN_PASSWORD)
    create_project(page, name, template="FP Standard")
    m = re.search(r"/projects/(\d+)/budget", page.url)
    assert m, f"project creation did not land on a budget page: {page.url}"
    pid = int(m.group(1))
    bid = int(page.evaluate("() => BID"))
    ns = SimpleNamespace(page=page, pid=pid, bid=bid, name=name,
                         budget_url=f"{BASE_URL}/projects/{pid}/budget/{bid}")
    yield ns
    try:
        go_to_projects(page)
        delete_project(page, name)
    finally:
        page.context.close()
