"""
Shared Playwright fixtures for FPBudget E2E tests.

Environment variables:
    BASE_URL        – FPBudget instance (default: https://fp-budget.onrender.com)
    ADMIN_EMAIL     – Admin email   (default: steven@thefp.tv)
    ADMIN_PASSWORD  – Admin password (required)
    TEST_EMAIL      – Test user email (default: claudes-test@thefp.tv)
    TEST_PASSWORD   – Test user password (required)
    HEADED          – Set to "1" to run in headed mode
"""

from __future__ import annotations

import os
import pytest
import allure
from pathlib import Path
from dotenv import load_dotenv
from playwright.sync_api import Page, Browser, BrowserContext, expect

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


# ---------------------------------------------------------------------------
# Pytest configuration
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "admin: tests requiring admin privileges")
    config.addinivalue_line("markers", "collab: multi-browser collaboration tests")
    config.addinivalue_line("markers", "slow: tests that take a long time")


# ---------------------------------------------------------------------------
# Allure: attach screenshot on failure
# ---------------------------------------------------------------------------

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call" and report.failed:
        page: Page | None = item.funcargs.get("page") or item.funcargs.get("admin_page")
        if page and not page.is_closed():
            screenshot = page.screenshot(full_page=True)
            allure.attach(screenshot, name="failure-screenshot", attachment_type=allure.attachment_type.PNG)


# ---------------------------------------------------------------------------
# Browser-level fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def browser_type_launch_args():
    """Override Playwright defaults for all browsers."""
    args = {
        "slow_mo": 100,
    }
    if os.getenv("HEADED", "0") == "1":
        args["headless"] = False
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


# ---------------------------------------------------------------------------
# Page fixtures — one per role
# ---------------------------------------------------------------------------

@pytest.fixture
def admin_page(browser: Browser) -> Page:
    """A fresh browser context logged in as admin."""
    context = browser.new_context(
        base_url=BASE_URL,
        viewport={"width": 1280, "height": 800},
    )
    context.set_default_timeout(DEFAULT_TIMEOUT)
    page = context.new_page()
    _login(page, ADMIN_EMAIL, ADMIN_PASSWORD)
    yield page
    context.close()


@pytest.fixture
def test_user_page(browser: Browser) -> Page:
    """A fresh browser context logged in as the test user."""
    context = browser.new_context(
        base_url=BASE_URL,
        viewport={"width": 1280, "height": 800},
    )
    context.set_default_timeout(DEFAULT_TIMEOUT)
    page = context.new_page()
    _login(page, TEST_EMAIL, TEST_PASSWORD)
    yield page
    context.close()


@pytest.fixture
def anon_page(browser: Browser) -> Page:
    """An unauthenticated browser context."""
    context = browser.new_context(
        base_url=BASE_URL,
        viewport={"width": 1280, "height": 800},
    )
    context.set_default_timeout(DEFAULT_TIMEOUT)
    page = context.new_page()
    yield page
    context.close()


# ---------------------------------------------------------------------------
# Collab fixture — two independent browser contexts
# ---------------------------------------------------------------------------

@pytest.fixture
def collab_pages(browser: Browser) -> tuple[Page, Page]:
    """Two browser contexts: admin + test user, both logged in."""
    ctx_a = browser.new_context(base_url=BASE_URL, viewport={"width": 1280, "height": 800})
    ctx_b = browser.new_context(base_url=BASE_URL, viewport={"width": 1280, "height": 800})
    ctx_a.set_default_timeout(DEFAULT_TIMEOUT)
    ctx_b.set_default_timeout(DEFAULT_TIMEOUT)

    page_a = ctx_a.new_page()
    page_b = ctx_b.new_page()

    _login(page_a, ADMIN_EMAIL, ADMIN_PASSWORD)
    _login(page_b, TEST_EMAIL, TEST_PASSWORD)

    yield page_a, page_b

    ctx_a.close()
    ctx_b.close()
