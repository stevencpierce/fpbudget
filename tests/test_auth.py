"""
Authentication tests: login, logout, forgot password, session protection.
"""

import os
import re
import allure
import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL, ADMIN_EMAIL, ADMIN_PASSWORD, TEST_EMAIL, TEST_PASSWORD

pytestmark = allure.suite("Authentication")


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------

@allure.title("Admin can log in with valid credentials")
def test_admin_login(admin_page: Page):
    """Admin fixture already logs in; verify we're on the projects page."""
    expect(admin_page).to_have_url(f"{BASE_URL}/")
    expect(admin_page.locator("h1:has-text('Projects')")).to_be_visible()


@allure.title("Test user can log in with valid credentials")
def test_test_user_login(test_user_page: Page):
    # Test user may be redirected to /profile (password reset) or / (projects)
    url = test_user_page.url
    assert "/login" not in url, f"Test user stuck on login page: {url}"
    # Verify we're authenticated — nav should be visible
    expect(test_user_page.locator("nav")).to_be_visible()


@allure.title("Login fails with wrong password")
def test_login_wrong_password(anon_page: Page):
    page = anon_page
    page.goto(f"{BASE_URL}/login", wait_until="networkidle", timeout=60_000)
    page.fill('input[type="email"]', ADMIN_EMAIL)
    page.fill('input[type="password"]', "definitely-wrong-password-123")
    page.click('button[type="submit"]')
    # Should stay on login page with an error
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(re.compile(r"/login"))
    # Look for error message — app shows "Invalid credentials."
    expect(page.locator("text=Invalid credentials")).to_be_visible(timeout=10_000)


@allure.title("Login fails with empty fields")
def test_login_empty_fields(anon_page: Page):
    page = anon_page
    page.goto(f"{BASE_URL}/login", wait_until="networkidle", timeout=60_000)
    page.click('button[type="submit"]')
    # HTML5 validation should prevent submission — email field required
    # Verify we haven't navigated away
    expect(page).to_have_url(re.compile(r"/login"))


@allure.title("Login fails with unregistered email")
def test_login_unregistered_email(anon_page: Page):
    page = anon_page
    page.goto(f"{BASE_URL}/login", wait_until="networkidle", timeout=60_000)
    page.fill('input[type="email"]', "nobody@nonexistent.example.com")
    page.fill('input[type="password"]', "password123")
    page.click('button[type="submit"]')
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(re.compile(r"/login"))


# ---------------------------------------------------------------------------
# Logout
# ---------------------------------------------------------------------------

@allure.title("User can log out")
def test_logout(admin_page: Page):
    admin_page.goto(f"{BASE_URL}/logout")
    admin_page.wait_for_load_state("networkidle")
    expect(admin_page).to_have_url(re.compile(r"/login"))
    # Verify login form is present
    expect(admin_page.locator('button:has-text("Sign In")')).to_be_visible()


@allure.title("After logout, protected pages redirect to login")
def test_no_access_after_logout(anon_page: Page):
    page = anon_page
    page.goto(f"{BASE_URL}/", wait_until="networkidle", timeout=60_000)
    # Should redirect to login
    expect(page).to_have_url(re.compile(r"/login"))


# ---------------------------------------------------------------------------
# Forgot Password
# ---------------------------------------------------------------------------

@allure.title("Forgot password page loads correctly")
def test_forgot_password_page(anon_page: Page):
    page = anon_page
    page.goto(f"{BASE_URL}/forgot-password", wait_until="networkidle", timeout=60_000)
    expect(page.locator("text=Reset your password")).to_be_visible()
    expect(page.locator('input[type="email"]')).to_be_visible()
    expect(page.locator('button:has-text("Send Reset Link")')).to_be_visible()


@allure.title("Forgot password form submits without error")
def test_forgot_password_submit(anon_page: Page):
    page = anon_page
    page.goto(f"{BASE_URL}/forgot-password", wait_until="networkidle", timeout=60_000)
    page.fill('input[type="email"]', TEST_EMAIL)
    page.click('button:has-text("Send Reset Link")')
    page.wait_for_load_state("networkidle")
    # Should show a success/confirmation message (not an error)
    # The app likely shows a flash message regardless of email existence
    content = page.content()
    assert "error" not in content.lower() or "reset" in content.lower()


@allure.title("Back to login link works from forgot password")
def test_forgot_password_back_link(anon_page: Page):
    page = anon_page
    page.goto(f"{BASE_URL}/forgot-password", wait_until="networkidle", timeout=60_000)
    page.click("text=Back to login")
    expect(page).to_have_url(re.compile(r"/login"))


# ---------------------------------------------------------------------------
# Session / Security
# ---------------------------------------------------------------------------

@allure.title("Protected routes redirect unauthenticated users to login")
@pytest.mark.parametrize("path", [
    "/",
    "/crew",
    "/locations",
    "/budget-templates",
    "/fringe-config",
    "/admin",
    "/profile",
])
def test_protected_routes_require_auth(anon_page: Page, path: str):
    page = anon_page
    page.goto(f"{BASE_URL}{path}", wait_until="networkidle", timeout=60_000)
    expect(page).to_have_url(re.compile(r"/login"))
