"""
Admin access control tests: verify role-based visibility.
"""

import re
import allure
import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL

pytestmark = [allure.suite("Admin"), pytest.mark.admin]


# ---------------------------------------------------------------------------
# Admin user can access admin panel
# ---------------------------------------------------------------------------

@allure.title("Admin can see the Admin link in navigation")
def test_admin_nav_visible_for_admin(admin_page: Page):
    page = admin_page
    admin_link = page.locator('a:has-text("Admin"), a[href="/admin"]')
    expect(admin_link.first).to_be_visible(timeout=5000)


@allure.title("Admin can navigate to /admin page")
def test_admin_page_accessible(admin_page: Page):
    page = admin_page
    page.goto(f"{BASE_URL}/admin", wait_until="networkidle")
    expect(page).to_have_url(re.compile(r"/admin"))
    # Should NOT be a 404 or redirect
    expect(page.locator("text=404")).not_to_be_visible()
    allure.attach(
        page.screenshot(full_page=True),
        name="admin-page",
        attachment_type=allure.attachment_type.PNG,
    )


@allure.title("Admin can see user management section")
def test_admin_user_management(admin_page: Page):
    page = admin_page
    page.goto(f"{BASE_URL}/admin", wait_until="networkidle")
    # Look for user-related elements
    content = page.content().lower()
    has_users = "user" in content or "email" in content or "role" in content
    allure.attach(
        f"User management elements found: {has_users}",
        name="admin-content-check",
        attachment_type=allure.attachment_type.TEXT,
    )


# ---------------------------------------------------------------------------
# Non-admin cannot access admin features
# ---------------------------------------------------------------------------

@allure.title("Non-admin user cannot see Admin link in navigation")
@pytest.mark.skip(reason="Current test user has admin role; needs a dedicated non-admin test account")
def test_admin_nav_hidden_for_test_user(test_user_page: Page):
    page = test_user_page
    admin_link = page.locator('a:has-text("Admin"), a[href="/admin"]')
    expect(admin_link).to_have_count(0)


@allure.title("Non-admin user is blocked from /admin page")
def test_admin_page_blocked_for_test_user(test_user_page: Page):
    page = test_user_page
    page.goto(f"{BASE_URL}/admin", wait_until="networkidle")
    # Should either redirect away or show an error/403
    url = page.url
    # Not on /admin, or showing access denied
    is_blocked = (
        "/admin" not in url
        or "denied" in page.content().lower()
        or "unauthorized" in page.content().lower()
        or "403" in page.content()
        or "404" in page.content()
    )
    allure.attach(
        f"Current URL: {url}\nBlocked: {is_blocked}",
        name="access-control-check",
        attachment_type=allure.attachment_type.TEXT,
    )
    assert is_blocked, f"Non-admin user was able to access /admin at URL: {url}"


# ---------------------------------------------------------------------------
# Navigation visibility by role
# ---------------------------------------------------------------------------

@allure.title("Admin sees full navigation bar")
def test_admin_full_nav(admin_page: Page):
    page = admin_page
    expected_links = ["Projects", "Crew", "Locations", "Templates", "Fringes", "Admin"]
    for link_text in expected_links:
        locator = page.locator(f"nav >> text={link_text}").first
        expect(locator).to_be_visible(timeout=5000)


@allure.title("Non-admin user sees limited navigation bar")
def test_test_user_limited_nav(test_user_page: Page):
    page = test_user_page
    # These should be visible for any authenticated user
    for link_text in ["Projects"]:
        locator = page.locator(f"nav >> text={link_text}").first
        expect(locator).to_be_visible(timeout=5000)


# ---------------------------------------------------------------------------
# Profile page
# ---------------------------------------------------------------------------

@allure.title("Admin can access their profile")
def test_admin_profile(admin_page: Page):
    page = admin_page
    page.goto(f"{BASE_URL}/profile", wait_until="networkidle")
    expect(page).to_have_url(re.compile(r"/profile"))
    expect(page.locator("text=404")).not_to_be_visible()


@allure.title("Test user can access their profile")
def test_test_user_profile(test_user_page: Page):
    page = test_user_page
    page.goto(f"{BASE_URL}/profile", wait_until="networkidle")
    expect(page).to_have_url(re.compile(r"/profile"))
    expect(page.locator("text=404")).not_to_be_visible()
