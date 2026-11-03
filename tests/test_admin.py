"""
Admin access control tests: role-based nav and the Admin hub (Cleanup E:
Users · Project Access · Company · Libraries · Maintenance, plus
/admin/maintenance).
"""

import re

import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL

pytestmark = pytest.mark.admin


# ---------------------------------------------------------------------------
# Admin user can access the Admin hub
# ---------------------------------------------------------------------------

def test_admin_nav_visible_for_admin(admin_page: Page):
    expect(admin_page.locator('nav a[href="/admin"]').first).to_be_visible()


def test_admin_hub_sections(admin_page: Page):
    page = admin_page
    page.goto(f"{BASE_URL}/admin", wait_until="networkidle")
    expect(page).to_have_url(re.compile(r"/admin$"))
    expect(page.locator("h1:has-text('Admin')")).to_be_visible()
    for heading in ("Users", "Project Access", "Company", "Libraries", "Maintenance"):
        expect(page.locator(f"h2:has-text('{heading}')").first).to_be_visible()
    # The seeded users are listed with their roles.
    expect(page.locator("#users .users-table")).to_be_visible()
    assert page.locator("#users .users-table tbody tr").count() >= 1


def test_admin_maintenance_page(admin_page: Page):
    page = admin_page
    page.goto(f"{BASE_URL}/admin/maintenance", wait_until="networkidle")
    expect(page).to_have_url(re.compile(r"/admin/maintenance$"))
    assert page.locator(".adm-section-h h2").count() >= 1, "no tool groups on the Maintenance page"


# ---------------------------------------------------------------------------
# Non-admin cannot access admin features
# ---------------------------------------------------------------------------

def test_admin_nav_hidden_for_test_user(test_user_page: Page):
    expect(test_user_page.locator('nav a[href="/admin"]')).to_have_count(0)


def test_admin_page_blocked_for_test_user(test_user_page: Page):
    page = test_user_page
    page.goto(f"{BASE_URL}/admin", wait_until="networkidle")
    # admin_required flashes "Not authorized" and sends the user to Projects.
    assert not re.search(r"/admin$", page.url), f"non-admin reached {page.url}"
    expect(page.locator("text=Not authorized").first).to_be_visible()


# ---------------------------------------------------------------------------
# Navigation visibility by role (top nav per Cleanup E)
# ---------------------------------------------------------------------------

def test_admin_full_nav(admin_page: Page):
    page = admin_page
    for link_text in ("Projects", "Payees", "Libraries", "Upload", "Admin", "My Profile", "Logout"):
        expect(page.locator(f"nav >> text={link_text}").first).to_be_visible()


def test_test_user_limited_nav(test_user_page: Page):
    page = test_user_page
    for link_text in ("Projects", "Upload", "My Profile", "Logout"):
        expect(page.locator(f"nav >> text={link_text}").first).to_be_visible()
    for link_text in ("Payees", "Admin"):
        expect(page.locator(f"nav >> text={link_text}")).to_have_count(0)


# ---------------------------------------------------------------------------
# Profile page
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("who", ["admin_page", "test_user_page"])
def test_profile_accessible(who: str, request):
    page: Page = request.getfixturevalue(who)
    page.goto(f"{BASE_URL}/profile", wait_until="networkidle")
    expect(page).to_have_url(re.compile(r"/profile"))
    expect(page.locator("text=404")).not_to_be_visible()
