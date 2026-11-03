"""
Call Sheets tests: /projects/{pid}/budget/{bid}/callsheet — the internal
editing view: audience view switcher, key-contact table, distribution
controls, back link. (The audience pills and schedule-notes textarea the old
tests looked for were replaced by the ?view= switcher and the message tabs.)
"""

import re

import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL
from tests.utils.helpers import take_step_screenshot

VIEWS = ("internal", "crew", "client", "talent", "union")


@pytest.fixture
def callsheet_page(project) -> Page:
    page = project.page
    page.goto(f"{project.budget_url}/callsheet", wait_until="networkidle", timeout=30_000)
    expect(page.locator(".callsheet-wrapper").first).to_be_visible(timeout=10_000)
    return page


def test_callsheet_reachable_from_more_menu(project):
    page = project.page
    page.goto(project.budget_url, wait_until="networkidle")
    page.click("#tab-more-btn")
    page.locator('#tab-more-menu a.tab-menu-item:has-text("Call Sheets")').click()
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(re.compile(r"/projects/\d+/budget/\d+/callsheet"))
    expect(page.locator(".callsheet-wrapper").first).to_be_visible(timeout=10_000)
    take_step_screenshot(page, "callsheet-loaded")


def test_callsheet_audience_views(callsheet_page: Page):
    page = callsheet_page
    links = page.locator('.cs-screen-header a[href*="?view="]')
    assert links.count() >= len(VIEWS), f"expected one link per audience view, got {links.count()}"
    hrefs = " ".join(links.nth(i).get_attribute("href") or "" for i in range(links.count()))
    for v in VIEWS:
        assert f"view={v}" in hrefs, f"missing audience view link: {v}"
    # Internal is the default (highlighted) view.
    expect(page.locator('.cs-screen-header a.btn-primary[href*="view=internal"]')).to_be_visible()


def test_callsheet_other_view_renders(callsheet_page: Page):
    page = callsheet_page
    page.locator('.cs-screen-header a[href*="view=crew"]').first.click()
    page.wait_for_load_state("networkidle")
    assert "view=crew" in page.url
    expect(page.locator(".callsheet-wrapper").first).to_be_visible(timeout=10_000)


def test_callsheet_key_contact_table(callsheet_page: Page):
    expect(callsheet_page.locator("#kp-table")).to_be_visible()


def test_callsheet_distribution_controls(callsheet_page: Page):
    page = callsheet_page
    for sel in ("#cs-dist-notes-input", "#cs-dist-toggle-btn"):
        assert page.locator(sel).count() >= 1, f"Missing distribution control: {sel}"


def test_callsheet_back_to_budget(callsheet_page: Page):
    page = callsheet_page
    back = page.locator('a:has-text("Back to Budget")')
    assert back.count() >= 1, "No Back to Budget link"
    href = back.first.get_attribute("href")
    assert href and "/budget/" in href, f"Unexpected Back-to-Budget href: {href}"
    page.goto(f"{BASE_URL}{href}", wait_until="networkidle", timeout=15_000)
    expect(page).to_have_url(re.compile(r"/projects/\d+/budget/\d+"))
