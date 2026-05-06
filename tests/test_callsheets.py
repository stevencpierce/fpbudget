"""
Call Sheets tests: /projects/{pid}/budget/{vid}/callsheet page,
audience pills, schedule notes, and distribution controls.
"""

import re
import time
import allure
import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL
from tests.utils.helpers import (
    create_project, delete_project, go_to_projects, open_project_by_name,
    take_step_screenshot,
)

pytestmark = allure.suite("Call Sheets")

PROJECT_NAME = f"CallsheetTest-{int(time.time()) % 100000}"


@pytest.fixture
def callsheet_page(admin_page: Page) -> Page:
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    # Navigate to call sheet via the tab link
    page.locator('a.tab-btn:has-text("Call Sheets")').first.click()
    page.wait_for_load_state("networkidle", timeout=30_000)
    page.wait_for_timeout(1500)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


@allure.title("Call Sheet page loads at /callsheet")
def test_callsheet_url(callsheet_page: Page):
    page = callsheet_page
    expect(page).to_have_url(re.compile(r"/projects/\d+/budget/\d+/callsheet"))
    expect(page.locator(".callsheet-wrapper, .callsheet-page").first).to_be_visible(timeout=10_000)
    take_step_screenshot(page, "callsheet-loaded")


@allure.title("Call Sheet audience pills are rendered")
def test_callsheet_audience_pills(callsheet_page: Page):
    page = callsheet_page
    pills = page.locator(".cs-aud-pill")
    assert pills.count() >= 3, f"Expected multiple audience pills, got {pills.count()}"
    # One pill should be active by default
    active = page.locator(".cs-aud-pill.active")
    expect(active.first).to_be_visible(timeout=5000)


@allure.title("Call Sheet schedule notes textarea is editable")
def test_callsheet_schedule_notes_editable(callsheet_page: Page):
    page = callsheet_page
    notes = page.locator("#cs-schedule-notes")
    expect(notes).to_be_visible(timeout=5000)
    notes.fill("Test note — 6am call")
    page.wait_for_timeout(500)
    assert notes.input_value() == "Test note — 6am call"


@allure.title("Call Sheet key-contact table is visible")
def test_callsheet_key_contact_table(callsheet_page: Page):
    page = callsheet_page
    table = page.locator("#kp-table")
    expect(table).to_be_visible(timeout=5000)


@allure.title("Call Sheet distribution controls are present")
def test_callsheet_distribution_controls(callsheet_page: Page):
    page = callsheet_page
    for sel in ("#cs-dist-notes-input", "#cs-dist-toggle-btn"):
        assert page.locator(sel).count() >= 1, f"Missing distribution control: {sel}"


@allure.title("Back to Budget link returns to budget page")
def test_callsheet_back_to_budget(callsheet_page: Page):
    page = callsheet_page
    back = page.locator('a:has-text("Back to Budget")')
    if back.count() == 0:
        pytest.skip("No Back to Budget link visible")
    # The link is CSS-hidden (screen-only print toolbar variant), so navigate via href
    href = back.first.get_attribute("href")
    assert href and "/budget/" in href, f"Unexpected Back-to-Budget href: {href}"
    page.goto(f"{BASE_URL}{href}", wait_until="networkidle", timeout=15_000)
    expect(page).to_have_url(re.compile(r"/projects/\d+/budget/\d+"))
