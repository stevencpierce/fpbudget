"""
Project-Locations tab tests: the Locations tab inside a budget page (#tab-locations).
Separate from /locations (global locations database — that is tested in
test_locations.py once that module is added).
"""

import time
import allure
import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL
from tests.utils.helpers import (
    create_project, delete_project, go_to_projects, open_project_by_name,
    take_step_screenshot,
)

pytestmark = allure.suite("Project Locations")

PROJECT_NAME = f"ProjLocTest-{int(time.time()) % 100000}"


@pytest.fixture
def locations_tab_page(admin_page: Page) -> Page:
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    page.locator('button.tab-btn:has-text("Locations")').first.click()
    page.wait_for_timeout(1000)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


@allure.title("Project Locations tab activates")
def test_project_locations_tab_activates(locations_tab_page: Page):
    page = locations_tab_page
    expect(page.locator("#tab-locations.active")).to_be_visible(timeout=10_000)
    expect(page.locator("#tab-locations h3:has-text('Locations')").first).to_be_visible()
    take_step_screenshot(page, "project-locations-tab-active")


@allure.title("Project Locations tab has Add Location button")
def test_project_locations_add_button(locations_tab_page: Page):
    page = locations_tab_page
    add_btn = page.locator('#tab-locations button:has-text("+ Add Location")').first
    expect(add_btn).to_be_visible(timeout=5000)


@allure.title("Clicking Add Location opens a modal or form")
def test_project_locations_add_opens_form(locations_tab_page: Page):
    page = locations_tab_page
    add_btn = page.locator('#tab-locations button:has-text("+ Add Location")').first
    add_btn.click()
    page.wait_for_timeout(1000)
    # Some form of input should now be visible (modal, inline form, etc.)
    visible_inputs = page.locator('input:visible, textarea:visible').count()
    assert visible_inputs > 0, "Expected Add Location to open a form with inputs"
    take_step_screenshot(page, "location-add-opened")
