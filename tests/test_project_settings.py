"""
Project Settings tab tests: #tab-settings inside the budget page.
Budget info, display, payroll, production details.
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

pytestmark = allure.suite("Project Settings")

PROJECT_NAME = f"ProjSettingsTest-{int(time.time()) % 100000}"


@pytest.fixture
def settings_tab_page(admin_page: Page) -> Page:
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    page.locator('button.tab-btn:has-text("Settings")').first.click()
    page.wait_for_timeout(1000)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


@allure.title("Settings tab activates and shows Project Settings heading")
def test_settings_tab_activates(settings_tab_page: Page):
    page = settings_tab_page
    expect(page.locator("#tab-settings.active")).to_be_visible(timeout=10_000)
    expect(page.locator("h2.settings-heading").first).to_be_visible()
    take_step_screenshot(page, "settings-tab-active")


@allure.title("Settings tab has budget name input")
def test_settings_budget_name_input(settings_tab_page: Page):
    page = settings_tab_page
    expect(page.locator("#set-name")).to_be_visible(timeout=5000)
    assert page.locator("#set-name").input_value() != "", "Budget name should be populated"


@allure.title("Settings tab has fee and workers-comp rate inputs")
def test_settings_fee_inputs(settings_tab_page: Page):
    page = settings_tab_page
    for input_id in ("#set-fee", "#set-workers-comp", "#set-payroll-fee"):
        expect(page.locator(input_id)).to_be_visible(timeout=5000)


@allure.title("Settings tab has timezone and payroll profile selects")
def test_settings_display_selects(settings_tab_page: Page):
    page = settings_tab_page
    expect(page.locator("#set-timezone")).to_be_visible(timeout=5000)
    expect(page.locator("#set-payroll-profile")).to_be_visible(timeout=5000)
    expect(page.locator("#set-payroll-week-start")).to_be_visible(timeout=5000)


@allure.title("Settings tab has production details section")
def test_settings_production_details(settings_tab_page: Page):
    page = settings_tab_page
    for input_id in ("#set-client-name", "#set-prepared-by", "#set-prepared-by-email"):
        expect(page.locator(input_id)).to_be_visible(timeout=5000)


@allure.title("Can edit the budget name in Settings")
def test_settings_edit_budget_name(settings_tab_page: Page):
    page = settings_tab_page
    name_input = page.locator("#set-name")
    original = name_input.input_value()
    new_name = original + " [edited]"
    name_input.fill(new_name)
    page.wait_for_timeout(500)
    assert name_input.input_value() == new_name
