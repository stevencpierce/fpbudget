"""
Actuals tab tests: the Actuals tab inside a budget page (#tab-actuals).
Shows working vs synced (BudgetSync) vs manual vs total actual columns.
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

pytestmark = allure.suite("Actuals")

PROJECT_NAME = f"ActualsTest-{int(time.time()) % 100000}"


@pytest.fixture
def actuals_tab_page(admin_page: Page) -> Page:
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    page.locator('button.tab-btn:has-text("Actuals")').first.click()
    page.wait_for_timeout(1000)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


@allure.title("Actuals tab activates")
def test_actuals_tab_activates(actuals_tab_page: Page):
    page = actuals_tab_page
    expect(page.locator("#tab-actuals.active")).to_be_visible(timeout=10_000)
    expect(page.locator("#tab-actuals h3:has-text('Actuals')").first).to_be_visible()
    take_step_screenshot(page, "actuals-tab-active")


@allure.title("Actuals table is visible")
def test_actuals_table_visible(actuals_tab_page: Page):
    page = actuals_tab_page
    table = page.locator(".actuals-table").first
    expect(table).to_be_visible(timeout=5000)


@allure.title("Actuals table has the expected columns")
def test_actuals_table_columns(actuals_tab_page: Page):
    page = actuals_tab_page
    headers = page.locator(".actuals-table th").all_text_contents()
    # Normalize whitespace
    headers = [h.strip() for h in headers]
    expected = {"Code", "Account", "Working", "Manual", "Total Actual"}
    missing = expected - set(headers)
    assert not missing, f"Missing actuals columns: {missing}. Got: {headers}"
