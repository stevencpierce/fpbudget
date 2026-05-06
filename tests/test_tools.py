"""
Tools tab tests: Import CSV, Apply Template, Save as Template,
inside the budget page (#tab-import-tools).
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

pytestmark = allure.suite("Tools")

PROJECT_NAME = f"ToolsTest-{int(time.time()) % 100000}"


@pytest.fixture
def tools_tab_page(admin_page: Page) -> Page:
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    page.locator('button.tab-btn:has-text("Tools")').first.click()
    page.wait_for_timeout(1000)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


@allure.title("Tools tab activates and shows Import CSV section")
def test_tools_tab_activates(tools_tab_page: Page):
    page = tools_tab_page
    expect(page.locator("#tab-import-tools.active")).to_be_visible(timeout=10_000)
    expect(page.locator("#tab-import-tools h3:has-text('Import CSV')").first).to_be_visible()
    take_step_screenshot(page, "tools-tab-active")


@allure.title("Tools tab has the Upload & Map CSV button")
def test_tools_upload_csv_button(tools_tab_page: Page):
    page = tools_tab_page
    btn = page.locator("#import-csv-btn")
    expect(btn).to_be_visible(timeout=5000)
    assert "CSV" in btn.inner_text()


@allure.title("Tools tab has the Export Budget section")
def test_tools_export_section(tools_tab_page: Page):
    page = tools_tab_page
    export_heading = page.locator('#tab-import-tools h3:has-text("Export Budget")').first
    expect(export_heading).to_be_visible(timeout=5000)


@allure.title("Tools tab has Apply Template and Save as Template sections")
def test_tools_template_sections(tools_tab_page: Page):
    page = tools_tab_page
    expect(page.locator('#tab-import-tools h3:has-text("Apply Template")').first).to_be_visible()
    expect(page.locator('#tab-import-tools h3:has-text("Save as Template")').first).to_be_visible()


@allure.title("Tools tab Apply Template dropdown has template options")
def test_tools_apply_template_options(tools_tab_page: Page):
    page = tools_tab_page
    # Scope the select to the Apply-Template form
    select = page.locator('#tab-import-tools select.select-sm').first
    expect(select).to_be_visible(timeout=5000)
    options = select.locator("option").all_text_contents()
    assert len(options) >= 1, f"Expected at least 1 template option, got {options}"
