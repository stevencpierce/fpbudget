"""
Import & Templates pane tests (#tab-import-tools) inside the budget page.
Since Cleanup B it lives under the "More" menu; `?tab=import-tools` deep-links
to it. (The Apply Template card was removed 2026-07-20; Export moved to the
header's Export menu — see test_export.py.)
"""

import pytest
from playwright.sync_api import Page, expect

from tests.utils.helpers import take_step_screenshot


@pytest.fixture
def tools_page(project) -> Page:
    page = project.page
    page.goto(f"{project.budget_url}?tab=import-tools", wait_until="networkidle")
    expect(page.locator("#tab-import-tools.active")).to_be_visible(timeout=10_000)
    return page


def test_tools_pane_reachable_from_more_menu(project):
    page = project.page
    page.goto(project.budget_url, wait_until="networkidle")
    page.click("#tab-more-btn")
    item = page.locator('#tab-more-menu .tab-menu-item[data-tab="import-tools"]')
    expect(item).to_be_visible()
    item.click()
    expect(page.locator("#tab-import-tools.active")).to_be_visible(timeout=10_000)
    take_step_screenshot(page, "tools-pane-active")


def test_tools_import_csv_section(tools_page: Page):
    page = tools_page
    expect(page.locator("#tab-import-tools h3:has-text('Import CSV')").first).to_be_visible()
    btn = page.locator("#import-csv-btn")
    expect(btn).to_be_visible()
    assert "CSV" in btn.inner_text()
    assert page.locator("#import-csv-file").count() == 1, "hidden CSV file input missing"


def test_tools_save_as_template_section(tools_page: Page):
    page = tools_page
    expect(page.locator('#tab-import-tools h3:has-text("Save as Template")').first).to_be_visible()
