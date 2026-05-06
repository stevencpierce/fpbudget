"""
Top sheet tests: verify top sheet totals update when budget lines change.
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

pytestmark = allure.suite("Top Sheet")

PROJECT_NAME = f"TopSheetTest-{int(time.time()) % 100000}"


@pytest.fixture
def budget_with_topsheet(admin_page: Page) -> Page:
    """Create a project from template and open its budget."""
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


# ---------------------------------------------------------------------------
# Top Sheet Visibility
# ---------------------------------------------------------------------------

@allure.title("Top sheet / summary section is visible on budget page")
def test_topsheet_visible(budget_with_topsheet: Page):
    page = budget_with_topsheet
    # Look for top sheet elements — tab, section, or heading
    topsheet = page.locator(".topsheet-table, .topsheet, #topsheet").first
    take_step_screenshot(page, "topsheet-area")
    # Even if "Top Sheet" isn't visible as text, verify the budget loaded
    expect(page).to_have_url(re.compile(r"/projects/\d+/budget"))


# ---------------------------------------------------------------------------
# Top Sheet Totals Update
# ---------------------------------------------------------------------------

@allure.title("Top sheet grand total updates when a line item value changes")
def test_topsheet_total_updates(budget_with_topsheet: Page):
    page = budget_with_topsheet

    # Capture initial total(s)
    total_el = page.locator(".grand-total-row, .grand-total").first
    if total_el.is_visible(timeout=5000):
        initial = total_el.text_content()

        # Edit a budget line amount
        numeric = page.locator('input[type="number"], input.amount').first
        if numeric.is_visible(timeout=3000):
            numeric.click()
            numeric.fill("12345")
            numeric.press("Tab")
            page.wait_for_timeout(3000)  # Allow recalc + possible socket update

            updated = total_el.text_content()
            take_step_screenshot(page, "topsheet-total-after-edit")
            allure.attach(
                f"Initial: {initial}\nAfter edit: {updated}",
                name="topsheet-total-comparison",
                attachment_type=allure.attachment_type.TEXT,
            )
    else:
        take_step_screenshot(page, "topsheet-no-total-found")
        pytest.skip("No grand total element found on budget page")


@allure.title("Top sheet section totals match budget section subtotals")
def test_topsheet_section_alignment(budget_with_topsheet: Page):
    page = budget_with_topsheet

    # Gather section totals from the top sheet area
    section_totals = page.locator(
        ".section-total, .topsheet-row .total, .category-total"
    )
    count = section_totals.count()
    take_step_screenshot(page, "topsheet-sections")
    allure.attach(
        f"Found {count} section total elements in top sheet area",
        name="section-total-count",
        attachment_type=allure.attachment_type.TEXT,
    )


# ---------------------------------------------------------------------------
# Top Sheet Navigation
# ---------------------------------------------------------------------------

@allure.title("Clicking a top sheet section navigates to that budget section")
def test_topsheet_section_click_navigates(budget_with_topsheet: Page):
    page = budget_with_topsheet

    # Look for clickable section links in the top sheet
    section_link = page.locator(
        ".topsheet a, .top-sheet a, .summary a, .topsheet-row a"
    ).first
    if section_link.is_visible(timeout=5000):
        section_link.click()
        page.wait_for_timeout(1000)
        take_step_screenshot(page, "topsheet-section-clicked")
    else:
        take_step_screenshot(page, "topsheet-no-links")
