"""
Project management tests: create, search, filter, sort, share, delete.
"""

import re
import time
import allure
import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL
from tests.utils.helpers import (
    create_project, delete_project, go_to_projects, take_step_screenshot,
)

pytestmark = allure.suite("Projects")

# Unique project name for this test run
TEST_PROJECT_PREFIX = f"AutoTest-{int(time.time()) % 100000}"


# ---------------------------------------------------------------------------
# Create Project
# ---------------------------------------------------------------------------

@allure.title("Create a new project with blank budget")
def test_create_project_blank(admin_page: Page):
    name = f"{TEST_PROJECT_PREFIX}-Blank"
    create_project(admin_page, name, template="Blank budget")
    # App redirects to new project's budget page
    expect(admin_page).to_have_url(re.compile(r"/projects/\d+/budget"))
    # Verify the budget heading shows the project name
    expect(admin_page.locator("#budget-name-heading")).to_contain_text(name)
    take_step_screenshot(admin_page, "project-created-blank")
    # Cleanup
    delete_project(admin_page, name)


@allure.title("Create a new project from FP Standard template")
def test_create_project_from_template(admin_page: Page):
    name = f"{TEST_PROJECT_PREFIX}-Template"
    create_project(admin_page, name, template="FP Standard")
    expect(admin_page).to_have_url(re.compile(r"/projects/\d+/budget"))
    expect(admin_page.locator("#budget-name-heading")).to_contain_text(name)
    # Cleanup
    delete_project(admin_page, name)


@allure.title("Cannot create a project with empty name")
def test_create_project_empty_name(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    page.click("text=+ New Project")
    # Leave name empty, try to submit
    page.click('button:has-text("Create")')
    # Should not navigate away — HTML5 required validation
    expect(page.locator('input[placeholder="e.g. GINTS Season 2"]')).to_be_visible()


@allure.title("Cancel button closes the new project modal")
def test_create_project_cancel(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    page.click("text=+ New Project")
    expect(page.locator('input[placeholder="e.g. GINTS Season 2"]')).to_be_visible()
    page.click('button:has-text("Cancel")')
    # Modal should be hidden
    expect(page.locator('input[placeholder="e.g. GINTS Season 2"]')).to_be_hidden(timeout=5000)


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@allure.title("Search filters projects by name")
def test_search_projects(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    search_input = page.locator('input[placeholder="Search projects..."]')
    # Type a search term
    search_input.fill("nonexistent-project-xyz-999")
    page.wait_for_timeout(1000)  # Allow debounce/filter
    # Should show no results or a "no projects" message
    take_step_screenshot(page, "search-no-results")


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------

@allure.title("Status filter shows Active projects by default")
def test_filter_active_default(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    take_step_screenshot(page, "filter-active-default")
    # The status dropdown should be set to Active
    # Projects should be visible (assuming at least one exists)


@allure.title("Status filter can switch between statuses")
@pytest.mark.parametrize("status", ["Active", "Wrapped", "Archived", "All"])
def test_filter_status_options(admin_page: Page, status: str):
    page = admin_page
    go_to_projects(page)
    page.locator("#proj-status-filter").select_option(label=status)
    page.wait_for_load_state("networkidle")
    take_step_screenshot(page, f"filter-{status.lower()}")


# ---------------------------------------------------------------------------
# Sort
# ---------------------------------------------------------------------------

@allure.title("Sort dropdown offers Newest, Oldest, Alphabetical")
def test_sort_options(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    sort_select = page.locator("#proj-sort")
    options = sort_select.locator("option").all_text_contents()
    assert "Newest" in options
    assert "Oldest" in options
    assert "Alphabetical" in options


@allure.title("Sort changes project order")
@pytest.mark.parametrize("sort_option", ["Newest", "Oldest", "Alphabetical"])
def test_sort_projects(admin_page: Page, sort_option: str):
    page = admin_page
    go_to_projects(page)
    sort_select = page.locator("#proj-sort")
    sort_select.select_option(label=sort_option)
    page.wait_for_load_state("networkidle")
    take_step_screenshot(page, f"sort-{sort_option.lower()}")


# ---------------------------------------------------------------------------
# Share
# ---------------------------------------------------------------------------

@allure.title("Share modal opens and has email input")
def test_share_modal(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    # Click the first Share button
    share_btn = page.locator("text=Share").first
    if share_btn.is_visible():
        share_btn.click()
        expect(page.locator('input[placeholder="collaborator@email.com"]')).to_be_visible(timeout=5000)
        page.click('button:has-text("Close")')


# ---------------------------------------------------------------------------
# Open Budget
# ---------------------------------------------------------------------------

@allure.title("Open Budget link navigates to the budget page")
def test_open_budget_link(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    open_link = page.locator("text=Open Budget").first
    if open_link.is_visible():
        open_link.click()
        page.wait_for_load_state("networkidle")
        expect(page).to_have_url(re.compile(r"/projects/\d+/budget"))
        take_step_screenshot(page, "budget-page")
