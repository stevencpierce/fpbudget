"""
Project management tests: create, search, filter, sort, share, delete.
"""

import re
import time

import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL
from tests.utils.helpers import (
    create_project, delete_project, find_project_card, go_to_projects, take_step_screenshot,
)

# Unique project name for this test run
TEST_PROJECT_PREFIX = f"AutoTest-{int(time.time()) % 100000}"


def _visible_cards(page: Page):
    return page.locator("#proj-grid .project-card:visible")


# ---------------------------------------------------------------------------
# Create / delete
# ---------------------------------------------------------------------------

def test_create_project_blank_then_delete(admin_page: Page):
    name = f"{TEST_PROJECT_PREFIX}-Blank"
    create_project(admin_page, name, template="Blank budget")
    expect(admin_page).to_have_url(re.compile(r"/projects/\d+/budget"))
    expect(admin_page.locator("#budget-name-heading")).to_contain_text(name)
    take_step_screenshot(admin_page, "project-created-blank")
    delete_project(admin_page, name)
    go_to_projects(admin_page)
    expect(find_project_card(admin_page, name)).to_have_count(0)


def test_create_project_from_template(admin_page: Page):
    name = f"{TEST_PROJECT_PREFIX}-Template"
    create_project(admin_page, name, template="FP Standard")
    expect(admin_page).to_have_url(re.compile(r"/projects/\d+/budget"))
    expect(admin_page.locator("#budget-name-heading")).to_contain_text(name)
    # The template brought sections with it.
    assert admin_page.locator("#tab-working .section-block[data-section]").count() >= 5
    delete_project(admin_page, name)


def test_create_project_empty_name(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    page.click("text=+ New Project")
    page.click('button:has-text("Create")')
    # HTML5 required validation keeps the modal open
    expect(page.locator('input[placeholder="e.g. GINTS Season 2"]')).to_be_visible()
    expect(page).to_have_url(re.compile(r"/$"))


def test_create_project_cancel(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    page.click("text=+ New Project")
    expect(page.locator('input[placeholder="e.g. GINTS Season 2"]')).to_be_visible()
    page.click('button:has-text("Cancel")')
    expect(page.locator('input[placeholder="e.g. GINTS Season 2"]')).to_be_hidden(timeout=5000)


# ---------------------------------------------------------------------------
# Search / filter / sort (client-side on the card grid)
# ---------------------------------------------------------------------------

def test_search_projects(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    cards = _visible_cards(page)
    assert cards.count() >= 1, "the seeded app should list at least one active project"
    first_name = cards.first.locator(".project-card-name").inner_text().strip()
    search = page.locator("#proj-search")
    search.fill(first_name[: max(3, len(first_name) // 2)])
    expect(_visible_cards(page).first).to_be_visible()
    assert all(first_name.lower()[:3] in n.lower() for n in _visible_cards(page).locator(".project-card-name").all_inner_texts())
    search.fill("nonexistent-project-xyz-999")
    expect(_visible_cards(page)).to_have_count(0)
    take_step_screenshot(page, "search-no-results")


def test_filter_active_default(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    assert page.locator("#proj-status-filter").input_value() == "active"
    statuses = {c.get_attribute("data-status") for c in _visible_cards(page).all()}
    assert statuses <= {"active"}, statuses


@pytest.mark.parametrize("status", ["Active", "Wrapped", "Archived", "All"])
def test_filter_status_options(admin_page: Page, status: str):
    page = admin_page
    go_to_projects(page)
    page.locator("#proj-status-filter").select_option(label=status)
    value = page.locator("#proj-status-filter").input_value()
    statuses = {c.get_attribute("data-status") for c in _visible_cards(page).all()}
    if value != "all":
        assert statuses <= {value}, statuses


def test_sort_options(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    options = page.locator("#proj-sort option").all_text_contents()
    for o in ("Newest", "Oldest", "Alphabetical"):
        assert o in options, options


def test_sort_alphabetical_orders_cards(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    page.locator("#proj-sort").select_option(label="Alphabetical")
    names = [n.strip().lower() for n in _visible_cards(page).locator(".project-card-name").all_inner_texts()]
    assert names == sorted(names), names


# ---------------------------------------------------------------------------
# Share / Open Budget
# ---------------------------------------------------------------------------

def test_share_modal(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    page.locator("#proj-grid .project-card:visible").first.locator("text=Share").first.click()
    expect(page.locator("#share-email-input")).to_be_visible(timeout=5000)
    page.click('#share-modal button:has-text("Close")')
    expect(page.locator("#share-modal")).to_be_hidden()


def test_open_budget_link(admin_page: Page):
    page = admin_page
    go_to_projects(page)
    page.locator("#proj-grid .project-card:visible").first.locator("text=Open Budget").first.click()
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(re.compile(r"/projects/\d+/budget"))
    take_step_screenshot(page, "budget-page")
