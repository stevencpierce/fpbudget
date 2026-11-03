"""
Project-Locations pane tests: the Locations pane inside a budget page
(#tab-locations, under the "More" menu since Cleanup B; `?tab=locations`
deep-links to it). Separate from /locations, the global locations library.
"""

import pytest
from playwright.sync_api import Page, expect

from tests.utils.helpers import take_step_screenshot


@pytest.fixture
def locations_page(project) -> Page:
    page = project.page
    page.goto(f"{project.budget_url}?tab=locations", wait_until="networkidle")
    expect(page.locator("#tab-locations.active")).to_be_visible(timeout=10_000)
    return page


def test_project_locations_pane_activates(project):
    page = project.page
    page.goto(project.budget_url, wait_until="networkidle")
    page.click("#tab-more-btn")
    page.click('#tab-more-menu .tab-menu-item[data-tab="locations"]')
    expect(page.locator("#tab-locations.active")).to_be_visible(timeout=10_000)
    expect(page.locator("#tab-locations h3:has-text('Locations')").first).to_be_visible()
    take_step_screenshot(page, "project-locations-pane-active")


def test_project_locations_add_button(locations_page: Page):
    page = locations_page
    expect(page.locator('#tab-locations button:has-text("+ Add Location")').first).to_be_visible()


def test_project_locations_add_opens_form(locations_page: Page):
    page = locations_page
    page.locator('#tab-locations button:has-text("+ Add Location")').first.click()
    expect(page.locator("#loc-modal-title")).to_be_visible()
    expect(page.locator("#loc-modal-title")).to_have_text("Add Location")
    assert page.locator("input:visible, textarea:visible").count() > 0, "Add Location opened no form"
    take_step_screenshot(page, "location-add-opened")
