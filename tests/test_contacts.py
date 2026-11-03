"""
People tab tests: the contact sheet inside the budget page (#tab-contacts).
The tab is labelled "People" since Cleanup B; `?tab=contacts` deep-links to it.
"""

import pytest
from playwright.sync_api import Page, expect

from tests.utils.helpers import take_step_screenshot


@pytest.fixture
def contacts_page(project) -> Page:
    page = project.page
    page.goto(f"{project.budget_url}?tab=contacts", wait_until="networkidle")
    expect(page.locator("#tab-contacts.active")).to_be_visible(timeout=10_000)
    return page


def test_people_tab_activates(project):
    page = project.page
    page.goto(project.budget_url, wait_until="networkidle")
    page.click('.tabs > .tab-btn[data-tab="contacts"]')
    expect(page.locator("#tab-contacts.active")).to_be_visible(timeout=10_000)
    expect(page.locator("#tab-contacts h3:has-text('Contact Sheet')").first).to_be_visible()
    take_step_screenshot(page, "people-tab-active")


def test_contacts_add_contact_button(contacts_page: Page):
    page = contacts_page
    expect(page.locator('#tab-contacts button:has-text("+ Add Contact")').first).to_be_visible()


def test_contacts_add_union_client(contacts_page: Page):
    page = contacts_page
    for label in ("+ Add Union", "+ Add Client"):
        btn = page.locator(f'#tab-contacts button:has-text("{label}")').first
        expect(btn).to_be_visible()


def test_contacts_omit_buttons(contacts_page: Page):
    page = contacts_page
    for label in ("Omit Name", "Omit Phone", "Omit Email"):
        assert page.locator(f'.cs-ctx-btn:has-text("{label}")').count() >= 1, f"Missing context button: {label}"
