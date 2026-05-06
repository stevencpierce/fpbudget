"""
Contacts tab tests: contact sheet inside the budget page (#tab-contacts).
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

pytestmark = allure.suite("Contacts")

PROJECT_NAME = f"ContactsTest-{int(time.time()) % 100000}"


@pytest.fixture
def contacts_tab_page(admin_page: Page) -> Page:
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    page.locator('button.tab-btn:has-text("Contacts")').first.click()
    page.wait_for_timeout(1000)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


@allure.title("Contacts tab activates and shows Contact Sheet heading")
def test_contacts_tab_activates(contacts_tab_page: Page):
    page = contacts_tab_page
    expect(page.locator("#tab-contacts.active")).to_be_visible(timeout=10_000)
    expect(page.locator("#tab-contacts h3:has-text('Contact Sheet')").first).to_be_visible()
    take_step_screenshot(page, "contacts-tab-active")


@allure.title("Contacts tab has Add Contact button")
def test_contacts_add_contact_button(contacts_tab_page: Page):
    page = contacts_tab_page
    add_btn = page.locator('#tab-contacts button:has-text("+ Add Contact")').first
    expect(add_btn).to_be_visible(timeout=5000)


@allure.title("Contacts tab has Add Union and Add Client buttons")
def test_contacts_add_union_client(contacts_tab_page: Page):
    page = contacts_tab_page
    for label in ("+ Add Union", "+ Add Client"):
        btn = page.locator(f'#tab-contacts button:has-text("{label}")').first
        expect(btn).to_be_visible(timeout=5000)


@allure.title("Contacts tab has omit-field context buttons")
def test_contacts_omit_buttons(contacts_tab_page: Page):
    page = contacts_tab_page
    for label in ("Omit Name", "Omit Phone", "Omit Email"):
        btn = page.locator(f'.cs-ctx-btn:has-text("{label}")').first
        assert btn.count() >= 1, f"Missing context button: {label}"
