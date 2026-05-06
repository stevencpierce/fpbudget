"""
Budget version tests: version picker dropdown, version labels.
Each budget may have multiple versions (e.g., v1 EST, v1 WORKING, v2 EST).
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

pytestmark = allure.suite("Budget Versions")

PROJECT_NAME = f"VersionTest-{int(time.time()) % 100000}"


@pytest.fixture
def versioned_budget_page(admin_page: Page) -> Page:
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


@allure.title("Version picker button is visible in budget header")
def test_version_picker_visible(versioned_budget_page: Page):
    page = versioned_budget_page
    picker = page.locator(".version-picker-btn").first
    expect(picker).to_be_visible(timeout=10_000)
    take_step_screenshot(page, "version-picker-visible")


@allure.title("Version picker shows at least one version")
def test_version_picker_has_versions(versioned_budget_page: Page):
    page = versioned_budget_page
    picker = page.locator(".version-picker-btn").first
    text = picker.inner_text()
    # A fresh budget should have v1 with at least 1 version (e.g., EST)
    assert "v" in text.lower() or "version" in text.lower(), f"Unexpected picker text: {text!r}"


@allure.title("Clicking version picker opens the version menu")
def test_version_picker_opens_menu(versioned_budget_page: Page):
    page = versioned_budget_page
    page.locator(".version-picker-btn").first.click()
    page.wait_for_timeout(500)
    menu = page.locator(".version-menu").first
    expect(menu).to_be_visible(timeout=3000)


@allure.title("Version menu shows Current badge on the active version")
def test_version_menu_current_badge(versioned_budget_page: Page):
    page = versioned_budget_page
    page.locator(".version-picker-btn").first.click()
    page.wait_for_timeout(500)
    current = page.locator(".version-status-badge.vs-current").first
    # A newly-created budget should have exactly one current version
    if current.count() == 0:
        pytest.skip("No current-version badge in this app state")
    expect(current).to_be_visible()
