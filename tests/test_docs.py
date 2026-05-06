"""
Docs tab tests: file drop zone, upload queue, upload history.
The Docs tab lives inside the budget page at #tab-docs.
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

pytestmark = allure.suite("Docs")

PROJECT_NAME = f"DocsTest-{int(time.time()) % 100000}"


@pytest.fixture
def docs_tab_page(admin_page: Page) -> Page:
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    # Switch to Docs tab
    page.locator('button.tab-btn:has-text("Docs")').first.click()
    page.wait_for_timeout(1000)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


@allure.title("Docs tab activates and drop zone is visible")
def test_docs_tab_activates(docs_tab_page: Page):
    page = docs_tab_page
    expect(page.locator("#tab-docs.active")).to_be_visible(timeout=10_000)
    expect(page.locator("#docsDropZone")).to_be_visible()
    take_step_screenshot(page, "docs-tab-active")


@allure.title("Docs tab shows the hidden file inputs for upload")
def test_docs_file_inputs_present(docs_tab_page: Page):
    page = docs_tab_page
    for input_id in ("docsFileInput", "docsCameraInput", "docsGalleryInput"):
        assert page.locator(f"#{input_id}").count() >= 1, f"Missing {input_id}"


@allure.title("Docs Upload All button is visible")
def test_docs_upload_button_visible(docs_tab_page: Page):
    page = docs_tab_page
    upload_btn = page.locator("#docsBtnUpload")
    expect(upload_btn).to_be_visible(timeout=5000)


@allure.title("Docs history list container is present")
def test_docs_history_list_present(docs_tab_page: Page):
    page = docs_tab_page
    history = page.locator("#docsHistory")
    assert history.count() == 1, "Expected exactly one #docsHistory container"
