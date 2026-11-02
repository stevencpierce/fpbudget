"""
Docs tests: the budget page's Docs tab opens the Docs inbox
(/projects/<pid>/docs) — drop zone, upload control, search.
(Cleanup B, 2026-11-02: the in-page #tab-docs pane is gone.)
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

pytestmark = allure.suite("Docs")

PROJECT_NAME = f"DocsTest-{int(time.time()) % 100000}"


@pytest.fixture
def docs_inbox_page(admin_page: Page) -> Page:
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    # The Docs tab is a link to the inbox, not an in-page pane.
    page.locator('a.tab-btn:has-text("Docs")').first.click()
    page.wait_for_load_state("networkidle", timeout=10_000)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


@allure.title("Docs tab opens the Docs inbox")
def test_docs_tab_opens_inbox(docs_inbox_page: Page):
    page = docs_inbox_page
    assert re.search(r"/projects/\d+/docs$", page.url.split("?")[0]), page.url
    expect(page.locator("#inbxDropZone")).to_be_visible(timeout=10_000)
    take_step_screenshot(page, "docs-inbox")


@allure.title("Docs inbox shows the upload control and hidden file input")
def test_docs_inbox_upload_controls(docs_inbox_page: Page):
    page = docs_inbox_page
    expect(page.locator("#inbxUploadBtn")).to_be_visible(timeout=5000)
    assert page.locator("#inbxFileInput").count() == 1, "Missing #inbxFileInput"


@allure.title("Docs inbox search box is present")
def test_docs_inbox_search_present(docs_inbox_page: Page):
    page = docs_inbox_page
    expect(page.locator("#inbxSearch")).to_be_visible(timeout=5000)
