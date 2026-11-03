"""
Docs tests: the budget page's Docs tab opens the Docs inbox
(/projects/<pid>/docs) — drop zone, upload control, search.
(Cleanup B, 2026-11-02: the in-page #tab-docs pane is gone.)

Uploading a document needs the OCR (Veryfi) + Dropbox pipeline, so the
upload round-trip only runs when those env vars are present.
"""

import os
import re

import pytest
from playwright.sync_api import Page, expect

from tests.conftest import BASE_URL
from tests.utils.helpers import take_step_screenshot


@pytest.fixture
def docs_inbox_page(project) -> Page:
    page = project.page
    page.goto(f"{BASE_URL}/projects/{project.pid}/docs", wait_until="networkidle")
    expect(page.locator("#inbxDropZone")).to_be_visible(timeout=10_000)
    return page


def test_docs_tab_opens_inbox(project):
    page = project.page
    page.goto(project.budget_url, wait_until="networkidle")
    page.locator('nav.budget-tabstrip a.tab-btn:has-text("Docs")').first.click()
    page.wait_for_load_state("networkidle")
    assert re.search(r"/projects/\d+/docs$", page.url.split("?")[0]), page.url
    expect(page.locator("#inbxDropZone")).to_be_visible(timeout=10_000)
    take_step_screenshot(page, "docs-inbox")


def test_docs_inbox_upload_controls(docs_inbox_page: Page):
    page = docs_inbox_page
    expect(page.locator("#inbxUploadBtn")).to_be_visible()
    assert page.locator("#inbxFileInput").count() == 1, "Missing #inbxFileInput"


def test_docs_inbox_search_present(docs_inbox_page: Page):
    expect(docs_inbox_page.locator("#inbxSearch")).to_be_visible()


def test_old_tab_docs_link_lands_on_inbox(project):
    """?tab=docs bookmarks from before Cleanup B redirect to the inbox."""
    page = project.page
    page.goto(f"{project.budget_url}?tab=docs", wait_until="networkidle")
    page.wait_for_url(re.compile(r"/projects/\d+/docs"), timeout=10_000)
    expect(page.locator("#inbxDropZone")).to_be_visible(timeout=10_000)


@pytest.mark.skipif(not (os.getenv("VERYFI_CLIENT_ID") and (os.getenv("DROPBOX_ACCESS_TOKEN") or os.getenv("DROPBOX_REFRESH_TOKEN"))),
                    reason="uploading a receipt needs Veryfi + Dropbox (VERYFI_CLIENT_ID + DROPBOX_ACCESS_TOKEN or DROPBOX_REFRESH_TOKEN)")
def test_docs_upload_round_trip(docs_inbox_page: Page):
    page = docs_inbox_page
    page.set_input_files("#inbxFileInput", {
        "name": "e2e-receipt.pdf", "mimeType": "application/pdf",
        "buffer": b"%PDF-1.4\n%fake\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"})
    expect(page.locator("text=e2e-receipt").first).to_be_visible(timeout=60_000)
