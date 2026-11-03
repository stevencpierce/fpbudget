"""
Export tests: the budget header's Export menu → Export Options dialog →
PDF / CSV download. (Direct export.pdf / export.csv links were replaced by
the options dialog; the dialog opens PDFs in a new tab and navigates to
CSVs, so each path is covered on its own.)
"""

import os

import pytest
from playwright.sync_api import Page, expect

from tests.utils.helpers import take_step_screenshot


@pytest.fixture
def budget_page(project) -> Page:
    page = project.page
    page.goto(project.budget_url, wait_until="networkidle")
    expect(page.locator("#budget-name-heading")).to_be_visible(timeout=10_000)
    return page


def _export_menu(page: Page):
    """The Export ▾ dropdown (not the nav's Libraries ▾ dropdown)."""
    return page.locator('.dropdown:has(> button:has-text("Export")) .dropdown-menu').first


def _open_export_options(page: Page, item_text: str):
    """Open the Export ▾ menu, pick an item, and wait for the options dialog."""
    page.locator('button:has-text("Export")').first.click()
    _export_menu(page).locator(f'a:has-text("{item_text}")').first.click()
    expect(page.locator("#export-options-overlay")).to_be_visible(timeout=5_000)
    expect(page.locator("#btn-export-go")).to_be_visible()


def test_export_menu_lists_pdf_and_csv(budget_page: Page):
    page = budget_page
    page.locator('button:has-text("Export")').first.click()
    menu = _export_menu(page)
    for label in ("Top Sheet PDF", "Full Detail PDF", "Top Sheet CSV", "Line Detail CSV"):
        expect(menu.locator(f'a:has-text("{label}")')).to_be_visible(timeout=5_000)
    take_step_screenshot(page, "export-menu")


def test_pdf_export_options_dialog(budget_page: Page):
    page = budget_page
    _open_export_options(page, "Top Sheet PDF")
    expect(page.locator("#export-opts-target")).to_contain_text("PDF")
    page.locator('#export-options-overlay button:has-text("Cancel")').click()
    expect(page.locator("#export-options-overlay")).to_be_hidden()


def test_pdf_export_serves_a_pdf(budget_page: Page, project):
    """The dialog opens the PDF in a new tab (window.open). Headless Chromium
    has no PDF viewer, so capture the URL the dialog built and fetch it."""
    page = budget_page
    _open_export_options(page, "Top Sheet PDF")
    page.evaluate("() => { window.__exportUrl = null; window.open = (u) => { window.__exportUrl = u; return null; }; }")
    page.click("#btn-export-go")
    page.wait_for_function("() => !!window.__exportUrl", timeout=5_000)
    url = page.evaluate("() => window.__exportUrl")
    assert f"/projects/{project.pid}/budget/{project.bid}/export.pdf" in url, url
    r = page.evaluate(
        """async (url) => { const r = await fetch(url, {credentials: 'same-origin'});
                            const b = await r.arrayBuffer();
                            return {status: r.status, type: r.headers.get('content-type'), size: b.byteLength,
                                    head: String.fromCharCode(...new Uint8Array(b.slice(0, 5)))}; }""",
        url)
    assert r["status"] == 200, r
    assert "pdf" in (r["type"] or "").lower(), r
    assert r["head"] == "%PDF-", r
    assert r["size"] > 1000, f"PDF too small ({r['size']} bytes)"


def test_csv_export_downloads(budget_page: Page):
    page = budget_page
    _open_export_options(page, "Line Detail CSV")
    with page.expect_download(timeout=60_000) as download_info:
        page.click("#btn-export-go")
    download = download_info.value
    assert ".csv" in download.suggested_filename.lower(), download.suggested_filename
    path = download.path()
    assert path is not None and os.path.getsize(path) > 50, "CSV file empty"
    with open(path, encoding="utf-8", errors="replace") as f:
        head = f.read(2000)
    assert "," in head, "not a CSV"
