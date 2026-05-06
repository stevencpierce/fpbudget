"""
Export tests: PDF and CSV download from budget pages.
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

pytestmark = allure.suite("Export")

PROJECT_NAME = f"ExportTest-{int(time.time()) % 100000}"


@pytest.fixture
def export_budget_page(admin_page: Page) -> Page:
    """Create a test project with template and navigate to its budget."""
    page = admin_page
    create_project(page, PROJECT_NAME, template="FP Standard")
    open_project_by_name(page, PROJECT_NAME)
    page.wait_for_timeout(2000)
    yield page
    go_to_projects(page)
    delete_project(page, PROJECT_NAME)


# ---------------------------------------------------------------------------
# PDF Export
# ---------------------------------------------------------------------------

def _open_export_menu(page: Page):
    """Open the Export dropdown menu on the budget page."""
    btn = page.locator('button:has-text("Export")').first
    expect(btn).to_be_visible(timeout=10_000)
    btn.click()
    page.wait_for_timeout(500)


@allure.title("PDF export link is available")
def test_pdf_export_button_visible(export_budget_page: Page):
    page = export_budget_page
    _open_export_menu(page)
    pdf_link = page.locator('a[href*="export.pdf"]').first
    expect(pdf_link).to_be_visible(timeout=5000)
    take_step_screenshot(page, "pdf-link-visible")


@allure.title("PDF export triggers a download")
def test_pdf_export_download(export_budget_page: Page):
    page = export_budget_page
    _open_export_menu(page)
    pdf_link = page.locator('a[href*="export.pdf"]').first
    expect(pdf_link).to_be_visible(timeout=5000)

    with page.expect_download(timeout=60_000) as download_info:
        pdf_link.click()
    download = download_info.value

    assert ".pdf" in download.suggested_filename.lower(), (
        f"Expected .pdf file, got: {download.suggested_filename}"
    )
    path = download.path()
    assert path is not None
    allure.attach(
        f"Downloaded: {download.suggested_filename} ({path})",
        name="pdf-download-info",
        attachment_type=allure.attachment_type.TEXT,
    )


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------

@allure.title("CSV export link is available")
def test_csv_export_button_visible(export_budget_page: Page):
    page = export_budget_page
    _open_export_menu(page)
    csv_link = page.locator('a[href*="export.csv"]').first
    expect(csv_link).to_be_visible(timeout=5000)
    take_step_screenshot(page, "csv-link-visible")


@allure.title("CSV export triggers a download")
def test_csv_export_download(export_budget_page: Page):
    page = export_budget_page
    _open_export_menu(page)
    csv_link = page.locator('a[href*="export.csv"]').first
    expect(csv_link).to_be_visible(timeout=5000)

    with page.expect_download(timeout=60_000) as download_info:
        csv_link.click()
    download = download_info.value

    assert ".csv" in download.suggested_filename.lower(), (
        f"Expected .csv file, got: {download.suggested_filename}"
    )
    path = download.path()
    assert path is not None
    allure.attach(
        f"Downloaded: {download.suggested_filename} ({path})",
        name="csv-download-info",
        attachment_type=allure.attachment_type.TEXT,
    )


# ---------------------------------------------------------------------------
# Export content sanity
# ---------------------------------------------------------------------------

@allure.title("Exported PDF has non-zero file size")
def test_pdf_export_not_empty(export_budget_page: Page):
    page = export_budget_page
    _open_export_menu(page)
    pdf_link = page.locator('a[href*="export.pdf"]').first
    expect(pdf_link).to_be_visible(timeout=5000)

    with page.expect_download(timeout=60_000) as download_info:
        pdf_link.click()
    download = download_info.value
    path = download.path()
    if path:
        import os
        size = os.path.getsize(path)
        assert size > 100, f"PDF file too small ({size} bytes), likely empty or error"
        allure.attach(f"PDF size: {size} bytes", name="pdf-size", attachment_type=allure.attachment_type.TEXT)
