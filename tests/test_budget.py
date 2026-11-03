"""
Budget page tests on one FP Standard project: structure, adding a line
through the modal, the Top Sheet pane, the version picker. (Folds in the
old test_topsheet.py and test_versions.py, which only looked for elements
"if visible".)
"""

import re

import pytest
from playwright.sync_api import Page, expect

from tests.utils.helpers import take_step_screenshot


def _money(text: str) -> float:
    m = re.search(r"-?[\d,]+(?:\.\d+)?", text or "")
    return float(m.group(0).replace(",", "")) if m else 0.0


@pytest.fixture
def budget_page(project) -> Page:
    page = project.page
    page.goto(f"{project.budget_url}?tab=working", wait_until="networkidle")
    expect(page.locator("#tab-working.active")).to_be_visible(timeout=10_000)
    return page


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------

def test_budget_loads_with_sections(budget_page: Page, project):
    page = budget_page
    expect(page).to_have_url(re.compile(r"/projects/\d+/budget/\d+"))
    expect(page.locator("#budget-name-heading")).to_contain_text(project.name)
    blocks = page.locator("#tab-working .section-block[data-section]")
    assert blocks.count() >= 5, f"FP Standard should bring several sections, got {blocks.count()}"
    expect(page.locator("#budget-control-bar")).to_be_visible()
    expect(page.locator("#wb-search")).to_be_visible()
    take_step_screenshot(page, "budget-loaded")


def test_budget_tabs_present(budget_page: Page):
    page = budget_page
    tabs = page.locator("nav.budget-tabstrip .tabs .tab-btn")
    labels = " ".join(tabs.all_inner_texts())
    for t in ("Overview", "Top Sheet", "Budget", "Schedule", "People", "Actuals", "Docs", "More"):
        assert t in labels, f"missing tab {t!r} in {labels!r}"
    # Docs and Actuals are links to their own screens (Cleanup B).
    assert "/docs" in page.locator('nav.budget-tabstrip a.tab-btn:has-text("Docs")').get_attribute("href")
    assert "/actuals" in page.locator('nav.budget-tabstrip a.tab-btn:has-text("Actuals")').get_attribute("href")


# ---------------------------------------------------------------------------
# Add a line through the modal — the row exists after the reload and the
# Top Sheet grand total moved by its amount
# ---------------------------------------------------------------------------

def test_add_flat_line_updates_totals(project):
    page = project.page
    page.goto(f"{project.budget_url}?tab=topsheet-detail", wait_until="networkidle")
    before = _money(page.locator("#tab-topsheet-detail .grand-total-row td").nth(1).inner_text())

    page.goto(f"{project.budget_url}?tab=working", wait_until="networkidle")
    page.locator('#budget-control-bar button:has-text("Single Line")').click()
    expect(page.locator("#add-line-modal")).to_be_visible()
    section = page.locator("#modal-section")
    section.select_option(index=1)
    page.fill("#modal-desc", "E2E flat line")
    # Per-unit rate × 1 × 1. (The "Flat total $" box is sent together with
    # rate=0, which upsert_line treats as an explicit zero rate and so saves
    # $0 — a pre-existing bug noted in AUDIT_CHECKLIST Cleanup F.)
    page.fill("#modal-nl-rate", "1234")
    with page.expect_navigation(wait_until="networkidle", timeout=30_000):
        page.locator('#add-line-modal button:has-text("Add Line")').click()
    row = page.locator("#tab-working tr.line-row", has_text="E2E flat line")
    expect(row.first).to_be_visible(timeout=10_000)

    page.goto(f"{project.budget_url}?tab=topsheet-detail", wait_until="networkidle")
    after = _money(page.locator("#tab-topsheet-detail .grand-total-row td").nth(1).inner_text())
    assert after >= before + 1234 - 0.01, f"grand total did not move: {before} -> {after}"
    take_step_screenshot(page, "topsheet-after-add-line")


# ---------------------------------------------------------------------------
# Top Sheet pane
# ---------------------------------------------------------------------------

def test_topsheet_pane(project):
    page = project.page
    page.goto(f"{project.budget_url}?tab=topsheet-detail", wait_until="networkidle")
    expect(page.locator("#tab-topsheet-detail.active")).to_be_visible(timeout=10_000)
    table = page.locator("#tab-topsheet-detail table.topsheet-table").first
    expect(table).to_be_visible()
    assert table.locator("tr.subtotal-row").count() >= 1
    expect(table.locator("tr.grand-total-row")).to_have_count(1)
    expect(table.locator("tr.grand-total-row")).to_contain_text("GRAND TOTAL")


def test_overview_pane_is_default(project):
    page = project.page
    page.goto(project.budget_url, wait_until="networkidle")
    expect(page.locator("#tab-topsheet.active")).to_be_visible(timeout=10_000)
    expect(page.locator("#ov-bydept")).to_be_visible()


# ---------------------------------------------------------------------------
# Versions
# ---------------------------------------------------------------------------

def test_version_picker(budget_page: Page):
    page = budget_page
    picker = page.locator(".version-picker-btn").first
    expect(picker).to_be_visible(timeout=10_000)
    text = picker.inner_text()
    assert re.search(r"\bv\d+\b", text), f"Unexpected picker text: {text!r}"
    assert "1 version" in text, f"a fresh project should have one version: {text!r}"
    picker.click()
    menu = page.locator(".version-menu").first
    expect(menu).to_be_visible(timeout=3_000)
    expect(menu.locator(".version-item-name").first).to_have_text(re.compile(r"v\d+"))
