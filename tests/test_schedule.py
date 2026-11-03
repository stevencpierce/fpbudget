"""
Schedule / Gantt tests: /projects/{pid}/budget/{bid}/gantt —
date range controls, week navigation, range buttons, totals panel.
"""

import re

import pytest
from playwright.sync_api import Page, expect

from tests.utils.helpers import take_step_screenshot


@pytest.fixture
def gantt_page(project) -> Page:
    page = project.page
    page.goto(f"{project.budget_url}/gantt", wait_until="networkidle", timeout=30_000)
    expect(page.locator(".gantt-toolbar")).to_be_visible(timeout=10_000)
    return page


def test_schedule_tab_opens_gantt(project):
    page = project.page
    page.goto(project.budget_url, wait_until="networkidle")
    page.locator('nav.budget-tabstrip a.tab-btn:has-text("Schedule")').first.click()
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(re.compile(r"/projects/\d+/budget/\d+/gantt"))
    expect(page.locator(".gantt-toolbar")).to_be_visible(timeout=10_000)
    take_step_screenshot(page, "gantt-loaded")


def test_gantt_toolbar_and_totals(gantt_page: Page):
    page = gantt_page
    expect(page.locator("#gantt-totals-panel")).to_be_visible()
    expect(page.locator("#gantt-totals-table")).to_be_visible()


def test_gantt_date_controls(gantt_page: Page):
    page = gantt_page
    for sel in ("#gantt-start-input", "#gantt-end-input", "#btn-prev-week", "#btn-next-week"):
        expect(page.locator(sel)).to_be_visible()


def test_gantt_next_week_advances_start(gantt_page: Page):
    page = gantt_page
    start_input = page.locator("#gantt-start-input")
    before = start_input.input_value()
    assert before, "start date should be populated"
    page.locator("#btn-next-week").click()
    expect(start_input).not_to_have_value(before, timeout=5_000)
    after = start_input.input_value()
    from datetime import date
    d0, d1 = date.fromisoformat(before), date.fromisoformat(after)
    assert (d1 - d0).days == 7, f"Next Week moved {before} -> {after}"


def test_gantt_range_buttons(gantt_page: Page):
    page = gantt_page
    for label in ("2W", "3W", "4W"):
        expect(page.locator(f'.gantt-range-btns button:has-text("{label}")').first).to_be_visible()
