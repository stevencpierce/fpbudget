"""
Project Settings pane tests: #tab-settings inside the budget page —
Budget Info, Display, Payroll and fees, Project Fringes, Maintenance
(regrouped in Cleanup C; company-wide settings moved to Admin in Cleanup E).
"""

import pytest
from playwright.sync_api import Page, expect

from tests.utils.helpers import take_step_screenshot


@pytest.fixture
def settings_page(project) -> Page:
    page = project.page
    page.goto(f"{project.budget_url}?tab=settings", wait_until="networkidle")
    expect(page.locator("#tab-settings.active")).to_be_visible(timeout=10_000)
    return page


def test_settings_tab_activates(project):
    page = project.page
    page.goto(project.budget_url, wait_until="networkidle")
    page.click('.tab-btn.tab-settings')
    expect(page.locator("#tab-settings.active")).to_be_visible(timeout=10_000)
    for heading in ("Budget Info", "Project Fringes", "Maintenance"):
        expect(page.locator(f"#tab-settings h3:has-text('{heading}')").first).to_be_visible()
    take_step_screenshot(page, "settings-tab-active")


def test_settings_points_company_wide_things_at_admin(settings_page: Page):
    page = settings_page
    link = page.locator('#tab-settings a:has-text("Open Admin")').first
    expect(link).to_be_visible()
    assert "/admin" in (link.get_attribute("href") or "")


def test_settings_budget_name_input(settings_page: Page):
    page = settings_page
    expect(page.locator("#set-name")).to_be_visible()
    assert page.locator("#set-name").input_value() != "", "Budget name should be populated"


def test_settings_payroll_and_fee_inputs(settings_page: Page):
    page = settings_page
    for input_id in ("#set-workers-comp", "#set-payroll-fee"):
        expect(page.locator(input_id)).to_be_visible()


def test_settings_display_selects(settings_page: Page):
    page = settings_page
    for sel in ("#set-timezone", "#set-payroll-profile", "#set-payroll-week-start"):
        expect(page.locator(sel)).to_be_visible()
    assert page.locator("#set-payroll-profile option").count() >= 1


def test_settings_production_details(settings_page: Page):
    page = settings_page
    for input_id in ("#set-client-name", "#set-prepared-by", "#set-prepared-by-email"):
        expect(page.locator(input_id)).to_be_visible()


def test_settings_save_budget_name_round_trips(project):
    page = project.page
    page.goto(f"{project.budget_url}?tab=settings", wait_until="networkidle")
    name_input = page.locator("#set-name")
    expect(name_input).to_be_visible(timeout=10_000)
    original = name_input.input_value()
    new_name = original + " [edited]"
    name_input.fill(new_name)
    page.click("#btn-save-settings")
    page.wait_for_timeout(800)
    page.goto(f"{project.budget_url}?tab=settings", wait_until="networkidle")
    expect(page.locator("#set-name")).to_have_value(new_name, timeout=10_000)
    # put it back so the other modules' assumptions about the name hold
    page.locator("#set-name").fill(original)
    page.click("#btn-save-settings")
    page.wait_for_timeout(800)
