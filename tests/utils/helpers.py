"""
Reusable helpers for FPBudget Playwright tests.
"""

from __future__ import annotations

import os
import re
import allure
from playwright.sync_api import Page, expect

BASE_URL = os.getenv("BASE_URL", "https://fp-budget.onrender.com")
COLD_START_TIMEOUT = 60_000
DEFAULT_TIMEOUT = 15_000


# ---------------------------------------------------------------------------
# Login / Logout
# ---------------------------------------------------------------------------

def login(page: Page, email: str, password: str):
    """Navigate to /login, fill credentials, wait for projects page."""
    with allure.step(f"Login as {email}"):
        page.goto(f"{BASE_URL}/login", wait_until="networkidle", timeout=COLD_START_TIMEOUT)
        page.fill('input[type="email"]', email)
        page.fill('input[type="password"]', password)
        page.click('button[type="submit"]')
        page.wait_for_url("**/", timeout=DEFAULT_TIMEOUT)


def logout(page: Page):
    """Click the logout link (or navigate directly)."""
    with allure.step("Logout"):
        page.goto(f"{BASE_URL}/logout", wait_until="networkidle", timeout=DEFAULT_TIMEOUT)
        expect(page).to_have_url(re.compile(r"/login"), timeout=DEFAULT_TIMEOUT)


# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------

def go_to_projects(page: Page):
    """Navigate to the projects dashboard."""
    page.goto(f"{BASE_URL}/", wait_until="networkidle", timeout=DEFAULT_TIMEOUT)
    expect(page.locator("h1:has-text('Projects')")).to_be_visible(timeout=DEFAULT_TIMEOUT)


def open_project_budget(page: Page, project_name: str):
    """Find a project card by name and click 'Open Budget'."""
    with allure.step(f"Open budget for project: {project_name}"):
        card = page.locator(f"text={project_name}").locator("..")
        card.locator("text=Open Budget").click()
        page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT)


# ---------------------------------------------------------------------------
# Project creation / cleanup
# ---------------------------------------------------------------------------

def create_project(page: Page, name: str, template: str = "Blank budget") -> str:
    """Create a new project via the modal. Returns the project name."""
    with allure.step(f"Create project: {name}"):
        go_to_projects(page)
        page.click("text=+ New Project")
        page.fill('input[placeholder="e.g. GINTS Season 2"]', name)
        # The template dropdown uses name="template_id"; "Blank budget" actually
        # shows as "— Blank budget —" in options. Use partial match.
        if template and template.lower() not in ("blank budget", "blank", ""):
            template_select = page.locator('select[name="template_id"]')
            # Match option by visible text, fuzzy on dashes
            options = template_select.locator("option").all_text_contents()
            match = next((o for o in options if template.lower() in o.lower()), None)
            if match:
                template_select.select_option(label=match)
        page.click('button:has-text("Create")')
        page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT)
        # App redirects to the new project's budget page after creation.
        # Verify by URL (either on /projects/N/budget or back on projects list).
        import re as _re
        current_url = page.url
        if _re.search(r"/projects/\d+/budget", current_url):
            # Successfully redirected to new budget page
            return name
        # Fallback: back on projects list, verify card
        expect(
            page.locator(f".project-card:has-text('{name}')").first
        ).to_be_visible(timeout=DEFAULT_TIMEOUT)
    return name


def find_project_card(page: Page, name: str):
    """Return the .project-card locator that contains the given project name."""
    return page.locator(f".project-card:has-text('{name}')").first


def open_project_by_name(page: Page, name: str):
    """Navigate to the budget page for the named project via its card."""
    # If we're already on a budget page, we're done
    if re.search(r"/projects/\d+/budget", page.url):
        return
    go_to_projects(page)
    card = find_project_card(page, name)
    expect(card).to_be_visible(timeout=DEFAULT_TIMEOUT)
    card.locator("text=Open Budget").first.click()
    page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT)


def delete_project(page: Page, name: str):
    """Delete a project via the three-dot menu on its card."""
    with allure.step(f"Delete project: {name}"):
        go_to_projects(page)
        card = find_project_card(page, name)
        if card.count() == 0:
            return  # Already gone
        # Auto-accept any browser confirm() dialogs triggered by the delete form
        page.once("dialog", lambda d: d.accept())
        # The three-dot menu button has title="More actions" and text "⋯"
        card.locator('button[title="More actions"]').first.click()
        # Click the Delete Project button inside the now-visible menu
        card.locator('button:has-text("Delete Project")').first.click()
        # If a fallback confirmation div appears
        confirm = page.locator("text=Confirm")
        if confirm.is_visible(timeout=3000):
            confirm.click()
        page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT)


# ---------------------------------------------------------------------------
# Wait helpers
# ---------------------------------------------------------------------------

def wait_for_toast(page: Page, text: str | None = None, timeout: int = 10_000):
    """Wait for a toast/flash message to appear."""
    selector = ".flash, .toast, .alert, [role='alert']"
    locator = page.locator(selector)
    if text:
        locator = locator.filter(has_text=text)
    expect(locator.first).to_be_visible(timeout=timeout)


def wait_for_value_update(page: Page, selector: str, expected: str, timeout: int = 10_000):
    """Poll until an element's text content matches expected value."""
    expect(page.locator(selector)).to_have_text(expected, timeout=timeout)


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------

def assert_url_contains(page: Page, fragment: str):
    """Assert the current URL contains the given fragment."""
    expect(page).to_have_url(f"**{fragment}**")


def assert_element_count(page: Page, selector: str, count: int):
    """Assert exact number of matching elements."""
    expect(page.locator(selector)).to_have_count(count)


def take_step_screenshot(page: Page, name: str):
    """Capture a screenshot and attach to Allure report."""
    screenshot = page.screenshot(full_page=True)
    allure.attach(screenshot, name=name, attachment_type=allure.attachment_type.PNG)
